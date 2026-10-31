"""F6: cockpit.job_reliability recounts retries per job_run_id (upstream groups by task run_id)."""

from __future__ import annotations

import shutil
from datetime import datetime, timedelta, timezone

import pytest

from cockpit.pipeline import StepContext, discover_steps
from cockpit.steps import job_reliability as step_module
from cockpit.steps.job_reliability import (
    SOURCE_TASK_TIMELINE,
    SOURCE_UPSTREAM_FALLBACK,
    STEP,
    build_job_reliability_sql,
    retry_fallback_reason,
)
from databricks_finops.config import AppConfig, ThresholdConfig
from databricks_finops.preflight import PreflightResult, TableCapability

TASK = "system.lakeflow.job_task_run_timeline"
RUNS = "system.lakeflow.job_run_timeline"
TASK_COLUMNS = {"account_id", "workspace_id", "job_id", "run_id", "job_run_id", "parent_run_id",
                "period_start_time", "period_end_time", "task_key", "result_state"}
RUN_COLUMNS = {"workspace_id", "job_id", "run_id", "period_start_time", "period_end_time", "result_state"}
FULL = {TASK: TASK_COLUMNS, RUNS: RUN_COLUMNS}


def _config(**thresholds) -> AppConfig:
    return AppConfig(catalog="spark_catalog", schema="cockpit", lookback_days=30, currency_code="USD",
                     display_currency="USD", fallback_dbu_price=0.55, bundle_root=".",
                     thresholds=ThresholdConfig(**thresholds))


def _preflight(tables: dict[str, set[str]]) -> PreflightResult:
    caps = [TableCapability(source_name=name, check_name=name, required=False, available=True,
                            message="ok", affected_output="x") for name in tables]
    return PreflightResult(capabilities=caps, columns_by_table=dict(tables))


def _ctx(preflight, spark=None, **thresholds) -> StepContext:
    return StepContext(spark=spark, config=_config(**thresholds), run_id="run-1", preflight=preflight)


# ---------------------------------------------------------------------------
# SQL generation (no Spark needed; runs in CI)
# ---------------------------------------------------------------------------

def test_step_is_discovered_and_writes_job_reliability():
    assert "build_job_reliability" in [s.name for s in discover_steps()]
    assert STEP.output_table == "job_reliability"
    assert STEP.required


def test_sql_groups_task_attempts_by_job_run_id_and_task_key():
    sql, reason = build_job_reliability_sql(_ctx(_preflight(FULL)))

    assert reason is None
    assert "CREATE OR REPLACE TABLE `spark_catalog`.`cockpit`.`job_reliability`" in sql
    assert "FROM `spark_catalog`.`cockpit`.`job_reliability_summary` u" in sql
    assert "COUNT(DISTINCT run_id) AS attempts" in sql
    assert "SUM(task_attempts.attempts - 1)" in sql
    assert "GROUP BY task_attempts.workspace_id, task_attempts.job_id, task_attempts.job_run_id" in sql
    assert "LEFT SEMI JOIN" in sql and f"FROM {RUNS}" in sql
    assert f"'{SOURCE_TASK_TIMELINE}' AS retry_count_source" in sql
    assert "INTERVAL 32 DAYS" in sql  # task window = lookback + margin
    assert "INTERVAL 30 DAYS" in sql  # counted runs = lookback
    assert "parent_run_id IS NULL OR" in sql
    assert "unknown_state_run_count, 0) > 0 AS review_required" in sql
    assert ">= 5 THEN 'RETRY_HEAVY'" in sql and ">= 20 THEN 'FAILURE_HEAVY'" in sql
    assert "{" not in sql and "}" not in sql


def test_without_parent_run_id_for_each_iterations_are_not_excluded_and_log_says_so():
    statements: list[str] = []

    class FakeSpark:
        def sql(self, statement):
            statements.append(statement)

    preflight = _preflight({TASK: TASK_COLUMNS - {"parent_run_id"}, RUNS: RUN_COLUMNS})
    message = step_module.build(_ctx(preflight, spark=FakeSpark()))
    assert "parent_run_id" not in statements[0]
    assert "for-each iterations cannot be excluded" in message


@pytest.mark.parametrize("tables, expected", [
    ({}, f"{TASK} unavailable"),
    ({TASK: TASK_COLUMNS - {"job_run_id"}}, "lacks columns ['job_run_id']"),
    ({TASK: TASK_COLUMNS - {"task_key", "run_id"}, RUNS: RUN_COLUMNS}, "lacks columns ['run_id', 'task_key']"),
    ({TASK: TASK_COLUMNS}, f"{RUNS} unavailable"),
    ({TASK: TASK_COLUMNS, RUNS: RUN_COLUMNS - {"result_state"}}, "lacks columns ['result_state']"),
])
def test_falls_back_to_upstream_retry_count_when_task_timeline_is_unusable(tables, expected):
    preflight = _preflight(tables)
    assert expected in retry_fallback_reason(preflight)

    sql, reason = build_job_reliability_sql(_ctx(preflight))
    assert expected in reason
    assert TASK not in sql and RUNS not in sql
    assert "CAST(u.retry_count AS BIGINT) AS corrected_retry_count" in sql
    assert f"'{SOURCE_UPSTREAM_FALLBACK}' AS retry_count_source" in sql


def test_build_reports_fallback_in_the_run_log():
    statements: list[str] = []

    class FakeSpark:
        def sql(self, statement):
            statements.append(statement)

    message = step_module.build(_ctx(_preflight({}), spark=FakeSpark()))
    assert len(statements) == 1
    assert "upstream retry_count" in message and "unavailable" in message


# ---------------------------------------------------------------------------
# SQL behaviour on a tiny local Spark fixture (skipped when pyspark/delta/Java are absent, e.g. CI)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    delta = pytest.importorskip("delta")
    if shutil.which("java") is None and not __import__("os").environ.get("JAVA_HOME"):
        pytest.skip("Java is not available for local Spark")
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("warehouse").as_posix()
    builder = (
        SparkSession.builder.appName("test-job-reliability").master("local[1]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    session = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


class _Redirect:
    """Minimal stand-in for sandbox RedirectingSpark: system.* -> temp views."""

    def __init__(self, spark):
        self._spark = spark

    def sql(self, statement):
        statement = statement.replace(TASK, "fx_task_timeline").replace(RUNS, "fx_run_timeline")
        return self._spark.sql(statement)


def _load_fixture(spark):
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    t0 = now - timedelta(days=2)
    edge = now - timedelta(days=31)  # before the 30-day lookback, inside the 32-day task window
    old = now - timedelta(days=40)

    # (job_id, job_run_id, parent_run_id, task_key, task run_id, start)
    task_rows = [
        # job 1, run r1: extract has one attempt split over two hourly rows -> 0 retries
        ("1", "r1", "r1", "extract", "t1", t0), ("1", "r1", "r1", "extract", "t1", t0 + timedelta(hours=1)),
        # transform: 3 attempts (NULL parent also counts as top level) -> 2 retries
        ("1", "r1", "r1", "transform", "t2", t0), ("1", "r1", None, "transform", "t3", t0),
        ("1", "r1", "r1", "transform", "t4", t0),
        # for-each task: one top-level run plus 3 iterations parented to it -> 0 retries
        ("1", "r1", "r1", "fanout", "f1", t0), ("1", "r1", "f1", "fanout", "f2", t0),
        ("1", "r1", "f1", "fanout", "f3", t0), ("1", "r1", "f1", "fanout", "f4", t0),
        # NULL task_key rows are ignored
        ("1", "r1", "r1", None, "n1", t0), ("1", "r1", "r1", None, "n2", t0),
        # job 1, run r2: load repaired once -> 1 retry
        ("1", "r2", "r2", "load", "t5", t0), ("1", "r2", "r2", "load", "t6", t0),
        # run r3: in the lookback but missing from job_run_timeline -> excluded
        ("1", "r3", "r3", "load", "t7", t0), ("1", "r3", "r3", "load", "t8", t0),
        # run r4: in the task window, but its job run is outside the lookback -> excluded
        ("1", "r4", "r4", "load", "t9", edge), ("1", "r4", "r4", "load", "t10", edge),
        # run r6: started before the lookback, still running inside it -> counted (1 retry)
        ("1", "r6", "r6", "load", "t11", edge), ("1", "r6", "r6", "load", "t12", edge),
        # NULL job_run_id rows are ignored
        ("1", None, None, "load", "t13", t0), ("1", None, None, "load", "t14", t0),
        # job 2: one attempt per task -> 0 retries
        ("2", "r5", "r5", "extract", "t15", t0), ("2", "r5", "r5", "load", "t16", t0),
    ]
    spark.createDataFrame(
        [("w1", j, rid, jr, parent, key, start) for j, jr, parent, key, rid, start in task_rows],
        "workspace_id string, job_id string, run_id string, job_run_id string, parent_run_id string, "
        "task_key string, period_start_time timestamp",
    ).createOrReplaceTempView("fx_task_timeline")

    hour = timedelta(hours=1)
    run_rows = [
        ("1", "r1", t0, "SUCCEEDED"), ("1", "r2", t0, "SUCCEEDED"),
        ("1", "r4", old, "SUCCEEDED"),
        ("1", "r6", edge, None), ("1", "r6", now - timedelta(days=29), "SUCCEEDED"),
        ("2", "r5", t0, "FAILED"),
        ("4", "r8", t0, None),  # final state unknown -> REVIEW_REQUIRED
    ]
    spark.createDataFrame(
        [("w1", j, r, start, start + hour, state) for j, r, start, state in run_rows],
        "workspace_id string, job_id string, run_id string, period_start_time timestamp, "
        "period_end_time timestamp, result_state string",
    ).createOrReplaceTempView("fx_run_timeline")

    spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.cockpit")
    spark.sql("""
        CREATE OR REPLACE TABLE spark_catalog.cockpit.job_reliability_summary USING DELTA AS
        SELECT * FROM VALUES
            ('w1', '1', 'etl', 3L, 3L, 0L, 0L, 0L, 0L, 0.0D, 'HEALTHY'),
            ('w1', '2', 'report', 5L, 4L, 1L, 0L, 0L, 0L, 20.0D, 'FAILURE_HEAVY'),
            ('w1', '3', 'never_ran', 0L, 0L, 0L, 0L, 0L, 0L, CAST(NULL AS DOUBLE), 'INSUFFICIENT_DATA'),
            ('w1', '4', 'unknown_state', 1L, 0L, 0L, 0L, 0L, 9L, 0.0D, 'RETRY_HEAVY'),
            ('w1', '5', 'almost_failing', 10L, 8L, 1L, 0L, 0L, 0L, 19.99D, 'HEALTHY')
        AS t(workspace_id, job_id, job_name, run_count, success_count, failed_count, cancelled_count,
             skipped_count, retry_count, failure_rate_pct, reliability_category)
    """)
    spark.sql("""
        CREATE OR REPLACE TABLE spark_catalog.cockpit.job_reliability_summary USING DELTA AS
        SELECT *, 10.0D AS avg_duration_minutes, 20.0D AS total_duration_minutes,
               1.5D AS estimated_failed_dbus, 0.825D AS estimated_failed_cost,
               TIMESTAMP'2026-01-01 00:00:00' AS last_run_start_time, 'SUCCEEDED' AS last_run_result_state,
               'LOW' AS confidence, 'up-run' AS run_id
        FROM spark_catalog.cockpit.job_reliability_summary
    """)


def _rows(spark):
    return {r["job_id"]: r.asDict() for r in spark.table("spark_catalog.cockpit.job_reliability").collect()}


SPARK_TASK_COLUMNS = {"workspace_id", "job_id", "run_id", "job_run_id", "parent_run_id", "task_key",
                      "period_start_time"}


def test_retries_are_counted_per_job_run_and_task(spark):
    _load_fixture(spark)
    message = STEP.run(_ctx(_preflight({TASK: SPARK_TASK_COLUMNS, RUNS: RUN_COLUMNS}),
                            spark=_Redirect(spark), retry_heavy_count=4))
    assert message.endswith("recounted per job_run_id and task_key")

    rows = _rows(spark)
    job1 = rows["1"]
    # r1 transform 2 + r2 load 1 + r6 load 1; for-each iterations, NULL task_key, r3 and r4 excluded
    assert (job1["retry_count"], job1["retried_run_count"], job1["upstream_retry_count"]) == (4, 3, 0)
    assert job1["retry_rate_pct"] == pytest.approx(100.0)
    assert job1["reliability_category"] == "RETRY_HEAVY"
    assert job1["upstream_reliability_category"] == "HEALTHY"
    assert job1["retry_count_source"] == SOURCE_TASK_TIMELINE
    assert job1["run_id"] == "run-1"

    job2 = rows["2"]
    assert (job2["retry_count"], job2["retried_run_count"]) == (0, 0)
    assert job2["reliability_category"] == "FAILURE_HEAVY"  # exactly high_failure_rate_pct (20)
    assert float(job2["estimated_failed_cost"]) == pytest.approx(0.825)

    assert rows["3"]["retry_count"] == 0 and rows["3"]["retry_rate_pct"] is None
    assert rows["3"]["reliability_category"] == "INSUFFICIENT_DATA"

    # Upstream said RETRY_HEAVY; corrected retries are 0 and a run has no final state.
    assert rows["4"]["retry_count"] == 0
    assert rows["4"]["reliability_category"] == "REVIEW_REQUIRED"

    assert rows["5"]["reliability_category"] == "HEALTHY"  # just under the failure threshold


def test_without_parent_run_id_for_each_iterations_count_as_retries(spark):
    _load_fixture(spark)
    preflight = _preflight({TASK: SPARK_TASK_COLUMNS - {"parent_run_id"}, RUNS: RUN_COLUMNS})
    message = STEP.run(_ctx(preflight, spark=_Redirect(spark), retry_heavy_count=4))
    assert "for-each iterations cannot be excluded" in message
    assert _rows(spark)["1"]["retry_count"] == 4 + 3


def test_fallback_without_task_timeline_keeps_upstream_values(spark):
    _load_fixture(spark)
    STEP.run(_ctx(_preflight({RUNS: RUN_COLUMNS}), spark=_Redirect(spark)))

    rows = _rows(spark)
    assert rows["1"]["retry_count"] == 0
    assert rows["1"]["retried_run_count"] is None
    assert rows["1"]["retry_count_source"] == SOURCE_UPSTREAM_FALLBACK
    assert rows["1"]["reliability_category"] == "HEALTHY"


def test_fallback_without_job_run_timeline_does_not_count_unfiltered_retries(spark):
    _load_fixture(spark)
    message = STEP.run(_ctx(_preflight({TASK: SPARK_TASK_COLUMNS}), spark=_Redirect(spark),
                            retry_heavy_count=4))
    assert f"{RUNS} unavailable" in message

    rows = _rows(spark)
    assert rows["1"]["retry_count"] == 0  # upstream value, not the unfiltered task timeline
    assert rows["1"]["retry_count_source"] == SOURCE_UPSTREAM_FALLBACK
    assert rows["4"]["retry_count"] == 9
    assert rows["4"]["reliability_category"] == "RETRY_HEAVY"
