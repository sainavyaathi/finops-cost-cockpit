"""F4 all-purpose misuse against the synthetic sandbox with local Spark + Delta.

Skipped without pyspark/delta (CI). Runs the full upstream + cockpit pipeline once, then reruns
only the F4 step against modified copies of the sources for the multi-type (R1) and
missing-source cases.
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "sandbox"))

from finops_sandbox import (  # noqa: E402
    ACCOUNT_ID, BROWSER_AGENT, PROD_WS, SYSTEM_TABLE_MAP, RedirectingSpark, _audit_row, _cluster_row, _schemas,
    _timeline_rows, _usage_row, build_synthetic_rows, generate_synthetic_system_tables, run_accelerator,
)

from cockpit.pipeline import StepContext  # noqa: E402
from cockpit.steps import allpurpose_misuse as f4  # noqa: E402
from databricks_finops.config import config_from_args, parse_args  # noqa: E402

SOURCE = "spark_catalog.f4_synthetic"
CATALOG = "spark_catalog"
SCHEMA = "f4_cockpit"
KPI = "0303-kpi-allpurpose-cluster"
EXPECTED = {
    "0101-shared-ds-cluster": ["SHARED_USERS"],
    "0202-bi-tableau-cluster": ["BI_TOOL_JDBC"],
    KPI: ["SCHEDULED_JOB"],
}


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()
    warehouse = tmp_path_factory.mktemp("f4_warehouse").as_posix()
    builder = (
        SparkSession.builder.appName("f4-tests").master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
    )
    try:
        session = configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception as exc:  # noqa: BLE001 - no JVM / Delta jars on this machine
        pytest.skip(f"no Spark session: {exc}")
    session.sparkContext.setLogLevel("ERROR")
    generate_synthetic_system_tables(session, SOURCE)
    _, logger = run_accelerator(session, SOURCE, CATALOG, SCHEMA, str(ROOT))
    session.f4_logger = logger
    yield session
    session.stop()


def _rows(spark, schema=SCHEMA):
    return {r["cluster_id"]: r for r in spark.table(f"{CATALOG}.{schema}.allpurpose_misuse").collect()}


def _run_step(spark, source_schema: str, out_schema: str) -> str:
    """Run only the F4 step against ``source_schema`` into ``out_schema`` (cost_base from the main run)."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{out_schema}")
    spark.sql(f"CREATE OR REPLACE VIEW {CATALOG}.{out_schema}.cost_base AS "
              f"SELECT * FROM {CATALOG}.{SCHEMA}.daily_cost")
    config = config_from_args(parse_args(["--catalog", CATALOG, "--schema", out_schema,
                                          "--bundle-root", str(ROOT)]))
    ctx = StepContext(spark=RedirectingSpark(spark, source_schema), config=config, run_id="f4-test",
                      preflight=None)
    return f4.STEP.run(ctx)


def _source_copy(spark, name: str, drop=(), replace=None) -> str:
    """A source schema of views over the synthetic tables, minus ``drop``, with ``replace`` rows."""
    schema = f"{CATALOG}.{name}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    schemas = _schemas()
    for table in SYSTEM_TABLE_MAP.values():
        spark.sql(f"DROP VIEW IF EXISTS {schema}.{table}")
        spark.sql(f"DROP TABLE IF EXISTS {schema}.{table}")
        if table in drop:
            continue
        if replace and table in replace:
            spark.createDataFrame(replace[table], schema=schemas[table]).write.format("delta") \
                .mode("overwrite").saveAsTable(f"{schema}.{table}")
        else:
            spark.sql(f"CREATE VIEW {schema}.{table} AS SELECT * FROM {SOURCE}.{table}")
    return schema


# --- the sandbox as generated ------------------------------------------------------------

def test_step_succeeds_in_the_full_pipeline(spark):
    record = next(r for r in spark.f4_logger.records if r.task_name == "build_allpurpose_misuse")
    assert record.result == "SUCCESS"
    assert record.rows_written == 3
    assert "BI_TOOL_JDBC 1, SCHEDULED_JOB 1, SHARED_USERS 1" in record.message


def test_exactly_the_planted_clusters_get_exactly_their_types(spark):
    rows = _rows(spark)
    assert {cid: list(r["misuse_types"]) for cid, r in rows.items()} == EXPECTED
    assert {cid: r["misuse_type"] for cid, r in rows.items()} == {cid: t[0] for cid, t in EXPECTED.items()}


@pytest.mark.parametrize("cluster_id, why", [
    ("job-1001-cluster", "CRON job on a job cluster is correct usage"),
    ("0404-zombie-autoterm-off", "a single user is not a shared cluster"),
    ("0707-autoterm-61-cluster", "no misuse signal"),
    ("wh-bi-001", "Tableau on a SQL warehouse is correct usage"),
])
def test_near_misses_are_not_flagged(spark, cluster_id, why):
    assert cluster_id not in _rows(spark), why


def test_manual_run_on_the_shared_cluster_is_not_a_scheduled_job(spark):
    shared = _rows(spark)["0101-shared-ds-cluster"]
    assert "SCHEDULED_JOB" not in shared["misuse_types"]
    assert shared["scheduled_run_count"] == 0


def test_sdk_token_login_and_warehouse_traffic_do_not_count_as_bi_on_a_cluster(spark):
    bi = _rows(spark)["0202-bi-tableau-cluster"]
    assert bi["bi_client_count"] == 2
    assert "Tableau JDBC" in bi["evidence"] and "Power BI ODBC" in bi["evidence"]
    assert "ivan@example.com" not in bi["evidence"]
    assert bi["confidence"] == "LOW"
    assert bi["suggested_action"] == "MOVE_BI_TO_SQL_WAREHOUSE"


def test_shared_users_counted_from_attach_notebook_events(spark):
    shared = _rows(spark)["0101-shared-ds-cluster"]
    assert shared["interactive_user_count"] == 3
    assert shared["estimated_monthly_savings"] == 0
    assert shared["suggested_action"] == "APPLY_SHARED_CLUSTER_POLICY"


def test_scheduled_job_found_without_billing_job_id_and_priced_at_jobs_sku(spark):
    kpi = _rows(spark)[KPI]
    assert kpi["scheduled_job_count"] == 1 and kpi["scheduled_run_count"] >= 100
    assert "hourly_kpi_refresh (CRON)" in kpi["evidence"]
    assert kpi["confidence"] == "HIGH"
    assert kpi["suggested_action"] == "MOVE_TO_JOB_COMPUTE"
    # Jobs SKU 0.15 vs all-purpose 0.55 on the run-overlapping usage.
    expected = kpi["scheduled_job_attributable_cost"] * (1 - Decimal("0.15") / Decimal("0.55"))
    assert abs(kpi["estimated_monthly_savings"] - expected) < Decimal("0.01")


def test_cost_matches_cost_base_and_savings_never_exceed_cost(spark):
    base = {r["cluster_id"]: r["cost"] for r in spark.sql(
        f"SELECT cluster_id, SUM(estimated_cost) AS cost FROM {CATALOG}.{SCHEMA}.cost_base "
        "GROUP BY cluster_id").collect()}
    for cid, row in _rows(spark).items():
        assert abs(float(row["cluster_cost"]) - float(base[cid])) < 0.01, cid
        assert 0 <= row["estimated_monthly_savings"] <= row["estimated_monthly_cost"], cid
        assert row["bi_tool_attributable_cost"] <= row["cluster_cost"], cid
        assert row["scheduled_job_attributable_cost"] <= row["cluster_cost"], cid
        assert 0 <= row["priority_score"] <= 100, cid


def test_one_row_per_cluster_and_columns_for_the_app_contract(spark):
    df = spark.table(f"{CATALOG}.{SCHEMA}.allpurpose_misuse")
    assert df.count() == df.select("workspace_id", "cluster_id").distinct().count()
    for column in ("cluster_id", "cluster_name", "owner", "misuse_type", "evidence", "estimated_monthly_cost",
                   "estimated_monthly_savings", "suggested_action", "workspace_id", "confidence",
                   "priority_score", "created_at", "run_id"):
        assert column in df.columns, column
    types = dict(df.dtypes)
    assert types["estimated_monthly_savings"] == "decimal(38,6)"
    assert types["misuse_types"] == "array<string>"


# --- several types on one cluster (R1) and missing sources -------------------------------

def _multi_type_audit_rows(rows):
    """Add BI JDBC requests and three notebook users to the scheduled-job cluster 0303."""
    import random

    rng = random.Random(7)
    extra = []
    today = date.today()
    for offset in range(1, 8):
        day = today - timedelta(days=offset)
        midnight = datetime.combine(day, datetime.min.time())
        for i in range(3):
            extra.append(_audit_row(rng, day, midnight + timedelta(hours=1, minutes=10 * i), "svc-tableau@example.com",
                                    "SimbaSparkJDBCDriver/2.6.36 (Tableau 2024.2)", "databrickssql", "commandSubmit",
                                    {"clusterId": KPI, "commandId": f"cmd-{offset}-{i}"}, workspace_id=PROD_WS))
        for user in ("amy@example.com", "ben@example.com", "cat@example.com"):
            extra.append(_audit_row(rng, day, midnight + timedelta(hours=10), user, BROWSER_AGENT, "notebook",
                                    "attachNotebook", {"notebookId": "nb-1", "clusterId": KPI, "path": "/x"}))
    return rows["access_audit"] + extra


@pytest.fixture(scope="module")
def multi_type(spark):
    rows = build_synthetic_rows()
    source = _source_copy(spark, "f4_multi_src", replace={"access_audit": _multi_type_audit_rows(rows)})
    message = _run_step(spark, source, "f4_multi")
    return message, _rows(spark, "f4_multi")


def test_several_types_on_one_cluster_count_savings_once(multi_type, spark):
    _, rows = multi_type
    kpi = rows[KPI]
    assert list(kpi["misuse_types"]) == ["SCHEDULED_JOB", "BI_TOOL_JDBC", "SHARED_USERS"]
    assert kpi["misuse_type"] == "SCHEDULED_JOB" and kpi["misuse_type_count"] == 3
    # Interactive hours (BI requests, notebook attaches) are taken out of the scheduled-job time.
    assert kpi["scheduled_job_attributable_cost"] < _rows(spark)[KPI]["scheduled_job_attributable_cost"]
    # R1: the kept saving is the largest candidate (scheduled job), the BI saving is not added.
    scheduled_only = kpi["scheduled_job_attributable_cost"] * (1 - Decimal("0.15") / Decimal("0.55"))
    assert abs(kpi["estimated_monthly_savings"] - scheduled_only) < Decimal("0.01")
    assert kpi["bi_tool_attributable_cost"] > 0
    assert "not added: BI_TOOL_JDBC" in kpi["savings_basis"]
    assert kpi["evidence"].index("SCHEDULED_JOB") < kpi["evidence"].index("BI_TOOL_JDBC") \
        < kpi["evidence"].index("SHARED_USERS")
    assert kpi["estimated_monthly_savings"] <= kpi["estimated_monthly_cost"]


def test_multi_type_run_leaves_other_clusters_unchanged(multi_type, spark):
    _, rows = multi_type
    base = _rows(spark)
    for cid in ("0101-shared-ds-cluster", "0202-bi-tableau-cluster"):
        assert list(rows[cid]["misuse_types"]) == list(base[cid]["misuse_types"])
        assert rows[cid]["estimated_monthly_savings"] == base[cid]["estimated_monthly_savings"]


def test_missing_audit_skips_bi_and_shared_users_but_keeps_scheduled_jobs(spark):
    source = _source_copy(spark, "f4_no_audit_src", drop=("access_audit",))
    message = _run_step(spark, source, "f4_no_audit")
    assert {cid: list(r["misuse_types"]) for cid, r in _rows(spark, "f4_no_audit").items()} == {
        KPI: ["SCHEDULED_JOB"]}
    assert "access.audit unavailable" in message


def test_missing_clusters_uses_billing_skus_and_still_finds_all_three(spark):
    source = _source_copy(spark, "f4_no_clusters_src", drop=("compute_clusters",))
    message = _run_step(spark, source, "f4_no_clusters")
    rows = _rows(spark, "f4_no_clusters")
    assert {cid: list(r["misuse_types"]) for cid, r in rows.items()} == EXPECTED
    assert all(r["cluster_source"] == "UNKNOWN" for r in rows.values())
    assert "compute.clusters unavailable" in message


def test_missing_task_timeline_skips_scheduled_job_with_a_note(spark):
    source = _source_copy(spark, "f4_no_tasks_src", drop=("lakeflow_job_task_run_timeline",))
    message = _run_step(spark, source, "f4_no_tasks")
    assert {cid: list(r["misuse_types"]) for cid, r in _rows(spark, "f4_no_tasks").items()} == {
        "0101-shared-ds-cluster": ["SHARED_USERS"], "0202-bi-tableau-cluster": ["BI_TOOL_JDBC"]}
    assert "SCHEDULED_JOB skipped" in message


def test_missing_lakeflow_and_query_history_still_build(spark):
    source = _source_copy(spark, "f4_no_jobs_src", drop=(
        "lakeflow_job_run_timeline", "lakeflow_job_task_run_timeline", "lakeflow_jobs", "query_history"))
    message = _run_step(spark, source, "f4_no_jobs")
    assert {cid: list(r["misuse_types"]) for cid, r in _rows(spark, "f4_no_jobs").items()} == {
        "0101-shared-ds-cluster": ["SHARED_USERS"], "0202-bi-tableau-cluster": ["BI_TOOL_JDBC"]}
    assert "SCHEDULED_JOB skipped" in message and "query.history unavailable" in message


# --- crafted scenarios: thresholds at n-1 / n, automation rules, exact window arithmetic ----

TABLEAU = "SimbaSparkJDBCDriver/2.6.36 (Tableau 2024.2)"
DBUS = 2.0           # each crafted cluster bills one 2-hour row of 2 DBUs at 0.55: 1.10, so 0.55 per hour
ONE_HOUR = Decimal("0.55")
JOBS_RATIO = Decimal("0.15") / Decimal("0.55")


def _crafted_rows():
    import random

    rng = random.Random(11)
    rows = build_synthetic_rows()
    day = date.today() - timedelta(days=1)
    at = lambda h, m=0: datetime.combine(day, datetime.min.time()) + timedelta(hours=h, minutes=m)  # noqa: E731

    def cluster(cid):
        w = {"kind": "CLUSTER", "workspace_id": PROD_WS, "cluster": cid, "tags": {}, "run_as_pool": ["x@example.com"],
             "sku": "PREMIUM_ALL_PURPOSE_COMPUTE", "product": "ALL_PURPOSE"}
        rows["compute_clusters"].append(_cluster_row(w, 30, datetime(2025, 6, 1)))
        rows["billing_usage"].append(_usage_row(rng, w, day, at(10), at(12), DBUS, None, cluster_id=cid))

    def job(job_id, name, trigger):
        rows["lakeflow_jobs"].append({
            "account_id": ACCOUNT_ID, "workspace_id": PROD_WS, "job_id": job_id, "name": name, "description": None,
            "creator_id": "1", "tags": {}, "change_time": datetime(2025, 6, 1), "delete_time": None,
            "run_as": None, "trigger_type": trigger, "paused": False})

    def run(cid, job_id, run_id, start, end, trigger, run_type="JOB_RUN", name=None):
        rows["lakeflow_job_run_timeline"].extend(_timeline_rows(
            {"account_id": ACCOUNT_ID, "workspace_id": PROD_WS, "job_id": job_id, "run_id": run_id,
             "trigger_type": trigger, "run_type": run_type, "run_name": name or f"run {run_id}",
             "compute_ids": [cid]}, start, end, "SUCCEEDED", "SUCCESS"))

    def bi(cid, n):
        for i in range(n):
            rows["access_audit"].append(_audit_row(rng, day, at(10, 5 + 10 * i), "svc-tableau@example.com", TABLEAU,
                                                   "databrickssql", "commandSubmit", {"clusterId": cid}))

    for cid in ("f4-bi-5", "f4-bi-4", "f4-jobs-overlap", "f4-one-run", "f4-two-runs", "f4-submit", "f4-manual",
                "f4-retry", "f4-jobs-interactive"):
        cluster(cid)
    bi("f4-bi-5", 5)                       # min_bi_events = 5: flagged
    bi("f4-bi-4", 4)                       # n - 1: not flagged
    job("3001", "overlap_job", "CRON")     # overlapping + adjacent runs, union = 10:00-11:00
    run("f4-jobs-overlap", "3001", "30011", at(10), at(10, 40), "CRON")
    run("f4-jobs-overlap", "3001", "30012", at(10, 20), at(10, 50), "CRON")
    run("f4-jobs-overlap", "3001", "30013", at(10, 50), at(11), "CRON")
    job("3002", "one_run_job", "CRON")     # min_scheduled_runs = 2: one run is n - 1
    run("f4-one-run", "3002", "30021", at(10), at(11), "CRON")
    job("3003", "two_run_job", "CRON")
    run("f4-two-runs", "3003", "30031", at(10), at(10, 30), "CRON")
    run("f4-two-runs", "3003", "30032", at(10, 30), at(11), "CRON")
    # External orchestrator: SUBMIT_RUN with trigger ONETIME and no job_id.
    run("f4-submit", None, "30041", at(10), at(10, 30), "ONETIME", "SUBMIT_RUN", "adf_pipeline_load")
    run("f4-submit", None, "30042", at(10, 30), at(11), "ONETIME", "SUBMIT_RUN", "adf_pipeline_load")
    # Manual UI runs and retries of a manual job: never automated.
    job("3005", "manual_job", None)
    run("f4-manual", "3005", "30051", at(10), at(10, 20), "ONETIME")
    run("f4-manual", "3005", "30052", at(10, 20), at(10, 40), "ONETIME_RETRY")
    run("f4-manual", "3005", "30053", at(10, 40), at(11), "ONETIME_RETRY")
    # A retry of a CRON job is automated: 1 CRON run + 1 retry = 2 runs, flagged.
    job("3006", "retry_job", "CRON")
    run("f4-retry", "3006", "30061", at(10), at(10, 30), "CRON")
    run("f4-retry", "3006", "30062", at(10, 30), at(11), "ONETIME_RETRY")
    # Two hours of CRON runs, one notebook attach at 11:15: the 11:00 hour is interactive.
    job("3007", "interactive_job", "CRON")
    run("f4-jobs-interactive", "3007", "30071", at(10), at(11), "CRON")
    run("f4-jobs-interactive", "3007", "30072", at(11), at(12), "CRON")
    rows["access_audit"].append(_audit_row(rng, day, at(11, 15), "amy@example.com", BROWSER_AGENT, "notebook",
                                           "attachNotebook", {"notebookId": "nb-9", "clusterId": "f4-jobs-interactive"}))
    return rows


@pytest.fixture(scope="module")
def crafted(spark):
    rows = _crafted_rows()
    tables = ("billing_usage", "compute_clusters", "lakeflow_jobs", "lakeflow_job_run_timeline", "access_audit")
    source = _source_copy(spark, "f4_crafted_src", replace={t: rows[t] for t in tables})
    message = _run_step(spark, source, "f4_crafted")
    return message, _rows(spark, "f4_crafted")


def test_bi_threshold_n_minus_one_and_n(crafted):
    _, rows = crafted
    assert "f4-bi-4" not in rows
    assert list(rows["f4-bi-5"]["misuse_types"]) == ["BI_TOOL_JDBC"]
    assert rows["f4-bi-5"]["bi_event_count"] == 5


def test_scheduled_threshold_n_minus_one_and_n(crafted):
    _, rows = crafted
    assert "f4-one-run" not in rows
    assert rows["f4-two-runs"]["scheduled_run_count"] == 2


def test_five_bi_requests_in_one_hour_attribute_exactly_one_hour(crafted):
    bi = crafted[1]["f4-bi-5"]
    assert bi["cluster_cost"] == 2 * ONE_HOUR
    assert abs(bi["bi_tool_attributable_cost"] - ONE_HOUR) < Decimal("0.000001")
    assert abs(bi["estimated_monthly_savings"] - ONE_HOUR * Decimal("0.3")) < Decimal("0.000001")


def test_overlapping_and_adjacent_runs_attribute_exactly_one_hour(crafted):
    jobs = crafted[1]["f4-jobs-overlap"]
    assert jobs["scheduled_run_count"] == 3
    assert abs(jobs["scheduled_job_attributable_cost"] - ONE_HOUR) < Decimal("0.000001")
    assert abs(jobs["estimated_monthly_savings"] - ONE_HOUR * (1 - JOBS_RATIO)) < Decimal("0.000001")


def test_interactive_hour_is_subtracted_from_scheduled_job_time(crafted):
    row = crafted[1]["f4-jobs-interactive"]
    assert list(row["misuse_types"]) == ["SCHEDULED_JOB"]        # one user is below min_interactive_users
    assert abs(row["scheduled_job_attributable_cost"] - ONE_HOUR) < Decimal("0.000001")   # 2 h run - 1 h interactive
    assert "DBUs assumed unchanged" in row["savings_basis"]


def test_submit_runs_from_orchestrators_count_and_manual_runs_do_not(crafted):
    _, rows = crafted
    submit = rows["f4-submit"]
    assert list(submit["misuse_types"]) == ["SCHEDULED_JOB"] and submit["scheduled_run_count"] == 2
    assert "adf_pipeline_load (SUBMIT_RUN)" in submit["evidence"]
    assert "f4-manual" not in rows


def test_retry_counts_only_for_an_automated_job(crafted):
    _, rows = crafted
    assert rows["f4-retry"]["scheduled_run_count"] == 2
    assert "retry_job (ONETIME_RETRY)" in rows["f4-retry"]["evidence"]


def test_crafted_run_keeps_the_sandbox_results(crafted, spark):
    _, rows = crafted
    base = _rows(spark)
    for cid, types in EXPECTED.items():
        assert list(rows[cid]["misuse_types"]) == types
        assert rows[cid]["estimated_monthly_savings"] == base[cid]["estimated_monthly_savings"]


def test_hour_buckets_are_utc_whatever_the_session_time_zone(spark):
    expr = ("SELECT unix_seconds(timestamp_seconds(FLOOR(unix_seconds(t) / 3600) * 3600)) % 3600 AS ours, "
            "unix_seconds(date_trunc('HOUR', t)) % 3600 AS session FROM (SELECT TIMESTAMP'2026-10-01 10:45:00Z' AS t)")
    spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")
    try:
        row = spark.sql(expr).collect()[0]
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
    assert row["ours"] == 0
    assert row["session"] == 1800   # +05:30: a session-zone date_trunc would cut at :30 UTC
