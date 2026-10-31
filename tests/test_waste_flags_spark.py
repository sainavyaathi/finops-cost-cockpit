"""F7 waste_flags and the shared detection window against the synthetic sandbox (local Spark + Delta).

Skipped without pyspark/delta (CI). Runs the full upstream + cockpit pipeline twice on the same
synthetic data (45 days): lookback 30 (the default profile) and lookback 45 (a long trend profile,
detection still 30). Then reruns only the F7 step against crafted copies of its sources for the
R1 overlap and missing-source cases.
"""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "sandbox"))

from finops_sandbox import generate_synthetic_system_tables, run_accelerator  # noqa: E402

from cockpit.pipeline import StepContext  # noqa: E402
from cockpit.steps import waste_flags as f7  # noqa: E402
from databricks_finops.config import config_from_args, parse_args  # noqa: E402

SOURCE = "spark_catalog.f7_synthetic"
CATALOG = "spark_catalog"
SCHEMA = "f7_cockpit"
LONG_SCHEMA = "f7_cockpit_long"
LONG_LOOKBACK = 45
SOURCE_TABLES = ("cost_base", "allpurpose_misuse", "idle_compute", "job_reliability", "optimization_candidates")


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()
    warehouse = tmp_path_factory.mktemp("f7_warehouse").as_posix()
    builder = (
        SparkSession.builder.appName("f7-tests").master("local[2]")
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
    generate_synthetic_system_tables(session, SOURCE, days=45)
    _, session.f7_logger = run_accelerator(session, SOURCE, CATALOG, SCHEMA, str(ROOT))
    _, session.f7_long_logger = run_accelerator(session, SOURCE, CATALOG, LONG_SCHEMA, str(ROOT),
                                                lookback_days=LONG_LOOKBACK)
    yield session
    session.stop()


def _flags(spark, schema=SCHEMA):
    return {(r["resource_type"], r["resource_id"]): r
            for r in spark.table(f"{CATALOG}.{schema}.waste_flags").collect()}


def _findings(row) -> list[dict]:
    return json.loads(row["findings_json"])


def _run_f7(spark, schema: str) -> str:
    config = config_from_args(parse_args(["--catalog", CATALOG, "--schema", schema, "--bundle-root", str(ROOT)]))
    return f7.STEP.run(StepContext(spark=spark, config=config, run_id="f7-test", preflight=None))


def _crafted_schema(spark, name: str, drop=(), replace=None) -> str:
    """A schema of views over the main run's F7 sources, minus ``drop``, with ``replace`` DataFrames."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{name}")
    for table in SOURCE_TABLES + ("waste_flags",):
        spark.sql(f"DROP VIEW IF EXISTS {CATALOG}.{name}.{table}")
        spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{name}.{table}")
    for table in SOURCE_TABLES:
        if table in drop:
            continue
        if replace and table in replace:
            replace[table].write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{name}.{table}")
        else:
            spark.sql(f"CREATE VIEW {CATALOG}.{name}.{table} AS SELECT * FROM {CATALOG}.{SCHEMA}.{table}")
    return name


# --- the sandbox as generated -----------------------------------------------------------

def test_step_succeeds_after_its_sources(spark):
    names = [r.task_name for r in spark.f7_logger.records]
    record = next(r for r in spark.f7_logger.records if r.task_name == "build_waste_flags")
    assert record.result == "SUCCESS", record.message
    assert record.rows_written == len(_flags(spark))
    for dep in f7.STEP.depends_on:
        assert names.index(dep) < names.index("build_waste_flags")


def test_one_row_per_resource_and_contract_columns(spark):
    df = spark.table(f"{CATALOG}.{SCHEMA}.waste_flags")
    assert df.count() == df.select("resource_type", "workspace_id", "resource_id").distinct().count()
    expected = ["resource_type", "workspace_id", "resource_id", "resource_name", "owner", "team",
                "primary_flag_type", "flag_types", "findings", "findings_json", "evidence", "suggested_action",
                "estimated_monthly_cost", "estimated_monthly_savings", "confidence", "priority_score",
                "priority_rank", "sources", "primary_source", "flag_type_count", "detection_days",
                "currency_code", "created_at", "run_id"]
    assert df.columns == expected
    types = dict(df.dtypes)
    assert types["estimated_monthly_savings"] == "decimal(38,6)"
    assert types["estimated_monthly_cost"] == "decimal(38,6)"
    assert types["flag_types"] == "array<string>" and types["sources"] == "array<string>"
    assert types["findings"].startswith("array<struct<source:string,flag_type:string,savings:decimal(38,6)")


def test_savings_capped_scores_bounded_and_ranks_unique(spark):
    rows = list(_flags(spark).values())
    for row in rows:
        assert 0 <= row["estimated_monthly_savings"] <= row["estimated_monthly_cost"], row["resource_id"]
        assert 0 <= row["priority_score"] <= 100
        assert row["detection_days"] == 30
    assert sorted(r["priority_rank"] for r in rows) == list(range(1, len(rows) + 1))
    by_rank = sorted(rows, key=lambda r: r["priority_rank"])
    scores = [r["priority_score"] for r in by_rank]
    assert scores == sorted(scores, reverse=True)


def _check_r1(rows):
    for row in rows:
        found = [Decimal(str(f["savings"])) for f in _findings(row) if f["savings"] is not None]
        expected = min(max(found) if found else Decimal(0), row["estimated_monthly_cost"])
        if "[waste_flags]" in row["evidence"]:  # JOB saving capped by the cost it shares with a flagged cluster
            assert row["estimated_monthly_savings"] < expected
        else:
            assert abs(row["estimated_monthly_savings"] - expected) < Decimal("0.000002"), row["resource_id"]


def test_savings_equal_the_largest_finding_r1(spark):
    _check_r1(_flags(spark).values())


def test_cockpit_sources_present_with_their_savings(spark):
    flags = _flags(spark)
    misuse = {r["cluster_id"]: r for r in spark.table(f"{CATALOG}.{SCHEMA}.allpurpose_misuse").collect()}
    idle = {r["cluster_id"]: r for r in spark.table(f"{CATALOG}.{SCHEMA}.idle_compute").collect()}
    for cid, src in list(misuse.items()) + list(idle.items()):
        assert ("CLUSTER", cid) in flags, cid
    for cid, src in idle.items():
        row = flags[("CLUSTER", cid)]
        assert row["primary_flag_type"] == src["idle_reason"]
        assert row["estimated_monthly_savings"] >= (src["estimated_monthly_savings"] or 0)
    kpi = flags[("CLUSTER", "0303-kpi-allpurpose-cluster")]
    assert kpi["primary_flag_type"] == "SCHEDULED_JOB" and kpi["confidence"] == "HIGH"
    assert kpi["estimated_monthly_savings"] == misuse["0303-kpi-allpurpose-cluster"]["estimated_monthly_savings"]


def test_f6_failure_and_retry_heavy_jobs_are_flagged(spark):
    flags = _flags(spark)
    reliability = {r["job_name"]: r for r in spark.table(f"{CATALOG}.{SCHEMA}.job_reliability").collect()}

    flaky = flags[("JOB", reliability["flaky_vendor_ingest"]["job_id"])]
    assert flaky["primary_flag_type"] == "FAILURE_HEAVY" and flaky["sources"] == ["job_reliability"]
    assert flaky["estimated_monthly_savings"] == reliability["flaky_vendor_ingest"]["estimated_failed_cost"]
    assert flaky["suggested_action"] == "INVESTIGATE_FAILURES"

    finance = flags[("JOB", reliability["finance_reconcile"]["job_id"])]
    assert finance["primary_flag_type"] == "RETRY_HEAVY"
    assert finance["confidence"] == "MEDIUM"  # retry cost is an estimate, capped at MEDIUM
    assert 0 < finance["estimated_monthly_savings"] < finance["estimated_monthly_cost"]
    assert "task retries" in finance["evidence"]

    healthy = reliability["nightly_sales_etl"]["job_id"]
    assert ("JOB", healthy) not in flags  # HEALTHY_EXPENSIVE / LOOKS_HEALTHY upstream item is not taken


def test_upstream_items_are_deduplicated_and_mapped(spark):
    flags = _flags(spark)
    cockpit_clusters = {rid for (rtype, rid), r in flags.items()
                        if rtype == "CLUSTER" and set(r["sources"]) & {"allpurpose_misuse", "idle_compute"}}
    for (rtype, rid), row in flags.items():
        for finding in _findings(row):
            if finding["source"] != "optimization_candidates":
                continue
            assert finding["flag_type"] not in ("RETRY_HEAVY", "HIGH_FAILURE_COST", "HIGH_FAILURE_RATE")
            if finding["flag_type"] == "SHARED_CLUSTER_ATTRIBUTION":
                assert rid not in cockpit_clusters
            assert finding["savings"] is None
    assert ("WAREHOUSE", "wh-bi-001") in flags  # SQL_WAREHOUSE -> WAREHOUSE
    assert flags[("JOB", "1002")]["primary_flag_type"] == "HIGH_COST_LOW_UTILIZATION"
    # A cluster F5 flags: upstream's generic attribution item is superseded there.
    assert "SHARED_CLUSTER_ATTRIBUTION" not in flags[("CLUSTER", "0404-zombie-autoterm-off")]["flag_types"]


# --- detection window: a long upstream lookback does not dilute the flags ----------------

def _key_rows(spark, schema, table, key, columns):
    return {r[key]: tuple(r[c] for c in columns) for r in spark.table(f"{CATALOG}.{schema}.{table}").collect()}


def test_long_lookback_steps_succeed(spark):
    for record in spark.f7_long_logger.records:
        if record.task_name.startswith("build_"):
            assert record.result == "SUCCESS", (record.task_name, record.message)
    record = next(r for r in spark.f7_long_logger.records if r.task_name == "build_job_reliability")
    assert "upstream summary re-rendered" in record.message


@pytest.mark.parametrize("table, key, columns", [
    ("allpurpose_misuse", "cluster_id", ("misuse_types", "cluster_cost", "estimated_monthly_cost",
                                         "estimated_monthly_savings", "scheduled_run_count", "detection_days")),
    ("idle_compute", "cluster_id", ("idle_reason", "estimated_cost", "idle_hours", "estimated_monthly_savings",
                                    "detection_days")),
    ("job_reliability", "job_id", ("run_count", "failed_count", "retry_count", "estimated_failed_cost",
                                   "reliability_category", "retry_task_minutes", "detection_days")),
])
def test_cockpit_flags_with_lookback_45_match_lookback_30(spark, table, key, columns):
    assert _key_rows(spark, LONG_SCHEMA, table, key, columns) == _key_rows(spark, SCHEMA, table, key, columns)


def test_upstream_reflects_the_long_lookback_but_job_reliability_does_not(spark):
    upstream = _key_rows(spark, LONG_SCHEMA, "job_reliability_summary", "job_id", ("run_count",))
    short = _key_rows(spark, SCHEMA, "job_reliability_summary", "job_id", ("run_count",))
    assert sum(v[0] for v in upstream.values()) > sum(v[0] for v in short.values())  # 45 days > 30 days
    lookbacks = {r["lookback_days"] for r in spark.table(f"{CATALOG}.{LONG_SCHEMA}.allpurpose_misuse").collect()}
    assert lookbacks == {LONG_LOOKBACK}


def test_waste_flags_from_cockpit_sources_match_across_profiles(spark):
    def cockpit_rows(schema):
        return {k: (r["primary_flag_type"], r["estimated_monthly_cost"], r["estimated_monthly_savings"])
                for k, r in _flags(spark, schema).items() if r["primary_source"] != "optimization_candidates"}
    assert cockpit_rows(LONG_SCHEMA) == cockpit_rows(SCHEMA)


# --- crafted sources: R1 overlap and missing sources -----------------------------------

def test_cluster_flagged_by_f4_and_f5_keeps_the_max_saving_not_the_sum(spark):
    from pyspark.sql import functions as F

    misuse = spark.table(f"{CATALOG}.{SCHEMA}.allpurpose_misuse")
    template = misuse.filter(F.col("cluster_id") == "0202-bi-tableau-cluster")
    extra = (template
             .withColumn("cluster_id", F.lit("0404-zombie-autoterm-off"))
             .withColumn("cluster_name", F.lit("0404-zombie-autoterm-off"))
             .withColumn("misuse_types", F.array(F.lit("BI_TOOL_JDBC"), F.lit("SHARED_USERS")))
             .withColumn("estimated_monthly_savings", F.lit(Decimal("100")).cast("decimal(38,6)")))
    huge = (template
            .withColumn("cluster_id", F.lit("0707-autoterm-61-cluster"))
            .withColumn("cluster_name", F.lit("0707-autoterm-61-cluster"))
            .withColumn("estimated_monthly_savings", F.lit(Decimal("100000")).cast("decimal(38,6)")))
    schema = _crafted_schema(spark, "f7_overlap", replace={"allpurpose_misuse": misuse.unionByName(extra)
                                                           .unionByName(huge)})
    message = _run_f7(spark, schema)
    assert message.startswith("waste_flags:")

    flags = _flags(spark, schema)
    idle = {r["cluster_id"]: r for r in spark.table(f"{CATALOG}.{SCHEMA}.idle_compute").collect()}
    zombie = flags[("CLUSTER", "0404-zombie-autoterm-off")]
    f5_saving = idle["0404-zombie-autoterm-off"]["estimated_monthly_savings"]
    assert zombie["estimated_monthly_savings"] == min(f5_saving, zombie["estimated_monthly_cost"])  # max, not 100 + F5
    assert zombie["sources"] == ["idle_compute", "allpurpose_misuse"]
    assert zombie["primary_source"] == "idle_compute"
    assert set(zombie["flag_types"]) == {"AUTO_TERMINATION_OFF+LOW_UTILIZATION", "BI_TOOL_JDBC", "SHARED_USERS"}
    assert zombie["flag_type_count"] == 3
    findings = {(f["source"], f["flag_type"]): f for f in _findings(zombie)}
    assert findings[("allpurpose_misuse", "BI_TOOL_JDBC")]["savings"] == 100
    assert findings[("allpurpose_misuse", "SHARED_USERS")]["savings"] is None
    assert "[idle_compute]" in zombie["evidence"] and "[allpurpose_misuse]" in zombie["evidence"]
    # One row per resource still holds, and an F4 claim above the cluster cost is capped (R1).
    assert sum(1 for k in flags if k == ("CLUSTER", "0404-zombie-autoterm-off")) == 1
    capped = flags[("CLUSTER", "0707-autoterm-61-cluster")]
    assert capped["estimated_monthly_savings"] == capped["estimated_monthly_cost"]
    assert all(f["savings"] is None or Decimal(str(f["savings"])) <= capped["estimated_monthly_cost"]
               for f in _findings(capped))
    # Breadth raises the score of the overlapping cluster above its single-source score.
    assert zombie["priority_score"] > _flags(spark)[("CLUSTER", "0404-zombie-autoterm-off")]["priority_score"]


def test_missing_job_reliability_is_skipped_and_upstream_failures_return(spark):
    schema = _crafted_schema(spark, "f7_no_f6", drop=("job_reliability",))
    message = _run_f7(spark, schema)
    assert "job_reliability unavailable: job_reliability skipped" in message
    flags = _flags(spark, schema)
    assert all("job_reliability" not in r["sources"] for r in flags.values())
    flaky = flags[("JOB", "1004")]
    assert flaky["primary_flag_type"] == "HIGH_FAILURE_COST" and flaky["sources"] == ["optimization_candidates"]


def test_without_any_source_the_table_is_created_empty(spark):
    schema = _crafted_schema(spark, "f7_empty", drop=SOURCE_TABLES)
    message = _run_f7(spark, schema)
    assert message.startswith("waste_flags: 0 resource(s)")
    assert spark.table(f"{CATALOG}.{schema}.waste_flags").count() == 0


def _run_f7_with(spark, schema: str, thresholds: dict) -> None:
    """Run F7 with settings that bypass validation (to exercise the score clamp)."""
    config = config_from_args(parse_args(["--catalog", CATALOG, "--schema", schema, "--bundle-root", str(ROOT)]))
    ctx = StepContext(spark=spark, config=config, run_id="f7-test", preflight=None)
    columns = {name: f7.probe_columns(spark, ctx.table(table)) for name, (table, _, _) in f7.SOURCES.items()}
    columns["cost_base"] = f7.probe_columns(spark, ctx.table("cost_base"))
    statement, _, _ = f7.build_statement(ctx, columns, thresholds)
    spark.sql(statement)


# --- review fixes: unknown cost, JOB/CLUSTER overlap, thresholds, score clamp -------------

def test_without_cost_base_f6_jobs_keep_their_saving_and_rank(spark):
    schema = _crafted_schema(spark, "f7_no_cost_base", drop=("cost_base",))
    message = _run_f7(spark, schema)
    assert "cost_base unavailable" in message
    flags = _flags(spark, schema)
    reliability = {r["job_id"]: r for r in spark.table(f"{CATALOG}.{SCHEMA}.job_reliability").collect()}
    flaky = flags[("JOB", "1004")]
    assert flaky["primary_flag_type"] == "FAILURE_HEAVY"
    assert flaky["estimated_monthly_savings"] == reliability["1004"]["estimated_failed_cost"] > 0
    assert flaky["estimated_monthly_cost"] == flaky["estimated_monthly_savings"]  # failed cost is the floor
    assert flaky["priority_score"] > 0
    ranks = sorted(r["priority_rank"] for r in flags.values())
    assert ranks == list(range(1, len(flags) + 1))
    _check_r1(flags.values())


def test_job_on_a_flagged_cluster_leaves_the_shared_cost_to_the_cluster(spark):
    from pyspark.sql import functions as F

    base = spark.table(f"{CATALOG}.{SCHEMA}.cost_base")
    on_zombie = (F.col("workload_type") == "JOB") & (F.col("workload_id") == "1004")
    moved = base.withColumn("cluster_id", F.when(on_zombie, F.lit("0404-zombie-autoterm-off"))
                            .otherwise(F.col("cluster_id")))
    schema = _crafted_schema(spark, "f7_job_on_cluster", replace={"cost_base": moved})
    _run_f7(spark, schema)
    flags = _flags(spark, schema)
    original = _flags(spark)

    job = flags[("JOB", "1004")]
    assert job["estimated_monthly_cost"] == original[("JOB", "1004")]["estimated_monthly_cost"]  # exclusive key
    assert job["estimated_monthly_savings"] == 0  # all its DBUs sit on the flagged cluster
    assert _findings(job)[0]["savings"] > 0  # the F6 finding still shows its own claim
    assert "left to the cluster" in job["evidence"]

    cluster = flags[("CLUSTER", "0404-zombie-autoterm-off")]
    extra = cluster["estimated_monthly_cost"] - original[("CLUSTER", "0404-zombie-autoterm-off")]["estimated_monthly_cost"]
    assert abs(extra - job["estimated_monthly_cost"]) < Decimal("0.00001")  # CLUSTER keeps every cluster_id row
    assert cluster["estimated_monthly_savings"] > 0
    _check_r1(flags.values())


BOUNDARY_JOBS = {
    # job_id: (failure_rate_pct, estimated_failed_cost over 30 days, retry_count, expected flag or None)
    "b-rate-eq": (20.0, "0", 0, "FAILURE_HEAVY"),
    "b-rate-below": (19.99, "0", 0, None),
    "b-cost-eq": (0.0, "10", 0, "HIGH_FAILURE_COST"),
    "b-cost-below": (0.0, "9.99", 0, None),
    "b-retry-eq": (0.0, "0", 5, "RETRY_HEAVY"),
    "b-retry-below": (0.0, "0", 4, None),
}


def test_f6_thresholds_are_inclusive_boundaries(spark):
    from pyspark.sql import functions as F

    reliability = spark.table(f"{CATALOG}.{SCHEMA}.job_reliability")
    template = reliability.filter(F.col("job_name") == "nightly_sales_etl")
    rows = reliability
    for job_id, (rate, failed, retries, _) in BOUNDARY_JOBS.items():
        rows = rows.unionByName(template
                                .withColumn("job_id", F.lit(job_id)).withColumn("job_name", F.lit(job_id))
                                .withColumn("failure_rate_pct", F.lit(rate).cast("double"))
                                .withColumn("estimated_failed_cost", F.lit(Decimal(failed)).cast("decimal(38,6)"))
                                .withColumn("retry_count", F.lit(retries).cast("bigint")))
    schema = _crafted_schema(spark, "f7_boundaries", replace={"job_reliability": rows})
    _run_f7(spark, schema)
    flags = _flags(spark, schema)
    for job_id, (_, _, _, expected) in BOUNDARY_JOBS.items():
        if expected is None:
            assert ("JOB", job_id) not in flags, job_id
        else:
            assert flags[("JOB", job_id)]["primary_flag_type"] == expected, job_id
    assert flags[("JOB", "b-cost-eq")]["estimated_monthly_savings"] == 10


def test_priority_reaches_100_at_most_and_the_clamp_holds(spark):
    from pyspark.sql import functions as F

    misuse = spark.table(f"{CATALOG}.{SCHEMA}.allpurpose_misuse")
    shared = "0101-shared-ds-cluster"  # monthly cost > 1000 in the sandbox
    boosted = misuse.withColumn("misuse_types", F.when(F.col("cluster_id") == shared, F.array(
        F.lit("SCHEDULED_JOB"), F.lit("BI_TOOL_JDBC"), F.lit("SHARED_USERS"))).otherwise(F.col("misuse_types"))) \
        .withColumn("misuse_type", F.when(F.col("cluster_id") == shared, F.lit("SCHEDULED_JOB"))
                    .otherwise(F.col("misuse_type"))) \
        .withColumn("confidence", F.when(F.col("cluster_id") == shared, F.lit("HIGH")).otherwise(F.col("confidence"))) \
        .withColumn("estimated_monthly_savings", F.when(F.col("cluster_id") == shared,
                    F.lit(Decimal("5000")).cast("decimal(38,6)")).otherwise(F.col("estimated_monthly_savings")))
    schema = _crafted_schema(spark, "f7_max_score", replace={"allpurpose_misuse": boosted})
    _run_f7(spark, schema)
    top = _flags(spark, schema)[("CLUSTER", shared)]
    # S = C = B = 100 and HIGH: the validated formula tops out at exactly 100.
    assert top["priority_score"] == 100.0 and top["priority_rank"] == 1
    assert top["estimated_monthly_savings"] == top["estimated_monthly_cost"]  # 5000 capped at the cost

    # Unvalidated weights (sum 3) would give 300 before the clamp; the table still holds 0-100.
    weights = dict(f7.DEFAULTS, priority_weight_savings=1.0, priority_weight_cost=1.0, priority_weight_breadth=1.0)
    _run_f7_with(spark, schema, weights)
    scores = [r["priority_score"] for r in _flags(spark, schema).values()]
    assert max(scores) == 100.0 and min(scores) >= 0.0
