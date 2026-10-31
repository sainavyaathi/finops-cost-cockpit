"""F5: cockpit.idle_compute flags idle all-purpose clusters with zombie auto-termination (one row each)."""

from __future__ import annotations

import inspect
import re
import shutil
import sys
from datetime import datetime, timedelta, timezone
from importlib import resources
from pathlib import Path

import pytest

from cockpit.pipeline import StepContext, discover_steps
from cockpit.steps import idle_compute as step_module
from cockpit.step_config import load_step_thresholds
from cockpit.steps.idle_compute import (
    DEFAULTS,
    SECTION,
    IdleComputeThresholds,
    load_idle_compute_thresholds,
    CLUSTERS,
    MODE_CONFIG_ONLY,
    MODE_FULL,
    MODE_NONE,
    MODE_UTILIZATION_ONLY,
    NODE_TIMELINE,
    REQUIRED_CLUSTER_COLUMNS,
    STEP,
    build_idle_compute_sql,
    probe_table,
)
from databricks_finops.config import AppConfig

ROOT = Path(__file__).resolve().parents[1]
CLUSTER_COLUMNS = set(REQUIRED_CLUSTER_COLUMNS) | {"cluster_name", "owned_by"}
NODE_COLUMNS = {"workspace_id", "cluster_id", "start_time", "cpu_user_percent", "cpu_system_percent",
                "mem_used_percent"}


def _config(lookback_days: int = 30, bundle_root: str = str(ROOT), schema: str = "cockpit") -> AppConfig:
    return AppConfig(catalog="spark_catalog", schema=schema, lookback_days=lookback_days,
                     currency_code="USD", display_currency="USD", fallback_dbu_price=0.55,
                     bundle_root=bundle_root)


def _ctx(spark=None, **kwargs) -> StepContext:
    return StepContext(spark=spark, config=_config(**kwargs), run_id="run-1", preflight=None)


def _sql(cluster_problem=None, node_problem=None, **kwargs):
    return build_idle_compute_sql(_ctx(**kwargs), CLUSTER_COLUMNS, cluster_problem, NODE_COLUMNS, node_problem)


# ---------------------------------------------------------------------------
# SQL generation and helpers (no Spark needed; runs in CI)
# ---------------------------------------------------------------------------

def test_step_is_discovered_and_writes_idle_compute():
    assert "build_idle_compute" in [s.name for s in discover_steps()]
    assert STEP.output_table == "idle_compute"
    assert STEP.required


def test_full_sql_uses_latest_all_purpose_state_and_thresholds():
    sql, mode = _sql()
    assert mode == MODE_FULL
    assert "CREATE OR REPLACE TABLE `spark_catalog`.`cockpit`.`idle_compute`" in sql
    assert "FROM `spark_catalog`.`cockpit`.`daily_cost`" in sql and "WHERE run_id = 'run-1'" in sql
    assert f"FROM {CLUSTERS}" in sql and f"FROM {NODE_TIMELINE}" in sql
    assert "ORDER BY change_time DESC NULLS LAST, delete_time DESC NULLS LAST" in sql
    assert "delete_time IS NULL" in sql and "cluster_source IN ('UI', 'API')" in sql
    assert "s.autotermination_minutes = 0 THEN 'AUTO_TERMINATION_OFF'" in sql
    assert "s.autotermination_minutes > 60 THEN 'AUTO_TERMINATION_TOO_LONG'" in sql  # strictly over 60
    assert "hour_cpu_pct < 20 AND COALESCE(hour_memory_pct, 0) < 40" in sql  # upstream thresholds.yml
    assert ">= 50" in sql  # idle_hours_share_pct
    assert "CAST(cpu_user_percent AS DOUBLE) + CAST(cpu_system_percent AS DOUBLE)" in sql
    assert "TRUE AS config_known" in sql and "TRUE AS utilization_known" in sql
    # Same window as daily_cost (usage_date >= current_date - lookback), not a rolling 30 x 24 hours.
    assert "start_time >= CAST(current_date() - INTERVAL 30 DAYS AS TIMESTAMP)" in sql
    assert "COUNT(DISTINCT date_trunc('MINUTE', start_time))" in sql  # hours weighted by minutes sampled
    assert "IN ('ALL_PURPOSE', 'INTERACTIVE')" in sql  # upstream is_shared_cluster predicate
    assert "NOT TRUE AND estimated_cost > 0" in sql  # current rule: config flag requires idleness
    assert "DECIMAL(38,6)" in sql
    assert "{" not in sql and "}" not in sql


def test_no_hard_coded_catalog_or_schema():
    sql, _ = _sql()
    for name in ("workspace.", "finops.", "`finops`", "finops_accelerator"):
        assert name not in sql
    assert "spark_catalog" not in re.sub(r"`spark_catalog`\.`cockpit`", "", sql)


def test_monthly_multiplier_follows_lookback():
    sql, _ = _sql(lookback_days=15)
    assert "estimated_cost * 2 AS DECIMAL(38,6)) AS estimated_monthly_cost" in sql
    assert "over 15 days" in sql


def test_without_clusters_table_population_comes_from_billing():
    sql, mode = _sql(cluster_problem=f"{CLUSTERS} is empty")
    assert mode == MODE_UTILIZATION_ONLY
    assert f"FROM {CLUSTERS}" not in sql
    assert "FALSE AS config_known" in sql
    assert "billed_all_purpose = 1" in sql and "billed_to_job = 0" in sql


def test_without_node_timeline_utilisation_is_unknown():
    sql, mode = _sql(node_problem=f"{NODE_TIMELINE} unavailable")
    assert mode == MODE_CONFIG_ONLY
    assert f"FROM {NODE_TIMELINE}" not in sql
    assert "FALSE AS utilization_known" in sql


def test_or_variant_is_one_switch():
    and_sql, _ = build_idle_compute_sql(_ctx(), CLUSTER_COLUMNS, None, NODE_COLUMNS, None)
    or_sql, _ = build_idle_compute_sql(_ctx(), CLUSTER_COLUMNS, None, NODE_COLUMNS, None,
                                       config_flag_requires_idle=False)
    assert step_module.CONFIG_FLAG_REQUIRES_IDLE is True
    assert or_sql == and_sql.replace("NOT TRUE AND estimated_cost > 0", "NOT FALSE AND estimated_cost > 0")


def test_private_upstream_config_helpers_keep_their_signature():
    # cockpit/step_config.py imports these private helpers read-only; fail loudly if upstream changes them.
    from databricks_finops.config import _numeric_section, _read_yaml_file

    assert list(inspect.signature(_read_yaml_file).parameters) == ["path"]
    assert list(inspect.signature(_numeric_section).parameters) == ["data", "section", "defaults"]
    data, warning = _read_yaml_file(ROOT / "config" / "cockpit_thresholds.yml")
    assert warning is None and isinstance(data, dict)
    values, warnings = _numeric_section(data, SECTION, DEFAULTS)
    assert warnings == [] and set(values) == set(DEFAULTS)
    assert load_step_thresholds(str(ROOT), SECTION, DEFAULTS) == (values, [])


def test_without_either_source_the_mode_is_no_signal():
    _, mode = _sql(cluster_problem="x", node_problem="y")
    assert mode == MODE_NONE


def test_node_timeline_without_cpu_columns_counts_as_unusable():
    sql, mode = build_idle_compute_sql(_ctx(), CLUSTER_COLUMNS, None,
                                       {"workspace_id", "cluster_id", "start_time"}, None)
    assert mode == MODE_CONFIG_ONLY
    assert f"FROM {NODE_TIMELINE}" not in sql


def test_optional_cluster_columns_fall_back_to_null():
    sql, _ = build_idle_compute_sql(_ctx(), set(REQUIRED_CLUSTER_COLUMNS), None, NODE_COLUMNS, None)
    assert "CAST(NULL AS STRING) AS cluster_name" in sql
    assert "CAST(NULL AS STRING) AS owned_by" in sql


def test_thresholds_are_placeholders_not_literals():
    t = IdleComputeThresholds(zombie_auto_term_minutes=90, idle_hours_share_pct=75)
    sql, _ = build_idle_compute_sql(_ctx(), CLUSTER_COLUMNS, None, NODE_COLUMNS, None, t)
    assert "s.autotermination_minutes > 90" in sql and ">= 75" in sql
    assert "> 60" not in sql


def test_sql_comments_have_no_quotes():
    # RedirectingSpark skips text inside quotes, so an apostrophe in a comment hides the next
    # system.* name from the redirect (and real SQL parsers may also choke on it).
    for name in ("idle_compute", "idle_compute_cluster_state", "idle_compute_cluster_state_from_billing",
                 "idle_compute_node_hours"):
        text = resources.files("cockpit").joinpath("sql", f"{name}.sql").read_text(encoding="utf-8")
        for line in text.splitlines():
            if line.strip().startswith("--"):
                assert "'" not in line and '"' not in line, (name, line)


class _ProbeSpark:
    def __init__(self, columns=None, rows=(1,), fail=False):
        self._columns, self._rows, self._fail = columns, rows, fail

    def table(self, name):
        if self._fail:
            raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND")
        return type("T", (), {"columns": list(self._columns)})()

    def sql(self, statement):
        return type("R", (), {"collect": lambda _self: list(self._rows)})()


@pytest.mark.parametrize("spark, problem", [
    (_ProbeSpark(fail=True), "unavailable"),
    (_ProbeSpark(columns={"cluster_id"}), "lacks columns"),
    (_ProbeSpark(columns=CLUSTER_COLUMNS, rows=()), "is empty"),
    (_ProbeSpark(columns=CLUSTER_COLUMNS), None),
])
def test_probe_table(spark, problem):
    _, found = probe_table(spark, CLUSTERS, REQUIRED_CLUSTER_COLUMNS)
    assert found is None if problem is None else problem in found


def test_build_creates_empty_table_and_logs_why_when_no_source_is_usable():
    statements: list[str] = []

    class FakeSpark:
        def table(self, name):
            raise RuntimeError("missing")

        def sql(self, statement):
            statements.append(statement)

    message = step_module.build(_ctx(spark=FakeSpark()))
    assert "created empty" in message
    assert f"{CLUSTERS} unavailable" in message and f"{NODE_TIMELINE} unavailable" in message
    assert len(statements) == 1 and "FALSE AS config_known" in statements[0]


def test_repo_config_matches_code_defaults():
    thresholds, warnings = load_idle_compute_thresholds(str(ROOT))
    assert warnings == ()
    assert thresholds == IdleComputeThresholds(**DEFAULTS)
    assert thresholds.zombie_auto_term_minutes == 60


def test_missing_or_bad_cockpit_config_falls_back_to_defaults(tmp_path: Path):
    thresholds, warnings = load_idle_compute_thresholds(str(tmp_path))
    assert thresholds == IdleComputeThresholds() and "not found" in warnings[0]

    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "cockpit_thresholds.yml").write_text(
        "idle_compute:\n  zombie_auto_term_minutes: 90\n  priority_weight_savings: 0.9\n"
        "  idle_hours_share_pct: lots\n", encoding="utf-8")
    thresholds, warnings = load_idle_compute_thresholds(str(tmp_path))
    assert thresholds.zombie_auto_term_minutes == 90
    assert thresholds.priority_weight_savings == DEFAULTS["priority_weight_savings"]
    assert thresholds.idle_hours_share_pct == DEFAULTS["idle_hours_share_pct"]
    assert any("add up to 1.0" in w for w in warnings) and any("numeric" in w for w in warnings)


# ---------------------------------------------------------------------------
# On the synthetic sandbox with local Spark (skipped when pyspark/delta/Java are absent, e.g. CI)
# ---------------------------------------------------------------------------

SOURCE_SCHEMA = "spark_catalog.idle_sandbox"
FLAGGED = {
    "0404-zombie-autoterm-off": "AUTO_TERMINATION_OFF+LOW_UTILIZATION",
    "0505-zombie-long-autoterm": "AUTO_TERMINATION_TOO_LONG+LOW_UTILIZATION",
    "0707-autoterm-61-cluster": "AUTO_TERMINATION_TOO_LONG+LOW_UTILIZATION",
}


@pytest.fixture(scope="module")
def sandbox(tmp_path_factory):
    pytest.importorskip("pyspark")
    delta = pytest.importorskip("delta")
    if shutil.which("java") is None and not __import__("os").environ.get("JAVA_HOME"):
        pytest.skip("Java is not available")
    from pyspark.sql import SparkSession

    sys.path.insert(0, str(ROOT / "sandbox"))
    from finops_sandbox import RedirectingSpark, generate_synthetic_system_tables
    from databricks_finops.costs import create_daily_cost
    from databricks_finops.preflight import run_preflight

    warehouse = tmp_path_factory.mktemp("idle-warehouse").as_posix()
    builder = (
        SparkSession.builder.appName("test-idle-compute").master("local[1]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    generate_synthetic_system_tables(spark, SOURCE_SCHEMA)
    spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.cockpit")
    proxy = RedirectingSpark(spark, SOURCE_SCHEMA)
    config = _config()
    create_daily_cost(proxy, config, "run-1", run_preflight(proxy))
    # An empty compute.clusters, as on Databricks Free Edition.
    spark.sql(f"CREATE OR REPLACE TABLE {SOURCE_SCHEMA}.empty_clusters USING DELTA AS "
              f"SELECT * FROM {SOURCE_SCHEMA}.compute_clusters WHERE FALSE")
    yield spark, proxy
    spark.stop()


class _Swap:
    """Point one system table somewhere else (empty or missing), then apply the sandbox redirect."""

    def __init__(self, proxy, table: str, replacement: str):
        self._proxy, self._table, self._replacement = proxy, table, replacement

    def _swap(self, text: str) -> str:
        return text.replace(self._table, self._replacement)

    def sql(self, statement):
        return self._proxy.sql(self._swap(statement))

    def table(self, name):
        return self._proxy.table(self._swap(name))


def _run(spark_like, spark) -> tuple[str, dict[str, dict]]:
    message = STEP.run(_ctx(spark=spark_like))
    rows = [r.asDict() for r in spark.table("spark_catalog.cockpit.idle_compute").collect()]
    by_cluster = {r["cluster_id"]: r for r in rows}
    assert len(by_cluster) == len(rows)  # R1: one row per cluster
    return message, by_cluster


def test_sandbox_flags_exactly_the_zombie_idle_clusters(sandbox):
    spark, proxy = sandbox
    message, rows = _run(proxy, spark)

    assert "node_timeline utilisation" in message
    assert {c: r["idle_reason"] for c, r in rows.items()} == FLAGGED
    # Near-misses: exactly 60 min, deleted, job cluster reporting 0, busy, memory-bound.
    for near_miss in ("0101-shared-ds-cluster", "0606-deleted-old-cluster", "job-1004-cluster",
                      "0808-streaming-autoterm-off", "0909-memory-heavy-cluster"):
        assert near_miss not in rows

    assert rows["0404-zombie-autoterm-off"]["autotermination_minutes"] == 0  # latest change (30 -> 0) wins
    assert rows["0505-zombie-long-autoterm"]["autotermination_minutes"] == 240
    assert rows["0707-autoterm-61-cluster"]["autotermination_minutes"] == 61
    assert rows["0505-zombie-long-autoterm"]["workspace_id"] == "2222222222222222"
    assert rows["0404-zombie-autoterm-off"]["owner"] == "nina@example.com"
    assert rows["0404-zombie-autoterm-off"]["suggested_action"].startswith("Turn auto-termination on")
    assert "Lower auto-termination from 61 to 60" in rows["0707-autoterm-61-cluster"]["suggested_action"]

    for row in rows.values():
        assert row["cluster_source"] == "UI"
        assert row["detection_mode"] == MODE_FULL
        # The sandbox samples every 30 minutes (2 sampled minutes per hour), so minute-weighted active
        # hours are about 1/30 of clock hours and sit near high_confidence_min_active_hours (24).
        expected = "HIGH" if row["active_hours"] >= 24 else "MEDIUM"
        assert row["confidence"] == expected
        assert 0 < row["idle_hours"] <= row["active_hours"]
        assert 0 < row["idle_cost"] <= row["estimated_cost"]
        assert 0 < row["estimated_monthly_savings"] <= row["estimated_monthly_cost"]
        assert 0 <= row["priority_score"] <= 100
        assert row["avg_cpu_pct"] < 20 and row["run_id"] == "run-1"
        assert "active hours idle" in row["evidence"]

    # The always-on 24h zombie with the most savings ranks first.
    top = max(rows.values(), key=lambda r: r["priority_score"])
    assert top["cluster_id"] == "0404-zombie-autoterm-off"


def test_empty_clusters_table_degrades_to_utilisation_only(sandbox):
    spark, proxy = sandbox
    swapped = _Swap(proxy, CLUSTERS, f"{SOURCE_SCHEMA}.empty_clusters")
    message, rows = _run(swapped, spark)

    assert "utilisation only" in message and "is empty" in message
    # Idle all-purpose clusters by billing SKU. 0101 is idle too; without config it cannot be cleared.
    assert set(rows) == {"0101-shared-ds-cluster", *FLAGGED}
    for row in rows.values():
        assert row["idle_reason"] == "LOW_UTILIZATION"
        assert row["detection_mode"] == MODE_UTILIZATION_ONLY and row["confidence"] == "LOW"
        assert row["autotermination_minutes"] is None
        assert row["estimated_monthly_savings"] <= row["estimated_monthly_cost"]
        assert "auto-termination not checked" in row["evidence"]


def test_missing_node_timeline_flags_on_configuration_only(sandbox):
    spark, proxy = sandbox
    swapped = _Swap(proxy, NODE_TIMELINE, "spark_catalog.idle_sandbox.does_not_exist")
    message, rows = _run(swapped, spark)

    assert "auto-termination only" in message
    # Utilisation unknown, so the busy 0808 (auto-termination off) is flagged too, at LOW confidence.
    assert set(rows) == {*FLAGGED, "0808-streaming-autoterm-off"}
    assert rows["0808-streaming-autoterm-off"]["idle_reason"] == "AUTO_TERMINATION_OFF"
    for row in rows.values():
        assert row["detection_mode"] == MODE_CONFIG_ONLY and row["confidence"] == "LOW"
        assert row["idle_cost"] is None and row["estimated_monthly_savings"] is None
        assert row["estimated_cost"] > 0


# ---------------------------------------------------------------------------
# Hand-built boundaries on local Spark (no sandbox changes)
# ---------------------------------------------------------------------------

FIXTURE = "spark_catalog.idle_fixture"
FIXTURE_SCHEMA = "idle_fixture_out"

# cluster_id -> (auto_termination_minutes, [(minutes sampled, cpu %, memory %), one tuple per clock hour])
FIXTURE_CLUSTERS = {
    "half-idle": (0, [(60, 5.0, 10.0), (60, 50.0, 10.0)]),          # exactly 50% idle -> flagged
    "idle-49": (0, [(49, 5.0, 10.0), (51, 50.0, 10.0)]),            # 49% idle by minutes -> not flagged
    "cpu-20": (0, [(60, 20.0, 10.0)]),                              # CPU == low_cpu_pct is not idle
    "null-autoterm": (None, [(60, 5.0, 10.0)]),                     # unknown setting -> not flagged
    "partial-idle-hour": (0, [(3, 5.0, 10.0), (60, 50.0, 10.0)]),   # 3/63 idle; unweighted would be 50%
    "partial-busy-hour": (0, [(60, 5.0, 10.0), (3, 50.0, 10.0)]),   # 60/63 idle -> flagged
    "null-memory": (0, [(60, 5.0, None)]),                          # memory not reported: CPU decides
    "busy-off": (0, [(60, 70.0, 30.0)]),                            # busy, auto-termination off
}
FIXTURE_FLAGGED = {"half-idle", "partial-busy-hour", "null-memory"}


@pytest.fixture(scope="module")
def boundaries(sandbox):
    spark, _ = sandbox
    base = datetime.combine(datetime.now(timezone.utc).date() - timedelta(days=1), datetime.min.time())
    base = base + timedelta(hours=8)
    clusters, nodes, cost = [], [], []
    for cluster_id, (autoterm, hours) in FIXTURE_CLUSTERS.items():
        clusters.append(("w1", cluster_id, cluster_id, "UI", "owner@example.com", autoterm,
                         datetime(2025, 6, 1), None))
        for h, (minutes, cpu, mem) in enumerate(hours):
            for m in range(minutes):
                t = base + timedelta(hours=h, minutes=m)
                nodes.append(("w1", cluster_id, t, t + timedelta(minutes=1), cpu * 0.8, cpu * 0.2, mem))
        cost.append(("w1", cluster_id, 10.0, None, "PREMIUM_ALL_PURPOSE_COMPUTE", "ALL_PURPOSE", None, "run-1",
                     base.date()))

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FIXTURE}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS spark_catalog.{FIXTURE_SCHEMA}")
    spark.createDataFrame(clusters, "workspace_id string, cluster_id string, cluster_name string, "
                          "cluster_source string, owned_by string, auto_termination_minutes bigint, "
                          "change_time timestamp, delete_time timestamp") \
        .write.format("delta").mode("overwrite").saveAsTable(f"{FIXTURE}.clusters")
    spark.createDataFrame(nodes, "workspace_id string, cluster_id string, start_time timestamp, "
                          "end_time timestamp, cpu_user_percent double, cpu_system_percent double, "
                          "mem_used_percent double") \
        .write.format("delta").mode("overwrite").saveAsTable(f"{FIXTURE}.node_timeline")
    spark.createDataFrame(cost, "workspace_id string, cluster_id string, estimated_cost double, owner string, "
                          "sku_name string, billing_origin_product string, job_id string, run_id string, "
                          "usage_date date") \
        .write.format("delta").mode("overwrite").saveAsTable(f"spark_catalog.{FIXTURE_SCHEMA}.daily_cost")

    class Redirect:
        def _swap(self, text):
            return text.replace(CLUSTERS, f"{FIXTURE}.clusters").replace(NODE_TIMELINE, f"{FIXTURE}.node_timeline")

        def sql(self, statement):
            return spark.sql(self._swap(statement))

        def table(self, name):
            return spark.table(self._swap(name))

    return spark, Redirect()


def _fixture_rows(spark, redirect, config_flag_requires_idle=True):
    ctx = _ctx(spark=redirect, schema=FIXTURE_SCHEMA)
    statement, mode = build_idle_compute_sql(ctx, CLUSTER_COLUMNS, None, NODE_COLUMNS, None,
                                             config_flag_requires_idle=config_flag_requires_idle)
    assert mode == MODE_FULL
    redirect.sql(statement)
    rows = [r.asDict() for r in spark.table(f"spark_catalog.{FIXTURE_SCHEMA}.idle_compute").collect()]
    return {r["cluster_id"]: r for r in rows}


def test_boundaries_flag_exactly_the_expected_clusters(boundaries):
    spark, redirect = boundaries
    STEP.run(_ctx(spark=redirect, schema=FIXTURE_SCHEMA))  # the real entry point, probes included
    rows = {r["cluster_id"]: r.asDict()
            for r in spark.table(f"spark_catalog.{FIXTURE_SCHEMA}.idle_compute").collect()}
    assert set(rows) == FIXTURE_FLAGGED

    assert rows["half-idle"]["idle_hours_pct"] == 50.0
    assert float(rows["half-idle"]["idle_cost"]) == pytest.approx(5.0)
    assert rows["null-memory"]["avg_memory_pct"] is None and "memory n/a" in rows["null-memory"]["evidence"]


def test_hours_are_weighted_by_minutes_sampled(boundaries):
    spark, redirect = boundaries
    rows = _fixture_rows(spark, redirect)
    partial = rows["partial-busy-hour"]
    assert partial["active_hours"] == pytest.approx(63 / 60, abs=0.01)
    assert partial["idle_hours"] == pytest.approx(1.0)
    assert partial["idle_hours_pct"] == pytest.approx(60 / 63 * 100, abs=0.01)
    assert float(partial["idle_cost"]) == pytest.approx(10.0 * 60 / 63, abs=1e-4)
    assert "partial-idle-hour" not in rows  # 3 idle minutes are not half of 63


def test_or_variant_flags_configuration_alone_without_savings(boundaries):
    spark, redirect = boundaries
    rows = _fixture_rows(spark, redirect, config_flag_requires_idle=False)
    # Every cluster with a zombie setting and cost; the NULL setting stays unflagged.
    assert set(rows) == set(FIXTURE_CLUSTERS) - {"null-autoterm"}
    busy = rows["busy-off"]
    assert busy["idle_reason"] == "AUTO_TERMINATION_OFF"
    assert busy["estimated_monthly_savings"] is None
    assert busy["priority_score"] < min(rows[c]["priority_score"] for c in FIXTURE_FLAGGED)
    assert rows["half-idle"]["idle_reason"] == "AUTO_TERMINATION_OFF+LOW_UTILIZATION"
    assert rows["half-idle"]["estimated_monthly_savings"] is not None
