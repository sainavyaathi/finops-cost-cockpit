"""Shared detection window (F4/F5/F6) and F7 waste_flags SQL composition (no Spark needed; runs in CI)."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from cockpit.pipeline import StepContext, discover_steps
from cockpit.step_config import DetectionWindow, detection_window
from cockpit.steps import allpurpose_misuse as f4
from cockpit.steps import idle_compute as f5
from cockpit.steps import job_reliability as f6
from cockpit.steps import waste_flags as f7
from databricks_finops.config import AppConfig
from databricks_finops.preflight import PreflightResult, TableCapability

ROOT = Path(__file__).resolve().parents[1]
TASK = "system.lakeflow.job_task_run_timeline"
RUNS = "system.lakeflow.job_run_timeline"
TASK_COLUMNS = {"workspace_id", "job_id", "run_id", "job_run_id", "parent_run_id", "period_start_time",
                "period_end_time", "task_key", "result_state"}
RUN_COLUMNS = {"workspace_id", "job_id", "run_id", "period_start_time", "period_end_time", "result_state"}


def _config(lookback_days: int = 30, bundle_root: str = str(ROOT)) -> AppConfig:
    return AppConfig(catalog="spark_catalog", schema="cockpit", lookback_days=lookback_days, currency_code="USD",
                     display_currency="USD", fallback_dbu_price=0.55, bundle_root=bundle_root)


def _preflight(tables: dict[str, set[str]]) -> PreflightResult:
    caps = [TableCapability(source_name=name, check_name=name, required=False, available=True,
                            message="ok", affected_output="x") for name in tables]
    return PreflightResult(capabilities=caps, columns_by_table=dict(tables))


def _ctx(lookback_days: int = 30, preflight=None, bundle_root: str = str(ROOT)) -> StepContext:
    return StepContext(spark=None, config=_config(lookback_days, bundle_root), run_id="run-1", preflight=preflight)


def _write_config(tmp_path: Path, text: str) -> str:
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "config" / "cockpit_thresholds.yml").write_text(text, encoding="utf-8")
    return str(tmp_path)


# ---------------------------------------------------------------------------
# Detection window
# ---------------------------------------------------------------------------

def test_detection_window_defaults_to_30_and_is_clamped_to_the_lookback():
    assert detection_window(str(ROOT), 30) == DetectionWindow(days=30, configured_days=30, lookback_days=30)
    long = detection_window(str(ROOT), 180)
    assert (long.days, long.narrowed, long.monthly_multiplier) == (30, True, 1.0)
    short = detection_window(str(ROOT), 15)
    assert (short.days, short.narrowed, short.monthly_multiplier) == (15, False, 2.0)


def test_detection_window_reads_the_shared_section(tmp_path):
    root = _write_config(tmp_path, "detection:\n  detection_days: 14\n")
    window = detection_window(root, 180)
    assert (window.days, window.configured_days, window.warnings) == (14, 14, ())
    assert window.monthly_multiplier == pytest.approx(30 / 14)


@pytest.mark.parametrize("value", ["0", "-3", "7.5"])
def test_invalid_detection_days_falls_back_to_default_with_warning(tmp_path, value):
    root = _write_config(tmp_path, f"detection:\n  detection_days: {value}\n")
    window = detection_window(root, 180)
    assert window.days == 30
    assert any("detection.detection_days" in w for w in window.warnings)


def test_f4_reads_every_source_over_the_detection_window_not_the_lookback():
    available = {name: True for name in f4.PROBES}
    sql, _ = f4.build_statement(_ctx(180), available, dict(f4.DEFAULTS))
    assert "INTERVAL 180 DAYS" not in sql
    # billing, audit (BI + shared users), query history, job runs and task runs all use 30 days.
    assert sql.count("INTERVAL 30 DAYS") >= 6
    assert "ap.cluster_cost * 30.0 / 30 AS monthly_cost" in sql  # monthly multiplier = 30 / detection
    assert "CAST(180 AS INT) AS lookback_days" in sql and "CAST(30 AS INT) AS detection_days" in sql
    assert "{" not in sql and "}" not in sql


def test_f4_default_profile_is_unchanged():
    available = {name: True for name in f4.PROBES}
    sql, _ = f4.build_statement(_ctx(30), available, dict(f4.DEFAULTS))
    assert "u.usage_date >= current_date() - INTERVAL 30 DAYS" in sql
    assert "CAST(30 AS INT) AS lookback_days" in sql and "CAST(30 AS INT) AS detection_days" in sql


def test_f5_cost_and_utilisation_use_the_detection_window():
    sql, _ = f5.build_idle_compute_sql(_ctx(180), set(f5.REQUIRED_CLUSTER_COLUMNS) | {"cluster_name", "owned_by"},
                                       None, {"workspace_id", "cluster_id", "start_time", "cpu_user_percent",
                                              "cpu_system_percent", "mem_used_percent"}, None)
    assert "INTERVAL 180 DAYS" not in sql
    assert "usage_date >= current_date() - INTERVAL 30 DAYS" in sql  # daily_cost holds 180 days
    assert "start_time >= CAST(current_date() - INTERVAL 30 DAYS AS TIMESTAMP)" in sql
    assert "estimated_cost * 1 AS DECIMAL(38,6)) AS estimated_monthly_cost" in sql
    assert "over 30 days" in sql
    assert "CAST(180 AS INT) AS lookback_days" in sql and "CAST(30 AS INT) AS detection_days" in sql


def test_f6_narrow_window_re_renders_upstream_summary_without_touching_it():
    sql, reason = f6.build_job_reliability_sql(_ctx(180, _preflight({TASK: TASK_COLUMNS, RUNS: RUN_COLUMNS})))
    assert reason is None
    assert "INTERVAL 180 DAYS" not in sql
    assert "FROM `spark_catalog`.`cockpit`.`job_reliability_summary`" not in sql
    assert "CREATE OR REPLACE TABLE `spark_catalog`.`cockpit`.`job_reliability`" in sql
    assert sql.count("CREATE OR REPLACE TABLE") == 1  # upstream header stripped, body inlined
    assert "terminal_runs AS" in sql and "u.usage_date >= current_date() - INTERVAL 30 DAYS" in sql
    assert "INTERVAL 32 DAYS" in sql  # task window = detection + margin
    assert "AS retry_task_minutes" in sql and "CAST(30 AS INT) AS detection_days" in sql


def test_f6_default_window_reads_the_upstream_table():
    sql, _ = f6.build_job_reliability_sql(_ctx(30, _preflight({TASK: TASK_COLUMNS, RUNS: RUN_COLUMNS})))
    assert "FROM `spark_catalog`.`cockpit`.`job_reliability_summary` u" in sql
    assert "unix_timestamp(MAX(period_end_time))" in sql  # retry minutes from the attempt span


def test_f6_without_job_run_timeline_keeps_the_upstream_placeholder(tmp_path):
    ctx = _ctx(180, _preflight({TASK: TASK_COLUMNS}))
    relation, note = f6.upstream_relation(ctx, detection_window(str(ROOT), 180))
    assert relation == "`spark_catalog`.`cockpit`.`job_reliability_summary`"
    assert "not applied" in note


def test_f6_without_period_end_time_retry_minutes_are_null():
    columns = TASK_COLUMNS - {"period_end_time"}
    sql, _ = f6.build_job_reliability_sql(_ctx(30, _preflight({TASK: columns, RUNS: RUN_COLUMNS})))
    assert "unix_timestamp(MAX(period_end_time))" not in sql
    assert "CAST(NULL AS DOUBLE) AS corrected_retry_task_minutes" in sql


# ---------------------------------------------------------------------------
# F7 waste_flags
# ---------------------------------------------------------------------------

def _all_columns() -> dict[str, set[str] | None]:
    columns: dict[str, set[str] | None] = {name: set(req) for name, (_, _, req) in f7.SOURCES.items()}
    columns["job_reliability"] |= {"retried_run_count", "retry_task_minutes", "task_attempt_minutes",
                                   "detection_days"}
    columns["cost_base"] = set(f7.COST_BASE_COLUMNS)
    return columns


def _f7(columns=None, lookback_days=30, **kwargs):
    return f7.build_statement(_ctx(lookback_days), columns or _all_columns(), **kwargs)


def test_step_is_discovered_after_its_sources():
    names = [s.name for s in discover_steps()]
    assert "build_waste_flags" in names
    for dep in ("build_allpurpose_misuse", "build_idle_compute", "build_job_reliability", "build_cost_base"):
        assert dep in f7.STEP.depends_on
        assert names.index(dep) < names.index("build_waste_flags")
    assert f7.STEP.output_table == "waste_flags" and f7.STEP.required


def test_statement_reads_every_source_and_resolves_placeholders():
    sql, notes, available = _f7()
    assert notes == [] and all(available.values())
    assert "CREATE OR REPLACE TABLE `spark_catalog`.`cockpit`.`waste_flags`" in sql
    for table in ("idle_compute", "allpurpose_misuse", "job_reliability", "optimization_candidates", "cost_base"):
        assert f"FROM `spark_catalog`.`cockpit`.`{table}`" in sql, table
    assert "usage_date >= current_date() - INTERVAL 30 DAYS" in sql
    assert re.search(r"\{[a-z_]+\}", sql) is None


def test_no_hard_coded_catalog_or_schema():
    sql, _, _ = _f7()
    for name in ("workspace.", "finops.", "`finops`", "finops_accelerator"):
        assert name not in sql
    assert "spark_catalog" not in re.sub(r"`spark_catalog`\.`cockpit`", "", sql)


def test_savings_are_the_max_of_findings_never_the_sum_and_capped_at_cost():
    sql, _, _ = _f7()
    assert "MAX(capped_savings) AS kept_savings" in sql
    assert not re.search(r"SUM\(\s*(capped_)?savings", sql)
    assert "LEAST(CAST(savings AS DOUBLE), resource_monthly_cost)" in sql
    # Unknown resource cost: the finding's own basis stands, it is never capped to 0.
    assert "WHEN resource_monthly_cost IS NULL THEN CAST(savings AS DECIMAL(38,6))" in sql
    assert "COALESCE(resource_monthly_cost, monthly_savings, 0.0) AS DOUBLE) AS monthly_cost" in sql
    # A JOB leaves the cost it shares with a CLUSTER that carries a saving to the cluster.
    assert "COALESCE(p.resource_monthly_cost, 0.0) - s.shared_monthly_cost" in sql


def test_priority_formula_uses_configured_weights_and_rank_is_unique():
    sql, _, _ = _f7()
    assert "0.6 * LEAST(100.0, monthly_savings / 1000 * 100.0)" in sql
    assert "0.3 * LEAST(100.0, monthly_cost / 1000 * 100.0)" in sql
    assert "0.1 * LEAST(100.0, (flag_type_count - 1) * 50)" in sql
    assert "WHEN 'MEDIUM' THEN 0.85" in sql
    assert "ROW_NUMBER() OVER (\n        ORDER BY score DESC" in sql


def test_upstream_items_superseded_by_f6_and_retry_never_taken():
    sql, _, _ = _f7()
    clause = re.search(r"WHERE (issue_type IN \([^)]*\))", sql).group(1)
    assert "HIGH_FAILURE_COST" not in clause and "HIGH_FAILURE_RATE" not in clause
    assert "RETRY_HEAVY" not in clause and "SHARED_CLUSTER_ATTRIBUTION" in clause
    assert "<> 'LOOKS_HEALTHY'" in sql
    assert "f.flag_type = 'SHARED_CLUSTER_ATTRIBUTION'" in sql  # dropped where F4/F5 flag the cluster


def test_missing_sources_are_skipped_with_a_note():
    columns = _all_columns()
    columns["job_reliability"] = None
    columns["idle_compute"] = {"workspace_id"}
    columns["cost_base"] = None
    sql, notes, available = _f7(columns)
    assert not available["job_reliability"] and not available["idle_compute"]
    assert "job_reliability unavailable: job_reliability skipped" in notes
    assert any(n.startswith("idle_compute lacks columns") for n in notes)
    assert any(n.startswith("cost_base unavailable") for n in notes)
    assert "FROM `spark_catalog`.`cockpit`.`job_reliability`" not in sql
    # Without F6, upstream failure items stay in the backlog.
    clause = re.search(r"WHERE (issue_type IN \([^)]*\))", sql).group(1)
    assert "HIGH_FAILURE_COST" in clause
    assert "WHERE 1 = 0" in sql


def test_f6_monthly_figures_use_the_job_reliability_window():
    sql, _, _ = _f7(lookback_days=180)
    assert "COALESCE(j.detection_days, 30)" in sql
    assert "COALESCE(failed_cost, 0.0) * 30.0 / window_days AS monthly_failed_cost" in sql
    assert "monthly_failed_cost >= 10" in sql and "failure_rate_pct >= 20" in sql and "retry_count >= 5" in sql
    assert "CAST(30 AS INT) AS detection_days" in sql
    assert "INTERVAL 180 DAYS" not in sql


def test_settings_validate_weights_bounds_and_retry_type(tmp_path):
    root = _write_config(tmp_path, "waste_flags:\n  priority_weight_savings: 0.9\n"
                                   "  score_reference_monthly_cost: 0\n"
                                   "  upstream_issue_types: LOW_UTILIZATION, RETRY_HEAVY\n")
    values, types, warnings = f7.load_settings(root)
    assert values["priority_weight_savings"] == f7.DEFAULTS["priority_weight_savings"]
    assert values["score_reference_monthly_cost"] == f7.DEFAULTS["score_reference_monthly_cost"]
    assert types == ("LOW_UTILIZATION",)
    assert any("add up to 1.0" in w for w in warnings)
    assert any("RETRY_HEAVY ignored" in w for w in warnings)


def test_repo_config_matches_code_defaults():
    values, types, warnings = f7.load_settings(str(ROOT))
    assert warnings == []
    assert values == {k: float(v) for k, v in f7.DEFAULTS.items()}
    assert types == f7.LIST_DEFAULTS["upstream_issue_types"]


def test_resource_cost_keys_follow_upstream_workload_precedence():
    sql, _, _ = _f7()
    assert "WHERE workload_resource_type <> 'CLUSTER'" in sql  # JOB/WAREHOUSE/PIPELINE/NOTEBOOK exclusive
    assert "SELECT 'CLUSTER', workspace_id, cluster_id" in sql  # CLUSTER keeps every row (as F4/F5)
    assert "upper(workload_type) = 'JOB'" in sql  # job cost per cluster, for the cross-resource cap
    for old_key in ("warehouse_id", "pipeline_id", "notebook_id"):
        assert old_key not in sql


def test_stale_upstream_findings_dropped_only_when_cost_base_is_readable():
    sql, _, _ = _f7()
    assert "AND TRUE\n               AND COALESCE(rc.window_cost, 0.0) <= 0.0" in sql
    columns = _all_columns()
    columns["cost_base"] = None
    sql, _, _ = _f7(columns)
    assert "AND FALSE\n               AND COALESCE(rc.window_cost, 0.0) <= 0.0" in sql


def test_upstream_findings_deduplicated_per_resource_and_flag_type():
    sql, _, _ = _f7()
    assert "PARTITION BY resource_type, workspace_id, resource_id, flag_type" in sql
    assert "WHERE duplicate_rank = 1" in sql
    assert re.search(r"source_order,\s+flag_type,\s+evidence\s+\) AS finding_rank", sql)


def test_currency_is_a_sql_literal():
    config = AppConfig(catalog="spark_catalog", schema="cockpit", lookback_days=30, currency_code="E'UR",
                       display_currency="EUR", fallback_dbu_price=0.55, bundle_root=str(ROOT))
    ctx = StepContext(spark=None, config=config, run_id="run-1", preflight=None)
    sql, _, _ = f7.build_statement(ctx, _all_columns())
    assert "' ', 'E''UR', ' over '" in sql  # escaped by sql_string, not stripped


def test_owner_and_team_prefer_the_source_then_the_tag():
    sql, _, _ = _f7()
    assert "COALESCE(source_owner, owner_tag) AS STRING) AS owner" in sql
    assert "COALESCE(source_team, team_tag) AS STRING) AS team" in sql


def test_f6_builder_renders_the_upstream_summary_once():
    calls = []
    original = f6.build_upstream_reliability_sql

    def counting(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)

    f6.build_upstream_reliability_sql = counting
    try:
        sql, reason, note = f6.build_job_reliability_statement(
            _ctx(180, _preflight({TASK: TASK_COLUMNS, RUNS: RUN_COLUMNS})))
    finally:
        f6.build_upstream_reliability_sql = original
    assert len(calls) == 1 and reason is None and note is None
