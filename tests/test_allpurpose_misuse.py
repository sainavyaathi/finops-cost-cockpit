"""F4 all-purpose misuse: step wiring, SQL composition and thresholds (no Spark needed)."""

from __future__ import annotations

import re
from importlib import resources

import pytest

from cockpit.pipeline import StepContext, discover_steps
from cockpit.step_config import enforce_bounds, load_step_lists, load_step_thresholds
from cockpit.steps import allpurpose_misuse as f4
from databricks_finops.config import AppConfig

CONFIG = AppConfig(catalog="finops", schema="cockpit", lookback_days=30, currency_code="USD",
                   display_currency="USD", fallback_dbu_price=0.55, bundle_root=".")
ALL = {name: True for name in f4.PROBES}


def _ctx(spark=None, config=CONFIG) -> StepContext:
    return StepContext(spark=spark, config=config, run_id="run-1", preflight=None)


def _statement(available=ALL, thresholds=None, lists=None) -> tuple[str, list[str]]:
    return f4.build_statement(_ctx(), available, thresholds or dict(f4.DEFAULTS), lists)


def _without(*names: str) -> dict[str, bool]:
    return {name: name not in names for name in f4.PROBES}


def test_step_is_discovered_after_cost_base():
    names = [s.name for s in discover_steps()]
    assert "build_allpurpose_misuse" in names
    assert names.index("build_cost_base") < names.index("build_allpurpose_misuse")
    assert f4.STEP.output_table == "allpurpose_misuse"
    assert f4.STEP.depends_on == ("build_cost_base",)


def test_statement_writes_the_cockpit_table_and_resolves_every_placeholder():
    sql, notes = _statement()
    assert notes == []
    assert sql.startswith("-- F4")
    assert "CREATE OR REPLACE TABLE `finops`.`cockpit`.`allpurpose_misuse`" in sql
    assert "FROM `finops`.`cockpit`.`cost_base`" in sql
    assert "{" not in sql and "}" not in sql
    for source in ("system.billing.usage", "system.billing.list_prices", "system.compute.clusters",
                   "system.access.audit", "system.query.history", "system.lakeflow.jobs",
                   "system.lakeflow.job_run_timeline", "system.lakeflow.job_task_run_timeline"):
        assert source in sql, source


def test_statement_never_hard_codes_a_catalog_or_schema():
    sql, _ = _statement()
    without_targets = sql.replace("`finops`.`cockpit`.", "")
    for name in ("finops", "spark_catalog", "workspace.", "`cockpit`", "finops_synthetic"):
        assert name not in without_targets, name


def test_thresholds_are_placeholders_not_magic_numbers():
    thresholds = dict(f4.DEFAULTS, min_interactive_users=7, min_bi_events=11, min_scheduled_runs=13,
                      bi_to_sql_warehouse_savings_pct=42, jobs_price_ratio_fallback=0.123)
    sql, _ = _statement(thresholds=thresholds)
    assert "actor_count >= 7" in sql
    assert "event_count >= 11" in sql
    assert "item_count >= 13" in sql
    assert "* 42 / 100.0" in sql
    assert "* 0.123" in sql
    assert "'CRON', 'PERIODIC'" in sql
    assert "IN ('UI', 'API')" in sql


def test_sql_templates_have_no_quotes_in_comments():
    # RedirectingSpark does not parse comments: a quote there starts a fake string literal and
    # hides the system.* names after it from the rewrite.
    sql_dir = resources.files("cockpit").joinpath("sql")
    for template in sql_dir.iterdir():
        if not template.name.startswith("allpurpose_misuse"):
            continue
        for line in template.read_text(encoding="utf-8").splitlines():
            comment = line.split("--", 1)[1] if "--" in line else ""
            assert "'" not in comment and '"' not in comment, f"{template.name}: {line}"


def test_signals_are_independent_and_missing_audit_skips_only_its_signals():
    sql, notes = _statement(_without("audit"))
    assert "system.access.audit" not in sql
    assert "attachNotebook" not in sql
    assert "system.query.history" in sql and "system.lakeflow.job_run_timeline" in sql
    assert any("SHARED_USERS skipped" in n for n in notes)


def test_missing_query_history_keeps_the_audit_signal():
    sql, notes = _statement(_without("query_history"))
    assert "system.query.history" not in sql
    assert "system.access.audit" in sql
    assert any("query.history unavailable" in n for n in notes)


def test_missing_clusters_falls_back_to_all_purpose_billing_skus():
    sql, notes = _statement(_without("clusters"))
    assert "system.compute.clusters" not in sql
    assert "billed_as_all_purpose" in sql
    assert any("ALL_PURPOSE billing SKUs" in n for n in notes)


def test_missing_job_runs_skips_scheduled_job():
    sql, notes = _statement(_without("job_runs"))
    assert "system.lakeflow" not in sql
    assert any("SCHEDULED_JOB skipped" in n for n in notes)


def test_missing_task_runs_skips_scheduled_job_with_a_note():
    sql, notes = _statement(_without("task_runs"))
    assert "system.lakeflow" not in sql
    assert any("job_task_run_timeline unavailable: SCHEDULED_JOB skipped" in n for n in notes)


def test_missing_job_trigger_and_run_type_columns_degrade_gracefully():
    sql, notes = _statement(_without("jobs_trigger", "job_run_type"))
    assert "MAX_BY(CAST(NULL AS STRING), change_time) AS job_trigger_type" in sql
    assert "MAX(CAST(NULL AS STRING)) AS run_type" in sql
    assert any("automated_run_types not applied" in n for n in notes)


def test_default_automation_rules_cover_orchestrators_and_exclude_manual_runs():
    sql, _ = _statement()
    assert ("c.effective_trigger IN ('CRON', 'PERIODIC', 'CONTINUOUS', 'FILE_ARRIVAL', 'TABLE')" in sql)
    assert "c.run_type IN ('SUBMIT_RUN')" in sql
    assert "(TRUE AND c.effective_trigger = 'ONETIME_RETRY'" in sql
    assert "'ONETIME'," not in sql and "'JOB_RUN'" not in sql


def test_automation_rules_split_retries_and_drop_manual_values():
    triggers, run_types, retries, warnings = f4.automation_rules(
        {"automated_trigger_types": ("CRON", "ONETIME", "ONETIME_RETRY"), "automated_run_types": ("JOB_RUN",)})
    assert (triggers, run_types, retries) == (("CRON",), (), True)
    assert warnings
    sql, _ = _statement(lists={"automated_trigger_types": ("CRON",), "automated_run_types": ()})
    assert "(FALSE AND c.effective_trigger = 'ONETIME_RETRY'" in sql
    assert "WHEN FALSE THEN c.run_type" in sql


def test_query_history_switch_and_cluster_compute_filter():
    sql, _ = _statement()
    assert "NOT IN ('WAREHOUSE', 'SERVERLESS_COMPUTE')" in sql
    sql, notes = _statement(thresholds=dict(f4.DEFAULTS, use_query_history=0))
    assert "system.query.history" not in sql
    assert any("use_query_history: 0" in n for n in notes)


def test_hours_are_cut_in_utc_not_the_session_time_zone():
    sql, _ = _statement()
    assert "date_trunc" not in sql
    assert sql.count("timestamp_seconds(FLOOR(unix_seconds(") == 6


def test_interactive_hours_are_subtracted_from_scheduled_job_islands():
    sql, _ = _statement()
    assert "'INTERACTIVE' AS misuse_type" in sql and "-1.0 AS sign" in sql
    assert "DBUs assumed unchanged" in sql


def test_usage_is_filtered_to_all_purpose_candidates_before_pricing():
    sql, _ = _statement()
    assert sql.index("cluster_usage AS") < sql.index("usage_rows AS")
    assert "FROM cluster_usage u" in sql
    assert sql.count("FROM system.billing.usage") == 1


def test_missing_list_prices_uses_fallbacks():
    sql, notes = _statement(_without("list_prices"))
    assert "system.billing.list_prices" not in sql
    assert any("fallback" in n for n in notes)


def test_empty_relation_is_typed_and_has_no_rows():
    assert f4.empty_relation((("a", "STRING"), ("b", "TIMESTAMP"))) == (
        "SELECT CAST(NULL AS STRING) AS a, CAST(NULL AS TIMESTAMP) AS b WHERE 1 = 0")


def test_bi_client_regex_matches_bi_drivers_but_not_sdk_or_browser():
    pattern = re.compile(f4.BI_CLIENT_REGEX[len("(?i)"):], re.IGNORECASE)
    for agent in ("SimbaSparkJDBCDriver/2.6.36 (Tableau 2024.2)", "Simba Spark ODBC Driver/2.8.2 (PowerBI Desktop)",
                  "Databricks JDBC Driver", "Looker"):
        assert pattern.search(agent), agent
    for agent in ("databricks-sdk-py/0.38.0 python/3.11.9 os/linux auth/pat",
                  "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 Chrome/126.0"):
        assert not pattern.search(agent), agent


def test_thresholds_load_from_the_step_section(tmp_path):
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "cockpit_thresholds.yml").write_text(
        "other_step:\n  x: 1\nallpurpose_misuse:\n  min_interactive_users: 4\n  min_bi_events: lots\n",
        encoding="utf-8")
    values, warnings = load_step_thresholds(str(tmp_path), f4.SECTION, f4.DEFAULTS)
    assert values["min_interactive_users"] == 4
    assert values["min_bi_events"] == f4.DEFAULTS["min_bi_events"]
    assert any("min_bi_events" in w for w in warnings)


def test_repo_thresholds_file_carries_the_defaults():
    thresholds, lists, warnings = f4.load_settings(".")
    assert warnings == []
    assert thresholds == {k: float(v) for k, v in f4.DEFAULTS.items()}
    assert lists == f4.LIST_DEFAULTS


def _bundle(tmp_path, body: str) -> str:
    (tmp_path / "config").mkdir(exist_ok=True)
    (tmp_path / "config" / "cockpit_thresholds.yml").write_text(body, encoding="utf-8")
    return str(tmp_path)


def test_lists_accept_yaml_lists_or_comma_strings(tmp_path):
    root = _bundle(tmp_path, "allpurpose_misuse:\n  automated_trigger_types: CRON, TABLE\n"
                             "  automated_run_types:\n    - SUBMIT_RUN\n    - WORKFLOW_RUN\n")
    values, warnings = load_step_lists(root, f4.SECTION, f4.LIST_DEFAULTS)
    assert values == {"automated_trigger_types": ("CRON", "TABLE"), "automated_run_types": ("SUBMIT_RUN", "WORKFLOW_RUN")}
    assert warnings == []


@pytest.mark.parametrize("bad", ["cron", "CRON'; DROP TABLE x; --", "CRON) OR (1=1", "", "[]", "1CRON"])
def test_lists_reject_anything_but_uppercase_identifiers(tmp_path, bad):
    root = _bundle(tmp_path, f"allpurpose_misuse:\n  automated_trigger_types: \"{bad}\"\n")
    values, warnings = load_step_lists(root, f4.SECTION, f4.LIST_DEFAULTS)
    assert values["automated_trigger_types"] == f4.LIST_DEFAULTS["automated_trigger_types"]
    assert warnings and "UPPERCASE identifiers only" in warnings[0]


@pytest.mark.parametrize("key, value", [("score_reference_monthly_cost", 0), ("score_reference_monthly_cost", -5),
                                        ("min_bi_events", 0), ("bi_to_sql_warehouse_savings_pct", 150),
                                        ("jobs_price_ratio_fallback", 2), ("use_query_history", 3)])
def test_out_of_range_values_fall_back_to_defaults(tmp_path, key, value):
    thresholds, _, warnings = f4.load_settings(_bundle(tmp_path, f"allpurpose_misuse:\n  {key}: {value}\n"))
    assert thresholds[key] == f4.DEFAULTS[key]
    assert any(key in w for w in warnings)


def test_enforce_bounds_keeps_valid_values():
    values = {"a": 5.0, "b": 0.0}
    assert enforce_bounds(values, {"a": 1.0, "b": 1.0}, "s", {"a": (0, None, True), "b": (0, 1, False)}) == []
    assert values == {"a": 5.0, "b": 0.0}


def test_upstream_private_helpers_step_config_relies_on(tmp_path):
    # step_config imports two private helpers from the vendored engine (read-only, rule R3). Pin
    # their behaviour so an upstream refresh that changes them fails here, not at run time.
    from databricks_finops.config import _numeric_section, _read_yaml_file

    data, warning = _read_yaml_file(tmp_path / "missing.yml")
    assert data == {} and "built-in defaults" in warning
    path = tmp_path / "x.yml"
    path.write_text("s:\n  a: 2\n  b: nope\n", encoding="utf-8")
    data, warning = _read_yaml_file(path)
    assert warning is None and data == {"s": {"a": 2, "b": "nope"}}
    values, warnings = _numeric_section(data, "s", {"a": 1.0, "b": 1.0, "c": 3.0})
    assert values == {"a": 2.0, "b": 1.0, "c": 3.0}
    assert len(warnings) == 1 and "s.b" in warnings[0]
    assert _numeric_section({}, "s", {"a": 1.0}) == ({"a": 1.0}, [])


def test_missing_thresholds_file_uses_defaults_with_a_warning(tmp_path):
    values, warnings = load_step_thresholds(str(tmp_path), f4.SECTION, f4.DEFAULTS)
    assert values == f4.DEFAULTS
    assert warnings and "built-in defaults" in warnings[0]


class _FakeSpark:
    """Fails the probes whose SQL mentions a missing table; records everything else."""

    def __init__(self, missing=()):
        self.missing = missing
        self.statements: list[str] = []

    def sql(self, statement):
        self.statements.append(statement)
        if any(name in statement for name in self.missing) and "LIMIT 0" in statement:
            raise RuntimeError("TABLE_OR_VIEW_NOT_FOUND")

        class _Result:
            @staticmethod
            def collect():
                return [(["SCHEDULED_JOB", "SHARED_USERS"],)] if statement.startswith("SELECT misuse_types") else []

        return _Result()


def test_build_reports_flags_and_skipped_sources_in_the_run_log_message():
    spark = _FakeSpark(missing=("system.access.audit",))
    message = f4.STEP.run(_ctx(spark))
    creates = [s for s in spark.statements if "CREATE OR REPLACE TABLE" in s]
    assert len(creates) == 1 and "system.access.audit" not in creates[0]
    assert message.startswith("allpurpose_misuse: 1 cluster(s) flagged (BI_TOOL_JDBC 0, SCHEDULED_JOB 1, SHARED_USERS 1)")
    assert "access.audit unavailable" in message


@pytest.mark.parametrize("name", sorted(f4.PROBES))
def test_every_probe_reads_limit_zero(name):
    assert f4.PROBES[name].endswith("LIMIT 0")
