"""Fast checks on the sandbox "demo" profile (pure Python, no Spark session).

The demo rows are built once per test session. Aggregates (cost per 30-day month, untagged share,
failure rates) are computed here from the raw rows with a small list-price helper, the way the
engine would, so a generator change that breaks a storyline fails here before it reaches a demo.
"""

from __future__ import annotations

import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sandbox"))

import finops_demo as demo  # noqa: E402
from finops_sandbox import F12_WORKLOADS, SCHEMA_SPEC, WORKLOADS, build_synthetic_rows  # noqa: E402

TODAY = date(2026, 10, 30)
FALLBACK_PRICE = 0.55  # the engine's --fallback-dbu-price default
TEAM_KEYS = ("team", "Team", "TEAM", "department", "group")  # tagging_rules aliases


@pytest.fixture(scope="session")
def rows():
    return build_synthetic_rows(today=TODAY, profile="demo")


@pytest.fixture(scope="session")
def costed(rows):
    """(usage row, cost, month 1..6) for every billing row."""
    prices = defaultdict(list)
    for p in rows["billing_list_prices"]:
        prices[p["sku_name"]].append((p["price_start_time"], p["price_end_time"], float(p["pricing"]["default"])))

    def price(u):
        # Same join as daily_cost_list_prices.sql: start >= price_start, end <= price_end (or open).
        for start, end, value in prices.get(u["sku_name"], []):
            if u["usage_start_time"] >= start and (end is None or u["usage_end_time"] <= end):
                return value
        return FALLBACK_PRICE

    return [(u, float(u["usage_quantity"]) * price(u), _month(u["usage_date"])) for u in rows["billing_usage"]]


def _offset(d: date) -> int:
    return (TODAY - d).days


def _month(d: date) -> int:
    """Month 1 = offsets 180..151, ..., month 6 = offsets 30..1."""
    return (demo.DEMO_DAYS - _offset(d)) // 30 + 1


def _team(u):
    return next((u["custom_tags"][k] for k in TEAM_KEYS if k in u["custom_tags"]), None)


def _monthly(costed, keep=lambda u: True):
    out = defaultdict(float)
    for u, cost, month in costed:
        if keep(u):
            out[month] += cost
    return out


def _meta(key, value):
    return lambda u: u["usage_metadata"][key] == value


# --- profile wiring and the default subset ---------------------------------------------------

def test_default_profile_is_the_default():
    assert build_synthetic_rows(days=3, today=TODAY) == build_synthetic_rows(days=3, today=TODAY, profile="default")
    with pytest.raises(ValueError):
        build_synthetic_rows(days=3, today=TODAY, profile="nope")


def test_demo_contains_default_rows_unchanged(rows):
    # Open F4/F5/F6 work builds on the default scenarios: they must be present byte for byte.
    default = build_synthetic_rows(days=demo.DEMO_DAYS, today=TODAY)
    for table, data in default.items():
        assert rows[table][:len(data)] == data, table
        assert len(rows[table]) > len(data), f"demo adds nothing to {table}"


def test_demo_rows_match_schema(rows):
    for table, data in rows.items():
        names = {n for n, _ in SCHEMA_SPEC[table]}
        nested = {n: t for n, t in SCHEMA_SPEC[table] if isinstance(t, list)}
        for r in data:
            assert set(r) == names, table
            for col, spec in nested.items():
                if r[col] is not None:
                    assert set(r[col]) == {n for n, _ in spec}, (table, col)


def test_ids_are_unique(rows):
    for table, key in (("billing_usage", "record_id"), ("query_history", "statement_id")):
        ids = [r[key] for r in rows[table]]
        assert len(ids) == len(set(ids)), table
    # One job run id per (job, run); task run ids never collide across job runs.
    owner = {}
    for r in rows["lakeflow_job_task_run_timeline"]:
        assert owner.setdefault(r["run_id"], r["job_run_id"]) == r["job_run_id"]


# --- scale -----------------------------------------------------------------------------------

def test_scale(rows, costed):
    assert {r["workspace_id"] for r in rows["billing_usage"]} == set(demo.WORKSPACES.values())
    assert {_team(u) for u in rows["billing_usage"] if _team(u)} >= set(demo.TEAMS)
    assert {w["tags"]["team"] for w in demo.DEMO_WORKLOADS} == set(demo.TEAMS)
    assert 55 <= len(WORKLOADS) + len(F12_WORKLOADS) + len(demo.DEMO_WORKLOADS) <= 70
    dates = {r["usage_date"] for r in rows["billing_usage"]}
    assert len(dates) == 180 and min(dates) == TODAY - timedelta(days=180) and max(dates) == TODAY - timedelta(days=1)
    monthly = _monthly(costed)
    assert sorted(monthly) == [1, 2, 3, 4, 5, 6]
    assert all(38_000 <= v <= 62_000 for v in monthly.values()), monthly


def test_growth_and_ml_onboarding(costed):
    monthly = _monthly(costed)
    assert all(monthly[m + 1] > monthly[m] for m in range(1, 6)), monthly
    assert 1.02 < monthly[2] / monthly[1] < 1.10                        # ~5% before ML arrives
    recsys = _monthly(costed, lambda u: u["custom_tags"].get("project") == "recsys")
    assert recsys[1] == recsys[2] == 0 and recsys[3] > 4_000
    first = min(u["usage_date"] for u, _, _ in costed if u["custom_tags"].get("project") == "recsys")
    assert _offset(first) == demo.ML_START_OFFSET


def test_weekend_dip_for_interactive_bi(costed):
    per_day = defaultdict(float)
    for u, cost, _ in costed:
        if u["usage_metadata"]["warehouse_id"] == "wh-mkt-001":
            per_day[u["usage_date"]] += cost
    weekend = [v for d, v in per_day.items() if d.weekday() >= 5]
    weekday = [v for d, v in per_day.items() if d.weekday() < 5]
    assert 0.5 < (sum(weekend) / len(weekend)) / (sum(weekday) / len(weekday)) < 0.7


def test_month_end_finance_spike(costed):
    by_day = {u["usage_date"]: cost for u, cost, _ in costed if u["usage_metadata"]["job_id"] == "3007"
              and u["usage_date"] >= TODAY - timedelta(days=60)}
    spike = [c for d, c in by_day.items() if demo._is_month_end(d)]
    normal = [c for d, c in by_day.items() if not demo._is_month_end(d)]
    assert spike and sum(spike) / len(spike) > 2.4 * sum(normal) / len(normal)


# --- storylines --------------------------------------------------------------------------------

def test_autoterm_switched_off_in_month_4_then_idle(rows, costed):
    cid = demo.AUTOTERM_OFF_CLUSTER
    history = sorted((r for r in rows["compute_clusters"] if r["cluster_id"] == cid), key=lambda r: r["change_time"])
    assert [r["auto_termination_minutes"] for r in history] == [30, 0]
    assert _month(history[-1]["change_date"]) == 4 and _offset(history[-1]["change_date"]) == demo.AUTOTERM_OFF_OFFSET
    nodes = defaultdict(list)
    for r in rows["compute_node_timeline"]:
        if r["cluster_id"] == cid:
            nodes[_offset(r["start_time"].date()) <= demo.AUTOTERM_OFF_OFFSET].append(
                r["cpu_user_percent"] + r["cpu_system_percent"])
    assert sum(nodes[False]) / len(nodes[False]) > 30          # busy before
    assert sum(nodes[True]) / len(nodes[True]) < 5             # idle after
    after = [u for u in rows["billing_usage"] if u["usage_metadata"]["cluster_id"] == cid
             and _offset(u["usage_date"]) <= demo.AUTOTERM_OFF_OFFSET]
    assert all(u["usage_end_time"] - u["usage_start_time"] == timedelta(hours=24) for u in after)
    monthly = _monthly(costed, _meta("cluster_id", cid))
    assert monthly[6] > 3 * monthly[3]
    # Nobody attaches after the change: it is abandoned, not in use.
    attaches = [r for r in rows["access_audit"] if r["action_name"] == "attachNotebook"
                and r["request_params"].get("clusterId") == cid]
    assert attaches and min(_offset(r["event_date"]) for r in attaches) > demo.AUTOTERM_OFF_OFFSET - 3


def test_bi_traffic_moves_from_cluster_to_warehouse_in_month_5(rows, costed):
    cluster = _monthly(costed, _meta("cluster_id", demo.BI_SOURCE_CLUSTER))
    warehouse = _monthly(costed, _meta("warehouse_id", demo.BI_TARGET_WAREHOUSE))
    assert cluster[6] < 0.15 * cluster[4]                      # cluster cost drops
    assert warehouse[6] > 1.8 * warehouse[4]                   # warehouse rises
    assert cluster[6] + warehouse[6] < 0.85 * (cluster[4] + warehouse[4])   # visible saving
    jdbc = [r for r in rows["access_audit"] if r["action_name"] == "commandSubmit"]
    on_cluster = [r for r in jdbc if r["request_params"].get("clusterId") == demo.BI_SOURCE_CLUSTER]
    on_wh = [r for r in jdbc if r["request_params"].get("warehouseId") == demo.BI_TARGET_WAREHOUSE]
    # On the cluster from the first day until the day before the migration, then on the warehouse.
    assert max(_offset(r["event_date"]) for r in on_cluster) == 180
    assert min(_offset(r["event_date"]) for r in on_cluster) == demo.BI_MIGRATION_OFFSET + 1
    assert on_wh and max(_offset(r["event_date"]) for r in on_wh) == demo.BI_MIGRATION_OFFSET
    assert {r["user_agent"] for r in on_cluster} == {r["user_agent"] for r in on_wh}


def test_untagged_share_falls_from_40_to_10_percent(costed):
    total = _monthly(costed)
    untagged = _monthly(costed, lambda u: _team(u) is None)
    share = {m: untagged[m] / total[m] for m in total}
    assert 0.35 <= share[1] <= 0.45, share
    assert 0.05 <= share[6] <= 0.12, share
    assert all(share[m + 1] < share[m] for m in range(1, 6)), share


def test_failure_storm_in_month_2(rows):
    storm_jobs = {w["job_id"] for w in demo.DEMO_WORKLOADS if w.get("storm")}
    assert len(storm_jobs) == 3
    stats = defaultdict(lambda: [0, 0])
    for r in rows["lakeflow_job_run_timeline"]:
        if r["job_id"] in storm_jobs and r["result_state"]:
            in_storm = _offset(r["period_start_time"].date()) in demo.STORM_OFFSETS
            stats[in_storm][0] += 1
            stats[in_storm][1] += r["result_state"] == "FAILED"
    assert stats[True][1] / stats[True][0] > 0.5
    assert stats[False][1] == 0
    assert {_month(TODAY - timedelta(days=o)) for o in demo.STORM_OFFSETS} == {2}


def test_list_price_change_mid_period(rows, costed):
    windows = sorted((p for p in rows["billing_list_prices"] if p["sku_name"] == demo.SERVERLESS_SQL_SKU),
                     key=lambda p: p["price_start_time"])
    change = datetime.combine(TODAY - timedelta(days=demo.PRICE_CHANGE_OFFSET), datetime.min.time())
    assert [str(p["pricing"]["default"]) for p in windows] == list(demo.SERVERLESS_SQL_PRICES)
    assert windows[0]["price_end_time"] == windows[1]["price_start_time"] == change
    assert windows[1]["price_end_time"] is None
    # Every serverless SQL usage row falls inside exactly one window (no fallback pricing).
    for u, cost, _ in costed:
        if u["sku_name"] == demo.SERVERLESS_SQL_SKU:
            expected = 0.70 if u["usage_end_time"] <= change else 0.77
            assert cost == pytest.approx(float(u["usage_quantity"]) * expected)


# --- waste features: several cases of different size -------------------------------------------

def _current_clusters(rows):
    latest = {}
    for r in sorted(rows["compute_clusters"], key=lambda r: r["change_time"]):
        latest[r["cluster_id"]] = r
    return latest


def test_f4_bi_tools_scheduled_jobs_and_shared_clusters(rows):
    bi_clusters = {r["request_params"]["clusterId"] for r in rows["access_audit"]
                   if r["action_name"] == "commandSubmit" and "clusterId" in r["request_params"]}
    assert bi_clusters >= {"0202-bi-tableau-cluster", demo.BI_SOURCE_CLUSTER, "1203-finance-excel-odbc",
                           "1204-looker-jdbc-cluster"}
    current = _current_clusters(rows)
    runs = defaultdict(set)
    for r in rows["lakeflow_job_run_timeline"]:
        for c in r["compute_ids"]:
            if current[c]["cluster_source"] in ("UI", "API"):
                runs[r["job_id"]].add((r["trigger_type"], r["run_type"]))
    assert runs["3010"] == {("CRON", "JOB_RUN"), ("ONETIME_RETRY", "JOB_RUN")}
    assert runs["3012"] == {("PERIODIC", "JOB_RUN")}
    assert runs["3011"] == {("ONETIME", "SUBMIT_RUN")}
    assert current["1302-salesops-api-cluster"]["cluster_source"] == "API"
    users = defaultdict(set)
    for r in rows["access_audit"]:
        if r["action_name"] == "attachNotebook":
            users[r["request_params"]["clusterId"]].add(r["user_identity"]["email"])
    assert len(users["1101-analytics-shared"]) == 6 and len(users["1102-ds-shared-cluster"]) == 4
    assert len(users["1405-ml-gpu-cluster"]) == 1                  # near-miss: single user


def test_f4_run_shapes_external_orchestrator_repairs_and_run_types(rows):
    runs = rows["lakeflow_job_run_timeline"]
    # External orchestrator: runs/submit with no job_id on an existing all-purpose cluster, with task rows.
    orchestrated = [r for r in runs if r["job_id"] is None]
    assert orchestrated and {(r["trigger_type"], r["run_type"]) for r in orchestrated} == {("ONETIME", "SUBMIT_RUN")}
    assert {c for r in orchestrated for c in r["compute_ids"]} == {"1409-finance-adhoc"}
    assert _current_clusters(rows)["1409-finance-adhoc"]["cluster_source"] == "UI"
    run_ids = {r["run_id"] for r in orchestrated}
    task_parents = {t["job_run_id"] for t in rows["lakeflow_job_task_run_timeline"] if t["job_id"] is None}
    assert task_parents == run_ids
    assert not any(r["usage_metadata"]["job_id"] == demo.ORCHESTRATOR_JOB for r in rows["billing_usage"])
    # ONETIME_RETRY repairs: of a CRON job on all-purpose (counts) and of a manual job (does not).
    repairs = defaultdict(int)
    for r in runs:
        if r["trigger_type"] == "ONETIME_RETRY" and r["result_state"]:
            repairs[r["job_id"]] += 1
    assert repairs == {"3010": 2 * len(demo.REPAIR_OFFSETS), "3018": len(demo.REPAIR_OFFSETS)}
    jobs = {j["job_id"]: j["trigger_type"] for j in rows["lakeflow_jobs"]}
    assert jobs["3010"] == "CRON" and jobs["3018"] is None
    repaired = {r["run_id"][:-1] for r in runs if r["trigger_type"] == "ONETIME_RETRY"}
    assert all(r["result_state"] in (None, "FAILED") for r in runs if r["run_id"] in repaired)
    # run_type values beyond JOB_RUN.
    assert {r["run_type"] for r in runs} == {"JOB_RUN", "SUBMIT_RUN", "WORKFLOW_RUN"}


def test_query_history_on_classic_clusters(rows):
    on_clusters = [r for r in rows["query_history"] if r["compute"]["cluster_id"]]
    assert {r["compute"]["cluster_id"] for r in on_clusters} == {
        demo.BI_SOURCE_CLUSTER, "1204-looker-jdbc-cluster", "1203-finance-excel-odbc"}
    assert all(r["compute"]["type"] == "CLUSTER" and r["compute"]["warehouse_id"] is None for r in on_clusters)
    assert {r["client_driver"] for r in on_clusters} == {"Databricks JDBC Driver", "Databricks ODBC Driver"}
    # 1202's cluster queries stop with the migration.
    assert min(_offset(r["start_time"].date()) for r in on_clusters
               if r["compute"]["cluster_id"] == demo.BI_SOURCE_CLUSTER) > demo.BI_MIGRATION_OFFSET


def test_f5_auto_termination_cases(rows):
    current = _current_clusters(rows)
    autoterm = {c: current[c]["auto_termination_minutes"] for c in current}
    assert autoterm["1405-ml-gpu-cluster"] == 120 and autoterm["1408-dev-sandbox-autoterm-90"] == 90  # flagged
    assert autoterm["1406-analytics-autoterm-60"] == 60 and autoterm["1101-analytics-shared"] == 60    # boundary
    assert autoterm["1407-platform-ops-autoterm-off"] == 0 and autoterm[demo.AUTOTERM_OFF_CLUSTER] == 0
    cpu = defaultdict(list)
    for r in rows["compute_node_timeline"]:
        cpu[r["cluster_id"]].append(r["cpu_user_percent"] + r["cpu_system_percent"])
    avg = {c: sum(v) / len(v) for c, v in cpu.items()}
    assert avg["1407-platform-ops-autoterm-off"] > 60               # off but busy: near-miss
    assert avg["1405-ml-gpu-cluster"] < 15 and avg["1408-dev-sandbox-autoterm-90"] < 10


def test_f6_retries_and_failure_heavy(rows):
    attempts = defaultdict(set)
    for r in rows["lakeflow_job_task_run_timeline"]:
        if r["job_id"] == "3013":
            attempts[(r["job_run_id"], r["task_key"])].add(r["run_id"])
    retried = {jr for (jr, _), ids in attempts.items() if len(ids) > 1}
    assert len(retried) > 100
    states = [r["result_state"] for r in rows["lakeflow_job_run_timeline"] if r["job_id"] == "3009" and r["result_state"]]
    assert 0.2 < states.count("FAILED") / len(states) < 0.4


def test_f3_shared_warehouses_span_catalogs(rows):
    import re

    three_part = re.compile(r"(?<![\w.`])`?([A-Za-z_]\w*)`?\s*\.\s*`?\w+`?\s*\.\s*`?\w+`?")
    catalogs = defaultdict(set)
    for r in rows["query_history"]:
        found = {m.group(1).lower() for m in three_part.finditer(r["statement_text"])}
        catalogs[r["compute"]["warehouse_id"]] |= found or {"UNATTRIBUTED"}
    assert catalogs["wh-analytics-shared"] == {"sales", "marketing", "finance", "product"}
    assert {"ml", "sales", "product", "UNATTRIBUTED"} <= catalogs["wh-ds-adhoc"]
    assert catalogs[demo.BI_TARGET_WAREHOUSE] == {"sales", "sales_ops"}
    assert catalogs["wh-exec-dash"] == {"finance", "sales"}
    assert catalogs["wh-mkt-001"] == {"marketing"}                   # near-miss
    ws = {r["compute"]["warehouse_id"]: r["workspace_id"] for r in rows["query_history"]}
    assert ws["wh-analytics-shared"] == demo.ANALYTICS_WS


# --- determinism -----------------------------------------------------------------------------

def test_demo_is_deterministic_and_date_relative(rows):
    assert build_synthetic_rows(today=TODAY, profile="demo") == rows
    later = build_synthetic_rows(days=10, today=TODAY + timedelta(days=7), profile="demo")
    assert max(r["usage_date"] for r in later["billing_usage"]) == TODAY + timedelta(days=6)
    change = [p["price_start_time"] for p in later["billing_list_prices"] if p["price_end_time"] is None
              and p["sku_name"] == demo.SERVERLESS_SQL_SKU]
    assert change == [datetime.combine(TODAY + timedelta(days=7 - demo.PRICE_CHANGE_OFFSET), datetime.min.time())]


def test_workflow_run_child_uses_existing_cluster(rows):
    runs = [r for r in rows["lakeflow_job_run_timeline"] if r["job_id"] == demo.WORKFLOW_RUN_JOB]
    assert runs and {(r["trigger_type"], r["run_type"]) for r in runs} == {("ONETIME", "WORKFLOW_RUN")}
    assert {c for r in runs for c in r["compute_ids"]} == {"1101-analytics-shared"}
    assert not any(j["job_id"] == demo.WORKFLOW_RUN_JOB for j in rows["lakeflow_jobs"])
    assert not any(u["usage_metadata"]["job_id"] == demo.WORKFLOW_RUN_JOB for u in rows["billing_usage"])


def test_scd_change_rows_are_stamped_at_midnight(rows):
    demo_clusters = {w["cluster"] for w in demo.DEMO_WORKLOADS if w["kind"] == "CLUSTER"}
    by_cluster = defaultdict(list)
    for r in rows["compute_clusters"]:
        if r["cluster_id"] in demo_clusters:
            by_cluster[r["cluster_id"]].append(r)
    changes = [r for history in by_cluster.values() for r in sorted(history, key=lambda r: r["change_time"])[1:]]
    assert changes and all(r["change_time"].time() == datetime.min.time() for r in changes)


# --- run_local.py arguments --------------------------------------------------------------------

@pytest.mark.parametrize("argv, profile, days, lookback", [
    ([], "default", None, 30),
    (["--profile", "demo"], "demo", None, 180),
    (["--days", "60", "--profile", "demo"], "demo", 60, 60),
    (["--days", "10"], "default", 10, 10),
    (["--profile", "demo", "--lookback-days", "30"], "demo", None, 30),
])
def test_run_local_arguments(argv, profile, days, lookback):
    import run_local

    args = run_local.parse_args(argv)
    assert (args.profile, args.days, run_local.resolve_lookback(args)) == (profile, days, lookback)


@pytest.mark.parametrize("argv", [["--days", "0"], ["--lookback-days", "-5"], ["--profile", "nope"]])
def test_run_local_rejects_bad_arguments(argv):
    import run_local

    with pytest.raises(SystemExit):
        run_local.parse_args(argv)
