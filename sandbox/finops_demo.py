"""Demo profile for the synthetic sandbox: six months of company-scale data with storylines.

``build_demo_rows`` returns the default profile's rows for the same window, unchanged and
first in every table, followed by 48 extra workloads across three workspaces and eight
teams. The extra rows come from their own seeded RNG streams, so the default scenarios (and
the open F4/F5/F6 work built on them) look exactly as they do in the default profile.

Everything is a function of ``(days, seed, today)``. Storylines are anchored in days before
``today`` (an "offset"), so they land on the same relative dates on every run. With the
default 180 days, "month N" is offsets ``180 - 30(N-1)`` down to ``151 - 30(N-1)``:

| Storyline | Offsets (days before today) | Month |
| --- | --- | --- |
| Growth ~5% per month on every demo workload | all | 1-6 |
| Weekends ~40% lower for interactive / BI compute | Saturdays and Sundays | 1-6 |
| Month-end finance spikes (last 3 calendar days) | calendar month ends | 1-6 |
| Failure storm on 3 ingest jobs (vendor API outage) | 140 .. 134 | 2 |
| ML recommender squad onboards (5 workloads) | from 120 | 3 |
| Serverless SQL list price 0.70 -> 0.77 USD/DBU | from 90 | 4 |
| ``1404-ds-sandbox-cluster`` auto-termination 30 -> 0, then idle 24/7 | from 80 | 4 |
| BI JDBC/ODBC traffic moves from ``1202-bi-powerbi-cluster`` to ``wh-bi-serverless-01`` | from 55 | 5 |
| Tagging campaign: workloads gain tags one by one, untagged share ~41% -> ~8% | 155 .. 35 | 1-5 |

See sandbox/README.md ("Demo profile") for the scenario -> feature map.
"""

from __future__ import annotations

import calendar
import random
import zlib
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from finops_sandbox import (
    ACCOUNT_ID, BROWSER_AGENT, CLOUD, DEV_WS, PROD_WS, QUERY_TEMPLATES, SYSTEM_TABLE_MAP,
    _audit_row, _cluster_row, _emit_workload, _query_row, _timeline_rows, _usage_row, build_synthetic_rows,
)

DEMO_DAYS = 180
ANALYTICS_WS = "3333333333333333"
WORKSPACES = {"prod": PROD_WS, "dev": DEV_WS, "analytics": ANALYTICS_WS}

# --- Storyline anchors (days before today) ---------------------------------------------------
GROWTH_PER_MONTH = 0.05          # demo workloads' DBUs grow 5% per 30 days (levels below are "today")
DBU_SCALE = 0.7                  # calibrates total spend to ~$40k (month 1) .. ~$57k (month 6)
WEEKEND_FACTOR = 0.6             # interactive / BI usage on Saturday and Sunday
MONTH_END_DAYS = 3               # finance spikes on the last 3 calendar days of each month
STORM_OFFSETS = frozenset(range(134, 141))  # month 2, one week: vendor API outage
STORM_FAILURE_RATE = 0.7
ML_START_OFFSET = 120            # month 3 starts: ML recommender squad's workloads begin
PRICE_CHANGE_OFFSET = 90         # month 4 starts (mid-period): serverless SQL list price rises
AUTOTERM_OFF_OFFSET = 80         # month 4: 1404's auto-termination switched to 0
BI_MIGRATION_OFFSET = 55         # month 5: BI JDBC/ODBC moves from 1202 to wh-bi-serverless-01
EXISTING_OFFSET = 400            # create date of workloads that predate the window

SERVERLESS_SQL_SKU = "PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST"
SERVERLESS_SQL_PRICES = ("0.70", "0.77")  # before / from PRICE_CHANGE_OFFSET

BI_SOURCE_CLUSTER = "1202-bi-powerbi-cluster"
BI_TARGET_WAREHOUSE = "wh-bi-serverless-01"
AUTOTERM_OFF_CLUSTER = "1404-ds-sandbox-cluster"
SUBMIT_RUN_JOBS = {"3011"}        # runs created through POST /jobs/runs/submit (run_type SUBMIT_RUN)
ORCHESTRATOR_JOB = "4001"        # external orchestrator: SUBMIT_RUN with no job_id (key only builds run ids)
WORKFLOW_RUN_JOB = "3016"        # notebook-workflow child run (run_type WORKFLOW_RUN)
NO_JOB_DEFINITION = SUBMIT_RUN_JOBS | {ORCHESTRATOR_JOB, WORKFLOW_RUN_JOB}  # no lakeflow.jobs row
REPAIR_OFFSETS = (20, 47, 75)    # days a run fails and is repaired with an ONETIME_RETRY run

TEAMS = {  # team -> (cost_center, owner)
    "data-eng": ("CC-100", "alice@example.com"),
    "ml": ("CC-110", "bob@example.com"),
    "data-science": ("CC-120", "nina@example.com"),
    "finance": ("CC-200", "erin@example.com"),
    "analytics": ("CC-300", "ivan@example.com"),
    "marketing": ("CC-400", "mia@example.com"),
    "platform": ("CC-500", "pat@example.com"),
    "sales-ops": ("CC-600", "rachel@example.com"),
}


def _tags(team: str, project: str, env: str = "prod", owner: str | None = None, **extra: str) -> dict[str, str]:
    cost_center, default_owner = TEAMS[team]
    return {"project": project, "team": team, "owner": owner or default_owner, "environment": env,
            "cost_center": cost_center, **extra}


_J, _AP, _DLT, _SN = ("PREMIUM_JOBS_COMPUTE", "PREMIUM_ALL_PURPOSE_COMPUTE",
                      "PREMIUM_DLT_ADVANCED_COMPUTE", "PREMIUM_SERVERLESS_COMPUTE_US_EAST")
_SQL_PRO = "PREMIUM_SQL_PRO_COMPUTE_US_EAST"


def _job(job_id, name, team, project, dbus, runs=1, ws=PROD_WS, cpu=55, mem=50, **kw) -> dict[str, Any]:
    w = {"kind": "JOB", "workspace_id": ws, "job_id": job_id, "name": name, "sku": _J, "product": "JOBS",
         "cluster": f"job-{job_id}-cluster", "daily_dbus": dbus, "runs_per_day": runs, "failure_rate": 0.0,
         "cpu": cpu, "mem": mem, "tags": _tags(team, project, "dev" if ws == DEV_WS else "prod"),
         "run_as": f"svc-{team}@example.com"}
    w.update(kw)
    return w


def _cluster(cluster, team, project, dbus, users, ws=PROD_WS, cpu=30, mem=40, auto_term=30, **kw) -> dict[str, Any]:
    w = {"kind": "CLUSTER", "workspace_id": ws, "cluster": cluster, "sku": _AP, "product": "ALL_PURPOSE",
         "daily_dbus": dbus, "cpu": cpu, "mem": mem, "auto_term": auto_term, "interactive": True,
         "billing_run_as": False,  # classic all-purpose billing leaves run_as empty (see README)
         "tags": _tags(team, project, "dev" if ws == DEV_WS else "prod"), "run_as_pool": users}
    w.update(kw)
    return w


def _flat(kind, key, value, team, project, dbus, sku, product, ws=PROD_WS, **kw) -> dict[str, Any]:
    w = {"kind": kind, "workspace_id": ws, key: value, "sku": sku, "product": product, "daily_dbus": dbus,
         "tags": _tags(team, project), "run_as": None}
    w.update(kw)
    return w


# Each entry is one workload. ``daily_dbus`` is the level *today*; growth scales older days down.
# Storyline keys: start (first offset it exists), untagged_until (offset tags are applied; before
# that it carries pre_tags), interactive (weekend dip), month_end (factor), storm, story.
DEMO_WORKLOADS: list[dict[str, Any]] = [
    # --- data-eng (prod) ---------------------------------------------------------------------
    # F1/F8 bulk spend, healthy. F4 near-miss: CRON jobs on job clusters.
    _job("3001", "orders_ingest", "data-eng", "orders", 520, runs=4, cpu=62, mem=55),
    _job("3002", "inventory_snapshot", "data-eng", "inventory", 300, cpu=58, mem=60,
         untagged_until=150, pre_tags={"project": "inventory"}),      # F2: partial -> GOOD in month 2
    _job("3003", "clickstream_sessionize", "data-eng", "clickstream", 640, runs=2, cpu=66, mem=62),
    # F6 failure storm (month 2): vendor outage, failures spike to ~70% for one week, transform retried.
    _job("3004", "supplier_feed_ingest", "data-eng", "supplier_feeds", 200, runs=2, storm=True),
    _job("3005", "payments_ingest", "data-eng", "payments", 240, runs=3, storm=True),
    _job("3006", "crm_contacts_sync", "sales-ops", "crm", 150, runs=2, storm=True,
         untagged_until=100, pre_tags={}),                            # F2: untagged until month 3
    # F6 near-miss: long transform task sliced across hours (one run_id, not a retry).
    _job("3014", "data_quality_checks", "platform", "dq", 100, runs=4, long_task="transform"),
    _flat("PIPELINE", "pipeline", "pl-clickstream-dlt", "data-eng", "clickstream", 480, _DLT, "DLT"),
    _flat("PIPELINE", "pipeline", "pl-inventory-dlt", "data-eng", "inventory", 240, _DLT, "DLT",
          untagged_until=45, pre_tags={"environment": "prod"}),     # F2: partial until month 5
    _flat("NOTEBOOK", "notebook", "nb-de-scratch", "data-eng", "scratch", 40, _SN, "INTERACTIVE",
          interactive=True, run_as="alice@example.com", notebook_path="/Users/alice@example.com/scratch"),

    # --- finance (prod): month-end close spikes ----------------------------------------------
    _job("3007", "gl_close_batch", "finance", "gl_close", 260, month_end=3.0),
    _job("3008", "revenue_recognition", "finance", "rev_rec", 160, month_end=2.0,
         untagged_until=120, pre_tags={"project": "rev_rec"}),       # F2: partial until month 3
    _flat("PIPELINE", "pipeline", "pl-finance-cdc-dlt", "finance", "ledger_cdc", 190, _DLT, "DLT"),
    # F4 BI_TOOL_JDBC positive (small): Excel over ODBC on an all-purpose cluster.
    _cluster("1203-finance-excel-odbc", "finance", "fpa_excel", 65, ["sophie@example.com"],
             cpu=22, mem=35, auto_term=30, month_end=2.0),
    # F5 near-miss: busy enough (CPU 40%) and auto-termination 20.
    _cluster("1409-finance-adhoc", "finance", "fpa_adhoc", 55, ["sophie@example.com"], cpu=40, mem=45,
             auto_term=20, untagged_until=100, pre_tags={}),
    _flat("SQL_WAREHOUSE", "warehouse", "wh-exec-dash", "finance", "exec_dashboards", 110, _SQL_PRO, "SQL",
          interactive=True, month_end=2.0),                         # F3 positive: finance + sales
    _flat("NOTEBOOK", "notebook", "nb-finance-forecast", "finance", "forecast", 32, _SN, "INTERACTIVE",
          interactive=True, run_as="sophie@example.com", notebook_path="/Users/sophie@example.com/forecast",
          untagged_until=85, pre_tags={}),

    # --- marketing ---------------------------------------------------------------------------
    # F6 failure-heavy (~30% every week). F2: untagged until month 2.
    _job("3009", "marketing_attribution", "marketing", "attribution", 320, runs=2, failure_rate=0.3,
         untagged_until=130, pre_tags={}),
    # F4 SCHEDULED_JOB positive: CRON job on an all-purpose (UI) cluster, billed to the cluster.
    _job("3010", "daily_marketing_export", "marketing", "exports", 95, runs=2, sku=_AP, product="ALL_PURPOSE",
         cluster="1301-mkt-allpurpose", cluster_source="UI", auto_term=30, cluster_name="mkt-shared-ap",
         untagged_until=90, pre_tags={"project": "exports"},
         repair=True),  # F4 positive: its ONETIME_RETRY repair runs on REPAIR_OFFSETS count as scheduled
    # F4 BI_TOOL_JDBC positive (medium): Looker JDBC on an all-purpose cluster. F2: untagged until month 5.
    _cluster("1204-looker-jdbc-cluster", "marketing", "looker", 110, ["svc-looker@example.com"],
             ws=ANALYTICS_WS, cpu=28, mem=35, untagged_until=35, pre_tags={}),
    _cluster("1410-mkt-adhoc", "marketing", "campaign_analysis", 48, ["mia@example.com"], ws=ANALYTICS_WS,
             cpu=35, mem=40, untagged_until=60, pre_tags={}),
    # F3 near-miss: marketing-only warehouse.
    _flat("SQL_WAREHOUSE", "warehouse", "wh-mkt-001", "marketing", "campaign_bi", 60, SERVERLESS_SQL_SKU, "SQL",
          ws=ANALYTICS_WS, interactive=True),

    # --- sales-ops ---------------------------------------------------------------------------
    # F4 SCHEDULED_JOB positive via API: runs/submit (SUBMIT_RUN, ONETIME) on an API-created
    # all-purpose cluster, four times a day. F2: never tagged (legacy integration).
    _job("3011", "crm_api_push", "sales-ops", "crm", 80, runs=4, sku=_AP, product="ALL_PURPOSE",
         cluster="1302-salesops-api-cluster", cluster_source="API", auto_term=15, trigger="ONETIME",
         cluster_name="crm-api-cluster", untagged_until=0, pre_tags={}),
    # F6 RETRY_HEAVY positive: about half of the runs retry transform.
    _job("3013", "lead_scoring", "sales-ops", "lead_scoring", 130, runs=3, task_retries=True,
         untagged_until=70, pre_tags={"project": "lead_scoring"}),
    # Storyline + F4 BI_TOOL_JDBC: Power BI ODBC / Tableau JDBC on an all-purpose cluster until
    # month 5, then the traffic moves to wh-bi-serverless-01 and the cluster drops to ~8%.
    _cluster(BI_SOURCE_CLUSTER, "sales-ops", "sales_bi", 230, ["svc-powerbi@example.com", "rachel@example.com"],
             cpu=33, mem=40, auto_term=45, story="bi_migration"),
    _flat("SQL_WAREHOUSE", "warehouse", BI_TARGET_WAREHOUSE, "sales-ops", "sales_bi", 95, SERVERLESS_SQL_SKU,
          "SQL", interactive=True, story="bi_target"),               # F3 positive: sales + sales_ops
    _flat("NOTEBOOK", "notebook", "nb-sales-ops-adhoc", "sales-ops", "adhoc", 24, _SN, "INTERACTIVE",
          interactive=True, run_as="rachel@example.com", notebook_path="/Users/rachel@example.com/adhoc",
          untagged_until=0, pre_tags={}),                            # F2: never tagged

    # --- analytics (analytics workspace) -----------------------------------------------------
    # F4 near-miss: run_type WORKFLOW_RUN. An analyst's notebook on 1101 calls dbutils.notebook.run;
    # the child run uses the parent's (existing all-purpose) cluster, is not scheduled (ONETIME) and
    # has no lakeflow.jobs definition. Billed to the cluster, so no billing rows of its own.
    # ASSUMPTION: the child run carries a job_id (an ephemeral one) in the timelines. Check on a real account.
    _job("3016", "exec_kpi_rollup", "analytics", "kpi", 0, ws=ANALYTICS_WS, run_type="WORKFLOW_RUN",
         trigger="ONETIME", sku=_AP, product="ALL_PURPOSE", cluster="1101-analytics-shared", cluster_source="UI",
         existing_cluster=True, bill=False, cpu=None, mem=None, run_as="ivan@example.com"),
    # F4 SHARED_USERS positive (6 users). F5 near-miss boundary: auto-termination exactly 60.
    _cluster("1101-analytics-shared", "analytics", "self_service", 250,
             ["ivan@example.com", "liam@example.com", "oscar@example.com", "tara@example.com",
              "victor@example.com", "wendy@example.com"], ws=ANALYTICS_WS, cpu=32, mem=45, auto_term=60,
             untagged_until=110, pre_tags={"environment": "prod"}),
    # F5 near-miss boundary: auto-termination exactly 60 and low CPU (8%): not flagged on autoterm.
    _cluster("1406-analytics-autoterm-60", "analytics", "adhoc", 45, ["tara@example.com"], ws=ANALYTICS_WS,
             cpu=8, mem=25, auto_term=60),
    # F3 positive: four catalogs (sales, marketing, finance, product) plus edge cases.
    _flat("SQL_WAREHOUSE", "warehouse", "wh-analytics-shared", "analytics", "self_service", 330,
          SERVERLESS_SQL_SKU, "SQL", ws=ANALYTICS_WS, interactive=True,
          untagged_until=155, pre_tags={"Project": "self_service"}),   # F2: alias key only until month 1 end

    # --- data-science (analytics workspace) --------------------------------------------------
    # HIGH_COST_LOW_UTILIZATION (CPU 9%).
    _job("3015", "customer_churn_features", "data-science", "churn", 220, ws=ANALYTICS_WS, cpu=9, mem=25,
         untagged_until=60, pre_tags={}),
    # F4 SHARED_USERS positive (4 users). F2: never tagged, the largest untagged item at the end.
    _cluster("1102-ds-shared-cluster", "data-science", "research", 200,
             ["nina@example.com", "quinn@example.com", "sam@example.com", "yara@example.com"],
             ws=ANALYTICS_WS, cpu=26, mem=40, auto_term=45, untagged_until=0, pre_tags={}),
    # Storyline + F5 positive: auto-termination 30 -> 0 in month 4, then idle around the clock.
    _cluster(AUTOTERM_OFF_CLUSTER, "data-science", "sandbox", 34, ["sam@example.com"], ws=ANALYTICS_WS,
             cpu=45, mem=40, auto_term=30, story="autoterm_off", idle_dbus=150),
    # F3 positive: ml + sales + product, with two-part names (UNATTRIBUTED).
    _flat("SQL_WAREHOUSE", "warehouse", "wh-ds-adhoc", "data-science", "research_sql", 120, SERVERLESS_SQL_SKU,
          "SQL", ws=ANALYTICS_WS, interactive=True, untagged_until=50, pre_tags={}),

    # --- platform ----------------------------------------------------------------------------
    # F4 SCHEDULED_JOB positive (small): PERIODIC trigger on an all-purpose cluster.
    _job("3012", "platform_audit_export", "platform", "audit_export", 30, sku=_AP, product="ALL_PURPOSE",
         cluster="1303-platform-ops", cluster_source="UI", auto_term=60, trigger="PERIODIC",
         cluster_name="platform-ops-ap"),
    # F5 near-miss: auto-termination off and always on, but busy (CPU 68%).
    _cluster("1407-platform-ops-autoterm-off", "platform", "ops_streaming", 95, ["svc-platform@example.com"],
             cpu=68, mem=60, auto_term=0, always_on=True, interactive=False),
    # Dev workspace.
    _job("3201", "dev_integration_tests", "platform", "ci", 100, runs=2, ws=DEV_WS, failure_rate=0.08),
    _job("3202", "dev_dbt_build", "data-eng", "dbt", 70, ws=DEV_WS, untagged_until=65, pre_tags={}),
    # F5 positive (small): dev cluster, auto-termination 90, CPU 5%. F2: never tagged.
    _cluster("1408-dev-sandbox-autoterm-90", "platform", "dev_sandbox", 40, ["kim@example.com"], ws=DEV_WS,
             cpu=5, mem=18, auto_term=90, untagged_until=0, pre_tags={}),
    # F2: legacy job nobody owns, never tagged.
    _job("3017", "legacy_report_mailer", "data-eng", "legacy", 110, untagged_until=0, pre_tags={}),

    # --- F4 run shapes (coordinator request) -------------------------------------------------
    # F4 positive: an external orchestrator (Airflow) calls runs/submit against the existing
    # finance all-purpose cluster: run_type SUBMIT_RUN, trigger ONETIME, job_id NULL in both
    # timelines. Billed to the cluster already, so no billing rows of its own.
    _job(ORCHESTRATOR_JOB, "airflow__finance_extract", "finance", "fpa_adhoc", 0, runs=2, sku=_AP,
         product="ALL_PURPOSE", cluster="1409-finance-adhoc", cluster_source="UI", existing_cluster=True,
         bill=False, trigger="ONETIME", cpu=None, mem=None, run_as="svc-airflow@example.com"),
    # F4 near-miss: a manual (no schedule) job on the same cluster, every 14 days. Its failed runs on
    # REPAIR_OFFSETS get an ONETIME_RETRY repair run, which must NOT count as scheduled.
    _job("3018", "finance_restatement", "finance", "restatement", 0, sku=_AP, product="ALL_PURPOSE",
         cluster="1409-finance-adhoc", cluster_source="UI", existing_cluster=True, bill=False,
         trigger="ONETIME", every_n_days=1, cpu=None, mem=None, run_as="sophie@example.com",
         active_offsets=frozenset(o for o in range(1, 400) if o % 14 == 0) | frozenset(REPAIR_OFFSETS)),

    # --- ml: the recommender squad onboards in month 3 (all tagged from day one) -------------
    _job("3101", "recsys_training", "ml", "recsys", 900, cpu=72, mem=65, start=ML_START_OFFSET),
    _job("3102", "recsys_batch_scoring", "ml", "recsys", 300, runs=2, start=ML_START_OFFSET),
    # F5 positive (large): GPU dev cluster with auto-termination 120 and CPU 9%.
    _cluster("1405-ml-gpu-cluster", "ml", "recsys", 190, ["uma@example.com"], ws=DEV_WS, cpu=9, mem=30,
             auto_term=120, start=ML_START_OFFSET),
    _flat("PIPELINE", "pipeline", "pl-ml-features-dlt", "ml", "recsys", 130, _DLT, "DLT", start=ML_START_OFFSET),
    _flat("NOTEBOOK", "notebook", "nb-ml-exploration", "ml", "recsys", 45, _SN, "INTERACTIVE",
          interactive=True, run_as="uma@example.com", notebook_path="/Users/uma@example.com/recsys",
          start=ML_START_OFFSET),
]

# Usage on the BI cluster after the migration (residual ad hoc use) and the extra warehouse DBUs
# the migrated dashboards add. Serverless SQL serves the same dashboards with far fewer DBUs.
BI_RESIDUAL_FACTOR = 0.08
BI_MIGRATED_DBUS = 85


def _is_month_end(day: date) -> bool:
    return day.day > calendar.monthrange(day.year, day.month)[1] - MONTH_END_DAYS


def _midnight(day: date) -> datetime:
    return datetime.combine(day, datetime.min.time())


def demo_tags_on(w: dict[str, Any], offset: int) -> dict[str, str]:
    """Tags a workload carries ``offset`` days before today (the tagging-campaign storyline)."""
    until = w.get("untagged_until")
    if until is not None and offset > until:
        return dict(w.get("pre_tags") or {})
    return dict(w["tags"])


def _day_view(w: dict[str, Any], day: date, offset: int) -> dict[str, Any] | None:
    """The workload as it is on ``day``, with every storyline applied; None before it exists."""
    if offset > w.get("start", EXISTING_OFFSET):
        return None
    if "active_offsets" in w and offset not in w["active_offsets"]:
        return None
    v = dict(w, tags=demo_tags_on(w, offset))
    if (w.get("repair") or w.get("job_id") == "3018") and offset in REPAIR_OFFSETS:
        v["failure_rate"] = 1.0  # fails, then gets a repair run (_repair_rows)
    dbus = w["daily_dbus"]
    story = w.get("story")
    if story == "autoterm_off" and offset <= AUTOTERM_OFF_OFFSET:
        # Switched to "never terminate" and then forgotten: on 24h, CPU 3%.
        v.update(always_on=True, cpu=3, mem=10, interactive=False)
        dbus = w["idle_dbus"]
    elif story == "bi_migration" and offset <= BI_MIGRATION_OFFSET:
        dbus *= BI_RESIDUAL_FACTOR
        v.update(cpu=12, mem=25)
    elif story == "bi_target" and offset <= BI_MIGRATION_OFFSET:
        dbus += BI_MIGRATED_DBUS
    if w.get("storm") and offset in STORM_OFFSETS:
        v.update(failure_rate=STORM_FAILURE_RATE, task_retries=True)
    scale = DBU_SCALE * (1 + GROWTH_PER_MONTH) ** (-offset / 30)
    if v.get("interactive") and day.weekday() >= 5:
        scale *= WEEKEND_FACTOR
    if w.get("month_end") and _is_month_end(day):
        scale *= w["month_end"]
    v["daily_dbus"] = dbus * scale
    return v


def _emit_flat(rng: random.Random, out: dict[str, list], v: dict[str, Any], day: date) -> None:
    """Warehouses, DLT pipelines and serverless notebooks: one billing row per day."""
    midnight = _midnight(day)
    kind = v["kind"]
    meta: dict[str, Any] = {}
    if kind == "SQL_WAREHOUSE":
        start, end = midnight + timedelta(hours=7), midnight + timedelta(hours=20)
        meta["warehouse_id"] = v["warehouse"]
    elif kind == "PIPELINE":  # continuous DLT pipelines run around the clock
        start, end = midnight, midnight + timedelta(hours=24)
        meta["dlt_pipeline_id"] = v["pipeline"]
    else:
        start, end = midnight + timedelta(hours=9), midnight + timedelta(hours=17)
        meta.update(notebook_id=v["notebook"], notebook_path=v["notebook_path"])
    jitter = rng.uniform(0.85, 1.15)
    out["billing_usage"].append(_usage_row(rng, v, day, start, end, v["daily_dbus"] * jitter, v.get("run_as"),
                                           **meta))


def _price_row(sku: str, price: str, start: datetime, end: datetime | None) -> dict[str, Any]:
    p = Decimal(price)
    return {"price_start_time": start, "price_end_time": end, "account_id": ACCOUNT_ID, "sku_name": sku,
            "cloud": CLOUD, "currency_code": "USD", "usage_unit": "DBU",
            "pricing": {"default": p, "promotional": {"default": p}, "effective_list": {"default": p}}}


def _demo_price_rows(today: date) -> list[dict[str, Any]]:
    # Effective windows: the old price ends exactly where the new one starts (end is exclusive).
    change = _midnight(today - timedelta(days=PRICE_CHANGE_OFFSET))
    old, new = SERVERLESS_SQL_PRICES
    return [_price_row(SERVERLESS_SQL_SKU, old, datetime(2023, 1, 1), change),
            _price_row(SERVERLESS_SQL_SKU, new, change, None)]


def _at(today: date, offset: int, hour: int = 10, minute: int = 0) -> datetime:
    return _midnight(today - timedelta(days=offset)) + timedelta(hours=hour, minutes=minute)


def _demo_cluster_rows(today: date) -> list[dict[str, Any]]:
    """compute.clusters SCD rows: created, tagged (campaign), and 1404's auto-termination change.

    Change rows are stamped at midnight of the change day, because that whole day's billing and
    node rows already reflect the new state (see _day_view)."""
    rows = []
    for w in DEMO_WORKLOADS:
        if not w.get("cluster") or w.get("existing_cluster"):
            continue
        created_offset = w.get("start", EXISTING_OFFSET) + 1
        created = _at(today, created_offset, 9)
        extra = {"create_time": created}
        if w.get("cluster_name"):
            extra["cluster_name"] = w["cluster_name"]
        rows.append(_cluster_row(w, w.get("auto_term"), created, tags=demo_tags_on(w, created_offset), **extra))
        until = w.get("untagged_until")
        if until:  # tags applied by the campaign (0 = never tagged)
            rows.append(_cluster_row(w, w.get("auto_term"), _at(today, until, 0), tags=dict(w["tags"]), **extra))
        if w.get("story") == "autoterm_off":
            rows.append(_cluster_row(w, 0, _at(today, AUTOTERM_OFF_OFFSET, 0), tags=dict(w["tags"]), **extra))
    return rows


def _demo_job_rows(today: date) -> list[dict[str, Any]]:
    rows = []
    for w in DEMO_WORKLOADS:
        # ASSUMPTION: runs/submit one-time runs have a job_id but no lakeflow.jobs definition.
        if w["kind"] != "JOB" or w["job_id"] in NO_JOB_DEFINITION:
            continue
        trigger = w.get("trigger", "CRON")
        created_offset = w.get("start", EXISTING_OFFSET) + 1
        base = {"account_id": ACCOUNT_ID, "workspace_id": w["workspace_id"], "job_id": w["job_id"],
                "name": w["name"], "description": None, "creator_id": "456", "delete_time": None,
                "run_as": w.get("run_as"), "trigger_type": None if trigger == "ONETIME" else trigger,
                "paused": False}
        rows.append({**base, "tags": demo_tags_on(w, created_offset), "change_time": _at(today, created_offset, 9)})
        if w.get("untagged_until"):
            rows.append({**base, "tags": dict(w["tags"]), "change_time": _at(today, w["untagged_until"], 0)})
    return rows


# --- access.audit ----------------------------------------------------------------------------

# (cluster, workspace, [(user, user_agent)]): BI tools on all-purpose compute (F4 BI_TOOL_JDBC).
# Same ASSUMPTION as the default profile: commandSubmit with request_params.clusterId.
DEMO_BI_CLIENTS = [
    (BI_SOURCE_CLUSTER, PROD_WS, [("svc-powerbi@example.com", "Simba Spark ODBC Driver/2.8.2 (PowerBI Desktop)"),
                                  ("rachel@example.com", "SimbaSparkJDBCDriver/2.6.36 (Tableau 2024.2)")]),
    ("1203-finance-excel-odbc", PROD_WS, [("sophie@example.com", "Simba Spark ODBC Driver/2.8.2 (Microsoft Excel)")]),
    ("1204-looker-jdbc-cluster", ANALYTICS_WS, [("svc-looker@example.com", "SimbaSparkJDBCDriver/2.6.36 (Looker)")]),
]


def _demo_audit_rows(rng: random.Random, days: int, today: date) -> list[dict[str, Any]]:
    rows = []
    by_cluster = {w["cluster"]: w for w in DEMO_WORKLOADS if w["kind"] == "CLUSTER"}
    for offset in range(days, 0, -1):
        day = today - timedelta(days=offset)
        midnight = _midnight(day)
        weekend = day.weekday() >= 5
        for cluster, ws, clients in DEMO_BI_CLIENTS:
            migrated = cluster == BI_SOURCE_CLUSTER and offset <= BI_MIGRATION_OFFSET
            key, compute = ("warehouseId", BI_TARGET_WAREHOUSE) if migrated else ("clusterId", cluster)
            for user, agent in clients:
                ts = midnight + timedelta(hours=rng.uniform(7, 9))
                rows.append(_audit_row(rng, day, ts, user, agent, "accounts", "tokenLogin",
                                       {"tokenId": f"dapi-{zlib.crc32(user.encode()) % 10**6:06d}", "user": user,
                                        "authenticationMethod": "API_INT_PAT_TOKEN"}, workspace_id=ws))
                for _ in range(rng.randint(1, 3) if weekend else rng.randint(4, 9)):
                    ts = midnight + timedelta(hours=rng.uniform(8, 19))
                    rows.append(_audit_row(rng, day, ts, user, agent, "databrickssql", "commandSubmit",
                                           {key: compute, "commandId": f"cmd-{rng.getrandbits(32):08x}"},
                                           workspace_id=ws))
        # F4 SHARED_USERS: attachNotebook per user per cluster (2+ users = shared; 1 = near-miss).
        for cluster, w in by_cluster.items():
            if offset > w.get("start", EXISTING_OFFSET) or w.get("always_on"):
                continue
            if w.get("story") == "autoterm_off" and offset <= AUTOTERM_OFF_OFFSET - 3:
                continue  # abandoned: nobody attaches after the change, it just keeps running
            for user in w["run_as_pool"]:
                if weekend and rng.random() < 0.6:
                    continue
                ts = midnight + timedelta(hours=rng.uniform(9, 11))
                rows.append(_audit_row(rng, day, ts, user, BROWSER_AGENT, "notebook", "attachNotebook",
                                       {"notebookId": f"nb-{rng.randint(5000, 5099)}", "clusterId": cluster,
                                        "path": f"/Users/{user}/work"}, workspace_id=w["workspace_id"]))
    return rows


# --- query.history ---------------------------------------------------------------------------

# warehouse -> (workspace, [(catalog, tables, users, client, queries per weekday, mean seconds)]).
# Execution-time share drives the F3 catalog split.
DEMO_WAREHOUSE_QUERIES: dict[str, tuple[str, list[tuple]]] = {
    "wh-analytics-shared": (ANALYTICS_WS, [
        ("sales", ["sales.gold.orders", "sales.gold.pipeline"], ["ivan@example.com", "liam@example.com"],
         "Tableau", 14, 18),
        ("marketing", ["marketing.gold.campaigns", "marketing.gold.web_sessions"], ["mia@example.com"],
         "Databricks SQL Editor", 10, 15),
        ("finance", ["finance.gold.ledger"], ["erin@example.com"], "Power BI", 5, 25),
        ("product", ["product.gold.events", "product.gold.feature_usage"], ["tara@example.com"],
         "Databricks SQL Editor", 8, 22),
    ]),
    BI_TARGET_WAREHOUSE: (PROD_WS, [
        ("sales_ops", ["sales_ops.gold.leads", "sales_ops.gold.accounts"], ["rachel@example.com"],
         "Power BI", 8, 12),
        ("sales", ["sales.gold.orders"], ["rachel@example.com"], "Tableau", 4, 15),
    ]),
    "wh-exec-dash": (PROD_WS, [
        ("finance", ["finance.gold.ledger", "finance.gold.revenue"], ["erin@example.com", "sophie@example.com"],
         "Power BI", 10, 25),
        ("sales", ["sales.gold.orders"], ["ivan@example.com"], "Tableau", 6, 20),
    ]),
    "wh-ds-adhoc": (ANALYTICS_WS, [
        ("sales", ["sales.gold.customers"], ["quinn@example.com"], "Databricks SQL Editor", 4, 30),
        ("product", ["product.gold.events"], ["yara@example.com"], "Databricks SQL Editor", 4, 30),
    ]),
    "wh-mkt-001": (ANALYTICS_WS, [   # F3 near-miss: a single catalog
        ("marketing", ["marketing.gold.campaigns", "marketing.gold.leads"], ["mia@example.com"],
         "Databricks SQL Editor", 10, 12),
    ]),
}
# The ML squad's queries on wh-ds-adhoc start in month 3.
ML_QUERIES = ("ml", ["ml.features.customer_vectors", "ml.gold.recsys_predictions"], ["uma@example.com"],
              "Databricks SQL Editor", 8, 40)
# The dashboards that move off 1202 in month 5 (same users and tools, now on the warehouse).
MIGRATED_BI_QUERIES = [
    ("sales", ["sales.gold.orders", "sales.gold.customers"], ["svc-powerbi@example.com"], "Power BI", 14, 14),
    ("sales_ops", ["sales_ops.gold.pipeline"], ["rachel@example.com"], "Tableau", 8, 12),
]


def _demo_query_rows(rng: random.Random, days: int, today: date) -> list[dict[str, Any]]:
    rows = []
    for offset in range(days, 0, -1):
        day = today - timedelta(days=offset)
        midnight = _midnight(day)
        weekend = day.weekday() >= 5

        def ts():
            return midnight + timedelta(hours=rng.uniform(7, 20))

        for warehouse, (ws, mix) in DEMO_WAREHOUSE_QUERIES.items():
            mix = list(mix)
            if warehouse == "wh-ds-adhoc" and offset <= ML_START_OFFSET:
                mix.append(ML_QUERIES)
            if warehouse == BI_TARGET_WAREHOUSE and offset <= BI_MIGRATION_OFFSET:
                mix += MIGRATED_BI_QUERIES
            start = len(rows)
            for _catalog, tables, users, client, per_day, mean_s in mix:
                n = round(per_day * WEEKEND_FACTOR) if weekend else per_day
                if warehouse == "wh-exec-dash" and _is_month_end(day):
                    n *= 2
                for _ in range(n):
                    text = rng.choice(QUERY_TEMPLATES).format(t=rng.choice(tables))
                    status = "FAILED" if rng.random() < 0.02 else "FINISHED"
                    cached = status == "FINISHED" and rng.random() < 0.12
                    exec_ms = 0 if cached else int(max(0.2, rng.gauss(mean_s, mean_s * 0.3)) * 1000)
                    rows.append(_query_row(rng, ts(), rng.choice(users), text, client, exec_ms, status, cached,
                                           warehouse=warehouse))
            # F3 edge cases on the shared warehouses: a cross-catalog join and a two-part name.
            if warehouse == "wh-analytics-shared":
                rows.append(_query_row(
                    rng, ts(), "tara@example.com",
                    "SELECT e.feature, COUNT(*) FROM product.gold.events e JOIN `sales`.`gold`.`orders` o "
                    "ON e.account_id = o.account_id GROUP BY e.feature", "Databricks SQL Editor",
                    rng.randint(20_000, 50_000), warehouse=warehouse))
            if warehouse == "wh-ds-adhoc":
                rows.append(_query_row(rng, ts(), "quinn@example.com", "SELECT * FROM gold.customers LIMIT 100",
                                       "Databricks SQL Editor", rng.randint(2_000, 9_000), warehouse=warehouse))
            for r in rows[start:]:
                r["workspace_id"] = ws
    return rows


def _repair_rows(extra: dict[str, list]) -> tuple[list[dict], list[dict]]:
    """ONETIME_RETRY repair runs for the failed runs of 3010 (CRON) and 3018 (manual).

    ASSUMPTION: the repair is recorded as its own run row (new run_id, trigger_type ONETIME_RETRY,
    run_type JOB_RUN) that re-runs only the failed task and what it blocked. Check on a real account.
    """
    runs, tasks = [], []
    ends: dict[str, dict] = {}
    for r in extra["lakeflow_job_run_timeline"]:
        if r["job_id"] in ("3010", "3018") and r["result_state"] == "FAILED":
            ends[r["run_id"]] = r
    for run_id, last in sorted(ends.items()):
        start = last["period_end_time"] + timedelta(minutes=35)
        mid, end = start + timedelta(minutes=40), start + timedelta(minutes=55)
        repair_id = f"{run_id}1"
        base = {k: last[k] for k in ("account_id", "workspace_id", "job_id", "run_name", "compute_ids")}
        runs += _timeline_rows({**base, "run_id": repair_id, "trigger_type": "ONETIME_RETRY", "run_type": "JOB_RUN"},
                               start, end, "SUCCEEDED", "SUCCESS")
        common = {k: last[k] for k in ("account_id", "workspace_id", "job_id", "compute_ids")}
        for i, (task_key, t0, t1) in enumerate((("transform", start, mid), ("load", mid, end))):
            tasks += _timeline_rows({**common, "run_id": f"{repair_id}{i + 1}0", "job_run_id": repair_id,
                                     "parent_run_id": repair_id, "task_key": task_key}, t0, t1, "SUCCEEDED", "SUCCESS")
    return runs, tasks


# ASSUMPTION (F4 MEDIUM-confidence BI path): the query.history reference describes SQL warehouses and
# serverless compute; these rows model BI queries on classic all-purpose clusters with compute.type
# "CLUSTER" and compute.cluster_id set. Check on a real account before relying on them.
CLUSTER_BI_QUERIES = [  # (cluster, workspace, user, client_application, driver key, per weekday, until offset)
    (BI_SOURCE_CLUSTER, PROD_WS, "rachel@example.com", "Tableau", "Tableau", 3, BI_MIGRATION_OFFSET + 1),
    ("1204-looker-jdbc-cluster", ANALYTICS_WS, "svc-looker@example.com", "Looker", "Tableau", 2, 1),
    ("1203-finance-excel-odbc", PROD_WS, "sophie@example.com", "Microsoft Excel", "Power BI", 1, 1),
]


def _cluster_query_rows(rng: random.Random, days: int, today: date) -> list[dict[str, Any]]:
    rows = []
    for offset in range(days, 0, -1):
        day = today - timedelta(days=offset)
        if day.weekday() >= 5:
            continue
        for cluster, ws, user, app, driver_key, per_day, until in CLUSTER_BI_QUERIES:
            if offset < until:
                continue
            for _ in range(per_day):
                ts = _midnight(day) + timedelta(hours=rng.uniform(8, 18))
                text = rng.choice(QUERY_TEMPLATES).format(t=rng.choice(["sales.gold.orders", "finance.gold.ledger"]))
                r = _query_row(rng, ts, user, text, driver_key, int(rng.uniform(3, 40) * 1000), warehouse=None)
                r.update(workspace_id=ws, client_application=app,
                         compute={"type": "CLUSTER", "cluster_id": cluster, "warehouse_id": None})
                rows.append(r)
    return rows


# --- Assembly --------------------------------------------------------------------------------

def build_demo_rows(days: int = DEMO_DAYS, seed: int = 42, today: date | None = None) -> dict[str, list[dict]]:
    """Default-profile rows for ``days`` (unchanged, first in each table) plus the demo workloads."""
    today = today or date.today()
    out = build_synthetic_rows(days=days, seed=seed, today=today, profile="default")
    extra: dict[str, list[dict]] = {name: [] for name in SYSTEM_TABLE_MAP.values()}

    rng = random.Random(seed + 101)
    for offset in range(days, 0, -1):
        day = today - timedelta(days=offset)
        midnight = _midnight(day)
        for w in DEMO_WORKLOADS:
            v = _day_view(w, day, offset)
            if v is None:
                continue
            if v["kind"] in ("JOB", "CLUSTER"):
                _emit_workload(rng, extra, v, day, offset, midnight)
            else:
                _emit_flat(rng, extra, v, day)

    repair_runs, repair_tasks = _repair_rows(extra)
    extra["lakeflow_job_run_timeline"] += repair_runs
    extra["lakeflow_job_task_run_timeline"] += repair_tasks
    run_types = {w["job_id"]: w["run_type"] for w in DEMO_WORKLOADS if w.get("run_type")}
    for r in extra["lakeflow_job_run_timeline"]:
        if r["job_id"] in SUBMIT_RUN_JOBS | {ORCHESTRATOR_JOB}:
            r["run_type"] = "SUBMIT_RUN"  # runs/submit: one-time runs (trigger ONETIME)
        elif r["job_id"] in run_types:
            r["run_type"] = run_types[r["job_id"]]
    # The external orchestrator's runs have no job_id at all (only run ids).
    for table in ("lakeflow_job_run_timeline", "lakeflow_job_task_run_timeline"):
        for r in extra[table]:
            if r["job_id"] == ORCHESTRATOR_JOB:
                r["job_id"] = None

    extra["billing_list_prices"] = _demo_price_rows(today)
    extra["compute_clusters"] = _demo_cluster_rows(today)
    extra["lakeflow_jobs"] = _demo_job_rows(today)
    extra["access_audit"] = _demo_audit_rows(random.Random(seed + 102), days, today)
    extra["query_history"] = _demo_query_rows(random.Random(seed + 103), days, today)
    extra["query_history"] += _cluster_query_rows(random.Random(seed + 104), days, today)
    for table, rows in extra.items():
        out[table].extend(rows)
    return out
