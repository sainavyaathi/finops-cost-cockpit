"""Sandbox helpers for running the FinOps accelerator against synthetic System Tables.

Two pieces:

1. ``generate_synthetic_system_tables`` writes nine Delta tables whose schemas mirror the
   Databricks System Tables the accelerator and the cockpit steps read (billing.usage,
   billing.list_prices, compute.node_timeline, compute.clusters, lakeflow.jobs,
   lakeflow.job_run_timeline, lakeflow.job_task_run_timeline, access.audit,
   query.history). The data is deterministic (seeded) and is shaped so every
   classification path lights up: healthy jobs, oversized jobs, memory-bound jobs, flaky
   jobs, shared all-purpose clusters, BI tools on all-purpose compute, scheduled jobs on
   all-purpose compute, zombie clusters, a multi-catalog shared warehouse, untagged spend,
   fallback pricing, and so on. ``F12_WORKLOADS`` holds the F3-F5 scenarios and their
   near-misses; see sandbox/README.md for the scenario -> feature map.

2. ``RedirectingSpark`` wraps a SparkSession and rewrites ``system.<schema>.<table>``
   references to the synthetic tables, so the accelerator code runs completely unmodified.

Works on a local PySpark + Delta session and on Databricks (including Free Edition).
"""

from __future__ import annotations

import random
import re
import zlib
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

SYSTEM_TABLE_MAP = {
    "system.billing.usage": "billing_usage",
    "system.billing.list_prices": "billing_list_prices",
    "system.compute.node_timeline": "compute_node_timeline",
    "system.compute.clusters": "compute_clusters",
    "system.lakeflow.jobs": "lakeflow_jobs",
    "system.lakeflow.job_run_timeline": "lakeflow_job_run_timeline",
    "system.lakeflow.job_task_run_timeline": "lakeflow_job_task_run_timeline",
    "system.access.audit": "access_audit",
    "system.query.history": "query_history",
}

ACCOUNT_ID = "acc-00000000-synthetic"
PROD_WS = "1111111111111111"
DEV_WS = "2222222222222222"
CLOUD = "AWS"

# Per-SKU list prices (USD per DBU). SKUs missing here have no list price on purpose,
# so the accelerator has to use fallback_dbu_price for them.
LIST_PRICES = {
    "PREMIUM_JOBS_COMPUTE": "0.15",
    "PREMIUM_ALL_PURPOSE_COMPUTE": "0.55",
    "PREMIUM_SQL_PRO_COMPUTE_US_EAST": "0.55",
    "PREMIUM_DLT_ADVANCED_COMPUTE": "0.36",
    "PREMIUM_JOBS_SERVERLESS_COMPUTE_US_EAST": None,
    "PREMIUM_SERVERLESS_COMPUTE_US_EAST": "0.75",
    "PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_EAST": None,
}

FULL_TAGS = {
    "project": "sales_analytics",
    "team": "data-eng",
    "owner": "alice@example.com",
    "environment": "prod",
    "cost_center": "CC-100",
}

# Each workload is designed to trigger a specific accelerator outcome.
WORKLOADS: list[dict[str, Any]] = [
    {
        "kind": "JOB", "workspace_id": PROD_WS, "job_id": "1001", "name": "nightly_sales_etl",
        "sku": "PREMIUM_JOBS_COMPUTE", "product": "JOBS", "cluster": "job-1001-cluster",
        "daily_dbus": 40, "runs_per_day": 1, "failure_rate": 0.0,
        # F4 near-miss: CRON job on a job cluster (correct usage). F6 near-miss: 3 tasks, no retries.
        "cpu": 65, "mem": 60, "tags": FULL_TAGS, "run_as": "svc-etl@example.com",
        "expect": "HEALTHY_EXPENSIVE / LOOKS_HEALTHY",
    },
    {
        "kind": "JOB", "workspace_id": PROD_WS, "job_id": "1002", "name": "ml_feature_training",
        "sku": "PREMIUM_JOBS_COMPUTE", "product": "JOBS", "cluster": "job-1002-cluster",
        "daily_dbus": 160, "runs_per_day": 1, "failure_rate": 0.0,
        "cpu": 7, "mem": 22,
        # F6 near-miss: the long "transform" task is sliced across clock hours (same task run_id).
        "long_task": "transform",
        "tags": {**FULL_TAGS, "project": "churn_model", "team": "ml", "owner": "bob@example.com"},
        "run_as": "svc-ml@example.com",
        "expect": "HIGH_COST_LOW_UTILIZATION / REVIEW_WORKER_COUNT",
    },
    {
        "kind": "JOB", "workspace_id": PROD_WS, "job_id": "1003", "name": "customer_360_build",
        "sku": "PREMIUM_JOBS_COMPUTE", "product": "JOBS", "cluster": "job-1003-cluster",
        "daily_dbus": 70, "runs_per_day": 1, "failure_rate": 0.0,
        "cpu": 15, "mem": 88,
        "tags": {"project": "c360", "team": "data-eng", "owner": "carol@example.com", "environment": "prod"},
        "run_as": "svc-etl@example.com",
        "expect": "MEMORY_BOUND / INVESTIGATE_MEMORY_PRESSURE (missing cost_center)",
    },
    {
        "kind": "JOB", "workspace_id": PROD_WS, "job_id": "1004", "name": "flaky_vendor_ingest",
        # F5 near-miss: a job cluster that reports auto-termination 0 (real job clusters can).
        # It ends with its run, so F5 must filter on cluster_source, not on the setting alone.
        "job_cluster_autoterm": 0,
        "sku": "PREMIUM_JOBS_COMPUTE", "product": "JOBS", "cluster": "job-1004-cluster",
        "daily_dbus": 30, "runs_per_day": 2, "failure_rate": 0.4,
        "cpu": 45, "mem": 50,
        "tags": {"project": "vendor_feeds", "environment": "prod"},
        "run_as": "dave@example.com",
        "expect": "HIGH_FAILURE_RATE / INVESTIGATE_FAILURES (missing owner/team/cost_center)",
    },
    {
        "kind": "JOB", "workspace_id": PROD_WS, "job_id": "1005", "name": "finance_reconcile",
        "sku": "PREMIUM_JOBS_COMPUTE", "product": "JOBS", "cluster": "job-1005-cluster",
        # F6 positive: ~60% of runs retry "transform" (2 task runs, one job_run_id).
        "daily_dbus": 12, "runs_per_day": 3, "failure_rate": 0.05, "task_retries": True,
        "cpu": 50, "mem": 55,
        "tags": {**FULL_TAGS, "project": "finance", "team": "fin-eng", "owner": "erin@example.com", "cost_center": "CC-200"},
        "run_as": "svc-fin@example.com",
        "expect": "Task retries in job_task_run_timeline (see README note on retry join)",
    },
    {
        "kind": "JOB", "workspace_id": PROD_WS, "job_id": "1006", "name": "serverless_daily_report",
        "sku": "PREMIUM_JOBS_SERVERLESS_COMPUTE_US_EAST", "product": "JOBS", "cluster": None,
        "daily_dbus": 18, "runs_per_day": 1, "failure_rate": 0.0,
        "cpu": None, "mem": None,
        "tags": {**FULL_TAGS, "project": "exec_reporting"},
        "run_as": "svc-bi@example.com",
        "expect": "FALLBACK_DBU_PRICE (SKU has no list price), no utilization",
    },
    {
        "kind": "CLUSTER", "workspace_id": PROD_WS, "cluster": "0101-shared-ds-cluster",
        "sku": "PREMIUM_ALL_PURPOSE_COMPUTE", "product": "ALL_PURPOSE",
        "daily_dbus": 90, "cpu": 12, "mem": 30,
        # Exactly 60 minutes: the zombie rule is "above 60", so this is the boundary case.
        "auto_term": 60,
        "tags": {"team": "data-science"},
        "run_as_pool": ["frank@example.com", "grace@example.com", "heidi@example.com"],
        "expect": "SHARED_CLUSTER_ATTRIBUTION + low utilization + missing tags; F4 SHARED_USERS",
    },
    {
        "kind": "SQL_WAREHOUSE", "workspace_id": PROD_WS, "warehouse": "wh-bi-001",
        "sku": "PREMIUM_SQL_PRO_COMPUTE_US_EAST", "product": "SQL",
        "daily_dbus": 55,
        # Uses alias keys (Team, Owner, CostCenter, env) to exercise tagging_rules.yml aliases.
        "tags": {"Project": "bi_dashboards", "Team": "analytics", "Owner": "ivan@example.com", "env": "prod", "CostCenter": "CC-300"},
        "run_as": None,
        "expect": "HIGH attribution, GOOD tagging via aliases",
    },
    {
        "kind": "PIPELINE", "workspace_id": PROD_WS, "pipeline": "pl-orders-dlt",
        "sku": "PREMIUM_DLT_ADVANCED_COMPUTE", "product": "DLT",
        "daily_dbus": 25,
        "tags": {**FULL_TAGS, "project": "orders_stream"},
        "run_as": "svc-etl@example.com",
        "expect": "PIPELINE workload, HIGH attribution",
    },
    {
        "kind": "NOTEBOOK", "workspace_id": PROD_WS, "notebook": "nb-7777",
        "sku": "PREMIUM_SERVERLESS_COMPUTE_US_EAST", "product": "INTERACTIVE",
        "daily_dbus": 6, "tags": {}, "run_as": "judy@example.com",
        "expect": "NOTEBOOK workload, MISSING tags",
    },
    {
        "kind": "UNKNOWN", "workspace_id": PROD_WS,
        "sku": "PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_EAST", "product": "MODEL_SERVING",
        "daily_dbus": 4, "tags": {}, "run_as": None,
        "expect": "UNKNOWN attribution, fallback price",
    },
    {
        "kind": "JOB", "workspace_id": DEV_WS, "job_id": "2001", "name": "dev_experiment",
        "sku": "PREMIUM_JOBS_COMPUTE", "product": "JOBS", "cluster": "job-2001-cluster",
        "daily_dbus": 3, "runs_per_day": 1, "failure_rate": 0.1,
        "cpu": 30, "mem": 35,
        "tags": {**FULL_TAGS, "environment": "dev", "owner": "kim@example.com"},
        "run_as": "kim@example.com",
        "expect": "Small dev-workspace job",
    },
]

# F12 scenarios (F3-F6 rules and their near-misses). Generated with their own RNG after
# WORKLOADS, so adding or changing one never shifts the original workloads' billing_usage or
# compute_node_timeline rows (pinned by a digest test against development's generator).
# Timeline tables are not pinned: they changed on purpose (hour slicing, ordered tasks).
F12_WORKLOADS: list[dict[str, Any]] = [
    {
        "kind": "CLUSTER", "workspace_id": PROD_WS, "cluster": "0202-bi-tableau-cluster",
        "sku": "PREMIUM_ALL_PURPOSE_COMPUTE", "product": "ALL_PURPOSE",
        "daily_dbus": 45, "cpu": 35, "mem": 40, "auto_term": 30,
        "tags": {**FULL_TAGS, "project": "bi_dashboards", "team": "analytics", "owner": "ivan@example.com",
                 "cost_center": "CC-300"},
        "run_as_pool": ["svc-tableau@example.com", "olivia@example.com"],
        "expect": "F4 BI_TOOL_JDBC (Tableau JDBC + Power BI ODBC in access.audit)",
    },
    {
        "kind": "JOB", "workspace_id": PROD_WS, "job_id": "1007", "name": "hourly_kpi_refresh",
        "sku": "PREMIUM_ALL_PURPOSE_COMPUTE", "product": "ALL_PURPOSE", "cluster": "0303-kpi-allpurpose-cluster",
        # F4 positive: CRON job on an all-purpose (UI) cluster. Billed to the cluster only, like
        # real billing.usage, so detect it via job_run_timeline.compute_ids + compute.clusters.
        "cluster_source": "UI", "auto_term": 45,
        "daily_dbus": 36, "runs_per_day": 4, "failure_rate": 0.0,
        "cpu": 40, "mem": 45,
        "tags": {**FULL_TAGS, "project": "kpi_refresh", "team": "analytics", "owner": "liam@example.com",
                 "cost_center": "CC-300"},
        "run_as": "svc-bi@example.com",
        "expect": "F4 SCHEDULED_JOB (CRON job on an all-purpose cluster)",
    },
    {
        "kind": "CLUSTER", "workspace_id": PROD_WS, "cluster": "0404-zombie-autoterm-off",
        "sku": "PREMIUM_ALL_PURPOSE_COMPUTE", "product": "ALL_PURPOSE",
        "daily_dbus": 30, "cpu": 3, "mem": 12, "always_on": True,
        # Created with 30 minutes, later changed to 0 (disabled): the latest change_time must win.
        "auto_term": 0, "auto_term_history": [30],
        "tags": {"team": "data-science", "owner": "nina@example.com"},
        "run_as_pool": ["nina@example.com"],
        "expect": "F5 ZOMBIE_CLUSTER AUTO_TERM_OFF + IDLE_COMPUTE",
    },
    {
        "kind": "CLUSTER", "workspace_id": DEV_WS, "cluster": "0505-zombie-long-autoterm",
        "sku": "PREMIUM_ALL_PURPOSE_COMPUTE", "product": "ALL_PURPOSE",
        "daily_dbus": 14, "cpu": 4, "mem": 15, "always_on": True, "auto_term": 240,
        "tags": {"environment": "dev", "owner": "kim@example.com"},
        "run_as_pool": ["kim@example.com"],
        "expect": "F5 ZOMBIE_CLUSTER (auto-termination 240 min) + IDLE_COMPUTE",
    },
    {
        # F5 positive, boundary: auto-termination 61 min is "over 60", so flagged; CPU 6%.
        "kind": "CLUSTER", "workspace_id": PROD_WS, "cluster": "0707-autoterm-61-cluster", "billing_run_as": False,
        "sku": "PREMIUM_ALL_PURPOSE_COMPUTE", "product": "ALL_PURPOSE",
        "daily_dbus": 18, "cpu": 6, "mem": 20, "auto_term": 61,
        "tags": {**FULL_TAGS, "project": "adhoc_analysis", "team": "analytics", "owner": "oscar@example.com",
                 "cost_center": "CC-300"},
        "run_as_pool": ["oscar@example.com"],
        "expect": "F5 AUTO_TERM_OVER_60 (61 min) + low utilisation",
    },
    {
        # F5 near-miss: auto-termination off and on 24/7, but CPU 72% (a busy streaming cluster).
        # Flag it only if the rule ignores utilisation; it is not idle.
        "kind": "CLUSTER", "workspace_id": PROD_WS, "cluster": "0808-streaming-autoterm-off", "billing_run_as": False,
        "sku": "PREMIUM_ALL_PURPOSE_COMPUTE", "product": "ALL_PURPOSE",
        "daily_dbus": 20, "cpu": 72, "mem": 65, "always_on": True, "auto_term": 0,
        "tags": {**FULL_TAGS, "project": "clickstream", "owner": "paula@example.com"},
        "run_as_pool": ["svc-stream@example.com"],
        "expect": "F5 near-miss: auto-termination off but busy, not idle",
    },
    {
        # F5 near-miss: low CPU (8%) but memory 85%, auto-termination 30: memory-bound, not idle.
        "kind": "CLUSTER", "workspace_id": PROD_WS, "cluster": "0909-memory-heavy-cluster", "billing_run_as": False,
        "sku": "PREMIUM_ALL_PURPOSE_COMPUTE", "product": "ALL_PURPOSE",
        "daily_dbus": 16, "cpu": 8, "mem": 85, "auto_term": 30,
        "tags": {**FULL_TAGS, "project": "graph_features", "team": "ml", "owner": "quinn@example.com"},
        "run_as_pool": ["quinn@example.com"],
        "expect": "F5 near-miss: low CPU but high memory is not waste",
    },
    {
        # F4 near-miss: a manual (ONETIME) weekly backfill on the shared all-purpose cluster.
        # Not scheduled, so not SCHEDULED_JOB. Its compute is already in the cluster's billing,
        # so it adds no billing or node rows of its own.
        "kind": "JOB", "workspace_id": PROD_WS, "job_id": "1008", "name": "adhoc_backfill",
        "sku": "PREMIUM_ALL_PURPOSE_COMPUTE", "product": "ALL_PURPOSE", "cluster": "0101-shared-ds-cluster",
        "cluster_source": "UI", "existing_cluster": True, "bill": False, "trigger": "ONETIME",
        "every_n_days": 7, "daily_dbus": 0, "runs_per_day": 1, "failure_rate": 0.0,
        "cpu": None, "mem": None, "tags": {"team": "data-science"}, "run_as": "frank@example.com",
        "expect": "F4 near-miss: manual run on all-purpose compute",
    },
    {
        # F3 near-miss: a warehouse that only queries the finance catalog. Not shared, so its
        # whole cost goes to finance and the tags already agree with it.
        "kind": "SQL_WAREHOUSE", "workspace_id": PROD_WS, "warehouse": "wh-fin-002",
        "sku": "PREMIUM_SQL_PRO_COMPUTE_US_EAST", "product": "SQL", "daily_dbus": 12,
        "tags": {**FULL_TAGS, "project": "finance", "team": "fin-eng", "owner": "erin@example.com",
                 "cost_center": "CC-200"},
        "run_as": None,
        "expect": "F3 near-miss: single-catalog warehouse, 100% finance",
    },
]


# ---------------------------------------------------------------------------
# Schemas: a subset of the real System Table columns, with the real names, types and nesting.
# Declared as pure Python so tests can check them without pyspark; _schemas() builds the
# Spark StructTypes. A type is a scalar name, "array<string>", "map<string,string>",
# "decimal(p,s)", or a list of (name, type) pairs for a struct.
# ---------------------------------------------------------------------------

_S = "string"
_TS = "timestamp"

SCHEMA_SPEC: dict[str, list[tuple[str, Any]]] = {
    "billing_usage": [
        ("account_id", _S), ("workspace_id", _S), ("record_id", _S), ("sku_name", _S), ("cloud", _S),
        ("usage_start_time", _TS), ("usage_end_time", _TS), ("usage_date", "date"),
        ("custom_tags", "map<string,string>"), ("usage_unit", _S), ("usage_quantity", "decimal(38,18)"),
        ("usage_metadata", [(c, _S) for c in (
            "cluster_id", "job_id", "warehouse_id", "instance_pool_id", "node_type", "job_run_id",
            "notebook_id", "dlt_pipeline_id", "endpoint_name", "endpoint_id", "dlt_update_id",
            "job_name", "notebook_path")]),
        ("identity_metadata", [("run_as", _S), ("owned_by", _S), ("created_by", _S)]),
        ("record_type", _S), ("ingestion_date", "date"), ("billing_origin_product", _S), ("usage_type", _S),
    ],
    "billing_list_prices": [
        ("price_start_time", _TS), ("price_end_time", _TS),
        ("account_id", _S), ("sku_name", _S), ("cloud", _S), ("currency_code", _S), ("usage_unit", _S),
        ("pricing", [("default", "decimal(30,18)"),
                     ("promotional", [("default", "decimal(30,18)")]),
                     ("effective_list", [("default", "decimal(30,18)")])]),
    ],
    "compute_node_timeline": [
        ("account_id", _S), ("workspace_id", _S), ("cluster_id", _S), ("instance_id", _S),
        ("start_time", _TS), ("end_time", _TS), ("driver", "boolean"),
        ("cpu_user_percent", "double"), ("cpu_system_percent", "double"), ("cpu_wait_percent", "double"),
        ("mem_used_percent", "double"), ("mem_swap_percent", "double"),
        ("network_sent_bytes", "bigint"), ("network_received_bytes", "bigint"), ("node_type", _S),
    ],
    # Slowly changing: one row per configuration change, the latest change_time is current.
    "compute_clusters": [
        ("account_id", _S), ("workspace_id", _S), ("cluster_id", _S), ("cluster_name", _S), ("owned_by", _S),
        ("create_time", _TS), ("delete_time", _TS), ("driver_node_type", _S), ("worker_node_type", _S),
        ("worker_count", "bigint"), ("min_autoscale_workers", "bigint"), ("max_autoscale_workers", "bigint"),
        ("auto_termination_minutes", "bigint"), ("enable_elastic_disk", "boolean"),
        ("tags", "map<string,string>"), ("cluster_source", _S), ("init_scripts", "array<string>"),
        ("driver_instance_pool_id", _S), ("worker_instance_pool_id", _S), ("dbr_version", _S),
        ("data_security_mode", _S), ("policy_id", _S), ("change_time", _TS), ("change_date", "date"),
    ],
    "lakeflow_jobs": [
        ("account_id", _S), ("workspace_id", _S), ("job_id", _S), ("name", _S), ("description", _S),
        ("creator_id", _S), ("tags", "map<string,string>"), ("change_time", _TS), ("delete_time", _TS),
        ("run_as", _S),
        # F4: CRON / PERIODIC = scheduled; NULL = not set (manual / API only).
        ("trigger_type", _S), ("paused", "boolean"),
    ],
    "lakeflow_job_run_timeline": [
        ("account_id", _S), ("workspace_id", _S), ("job_id", _S), ("run_id", _S),
        ("period_start_time", _TS), ("period_end_time", _TS), ("trigger_type", _S), ("run_type", _S),
        ("run_name", _S), ("compute_ids", "array<string>"), ("result_state", _S), ("termination_code", _S),
    ],
    # Matches the real table: run_id is the *task* run id, job_run_id is the parent job run.
    "lakeflow_job_task_run_timeline": [
        ("account_id", _S), ("workspace_id", _S), ("job_id", _S), ("run_id", _S), ("job_run_id", _S),
        ("parent_run_id", _S), ("period_start_time", _TS), ("period_end_time", _TS), ("task_key", _S),
        ("compute_ids", "array<string>"), ("result_state", _S), ("termination_code", _S),
    ],
    "access_audit": [
        ("version", _S), ("event_time", _TS), ("event_date", "date"), ("account_id", _S), ("workspace_id", _S),
        ("source_ip_address", _S), ("user_agent", _S), ("session_id", _S),
        ("user_identity", [("email", _S), ("subject_name", _S)]),
        ("service_name", _S), ("action_name", _S), ("request_id", _S),
        ("request_params", "map<string,string>"),
        ("response", [("status_code", "int"), ("error_message", _S), ("result", _S)]),
        ("audit_level", _S), ("event_id", _S),
        ("identity_metadata", [("run_by", _S), ("run_as", _S), ("acting_resource", _S)]),
    ],
    # Like the real table there is no catalog column: the catalog lives in statement_text.
    "query_history": [
        ("account_id", _S), ("workspace_id", _S), ("statement_id", _S), ("session_id", _S),
        ("execution_status", _S),
        ("compute", [("type", _S), ("cluster_id", _S), ("warehouse_id", _S)]),
        ("executed_by", _S), ("executed_by_user_id", _S), ("statement_text", _S), ("statement_type", _S),
        ("error_message", _S), ("client_application", _S), ("client_driver", _S),
        ("cache_origin_statement_id", _S),
        ("total_duration_ms", "bigint"), ("execution_duration_ms", "bigint"),
        ("compilation_duration_ms", "bigint"), ("waiting_for_compute_duration_ms", "bigint"),
        ("waiting_at_capacity_duration_ms", "bigint"), ("total_task_duration_ms", "bigint"),
        ("result_fetch_duration_ms", "bigint"), ("read_rows", "bigint"), ("read_bytes", "bigint"),
        ("produced_rows", "bigint"), ("from_result_cache", "boolean"),
        ("start_time", _TS), ("end_time", _TS), ("update_time", _TS),
        ("query_source", [
            ("job_info", [("job_id", _S), ("job_run_id", _S), ("job_task_run_id", _S)]),
            ("alert_id", _S), ("sql_query_id", _S), ("dashboard_id", _S), ("legacy_dashboard_id", _S),
            ("notebook_id", _S), ("genie_space_id", _S)]),
        ("executed_as", _S), ("executed_as_user_id", _S), ("query_tags", "map<string,string>"),
    ],
}


def _spark_type(spec: Any):
    from pyspark.sql import types as T

    if isinstance(spec, list):
        return T.StructType([T.StructField(name, _spark_type(t)) for name, t in spec])
    if spec.startswith("decimal("):
        precision, scale = spec[len("decimal("):-1].split(",")
        return T.DecimalType(int(precision), int(scale))
    if spec == "array<string>":
        return T.ArrayType(T.StringType())
    if spec == "map<string,string>":
        return T.MapType(T.StringType(), T.StringType())
    return {
        "string": T.StringType(), "timestamp": T.TimestampType(), "date": T.DateType(),
        "bigint": T.LongType(), "int": T.IntegerType(), "double": T.DoubleType(), "boolean": T.BooleanType(),
    }[spec]


def _schemas():
    """Spark StructType per synthetic table (needs pyspark)."""
    return {table: _spark_type(spec) for table, spec in SCHEMA_SPEC.items()}


# ---------------------------------------------------------------------------
# Row generation
# ---------------------------------------------------------------------------

def _dec(value: float) -> Decimal:
    return Decimal(f"{value:.6f}")


def _usage_metadata(**kwargs: Any) -> dict[str, Any]:
    fields = [
        "cluster_id", "job_id", "warehouse_id", "instance_pool_id", "node_type", "job_run_id",
        "notebook_id", "dlt_pipeline_id", "endpoint_name", "endpoint_id", "dlt_update_id",
        "job_name", "notebook_path",
    ]
    return {f: kwargs.get(f) for f in fields}


def _usage_row(rng, w, day, start, end, dbus, run_as, **meta) -> dict[str, Any]:
    return {
        "account_id": ACCOUNT_ID,
        "workspace_id": w["workspace_id"],
        "record_id": f"rec-{rng.getrandbits(48):012x}",
        "sku_name": w["sku"],
        "cloud": CLOUD,
        "usage_start_time": start,
        "usage_end_time": end,
        "usage_date": day,
        "custom_tags": dict(w.get("tags") or {}),
        "usage_unit": "DBU",
        "usage_quantity": _dec(dbus),
        "usage_metadata": _usage_metadata(**meta),
        "identity_metadata": {"run_as": run_as, "owned_by": None, "created_by": None},
        "record_type": "ORIGINAL",
        "ingestion_date": day,
        "billing_origin_product": w["product"],
        "usage_type": "COMPUTE_TIME",
    }


def _node_rows(rng, w, day, start_hour, hours) -> list[dict[str, Any]]:
    rows = []
    if w.get("cpu") is None or not w.get("cluster"):
        return rows
    samples = max(2, int(hours * 2))
    for i in range(samples):
        t0 = datetime.combine(day, datetime.min.time()) + timedelta(hours=start_hour, minutes=30 * i)
        cpu_total = max(0.5, rng.gauss(w["cpu"], 3))
        mem = min(99.0, max(1.0, rng.gauss(w["mem"], 3)))
        for node in range(3):
            rows.append({
                "account_id": ACCOUNT_ID,
                "workspace_id": w["workspace_id"],
                "cluster_id": w["cluster"],
                "instance_id": f"i-{w['cluster']}-{node}",
                "start_time": t0,
                "end_time": t0 + timedelta(minutes=1),
                "driver": node == 0,
                "cpu_user_percent": round(cpu_total * 0.8, 3),
                "cpu_system_percent": round(cpu_total * 0.2, 3),
                "cpu_wait_percent": round(rng.uniform(0, 3), 3),
                "mem_used_percent": round(mem, 3),
                "mem_swap_percent": 0.0,
                "network_sent_bytes": int(rng.uniform(1e6, 5e7)),
                "network_received_bytes": int(rng.uniform(1e6, 8e7)),
                "node_type": "i3.xlarge",
            })
    return rows


def _hour_slices(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    """Clock-hour-aligned slices, as the real timeline tables emit for rows written after
    19 January 2026: a run from 04:47 to 06:10 becomes 04:47-05:00, 05:00-06:00, 06:00-06:10."""
    slices, cur = [], start
    while True:
        boundary = cur.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        if boundary >= end:
            slices.append((cur, end))
            return slices
        slices.append((cur, boundary))
        cur = boundary


def _timeline_rows(base: dict[str, Any], start: datetime, end: datetime, state: str | None,
                   code: str | None) -> list[dict[str, Any]]:
    # result_state / termination_code are only set on the slice that ends the run.
    rows = []
    slices = _hour_slices(start, end)
    for i, (s0, s1) in enumerate(slices):
        last = i == len(slices) - 1
        rows.append({**base, "period_start_time": s0, "period_end_time": s1,
                     "result_state": state if last else None, "termination_code": code if last else None})
    return rows


def _on_all_purpose(w: dict[str, Any]) -> bool:
    return w["kind"] == "JOB" and w.get("cluster_source", "JOB") != "JOB"


TASK_GAP = timedelta(minutes=1)


def _task_rows(w, run_id, compute, start, end, failed, attempts) -> list[dict[str, Any]]:
    """Tasks extract -> transform -> load, laid end to end inside the job run window.

    Each task (and each retry attempt) starts after the previous one ends, and the last task
    that runs ends exactly at the job run's end. A failed run fails in "transform" (every
    attempt fails); "load" then never starts: result_state BLOCKED, termination_code SKIPPED,
    period_start_time = period_end_time (the documented shape of a run that never began).
    """
    long_key = w.get("long_task")  # F6 near-miss: this task gets most of the window
    plan = [("extract", 0, 1)]
    plan += [("transform", a, 6 if long_key == "transform" else 1) for a in range(attempts["transform"])]
    if not failed:
        plan.append(("load", 0, 1))
    unit = (end - start - TASK_GAP * len(plan)) / sum(weight for _, _, weight in plan)
    order = {"extract": 0, "transform": 1, "load": 2}
    rows, cur = [], start
    for i, (task_key, a, weight) in enumerate(plan):
        t0 = cur + TASK_GAP
        t1 = end if i == len(plan) - 1 else t0 + unit * weight
        last_attempt = a == attempts[task_key] - 1
        ok = not (task_key == "transform" and (failed or not last_attempt))
        rows.extend(_timeline_rows(
            {"account_id": ACCOUNT_ID, "workspace_id": w["workspace_id"], "job_id": w["job_id"],
             "run_id": f"{run_id}{order[task_key]}{a}", "job_run_id": run_id, "parent_run_id": run_id,
             "task_key": task_key, "compute_ids": compute},
            t0, t1, "SUCCEEDED" if ok else "FAILED", "SUCCESS" if ok else "RUN_EXECUTION_ERROR",
        ))
        cur = t1
    if failed:
        rows.append({
            "account_id": ACCOUNT_ID, "workspace_id": w["workspace_id"], "job_id": w["job_id"],
            "run_id": f"{run_id}20", "job_run_id": run_id, "parent_run_id": run_id,
            "period_start_time": end, "period_end_time": end, "task_key": "load",
            "compute_ids": compute, "result_state": "BLOCKED", "termination_code": "SKIPPED",
        })
    return rows


def _emit_workload(rng, out, w, day, offset, midnight) -> None:
    jitter = rng.uniform(0.8, 1.2)
    kind = w["kind"]

    if kind == "JOB":
        if offset % w.get("every_n_days", 1):
            return
        runs = w["runs_per_day"]
        for n in range(runs):
            run_id = f"{w['job_id']}{offset:03d}{n}"
            start = midnight + timedelta(hours=1 + n * 6, minutes=rng.randint(0, 20))
            failed = rng.random() < w["failure_rate"]
            hours = rng.uniform(0.5, 2.5) * (0.4 if failed else 1.0)
            end = start + timedelta(hours=hours)
            dbus = w["daily_dbus"] * jitter / runs * (0.5 if failed else 1.0)
            state = "FAILED" if failed else "SUCCEEDED"

            if w.get("bill", True):
                # Real billing.usage only fills job_id / job_run_id for job and serverless compute.
                # A job on an all-purpose cluster is billed to the cluster alone (see the Jobs
                # system table reference, "Job run not visible in billing.usage table").
                job_meta = {} if _on_all_purpose(w) else {"job_id": w["job_id"], "job_run_id": run_id}
                out["billing_usage"].append(_usage_row(
                    rng, w, day, start, end, dbus, w.get("run_as"),
                    cluster_id=w.get("cluster"), node_type="i3.xlarge" if w.get("cluster") else None,
                    **job_meta,
                ))
            compute = [w["cluster"]] if w.get("cluster") else []
            out["lakeflow_job_run_timeline"].extend(_timeline_rows(
                {"account_id": ACCOUNT_ID, "workspace_id": w["workspace_id"], "job_id": w["job_id"],
                 "run_id": run_id, "trigger_type": w.get("trigger", "CRON"), "run_type": "JOB_RUN",
                 "run_name": w["name"], "compute_ids": compute},
                start, end, state, "RUN_EXECUTION_ERROR" if failed else "SUCCESS",
            ))

            # F6 retries: a failed "transform" attempt followed by a retry inside the same
            # job_run_id (new task run_id, same task_key). Grouping by run_id hides these.
            # (Same RNG draw, in the same place, as before, so billing/node rows do not shift.)
            attempts = {"extract": 1, "transform": 1, "load": 1}
            if w.get("task_retries") and rng.random() < 0.6:
                attempts["transform"] = 2
            out["lakeflow_job_task_run_timeline"].extend(
                _task_rows(w, run_id, compute, start, end, failed, attempts))

            out["compute_node_timeline"].extend(_node_rows(rng, w, day, 1 + n * 6, hours))

    elif kind == "CLUSTER":
        # Zombie clusters never shut down, so they bill and report nodes around the clock.
        always_on = w.get("always_on", False)
        # ASSUMPTION: real billing.usage probably leaves identity_metadata.run_as empty for
        # classic all-purpose compute. The original 0101 cluster keeps per-user rows (upstream
        # shared-cluster attribution reads them); new F12 clusters set billing_run_as False so
        # the NULL case is exercised. Count users from access.audit attachNotebook instead.
        users = w["run_as_pool"] if w.get("billing_run_as", True) else [None]
        for user in users:
            if always_on:
                start, end = midnight, midnight + timedelta(hours=24)
            else:
                start = midnight + timedelta(hours=9 + rng.randint(0, 3))
                end = start + timedelta(hours=rng.uniform(1, 4))
            out["billing_usage"].append(_usage_row(
                rng, w, day, start, end, w["daily_dbus"] * jitter / len(users), user,
                cluster_id=w["cluster"], node_type="r5.2xlarge",
            ))
        out["compute_node_timeline"].extend(
            _node_rows(rng, w, day, 0, 24) if always_on else _node_rows(rng, w, day, 9, 8)
        )

    else:
        start = midnight + timedelta(hours=8)
        end = start + timedelta(hours=10)
        meta: dict[str, Any] = {}
        if kind == "SQL_WAREHOUSE":
            meta["warehouse_id"] = w["warehouse"]
        elif kind == "PIPELINE":
            meta["dlt_pipeline_id"] = w["pipeline"]
        elif kind == "NOTEBOOK":
            meta["notebook_id"] = w["notebook"]
            meta["notebook_path"] = "/Users/judy@example.com/scratch"
        out["billing_usage"].append(_usage_row(
            rng, w, day, start, end, w["daily_dbus"] * jitter, w.get("run_as"), **meta,
        ))


def build_synthetic_rows(days: int = 45, seed: int = 42, today: date | None = None) -> dict[str, list[dict]]:
    """Every row is a function of (days, seed, today). ``today`` defaults to the current date
    so a fresh run is date-relative; tests pin it."""
    rng = random.Random(seed)
    today = today or date.today()
    out: dict[str, list[dict]] = {name: [] for name in SYSTEM_TABLE_MAP.values()}

    # list_prices
    for sku, price in LIST_PRICES.items():
        if price is None:
            continue
        p = Decimal(price)
        out["billing_list_prices"].append({
            "price_start_time": datetime(2023, 1, 1),
            "price_end_time": None,
            "account_id": ACCOUNT_ID,
            "sku_name": sku,
            "cloud": CLOUD,
            "currency_code": "USD",
            "usage_unit": "DBU",
            "pricing": {"default": p, "promotional": {"default": p}, "effective_list": {"default": p}},
        })

    # lakeflow.jobs (job 1004 was renamed once, to exercise "latest name wins")
    for w in WORKLOADS + F12_WORKLOADS:
        if w["kind"] != "JOB":
            continue
        trigger = w.get("trigger", "CRON")
        base = {
            "account_id": ACCOUNT_ID, "workspace_id": w["workspace_id"], "job_id": w["job_id"],
            "description": None, "creator_id": "123", "tags": dict(w.get("tags") or {}),
            "delete_time": None, "run_as": w.get("run_as"),
            # The jobs table has no ONETIME value: a job with no schedule has trigger_type NULL.
            "trigger_type": None if trigger == "ONETIME" else trigger, "paused": False,
        }
        if w["job_id"] == "1004":
            out["lakeflow_jobs"].append({**base, "name": "vendor_ingest_v1", "change_time": datetime(2024, 1, 1)})
        out["lakeflow_jobs"].append({**base, "name": w["name"], "change_time": datetime(2025, 6, 1)})

    for offset in range(days, 0, -1):
        day = today - timedelta(days=offset)
        midnight = datetime.combine(day, datetime.min.time())
        for w in WORKLOADS:
            _emit_workload(rng, out, w, day, offset, midnight)

    # F12 scenarios get their own generator so the original workloads' billing and node rows
    # stay identical.
    f12_rng = random.Random(seed + 3)
    for offset in range(days, 0, -1):
        day = today - timedelta(days=offset)
        midnight = datetime.combine(day, datetime.min.time())
        for w in F12_WORKLOADS:
            _emit_workload(f12_rng, out, w, day, offset, midnight)

    # The newer tables use their own generators so they do not shift the data above.
    out["compute_clusters"] = _cluster_rows()
    out["access_audit"] = _audit_rows(random.Random(seed + 1), days, today)
    out["query_history"] = _query_history_rows(random.Random(seed + 2), days, today)
    return out


# ---------------------------------------------------------------------------
# compute.clusters, access.audit, query.history
# ---------------------------------------------------------------------------

BI_CLUSTER = "0202-bi-tableau-cluster"
BI_WAREHOUSE = "wh-bi-001"


def _cluster_row(w, auto_term, change_time, **overrides) -> dict[str, Any]:
    is_job_cluster = w["kind"] == "JOB" and w.get("cluster_source", "JOB") == "JOB"
    owner = w.get("run_as") or (w.get("run_as_pool") or [None])[0]
    row = {
        "account_id": ACCOUNT_ID, "workspace_id": w["workspace_id"], "cluster_id": w["cluster"],
        "cluster_name": w.get("name") or w["cluster"], "owned_by": owner,
        "create_time": datetime(2025, 1, 15), "delete_time": None,
        "driver_node_type": "i3.xlarge" if w["kind"] == "JOB" else "r5.2xlarge",
        "worker_node_type": "i3.xlarge" if w["kind"] == "JOB" else "r5.2xlarge",
        "worker_count": 2, "min_autoscale_workers": None, "max_autoscale_workers": None,
        # Job clusters terminate with their run; most report no auto-termination setting.
        "auto_termination_minutes": w.get("job_cluster_autoterm") if is_job_cluster else auto_term,
        "enable_elastic_disk": True, "tags": dict(w.get("tags") or {}),
        "cluster_source": "JOB" if is_job_cluster else w.get("cluster_source", "UI"),
        "init_scripts": [], "driver_instance_pool_id": None, "worker_instance_pool_id": None,
        "dbr_version": "15.4.x-scala2.12",
        "data_security_mode": "SINGLE_USER" if is_job_cluster else "USER_ISOLATION",
        "policy_id": None, "change_time": change_time, "change_date": change_time.date(),
    }
    row.update(overrides)
    return row


def _cluster_rows() -> list[dict[str, Any]]:
    rows = []
    for w in WORKLOADS + F12_WORKLOADS:
        if not w.get("cluster") or w.get("existing_cluster"):
            continue
        for i, old in enumerate(w.get("auto_term_history", [])):
            rows.append(_cluster_row(w, old, datetime(2025, 1, 15) + timedelta(days=30 * i)))
        rows.append(_cluster_row(w, w.get("auto_term"), datetime(2025, 6, 1)))

    # Auto-termination is off, but the cluster was deleted, so it must not be flagged.
    deleted = {"kind": "CLUSTER", "workspace_id": DEV_WS, "cluster": "0606-deleted-old-cluster",
               "tags": {}, "run_as_pool": ["kim@example.com"]}
    # It was live (auto-termination 0, not deleted) before the delete row, so the latest row wins.
    rows.append(_cluster_row(deleted, 0, datetime(2025, 1, 15)))
    rows.append(_cluster_row(deleted, 0, datetime(2025, 3, 1), delete_time=datetime(2025, 3, 1)))
    return rows


# (user, user_agent, compute key, compute id): BI tools reach the cluster through JDBC/ODBC.
# The warehouse client is the correct way to serve BI and must not be flagged (F4 near-miss).
#
# ASSUMPTION (check on a real account before relying on it): the audit reference documents
# databrickssql.commandSubmit for SQL warehouses only (request_params.warehouseId). It documents
# no event that ties a JDBC/ODBC session to an all-purpose cluster id, so the clusterId rows
# below model the signal F4 wants, not a documented event. The documented, real signal is the
# accounts.tokenLogin event below: user_agent names the driver, but there is no cluster id.
AUDIT_CLIENTS = [
    ("svc-tableau@example.com", "SimbaSparkJDBCDriver/2.6.36 (Tableau 2024.2)", "clusterId", BI_CLUSTER),
    ("olivia@example.com", "Simba Spark ODBC Driver/2.8.2 (PowerBI Desktop)", "clusterId", BI_CLUSTER),
    ("ivan@example.com", "SimbaSparkJDBCDriver/2.6.36 (Tableau 2024.2)", "warehouseId", BI_WAREHOUSE),
]
# Token logins (documented: service accounts, action tokenLogin, params tokenId/user/
# authenticationMethod). The SDK login is the F4 near-miss: a PAT client that is not a BI driver.
TOKEN_LOGINS = [
    ("svc-tableau@example.com", "SimbaSparkJDBCDriver/2.6.36 (Tableau 2024.2)", "dapi-tableau-01"),
    ("olivia@example.com", "Simba Spark ODBC Driver/2.8.2 (PowerBI Desktop)", "dapi-powerbi-02"),
    ("svc-etl@example.com", "databricks-sdk-py/0.38.0 python/3.11.9 os/linux auth/pat", "dapi-sdk-03"),
]
BROWSER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 Chrome/126.0"
SHARED_CLUSTER = "0101-shared-ds-cluster"
SHARED_USERS = ["frank@example.com", "grace@example.com", "heidi@example.com"]


def _audit_row(rng, day, ts, user, agent, service, action, params, workspace_id=PROD_WS) -> dict[str, Any]:
    return {
        "version": "2.0", "event_time": ts, "event_date": day,
        "account_id": ACCOUNT_ID, "workspace_id": workspace_id,
        "source_ip_address": f"10.0.{rng.randint(0, 9)}.{rng.randint(1, 254)}",
        "user_agent": agent, "session_id": f"sess-{rng.getrandbits(32):08x}",
        "user_identity": {"email": user, "subject_name": None},
        "service_name": service, "action_name": action,
        "request_id": f"req-{rng.getrandbits(48):012x}",
        "request_params": params,
        "response": {"status_code": 200, "error_message": None, "result": None},
        "audit_level": "WORKSPACE_LEVEL", "event_id": f"evt-{rng.getrandbits(48):012x}",
        "identity_metadata": {"run_by": user, "run_as": user, "acting_resource": None},
    }


def _audit_rows(rng, days: int, today: date) -> list[dict[str, Any]]:
    rows = []
    for offset in range(days, 0, -1):
        day = today - timedelta(days=offset)
        midnight = datetime.combine(day, datetime.min.time())
        # F4 BI_TOOL_JDBC positive (clusterId) and near-miss (warehouseId).
        for user, agent, key, compute in AUDIT_CLIENTS:
            for _ in range(rng.randint(4, 8)):
                ts = midnight + timedelta(hours=rng.uniform(8, 18))
                rows.append(_audit_row(rng, day, ts, user, agent, "databrickssql", "commandSubmit",
                                       {key: compute, "commandId": f"cmd-{rng.getrandbits(32):08x}"}))
        for user, agent, token in TOKEN_LOGINS:
            ts = midnight + timedelta(hours=rng.uniform(7, 9))
            rows.append(_audit_row(rng, day, ts, user, agent, "accounts", "tokenLogin",
                                   {"tokenId": token, "user": user,
                                    "authenticationMethod": "API_INT_PAT_TOKEN"}))
        # F4 SHARED_USERS: three people attach notebooks to the same all-purpose cluster
        # (documented notebook.attachNotebook carries clusterId; runCommand does not).
        for user in SHARED_USERS:
            notebook = f"nb-{rng.randint(1000, 1009)}"
            ts = midnight + timedelta(hours=rng.uniform(9, 11))
            rows.append(_audit_row(rng, day, ts, user, BROWSER_AGENT, "notebook", "attachNotebook",
                                   {"notebookId": notebook, "clusterId": SHARED_CLUSTER,
                                    "path": f"/Users/{user}/analysis"}))
            for _ in range(rng.randint(2, 5)):
                ts = midnight + timedelta(hours=rng.uniform(11, 16))
                rows.append(_audit_row(rng, day, ts, user, BROWSER_AGENT, "notebook", "runCommand",
                                       {"notebookId": notebook, "commandLanguage": "python",
                                        "status": "finished",
                                        "executionTime": f"{rng.uniform(0.5, 30):.2f}"}))
        # F4 near-miss: a single user on 0404 is not a shared cluster.
        ts = midnight + timedelta(hours=rng.uniform(9, 11))
        rows.append(_audit_row(rng, day, ts, "nina@example.com", BROWSER_AGENT, "notebook", "attachNotebook",
                               {"notebookId": "nb-2001", "clusterId": "0404-zombie-autoterm-off",
                                "path": "/Users/nina@example.com/explore"}))
    return rows


# (catalog, tables, users, client, queries per day, mean execution seconds).
# Execution-time shares come out near sales 50%, finance 30%, marketing 15%.
QUERY_CATALOGS = [
    ("sales", ["sales.gold.orders", "sales.gold.customers"], ["ivan@example.com", "liam@example.com"],
     "Tableau", 30, 20),
    ("finance", ["finance.gold.ledger", "finance.gold.invoices"], ["erin@example.com"],
     "Power BI", 12, 30),
    ("marketing", ["marketing.gold.campaigns"], ["mia@example.com"],
     "Databricks SQL Editor", 10, 18),
]
QUERY_TEMPLATES = [
    "SELECT region, SUM(amount) AS total FROM {t} WHERE event_date >= current_date() - 7 GROUP BY region",
    "SELECT * FROM {t} ORDER BY updated_at DESC LIMIT 1000",
    "SELECT COUNT(*) FROM {t}",
]


# client_driver values follow the documented examples ("Databricks JDBC Driver", ...).
CLIENT_DRIVERS = {"Tableau": "Databricks JDBC Driver", "Power BI": "Databricks ODBC Driver",
                  "Databricks SQL Editor": None}
FIN_WAREHOUSE = "wh-fin-002"


def _query_row(rng, ts, user, text, client, exec_ms, status="FINISHED", cached=False,
               warehouse=BI_WAREHOUSE) -> dict[str, Any]:
    compile_ms = 0 if cached else rng.randint(50, 400)
    wait_ms = 0 if cached else rng.randint(0, 2000)
    total = exec_ms + compile_ms + wait_ms
    return {
        "account_id": ACCOUNT_ID, "workspace_id": PROD_WS,
        "statement_id": f"stmt-{rng.getrandbits(64):016x}", "session_id": f"sess-{rng.getrandbits(32):08x}",
        "execution_status": status,
        "compute": {"type": "WAREHOUSE", "cluster_id": None, "warehouse_id": warehouse},
        "executed_by": user, "executed_by_user_id": str(zlib.crc32(user.encode()) % 10**8),
        "statement_text": text, "statement_type": text.split()[0].upper(),
        "error_message": "Query failed: [TABLE_OR_VIEW_NOT_FOUND]" if status == "FAILED" else None,
        "client_application": client, "client_driver": CLIENT_DRIVERS[client],
        "cache_origin_statement_id": f"stmt-{rng.getrandbits(64):016x}" if cached else None,
        "total_duration_ms": total, "execution_duration_ms": exec_ms,
        "compilation_duration_ms": compile_ms, "waiting_for_compute_duration_ms": wait_ms,
        "waiting_at_capacity_duration_ms": 0, "total_task_duration_ms": exec_ms * rng.randint(2, 8),
        "result_fetch_duration_ms": rng.randint(5, 500),
        "read_rows": 0 if cached else rng.randint(10_000, 5_000_000),
        "read_bytes": 0 if cached else rng.randint(10**6, 2 * 10**9),
        "produced_rows": rng.randint(1, 1000), "from_result_cache": cached,
        "start_time": ts, "end_time": ts + timedelta(milliseconds=total),
        "update_time": ts + timedelta(milliseconds=total),
        "query_source": {"job_info": None, "alert_id": None, "sql_query_id": None, "dashboard_id": None,
                         "legacy_dashboard_id": None, "notebook_id": None, "genie_space_id": None},
        "executed_as": user, "executed_as_user_id": str(zlib.crc32(user.encode()) % 10**8),
        "query_tags": {},
    }


def _query_history_rows(rng, days: int, today: date) -> list[dict[str, Any]]:
    rows = []
    for offset in range(days, 0, -1):
        midnight = datetime.combine(today - timedelta(days=offset), datetime.min.time())

        def ts():
            return midnight + timedelta(hours=rng.uniform(8, 18))

        for _catalog, tables, users, client, per_day, mean_s in QUERY_CATALOGS:
            for _ in range(per_day):
                text = rng.choice(QUERY_TEMPLATES).format(t=rng.choice(tables))
                status = "FAILED" if rng.random() < 0.03 else "FINISHED"
                cached = status == "FINISHED" and rng.random() < 0.1
                exec_ms = 0 if cached else int(max(0.2, rng.gauss(mean_s, mean_s * 0.3)) * 1000)
                rows.append(_query_row(rng, ts(), rng.choice(users), text, client, exec_ms, status, cached))

        # Edge cases: a cross-catalog join, queries with no catalog, and a cancelled statement.
        rows.append(_query_row(
            rng, ts(), "liam@example.com",
            "SELECT c.campaign, SUM(o.amount) FROM sales.gold.orders o "
            "JOIN marketing.gold.campaigns c ON o.campaign_id = c.id GROUP BY c.campaign",
            "Tableau", rng.randint(30_000, 60_000)))
        rows.append(_query_row(rng, ts(), "ivan@example.com", "SELECT 1", "Tableau", rng.randint(5, 50)))
        rows.append(_query_row(rng, ts(), "mia@example.com", "SHOW TABLES", "Databricks SQL Editor",
                               rng.randint(50, 300)))
        rows.append(_query_row(rng, ts(), "erin@example.com", "SELECT * FROM finance.gold.ledger",
                               "Power BI", rng.randint(1_000, 5_000), status="CANCELED"))

        # F3 parsing cases on wh-bi-001: catalog names are case-insensitive and may be
        # backticked, so these count as sales and finance. The two-part name has no catalog in
        # the text, and query.history records neither the session's current catalog nor a
        # catalog in query_source, so F3 must leave it UNATTRIBUTED (not guess a default).
        rows.append(_query_row(rng, ts(), "ivan@example.com",
                               "SELECT region, SUM(amount) FROM `sales`.`gold`.`orders` GROUP BY region",
                               "Tableau", rng.randint(10_000, 30_000)))
        rows.append(_query_row(rng, ts(), "erin@example.com",
                               "SELECT * FROM Finance.Gold.Invoices WHERE paid = false",
                               "Power BI", rng.randint(10_000, 30_000)))
        rows.append(_query_row(rng, ts(), "liam@example.com", "SELECT COUNT(*) FROM gold.orders",
                               "Tableau", rng.randint(2_000, 8_000)))

        # F3 near-miss: wh-fin-002 only ever touches the finance catalog.
        for _ in range(8):
            text = rng.choice(QUERY_TEMPLATES).format(t=rng.choice(["finance.gold.ledger", "finance.gold.invoices"]))
            rows.append(_query_row(rng, ts(), "erin@example.com", text, "Power BI",
                                   int(max(0.2, rng.gauss(25, 7)) * 1000), warehouse=FIN_WAREHOUSE))
    return rows


def generate_synthetic_system_tables(
    spark: Any,
    target_schema: str,
    days: int = 45,
    seed: int = 42,
    today: date | None = None,
) -> dict[str, int]:
    """Create (or replace) the synthetic System Tables under ``target_schema``.

    ``target_schema`` is a two-part name such as ``workspace.finops_synthetic`` (Databricks)
    or ``spark_catalog.synthetic_system`` (local).
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
    schemas = _schemas()
    rows = build_synthetic_rows(days=days, seed=seed, today=today)
    counts = {}
    for table, data in rows.items():
        df = spark.createDataFrame(data, schema=schemas[table])
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
            .saveAsTable(f"{target_schema}.{table}")
        counts[table] = len(data)
    return counts


# ---------------------------------------------------------------------------
# Redirect system.* to the synthetic tables
# ---------------------------------------------------------------------------

class RedirectingSpark:
    """Proxy around SparkSession that rewrites system table names in sql() and table().

    Matching is case-insensitive and allows each name part to be backticked
    (`system`.`billing`.`usage`). Text inside '...' or "..." string literals is left alone.
    A single backticked identifier holding the whole dotted name is a different table in
    Spark, so it is not rewritten. Comments are not parsed: a name in a comment is rewritten,
    which is harmless.
    """

    _LITERAL = re.compile(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"")

    def __init__(self, spark: Any, source_schema: str) -> None:
        self._spark = spark
        self._source_schema = source_schema
        names = sorted(SYSTEM_TABLE_MAP, key=len, reverse=True)

        def name_pattern(name: str) -> str:
            return r"\.".join(rf"(?:`{re.escape(p)}`|{re.escape(p)})" for p in name.split("."))

        self._pattern = re.compile(
            r"(?<![\w.`])(" + "|".join(name_pattern(n) for n in names) + r")(?![\w`])",
            re.IGNORECASE,
        )

    def _replace(self, m: re.Match) -> str:
        key = m.group(1).replace("`", "").lower()
        return f"{self._source_schema}.{SYSTEM_TABLE_MAP[key]}"

    def _rewrite(self, text: str) -> str:
        out, pos = [], 0
        for lit in self._LITERAL.finditer(text):
            out.append(self._pattern.sub(self._replace, text[pos:lit.start()]))
            out.append(lit.group(0))
            pos = lit.end()
        out.append(self._pattern.sub(self._replace, text[pos:]))
        return "".join(out)

    def sql(self, query: str, *args: Any, **kwargs: Any):
        return self._spark.sql(self._rewrite(query), *args, **kwargs)

    def table(self, name: str):
        return self._spark.table(self._rewrite(name))

    def __getattr__(self, item: str) -> Any:
        return getattr(self._spark, item)


# ---------------------------------------------------------------------------
# Run the real accelerator pipeline
# ---------------------------------------------------------------------------

def run_accelerator(
    spark: Any,
    source_schema: str,
    catalog: str,
    schema: str,
    bundle_root: str,
    lookback_days: int = 30,
):
    """Run the upstream pipeline plus every cockpit step against the synthetic tables."""
    from cockpit.pipeline import run_cockpit_pipeline
    from databricks_finops.config import config_from_args, parse_args
    from databricks_finops.main import _run_id
    from databricks_finops.run_logging import StepLogger, write_run_log

    proxy = RedirectingSpark(spark, source_schema)
    config = config_from_args(parse_args([
        "--catalog", catalog,
        "--schema", schema,
        "--lookback-days", str(lookback_days),
        "--bundle-root", bundle_root,
    ]))
    run_id = _run_id()
    logger = StepLogger(run_id)
    try:
        run_cockpit_pipeline(proxy, config, run_id, logger)
    finally:
        try:
            write_run_log(proxy, catalog, schema, logger)
        except Exception as exc:  # noqa: BLE001
            print(f"run log write failed: {exc}")
    return run_id, logger
