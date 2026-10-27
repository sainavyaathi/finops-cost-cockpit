"""Sandbox helpers for running the FinOps accelerator against synthetic System Tables.

Two pieces:

1. ``generate_synthetic_system_tables`` writes six Delta tables whose schemas mirror the
   Databricks System Tables the accelerator reads (billing.usage, billing.list_prices,
   compute.node_timeline, lakeflow.jobs, lakeflow.job_run_timeline,
   lakeflow.job_task_run_timeline). The data is deterministic (seeded) and is shaped so
   every classification path in the accelerator lights up: healthy jobs, oversized jobs,
   memory-bound jobs, flaky jobs, shared all-purpose clusters, untagged spend, fallback
   pricing, and so on.

2. ``RedirectingSpark`` wraps a SparkSession and rewrites ``system.<schema>.<table>``
   references to the synthetic tables, so the accelerator code runs completely unmodified.

Works on a local PySpark + Delta session and on Databricks (including Free Edition).
"""

from __future__ import annotations

import random
import re
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

SYSTEM_TABLE_MAP = {
    "system.billing.usage": "billing_usage",
    "system.billing.list_prices": "billing_list_prices",
    "system.compute.node_timeline": "compute_node_timeline",
    "system.lakeflow.jobs": "lakeflow_jobs",
    "system.lakeflow.job_run_timeline": "lakeflow_job_run_timeline",
    "system.lakeflow.job_task_run_timeline": "lakeflow_job_task_run_timeline",
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
        "cpu": 65, "mem": 60, "tags": FULL_TAGS, "run_as": "svc-etl@example.com",
        "expect": "HEALTHY_EXPENSIVE / LOOKS_HEALTHY",
    },
    {
        "kind": "JOB", "workspace_id": PROD_WS, "job_id": "1002", "name": "ml_feature_training",
        "sku": "PREMIUM_JOBS_COMPUTE", "product": "JOBS", "cluster": "job-1002-cluster",
        "daily_dbus": 160, "runs_per_day": 1, "failure_rate": 0.0,
        "cpu": 7, "mem": 22,
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
        "tags": {"team": "data-science"},
        "run_as_pool": ["frank@example.com", "grace@example.com", "heidi@example.com"],
        "expect": "SHARED_CLUSTER_ATTRIBUTION + low utilization + missing tags",
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


# ---------------------------------------------------------------------------
# Schemas (subset of the real System Table columns; all columns the accelerator reads)
# ---------------------------------------------------------------------------

def _schemas():
    from pyspark.sql.types import (
        ArrayType, BooleanType, DateType, DecimalType, DoubleType, LongType,
        MapType, StringType, StructField, StructType, TimestampType,
    )

    s = StringType()
    usage_metadata = StructType([
        StructField("cluster_id", s), StructField("job_id", s), StructField("warehouse_id", s),
        StructField("instance_pool_id", s), StructField("node_type", s),
        StructField("job_run_id", s), StructField("notebook_id", s),
        StructField("dlt_pipeline_id", s), StructField("endpoint_name", s),
        StructField("endpoint_id", s), StructField("dlt_update_id", s),
        StructField("job_name", s), StructField("notebook_path", s),
    ])
    identity_metadata = StructType([
        StructField("run_as", s), StructField("owned_by", s), StructField("created_by", s),
    ])
    billing_usage = StructType([
        StructField("account_id", s), StructField("workspace_id", s),
        StructField("record_id", s), StructField("sku_name", s), StructField("cloud", s),
        StructField("usage_start_time", TimestampType()),
        StructField("usage_end_time", TimestampType()),
        StructField("usage_date", DateType()),
        StructField("custom_tags", MapType(s, s)),
        StructField("usage_unit", s),
        StructField("usage_quantity", DecimalType(38, 18)),
        StructField("usage_metadata", usage_metadata),
        StructField("identity_metadata", identity_metadata),
        StructField("record_type", s),
        StructField("ingestion_date", DateType()),
        StructField("billing_origin_product", s),
        StructField("usage_type", s),
    ])

    price_struct = StructType([StructField("default", DecimalType(30, 18))])
    list_prices = StructType([
        StructField("price_start_time", TimestampType()),
        StructField("price_end_time", TimestampType()),
        StructField("account_id", s), StructField("sku_name", s), StructField("cloud", s),
        StructField("currency_code", s), StructField("usage_unit", s),
        StructField("pricing", StructType([
            StructField("default", DecimalType(30, 18)),
            StructField("promotional", price_struct),
            StructField("effective_list", price_struct),
        ])),
    ])

    node_timeline = StructType([
        StructField("account_id", s), StructField("workspace_id", s),
        StructField("cluster_id", s), StructField("instance_id", s),
        StructField("start_time", TimestampType()), StructField("end_time", TimestampType()),
        StructField("driver", BooleanType()),
        StructField("cpu_user_percent", DoubleType()), StructField("cpu_system_percent", DoubleType()),
        StructField("cpu_wait_percent", DoubleType()), StructField("mem_used_percent", DoubleType()),
        StructField("mem_swap_percent", DoubleType()),
        StructField("network_sent_bytes", LongType()), StructField("network_received_bytes", LongType()),
        StructField("node_type", s),
    ])

    jobs = StructType([
        StructField("account_id", s), StructField("workspace_id", s), StructField("job_id", s),
        StructField("name", s), StructField("description", s), StructField("creator_id", s),
        StructField("tags", MapType(s, s)), StructField("change_time", TimestampType()),
        StructField("delete_time", TimestampType()), StructField("run_as", s),
    ])

    job_run_timeline = StructType([
        StructField("account_id", s), StructField("workspace_id", s), StructField("job_id", s),
        StructField("run_id", s),
        StructField("period_start_time", TimestampType()), StructField("period_end_time", TimestampType()),
        StructField("trigger_type", s), StructField("run_type", s), StructField("run_name", s),
        StructField("compute_ids", ArrayType(s)), StructField("result_state", s),
        StructField("termination_code", s),
    ])

    # Matches the real table: run_id is the *task* run id, job_run_id is the parent job run.
    task_run_timeline = StructType([
        StructField("account_id", s), StructField("workspace_id", s), StructField("job_id", s),
        StructField("run_id", s), StructField("job_run_id", s), StructField("parent_run_id", s),
        StructField("period_start_time", TimestampType()), StructField("period_end_time", TimestampType()),
        StructField("task_key", s), StructField("compute_ids", ArrayType(s)),
        StructField("result_state", s), StructField("termination_code", s),
    ])

    return {
        "billing_usage": billing_usage,
        "billing_list_prices": list_prices,
        "compute_node_timeline": node_timeline,
        "lakeflow_jobs": jobs,
        "lakeflow_job_run_timeline": job_run_timeline,
        "lakeflow_job_task_run_timeline": task_run_timeline,
    }


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


def build_synthetic_rows(days: int = 45, seed: int = 42, today: date | None = None) -> dict[str, list[dict]]:
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
    for w in WORKLOADS:
        if w["kind"] != "JOB":
            continue
        base = {
            "account_id": ACCOUNT_ID, "workspace_id": w["workspace_id"], "job_id": w["job_id"],
            "description": None, "creator_id": "123", "tags": dict(w.get("tags") or {}),
            "delete_time": None, "run_as": w.get("run_as"),
        }
        if w["job_id"] == "1004":
            out["lakeflow_jobs"].append({**base, "name": "vendor_ingest_v1", "change_time": datetime(2024, 1, 1)})
        out["lakeflow_jobs"].append({**base, "name": w["name"], "change_time": datetime(2025, 6, 1)})

    for offset in range(days, 0, -1):
        day = today - timedelta(days=offset)
        midnight = datetime.combine(day, datetime.min.time())
        for w in WORKLOADS:
            jitter = rng.uniform(0.8, 1.2)
            kind = w["kind"]

            if kind == "JOB":
                runs = w["runs_per_day"]
                for n in range(runs):
                    run_id = f"{w['job_id']}{offset:03d}{n}"
                    start = midnight + timedelta(hours=1 + n * 6, minutes=rng.randint(0, 20))
                    failed = rng.random() < w["failure_rate"]
                    hours = rng.uniform(0.5, 2.5) * (0.4 if failed else 1.0)
                    end = start + timedelta(hours=hours)
                    dbus = w["daily_dbus"] * jitter / runs * (0.5 if failed else 1.0)
                    state = "FAILED" if failed else "SUCCEEDED"

                    out["billing_usage"].append(_usage_row(
                        rng, w, day, start, end, dbus, w.get("run_as"),
                        job_id=w["job_id"], job_run_id=run_id, cluster_id=w.get("cluster"),
                        node_type="i3.xlarge" if w.get("cluster") else None,
                    ))
                    compute = [w["cluster"]] if w.get("cluster") else []
                    out["lakeflow_job_run_timeline"].append({
                        "account_id": ACCOUNT_ID, "workspace_id": w["workspace_id"], "job_id": w["job_id"],
                        "run_id": run_id, "period_start_time": start, "period_end_time": end,
                        "trigger_type": "CRON", "run_type": "JOB_RUN", "run_name": w["name"],
                        "compute_ids": compute, "result_state": state,
                        "termination_code": "RUN_EXECUTION_ERROR" if failed else "SUCCESS",
                    })

                    for t, task_key in enumerate(["extract", "transform", "load"]):
                        attempts = 1
                        if w.get("task_retries") and task_key == "transform" and rng.random() < 0.6:
                            attempts = 2
                        for a in range(attempts):
                            last = a == attempts - 1
                            t_state = state if last else "FAILED"
                            out["lakeflow_job_task_run_timeline"].append({
                                "account_id": ACCOUNT_ID, "workspace_id": w["workspace_id"],
                                "job_id": w["job_id"], "run_id": f"{run_id}{t}{a}",
                                "job_run_id": run_id, "parent_run_id": run_id,
                                "period_start_time": start + timedelta(minutes=10 * t + 3 * a),
                                "period_end_time": start + timedelta(minutes=10 * t + 3 * a + 8),
                                "task_key": task_key, "compute_ids": compute,
                                "result_state": t_state,
                                "termination_code": "SUCCESS" if t_state == "SUCCEEDED" else "RUN_EXECUTION_ERROR",
                            })

                    out["compute_node_timeline"].extend(
                        _node_rows(rng, w, day, 1 + n * 6, hours)
                    )

            elif kind == "CLUSTER":
                for user in w["run_as_pool"]:
                    start = midnight + timedelta(hours=9 + rng.randint(0, 3))
                    end = start + timedelta(hours=rng.uniform(1, 4))
                    out["billing_usage"].append(_usage_row(
                        rng, w, day, start, end, w["daily_dbus"] * jitter / len(w["run_as_pool"]), user,
                        cluster_id=w["cluster"], node_type="r5.2xlarge",
                    ))
                out["compute_node_timeline"].extend(_node_rows(rng, w, day, 9, 8))

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

    return out


def generate_synthetic_system_tables(
    spark: Any,
    target_schema: str,
    days: int = 45,
    seed: int = 42,
) -> dict[str, int]:
    """Create (or replace) the synthetic System Tables under ``target_schema``.

    ``target_schema`` is a two-part name such as ``workspace.finops_synthetic`` (Databricks)
    or ``spark_catalog.synthetic_system`` (local).
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
    schemas = _schemas()
    rows = build_synthetic_rows(days=days, seed=seed)
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
    """Proxy around SparkSession that rewrites system table names in sql() and table()."""

    def __init__(self, spark: Any, source_schema: str) -> None:
        self._spark = spark
        self._source_schema = source_schema
        names = sorted(SYSTEM_TABLE_MAP, key=len, reverse=True)
        self._pattern = re.compile(r"\b(" + "|".join(re.escape(n) for n in names) + r")\b")

    def _rewrite(self, text: str) -> str:
        return self._pattern.sub(
            lambda m: f"{self._source_schema}.{SYSTEM_TABLE_MAP[m.group(1)]}", text
        )

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
    """Run databricks_finops.main.run_pipeline against the synthetic tables."""
    from databricks_finops.config import config_from_args, parse_args
    from databricks_finops.main import _run_id, run_pipeline
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
        run_pipeline(proxy, config, run_id, logger)
    finally:
        try:
            write_run_log(proxy, catalog, schema, logger)
        except Exception as exc:  # noqa: BLE001
            print(f"run log write failed: {exc}")
    return run_id, logger
