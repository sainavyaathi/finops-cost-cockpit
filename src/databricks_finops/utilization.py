from __future__ import annotations

from typing import Any

from .config import AppConfig
from .preflight import PreflightResult
from .spark_utils import comment_on_table, first_existing, load_sql, qname, sql_string


def _metric_expr(columns: set[str], candidates: list[str]) -> str:
    column = first_existing(columns, candidates)
    if column is None:
        return "CAST(NULL AS DOUBLE)"
    return f"CAST({column} AS DOUBLE)"


def build_compute_utilization_placeholder_sql(config: AppConfig, run_id: str) -> str:
    return load_sql(
        "compute_utilization_placeholder",
        target_table=qname(config.catalog, config.schema, "compute_utilization_summary"),
        workload_table=qname(config.catalog, config.schema, "workload_cost_summary"),
        run_id_sql=sql_string(run_id),
    )


def build_compute_utilization_sql(
    config: AppConfig,
    run_id: str,
    preflight: PreflightResult,
) -> str:
    node_columns = preflight.columns("system.compute.node_timeline")
    thresholds = config.thresholds
    return load_sql(
        "compute_utilization_summary",
        target_table=qname(config.catalog, config.schema, "compute_utilization_summary"),
        daily_cost_table=qname(config.catalog, config.schema, "daily_cost"),
        run_id_sql=sql_string(run_id),
        cpu_user_expr=_metric_expr(node_columns, ["cpu_user_percent"]),
        cpu_system_expr=_metric_expr(node_columns, ["cpu_system_percent"]),
        memory_expr=_metric_expr(node_columns, ["mem_used_percent"]),
        network_sent_expr=_metric_expr(
            node_columns,
            ["network_sent_bytes", "network_bytes_sent", "network_transmitted_bytes"],
        ),
        network_received_expr=_metric_expr(
            node_columns,
            ["network_received_bytes", "network_bytes_received", "network_received"],
        ),
        lookback_days=str(config.lookback_days),
        low_cpu_pct=f"{thresholds.low_cpu_pct:.12g}",
        low_memory_pct=f"{thresholds.low_memory_pct:.12g}",
        high_memory_pct=f"{thresholds.high_memory_pct:.12g}",
        high_cost_low_utilization_min_cost=(
            f"{thresholds.high_cost_low_utilization_min_cost:.12g}"
        ),
    )


def create_compute_utilization_summary(
    spark: Any,
    config: AppConfig,
    run_id: str,
    preflight: PreflightResult,
) -> str:
    if not preflight.available("system.compute.node_timeline"):
        spark.sql(build_compute_utilization_placeholder_sql(config, run_id))
        return "node_timeline unavailable; compute utilization created with INSUFFICIENT_DATA"

    try:
        spark.sql(build_compute_utilization_sql(config, run_id, preflight))
        return "compute_utilization_summary built at CLUSTER_LOOKBACK_WINDOW grain"
    except Exception:
        spark.sql(build_compute_utilization_placeholder_sql(config, run_id))
        return "compute utilization SQL failed; placeholder created with INSUFFICIENT_DATA"


def comment_utilization_table(spark: Any, config: AppConfig) -> None:
    comment_on_table(
        spark,
        qname(config.catalog, config.schema, "compute_utilization_summary"),
        (
            "Compute utilization and sizing signals derived from system.compute.node_timeline "
            "where available."
        ),
    )
