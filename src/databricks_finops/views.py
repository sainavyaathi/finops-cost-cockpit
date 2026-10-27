from __future__ import annotations

from typing import Any

from .config import AppConfig, OUTPUT_VIEWS
from .spark_utils import load_sql, qname


def create_views(spark: Any, config: AppConfig) -> str:
    for view_name in OUTPUT_VIEWS:
        spark.sql(build_view_sql_statements(config)[view_name])

    return f"{len(OUTPUT_VIEWS)} dashboard-ready views created"


def build_view_sql_statements(config: AppConfig) -> dict[str, str]:
    template_args = {
        "daily_table": qname(config.catalog, config.schema, "daily_cost"),
        "workload_table": qname(config.catalog, config.schema, "workload_cost_summary"),
        "utilization_table": qname(config.catalog, config.schema, "compute_utilization_summary"),
        "reliability_table": qname(config.catalog, config.schema, "job_reliability_summary"),
        "tagging_table": qname(config.catalog, config.schema, "tagging_quality_summary"),
        "candidates_table": qname(config.catalog, config.schema, "optimization_candidates"),
        "summary_table": qname(config.catalog, config.schema, "accelerator_summary"),
        "health_table": qname(config.catalog, config.schema, "accelerator_health"),
    }

    return {
        view_name: load_sql(
            f"views/{view_name}",
            view_name=qname(config.catalog, config.schema, view_name),
            **template_args,
        )
        for view_name in OUTPUT_VIEWS
    }
