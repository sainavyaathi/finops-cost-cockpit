from __future__ import annotations

from typing import Any

from .config import AppConfig
from .spark_utils import comment_on_table, load_sql, qname, sql_string


def create_accelerator_summary(spark: Any, config: AppConfig, run_id: str) -> str:
    spark.sql(build_accelerator_summary_sql(config, run_id))
    return "accelerator_summary refreshed"


def build_accelerator_summary_sql(config: AppConfig, run_id: str) -> str:
    return load_sql(
        "accelerator_summary",
        target_table=qname(config.catalog, config.schema, "accelerator_summary"),
        daily_cost_table=qname(config.catalog, config.schema, "daily_cost"),
        workload_table=qname(config.catalog, config.schema, "workload_cost_summary"),
        candidates_table=qname(config.catalog, config.schema, "optimization_candidates"),
        reliability_table=qname(config.catalog, config.schema, "job_reliability_summary"),
        tagging_table=qname(config.catalog, config.schema, "tagging_quality_summary"),
        run_id_sql=sql_string(run_id),
    )


def comment_summary_table(spark: Any, config: AppConfig) -> None:
    comments = {
        "accelerator_summary": (
            "Executive FinOps summary for the latest accelerator run. Cost values are "
            "estimated from Databricks System Tables and are not exact invoices."
        ),
        "accelerator_health": (
            "Health checks showing source table coverage, pricing quality, attribution "
            "quality, and output limitations."
        ),
    }
    for table_name, comment in comments.items():
        comment_on_table(spark, qname(config.catalog, config.schema, table_name), comment)
