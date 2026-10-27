from __future__ import annotations

from typing import Any

from .config import AppConfig
from .scoring import priority_score_sql_expr
from .spark_utils import comment_on_table, load_sql, qname, sql_string


def create_optimization_candidates(spark: Any, config: AppConfig, run_id: str) -> str:
    spark.sql(build_optimization_candidates_sql(config, run_id))
    return "optimization_candidates generated with transparent 0-100 priority scoring"


def build_optimization_candidates_sql(config: AppConfig, run_id: str) -> str:
    workload = qname(config.catalog, config.schema, "workload_cost_summary")
    utilization = qname(config.catalog, config.schema, "compute_utilization_summary")
    reliability = qname(config.catalog, config.schema, "job_reliability_summary")
    tagging = qname(config.catalog, config.schema, "tagging_quality_summary")
    target = qname(config.catalog, config.schema, "optimization_candidates")
    run_id_sql = sql_string(run_id)
    multiplier = f"{30.0 / config.lookback_days:.12g}"
    priority_expr = priority_score_sql_expr(weights=config.scoring_weights.as_dict())
    thresholds = config.thresholds

    return load_sql(
        "optimization_candidates",
        target_table=target,
        tagging_table=tagging,
        workload_table=workload,
        utilization_table=utilization,
        reliability_table=reliability,
        run_id_sql=run_id_sql,
        monthly_multiplier=multiplier,
        lookback_days=str(config.lookback_days),
        default_missing_required_tag_count=str(len(config.tagging_rules.required_tags)),
        default_missing_critical_tag_count=str(len(config.tagging_rules.critical_tags)),
        high_failed_cost=f"{thresholds.high_failed_cost:.12g}",
        high_failure_rate_pct=f"{thresholds.high_failure_rate_pct:.12g}",
        retry_heavy_count=f"{thresholds.retry_heavy_count:.12g}",
        expensive_monthly_cost=f"{thresholds.expensive_monthly_cost:.12g}",
        priority_score_expr=priority_expr,
    )


def comment_optimization_table(spark: Any, config: AppConfig) -> None:
    comment_on_table(
        spark,
        qname(config.catalog, config.schema, "optimization_candidates"),
        (
            "Prioritized advisory optimization backlog combining cost, utilization, "
            "reliability, tagging, attribution, and confidence signals."
        ),
    )
