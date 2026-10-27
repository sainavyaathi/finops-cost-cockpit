from __future__ import annotations

from typing import Any

from .config import AppConfig
from .spark_utils import comment_on_table, load_sql, qname, sql_string


TAG_FLAG_COLUMNS = {
    "project": "missing_project_tag",
    "team": "missing_team_tag",
    "owner": "missing_owner_tag",
    "environment": "missing_environment_tag",
    "cost_center": "missing_cost_center_tag",
}


def create_tagging_quality_summary(spark: Any, config: AppConfig, run_id: str) -> str:
    spark.sql(build_tagging_quality_sql(config, run_id))
    return "tagging_quality_summary refreshed from daily_cost tags"


def tagging_quality_bucket(missing_required_tag_count: int, workload_id: str = "WORKLOAD") -> str:
    if workload_id == "UNKNOWN":
        return "UNKNOWN"
    if missing_required_tag_count == 0:
        return "GOOD"
    if missing_required_tag_count <= 2:
        return "PARTIAL"
    if missing_required_tag_count < 5:
        return "POOR"
    return "MISSING"


def _missing_count_expr(tags: tuple[str, ...]) -> str:
    parts = [
        f"CASE WHEN {TAG_FLAG_COLUMNS[tag]} THEN 1 ELSE 0 END"
        for tag in tags
        if tag in TAG_FLAG_COLUMNS
    ]
    if not parts:
        return "0"
    return "\n            + ".join(parts)


def build_tagging_quality_sql(config: AppConfig, run_id: str) -> str:
    required_tags = tuple(tag for tag in config.tagging_rules.required_tags if tag in TAG_FLAG_COLUMNS)
    critical_tags = tuple(tag for tag in config.tagging_rules.critical_tags if tag in TAG_FLAG_COLUMNS)
    return load_sql(
        "tagging_quality_summary",
        target_table=qname(config.catalog, config.schema, "tagging_quality_summary"),
        source_table=qname(config.catalog, config.schema, "daily_cost"),
        required_tag_count_expr=_missing_count_expr(required_tags),
        critical_tag_count_expr=_missing_count_expr(critical_tags),
        partial_missing_required_tag_count=str(min(2, max(0, len(required_tags)))),
        missing_required_tag_count=str(max(1, len(required_tags))),
        run_id_sql=sql_string(run_id),
    )


def comment_tagging_table(spark: Any, config: AppConfig) -> None:
    comment_on_table(
        spark,
        qname(config.catalog, config.schema, "tagging_quality_summary"),
        (
            "Tagging and ownership quality summary based on required FinOps tags."
        ),
    )
