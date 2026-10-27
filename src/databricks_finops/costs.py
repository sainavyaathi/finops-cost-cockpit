from __future__ import annotations

from typing import Any

from .config import AppConfig
from .preflight import PreflightResult
from .spark_utils import comment_on_table, load_sql, qname, sql_string


def _tag_expr(has_custom_tags: bool, key: str, aliases: tuple[str, ...]) -> str:
    if not has_custom_tags:
        return "CAST(NULL AS STRING)"
    unique_aliases = list(dict.fromkeys([*aliases, key, key[:1].upper() + key[1:]]))
    lookups = ", ".join(
        f"element_at(u.custom_tags, {sql_string(alias)})" for alias in unique_aliases
    )
    return f"COALESCE({lookups})"


def _latest_jobs_cte(has_jobs: bool) -> str:
    if not has_jobs:
        return ""
    return """
latest_jobs AS (
    SELECT workspace_id, job_id, name AS job_name
    FROM (
        SELECT
            workspace_id,
            job_id,
            name,
            ROW_NUMBER() OVER (
                PARTITION BY workspace_id, job_id
                ORDER BY change_time DESC
            ) AS rn
        FROM system.lakeflow.jobs
    )
    WHERE rn = 1
),
"""


def _job_join(has_jobs: bool) -> str:
    if not has_jobs:
        return ""
    return """
LEFT JOIN latest_jobs j
    ON u.workspace_id = j.workspace_id
    AND u.usage_metadata.job_id = j.job_id
"""


def _job_name_expr(has_jobs: bool) -> str:
    return "j.job_name" if has_jobs else "CAST(NULL AS STRING)"


def _usage_projection(config: AppConfig, has_custom_tags: bool, has_jobs: bool) -> str:
    aliases = config.tagging_rules.tag_aliases
    project = _tag_expr(has_custom_tags, "project", aliases.get("project", ("project",)))
    team = _tag_expr(has_custom_tags, "team", aliases.get("team", ("team",)))
    owner = _tag_expr(has_custom_tags, "owner", aliases.get("owner", ("owner",)))
    environment = _tag_expr(
        has_custom_tags,
        "environment",
        aliases.get("environment", ("environment",)),
    )
    cost_center = _tag_expr(
        has_custom_tags,
        "cost_center",
        aliases.get("cost_center", ("cost_center",)),
    )
    return f"""
SELECT
    u.usage_date,
    u.workspace_id,
    u.billing_origin_product,
    u.sku_name,
    u.cloud,
    u.usage_unit,
    CASE
        WHEN u.usage_metadata.job_id IS NOT NULL THEN 'JOB'
        WHEN u.usage_metadata.warehouse_id IS NOT NULL THEN 'SQL_WAREHOUSE'
        WHEN u.usage_metadata.dlt_pipeline_id IS NOT NULL THEN 'PIPELINE'
        WHEN u.usage_metadata.cluster_id IS NOT NULL THEN 'CLUSTER'
        WHEN u.usage_metadata.notebook_id IS NOT NULL THEN 'NOTEBOOK'
        ELSE 'UNKNOWN'
    END AS workload_type,
    COALESCE(
        u.usage_metadata.job_id,
        u.usage_metadata.warehouse_id,
        u.usage_metadata.dlt_pipeline_id,
        u.usage_metadata.cluster_id,
        u.usage_metadata.notebook_id,
        'UNKNOWN'
    ) AS workload_id,
    u.usage_metadata.job_id AS job_id,
    {_job_name_expr(has_jobs)} AS job_name,
    u.usage_metadata.cluster_id AS cluster_id,
    u.usage_metadata.warehouse_id AS warehouse_id,
    u.usage_metadata.dlt_pipeline_id AS pipeline_id,
    u.usage_metadata.notebook_id AS notebook_id,
    u.usage_metadata.job_run_id AS job_run_id,
    u.identity_metadata.run_as AS run_as,
    {project} AS project,
    {team} AS team,
    {owner} AS owner,
    {environment} AS environment,
    {cost_center} AS cost_center,
    CAST(u.usage_quantity AS DOUBLE) AS usage_quantity,
    CASE
        WHEN u.usage_metadata.cluster_id IS NOT NULL
         AND (
            upper(COALESCE(u.billing_origin_product, '')) IN ('ALL_PURPOSE', 'INTERACTIVE')
            OR upper(COALESCE(u.sku_name, '')) LIKE '%ALL_PURPOSE%'
         )
            THEN TRUE
        ELSE FALSE
    END AS is_shared_cluster
"""


def _daily_cost_template_args(
    config: AppConfig,
    run_id: str,
    preflight: PreflightResult,
) -> dict[str, str]:
    has_jobs = preflight.available("system.lakeflow.jobs")
    has_custom_tags = "custom_tags" in preflight.columns("system.billing.usage")
    return {
        "target_table": qname(config.catalog, config.schema, "daily_cost"),
        "latest_jobs_cte": _latest_jobs_cte(has_jobs),
        "usage_projection": _usage_projection(config, has_custom_tags, has_jobs),
        "fallback_dbu_price": f"{config.fallback_dbu_price:.12g}",
        "job_join": _job_join(has_jobs),
        "lookback_days": str(config.lookback_days),
        "currency_code_sql": sql_string(config.currency_code),
        "display_currency_sql": sql_string(config.display_currency),
        "run_id_sql": sql_string(run_id),
    }


def build_daily_cost_sql(
    config: AppConfig,
    run_id: str,
    preflight: PreflightResult,
    *,
    use_list_prices: bool,
) -> str:
    template_name = "daily_cost_list_prices" if use_list_prices else "daily_cost_fallback"
    return load_sql(template_name, **_daily_cost_template_args(config, run_id, preflight))


def create_daily_cost(
    spark: Any,
    config: AppConfig,
    run_id: str,
    preflight: PreflightResult,
) -> str:
    if preflight.available("system.billing.list_prices"):
        try:
            spark.sql(build_daily_cost_sql(config, run_id, preflight, use_list_prices=True))
            return "daily_cost built with list price join and fallback price for unmatched rows"
        except Exception:
            spark.sql(build_daily_cost_sql(config, run_id, preflight, use_list_prices=False))
            return "daily_cost built with fallback_dbu_price after list price join failed"

    spark.sql(build_daily_cost_sql(config, run_id, preflight, use_list_prices=False))
    return "daily_cost built with fallback_dbu_price because list_prices is unavailable"


def build_workload_cost_summary_sql(config: AppConfig, run_id: str) -> str:
    return load_sql(
        "workload_cost_summary",
        target_table=qname(config.catalog, config.schema, "workload_cost_summary"),
        source_table=qname(config.catalog, config.schema, "daily_cost"),
        run_id_sql=sql_string(run_id),
    )


def create_workload_cost_summary(spark: Any, config: AppConfig, run_id: str) -> str:
    spark.sql(build_workload_cost_summary_sql(config, run_id))
    return "workload_cost_summary refreshed for current lookback window"


def comment_cost_tables(spark: Any, config: AppConfig) -> None:
    comments = {
        "daily_cost": (
            "Daily estimated Databricks usage cost built from system.billing.usage. "
            "Costs are estimated and not a replacement for final invoices."
        ),
        "workload_cost_summary": (
            "Workload-level cost summary derived from daily_cost."
        ),
    }
    for table_name, comment in comments.items():
        comment_on_table(spark, qname(config.catalog, config.schema, table_name), comment)
