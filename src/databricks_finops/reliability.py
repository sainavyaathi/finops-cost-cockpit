from __future__ import annotations

from typing import Any

from .config import AppConfig
from .preflight import PreflightResult
from .spark_utils import comment_on_table, load_sql, qname, sql_string


def _placeholder_sql(config: AppConfig, run_id: str) -> str:
    return load_sql(
        "job_reliability_placeholder",
        target_table=qname(config.catalog, config.schema, "job_reliability_summary"),
        workload_table=qname(config.catalog, config.schema, "workload_cost_summary"),
        run_id_sql=sql_string(run_id),
    )


def _retry_cte(preflight: PreflightResult, config: AppConfig) -> str:
    if not preflight.available("system.lakeflow.job_task_run_timeline"):
        return """
        task_retries AS (
            SELECT
                CAST(NULL AS STRING) AS workspace_id,
                CAST(NULL AS STRING) AS job_id,
                CAST(NULL AS STRING) AS run_id,
                CAST(0 AS BIGINT) AS retry_count
            WHERE FALSE
        ),
        """

    columns = preflight.columns("system.lakeflow.job_task_run_timeline")
    required_columns = {"workspace_id", "job_id", "run_id", "period_start_time"}
    if not required_columns.issubset(columns):
        return """
        task_retries AS (
            SELECT
                CAST(NULL AS STRING) AS workspace_id,
                CAST(NULL AS STRING) AS job_id,
                CAST(NULL AS STRING) AS run_id,
                CAST(0 AS BIGINT) AS retry_count
            WHERE FALSE
        ),
        """

    attempt_column = next(
        (
            column
            for column in [
                "attempt_number",
                "attempt",
                "run_attempt",
                "task_attempt_number",
                "repair_count",
            ]
            if column in columns
        ),
        None,
    )
    if attempt_column is not None:
        retry_expr = (
            f"SUM(CASE WHEN TRY_CAST({attempt_column} AS BIGINT) > 0 THEN 1 ELSE 0 END)"
        )
    else:
        distinct_task = "task_run_id" if "task_run_id" in columns else "task_key"
        if distinct_task not in columns:
            distinct_task = "run_id"
        retry_expr = f"GREATEST(COUNT(*) - COUNT(DISTINCT {distinct_task}), 0)"

    return f"""
    task_retries AS (
        SELECT
            workspace_id,
            job_id,
            run_id,
            {retry_expr} AS retry_count
        FROM system.lakeflow.job_task_run_timeline
        WHERE period_start_time >= current_timestamp() - INTERVAL {config.lookback_days} DAYS
        GROUP BY workspace_id, job_id, run_id
    ),
    """


def _latest_jobs_cte(preflight: PreflightResult) -> str:
    if not preflight.available("system.lakeflow.jobs"):
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


def _latest_jobs_join(preflight: PreflightResult) -> str:
    if not preflight.available("system.lakeflow.jobs"):
        return ""
    return """
    LEFT JOIN latest_jobs j
        ON r.workspace_id = j.workspace_id
        AND r.job_id = j.job_id
    """


def _job_name_expr(preflight: PreflightResult) -> str:
    if not preflight.available("system.lakeflow.jobs"):
        return "CONCAT('Unknown Job ', r.job_id)"
    return "COALESCE(j.job_name, CONCAT('Unknown Job ', r.job_id))"


def _run_cost_cte(config: AppConfig, preflight: PreflightResult) -> str:
    fallback_dbu_price = f"{config.fallback_dbu_price:.12g}"
    currency_code = sql_string(config.currency_code)

    if preflight.available("system.billing.list_prices"):
        return f"""
    run_cost AS (
        SELECT
            u.workspace_id,
            u.usage_metadata.job_id AS job_id,
            u.usage_metadata.job_run_id AS run_id,
            ROUND(SUM(CAST(u.usage_quantity AS DOUBLE)), 6) AS dbus,
            ROUND(SUM(
                CAST(u.usage_quantity AS DOUBLE)
                * COALESCE(
                    TRY_CAST(p.pricing.effective_list.default AS DOUBLE),
                    TRY_CAST(p.pricing.default AS DOUBLE),
                    {fallback_dbu_price}
                )
            ), 6) AS estimated_cost
        FROM system.billing.usage u
        LEFT JOIN system.billing.list_prices p
            ON u.cloud = p.cloud
            AND u.sku_name = p.sku_name
            AND u.usage_unit = p.usage_unit
            AND p.currency_code = {currency_code}
            AND u.usage_start_time >= p.price_start_time
            AND (
                u.usage_end_time <= p.price_end_time
                OR p.price_end_time IS NULL
            )
        WHERE u.usage_date >= current_date() - INTERVAL {config.lookback_days} DAYS
          AND u.usage_unit = 'DBU'
          AND u.usage_metadata.job_id IS NOT NULL
          AND u.usage_metadata.job_run_id IS NOT NULL
        GROUP BY
            u.workspace_id,
            u.usage_metadata.job_id,
            u.usage_metadata.job_run_id
    ),
        """

    return f"""
    run_cost AS (
        SELECT
            u.workspace_id,
            u.usage_metadata.job_id AS job_id,
            u.usage_metadata.job_run_id AS run_id,
            ROUND(SUM(CAST(u.usage_quantity AS DOUBLE)), 6) AS dbus,
            ROUND(SUM(CAST(u.usage_quantity AS DOUBLE) * {fallback_dbu_price}), 6) AS estimated_cost
        FROM system.billing.usage u
        WHERE u.usage_date >= current_date() - INTERVAL {config.lookback_days} DAYS
          AND u.usage_unit = 'DBU'
          AND u.usage_metadata.job_id IS NOT NULL
          AND u.usage_metadata.job_run_id IS NOT NULL
        GROUP BY
            u.workspace_id,
            u.usage_metadata.job_id,
            u.usage_metadata.job_run_id
    ),
    """


def build_job_reliability_sql(
    config: AppConfig,
    run_id: str,
    preflight: PreflightResult,
) -> str:
    target = qname(config.catalog, config.schema, "job_reliability_summary")
    run_id_sql = sql_string(run_id)
    retry_cte = _retry_cte(preflight, config)
    latest_jobs_cte = _latest_jobs_cte(preflight)
    latest_jobs_join = _latest_jobs_join(preflight)
    job_name_expr = _job_name_expr(preflight)
    run_cost_cte = _run_cost_cte(config, preflight)

    return load_sql(
        "job_reliability_summary",
        target_table=target,
        latest_jobs_cte=latest_jobs_cte,
        retry_cte=retry_cte,
        run_cost_cte=run_cost_cte,
        job_name_expr=job_name_expr,
        latest_jobs_join=latest_jobs_join,
        lookback_days=str(config.lookback_days),
        retry_heavy_count=f"{config.thresholds.retry_heavy_count:.12g}",
        high_failure_rate_pct=f"{config.thresholds.high_failure_rate_pct:.12g}",
        run_id_sql=run_id_sql,
    )


def create_job_reliability_summary(
    spark: Any,
    config: AppConfig,
    run_id: str,
    preflight: PreflightResult,
) -> str:
    if not preflight.available("system.lakeflow.job_run_timeline"):
        spark.sql(_placeholder_sql(config, run_id))
        return "lakeflow tables unavailable; reliability created with INSUFFICIENT_DATA"

    try:
        spark.sql(build_job_reliability_sql(config, run_id, preflight))
        return "job_reliability_summary built from Lakeflow run/task timeline where available"
    except Exception as exc:
        spark.sql(_placeholder_sql(config, run_id))
        short_message = str(exc).splitlines()[0][:300]
        return (
            f"reliability SQL failed ({type(exc).__name__}: {short_message}); "
            "placeholder created with INSUFFICIENT_DATA"
        )


def comment_reliability_table(spark: Any, config: AppConfig) -> None:
    comment_on_table(
        spark,
        qname(config.catalog, config.schema, "job_reliability_summary"),
        (
            "Job reliability metrics derived from Lakeflow job run system tables where available."
        ),
    )
