CREATE OR REPLACE TABLE {target_table}
USING DELTA
AS
WITH
{latest_jobs_cte}
usage_rows AS (
    {usage_projection},
    COALESCE(
        TRY_CAST(p.pricing.effective_list.default AS DOUBLE),
        TRY_CAST(p.pricing.default AS DOUBLE),
        {fallback_dbu_price}
    ) AS dbu_price,
    CASE
        WHEN COALESCE(
            TRY_CAST(p.pricing.effective_list.default AS DOUBLE),
            TRY_CAST(p.pricing.default AS DOUBLE)
        ) IS NOT NULL THEN 'LIST_PRICES'
        WHEN {fallback_dbu_price} > 0 THEN 'FALLBACK_DBU_PRICE'
        ELSE 'UNKNOWN'
    END AS row_price_source
    FROM system.billing.usage u
    LEFT JOIN system.billing.list_prices p
        ON u.cloud = p.cloud
        AND u.sku_name = p.sku_name
        AND u.usage_unit = p.usage_unit
        AND p.currency_code = {currency_code_sql}
        AND u.usage_start_time >= p.price_start_time
        AND (
            u.usage_end_time <= p.price_end_time
            OR p.price_end_time IS NULL
        )
    {job_join}
    WHERE u.usage_date >= current_date() - INTERVAL {lookback_days} DAYS
      AND u.usage_unit = 'DBU'
)
SELECT
    usage_date,
    workspace_id,
    billing_origin_product,
    sku_name,
    cloud,
    usage_unit,
    workload_type,
    workload_id,
    job_id,
    job_name,
    cluster_id,
    warehouse_id,
    pipeline_id,
    notebook_id,
    run_as,
    project,
    team,
    owner,
    environment,
    cost_center,
    ROUND(SUM(usage_quantity), 6) AS dbus,
    ROUND(SUM(usage_quantity * dbu_price), 6) AS estimated_cost,
    {currency_code_sql} AS currency_code,
    {display_currency_sql} AS display_currency,
    CASE
        WHEN SUM(CASE WHEN row_price_source = 'LIST_PRICES' THEN 1 ELSE 0 END) > 0
         AND SUM(CASE WHEN row_price_source <> 'LIST_PRICES' THEN 1 ELSE 0 END) = 0
            THEN 'LIST_PRICES'
        WHEN SUM(CASE WHEN row_price_source = 'FALLBACK_DBU_PRICE' THEN 1 ELSE 0 END) > 0
         AND SUM(CASE WHEN row_price_source <> 'FALLBACK_DBU_PRICE' THEN 1 ELSE 0 END) = 0
            THEN 'FALLBACK_DBU_PRICE'
        WHEN COUNT(DISTINCT row_price_source) > 1
            THEN 'MIXED'
        ELSE 'UNKNOWN'
    END AS price_source,
    CASE
        WHEN is_shared_cluster THEN 'LOW'
        WHEN workload_type IN ('JOB', 'SQL_WAREHOUSE', 'PIPELINE') THEN 'HIGH'
        WHEN cluster_id IS NOT NULL OR run_as IS NOT NULL THEN 'MEDIUM'
        WHEN workload_type = 'UNKNOWN' THEN 'UNKNOWN'
        ELSE 'LOW'
    END AS attribution_quality,
    CONCAT_WS(
        '; ',
        CASE WHEN is_shared_cluster THEN 'SHARED_CLUSTER_ATTRIBUTION' END,
        CASE WHEN workload_type = 'UNKNOWN' THEN 'NO_WORKLOAD_METADATA' END,
        CASE WHEN run_as IS NULL THEN 'RUN_AS_UNAVAILABLE' END,
        CASE WHEN job_name IS NULL AND job_id IS NOT NULL THEN 'JOB_NAME_UNAVAILABLE' END
    ) AS attribution_notes,
    current_timestamp() AS created_at,
    {run_id_sql} AS run_id
FROM usage_rows
GROUP BY
    usage_date,
    workspace_id,
    billing_origin_product,
    sku_name,
    cloud,
    usage_unit,
    workload_type,
    workload_id,
    job_id,
    job_name,
    cluster_id,
    warehouse_id,
    pipeline_id,
    notebook_id,
    run_as,
    project,
    team,
    owner,
    environment,
    cost_center,
    is_shared_cluster
