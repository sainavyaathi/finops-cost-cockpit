CREATE OR REPLACE TABLE {target_table}
USING DELTA
AS
WITH
{latest_jobs_cte}
usage_rows AS (
    {usage_projection},
    {fallback_dbu_price} AS dbu_price,
    CASE
        WHEN {fallback_dbu_price} > 0 THEN 'FALLBACK_DBU_PRICE'
        ELSE 'UNKNOWN'
    END AS row_price_source
    FROM system.billing.usage u
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
        WHEN {fallback_dbu_price} > 0 THEN 'FALLBACK_DBU_PRICE'
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
