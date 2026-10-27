CREATE OR REPLACE TABLE {target_table}
USING DELTA
AS
WITH cost_rollup_source AS (
    SELECT
        ROUND(COALESCE(SUM(dbus), 0.0), 6) AS total_dbus,
        ROUND(COALESCE(SUM(estimated_cost), 0.0), 6) AS total_estimated_cost,
        -- MIN/MAX intentionally remain NULL when the lookback has no billing rows.
        MIN(usage_date) AS lookback_start_date,
        MAX(usage_date) AS lookback_end_date
    FROM {daily_cost_table}
    WHERE run_id = {run_id_sql}
),
cost_rollup AS (
    SELECT * FROM cost_rollup_source
    UNION ALL
    -- Sentinel row keeps downstream CROSS JOIN output stable if the cost source is empty.
    SELECT
        CAST(0.0 AS DOUBLE) AS total_dbus,
        CAST(0.0 AS DOUBLE) AS total_estimated_cost,
        CAST(NULL AS DATE) AS lookback_start_date,
        CAST(NULL AS DATE) AS lookback_end_date
    WHERE NOT EXISTS (SELECT 1 FROM cost_rollup_source)
),
workload_rollup AS (
    SELECT
        COUNT(*) AS workload_count,
        ROUND(COALESCE(SUM(CASE WHEN attribution_quality IN ('LOW', 'UNKNOWN') THEN estimated_cost ELSE 0 END), 0.0), 6)
            AS cost_with_low_attribution
    FROM {workload_table}
    WHERE run_id = {run_id_sql}
),
candidate_rollup AS (
    SELECT
        COUNT(*) AS optimization_candidate_count,
        COALESCE(SUM(CASE WHEN priority_score >= 60 THEN 1 ELSE 0 END), 0) AS high_priority_candidate_count
    FROM {candidates_table}
    WHERE run_id = {run_id_sql}
),
tagging_rollup AS (
    SELECT
        ROUND(COALESCE(SUM(CASE WHEN missing_required_tag_count > 0 THEN estimated_cost ELSE 0 END), 0.0), 6)
            AS untagged_cost
    FROM {tagging_table}
    WHERE run_id = {run_id_sql}
),
reliability_rollup AS (
    SELECT
        ROUND(COALESCE(SUM(estimated_failed_cost), 0.0), 6) AS failed_cost
    FROM {reliability_table}
    WHERE run_id = {run_id_sql}
)
SELECT
    {run_id_sql} AS run_id,
    current_timestamp() AS created_at,
    lookback_start_date,
    lookback_end_date,
    total_dbus,
    total_estimated_cost,
    workload_count,
    optimization_candidate_count,
    high_priority_candidate_count,
    untagged_cost,
    failed_cost,
    cost_with_low_attribution
FROM cost_rollup
CROSS JOIN workload_rollup
CROSS JOIN candidate_rollup
CROSS JOIN tagging_rollup
CROSS JOIN reliability_rollup
