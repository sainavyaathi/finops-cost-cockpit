CREATE OR REPLACE TABLE {target_table}
USING DELTA
AS
SELECT
    workspace_id,
    job_id,
    job_name,
    CAST(0 AS BIGINT) AS run_count,
    CAST(0 AS BIGINT) AS success_count,
    CAST(0 AS BIGINT) AS failed_count,
    CAST(0 AS BIGINT) AS cancelled_count,
    CAST(0 AS BIGINT) AS skipped_count,
    CAST(0 AS BIGINT) AS retry_count,
    CAST(NULL AS DOUBLE) AS failure_rate_pct,
    CAST(NULL AS DOUBLE) AS avg_duration_minutes,
    CAST(NULL AS DOUBLE) AS total_duration_minutes,
    CAST(NULL AS DOUBLE) AS estimated_failed_dbus,
    CAST(NULL AS DOUBLE) AS estimated_failed_cost,
    CAST(NULL AS TIMESTAMP) AS last_run_start_time,
    CAST(NULL AS STRING) AS last_run_result_state,
    'INSUFFICIENT_DATA' AS reliability_category,
    'LOW' AS confidence,
    current_timestamp() AS created_at,
    {run_id_sql} AS run_id
FROM {workload_table}
WHERE run_id = {run_id_sql}
  AND workload_type = 'JOB'
