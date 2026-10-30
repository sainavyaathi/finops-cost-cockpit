-- F6 cockpit.job_reliability: upstream job_reliability_summary with a corrected retry_count.
-- Upstream keeps its buggy table (rule R3); pages read this one instead.
CREATE OR REPLACE TABLE {target}
USING DELTA
AS
WITH run_retries AS (
{run_retries_sql}
),
run_states AS (
{run_states_sql}
),
job_retries AS (
    SELECT
        workspace_id,
        job_id,
        CAST(SUM(retry_count) AS BIGINT) AS retry_count,
        CAST(SUM(CASE WHEN retry_count > 0 THEN 1 ELSE 0 END) AS BIGINT) AS retried_run_count
    FROM run_retries
    GROUP BY workspace_id, job_id
),
merged AS (
    SELECT
        u.*,
        CAST(u.retry_count AS BIGINT) AS upstream_retry_count,
        {retry_count_expr} AS corrected_retry_count,
        {retried_run_count_expr} AS corrected_retried_run_count,
        {review_required_expr} AS review_required
    FROM {upstream_table} u
    LEFT JOIN job_retries r
        ON CAST(u.workspace_id AS STRING) = r.workspace_id
        AND CAST(u.job_id AS STRING) = r.job_id
    LEFT JOIN run_states s
        ON CAST(u.workspace_id AS STRING) = s.workspace_id
        AND CAST(u.job_id AS STRING) = s.job_id
)
SELECT
    CAST(workspace_id AS STRING) AS workspace_id,
    CAST(job_id AS STRING) AS job_id,
    CAST(job_name AS STRING) AS job_name,
    CAST(run_count AS BIGINT) AS run_count,
    CAST(success_count AS BIGINT) AS success_count,
    CAST(failed_count AS BIGINT) AS failed_count,
    CAST(cancelled_count AS BIGINT) AS cancelled_count,
    CAST(skipped_count AS BIGINT) AS skipped_count,
    corrected_retry_count AS retry_count,
    corrected_retried_run_count AS retried_run_count,
    CAST(ROUND(100.0 * try_divide(corrected_retried_run_count, run_count), 4) AS DOUBLE) AS retry_rate_pct,
    upstream_retry_count,
    {retry_count_source_sql} AS retry_count_source,
    CAST(failure_rate_pct AS DOUBLE) AS failure_rate_pct,
    CAST(avg_duration_minutes AS DOUBLE) AS avg_duration_minutes,
    CAST(total_duration_minutes AS DOUBLE) AS total_duration_minutes,
    CAST(estimated_failed_dbus AS DECIMAL(38,6)) AS estimated_failed_dbus,
    CAST(estimated_failed_cost AS DECIMAL(38,6)) AS estimated_failed_cost,
    last_run_start_time,
    CAST(last_run_result_state AS STRING) AS last_run_result_state,
    -- Same rules and order as upstream, re-evaluated with the corrected retry_count.
    -- review_required (runs with a NULL final result_state) is recomputed from job_run_timeline;
    -- in fallback mode it can only be inherited from the upstream category.
    CASE
        WHEN run_count = 0 THEN 'INSUFFICIENT_DATA'
        WHEN corrected_retry_count >= {retry_heavy_count} THEN 'RETRY_HEAVY'
        WHEN failure_rate_pct >= {high_failure_rate_pct} THEN 'FAILURE_HEAVY'
        WHEN review_required THEN 'REVIEW_REQUIRED'
        ELSE 'HEALTHY'
    END AS reliability_category,
    CAST(reliability_category AS STRING) AS upstream_reliability_category,
    CAST(confidence AS STRING) AS confidence,
    current_timestamp() AS created_at,
    {run_id_sql} AS run_id
FROM merged
