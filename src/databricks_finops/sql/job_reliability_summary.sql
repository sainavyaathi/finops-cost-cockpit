CREATE OR REPLACE TABLE {target_table}
USING DELTA
AS
WITH {latest_jobs_cte}
terminal_runs AS (
    SELECT
        workspace_id,
        job_id,
        run_id,
        MIN(period_start_time) AS run_start_time,
        MAX(period_end_time) AS run_end_time,
        MAX_BY(result_state, period_end_time) AS result_state
    FROM system.lakeflow.job_run_timeline
    WHERE period_start_time >= current_timestamp() - INTERVAL {lookback_days} DAYS
    GROUP BY workspace_id, job_id, run_id
),
{retry_cte}
{run_cost_cte}
joined AS (
    SELECT
        r.workspace_id,
        r.job_id,
        {job_name_expr} AS job_name,
        r.run_id,
        r.run_start_time,
        r.run_end_time,
        r.result_state,
        ROUND((unix_timestamp(r.run_end_time) - unix_timestamp(r.run_start_time)) / 60.0, 4) AS duration_minutes,
        COALESCE(c.dbus, 0) AS dbus,
        COALESCE(c.estimated_cost, 0) AS estimated_cost,
        COALESCE(t.retry_count, 0) AS retry_count
    FROM terminal_runs r
    LEFT JOIN run_cost c
        ON r.workspace_id = c.workspace_id
        AND r.job_id = c.job_id
        AND r.run_id = c.run_id
    {latest_jobs_join}
    LEFT JOIN task_retries t
        ON r.workspace_id = t.workspace_id
        AND r.job_id = t.job_id
        AND r.run_id = t.run_id
)
SELECT
    workspace_id,
    job_id,
    FIRST(job_name, TRUE) AS job_name,
    COUNT(DISTINCT run_id) AS run_count,
    SUM(CASE WHEN result_state = 'SUCCEEDED' THEN 1 ELSE 0 END) AS success_count,
    SUM(CASE WHEN result_state IN ('FAILED', 'TIMED_OUT', 'ERROR') THEN 1 ELSE 0 END) AS failed_count,
    SUM(CASE WHEN result_state IN ('CANCELLED', 'CANCELED') THEN 1 ELSE 0 END) AS cancelled_count,
    SUM(CASE WHEN result_state IN ('SKIPPED', 'BLOCKED') THEN 1 ELSE 0 END) AS skipped_count,
    SUM(retry_count) AS retry_count,
    ROUND(
        100.0 * try_divide(
            SUM(CASE WHEN result_state IN ('FAILED', 'TIMED_OUT', 'ERROR') THEN 1 ELSE 0 END),
            COUNT(DISTINCT run_id)
        ),
        4
    ) AS failure_rate_pct,
    ROUND(AVG(duration_minutes), 4) AS avg_duration_minutes,
    ROUND(SUM(duration_minutes), 4) AS total_duration_minutes,
    ROUND(SUM(CASE
        WHEN result_state IN ('FAILED', 'TIMED_OUT', 'ERROR') THEN dbus
        ELSE 0
    END), 6) AS estimated_failed_dbus,
    ROUND(SUM(CASE
        WHEN result_state IN ('FAILED', 'TIMED_OUT', 'ERROR') THEN estimated_cost
        ELSE 0
    END), 6) AS estimated_failed_cost,
    MAX(run_start_time) AS last_run_start_time,
    MAX_BY(result_state, run_start_time) AS last_run_result_state,
    CASE
        WHEN COUNT(DISTINCT run_id) = 0 THEN 'INSUFFICIENT_DATA'
        WHEN SUM(retry_count) >= {retry_heavy_count} THEN 'RETRY_HEAVY'
        WHEN 100.0 * try_divide(
            SUM(CASE WHEN result_state IN ('FAILED', 'TIMED_OUT', 'ERROR') THEN 1 ELSE 0 END),
            COUNT(DISTINCT run_id)
        ) >= {high_failure_rate_pct} THEN 'FAILURE_HEAVY'
        WHEN SUM(CASE WHEN result_state IS NULL THEN 1 ELSE 0 END) > 0 THEN 'REVIEW_REQUIRED'
        ELSE 'HEALTHY'
    END AS reliability_category,
    CASE
        WHEN COUNT(DISTINCT run_id) >= 10 THEN 'HIGH'
        WHEN COUNT(DISTINCT run_id) >= 3 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS confidence,
    current_timestamp() AS created_at,
    {run_id_sql} AS run_id
FROM joined
GROUP BY workspace_id, job_id
