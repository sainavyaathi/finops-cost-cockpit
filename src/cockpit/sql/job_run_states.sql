-- Job runs per job whose final result_state is NULL, built the same way as upstream terminal_runs,
-- so REVIEW_REQUIRED can be recomputed instead of inherited.
SELECT
    workspace_id,
    job_id,
    CAST(SUM(CASE WHEN result_state IS NULL THEN 1 ELSE 0 END) AS BIGINT) AS unknown_state_run_count
FROM (
    SELECT
        CAST(workspace_id AS STRING) AS workspace_id,
        CAST(job_id AS STRING) AS job_id,
        run_id,
        MAX_BY(result_state, period_end_time) AS result_state
    FROM system.lakeflow.job_run_timeline
    WHERE period_start_time >= current_timestamp() - INTERVAL {lookback_days} DAYS
    GROUP BY 1, 2, 3
) terminal_runs
GROUP BY workspace_id, job_id
