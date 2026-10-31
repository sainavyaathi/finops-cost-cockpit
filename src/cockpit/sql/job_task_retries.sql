-- F6 corrected retry counts, one row per job run.
-- In system.lakeflow.job_task_run_timeline, run_id is the TASK run id and job_run_id is the parent
-- job run. Every attempt of a task (retry or repair rerun) gets a new run_id under the same
-- (job_run_id, task_key). A long attempt can span several hourly rows, so attempts are counted as
-- DISTINCT run_id, never COUNT(*).
-- The task window is wider than the lookback ({task_lookback_days} days) so early attempts of runs
-- near the edge are kept; counted_runs (job runs upstream counted) decides which runs count.
SELECT
    task_attempts.workspace_id,
    task_attempts.job_id,
    task_attempts.job_run_id,
    CAST(SUM(task_attempts.attempts - 1) AS BIGINT) AS retry_count
FROM (
    SELECT
        CAST(workspace_id AS STRING) AS workspace_id,
        CAST(job_id AS STRING) AS job_id,
        CAST(job_run_id AS STRING) AS job_run_id,
        task_key,
        COUNT(DISTINCT run_id) AS attempts
    FROM system.lakeflow.job_task_run_timeline
    WHERE period_start_time >= current_timestamp() - INTERVAL {task_lookback_days} DAYS
      AND job_run_id IS NOT NULL
      AND task_key IS NOT NULL
      {top_level_task_filter}
    GROUP BY 1, 2, 3, 4
) task_attempts
LEFT SEMI JOIN (
    SELECT DISTINCT
        CAST(workspace_id AS STRING) AS workspace_id,
        CAST(job_id AS STRING) AS job_id,
        CAST(run_id AS STRING) AS run_id
    FROM system.lakeflow.job_run_timeline
    WHERE period_start_time >= current_timestamp() - INTERVAL {lookback_days} DAYS
) counted_runs
    ON task_attempts.workspace_id = counted_runs.workspace_id
    AND task_attempts.job_id = counted_runs.job_id
    AND task_attempts.job_run_id = counted_runs.run_id
GROUP BY task_attempts.workspace_id, task_attempts.job_id, task_attempts.job_run_id
