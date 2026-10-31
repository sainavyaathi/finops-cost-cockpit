-- F6 corrected retry counts, one row per job run.
-- In system.lakeflow.job_task_run_timeline, run_id is the TASK run id and job_run_id is the parent
-- job run. Every attempt of a task (retry or repair rerun) gets a new run_id under the same
-- (job_run_id, task_key). A long attempt can span several hourly rows, so attempts are counted as
-- DISTINCT run_id, never COUNT(*).
-- The task window is wider than the detection window ({task_lookback_days} days) so early attempts of
-- runs near the edge are kept; counted_runs (job runs in the detection window) decides which runs count.
-- Minutes per attempt run from its first start to its last end. retry_task_minutes is the task time
-- spent on attempts that were not the final one (the latest start); F7 uses its share of
-- task_attempt_minutes to estimate the cost of retries.
SELECT
    task_attempts.workspace_id,
    task_attempts.job_id,
    task_attempts.job_run_id,
    CAST(SUM(task_attempts.attempts - 1) AS BIGINT) AS retry_count,
    CAST(SUM(task_attempts.attempt_minutes) AS DOUBLE) AS task_attempt_minutes,
    CAST(SUM(task_attempts.attempt_minutes - task_attempts.final_attempt_minutes) AS DOUBLE) AS retry_task_minutes
FROM (
    SELECT
        workspace_id,
        job_id,
        job_run_id,
        task_key,
        COUNT(DISTINCT run_id) AS attempts,
        COALESCE(SUM(minutes), 0.0) AS attempt_minutes,
        COALESCE(MAX_BY(minutes, attempt_start), 0.0) AS final_attempt_minutes
    FROM (
        SELECT
            CAST(workspace_id AS STRING) AS workspace_id,
            CAST(job_id AS STRING) AS job_id,
            CAST(job_run_id AS STRING) AS job_run_id,
            task_key,
            run_id,
            MIN(period_start_time) AS attempt_start,
            {attempt_minutes_expr} AS minutes
        FROM system.lakeflow.job_task_run_timeline
        WHERE period_start_time >= current_timestamp() - INTERVAL {task_lookback_days} DAYS
          AND job_run_id IS NOT NULL
          AND task_key IS NOT NULL
          {top_level_task_filter}
        GROUP BY 1, 2, 3, 4, 5
    ) attempt_rows
    GROUP BY workspace_id, job_id, job_run_id, task_key
) task_attempts
LEFT SEMI JOIN (
    SELECT DISTINCT
        CAST(workspace_id AS STRING) AS workspace_id,
        CAST(job_id AS STRING) AS job_id,
        CAST(run_id AS STRING) AS run_id
    FROM system.lakeflow.job_run_timeline
    WHERE period_start_time >= current_timestamp() - INTERVAL {detection_days} DAYS
) counted_runs
    ON task_attempts.workspace_id = counted_runs.workspace_id
    AND task_attempts.job_id = counted_runs.job_id
    AND task_attempts.job_run_id = counted_runs.run_id
GROUP BY task_attempts.workspace_id, task_attempts.job_id, task_attempts.job_run_id
