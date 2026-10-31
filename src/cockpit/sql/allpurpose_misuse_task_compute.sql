-- F4 source: where each task run of a job run executed. Task slices (clock-hour rows) are
-- collapsed per task run_id; run_id here is the TASK run id and job_run_id the parent job run.
-- Columns: workspace_id, job_id, run_id (parent job run), cluster_id, activity_start, activity_end.
SELECT
    t.workspace_id,
    t.job_id,
    t.job_run_id AS run_id,
    cid AS cluster_id,
    MIN(t.period_start_time) AS activity_start,
    MAX(t.period_end_time) AS activity_end
FROM system.lakeflow.job_task_run_timeline t
LATERAL VIEW explode(t.compute_ids) task_compute AS cid
WHERE t.period_start_time >= current_date() - INTERVAL {lookback_days} DAYS
  AND cid IS NOT NULL
GROUP BY t.workspace_id, t.job_id, t.job_run_id, t.run_id, cid
