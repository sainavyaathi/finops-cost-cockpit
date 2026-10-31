-- F4 source: latest name and schedule trigger per job (lakeflow.jobs is slowly changing).
-- Columns: workspace_id, job_id, job_name, job_trigger_type.
SELECT
    workspace_id,
    job_id,
    MAX_BY(name, change_time) AS job_name,
    MAX_BY({jobs_trigger_column}, change_time) AS job_trigger_type
FROM system.lakeflow.jobs
GROUP BY workspace_id, job_id
