-- F4 signal SCHEDULED_JOB from system.lakeflow: automated job runs whose compute is a cluster.
-- Jobs on all-purpose compute have no usage_metadata.job_id in billing (billed to the cluster), so
-- they are found through compute_ids, not billing.
-- A run is automated when
--   * its trigger_type is in automated_trigger_types (the job trigger_type is used only when the
--     run has none, so a manual ONETIME run of a scheduled job is not counted), or
--   * its run_type is in automated_run_types (SUBMIT_RUN: ADF, Airflow and other orchestrators), or
--   * it is an ONETIME_RETRY of a job that is automated (job trigger or another automated run in the
--     lookback). job_run_timeline does not link a retry to the run it retries, so this is decided
--     per job, not per run.
-- A manual UI run (trigger ONETIME, run_type JOB_RUN) is never counted.
-- Activity windows are the task windows when job_task_run_timeline has the run on this cluster,
-- otherwise the whole run window. job_id is compared null-safe because submit runs may have none.
WITH latest_jobs AS (
    {jobs_relation}
),
runs AS (
    SELECT
        workspace_id,
        job_id,
        run_id,
        MIN(period_start_time) AS run_start,
        MAX(period_end_time) AS run_end,
        MAX(trigger_type) AS run_trigger_type,
        MAX({run_type_column}) AS run_type,
        MAX(run_name) AS run_name
    FROM system.lakeflow.job_run_timeline
    WHERE period_start_time >= current_date() - INTERVAL {lookback_days} DAYS
    GROUP BY workspace_id, job_id, run_id
),
classified AS (
    SELECT
        r.workspace_id,
        r.job_id,
        r.run_id,
        r.run_start,
        r.run_end,
        r.run_name,
        j.job_name,
        UPPER(j.job_trigger_type) AS job_trigger,
        UPPER(COALESCE(r.run_trigger_type, j.job_trigger_type)) AS effective_trigger,
        UPPER(r.run_type) AS run_type
    FROM runs r
    LEFT JOIN latest_jobs j
        ON r.workspace_id = j.workspace_id
        AND r.job_id <=> j.job_id
),
automated_jobs AS (
    SELECT DISTINCT workspace_id, job_id
    FROM classified c
    WHERE {trigger_test} OR {run_type_test} OR {job_trigger_test}
),
scheduled_runs AS (
    SELECT
        c.workspace_id,
        c.job_id,
        c.run_id,
        c.run_start,
        c.run_end,
        COALESCE(c.job_name, c.run_name, CONCAT('job ', c.job_id)) AS job_name,
        CASE WHEN {run_type_test} THEN c.run_type ELSE c.effective_trigger END AS automation
    FROM classified c
    LEFT JOIN automated_jobs aj
        ON c.workspace_id = aj.workspace_id
        AND c.job_id <=> aj.job_id
    WHERE {trigger_test}
       OR {run_type_test}
       OR ({count_retries} AND c.effective_trigger = 'ONETIME_RETRY' AND aj.workspace_id IS NOT NULL)
),
run_compute AS (
    SELECT DISTINCT t.workspace_id, t.job_id, t.run_id, cid AS cluster_id
    FROM system.lakeflow.job_run_timeline t
    LATERAL VIEW explode(t.compute_ids) run_compute_ids AS cid
    WHERE t.period_start_time >= current_date() - INTERVAL {lookback_days} DAYS
      AND cid IS NOT NULL
),
task_compute AS (
    {task_compute_relation}
),
activity AS (
    SELECT workspace_id, job_id, run_id, cluster_id, activity_start, activity_end
    FROM task_compute
    UNION ALL
    SELECT rc.workspace_id, rc.job_id, rc.run_id, rc.cluster_id, s.run_start, s.run_end
    FROM run_compute rc
    JOIN scheduled_runs s
        ON rc.workspace_id = s.workspace_id
        AND rc.job_id <=> s.job_id
        AND rc.run_id = s.run_id
    LEFT ANTI JOIN task_compute tc
        ON rc.workspace_id = tc.workspace_id
        AND rc.run_id = tc.run_id
        AND rc.cluster_id = tc.cluster_id
)
SELECT
    a.workspace_id,
    a.cluster_id,
    'SCHEDULED_JOB' AS misuse_type,
    a.activity_start,
    a.activity_end,
    COALESCE(a.job_id, s.job_name) AS actor,
    a.run_id AS item,
    CONCAT(s.job_name, ' (', s.automation, ')') AS detail,
    'lakeflow' AS source
FROM activity a
JOIN scheduled_runs s
    ON a.workspace_id = s.workspace_id
    AND a.job_id <=> s.job_id
    AND a.run_id = s.run_id
