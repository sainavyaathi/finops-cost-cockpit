CREATE OR REPLACE TABLE {target_table}
USING DELTA
AS
WITH grouped AS (
    SELECT
        workspace_id,
        workload_type,
        workload_id,
        FIRST(job_id, TRUE) AS job_id,
        FIRST(job_name, TRUE) AS job_name,
        FIRST(cluster_id, TRUE) AS cluster_id,
        FIRST(warehouse_id, TRUE) AS warehouse_id,
        FIRST(pipeline_id, TRUE) AS pipeline_id,
        FIRST(run_as, TRUE) AS run_as,
        FIRST(project, TRUE) AS project,
        FIRST(team, TRUE) AS team,
        FIRST(owner, TRUE) AS owner,
        FIRST(environment, TRUE) AS environment,
        FIRST(cost_center, TRUE) AS cost_center,
        ROUND(SUM(dbus), 6) AS total_dbus,
        ROUND(SUM(estimated_cost), 6) AS estimated_cost,
        COUNT(DISTINCT usage_date) AS active_days,
        MIN(usage_date) AS first_seen_date,
        MAX(usage_date) AS last_seen_date,
        ROUND(try_divide(SUM(estimated_cost), COUNT(DISTINCT usage_date)), 6) AS avg_daily_cost,
        CASE MAX(
            CASE attribution_quality
                WHEN 'UNKNOWN' THEN 4
                WHEN 'LOW' THEN 3
                WHEN 'MEDIUM' THEN 2
                ELSE 1
            END
        )
            WHEN 4 THEN 'UNKNOWN'
            WHEN 3 THEN 'LOW'
            WHEN 2 THEN 'MEDIUM'
            ELSE 'HIGH'
        END AS attribution_quality,
        array_join(array_sort(collect_set(attribution_notes)), '; ') AS attribution_notes
    FROM {source_table}
    WHERE run_id = {run_id_sql}
    GROUP BY workspace_id, workload_type, workload_id
)
SELECT
    workspace_id,
    workload_type,
    workload_id,
    job_id,
    job_name,
    cluster_id,
    warehouse_id,
    pipeline_id,
    run_as,
    project,
    team,
    owner,
    environment,
    cost_center,
    total_dbus,
    estimated_cost,
    active_days,
    first_seen_date,
    last_seen_date,
    avg_daily_cost,
    DENSE_RANK() OVER (ORDER BY estimated_cost DESC) AS cost_rank,
    attribution_quality,
    attribution_notes,
    current_timestamp() AS created_at,
    {run_id_sql} AS run_id
FROM grouped
