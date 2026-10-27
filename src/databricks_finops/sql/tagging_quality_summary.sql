CREATE OR REPLACE TABLE {target_table}
USING DELTA
AS
WITH grouped AS (
    SELECT
        usage_date,
        workspace_id,
        workload_type,
        workload_id,
        FIRST(run_as, TRUE) AS run_as,
        ROUND(SUM(dbus), 6) AS dbus,
        ROUND(SUM(estimated_cost), 6) AS estimated_cost,
        FIRST(project, TRUE) AS project,
        FIRST(team, TRUE) AS team,
        FIRST(owner, TRUE) AS owner,
        FIRST(environment, TRUE) AS environment,
        FIRST(cost_center, TRUE) AS cost_center
    FROM {source_table}
    WHERE run_id = {run_id_sql}
    GROUP BY usage_date, workspace_id, workload_type, workload_id
),
flags AS (
    SELECT
        *,
        CASE WHEN project IS NULL OR trim(project) = '' THEN TRUE ELSE FALSE END AS missing_project_tag,
        CASE WHEN team IS NULL OR trim(team) = '' THEN TRUE ELSE FALSE END AS missing_team_tag,
        CASE WHEN owner IS NULL OR trim(owner) = '' THEN TRUE ELSE FALSE END AS missing_owner_tag,
        CASE WHEN environment IS NULL OR trim(environment) = '' THEN TRUE ELSE FALSE END AS missing_environment_tag,
        CASE WHEN cost_center IS NULL OR trim(cost_center) = '' THEN TRUE ELSE FALSE END AS missing_cost_center_tag
    FROM grouped
),
scored AS (
    SELECT
        *,
        (
            {required_tag_count_expr}
        ) AS missing_required_tag_count,
        (
            {critical_tag_count_expr}
        ) AS missing_critical_tag_count
    FROM flags
),
classified AS (
    SELECT
        *,
        CASE
            WHEN workload_id = 'UNKNOWN' THEN 'UNKNOWN'
            WHEN missing_required_tag_count = 0 THEN 'GOOD'
            WHEN missing_required_tag_count <= {partial_missing_required_tag_count} THEN 'PARTIAL'
            WHEN missing_required_tag_count < {missing_required_tag_count} THEN 'POOR'
            ELSE 'MISSING'
        END AS tagging_quality
    FROM scored
)
SELECT
    usage_date,
    workspace_id,
    workload_type,
    workload_id,
    run_as,
    dbus,
    estimated_cost,
    project,
    team,
    owner,
    environment,
    cost_center,
    missing_project_tag,
    missing_team_tag,
    missing_owner_tag,
    missing_environment_tag,
    missing_cost_center_tag,
    missing_required_tag_count,
    missing_critical_tag_count,
    tagging_quality,
    current_timestamp() AS created_at,
    {run_id_sql} AS run_id
FROM classified
