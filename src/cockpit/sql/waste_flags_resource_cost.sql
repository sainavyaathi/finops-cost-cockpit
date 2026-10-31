    -- F7 cost, owner and team per resource from cockpit.cost_base over the detection window.
    -- cost_base holds the upstream lookback; the date filter keeps the detection window only.
    -- Keys follow the upstream workload precedence (JOB > SQL_WAREHOUSE > PIPELINE > CLUSTER > NOTEBOOK),
    -- so JOB, WAREHOUSE, PIPELINE, NOTEBOOK and UNKNOWN costs are exclusive: each cost_base row belongs
    -- to its workload_type / workload_id only. CLUSTER is the exception: it takes every row carrying the
    -- cluster_id (the same cost F4 and F5 report), so a job or pipeline running on a cluster is in both
    -- the JOB/PIPELINE and the CLUSTER cost. estimated_monthly_cost is therefore not additive across rows;
    -- waste_flags.sql caps JOB savings so a JOB and a flagged CLUSTER never claim the same DBUs.
    -- owner and team are the tag values carrying the most cost on the resource.
    WITH cost_rows AS (
        SELECT
            CAST(workspace_id AS STRING) AS workspace_id,
            CASE upper(COALESCE(workload_type, 'UNKNOWN'))
                WHEN 'SQL_WAREHOUSE' THEN 'WAREHOUSE'
                ELSE upper(COALESCE(workload_type, 'UNKNOWN'))
            END AS workload_resource_type,
            CAST(COALESCE(workload_id, 'UNKNOWN') AS STRING) AS workload_id,
            CAST(cluster_id AS STRING) AS cluster_id,
            CAST(owner AS STRING) AS owner,
            CAST(team AS STRING) AS team,
            CAST(estimated_cost AS DOUBLE) AS estimated_cost
        FROM {cost_base}
        WHERE usage_date >= current_date() - INTERVAL {detection_days} DAYS
    ),
    keyed AS (
        SELECT workload_resource_type AS resource_type, workspace_id, workload_id AS resource_id,
               owner, team, estimated_cost
        FROM cost_rows
        WHERE workload_resource_type <> 'CLUSTER'
        UNION ALL
        SELECT 'CLUSTER', workspace_id, cluster_id, owner, team, estimated_cost
        FROM cost_rows
        WHERE cluster_id IS NOT NULL
    ),
    totals AS (
        SELECT resource_type, workspace_id, resource_id, SUM(estimated_cost) AS window_cost
        FROM keyed
        GROUP BY resource_type, workspace_id, resource_id
    ),
    owners AS (
        SELECT resource_type, workspace_id, resource_id, owner,
               ROW_NUMBER() OVER (PARTITION BY resource_type, workspace_id, resource_id
                                  ORDER BY SUM(estimated_cost) DESC, owner) AS rn
        FROM keyed
        WHERE owner IS NOT NULL
        GROUP BY resource_type, workspace_id, resource_id, owner
    ),
    teams AS (
        SELECT resource_type, workspace_id, resource_id, team,
               ROW_NUMBER() OVER (PARTITION BY resource_type, workspace_id, resource_id
                                  ORDER BY SUM(estimated_cost) DESC, team) AS rn
        FROM keyed
        WHERE team IS NOT NULL
        GROUP BY resource_type, workspace_id, resource_id, team
    )
    SELECT
        t.resource_type,
        t.workspace_id,
        t.resource_id,
        CAST(t.window_cost AS DOUBLE) AS window_cost,
        o.owner AS owner_tag,
        m.team AS team_tag
    FROM totals t
    LEFT JOIN owners o
        ON t.resource_type = o.resource_type AND t.workspace_id = o.workspace_id
        AND t.resource_id = o.resource_id AND o.rn = 1
    LEFT JOIN teams m
        ON t.resource_type = m.resource_type AND t.workspace_id = m.workspace_id
        AND t.resource_id = m.resource_id AND m.rn = 1
