-- F4 source: current configuration of every cluster (compute.clusters is slowly changing,
-- the latest change_time wins). Columns: workspace_id, cluster_id, cluster_name, cluster_source, owned_by.
SELECT
    workspace_id,
    cluster_id,
    MAX_BY(cluster_name, change_time) AS cluster_name,
    MAX_BY(cluster_source, change_time) AS cluster_source,
    MAX_BY(owned_by, change_time) AS owned_by
FROM system.compute.clusters
WHERE cluster_id IS NOT NULL
GROUP BY workspace_id, cluster_id
