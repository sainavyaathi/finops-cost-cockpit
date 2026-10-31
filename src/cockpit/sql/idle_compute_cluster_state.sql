    -- Latest state per cluster from the slowly changing system.compute.clusters: the newest
    -- change_time wins (on a tie, a row with delete_time wins). Deleted clusters are dropped, and
    -- only all-purpose clusters are kept: job and pipeline clusters end with their run even when
    -- they report auto_termination_minutes = 0.
    SELECT
        workspace_id,
        cluster_id,
        cluster_name,
        cluster_source,
        owned_by,
        autotermination_minutes,
        config_change_time
    FROM (
        SELECT
            CAST(workspace_id AS STRING) AS workspace_id,
            CAST(cluster_id AS STRING) AS cluster_id,
            {cluster_name_expr} AS cluster_name,
            upper(CAST(cluster_source AS STRING)) AS cluster_source,
            {owned_by_expr} AS owned_by,
            CAST(auto_termination_minutes AS BIGINT) AS autotermination_minutes,
            CAST(change_time AS TIMESTAMP) AS config_change_time,
            delete_time,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(workspace_id AS STRING), CAST(cluster_id AS STRING)
                ORDER BY change_time DESC NULLS LAST, delete_time DESC NULLS LAST
            ) AS rn
        FROM system.compute.clusters
        WHERE cluster_id IS NOT NULL
    ) latest
    WHERE rn = 1
      AND delete_time IS NULL
      AND cluster_source IN ({all_purpose_sources_sql})
