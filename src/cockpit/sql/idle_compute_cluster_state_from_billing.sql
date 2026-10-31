    -- Degraded mode: system.compute.clusters is missing or empty (Databricks Free Edition), so the
    -- auto-termination setting is unknown. All-purpose clusters are recognised from billing instead:
    -- an all-purpose SKU and never billed to a job (job clusters carry usage_metadata.job_id).
    SELECT
        workspace_id,
        cluster_id,
        CAST(NULL AS STRING) AS cluster_name,
        CAST(NULL AS STRING) AS cluster_source,
        CAST(NULL AS STRING) AS owned_by,
        CAST(NULL AS BIGINT) AS autotermination_minutes,
        CAST(NULL AS TIMESTAMP) AS config_change_time
    FROM cluster_cost
    WHERE billed_all_purpose = 1
      AND billed_to_job = 0
