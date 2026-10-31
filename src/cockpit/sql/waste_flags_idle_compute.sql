    -- F7 findings from F5 cockpit.idle_compute: one finding per flagged cluster, flag type = idle_reason
    -- (for example AUTO_TERMINATION_OFF+LOW_UTILIZATION). Savings NULL when F5 could not price idleness.
    SELECT
        'CLUSTER' AS resource_type,
        CAST(workspace_id AS STRING) AS workspace_id,
        CAST(cluster_id AS STRING) AS resource_id,
        CAST(cluster_name AS STRING) AS resource_name,
        CAST(owner AS STRING) AS owner,
        CAST(NULL AS STRING) AS team,
        'idle_compute' AS source,
        1 AS source_order,
        CAST(idle_reason AS STRING) AS flag_type,
        CAST(estimated_monthly_savings AS DECIMAL(38,6)) AS savings,
        CAST(confidence AS STRING) AS confidence,
        CAST(evidence AS STRING) AS evidence,
        CAST(suggested_action AS STRING) AS suggested_action,
        CAST(estimated_monthly_cost AS DECIMAL(38,6)) AS source_monthly_cost
    FROM {source_table}
