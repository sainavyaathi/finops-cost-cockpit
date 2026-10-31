    -- F7 findings from F4 cockpit.allpurpose_misuse: one finding per misuse type.
    -- F4 already keeps one saving per cluster (R1): its primary type (misuse_type) carries the
    -- saving, confidence, evidence and action; the other types are listed with NULL saving and
    -- confidence (F4 does not publish them separately, its savings_basis lists them as "not added").
    SELECT
        'CLUSTER' AS resource_type,
        CAST(a.workspace_id AS STRING) AS workspace_id,
        CAST(a.cluster_id AS STRING) AS resource_id,
        CAST(a.cluster_name AS STRING) AS resource_name,
        CAST(a.owner AS STRING) AS owner,
        CAST(NULL AS STRING) AS team,
        'allpurpose_misuse' AS source,
        2 AS source_order,
        CAST(e.flag_type AS STRING) AS flag_type,
        CASE WHEN e.flag_type = a.misuse_type
            THEN CAST(a.estimated_monthly_savings AS DECIMAL(38,6)) END AS savings,
        CASE WHEN e.flag_type = a.misuse_type THEN CAST(a.confidence AS STRING) END AS confidence,
        CASE WHEN e.flag_type = a.misuse_type THEN CAST(a.evidence AS STRING) END AS evidence,
        CASE WHEN e.flag_type = a.misuse_type THEN CAST(a.suggested_action AS STRING) END AS suggested_action,
        CAST(a.estimated_monthly_cost AS DECIMAL(38,6)) AS source_monthly_cost
    FROM {source_table} a
    LATERAL VIEW explode(a.misuse_types) e AS flag_type
