    -- F7 findings from upstream optimization_candidates (sizing, tagging, attribution, expense) that
    -- F4/F5/F6 do not cover. Upstream estimates no saving, so savings are NULL; these items rank on
    -- cost and confidence. The issue types taken come from waste_flags.upstream_issue_types.
    -- Items whose suggested_action is LOOKS_HEALTHY are never taken.
    -- Upstream joins utilisation on cluster_id OR job_id, so one workload can appear more than once:
    -- keep one finding per (resource, issue type), the highest cost, then confidence, then evidence.
    SELECT
        resource_type, workspace_id, resource_id, resource_name, owner, team, source, source_order,
        flag_type, savings, confidence, evidence, suggested_action, source_monthly_cost
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY resource_type, workspace_id, resource_id, flag_type
                ORDER BY CAST(source_monthly_cost AS DOUBLE) DESC NULLS LAST,
                         CASE confidence WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 WHEN 'LOW' THEN 3 ELSE 4 END,
                         evidence
            ) AS duplicate_rank
        FROM (
            SELECT
                CASE upper(COALESCE(workload_type, 'UNKNOWN'))
                    WHEN 'SQL_WAREHOUSE' THEN 'WAREHOUSE'
                    ELSE upper(COALESCE(workload_type, 'UNKNOWN'))
                END AS resource_type,
                CAST(workspace_id AS STRING) AS workspace_id,
                CAST(COALESCE(workload_id, 'UNKNOWN') AS STRING) AS resource_id,
                CAST(COALESCE(job_name, workload_id) AS STRING) AS resource_name,
                CAST(owner AS STRING) AS owner,
                CAST(team AS STRING) AS team,
                'optimization_candidates' AS source,
                4 AS source_order,
                CAST(issue_type AS STRING) AS flag_type,
                CAST(NULL AS DECIMAL(38,6)) AS savings,
                CAST(confidence AS STRING) AS confidence,
                CAST(evidence AS STRING) AS evidence,
                CAST(suggested_action AS STRING) AS suggested_action,
                CAST(estimated_monthly_cost AS DECIMAL(38,6)) AS source_monthly_cost
            FROM {source_table}
            WHERE {issue_type_test}
              AND COALESCE(suggested_action, '') <> 'LOOKS_HEALTHY'
        ) upstream_rows
    ) ranked_upstream
    WHERE duplicate_rank = 1
