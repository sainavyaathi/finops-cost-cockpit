CREATE OR REPLACE TABLE {target_table}
USING DELTA
AS
WITH tagging_by_workload AS (
    SELECT
        workspace_id,
        workload_type,
        workload_id,
        MAX(missing_required_tag_count) AS missing_required_tag_count,
        MAX(missing_critical_tag_count) AS missing_critical_tag_count,
        ROUND(SUM(CASE WHEN missing_required_tag_count > 0 THEN estimated_cost ELSE 0 END), 6) AS untagged_cost
    FROM {tagging_table}
    WHERE run_id = {run_id_sql}
    GROUP BY workspace_id, workload_type, workload_id
),
signals AS (
    SELECT
        w.workspace_id,
        w.workload_type,
        w.workload_id,
        w.job_id,
        w.job_name,
        w.owner,
        w.team,
        w.project,
        w.environment,
        ROUND(w.total_dbus * {monthly_multiplier}, 6) AS monthly_dbus,
        ROUND(w.estimated_cost * {monthly_multiplier}, 6) AS estimated_monthly_cost,
        w.active_days,
        w.attribution_quality,
        w.attribution_notes,
        u.avg_cpu_pct,
        u.avg_memory_pct,
        u.utilization_category,
        u.confidence AS utilization_confidence,
        r.failure_rate_pct,
        r.retry_count,
        r.estimated_failed_cost,
        r.reliability_category,
        r.confidence AS reliability_confidence,
        COALESCE(t.missing_required_tag_count, {default_missing_required_tag_count}) AS missing_required_tag_count,
        COALESCE(t.missing_critical_tag_count, {default_missing_critical_tag_count}) AS missing_critical_tag_count
    FROM {workload_table} w
    LEFT JOIN {utilization_table} u
        ON w.workspace_id = u.workspace_id
        AND (
            w.cluster_id = u.cluster_id
            OR (w.job_id IS NOT NULL AND w.job_id = u.job_id)
        )
        AND u.run_id = {run_id_sql}
    LEFT JOIN {reliability_table} r
        ON w.workspace_id = r.workspace_id
        AND w.job_id = r.job_id
        AND r.run_id = {run_id_sql}
    LEFT JOIN tagging_by_workload t
        ON w.workspace_id = t.workspace_id
        AND w.workload_type = t.workload_type
        AND w.workload_id = t.workload_id
    WHERE w.run_id = {run_id_sql}
),
classified AS (
    SELECT
        *,
        CASE
            WHEN attribution_notes LIKE '%SHARED_CLUSTER_ATTRIBUTION%' THEN 'SHARED_CLUSTER_ATTRIBUTION'
            WHEN attribution_quality = 'UNKNOWN' THEN 'UNKNOWN_ATTRIBUTION'
            WHEN COALESCE(estimated_failed_cost, 0) * {monthly_multiplier} >= {high_failed_cost} THEN 'HIGH_FAILURE_COST'
            WHEN COALESCE(failure_rate_pct, 0) >= {high_failure_rate_pct} THEN 'HIGH_FAILURE_RATE'
            WHEN COALESCE(retry_count, 0) >= {retry_heavy_count} THEN 'RETRY_HEAVY'
            WHEN utilization_category = 'HIGH_COST_LOW_UTILIZATION' THEN 'HIGH_COST_LOW_UTILIZATION'
            WHEN utilization_category = 'LOW_UTILIZATION' THEN 'LOW_UTILIZATION'
            WHEN utilization_category = 'MEMORY_BOUND_DO_NOT_DOWNSIZE' THEN 'MEMORY_BOUND'
            WHEN COALESCE(missing_required_tag_count, 0) > 0 THEN 'MISSING_TAGS'
            WHEN utilization_category = 'INSUFFICIENT_DATA'
              OR reliability_category = 'INSUFFICIENT_DATA' THEN 'INSUFFICIENT_DATA'
            WHEN estimated_monthly_cost >= {expensive_monthly_cost} AND utilization_category = 'HEALTHY_UTILIZATION'
              AND COALESCE(failure_rate_pct, 0) < 5
              AND COALESCE(missing_required_tag_count, 0) = 0 THEN 'HEALTHY_EXPENSIVE'
            WHEN estimated_monthly_cost >= {expensive_monthly_cost} THEN 'EXPENSIVE_WORKLOAD'
            ELSE 'REVIEW_REQUIRED'
        END AS issue_type
    FROM signals
),
scored AS (
    SELECT
        *,
        LEAST(100.0, GREATEST(0.0, try_divide(estimated_monthly_cost, 1000.0) * 100.0)) AS cost_score,
        CASE
            WHEN issue_type = 'HIGH_COST_LOW_UTILIZATION' THEN 90.0
            WHEN issue_type = 'LOW_UTILIZATION' THEN 70.0
            WHEN issue_type = 'MEMORY_BOUND' THEN 65.0
            WHEN issue_type IN ('UNKNOWN_ATTRIBUTION', 'SHARED_CLUSTER_ATTRIBUTION') THEN 55.0
            WHEN issue_type = 'INSUFFICIENT_DATA' THEN 35.0
            WHEN issue_type = 'HEALTHY_EXPENSIVE' THEN 10.0
            ELSE 40.0
        END AS waste_score,
        LEAST(
            100.0,
            GREATEST(
                0.0,
                COALESCE(failure_rate_pct, 0) * 3.0
                + COALESCE(retry_count, 0) * 10.0
                + LEAST(40.0, COALESCE(estimated_failed_cost, 0) * {monthly_multiplier})
            )
        ) AS reliability_score,
        LEAST(
            100.0,
            GREATEST(
                0.0,
                COALESCE(missing_required_tag_count, 0) * 15.0
                + CASE
                    WHEN attribution_quality = 'UNKNOWN' THEN 40.0
                    WHEN attribution_quality = 'LOW' THEN 25.0
                    WHEN attribution_quality = 'MEDIUM' THEN 10.0
                    ELSE 0.0
                END
            )
        ) AS tagging_score,
        LEAST(100.0, GREATEST(0.0, try_divide(active_days, {lookback_days}) * 100.0)) AS frequency_score
    FROM classified
),
prioritized AS (
    SELECT
        *,
        ROUND(
            {priority_score_expr},
            6
        ) AS priority_score
    FROM scored
)
SELECT
    ROW_NUMBER() OVER (ORDER BY priority_score DESC, estimated_monthly_cost DESC) AS priority_rank,
    workspace_id,
    workload_type,
    workload_id,
    job_id,
    job_name,
    owner,
    team,
    project,
    environment,
    monthly_dbus,
    estimated_monthly_cost,
    avg_cpu_pct,
    avg_memory_pct,
    failure_rate_pct,
    retry_count,
    issue_type,
    CASE
        WHEN issue_type IN ('HIGH_COST_LOW_UTILIZATION', 'LOW_UTILIZATION') THEN 'REVIEW_WORKER_COUNT'
        WHEN issue_type = 'MEMORY_BOUND' THEN 'INVESTIGATE_MEMORY_PRESSURE'
        WHEN issue_type IN ('HIGH_FAILURE_COST', 'HIGH_FAILURE_RATE') THEN 'INVESTIGATE_FAILURES'
        WHEN issue_type = 'RETRY_HEAVY' THEN 'INVESTIGATE_RETRIES'
        WHEN issue_type = 'MISSING_TAGS' THEN 'IMPROVE_TAGGING'
        WHEN issue_type IN ('UNKNOWN_ATTRIBUTION', 'SHARED_CLUSTER_ATTRIBUTION') THEN 'REVIEW_ALL_PURPOSE_USAGE'
        WHEN issue_type = 'HEALTHY_EXPENSIVE' THEN 'LOOKS_HEALTHY'
        WHEN issue_type = 'INSUFFICIENT_DATA' THEN 'INSUFFICIENT_DATA'
        WHEN issue_type = 'EXPENSIVE_WORKLOAD' THEN 'REVIEW_NODE_TYPE'
        ELSE 'REVIEW_REQUIRED'
    END AS suggested_action,
    ROUND(waste_score, 6) AS waste_score,
    ROUND(frequency_score, 6) AS frequency_score,
    ROUND(reliability_score, 6) AS reliability_score,
    ROUND(tagging_score, 6) AS tagging_score,
    priority_score,
    CASE
        WHEN issue_type = 'INSUFFICIENT_DATA' THEN 'LOW'
        WHEN utilization_confidence = 'HIGH' OR reliability_confidence = 'HIGH' THEN 'HIGH'
        WHEN utilization_confidence = 'MEDIUM' OR reliability_confidence = 'MEDIUM' THEN 'MEDIUM'
        ELSE 'LOW'
    END AS confidence,
    CONCAT_WS(
        '; ',
        CONCAT('Estimated monthly cost $', CAST(ROUND(COALESCE(estimated_monthly_cost, 0), 2) AS STRING)),
        CONCAT('avg CPU ', CAST(ROUND(COALESCE(avg_cpu_pct, 0), 1) AS STRING), '%'),
        CONCAT('avg memory ', CAST(ROUND(COALESCE(avg_memory_pct, 0), 1) AS STRING), '%'),
        CONCAT('failure rate ', CAST(ROUND(COALESCE(failure_rate_pct, 0), 1) AS STRING), '%'),
        CONCAT('retry count ', CAST(COALESCE(retry_count, 0) AS STRING)),
        CONCAT('attribution quality ', COALESCE(attribution_quality, 'UNKNOWN'))
    ) AS evidence,
    current_timestamp() AS created_at,
    {run_id_sql} AS run_id
FROM prioritized
