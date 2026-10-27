CREATE OR REPLACE VIEW {view_name} AS
SELECT
    cost_rank AS rank,
    workload_type,
    workload_id,
    job_name,
    owner,
    team,
    project,
    estimated_cost,
    total_dbus,
    attribution_quality,
    CASE
        WHEN attribution_quality IN ('LOW', 'UNKNOWN') THEN 'Review attribution'
        WHEN cost_rank <= 10 THEN 'Top spend workload'
        ELSE 'Monitor'
    END AS recommended_review_reason
FROM {workload_table}
ORDER BY cost_rank
