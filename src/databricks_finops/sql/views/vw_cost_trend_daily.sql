CREATE OR REPLACE VIEW {view_name} AS
WITH reliability_days AS (
    SELECT
        d.run_id,
        d.workspace_id,
        d.job_id,
        COUNT(DISTINCT d.usage_date) AS active_days
    FROM {daily_table} d
    WHERE d.job_id IS NOT NULL
    GROUP BY d.run_id, d.workspace_id, d.job_id
),
failed_cost_by_day AS (
    SELECT
        d.run_id,
        d.workspace_id,
        d.usage_date,
        ROUND(
            SUM(
                COALESCE(r.estimated_failed_cost, 0)
                * (1.0 / NULLIF(rd.active_days, 0))
            ),
            6
        ) AS failed_cost
    FROM {daily_table} d
    LEFT JOIN {reliability_table} r
        ON d.run_id = r.run_id
        AND d.workspace_id = r.workspace_id
        AND d.job_id = r.job_id
    LEFT JOIN reliability_days rd
        ON d.run_id = rd.run_id
        AND d.workspace_id = rd.workspace_id
        AND d.job_id = rd.job_id
    GROUP BY d.run_id, d.workspace_id, d.usage_date
)
SELECT
    d.usage_date,
    ROUND(SUM(d.estimated_cost), 6) AS estimated_cost,
    ROUND(SUM(d.dbus), 6) AS dbus,
    COUNT(DISTINCT d.workload_id) AS workload_count,
    ROUND(SUM(CASE WHEN t.missing_required_tag_count > 0 THEN d.estimated_cost ELSE 0 END), 6)
        AS untagged_cost,
    ROUND(MAX(COALESCE(f.failed_cost, 0.0)), 6) AS failed_cost
FROM {daily_table} d
LEFT JOIN {tagging_table} t
    ON d.run_id = t.run_id
    AND d.usage_date = t.usage_date
    AND d.workspace_id = t.workspace_id
    AND d.workload_type = t.workload_type
    AND d.workload_id = t.workload_id
LEFT JOIN failed_cost_by_day f
    ON d.run_id = f.run_id
    AND d.workspace_id = f.workspace_id
    AND d.usage_date = f.usage_date
GROUP BY d.usage_date
ORDER BY d.usage_date
