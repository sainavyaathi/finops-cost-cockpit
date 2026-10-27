CREATE OR REPLACE VIEW {view_name} AS
SELECT
    attribution_quality,
    attribution_notes,
    COUNT(*) AS workload_count,
    ROUND(SUM(estimated_cost), 6) AS estimated_cost,
    ROUND(SUM(total_dbus), 6) AS total_dbus
FROM {workload_table}
GROUP BY attribution_quality, attribution_notes
ORDER BY estimated_cost DESC
