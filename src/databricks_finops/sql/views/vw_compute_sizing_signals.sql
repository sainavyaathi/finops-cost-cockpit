CREATE OR REPLACE VIEW {view_name} AS
SELECT
    workspace_id,
    cluster_id,
    job_id,
    job_name,
    avg_cpu_pct,
    avg_memory_pct,
    utilization_category,
    utilization_grain,
    sizing_signal,
    confidence,
    estimated_cost,
    dbus
FROM {utilization_table}
ORDER BY estimated_cost DESC
