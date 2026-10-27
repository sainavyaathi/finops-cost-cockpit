CREATE OR REPLACE TABLE {target_table}
USING DELTA
AS
SELECT
    workspace_id,
    cluster_id,
    job_id,
    job_name,
    CAST(NULL AS DOUBLE) AS avg_cpu_pct,
    CAST(NULL AS DOUBLE) AS peak_cpu_pct,
    CAST(NULL AS DOUBLE) AS avg_memory_pct,
    CAST(NULL AS DOUBLE) AS peak_memory_pct,
    CAST(NULL AS DOUBLE) AS avg_network_sent_bytes,
    CAST(NULL AS DOUBLE) AS avg_network_received_bytes,
    total_dbus AS dbus,
    estimated_cost,
    CAST(NULL AS DOUBLE) AS dbus_per_cpu_pct,
    'INSUFFICIENT_DATA' AS utilization_category,
    'INSUFFICIENT_DATA' AS utilization_grain,
    'INSUFFICIENT_DATA' AS sizing_signal,
    'LOW' AS confidence,
    current_timestamp() AS created_at,
    {run_id_sql} AS run_id
FROM {workload_table}
WHERE run_id = {run_id_sql}
