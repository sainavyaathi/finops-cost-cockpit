CREATE OR REPLACE TABLE {target_table}
USING DELTA
AS
WITH cluster_cost AS (
    SELECT
        workspace_id,
        cluster_id,
        FIRST(job_id, TRUE) AS job_id,
        FIRST(job_name, TRUE) AS job_name,
        ROUND(SUM(dbus), 6) AS dbus,
        ROUND(SUM(estimated_cost), 6) AS estimated_cost
    FROM {daily_cost_table}
    WHERE run_id = {run_id_sql}
      AND cluster_id IS NOT NULL
    GROUP BY workspace_id, cluster_id
),
cluster_metrics AS (
    SELECT
        workspace_id,
        cluster_id,
        ROUND(AVG({cpu_user_expr} + {cpu_system_expr}), 4) AS avg_cpu_pct,
        ROUND(MAX({cpu_user_expr} + {cpu_system_expr}), 4) AS peak_cpu_pct,
        ROUND(AVG({memory_expr}), 4) AS avg_memory_pct,
        ROUND(MAX({memory_expr}), 4) AS peak_memory_pct,
        ROUND(AVG({network_sent_expr}), 4) AS avg_network_sent_bytes,
        ROUND(AVG({network_received_expr}), 4) AS avg_network_received_bytes,
        COUNT(*) AS metric_rows
    FROM system.compute.node_timeline
    WHERE start_time >= current_timestamp() - INTERVAL {lookback_days} DAYS
    GROUP BY workspace_id, cluster_id
)
SELECT
    c.workspace_id,
    c.cluster_id,
    c.job_id,
    c.job_name,
    m.avg_cpu_pct,
    m.peak_cpu_pct,
    m.avg_memory_pct,
    m.peak_memory_pct,
    m.avg_network_sent_bytes,
    m.avg_network_received_bytes,
    c.dbus,
    c.estimated_cost,
    ROUND(try_divide(c.dbus, m.avg_cpu_pct), 6) AS dbus_per_cpu_pct,
    CASE
        WHEN m.avg_cpu_pct IS NULL THEN 'INSUFFICIENT_DATA'
        WHEN m.avg_cpu_pct < {low_cpu_pct}
         AND COALESCE(m.avg_memory_pct, 0) < {low_memory_pct}
         AND c.estimated_cost >= {high_cost_low_utilization_min_cost}
            THEN 'HIGH_COST_LOW_UTILIZATION'
        WHEN m.avg_cpu_pct < {low_cpu_pct}
         AND COALESCE(m.avg_memory_pct, 0) < {low_memory_pct}
            THEN 'LOW_UTILIZATION'
        WHEN m.avg_cpu_pct < {low_memory_pct}
         AND COALESCE(m.avg_memory_pct, 0) >= {high_memory_pct}
            THEN 'MEMORY_BOUND_DO_NOT_DOWNSIZE'
        WHEN m.avg_cpu_pct >= {low_memory_pct}
         AND COALESCE(m.avg_memory_pct, 0) < 90
            THEN 'HEALTHY_UTILIZATION'
        ELSE 'REVIEW_REQUIRED'
    END AS utilization_category,
    'CLUSTER_LOOKBACK_WINDOW' AS utilization_grain,
    CASE
        WHEN m.avg_cpu_pct IS NULL THEN 'INSUFFICIENT_DATA'
        WHEN m.avg_cpu_pct < {low_cpu_pct}
         AND COALESCE(m.avg_memory_pct, 0) < {low_memory_pct} THEN 'REVIEW_WORKER_COUNT'
        WHEN m.avg_cpu_pct < {low_memory_pct}
         AND COALESCE(m.avg_memory_pct, 0) >= {high_memory_pct} THEN 'INVESTIGATE_MEMORY_PRESSURE'
        WHEN m.avg_cpu_pct < {low_memory_pct} THEN 'REVIEW_NODE_TYPE'
        ELSE 'LOOKS_HEALTHY'
    END AS sizing_signal,
    CASE
        WHEN COALESCE(m.metric_rows, 0) >= 60 THEN 'HIGH'
        WHEN COALESCE(m.metric_rows, 0) > 0 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS confidence,
    current_timestamp() AS created_at,
    {run_id_sql} AS run_id
FROM cluster_cost c
LEFT JOIN cluster_metrics m
    ON c.workspace_id = m.workspace_id
    AND c.cluster_id = m.cluster_id
