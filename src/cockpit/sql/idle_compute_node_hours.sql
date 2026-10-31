    -- Utilisation per cluster and clock hour (session time zone, UTC on Databricks) across all its
    -- nodes, with the same CPU (user + system) and memory metrics as compute_utilization_summary
    -- (driver and workers averaged together, like upstream). The window starts at midnight of
    -- current_date - lookback, the same as daily_cost (usage_date >= current_date - lookback).
    -- sampled_minutes weights the hour: a cluster seen for 3 minutes of an hour counts 3/60 of an hour.
    SELECT
        CAST(workspace_id AS STRING) AS workspace_id,
        CAST(cluster_id AS STRING) AS cluster_id,
        date_trunc('HOUR', start_time) AS usage_hour,
        CAST(COUNT(DISTINCT date_trunc('MINUTE', start_time)) AS BIGINT) AS sampled_minutes,
        AVG({cpu_expr}) AS hour_cpu_pct,
        AVG({memory_expr}) AS hour_memory_pct,
        SUM({cpu_expr}) AS cpu_sum,
        COUNT({cpu_expr}) AS cpu_samples,
        SUM({memory_expr}) AS memory_sum,
        COUNT({memory_expr}) AS memory_samples
    FROM system.compute.node_timeline
    WHERE start_time >= CAST(current_date() - INTERVAL {lookback_days} DAYS AS TIMESTAMP)
      AND cluster_id IS NOT NULL
    GROUP BY CAST(workspace_id AS STRING), CAST(cluster_id AS STRING), date_trunc('HOUR', start_time)
