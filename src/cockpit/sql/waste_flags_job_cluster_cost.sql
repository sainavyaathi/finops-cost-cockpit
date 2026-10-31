    -- F7 job cost per cluster it ran on (cost_base, detection window): the DBUs a JOB row shares with a
    -- CLUSTER row. waste_flags.sql uses it to cap JOB savings when the cluster already carries a saving.
    SELECT
        CAST(workspace_id AS STRING) AS workspace_id,
        CAST(workload_id AS STRING) AS job_id,
        CAST(cluster_id AS STRING) AS cluster_id,
        CAST(SUM(estimated_cost) AS DOUBLE) AS window_cost
    FROM {cost_base}
    WHERE usage_date >= current_date() - INTERVAL {detection_days} DAYS
      AND upper(workload_type) = 'JOB'
      AND workload_id IS NOT NULL
      AND cluster_id IS NOT NULL
    GROUP BY CAST(workspace_id AS STRING), CAST(workload_id AS STRING), CAST(cluster_id AS STRING)
