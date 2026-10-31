-- F5 idle and zombie compute: one row per flagged all-purpose cluster (rule R1).
-- See src/cockpit/steps/idle_compute.py and docs/architecture.md for the rules and formulas.
CREATE OR REPLACE TABLE {target}
USING DELTA
AS
WITH cluster_cost AS (
    -- Cost in the lookback window, from the engine daily_cost (DBUs x list price, upstream pattern).
    SELECT
        CAST(workspace_id AS STRING) AS workspace_id,
        CAST(cluster_id AS STRING) AS cluster_id,
        CAST(SUM(estimated_cost) AS DECIMAL(38,6)) AS estimated_cost,
        MAX(owner) AS owner_tag,
        -- Same all-purpose predicate as upstream daily_cost is_shared_cluster.
        MAX(CASE
            WHEN upper(COALESCE(billing_origin_product, '')) IN ('ALL_PURPOSE', 'INTERACTIVE')
              OR upper(COALESCE(sku_name, '')) LIKE '%ALL_PURPOSE%' THEN 1
            ELSE 0
        END) AS billed_all_purpose,
        MAX(CASE WHEN job_id IS NOT NULL THEN 1 ELSE 0 END) AS billed_to_job
    FROM {daily_cost_table}
    WHERE run_id = {run_id_sql}
      AND cluster_id IS NOT NULL
    GROUP BY CAST(workspace_id AS STRING), CAST(cluster_id AS STRING)
),
cluster_state AS (
{cluster_state_sql}
),
node_hours AS (
{node_hours_sql}
),
cluster_utilization AS (
    SELECT
        workspace_id,
        cluster_id,
        ROUND(try_divide(SUM(cpu_sum), SUM(cpu_samples)), 4) AS avg_cpu_pct,
        ROUND(try_divide(SUM(memory_sum), SUM(memory_samples)), 4) AS avg_memory_pct,
        CAST(SUM(sampled_minutes) AS BIGINT) AS active_minutes,
        -- Idle hour: CPU below the threshold and, when memory is reported, memory below its threshold
        -- (an hour with NULL memory is judged on CPU alone).
        CAST(SUM(CASE
            WHEN hour_cpu_pct < {low_cpu_pct} AND COALESCE(hour_memory_pct, 0) < {low_memory_pct} THEN sampled_minutes
            ELSE 0
        END) AS BIGINT) AS idle_minutes
    FROM node_hours
    GROUP BY workspace_id, cluster_id
),
signals AS (
    SELECT
        s.workspace_id,
        s.cluster_id,
        COALESCE(s.cluster_name, s.cluster_id) AS cluster_name,
        s.cluster_source,
        COALESCE(s.owned_by, c.owner_tag) AS owner,
        s.autotermination_minutes,
        s.config_change_time,
        u.avg_cpu_pct,
        u.avg_memory_pct,
        CAST(COALESCE(u.active_minutes, 0) AS BIGINT) AS active_minutes,
        CAST(COALESCE(u.idle_minutes, 0) AS BIGINT) AS idle_minutes,
        ROUND(CAST(COALESCE(u.active_minutes, 0) AS DOUBLE) / 60.0, 2) AS active_hours,
        ROUND(CAST(COALESCE(u.idle_minutes, 0) AS DOUBLE) / 60.0, 2) AS idle_hours,
        CAST(COALESCE(c.estimated_cost, 0) AS DECIMAL(38,6)) AS estimated_cost,
        {config_known_sql} AS config_known,
        {utilization_known_sql} AS utilization_known,
        CASE
            WHEN NOT {config_known_sql} THEN CAST(NULL AS STRING)
            WHEN s.autotermination_minutes = 0 THEN 'AUTO_TERMINATION_OFF'
            WHEN s.autotermination_minutes > {zombie_auto_term_minutes} THEN 'AUTO_TERMINATION_TOO_LONG'
            ELSE CAST(NULL AS STRING)
        END AS config_reason,
        COALESCE(
            {utilization_known_sql}
            AND try_divide(CAST(u.idle_minutes AS DOUBLE) * 100.0, CAST(u.active_minutes AS DOUBLE)) >= {idle_hours_share_pct},
            FALSE
        ) AS is_low_utilization
    FROM cluster_state s
    LEFT JOIN cluster_cost c
        ON s.workspace_id = c.workspace_id
        AND s.cluster_id = c.cluster_id
    LEFT JOIN cluster_utilization u
        ON s.workspace_id = u.workspace_id
        AND s.cluster_id = u.cluster_id
),
flagged AS (
    -- Configuration known: a zombie auto-termination setting is required.
    -- config_flag_requires_idle TRUE (current rule, AND): the cluster must also be mostly idle, so a
    --   busy or memory-bound cluster is not flagged.
    -- config_flag_requires_idle FALSE (OR variant): the setting alone is the finding when the cluster
    --   had cost in the lookback; idleness only adds evidence, savings and priority.
    -- Configuration unknown: flag on idleness alone. Utilisation unknown: flag on the setting alone.
    SELECT
        *,
        ROUND(try_divide(CAST(idle_minutes AS DOUBLE) * 100.0, CAST(active_minutes AS DOUBLE)), 2) AS idle_hours_pct,
        CASE
            WHEN utilization_known AND active_minutes > 0
                THEN CAST(estimated_cost * try_divide(CAST(idle_minutes AS DOUBLE), CAST(active_minutes AS DOUBLE)) AS DECIMAL(38,6))
            ELSE CAST(NULL AS DECIMAL(38,6))
        END AS idle_cost
    FROM signals
    WHERE (config_known OR utilization_known)
      AND (config_reason IS NOT NULL OR NOT config_known)
      AND (
            is_low_utilization
         OR (NOT utilization_known AND estimated_cost > 0)
         OR (config_known AND NOT {config_flag_requires_idle_sql} AND estimated_cost > 0)
      )
),
costed AS (
    SELECT
        *,
        CAST(estimated_cost * {monthly_multiplier} AS DECIMAL(38,6)) AS estimated_monthly_cost,
        -- Savings = idle cost normalised to 30 days; never above the own cost of the cluster (R1: one row per cluster).
        -- Only an idle cluster has savings. LEAST skips NULLs, so an unknown idle cost stays NULL explicitly.
        CASE
            WHEN NOT is_low_utilization OR idle_cost IS NULL THEN CAST(NULL AS DECIMAL(38,6))
            ELSE CAST(LEAST(idle_cost, estimated_cost) * {monthly_multiplier} AS DECIMAL(38,6))
        END AS estimated_monthly_savings,
        concat_ws('+', config_reason, CASE WHEN is_low_utilization THEN 'LOW_UTILIZATION' END) AS idle_reason,
        CASE
            WHEN config_known AND utilization_known THEN 'CONFIG_AND_UTILIZATION'
            WHEN config_known THEN 'CONFIG_ONLY'
            ELSE 'UTILIZATION_ONLY'
        END AS detection_mode
    FROM flagged
)
SELECT
    CAST(workspace_id AS STRING) AS workspace_id,
    CAST(cluster_id AS STRING) AS cluster_id,
    CAST(cluster_name AS STRING) AS cluster_name,
    CAST(cluster_source AS STRING) AS cluster_source,
    CAST(owner AS STRING) AS owner,
    CAST(autotermination_minutes AS BIGINT) AS autotermination_minutes,
    CAST(config_change_time AS TIMESTAMP) AS config_change_time,
    CAST(avg_cpu_pct AS DOUBLE) AS avg_cpu_pct,
    CAST(avg_memory_pct AS DOUBLE) AS avg_memory_pct,
    CAST(active_hours AS DOUBLE) AS active_hours,
    CAST(idle_hours AS DOUBLE) AS idle_hours,
    CAST(idle_hours_pct AS DOUBLE) AS idle_hours_pct,
    CAST(estimated_cost AS DECIMAL(38,6)) AS estimated_cost,
    CAST(idle_cost AS DECIMAL(38,6)) AS idle_cost,
    CAST(estimated_monthly_cost AS DECIMAL(38,6)) AS estimated_monthly_cost,
    CAST(estimated_monthly_savings AS DECIMAL(38,6)) AS estimated_monthly_savings,
    CAST(idle_reason AS STRING) AS idle_reason,
    CAST(CASE
        WHEN config_reason = 'AUTO_TERMINATION_OFF'
            THEN 'Turn auto-termination on ({zombie_auto_term_minutes} minutes or less), then review sizing'
        WHEN config_reason = 'AUTO_TERMINATION_TOO_LONG'
            THEN concat('Lower auto-termination from ', CAST(autotermination_minutes AS STRING),
                        ' to {zombie_auto_term_minutes} minutes or less')
        WHEN NOT config_known
            THEN 'Check auto-termination ({zombie_auto_term_minutes} minutes or less) and review sizing'
        ELSE 'Review sizing'
    END AS STRING) AS suggested_action,
    CAST(detection_mode AS STRING) AS detection_mode,
    CAST(CASE
        WHEN config_known AND utilization_known AND active_hours >= {high_confidence_min_active_hours} THEN 'HIGH'
        WHEN config_known AND utilization_known THEN 'MEDIUM'
        ELSE 'LOW'
    END AS STRING) AS confidence,
    CAST(ROUND(LEAST(100.0, GREATEST(0.0,
        {priority_weight_savings} * LEAST(100.0,
            COALESCE(CAST(estimated_monthly_savings AS DOUBLE), 0.0) / {priority_full_savings_monthly} * 100.0)
        + {priority_weight_idle} * COALESCE(idle_hours_pct, 0.0)
        + {priority_weight_config} * CASE
            WHEN config_reason = 'AUTO_TERMINATION_OFF' THEN {config_score_off}
            WHEN config_reason = 'AUTO_TERMINATION_TOO_LONG' THEN {config_score_too_long}
            ELSE {config_score_unknown}
        END
    )), 2) AS DOUBLE) AS priority_score,
    CAST(concat_ws('; ',
        CASE
            WHEN NOT config_known THEN 'compute.clusters unavailable: auto-termination not checked'
            WHEN autotermination_minutes = 0
                THEN concat('auto-termination off (0 min) since ', COALESCE(date_format(config_change_time, 'yyyy-MM-dd'), 'unknown date'))
            ELSE concat('auto-termination ', CAST(autotermination_minutes AS STRING), ' min (over {zombie_auto_term_minutes}) since ',
                        COALESCE(date_format(config_change_time, 'yyyy-MM-dd'), 'unknown date'))
        END,
        CASE
            WHEN NOT utilization_known THEN 'node_timeline unavailable: utilisation not checked'
            ELSE concat(format_number(idle_hours, 1), ' of ', format_number(active_hours, 1),
                        ' active hours idle (CPU < {low_cpu_pct}% and memory < {low_memory_pct}%)')
        END,
        CASE
            WHEN avg_cpu_pct IS NOT NULL
                THEN concat('avg CPU ', format_number(avg_cpu_pct, 1), '%, memory ',
                            COALESCE(concat(format_number(avg_memory_pct, 1), '%'), 'n/a'))
        END,
        concat(format_number(estimated_cost, 2), ' ', {currency_code_sql}, ' over {lookback_days} days')
    ) AS STRING) AS evidence,
    CAST({lookback_days} AS INT) AS lookback_days,
    current_timestamp() AS created_at,
    {run_id_sql} AS run_id
FROM costed
