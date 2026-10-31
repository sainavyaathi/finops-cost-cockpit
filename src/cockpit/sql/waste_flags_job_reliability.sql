    -- F7 findings from F6 cockpit.job_reliability (corrected retry_count), up to two per job:
    -- * FAILURE_HEAVY (failure_rate_pct >= high_failure_rate_pct) or HIGH_FAILURE_COST (monthly failed
    --   cost >= high_failed_cost): saving = failed-run cost per month (billing joined by job_run_id
    --   upstream), confidence = F6 confidence. Same thresholds as upstream optimization_candidates.
    -- * RETRY_HEAVY (retry_count >= retry_heavy_count): saving = the job's monthly cost (cost_base)
    --   x retry_task_minutes / task_attempt_minutes, i.e. cost spread evenly over task time.
    --   Billing has no per-attempt cost, so this is an estimate and confidence is capped at MEDIUM.
    -- Both figures are window amounts scaled to 30 days. A job with both keeps the larger (R1).
    -- source_monthly_cost: job_reliability has no total job cost, so it is the job's cost_base cost in the
    -- window, or (cost_base unreadable / no row) the failed-run cost as a floor, so a failure saving is
    -- never capped to 0 by an unknown cost.
    WITH jobs AS (
        SELECT
            CAST(j.workspace_id AS STRING) AS workspace_id,
            CAST(j.job_id AS STRING) AS job_id,
            CAST(j.job_name AS STRING) AS job_name,
            CAST(j.run_count AS BIGINT) AS run_count,
            CAST(j.failed_count AS BIGINT) AS failed_count,
            CAST(j.failure_rate_pct AS DOUBLE) AS failure_rate_pct,
            CAST(j.retry_count AS BIGINT) AS retry_count,
            CAST({retried_run_count_column} AS BIGINT) AS retried_run_count,
            CAST({retry_task_minutes_column} AS DOUBLE) AS retry_task_minutes,
            CAST({task_attempt_minutes_column} AS DOUBLE) AS task_attempt_minutes,
            CAST(j.estimated_failed_cost AS DOUBLE) AS failed_cost,
            CAST({job_window_days_column} AS DOUBLE) AS window_days,
            CAST(j.confidence AS STRING) AS confidence,
            rc.window_cost AS job_window_cost
        FROM {source_table} j
        LEFT JOIN resource_cost rc
            ON rc.resource_type = 'JOB'
            AND rc.workspace_id = CAST(j.workspace_id AS STRING)
            AND rc.resource_id = CAST(j.job_id AS STRING)
    ),
    costed_jobs AS (
        SELECT
            *,
            COALESCE(failed_cost, 0.0) * 30.0 / window_days AS monthly_failed_cost,
            CAST(COALESCE(job_window_cost * {monthly_multiplier},
                          COALESCE(failed_cost, 0.0) * 30.0 / window_days) AS DECIMAL(38,6)) AS source_monthly_cost
        FROM jobs
    )
    SELECT
        'JOB' AS resource_type,
        workspace_id,
        job_id AS resource_id,
        job_name AS resource_name,
        CAST(NULL AS STRING) AS owner,
        CAST(NULL AS STRING) AS team,
        'job_reliability' AS source,
        3 AS source_order,
        CASE WHEN failure_rate_pct >= {high_failure_rate_pct} THEN 'FAILURE_HEAVY' ELSE 'HIGH_FAILURE_COST' END
            AS flag_type,
        CAST(monthly_failed_cost AS DECIMAL(38,6)) AS savings,
        confidence,
        CONCAT(failed_count, ' of ', run_count, ' runs failed (', format_number(failure_rate_pct, 1),
               '%); failed-run cost ', format_number(failed_cost, 2), ' ', {currency_code_sql}, ' over ',
               CAST(window_days AS INT), ' days') AS evidence,
        'INVESTIGATE_FAILURES' AS suggested_action,
        source_monthly_cost
    FROM costed_jobs
    WHERE run_count > 0
      AND (failure_rate_pct >= {high_failure_rate_pct} OR monthly_failed_cost >= {high_failed_cost})
    UNION ALL
    SELECT
        'JOB' AS resource_type,
        workspace_id,
        job_id AS resource_id,
        job_name AS resource_name,
        CAST(NULL AS STRING) AS owner,
        CAST(NULL AS STRING) AS team,
        'job_reliability' AS source,
        3 AS source_order,
        'RETRY_HEAVY' AS flag_type,
        CAST(job_window_cost * try_divide(retry_task_minutes, task_attempt_minutes)
             * {monthly_multiplier} AS DECIMAL(38,6)) AS savings,
        CASE WHEN confidence = 'HIGH' THEN 'MEDIUM' ELSE confidence END AS confidence,
        CONCAT(retry_count, ' task retries in ', COALESCE(CAST(retried_run_count AS STRING), '?'), ' of ',
               run_count, ' runs; ',
               COALESCE(CONCAT(format_number(100.0 * try_divide(retry_task_minutes, task_attempt_minutes), 1),
                               '% of task time spent on retried attempts'),
                        'retry time unknown'),
               CASE WHEN job_window_cost IS NULL THEN '; job cost unknown (no cost_base row), retry cost not estimated'
                    ELSE '; retry cost = job cost x that share (estimate)' END) AS evidence,
        'INVESTIGATE_RETRIES' AS suggested_action,
        source_monthly_cost
    FROM costed_jobs
    WHERE run_count > 0
      AND retry_count >= {retry_heavy_count}
