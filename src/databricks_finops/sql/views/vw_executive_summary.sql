CREATE OR REPLACE VIEW {view_name} AS
SELECT
    total_estimated_cost,
    total_dbus,
    workload_count,
    high_priority_candidate_count,
    untagged_cost,
    failed_cost,
    cost_with_low_attribution,
    CONCAT(CAST(lookback_start_date AS STRING), ' to ', CAST(lookback_end_date AS STRING)) AS lookback_window,
    created_at AS generated_at
FROM {summary_table}
