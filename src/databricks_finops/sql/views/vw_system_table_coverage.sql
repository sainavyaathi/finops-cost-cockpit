CREATE OR REPLACE VIEW {view_name} AS
WITH latest_run AS (
    SELECT run_id
    FROM {summary_table}
    ORDER BY created_at DESC
    LIMIT 1
)
SELECT
    run_id,
    check_name,
    status,
    severity,
    message,
    affected_output,
    created_at
FROM {health_table}
WHERE run_id IN (SELECT run_id FROM latest_run)
  AND check_name LIKE '%available'
ORDER BY severity DESC, check_name
