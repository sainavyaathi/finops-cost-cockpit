-- Clock hours are cut in UTC (unix seconds), whatever the session time zone.
-- F4 signal BI_TOOL_JDBC from system.query.history: statements sent over JDBC/ODBC (client_driver)
-- or by a BI application to a cluster (compute.cluster_id). Documented columns, so MEDIUM
-- confidence. On most accounts query.history covers SQL warehouses and serverless only, so this
-- source is usually empty for classic all-purpose clusters; access.audit is then the only signal.
SELECT
    q.workspace_id,
    q.compute.cluster_id AS cluster_id,
    'BI_TOOL_JDBC' AS misuse_type,
    timestamp_seconds(FLOOR(unix_seconds(q.start_time) / 3600) * 3600) AS activity_start,
    timestamp_seconds(FLOOR(unix_seconds(q.start_time) / 3600) * 3600 + 3600) AS activity_end,
    q.executed_by AS actor,
    q.statement_id AS item,
    CONCAT(
        COALESCE(q.client_application, 'BI client'),
        CASE WHEN q.client_driver RLIKE '(?i)odbc' THEN ' ODBC' ELSE ' JDBC' END
    ) AS detail,
    'query.history' AS source
FROM system.query.history q
WHERE q.start_time >= current_date() - INTERVAL {lookback_days} DAYS
  AND q.compute.cluster_id IS NOT NULL
  -- Cluster compute only: warehouse and serverless statements are the right place for BI traffic.
  AND UPPER(COALESCE(q.compute.type, '')) NOT IN ('WAREHOUSE', 'SERVERLESS_COMPUTE')
  AND (
      q.client_driver RLIKE '(?i)(jdbc|odbc)'
      OR q.client_application RLIKE {bi_client_regex}
  )
