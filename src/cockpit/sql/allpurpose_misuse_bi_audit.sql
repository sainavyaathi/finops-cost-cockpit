-- Clock hours are cut in UTC (unix seconds), whatever the session time zone.
-- F4 signal BI_TOOL_JDBC from system.access.audit: requests from a BI / JDBC / ODBC client
-- (user_agent) that name an all-purpose cluster in request_params.clusterId.
-- ASSUMPTION (sandbox/README.md): the audit reference documents no event tying a JDBC/ODBC
-- session to a cluster id, so findings from this source get LOW confidence. Each event marks
-- its clock hour as BI activity; cost is attributed to usage overlapping those hours.
SELECT
    a.workspace_id,
    a.request_params['clusterId'] AS cluster_id,
    'BI_TOOL_JDBC' AS misuse_type,
    timestamp_seconds(FLOOR(unix_seconds(a.event_time) / 3600) * 3600) AS activity_start,
    timestamp_seconds(FLOOR(unix_seconds(a.event_time) / 3600) * 3600 + 3600) AS activity_end,
    a.user_identity.email AS actor,
    COALESCE(a.request_id, a.event_id) AS item,
    CONCAT(
        CASE
            WHEN a.user_agent RLIKE '(?i)tableau' THEN 'Tableau'
            WHEN a.user_agent RLIKE '(?i)power ?bi' THEN 'Power BI'
            WHEN a.user_agent RLIKE '(?i)looker' THEN 'Looker'
            WHEN a.user_agent RLIKE '(?i)qlik' THEN 'Qlik'
            ELSE 'BI client'
        END,
        CASE WHEN a.user_agent RLIKE '(?i)odbc' THEN ' ODBC' ELSE ' JDBC' END
    ) AS detail,
    'access.audit' AS source
FROM system.access.audit a
WHERE a.event_date >= current_date() - INTERVAL {lookback_days} DAYS
  AND a.request_params['clusterId'] IS NOT NULL
  AND a.user_agent RLIKE {bi_client_regex}
  AND COALESCE(a.response.status_code, 200) < 400
