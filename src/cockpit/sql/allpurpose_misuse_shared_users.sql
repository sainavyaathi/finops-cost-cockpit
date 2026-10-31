-- Clock hours are cut in UTC (unix seconds), whatever the session time zone.
-- F4 signal SHARED_USERS from system.access.audit: distinct people attaching notebooks to a
-- cluster. notebook.attachNotebook is always logged and carries clusterId; runCommand is
-- verbose-audit only and has no clusterId, and billing run_as is usually empty on all-purpose
-- compute, so neither is used (sandbox/README.md).
SELECT
    a.workspace_id,
    a.request_params['clusterId'] AS cluster_id,
    'SHARED_USERS' AS misuse_type,
    timestamp_seconds(FLOOR(unix_seconds(a.event_time) / 3600) * 3600) AS activity_start,
    timestamp_seconds(FLOOR(unix_seconds(a.event_time) / 3600) * 3600 + 3600) AS activity_end,
    a.user_identity.email AS actor,
    a.request_params['notebookId'] AS item,
    CAST(NULL AS STRING) AS detail,
    'access.audit' AS source
FROM system.access.audit a
WHERE a.event_date >= current_date() - INTERVAL {detection_days} DAYS
  AND a.service_name = 'notebook'
  AND a.action_name = 'attachNotebook'
  AND a.request_params['clusterId'] IS NOT NULL
  AND a.user_identity.email IS NOT NULL
  AND COALESCE(a.response.status_code, 200) < 400
