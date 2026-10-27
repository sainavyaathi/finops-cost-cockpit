CREATE OR REPLACE VIEW {view_name} AS
SELECT *
FROM {reliability_table}
WHERE failed_count > 0
   OR cancelled_count > 0
   OR retry_count > 0
ORDER BY estimated_failed_cost DESC
