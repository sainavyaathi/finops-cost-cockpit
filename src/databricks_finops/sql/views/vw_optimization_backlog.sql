CREATE OR REPLACE VIEW {view_name} AS
SELECT
    priority_rank,
    issue_type,
    suggested_action,
    estimated_monthly_cost,
    owner,
    team,
    project,
    confidence,
    evidence
FROM {candidates_table}
WHERE suggested_action <> 'LOOKS_HEALTHY'
ORDER BY priority_rank
