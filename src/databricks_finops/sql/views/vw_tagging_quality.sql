CREATE OR REPLACE VIEW {view_name} AS
SELECT
    team,
    owner,
    project,
    ROUND(SUM(estimated_cost), 6) AS estimated_cost,
    MAX(missing_required_tag_count) AS missing_required_tag_count,
    CASE MIN(
        CASE tagging_quality
            WHEN 'MISSING' THEN 0
            WHEN 'POOR'    THEN 1
            WHEN 'PARTIAL' THEN 2
            WHEN 'GOOD'    THEN 3
            ELSE               4
        END
    )
        WHEN 0 THEN 'MISSING'
        WHEN 1 THEN 'POOR'
        WHEN 2 THEN 'PARTIAL'
        WHEN 3 THEN 'GOOD'
        ELSE        'UNKNOWN'
    END AS worst_tagging_quality
FROM {tagging_table}
GROUP BY team, owner, project
ORDER BY estimated_cost DESC
