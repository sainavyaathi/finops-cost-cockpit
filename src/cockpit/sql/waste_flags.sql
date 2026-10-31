-- F7 cockpit.waste_flags: the one waste backlog the app and the F10 alert read.
-- One row per (resource_type, workspace_id, resource_id). Findings come from F4 allpurpose_misuse,
-- F5 idle_compute, F6 job_reliability and upstream optimization_candidates; a source table that is
-- missing is replaced by an empty relation (see cockpit/steps/waste_flags.py).
-- R1: savings are counted once per resource: the largest finding saving (MAX, never SUM), capped at
-- the resource's monthly cost; a JOB's saving also leaves out the cost it shares with a CLUSTER that
-- carries a saving. Every finding stays visible in findings / flag_types / evidence.
-- R5: priority_score is one 0-100 scale for every source (formula in docs/architecture.md).
CREATE OR REPLACE TABLE {target}
USING DELTA
AS
WITH
resource_cost AS (
{resource_cost_relation}
),
job_cluster_cost AS (
{job_cluster_cost_relation}
),
f5_findings AS (
{idle_compute_relation}
),
f4_findings AS (
{allpurpose_misuse_relation}
),
f6_findings AS (
{job_reliability_relation}
),
upstream_findings AS (
{optimization_candidates_relation}
),
all_findings AS (
    SELECT * FROM f5_findings
    UNION ALL SELECT * FROM f4_findings
    UNION ALL SELECT * FROM f6_findings
    UNION ALL SELECT * FROM upstream_findings
),
-- Clusters with a cockpit diagnosis (F4 or F5): upstream's generic SHARED_CLUSTER_ATTRIBUTION is
-- superseded there.
cockpit_clusters AS (
    SELECT DISTINCT workspace_id, resource_id
    FROM all_findings
    WHERE resource_type = 'CLUSTER' AND source IN ('allpurpose_misuse', 'idle_compute')
),
-- Resource monthly cost: cost_base over the detection window x 30 / detection_days. When cost_base has
-- no row for the resource, the largest monthly cost a source reported is used; NULL when nobody knows.
-- Upstream findings on a resource with no cost in the window are stale (upstream classifies over its
-- own lookback) and are dropped while cost_base is readable.
kept_findings AS (
    SELECT
        f.*,
        CAST(COALESCE(
            rc.window_cost * {monthly_multiplier},
            MAX(CAST(f.source_monthly_cost AS DOUBLE)) OVER (
                PARTITION BY f.resource_type, f.workspace_id, f.resource_id)
        ) AS DOUBLE) AS resource_monthly_cost,
        rc.owner_tag,
        rc.team_tag
    FROM all_findings f
    LEFT JOIN cockpit_clusters k
        ON f.resource_type = 'CLUSTER'
        AND f.workspace_id = k.workspace_id
        AND f.resource_id = k.resource_id
    LEFT JOIN resource_cost rc
        ON f.resource_type = rc.resource_type
        AND f.workspace_id = rc.workspace_id
        AND f.resource_id = rc.resource_id
    WHERE f.workspace_id IS NOT NULL
      AND f.resource_id IS NOT NULL
      AND f.flag_type IS NOT NULL
      AND NOT (f.source = 'optimization_candidates'
               AND f.flag_type = 'SHARED_CLUSTER_ATTRIBUTION'
               AND k.resource_id IS NOT NULL)
      AND NOT (f.source = 'optimization_candidates'
               AND {cost_base_available_sql}
               AND COALESCE(rc.window_cost, 0.0) <= 0.0)
),
ranked_findings AS (
    SELECT
        *,
        -- Each finding's saving is capped at the resource cost when that is known; with no known cost
        -- the finding's own basis stands (never capped to 0). LEAST skips NULLs, so an unknown saving
        -- stays NULL explicitly.
        CASE WHEN savings IS NULL THEN CAST(NULL AS DECIMAL(38,6))
             WHEN resource_monthly_cost IS NULL THEN CAST(savings AS DECIMAL(38,6))
             ELSE CAST(LEAST(CAST(savings AS DOUBLE), resource_monthly_cost) AS DECIMAL(38,6))
        END AS capped_savings,
        -- Primary finding: largest saving, then higher confidence, then source order
        -- (idle_compute, allpurpose_misuse, job_reliability, optimization_candidates), then flag type,
        -- then evidence (deterministic).
        ROW_NUMBER() OVER (
            PARTITION BY resource_type, workspace_id, resource_id
            ORDER BY CAST(savings AS DOUBLE) DESC NULLS LAST,
                     CASE confidence WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 WHEN 'LOW' THEN 3 ELSE 4 END,
                     source_order,
                     flag_type,
                     evidence
        ) AS finding_rank
    FROM kept_findings
),
per_resource AS (
    SELECT
        resource_type,
        workspace_id,
        resource_id,
        MIN_BY(resource_name, CASE WHEN resource_name IS NULL THEN 1000000 ELSE finding_rank END) AS resource_name,
        MIN_BY(owner, CASE WHEN owner IS NULL THEN 1000000 ELSE finding_rank END) AS source_owner,
        MIN_BY(team, CASE WHEN team IS NULL THEN 1000000 ELSE finding_rank END) AS source_team,
        MAX(owner_tag) AS owner_tag,
        MAX(team_tag) AS team_tag,
        MAX(CASE WHEN finding_rank = 1 THEN flag_type END) AS primary_flag_type,
        MAX(CASE WHEN finding_rank = 1 THEN source END) AS primary_source,
        MAX(CASE WHEN finding_rank = 1 THEN suggested_action END) AS suggested_action,
        MAX(CASE WHEN finding_rank = 1 THEN confidence END) AS primary_confidence,
        array_distinct(transform(
            array_sort(collect_list(named_struct('r', finding_rank, 'v', flag_type))), x -> x.v)) AS flag_types,
        array_distinct(transform(
            array_sort(collect_list(named_struct('r', source_order, 'v', source))), x -> x.v)) AS sources,
        transform(
            array_sort(collect_list(named_struct(
                'r', finding_rank,
                'f', named_struct('source', source, 'flag_type', flag_type,
                                  'savings', capped_savings, 'confidence', confidence)))),
            x -> x.f) AS findings,
        concat_ws(' | ', transform(
            array_sort(collect_list(CASE WHEN evidence IS NOT NULL THEN named_struct(
                'r', finding_rank, 'v', CONCAT('[', source, '] ', evidence)) END)),
            x -> x.v)) AS evidence,
        CAST(COUNT(DISTINCT flag_type) AS INT) AS flag_type_count,
        -- R1: the largest finding saving, never the sum.
        MAX(capped_savings) AS kept_savings,
        MAX(resource_monthly_cost) AS resource_monthly_cost
    FROM ranked_findings
    GROUP BY resource_type, workspace_id, resource_id
),
-- R1 across resources: a job running on a cluster that carries a saving shares those DBUs with the
-- CLUSTER row (CLUSTER cost includes every row with its cluster_id). That shared cost is left to the
-- cluster, so the JOB saving is capped at its cost on other compute.
job_shared_cost AS (
    SELECT
        j.workspace_id,
        j.job_id,
        SUM(j.window_cost) * {monthly_multiplier} AS shared_monthly_cost,
        array_join(array_sort(collect_set(j.cluster_id)), ', ') AS shared_clusters
    FROM job_cluster_cost j
    JOIN per_resource c
        ON c.resource_type = 'CLUSTER'
        AND c.workspace_id = j.workspace_id
        AND c.resource_id = j.cluster_id
        AND COALESCE(c.kept_savings, 0) > 0
    GROUP BY j.workspace_id, j.job_id
),
scored AS (
    SELECT
        p.*,
        s.shared_monthly_cost,
        s.shared_clusters,
        CAST(COALESCE(CAST(p.kept_savings AS DOUBLE), 0.0) AS DOUBLE) AS uncapped_savings,
        CAST(CASE
            WHEN p.kept_savings IS NULL THEN 0.0
            WHEN p.resource_type = 'JOB' AND s.shared_monthly_cost IS NOT NULL
                THEN LEAST(CAST(p.kept_savings AS DOUBLE),
                           GREATEST(0.0, COALESCE(p.resource_monthly_cost, 0.0) - s.shared_monthly_cost))
            ELSE CAST(p.kept_savings AS DOUBLE)
        END AS DOUBLE) AS monthly_savings,
        COALESCE(p.primary_confidence, 'LOW') AS confidence
    FROM per_resource p
    LEFT JOIN job_shared_cost s
        ON p.resource_type = 'JOB'
        AND p.workspace_id = s.workspace_id
        AND p.resource_id = s.job_id
),
costed AS (
    SELECT
        *,
        -- Unknown resource cost: the saving is the floor (the cost is at least what can be saved).
        CAST(COALESCE(resource_monthly_cost, monthly_savings, 0.0) AS DOUBLE) AS monthly_cost
    FROM scored
),
prioritized AS (
    SELECT
        *,
        -- With validated settings (weights sum to 1, multipliers <= 1, each component <= 100) the
        -- weighted sum is already within 0-100; the clamp is a guard.
        ROUND(LEAST(100.0, GREATEST(0.0, (
            {priority_weight_savings} * LEAST(100.0, monthly_savings / {score_reference_monthly_savings} * 100.0)
            + {priority_weight_cost} * LEAST(100.0, monthly_cost / {score_reference_monthly_cost} * 100.0)
            + {priority_weight_breadth} * LEAST(100.0, (flag_type_count - 1) * {breadth_points_per_extra_flag})
        ) * CASE confidence
                WHEN 'HIGH' THEN {confidence_multiplier_high}
                WHEN 'MEDIUM' THEN {confidence_multiplier_medium}
                ELSE {confidence_multiplier_low}
            END)), 2) AS score
    FROM costed
)
SELECT
    CAST(resource_type AS STRING) AS resource_type,
    CAST(workspace_id AS STRING) AS workspace_id,
    CAST(resource_id AS STRING) AS resource_id,
    CAST(COALESCE(resource_name, resource_id) AS STRING) AS resource_name,
    -- Same precedence for owner and team: the source's value first, then the cost_base tag.
    CAST(COALESCE(source_owner, owner_tag) AS STRING) AS owner,
    CAST(COALESCE(source_team, team_tag) AS STRING) AS team,
    CAST(primary_flag_type AS STRING) AS primary_flag_type,
    flag_types,
    findings,
    -- Same as findings, as JSON text for CSV and clients without struct support; NULLs kept so every
    -- element has the same four keys.
    CAST(to_json(findings, map('ignoreNullFields', 'false')) AS STRING) AS findings_json,
    CAST(CASE
        WHEN monthly_savings < uncapped_savings THEN CONCAT(
            evidence, ' | [waste_flags] ', format_number(shared_monthly_cost, 2), ' ', {currency_code_sql},
            '/month of this job runs on flagged cluster(s) ', shared_clusters,
            '; that cost is left to the cluster, saving capped (R1)')
        ELSE evidence
    END AS STRING) AS evidence,
    CAST(suggested_action AS STRING) AS suggested_action,
    CAST(ROUND(monthly_cost, 6) AS DECIMAL(38,6)) AS estimated_monthly_cost,
    CAST(ROUND(monthly_savings, 6) AS DECIMAL(38,6)) AS estimated_monthly_savings,
    CAST(confidence AS STRING) AS confidence,
    CAST(score AS DOUBLE) AS priority_score,
    CAST(ROW_NUMBER() OVER (
        ORDER BY score DESC, monthly_savings DESC, monthly_cost DESC, resource_type, workspace_id, resource_id
    ) AS INT) AS priority_rank,
    sources,
    CAST(primary_source AS STRING) AS primary_source,
    flag_type_count,
    CAST({detection_days} AS INT) AS detection_days,
    CAST({currency_code_sql} AS STRING) AS currency_code,
    current_timestamp() AS created_at,
    CAST({run_id_sql} AS STRING) AS run_id
FROM prioritized
