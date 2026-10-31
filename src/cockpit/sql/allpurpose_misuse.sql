-- F4 all-purpose compute misuse: one row per all-purpose cluster with at least one misuse type.
-- Rule R1: a cluster can have several types, but estimated_monthly_savings is counted once per
-- cluster (the largest candidate saving); the other types are kept in misuse_types / evidence.
-- The four signal relations are built by cockpit/steps/allpurpose_misuse.py; a signal whose source
-- table is missing is replaced by an empty relation with the same columns.
CREATE OR REPLACE TABLE {target}
USING DELTA
AS
WITH
clusters AS (
    {clusters_relation}
),
prices AS (
    {prices_relation}
),
listed_all_purpose AS (
    SELECT workspace_id, cluster_id
    FROM clusters
    WHERE UPPER(cluster_source) IN ({all_purpose_sources})
),
-- Billing rows of all-purpose candidates only (listed as UI/API in compute.clusters, or billed with
-- an ALL_PURPOSE SKU), filtered before the price joins so job, warehouse and serverless usage is
-- never priced here. usage_rows is read by billed_clusters and row_attribution; Spark may scan it
-- twice, which this early filter keeps small.
cluster_usage AS (
    SELECT
        u.workspace_id,
        u.usage_metadata.cluster_id AS cluster_id,
        u.record_id,
        u.sku_name,
        u.cloud,
        u.usage_unit,
        u.usage_start_time,
        u.usage_end_time,
        u.usage_quantity
    FROM system.billing.usage u
    LEFT JOIN listed_all_purpose l
        ON u.workspace_id = l.workspace_id
        AND u.usage_metadata.cluster_id = l.cluster_id
    WHERE u.usage_date >= current_date() - INTERVAL {detection_days} DAYS
      AND u.usage_unit = 'DBU'
      AND u.usage_metadata.cluster_id IS NOT NULL
      AND (u.sku_name LIKE '%ALL_PURPOSE%' OR l.cluster_id IS NOT NULL)
),
-- Cost = DBUs x list price joined on SKU and the effective window of the price, with the configured
-- fallback DBU price for unmatched rows. KEEP IN STEP with upstream
-- engine template sql/daily_cost_list_prices.sql (price precedence, join keys; the date filter here is the detection window),
-- so cluster_cost matches cost_base; a test compares the two.
-- job_compute_cost prices the same DBUs (assumed unchanged) at the matching jobs SKU
-- (ALL_PURPOSE -> JOBS) for the SCHEDULED_JOB saving.
usage_rows AS (
    SELECT
        u.workspace_id,
        u.cluster_id,
        u.record_id,
        u.sku_name,
        u.usage_start_time,
        u.usage_end_time,
        CAST(u.usage_quantity AS DOUBLE) AS dbus,
        CAST(u.usage_quantity AS DOUBLE) * COALESCE(p.dbu_price, {fallback_dbu_price}) AS row_cost,
        CASE
            WHEN u.sku_name NOT LIKE '%ALL_PURPOSE%'
                THEN CAST(u.usage_quantity AS DOUBLE) * COALESCE(p.dbu_price, {fallback_dbu_price})
            WHEN jp.dbu_price IS NOT NULL
                THEN CAST(u.usage_quantity AS DOUBLE) * jp.dbu_price
            ELSE CAST(u.usage_quantity AS DOUBLE) * COALESCE(p.dbu_price, {fallback_dbu_price})
                 * {jobs_price_ratio_fallback}
        END AS job_compute_cost,
        CASE
            WHEN u.sku_name LIKE '%ALL_PURPOSE%' AND jp.dbu_price IS NULL THEN 'FALLBACK_RATIO'
            ELSE 'LIST_PRICES'
        END AS job_price_source
    FROM cluster_usage u
    LEFT JOIN prices p
        ON u.cloud = p.cloud
        AND u.sku_name = p.sku_name
        AND u.usage_unit = p.usage_unit
        AND p.currency_code = {currency_code_sql}
        AND u.usage_start_time >= p.price_start_time
        AND (u.usage_end_time <= p.price_end_time OR p.price_end_time IS NULL)
    LEFT JOIN prices jp
        ON u.cloud = jp.cloud
        AND REPLACE(u.sku_name, 'ALL_PURPOSE', 'JOBS') = jp.sku_name
        AND u.usage_unit = jp.usage_unit
        AND jp.currency_code = {currency_code_sql}
        AND u.usage_start_time >= jp.price_start_time
        AND (u.usage_end_time <= jp.price_end_time OR jp.price_end_time IS NULL)
),
billed_clusters AS (
    SELECT
        workspace_id,
        cluster_id,
        MAX(CASE WHEN sku_name LIKE '%ALL_PURPOSE%' THEN 1 ELSE 0 END) = 1 AS billed_as_all_purpose,
        SUM(row_cost) AS cluster_cost
    FROM usage_rows
    GROUP BY workspace_id, cluster_id
),
-- All-purpose = compute.clusters cluster_source UI/API. A cluster missing from compute.clusters
-- (table empty, e.g. Free Edition) counts when billing shows an ALL_PURPOSE SKU.
all_purpose AS (
    SELECT
        COALESCE(c.workspace_id, b.workspace_id) AS workspace_id,
        COALESCE(c.cluster_id, b.cluster_id) AS cluster_id,
        c.cluster_name,
        COALESCE(c.cluster_source, 'UNKNOWN') AS cluster_source,
        c.owned_by,
        COALESCE(b.cluster_cost, 0.0) AS cluster_cost
    FROM clusters c
    FULL OUTER JOIN billed_clusters b
        ON c.workspace_id = b.workspace_id
        AND c.cluster_id = b.cluster_id
    WHERE UPPER(c.cluster_source) IN ({all_purpose_sources})
       OR (c.cluster_id IS NULL AND b.billed_as_all_purpose)
),
tag_owner AS (
    SELECT workspace_id, cluster_id, MAX_BY(owner, estimated_cost) AS owner
    FROM {cost_base}
    WHERE cluster_id IS NOT NULL AND owner IS NOT NULL
    GROUP BY workspace_id, cluster_id
),
signals AS (
    SELECT s.workspace_id, s.cluster_id, s.misuse_type, s.activity_start, s.activity_end,
           s.actor, s.item, s.detail, s.source
    FROM (
        SELECT workspace_id, cluster_id, misuse_type, activity_start, activity_end, actor, item, detail, source
        FROM ({bi_audit_signal}) bi_audit
        UNION ALL
        SELECT workspace_id, cluster_id, misuse_type, activity_start, activity_end, actor, item, detail, source
        FROM ({bi_query_signal}) bi_query
        UNION ALL
        SELECT workspace_id, cluster_id, misuse_type, activity_start, activity_end, actor, item, detail, source
        FROM ({scheduled_signal}) scheduled
        UNION ALL
        SELECT workspace_id, cluster_id, misuse_type, activity_start, activity_end, actor, item, detail, source
        FROM ({shared_signal}) shared
    ) s
    JOIN all_purpose ap
        ON s.workspace_id = ap.workspace_id
        AND s.cluster_id = ap.cluster_id
),
type_stats AS (
    SELECT
        workspace_id,
        cluster_id,
        misuse_type,
        COUNT(*) AS event_count,
        COUNT(DISTINCT item) AS item_count,
        COUNT(DISTINCT actor) AS actor_count,
        COUNT(DISTINCT detail) AS detail_count,
        array_join(array_sort(collect_set(detail)), ', ') AS details,
        array_join(array_sort(collect_set(actor)), ', ') AS actors,
        array_join(array_sort(collect_set(source)), ', ') AS sources
    FROM signals
    GROUP BY workspace_id, cluster_id, misuse_type
),
flagged_types AS (
    SELECT *
    FROM type_stats
    WHERE (misuse_type = 'BI_TOOL_JDBC' AND event_count >= {min_bi_events})
       OR (misuse_type = 'SCHEDULED_JOB' AND item_count >= {min_scheduled_runs})
       OR (misuse_type = 'SHARED_USERS' AND actor_count >= {min_interactive_users})
),
-- Cost attribution for BI_TOOL_JDBC and SCHEDULED_JOB: merge the activity windows of each type into
-- disjoint islands (so overlapping runs or hours are not counted twice), then attribute each
-- billing row by the share of its duration that overlaps an island (capped at 100%).
-- SCHEDULED_JOB is net of interactive time: hours with any BI request or notebook attach on the
-- cluster (pseudo type INTERACTIVE, below the flag thresholds too) are subtracted from its islands,
-- because the cluster stays up for those users even after the job moves to job compute.
windows AS (
    SELECT DISTINCT s.workspace_id, s.cluster_id, s.misuse_type, s.activity_start, s.activity_end
    FROM signals s
    JOIN flagged_types f
        ON s.workspace_id = f.workspace_id
        AND s.cluster_id = f.cluster_id
        AND s.misuse_type = f.misuse_type
    WHERE s.misuse_type IN ('BI_TOOL_JDBC', 'SCHEDULED_JOB')
      AND s.activity_end > s.activity_start
    UNION
    SELECT DISTINCT s.workspace_id, s.cluster_id, 'INTERACTIVE' AS misuse_type, s.activity_start, s.activity_end
    FROM signals s
    JOIN flagged_types f
        ON s.workspace_id = f.workspace_id
        AND s.cluster_id = f.cluster_id
        AND f.misuse_type = 'SCHEDULED_JOB'
    WHERE s.misuse_type IN ('BI_TOOL_JDBC', 'SHARED_USERS')
      AND s.activity_end > s.activity_start
),
ordered_windows AS (
    SELECT
        *,
        MAX(activity_end) OVER (
            PARTITION BY workspace_id, cluster_id, misuse_type
            ORDER BY activity_start, activity_end
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS previous_max_end
    FROM windows
),
island_windows AS (
    SELECT
        *,
        SUM(CASE WHEN previous_max_end IS NULL OR activity_start > previous_max_end THEN 1 ELSE 0 END) OVER (
            PARTITION BY workspace_id, cluster_id, misuse_type
            ORDER BY activity_start, activity_end
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS island_id
    FROM ordered_windows
),
islands AS (
    SELECT workspace_id, cluster_id, misuse_type, MIN(activity_start) AS window_start, MAX(activity_end) AS window_end
    FROM island_windows
    GROUP BY workspace_id, cluster_id, misuse_type, island_id
),
-- Positive islands per type, and negative ones for SCHEDULED_JOB where it meets INTERACTIVE. Islands
-- are disjoint within a type, so the pairwise intersections are disjoint and the signed sum is exact.
signed_islands AS (
    SELECT workspace_id, cluster_id, misuse_type, window_start, window_end, 1.0 AS sign
    FROM islands
    WHERE misuse_type <> 'INTERACTIVE'
    UNION ALL
    SELECT
        s.workspace_id,
        s.cluster_id,
        s.misuse_type,
        GREATEST(s.window_start, i.window_start) AS window_start,
        LEAST(s.window_end, i.window_end) AS window_end,
        -1.0 AS sign
    FROM islands s
    JOIN islands i
        ON s.workspace_id = i.workspace_id
        AND s.cluster_id = i.cluster_id
        AND s.misuse_type = 'SCHEDULED_JOB'
        AND i.misuse_type = 'INTERACTIVE'
        AND i.window_start < s.window_end
        AND i.window_end > s.window_start
),
row_attribution AS (
    SELECT
        u.workspace_id,
        u.cluster_id,
        i.misuse_type,
        u.record_id,
        u.row_cost,
        u.job_compute_cost,
        u.job_price_source,
        LEAST(1.0, GREATEST(0.0, SUM(
            i.sign
            * (unix_seconds(LEAST(u.usage_end_time, i.window_end))
               - unix_seconds(GREATEST(u.usage_start_time, i.window_start)))
            / GREATEST(1, unix_seconds(u.usage_end_time) - unix_seconds(u.usage_start_time))
        ))) AS overlap_fraction
    FROM usage_rows u
    JOIN signed_islands i
        ON u.workspace_id = i.workspace_id
        AND u.cluster_id = i.cluster_id
        AND i.window_start < u.usage_end_time
        AND i.window_end > u.usage_start_time
    GROUP BY u.workspace_id, u.cluster_id, i.misuse_type, u.record_id, u.row_cost, u.job_compute_cost,
             u.job_price_source
),
type_cost AS (
    SELECT
        workspace_id,
        cluster_id,
        misuse_type,
        SUM(row_cost * overlap_fraction) AS attributable_cost,
        SUM(GREATEST(0.0, row_cost - job_compute_cost) * overlap_fraction) AS job_compute_saving,
        array_join(array_sort(collect_set(job_price_source)), ', ') AS job_price_sources
    FROM row_attribution
    GROUP BY workspace_id, cluster_id, misuse_type
),
per_type AS (
    SELECT
        f.workspace_id,
        f.cluster_id,
        f.misuse_type,
        f.event_count,
        f.item_count,
        f.actor_count,
        f.detail_count,
        CASE f.misuse_type
            WHEN 'SHARED_USERS' THEN ap.cluster_cost
            ELSE COALESCE(tc.attributable_cost, 0.0)
        END AS attributable_cost,
        -- Candidate saving over the detection window, before the R1 per-cluster deduplication.
        CASE f.misuse_type
            WHEN 'SCHEDULED_JOB' THEN COALESCE(tc.job_compute_saving, 0.0)
            WHEN 'BI_TOOL_JDBC' THEN COALESCE(tc.attributable_cost, 0.0) * {bi_to_sql_warehouse_savings_pct} / 100.0
            ELSE 0.0
        END AS candidate_saving,
        CASE f.misuse_type
            WHEN 'SCHEDULED_JOB' THEN 'HIGH'
            WHEN 'BI_TOOL_JDBC' THEN CASE WHEN f.sources LIKE '%query.history%' THEN 'MEDIUM' ELSE 'LOW' END
            ELSE 'MEDIUM'
        END AS type_confidence,
        CASE f.misuse_type WHEN 'SCHEDULED_JOB' THEN 1 WHEN 'BI_TOOL_JDBC' THEN 2 ELSE 3 END AS type_order,
        CASE f.misuse_type
            WHEN 'BI_TOOL_JDBC' THEN CONCAT(
                'BI_TOOL_JDBC: ', f.event_count, ' JDBC/ODBC requests from ', f.actor_count,
                ' principal(s) via ', f.details, ' (source: ', f.sources,
                CASE WHEN f.sources LIKE '%query.history%' THEN ')' ELSE '; clusterId in audit is an assumption)' END)
            WHEN 'SCHEDULED_JOB' THEN CONCAT(
                'SCHEDULED_JOB: ', f.item_count, ' automated run(s) of ', f.actor_count, ' job(s): ', f.details)
            ELSE CONCAT(
                'SHARED_USERS: ', f.actor_count, ' distinct users attached notebooks (', f.actors, ')')
        END AS type_evidence,
        CASE f.misuse_type
            WHEN 'SCHEDULED_JOB' THEN CONCAT(
                'SCHEDULED_JOB: usage overlapping automated runs (net of interactive hours) repriced at the ',
                'jobs SKU with DBUs assumed unchanged (', COALESCE(tc.job_price_sources, 'LIST_PRICES'), ')')
            WHEN 'BI_TOOL_JDBC' THEN CONCAT(
                'BI_TOOL_JDBC: ', {bi_to_sql_warehouse_savings_pct},
                '% of usage overlapping BI hours, assumed saving on a SQL warehouse')
            ELSE 'SHARED_USERS: no direct saving (attribution and cluster policy)'
        END AS type_basis
    FROM flagged_types f
    JOIN all_purpose ap
        ON f.workspace_id = ap.workspace_id
        AND f.cluster_id = ap.cluster_id
    LEFT JOIN type_cost tc
        ON f.workspace_id = tc.workspace_id
        AND f.cluster_id = tc.cluster_id
        AND f.misuse_type = tc.misuse_type
),
ranked_types AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY workspace_id, cluster_id
            ORDER BY candidate_saving DESC, type_order
        ) AS type_rank
    FROM per_type
),
per_cluster AS (
    SELECT
        workspace_id,
        cluster_id,
        MAX(CASE WHEN type_rank = 1 THEN misuse_type END) AS misuse_type,
        transform(array_sort(collect_list(named_struct('r', type_rank, 'v', misuse_type))), x -> x.v) AS misuse_types,
        CAST(COUNT(*) AS INT) AS misuse_type_count,
        concat_ws('; ', transform(array_sort(collect_list(named_struct('r', type_rank, 'v', type_evidence))), x -> x.v))
            AS evidence,
        MAX(CASE WHEN type_rank = 1 THEN type_basis END) AS primary_basis,
        concat_ws(', ', transform(
            array_sort(collect_list(CASE WHEN type_rank > 1 THEN named_struct('r', type_rank, 'v', CONCAT(
                misuse_type, ' ', CAST(ROUND(candidate_saving * 30.0 / {detection_days}, 2) AS STRING)))
            END)),
            x -> x.v)) AS other_candidates,
        MAX(CASE WHEN type_rank = 1 THEN candidate_saving END) AS kept_saving,
        MAX(CASE WHEN type_rank = 1 THEN type_confidence END) AS confidence,
        MAX(CASE WHEN misuse_type = 'BI_TOOL_JDBC' THEN detail_count END) AS bi_client_count,
        MAX(CASE WHEN misuse_type = 'BI_TOOL_JDBC' THEN event_count END) AS bi_event_count,
        MAX(CASE WHEN misuse_type = 'SCHEDULED_JOB' THEN actor_count END) AS scheduled_job_count,
        MAX(CASE WHEN misuse_type = 'SCHEDULED_JOB' THEN item_count END) AS scheduled_run_count,
        MAX(CASE WHEN misuse_type = 'SHARED_USERS' THEN actor_count END) AS interactive_user_count,
        MAX(CASE WHEN misuse_type = 'BI_TOOL_JDBC' THEN attributable_cost END) AS bi_tool_attributable_cost,
        MAX(CASE WHEN misuse_type = 'SCHEDULED_JOB' THEN attributable_cost END) AS scheduled_job_attributable_cost
    FROM ranked_types
    GROUP BY workspace_id, cluster_id
),
scored AS (
    SELECT
        pc.*,
        ap.cluster_name,
        ap.cluster_source,
        COALESCE(ap.owned_by, o.owner) AS owner,
        ap.cluster_cost,
        ap.cluster_cost * 30.0 / {detection_days} AS monthly_cost,
        -- R1: one saving per cluster (the largest candidate), never above the cost of the cluster.
        LEAST(ap.cluster_cost, pc.kept_saving) * 30.0 / {detection_days} AS monthly_saving
    FROM per_cluster pc
    JOIN all_purpose ap
        ON pc.workspace_id = ap.workspace_id
        AND pc.cluster_id = ap.cluster_id
    LEFT JOIN tag_owner o
        ON pc.workspace_id = o.workspace_id
        AND pc.cluster_id = o.cluster_id
)
SELECT
    CAST(workspace_id AS STRING) AS workspace_id,
    CAST(cluster_id AS STRING) AS cluster_id,
    CAST(COALESCE(cluster_name, cluster_id) AS STRING) AS cluster_name,
    CAST(cluster_source AS STRING) AS cluster_source,
    CAST(owner AS STRING) AS owner,
    CAST(misuse_type AS STRING) AS misuse_type,
    misuse_types,
    misuse_type_count,
    CAST(evidence AS STRING) AS evidence,
    CAST(COALESCE(bi_client_count, 0) AS BIGINT) AS bi_client_count,
    CAST(COALESCE(bi_event_count, 0) AS BIGINT) AS bi_event_count,
    CAST(COALESCE(scheduled_job_count, 0) AS BIGINT) AS scheduled_job_count,
    CAST(COALESCE(scheduled_run_count, 0) AS BIGINT) AS scheduled_run_count,
    CAST(COALESCE(interactive_user_count, 0) AS BIGINT) AS interactive_user_count,
    CAST({lookback_days} AS INT) AS lookback_days,
    CAST({detection_days} AS INT) AS detection_days,
    CAST(ROUND(cluster_cost, 6) AS DECIMAL(38,6)) AS cluster_cost,
    CAST(ROUND(COALESCE(bi_tool_attributable_cost, 0.0), 6) AS DECIMAL(38,6)) AS bi_tool_attributable_cost,
    CAST(ROUND(COALESCE(scheduled_job_attributable_cost, 0.0), 6) AS DECIMAL(38,6)) AS scheduled_job_attributable_cost,
    CAST(ROUND(monthly_cost, 6) AS DECIMAL(38,6)) AS estimated_monthly_cost,
    CAST(ROUND(monthly_saving, 6) AS DECIMAL(38,6)) AS estimated_monthly_savings,
    CAST(CONCAT(
        primary_basis, '; counted once per cluster (R1)',
        CASE WHEN other_candidates <> '' THEN CONCAT('; not added: ', other_candidates, ' per month') ELSE '' END
    ) AS STRING) AS savings_basis,
    CAST(CASE misuse_type
        WHEN 'SCHEDULED_JOB' THEN 'MOVE_TO_JOB_COMPUTE'
        WHEN 'BI_TOOL_JDBC' THEN 'MOVE_BI_TO_SQL_WAREHOUSE'
        ELSE 'APPLY_SHARED_CLUSTER_POLICY'
    END AS STRING) AS suggested_action,
    CAST(confidence AS STRING) AS confidence,
    CAST(ROUND(LEAST(100.0, GREATEST(0.0, (
        {priority_weight_savings} * LEAST(100.0, monthly_saving / {score_reference_monthly_cost} * 100.0)
        + {priority_weight_cost} * LEAST(100.0, monthly_cost / {score_reference_monthly_cost} * 100.0)
        + {priority_weight_breadth} * misuse_type_count / 3.0 * 100.0
    ) * CASE confidence
            WHEN 'HIGH' THEN {confidence_multiplier_high}
            WHEN 'MEDIUM' THEN {confidence_multiplier_medium}
            ELSE {confidence_multiplier_low}
        END)), 2) AS DOUBLE) AS priority_score,
    CAST({currency_code_sql} AS STRING) AS currency_code,
    current_timestamp() AS created_at,
    CAST({run_id_sql} AS STRING) AS run_id
FROM scored
