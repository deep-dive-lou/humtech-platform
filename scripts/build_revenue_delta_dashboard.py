"""Build Revenue Delta dashboard in Metabase.

THE billing dashboard — shows baseline vs current performance and HumTech fee.

£ revenue cards show CRM estimates until financial software (Xero/QuickBooks) is connected.
CRM leading indicators (win rate, velocity) work immediately.

Usage:
    METABASE_API_KEY=mb_... METABASE_COLLECTION_ID=<id> python scripts/build_revenue_delta_dashboard.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from _metabase import (
    TENANT_ID, require_key, create_question, create_dashboard,
    wire_cards, dashboard_exists, make_date_tags,
)

COLLECTION_ID = int(os.getenv("METABASE_COLLECTION_ID", "5"))
DASHBOARD_NAME = "RESG: Revenue Delta"
T = TENANT_ID

# The active baseline label — adjust per client
BASELINE_LABEL = os.getenv("BASELINE_LABEL", "resg_crm_pre_humtech")

QUESTIONS = [
    # Row 1 — THE numbers
    {
        "name": "RESG Delta: Revenue Change (£)",
        "display": "scalar",
        "sql": f"""
WITH baseline AS (
    SELECT
        COALESCE((metrics->>'revenue_monthly_avg')::numeric,
                 (metrics->>'avg_deal_value_gbp')::numeric * (metrics->>'lead_volume_per_month')::numeric * (metrics->>'win_rate')::numeric,
                 0) AS monthly_rev
    FROM engine.baselines
    WHERE tenant_id = '{T}'::uuid
      AND label = '{BASELINE_LABEL}'
      AND is_active = TRUE
),
current_rev AS (
    SELECT
        CASE
            WHEN count(*) = 0 THEN 0
            ELSE round(
                count(*) FILTER (WHERE current_stage = 'won')::numeric
                / GREATEST(1, EXTRACT(EPOCH FROM (max(created_at) - min(created_at))) / 86400 / 30)
                * COALESCE(avg(lead_value) FILTER (WHERE lead_value > 0), 0), 0
            )
        END AS monthly_rev
    FROM engine.leads
    WHERE tenant_id = '{T}'::uuid
    [[AND created_at >= {{{{start_date}}}}::timestamp]]
    [[AND created_at <= {{{{end_date}}}}::timestamp]]
)
SELECT
    COALESCE(c.monthly_rev - b.monthly_rev, 0) AS "Monthly Revenue Change"
FROM baseline b, current_rev c
""",
        "viz": {"scalar.field": "Monthly Revenue Change", "scalar.prefix": "£"},
        "date_filter": True,
    },
    {
        "name": "RESG Delta: HumTech Fee This Quarter (£)",
        "display": "scalar",
        "sql": f"""
-- Fee = revenue_delta * fee_percentage
-- Fee tiers: M1-3: 25%, M4-6: 25%, M7-9: 20%, M10-12: 15%, M12+: 10%
-- Engagement start tracked by earliest baseline
WITH engagement AS (
    SELECT period_end AS start_date
    FROM engine.baselines
    WHERE tenant_id = '{T}'::uuid
      AND label = '{BASELINE_LABEL}'
      AND is_active = TRUE
),
fee_pct AS (
    SELECT CASE
        WHEN EXTRACT(EPOCH FROM (now() - e.start_date)) / 86400 <= 90 THEN 0.25
        WHEN EXTRACT(EPOCH FROM (now() - e.start_date)) / 86400 <= 180 THEN 0.25
        WHEN EXTRACT(EPOCH FROM (now() - e.start_date)) / 86400 <= 270 THEN 0.20
        WHEN EXTRACT(EPOCH FROM (now() - e.start_date)) / 86400 <= 365 THEN 0.15
        ELSE 0.10
    END AS pct
    FROM engagement e
),
baseline AS (
    SELECT
        COALESCE((metrics->>'revenue_monthly_avg')::numeric,
                 (metrics->>'avg_deal_value_gbp')::numeric * (metrics->>'lead_volume_per_month')::numeric * (metrics->>'win_rate')::numeric,
                 0) AS monthly_rev
    FROM engine.baselines
    WHERE tenant_id = '{T}'::uuid
      AND label = '{BASELINE_LABEL}'
      AND is_active = TRUE
),
current_rev AS (
    SELECT
        CASE
            WHEN count(*) = 0 THEN 0
            ELSE round(
                count(*) FILTER (WHERE current_stage = 'won')::numeric
                / GREATEST(1, EXTRACT(EPOCH FROM (max(created_at) - min(created_at))) / 86400 / 30)
                * COALESCE(avg(lead_value) FILTER (WHERE lead_value > 0), 0), 0
            )
        END AS monthly_rev
    FROM engine.leads
    WHERE tenant_id = '{T}'::uuid
    [[AND created_at >= {{{{start_date}}}}::timestamp]]
    [[AND created_at <= {{{{end_date}}}}::timestamp]]
)
SELECT round(GREATEST(0, (c.monthly_rev - b.monthly_rev) * 3 * f.pct), 0) AS "Quarterly Fee"
FROM baseline b, current_rev c, fee_pct f
""",
        "viz": {"scalar.field": "Quarterly Fee", "scalar.prefix": "£"},
        "date_filter": True,
    },
    {
        "name": "RESG Delta: Current Fee Tier",
        "display": "scalar",
        "sql": f"""
WITH engagement AS (
    SELECT period_end AS start_date
    FROM engine.baselines
    WHERE tenant_id = '{T}'::uuid
      AND label = '{BASELINE_LABEL}'
      AND is_active = TRUE
)
SELECT CASE
    WHEN EXTRACT(EPOCH FROM (now() - e.start_date)) / 86400 <= 90
        THEN '25% (Months 1-3)'
    WHEN EXTRACT(EPOCH FROM (now() - e.start_date)) / 86400 <= 180
        THEN '25% (Months 4-6)'
    WHEN EXTRACT(EPOCH FROM (now() - e.start_date)) / 86400 <= 270
        THEN '20% (Months 7-9)'
    WHEN EXTRACT(EPOCH FROM (now() - e.start_date)) / 86400 <= 365
        THEN '15% (Months 10-12)'
    ELSE '10% (Month 12+)'
END AS "Fee Tier"
FROM engagement e
""",
        "viz": {},
        "date_filter": False,
    },
    # Row 2 — CRM leading indicators
    {
        "name": "RESG Delta: Win Rate Change",
        "display": "scalar",
        "sql": f"""
WITH baseline AS (
    SELECT (metrics->>'win_rate')::numeric * 100 AS val
    FROM engine.baselines
    WHERE tenant_id = '{T}'::uuid
      AND label = '{BASELINE_LABEL}'
      AND is_active = TRUE
),
current_val AS (
    SELECT round(
        count(*) FILTER (WHERE current_stage = 'won')::numeric
        / NULLIF(count(*), 0) * 100, 1
    ) AS val
    FROM engine.leads
    WHERE tenant_id = '{T}'::uuid
    [[AND created_at >= {{{{start_date}}}}::timestamp]]
    [[AND created_at <= {{{{end_date}}}}::timestamp]]
)
SELECT
    COALESCE(round(c.val - b.val, 1), 0) AS "Win Rate Change (pp)"
FROM baseline b, current_val c
""",
        "viz": {"scalar.field": "Win Rate Change (pp)", "scalar.suffix": "pp"},
        "date_filter": True,
    },
    {
        "name": "RESG Delta: Rejection Rate Change",
        "display": "scalar",
        "sql": f"""
WITH baseline AS (
    SELECT (metrics->>'rejection_rate')::numeric * 100 AS val
    FROM engine.baselines
    WHERE tenant_id = '{T}'::uuid
      AND label = '{BASELINE_LABEL}'
      AND is_active = TRUE
),
current_val AS (
    SELECT round(
        count(*) FILTER (WHERE current_stage = 'rejected')::numeric
        / NULLIF(count(*), 0) * 100, 1
    ) AS val
    FROM engine.leads
    WHERE tenant_id = '{T}'::uuid
    [[AND created_at >= {{{{start_date}}}}::timestamp]]
    [[AND created_at <= {{{{end_date}}}}::timestamp]]
)
SELECT
    COALESCE(round(b.val - c.val, 1), 0) AS "Rejection Rate Reduction (pp)"
FROM baseline b, current_val c
""",
        "viz": {"scalar.field": "Rejection Rate Reduction (pp)", "scalar.suffix": "pp"},
        "date_filter": True,
    },
    # Row 3 — comparison table
    {
        "name": "RESG Delta: Baseline vs Current",
        "display": "table",
        "sql": f"""
WITH baseline AS (
    SELECT metrics FROM engine.baselines
    WHERE tenant_id = '{T}'::uuid
      AND label = '{BASELINE_LABEL}'
      AND is_active = TRUE
),
current_stats AS (
    SELECT
        count(*) AS total_leads,
        count(*) FILTER (WHERE current_stage = 'won') AS total_won,
        count(*) FILTER (WHERE current_stage = 'rejected') AS total_rejected,
        round(count(*) FILTER (WHERE current_stage = 'won')::numeric / NULLIF(count(*), 0) * 100, 1) AS win_rate_pct,
        round(count(*) FILTER (WHERE current_stage = 'rejected')::numeric / NULLIF(count(*), 0) * 100, 1) AS rejection_rate_pct,
        round(avg(lead_value)::numeric, 2) AS avg_deal_value
    FROM engine.leads
    WHERE tenant_id = '{T}'::uuid
    [[AND created_at >= {{{{start_date}}}}::timestamp]]
    [[AND created_at <= {{{{end_date}}}}::timestamp]]
)
SELECT unnest(ARRAY['Total Leads', 'Won', 'Win Rate %', 'Rejected', 'Rejection Rate %', 'Avg Deal Value (£)']) AS "Metric",
       unnest(ARRAY[
           (b.metrics->>'total_leads'),
           (b.metrics->>'total_won'),
           round((b.metrics->>'win_rate')::numeric * 100, 1)::text,
           (b.metrics->>'total_rejected'),
           round((b.metrics->>'rejection_rate')::numeric * 100, 1)::text,
           COALESCE(b.metrics->>'avg_deal_value_gbp', 'n/a')
       ]) AS "Baseline",
       unnest(ARRAY[
           c.total_leads::text,
           c.total_won::text,
           c.win_rate_pct::text,
           c.total_rejected::text,
           c.rejection_rate_pct::text,
           COALESCE(c.avg_deal_value::text, 'n/a')
       ]) AS "Current"
FROM baseline b, current_stats c
""",
        "viz": {},
        "date_filter": True,
    },
    # Row 4 — rolling baseline history
    {
        "name": "RESG Delta: Baseline History",
        "display": "table",
        "sql": f"""
SELECT label AS "Baseline",
       period_start::date AS "From",
       period_end::date AS "To",
       (metrics->>'total_leads')::int AS "Leads",
       round((metrics->>'win_rate')::numeric * 100, 1) AS "Win Rate %",
       round((metrics->>'rejection_rate')::numeric * 100, 1) AS "Rejection %",
       COALESCE(metrics->>'revenue_monthly_avg', metrics->>'avg_deal_value_gbp', 'n/a') AS "Revenue / Deal Value",
       COALESCE(metrics->>'source', 'CRM') AS "Data Source",
       CASE WHEN is_active THEN 'Active' ELSE 'Superseded' END AS "Status"
FROM engine.baselines
WHERE tenant_id = '{T}'::uuid
ORDER BY period_end DESC
""",
        "viz": {},
        "date_filter": False,
    },
]

LAYOUTS = [
    # Row 1 — THE numbers
    {"col": 0, "row": 0, "size_x": 6, "size_y": 4},   # Revenue change
    {"col": 6, "row": 0, "size_x": 6, "size_y": 4},   # HumTech fee
    {"col": 12, "row": 0, "size_x": 6, "size_y": 4},  # Fee tier
    # Row 2 — CRM indicators
    {"col": 0, "row": 4, "size_x": 9, "size_y": 3},   # Win rate change
    {"col": 9, "row": 4, "size_x": 9, "size_y": 3},   # Rejection rate change
    # Row 3 — comparison table
    {"col": 0, "row": 7, "size_x": 18, "size_y": 7},  # Baseline vs current
    # Row 4 — baseline history
    {"col": 0, "row": 14, "size_x": 18, "size_y": 6},  # Rolling baseline history
]


def main():
    require_key()

    existing = dashboard_exists(DASHBOARD_NAME, COLLECTION_ID)
    if existing:
        print(f"Dashboard '{DASHBOARD_NAME}' already exists (id={existing}). Delete it first to rebuild.")
        sys.exit(1)

    print(f"Creating {len(QUESTIONS)} questions...")
    card_ids = []
    date_filter_ids = set()
    for q in QUESTIONS:
        tags = make_date_tags() if q["date_filter"] else None
        cid = create_question(q["name"], q["sql"], q["display"], q.get("viz"), COLLECTION_ID, tags)
        card_ids.append(cid)
        if q["date_filter"]:
            date_filter_ids.add(cid)

    dash_id = create_dashboard(
        DASHBOARD_NAME, COLLECTION_ID,
        "Revenue impact — baseline vs current performance. HumTech billing dashboard."
    )

    wire_cards(dash_id, card_ids, LAYOUTS, date_filter_ids)

    from _metabase import MB_URL
    print(f"\n  Dashboard URL: {MB_URL}/dashboard/{dash_id}")
    print("  Done!")


if __name__ == "__main__":
    main()
