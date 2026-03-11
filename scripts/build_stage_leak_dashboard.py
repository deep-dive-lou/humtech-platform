"""Build Stage Leak Analysis dashboard in Metabase.

Shows where leads fall out of the pipeline and estimated revenue cost.
Stage-weighted leak cost: exits × avg_deal_value × win_probability_from_that_stage
(Markov absorption probability approximation — see dashboard_methodology.md §5)

Usage:
    METABASE_API_KEY=mb_... METABASE_COLLECTION_ID=<id> python scripts/build_stage_leak_dashboard.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from _metabase import (
    TENANT_ID, TENANT_NAME, require_key, create_question, create_dashboard,
    wire_cards, dashboard_exists, make_date_tags,
)

COLLECTION_ID = int(os.getenv("METABASE_COLLECTION_ID", "5"))
DASHBOARD_NAME = f"{TENANT_NAME}: Stage Leak Analysis"
T = TENANT_ID

QUESTIONS = [
    # Row 1 — biggest leaks bar + leak cost scalar
    {
        "name": f"{TENANT_NAME} Leak: Biggest Stage Leaks",
        "display": "bar",
        "sql": f"""
SELECT le.canonical_stage AS "Exit Type",
       count(*) AS "Leads Lost"
FROM engine.lead_events le
WHERE le.tenant_id = '{T}'::uuid
  AND le.event_type = 'lead_lost'
[[AND le.occurred_at >= {{{{start_date}}}}::timestamp]]
[[AND le.occurred_at <= {{{{end_date}}}}::timestamp]]
GROUP BY le.canonical_stage
ORDER BY count(*) DESC
LIMIT 10
""",
        "viz": {"graph.dimensions": ["Exit Type"], "graph.metrics": ["Leads Lost"]},
        "date_filter": True,
    },
    {
        "name": f"{TENANT_NAME} Leak: Est. Monthly Leak Cost (Weighted)",
        "display": "scalar",
        "sql": f"""
WITH baseline AS (
    SELECT COALESCE((metrics->>'avg_deal_value_gbp')::numeric, 0) AS avg_val
    FROM engine.baselines
    WHERE tenant_id = '{T}'::uuid
      AND is_active = TRUE
    LIMIT 1
),
-- Win probability from each stage: what fraction of leads that reached
-- this stage eventually won? (at-or-beyond approximation)
stage_wins AS (
    SELECT sm.canonical_stage,
           count(DISTINCT le.lead_id) AS reached,
           count(DISTINCT le.lead_id) FILTER (
               WHERE l.current_stage = 'lead_won'
           ) AS won
    FROM engine.lead_events le
    JOIN engine.leads l ON l.lead_id = le.lead_id
    LEFT JOIN engine.stage_mappings sm
        ON sm.tenant_id = le.tenant_id
        AND sm.canonical_stage = le.canonical_stage
        AND sm.is_active = TRUE
    WHERE le.tenant_id = '{T}'::uuid
      AND le.canonical_stage IS NOT NULL
    GROUP BY sm.canonical_stage
),
win_prob AS (
    SELECT canonical_stage,
           CASE WHEN reached > 0 THEN won::numeric / reached ELSE 0 END AS win_probability
    FROM stage_wins
),
-- Exits per stage per month
exits AS (
    SELECT le.canonical_stage,
           count(*) AS exit_count,
           GREATEST(1, EXTRACT(EPOCH FROM (max(le.occurred_at) - min(le.occurred_at))) / 86400 / 30) AS months
    FROM engine.lead_events le
    WHERE le.tenant_id = '{T}'::uuid
      AND le.event_type = 'lead_lost'
    [[AND le.occurred_at >= {{{{start_date}}}}::timestamp]]
    [[AND le.occurred_at <= {{{{end_date}}}}::timestamp]]
    GROUP BY le.canonical_stage
)
SELECT round(sum(
    (e.exit_count::numeric / e.months)
    * b.avg_val
    * COALESCE(wp.win_probability, 0)
), 0) AS "Est. Monthly Leak"
FROM exits e
CROSS JOIN baseline b
LEFT JOIN win_prob wp ON wp.canonical_stage = e.canonical_stage
""",
        "viz": {"scalar.field": "Est. Monthly Leak", "scalar.prefix": "£"},
        "date_filter": True,
    },
    # Row 2 — leak detail table (stage-weighted)
    {
        "name": f"{TENANT_NAME} Leak: Detail by Stage (Weighted)",
        "display": "table",
        "sql": f"""
WITH baseline AS (
    SELECT COALESCE((metrics->>'avg_deal_value_gbp')::numeric, 0) AS avg_val
    FROM engine.baselines
    WHERE tenant_id = '{T}'::uuid
      AND is_active = TRUE
    LIMIT 1
),
stage_wins AS (
    SELECT le.canonical_stage,
           count(DISTINCT le.lead_id) AS reached,
           count(DISTINCT le.lead_id) FILTER (
               WHERE l.current_stage = 'lead_won'
           ) AS won
    FROM engine.lead_events le
    JOIN engine.leads l ON l.lead_id = le.lead_id
    WHERE le.tenant_id = '{T}'::uuid
      AND le.canonical_stage IS NOT NULL
    GROUP BY le.canonical_stage
),
win_prob AS (
    SELECT canonical_stage,
           CASE WHEN reached > 0 THEN won::numeric / reached ELSE 0 END AS win_probability
    FROM stage_wins
),
exits AS (
    SELECT le.canonical_stage,
           count(*) AS exit_count
    FROM engine.lead_events le
    WHERE le.tenant_id = '{T}'::uuid
      AND le.event_type = 'lead_lost'
    [[AND le.occurred_at >= {{{{start_date}}}}::timestamp]]
    [[AND le.occurred_at <= {{{{end_date}}}}::timestamp]]
    GROUP BY le.canonical_stage
),
period AS (
    SELECT GREATEST(1, EXTRACT(EPOCH FROM (max(occurred_at) - min(occurred_at))) / 86400 / 30) AS months
    FROM engine.lead_events
    WHERE tenant_id = '{T}'::uuid
      AND event_type = 'lead_lost'
    [[AND occurred_at >= {{{{start_date}}}}::timestamp]]
    [[AND occurred_at <= {{{{end_date}}}}::timestamp]]
)
SELECT e.canonical_stage AS "Exit Stage",
       e.exit_count AS "Total Exits",
       round(e.exit_count::numeric / p.months, 1) AS "Exits/Month",
       round(COALESCE(wp.win_probability, 0) * 100, 1) AS "Win Prob from Stage (%)",
       round(e.exit_count::numeric / p.months * b.avg_val, 0) AS "Unweighted Cost/Month (£)",
       round(e.exit_count::numeric / p.months * b.avg_val * COALESCE(wp.win_probability, 0), 0) AS "Weighted Cost/Month (£)"
FROM exits e
CROSS JOIN baseline b
CROSS JOIN period p
LEFT JOIN win_prob wp ON wp.canonical_stage = e.canonical_stage
ORDER BY round(e.exit_count::numeric / p.months * b.avg_val * COALESCE(wp.win_probability, 0), 0) DESC
""",
        "viz": {},
        "date_filter": True,
    },
    # Row 3 — leak trend over time
    {
        "name": f"{TENANT_NAME} Leak: Trend Over Time",
        "display": "line",
        "sql": f"""
SELECT date_trunc('month', occurred_at)::date AS "Month",
       canonical_stage AS "Exit Type",
       count(*) AS "Exits"
FROM engine.lead_events
WHERE tenant_id = '{T}'::uuid
  AND event_type = 'lead_lost'
[[AND occurred_at >= {{{{start_date}}}}::timestamp]]
[[AND occurred_at <= {{{{end_date}}}}::timestamp]]
GROUP BY 1, 2
ORDER BY 1
""",
        "viz": {"graph.dimensions": ["Month"], "graph.metrics": ["Exits"]},
        "date_filter": True,
    },
]

LAYOUTS = [
    # Row 1
    {"col": 0, "row": 0, "size_x": 12, "size_y": 6},
    {"col": 12, "row": 0, "size_x": 6, "size_y": 6},
    # Row 2
    {"col": 0, "row": 6, "size_x": 18, "size_y": 7},
    # Row 3
    {"col": 0, "row": 13, "size_x": 18, "size_y": 6},
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

    dash_id = create_dashboard(DASHBOARD_NAME, COLLECTION_ID, "Where leads exit the pipeline and estimated £ cost")

    wire_cards(dash_id, card_ids, LAYOUTS, date_filter_ids)

    from _metabase import MB_URL
    print(f"\n  Dashboard URL: {MB_URL}/dashboard/{dash_id}")
    print("  Done!")


if __name__ == "__main__":
    main()
