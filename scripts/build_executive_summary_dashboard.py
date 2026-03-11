"""Build Executive Summary hub dashboard in Metabase.

Top-level dashboard with 5 headline KPIs linking to detail dashboards.
Run this AFTER all spoke dashboards are created.

Usage:
    METABASE_API_KEY=mb_... METABASE_COLLECTION_ID=<id> python scripts/build_executive_summary_dashboard.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from _metabase import (
    TENANT_ID, TENANT_NAME, require_key, create_question, create_dashboard,
    wire_cards, dashboard_exists, make_date_tags, MB_URL,
)

COLLECTION_ID = int(os.getenv("METABASE_COLLECTION_ID", "5"))
DASHBOARD_NAME = f"{TENANT_NAME}: Executive Summary"
T = TENANT_ID

QUESTIONS = [
    # Row 1 — 4 headline scalars
    {
        "name": f"{TENANT_NAME}: Pipeline Value",
        "display": "scalar",
        "sql": f"""
SELECT COALESCE(round(sum(lead_value)::numeric, 0), 0) AS "Pipeline Value"
FROM engine.leads
WHERE tenant_id = '{T}'::uuid
  AND is_open = TRUE
[[AND created_at >= {{{{start_date}}}}::timestamp]]
[[AND created_at <= {{{{end_date}}}}::timestamp]]
""",
        "viz": {"scalar.field": "Pipeline Value", "scalar.prefix": "£"},
        "date_filter": True,
    },
    {
        "name": f"{TENANT_NAME}: Win Rate",
        "display": "scalar",
        "sql": f"""
SELECT round(
    count(*) FILTER (WHERE current_stage = 'lead_won')::numeric
    / NULLIF(count(*), 0) * 100, 1
) AS "Win Rate %"
FROM engine.leads
WHERE tenant_id = '{T}'::uuid
[[AND created_at >= {{{{start_date}}}}::timestamp]]
[[AND created_at <= {{{{end_date}}}}::timestamp]]
""",
        "viz": {"scalar.field": "Win Rate %", "scalar.suffix": "%"},
        "date_filter": True,
    },
    {
        "name": f"{TENANT_NAME}: Pipeline Velocity (£/day)",
        "display": "scalar",
        "sql": f"""
WITH open_pipeline AS (
    SELECT count(*) AS opps,
           COALESCE(avg(lead_value), 0) AS avg_val
    FROM engine.leads
    WHERE tenant_id = '{T}'::uuid
      AND is_open = TRUE
    [[AND created_at >= {{{{start_date}}}}::timestamp]]
    [[AND created_at <= {{{{end_date}}}}::timestamp]]
),
win_rate AS (
    SELECT CASE WHEN count(*) > 0
           THEN count(*) FILTER (WHERE current_stage = 'lead_won')::numeric / count(*)
           ELSE 0 END AS wr
    FROM engine.leads
    WHERE tenant_id = '{T}'::uuid
    [[AND created_at >= {{{{start_date}}}}::timestamp]]
    [[AND created_at <= {{{{end_date}}}}::timestamp]]
),
cycle_length AS (
    SELECT COALESCE(
        percentile_cont(0.5) WITHIN GROUP (
            ORDER BY EXTRACT(EPOCH FROM (l.won_at - l.created_at)) / 86400
        ), 1
    ) AS median_days
    FROM engine.leads l
    WHERE l.tenant_id = '{T}'::uuid
      AND l.current_stage = 'lead_won'
      AND l.won_at IS NOT NULL
    [[AND l.created_at >= {{{{start_date}}}}::timestamp]]
    [[AND l.created_at <= {{{{end_date}}}}::timestamp]]
)
SELECT round(
    (op.opps * op.avg_val * wr.wr) / GREATEST(cl.median_days, 1),
    0
) AS "Pipeline Velocity"
FROM open_pipeline op, win_rate wr, cycle_length cl
""",
        "viz": {"scalar.field": "Pipeline Velocity", "scalar.prefix": "£", "scalar.suffix": "/day"},
        "date_filter": True,
    },
    {
        "name": f"{TENANT_NAME}: Revenue per Lead",
        "display": "scalar",
        "sql": f"""
SELECT round(
    COALESCE(
        sum(lead_value) FILTER (WHERE current_stage = 'lead_won')
        / NULLIF(count(*), 0),
        0
    )::numeric, 0
) AS "Revenue per Lead"
FROM engine.leads
WHERE tenant_id = '{T}'::uuid
[[AND created_at >= {{{{start_date}}}}::timestamp]]
[[AND created_at <= {{{{end_date}}}}::timestamp]]
""",
        "viz": {"scalar.field": "Revenue per Lead", "scalar.prefix": "£"},
        "date_filter": True,
    },
    # Row 2 — lead volume trend
    {
        "name": f"{TENANT_NAME}: Lead Volume Trend",
        "display": "line",
        "sql": f"""
SELECT date_trunc('month', created_at)::date AS "Month",
       count(*) AS "Leads"
FROM engine.leads
WHERE tenant_id = '{T}'::uuid
[[AND created_at >= {{{{start_date}}}}::timestamp]]
[[AND created_at <= {{{{end_date}}}}::timestamp]]
GROUP BY 1
ORDER BY 1
""",
        "viz": {"graph.dimensions": ["Month"], "graph.metrics": ["Leads"]},
        "date_filter": True,
    },
]

LAYOUTS = [
    # Row 1 — headline scalars (4 across)
    {"col": 0, "row": 0, "size_x": 4, "size_y": 4},
    {"col": 4, "row": 0, "size_x": 5, "size_y": 4},
    {"col": 9, "row": 0, "size_x": 5, "size_y": 4},
    {"col": 14, "row": 0, "size_x": 4, "size_y": 4},
    # Row 2 — trend line
    {"col": 0, "row": 4, "size_x": 18, "size_y": 6},
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
        "Executive summary — headline KPIs across the revenue engine"
    )

    wire_cards(dash_id, card_ids, LAYOUTS, date_filter_ids)

    print(f"\n  Dashboard URL: {MB_URL}/dashboard/{dash_id}")
    print("  Done!")


if __name__ == "__main__":
    main()
