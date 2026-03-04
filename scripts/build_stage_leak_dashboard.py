"""Build Stage Leak Analysis dashboard in Metabase.

Shows where leads fall out of the pipeline and estimated revenue cost.

Usage:
    METABASE_API_KEY=mb_... METABASE_COLLECTION_ID=<id> python scripts/build_stage_leak_dashboard.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from _metabase import (
    TENANT_ID, require_key, create_question, create_dashboard,
    wire_cards, dashboard_exists, make_date_tags,
)

COLLECTION_ID = int(os.getenv("METABASE_COLLECTION_ID", "5"))
DASHBOARD_NAME = "RESG: Stage Leak Analysis"
T = TENANT_ID

QUESTIONS = [
    # Row 1 — biggest leaks bar + leak cost scalar
    {
        "name": "RESG Leak: Biggest Stage Leaks",
        "display": "bar",
        "sql": f"""
SELECT le.from_stage || ' -> ' || le.to_stage AS "Leak Point",
       count(*) AS "Leads Lost"
FROM engine.lead_events le
WHERE le.tenant_id = '{T}'::uuid
  AND le.event_type = 'stage_changed'
  AND le.to_stage IN ('lost', 'rejected')
[[AND le.occurred_at >= {{{{start_date}}}}::timestamp]]
[[AND le.occurred_at <= {{{{end_date}}}}::timestamp]]
GROUP BY le.from_stage, le.to_stage
ORDER BY count(*) DESC
LIMIT 10
""",
        "viz": {"graph.dimensions": ["Leak Point"], "graph.metrics": ["Leads Lost"]},
        "date_filter": True,
    },
    {
        "name": "RESG Leak: Est. Monthly Leak Cost",
        "display": "scalar",
        "sql": f"""
WITH baseline AS (
    SELECT COALESCE((metrics->>'avg_deal_value_gbp')::numeric, 0) AS avg_val
    FROM engine.baselines
    WHERE tenant_id = '{T}'::uuid
      AND is_active = TRUE
    LIMIT 1
),
exits_per_month AS (
    SELECT round(
        count(*)::numeric
        / GREATEST(1, EXTRACT(EPOCH FROM (max(occurred_at) - min(occurred_at))) / 86400 / 30), 1
    ) AS exits_pm
    FROM engine.lead_events
    WHERE tenant_id = '{T}'::uuid
      AND event_type = 'stage_changed'
      AND to_stage IN ('lost', 'rejected')
    [[AND occurred_at >= {{{{start_date}}}}::timestamp]]
    [[AND occurred_at <= {{{{end_date}}}}::timestamp]]
)
SELECT round(e.exits_pm * b.avg_val, 0) AS "Est. Monthly Leak"
FROM exits_per_month e, baseline b
""",
        "viz": {"scalar.field": "Est. Monthly Leak", "scalar.prefix": "£"},
        "date_filter": True,
    },
    # Row 2 — leak detail table
    {
        "name": "RESG Leak: Detail by Stage",
        "display": "table",
        "sql": f"""
WITH baseline AS (
    SELECT COALESCE((metrics->>'avg_deal_value_gbp')::numeric, 0) AS avg_val,
           GREATEST(1, (metrics->>'period_days')::numeric) AS period_days
    FROM engine.baselines
    WHERE tenant_id = '{T}'::uuid
      AND is_active = TRUE
    LIMIT 1
),
exits AS (
    SELECT le.from_stage,
           le.to_stage,
           count(*) AS exit_count
    FROM engine.lead_events le
    WHERE le.tenant_id = '{T}'::uuid
      AND le.event_type = 'stage_changed'
      AND le.to_stage IN ('lost', 'rejected')
    [[AND le.occurred_at >= {{{{start_date}}}}::timestamp]]
    [[AND le.occurred_at <= {{{{end_date}}}}::timestamp]]
    GROUP BY le.from_stage, le.to_stage
)
SELECT e.from_stage AS "From Stage",
       e.to_stage AS "Exit Type",
       e.exit_count AS "Total Exits",
       round(e.exit_count::numeric / (b.period_days / 30), 1) AS "Exits/Month",
       round(e.exit_count::numeric / (b.period_days / 30) * b.avg_val, 0) AS "Est. Cost/Month (GBP)"
FROM exits e, baseline b
ORDER BY e.exit_count DESC
""",
        "viz": {},
        "date_filter": True,
    },
    # Row 3 — leak trend over time
    {
        "name": "RESG Leak: Trend Over Time",
        "display": "line",
        "sql": f"""
SELECT date_trunc('month', occurred_at)::date AS "Month",
       to_stage AS "Exit Type",
       count(*) AS "Exits"
FROM engine.lead_events
WHERE tenant_id = '{T}'::uuid
  AND event_type = 'stage_changed'
  AND to_stage IN ('lost', 'rejected')
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
