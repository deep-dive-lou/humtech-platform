"""Build CRM Pipeline dashboard in Metabase.

Shows live pipeline movement — distinct from the static CRM Baseline.

Usage:
    METABASE_API_KEY=mb_... METABASE_COLLECTION_ID=<id> python scripts/build_crm_movement_dashboard.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from _metabase import (
    TENANT_ID, require_key, create_question, create_dashboard,
    wire_cards, dashboard_exists, make_date_tags,
)

COLLECTION_ID = int(os.getenv("METABASE_COLLECTION_ID", "5"))
DASHBOARD_NAME = "RESG: CRM Pipeline"
T = TENANT_ID

QUESTIONS = [
    # Row 1 — pipeline snapshot + movements
    {
        "name": "RESG CRM: Current Pipeline Snapshot",
        "display": "bar",
        "sql": f"""
WITH stage_order(stage, pos) AS (
    VALUES
        ('lead_created', 1), ('no_comms', 2), ('processing', 3),
        ('lead_qualified', 4), ('won', 5), ('rejected', 6), ('lost', 7)
)
SELECT so.stage AS "Stage",
       count(*) AS "Leads"
FROM engine.leads l
JOIN stage_order so ON so.stage = l.current_stage
WHERE l.tenant_id = '{T}'::uuid
  AND l.is_open = TRUE
GROUP BY so.stage, so.pos
ORDER BY so.pos
""",
        "viz": {"graph.dimensions": ["Stage"], "graph.metrics": ["Leads"]},
        "date_filter": False,
    },
    {
        "name": "RESG CRM: Stage Movements Over Time",
        "display": "line",
        "sql": f"""
SELECT date_trunc('month', occurred_at)::date AS "Month",
       to_stage AS "To Stage",
       count(*) AS "Movements"
FROM engine.lead_events
WHERE tenant_id = '{T}'::uuid
  AND event_type = 'stage_changed'
  AND to_stage IS NOT NULL
[[AND occurred_at >= {{{{start_date}}}}::timestamp]]
[[AND occurred_at <= {{{{end_date}}}}::timestamp]]
GROUP BY 1, 2
ORDER BY 1
""",
        "viz": {"graph.dimensions": ["Month"], "graph.metrics": ["Movements"]},
        "date_filter": True,
    },
    # Row 2 — conversion rates + avg days
    {
        "name": "RESG CRM: Live Conversion Rates",
        "display": "bar",
        "sql": f"""
WITH stage_order(stage, pos) AS (
    VALUES
        ('lead_created', 1), ('no_comms', 2), ('processing', 3),
        ('lead_qualified', 4)
),
funnel_counts AS (
    SELECT so.stage, so.pos,
           count(*) FILTER (
               WHERE l.current_stage NOT IN ('lost', 'rejected')
                 AND COALESCE(so2.pos, 0) >= so.pos
           ) AS cnt
    FROM stage_order so
    CROSS JOIN engine.leads l
    LEFT JOIN stage_order so2 ON so2.stage = l.current_stage
    WHERE l.tenant_id = '{T}'::uuid
    [[AND l.created_at >= {{{{start_date}}}}::timestamp]]
    [[AND l.created_at <= {{{{end_date}}}}::timestamp]]
    GROUP BY so.stage, so.pos
)
SELECT
    a.stage || ' -> ' || b.stage AS "Transition",
    round(b.cnt::numeric / NULLIF(a.cnt, 0) * 100, 1) AS "Conversion %"
FROM funnel_counts a
JOIN funnel_counts b ON b.pos = a.pos + 1
ORDER BY a.pos
""",
        "viz": {
            "graph.dimensions": ["Transition"],
            "graph.metrics": ["Conversion %"],
            "graph.y_axis.auto_range": False,
            "graph.y_axis.min": 0,
            "graph.y_axis.max": 100,
        },
        "date_filter": True,
    },
    {
        "name": "RESG CRM: Avg Days in Each Stage",
        "display": "bar",
        "sql": f"""
WITH transitions AS (
    SELECT lead_id, from_stage, to_stage, occurred_at,
           LAG(occurred_at) OVER (PARTITION BY lead_id ORDER BY occurred_at) AS prev_at
    FROM engine.lead_events
    WHERE tenant_id = '{T}'::uuid
      AND event_type = 'stage_changed'
    [[AND occurred_at >= {{{{start_date}}}}::timestamp]]
    [[AND occurred_at <= {{{{end_date}}}}::timestamp]]
),
stage_durations AS (
    SELECT from_stage AS stage,
           EXTRACT(EPOCH FROM (occurred_at - prev_at)) / 86400.0 AS days_in_stage
    FROM transitions
    WHERE prev_at IS NOT NULL AND from_stage IS NOT NULL
),
stage_order(stage, pos) AS (
    VALUES
        ('lead_created', 1), ('no_comms', 2), ('processing', 3),
        ('lead_qualified', 4)
)
SELECT so.stage AS "Stage",
       round(avg(sd.days_in_stage)::numeric, 1) AS "Avg Days"
FROM stage_durations sd
JOIN stage_order so ON so.stage = sd.stage
GROUP BY so.stage, so.pos
ORDER BY so.pos
""",
        "viz": {"graph.dimensions": ["Stage"], "graph.metrics": ["Avg Days"]},
        "date_filter": True,
    },
    # Row 3 — leads by source + recent transitions
    {
        "name": "RESG CRM: Leads by Source",
        "display": "pie",
        "sql": f"""
SELECT COALESCE(source, 'Unknown') AS "Source",
       count(*) AS "Leads"
FROM engine.leads
WHERE tenant_id = '{T}'::uuid
[[AND created_at >= {{{{start_date}}}}::timestamp]]
[[AND created_at <= {{{{end_date}}}}::timestamp]]
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10
""",
        "viz": {"pie.dimension": ["Source"], "pie.metric": "Leads"},
        "date_filter": True,
    },
    {
        "name": "RESG CRM: Recent Stage Transitions",
        "display": "table",
        "sql": f"""
SELECT l.name AS "Lead",
       le.from_stage AS "From",
       le.to_stage AS "To",
       le.source AS "Source",
       le.occurred_at AS "When"
FROM engine.lead_events le
JOIN engine.leads l ON l.lead_id = le.lead_id
WHERE le.tenant_id = '{T}'::uuid
  AND le.event_type = 'stage_changed'
ORDER BY le.occurred_at DESC
LIMIT 50
""",
        "viz": {},
        "date_filter": False,
    },
]

LAYOUTS = [
    # Row 1
    {"col": 0, "row": 0, "size_x": 9, "size_y": 6},
    {"col": 9, "row": 0, "size_x": 9, "size_y": 6},
    # Row 2
    {"col": 0, "row": 6, "size_x": 9, "size_y": 6},
    {"col": 9, "row": 6, "size_x": 9, "size_y": 6},
    # Row 3
    {"col": 0, "row": 12, "size_x": 9, "size_y": 6},
    {"col": 9, "row": 12, "size_x": 9, "size_y": 6},
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

    dash_id = create_dashboard(DASHBOARD_NAME, COLLECTION_ID, "Live CRM pipeline movement and conversion")

    wire_cards(dash_id, card_ids, LAYOUTS, date_filter_ids)

    from _metabase import MB_URL
    print(f"\n  Dashboard URL: {MB_URL}/dashboard/{dash_id}")
    print("  Done!")


if __name__ == "__main__":
    main()
