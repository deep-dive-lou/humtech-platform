"""Build CRM Pipeline dashboard in Metabase.

Shows live pipeline movement — distinct from the static CRM Baseline.

Usage:
    METABASE_API_KEY=mb_... METABASE_COLLECTION_ID=<id> python scripts/build_crm_movement_dashboard.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from _metabase import (
    TENANT_ID, TENANT_NAME, require_key, create_question, create_dashboard,
    wire_cards, dashboard_exists, make_date_tags,
)

COLLECTION_ID = int(os.getenv("METABASE_COLLECTION_ID", "5"))
DASHBOARD_NAME = f"{TENANT_NAME}: CRM Pipeline"
T = TENANT_ID

QUESTIONS = [
    # Row 0 — scalar KPIs
    {
        "name": f"{TENANT_NAME} CRM: Sales Velocity (GBP/month)",
        "display": "scalar",
        "sql": f"""
WITH metrics AS (
    SELECT
        count(*) AS total_leads,
        COALESCE(avg(lead_value) FILTER (WHERE lead_value > 0), 0) AS avg_deal,
        count(*) FILTER (WHERE current_stage = 'won')::numeric / NULLIF(count(*), 0) AS win_rate
    FROM engine.leads
    WHERE tenant_id = '{T}'::uuid
    [[AND created_at >= {{{{start_date}}}}::timestamp]]
    [[AND created_at <= {{{{end_date}}}}::timestamp]]
),
cycle AS (
    SELECT COALESCE(
        avg(EXTRACT(EPOCH FROM (l.won_at - l.created_at)) / 86400),
        1
    ) AS avg_cycle_days
    FROM engine.leads l
    WHERE l.tenant_id = '{T}'::uuid
      AND l.current_stage = 'won'
      AND l.won_at IS NOT NULL
    [[AND l.created_at >= {{{{start_date}}}}::timestamp]]
    [[AND l.created_at <= {{{{end_date}}}}::timestamp]]
)
SELECT round(
    (m.total_leads * m.avg_deal * m.win_rate) / GREATEST(c.avg_cycle_days, 1) * 30,
    0
) AS "Sales Velocity"
FROM metrics m, cycle c
""",
        "viz": {"scalar.field": "Sales Velocity", "scalar.prefix": "£"},
        "date_filter": True,
    },
    {
        "name": f"{TENANT_NAME} CRM: Median Sales Cycle (Days)",
        "display": "scalar",
        "sql": f"""
SELECT round(
    percentile_cont(0.5) WITHIN GROUP (
        ORDER BY EXTRACT(EPOCH FROM (won_at - created_at)) / 86400
    )::numeric, 1
) AS "Median Days"
FROM engine.leads
WHERE tenant_id = '{T}'::uuid
  AND current_stage = 'won'
  AND won_at IS NOT NULL
[[AND created_at >= {{{{start_date}}}}::timestamp]]
[[AND created_at <= {{{{end_date}}}}::timestamp]]
""",
        "viz": {"scalar.field": "Median Days"},
        "date_filter": True,
    },
    # Row 1 — pipeline snapshot + movements
    {
        "name": f"{TENANT_NAME} CRM: Current Pipeline Snapshot",
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
[[AND l.created_at >= {{{{start_date}}}}::timestamp]]
[[AND l.created_at <= {{{{end_date}}}}::timestamp]]
GROUP BY so.stage, so.pos
ORDER BY so.pos
""",
        "viz": {"graph.dimensions": ["Stage"], "graph.metrics": ["Leads"]},
        "date_filter": True,
    },
    {
        "name": f"{TENANT_NAME} CRM: Stage Movements Over Time",
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
        "name": f"{TENANT_NAME} CRM: Live Conversion Rates",
        "display": "bar",
        "sql": f"""
WITH stage_order(stage, pos) AS (
    VALUES
        ('lead_created', 1), ('no_comms', 2), ('processing', 3),
        ('lead_qualified', 4), ('won', 5)
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
        "name": f"{TENANT_NAME} CRM: Avg Days in Each Stage",
        "display": "bar",
        "sql": f"""
WITH transitions AS (
    SELECT lead_id, from_stage, to_stage, occurred_at,
           LAG(occurred_at) OVER (PARTITION BY lead_id ORDER BY occurred_at) AS prev_at
    FROM (
        SELECT l.lead_id, NULL AS from_stage, 'lead_created' AS to_stage,
               l.created_at AS occurred_at
        FROM engine.leads l
        WHERE l.tenant_id = '{T}'::uuid
        UNION ALL
        SELECT le.lead_id, le.from_stage, le.to_stage, le.occurred_at
        FROM engine.lead_events le
        WHERE le.tenant_id = '{T}'::uuid
          AND le.event_type = 'stage_changed'
        [[AND le.occurred_at >= {{{{start_date}}}}::timestamp]]
        [[AND le.occurred_at <= {{{{end_date}}}}::timestamp]]
    ) combined
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
        "name": f"{TENANT_NAME} CRM: Leads by Source",
        "display": "bar",
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
        "viz": {"graph.dimensions": ["Source"], "graph.metrics": ["Leads"]},
        "date_filter": True,
    },
    {
        "name": f"{TENANT_NAME} CRM: Recent Stage Transitions",
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
[[AND le.occurred_at >= {{{{start_date}}}}::timestamp]]
[[AND le.occurred_at <= {{{{end_date}}}}::timestamp]]
ORDER BY le.occurred_at DESC
LIMIT 50
""",
        "viz": {},
        "date_filter": True,
    },
    # Row 4 — bottleneck analysis
    {
        "name": f"{TENANT_NAME} CRM: Bottleneck Analysis",
        "display": "table",
        "sql": f"""
WITH stage_order(stage, pos) AS (
    VALUES
        ('lead_created', 1), ('no_comms', 2), ('processing', 3),
        ('lead_qualified', 4), ('won', 5)
),
wip AS (
    SELECT current_stage AS stage, count(*) AS wip_count
    FROM engine.leads
    WHERE tenant_id = '{T}'::uuid AND is_open = TRUE
    GROUP BY current_stage
),
throughput AS (
    SELECT from_stage AS stage,
           count(*) AS total_exits,
           round(count(*)::numeric / GREATEST(1, EXTRACT(EPOCH FROM (max(occurred_at) - min(occurred_at))) / 86400 / 7), 1) AS exits_per_week
    FROM engine.lead_events
    WHERE tenant_id = '{T}'::uuid
      AND event_type = 'stage_changed'
      AND from_stage IS NOT NULL
    [[AND occurred_at >= {{{{start_date}}}}::timestamp]]
    [[AND occurred_at <= {{{{end_date}}}}::timestamp]]
    GROUP BY from_stage
),
dwell AS (
    SELECT from_stage AS stage,
           round(percentile_cont(0.5) WITHIN GROUP (
               ORDER BY EXTRACT(EPOCH FROM (occurred_at - prev_at)) / 86400
           )::numeric, 1) AS median_dwell_days
    FROM (
        SELECT from_stage, occurred_at,
               LAG(occurred_at) OVER (PARTITION BY lead_id ORDER BY occurred_at) AS prev_at
        FROM engine.lead_events
        WHERE tenant_id = '{T}'::uuid AND event_type = 'stage_changed'
        [[AND occurred_at >= {{{{start_date}}}}::timestamp]]
        [[AND occurred_at <= {{{{end_date}}}}::timestamp]]
    ) t
    WHERE prev_at IS NOT NULL
    GROUP BY from_stage
)
SELECT so.stage AS "Stage",
       COALESCE(w.wip_count, 0) AS "WIP",
       COALESCE(th.exits_per_week, 0) AS "Exits/Week",
       COALESCE(d.median_dwell_days, 0) AS "Median Dwell (Days)",
       CASE WHEN COALESCE(th.exits_per_week, 0) = 0 THEN 'BLOCKED'
            ELSE round(COALESCE(w.wip_count, 0)::numeric / th.exits_per_week, 1)::text
       END AS "WIP/Throughput"
FROM stage_order so
LEFT JOIN wip w ON w.stage = so.stage
LEFT JOIN throughput th ON th.stage = so.stage
LEFT JOIN dwell d ON d.stage = so.stage
ORDER BY so.pos
""",
        "viz": {},
        "date_filter": True,
    },
]

LAYOUTS = [
    # Row 0 — scalars
    {"col": 0,  "row": 0,  "size_x": 6,  "size_y": 3},   # Sales Velocity
    {"col": 6,  "row": 0,  "size_x": 6,  "size_y": 3},   # Median Sales Cycle
    # Row 1 — pipeline + movements
    {"col": 0,  "row": 3,  "size_x": 9,  "size_y": 6},   # Pipeline Snapshot
    {"col": 9,  "row": 3,  "size_x": 9,  "size_y": 6},   # Stage Movements
    # Row 2 — conversion + avg days
    {"col": 0,  "row": 9,  "size_x": 9,  "size_y": 6},   # Live Conversion Rates
    {"col": 9,  "row": 9,  "size_x": 9,  "size_y": 6},   # Avg Days in Each Stage
    # Row 3 — source + transitions
    {"col": 0,  "row": 15, "size_x": 9,  "size_y": 6},   # Leads by Source
    {"col": 9,  "row": 15, "size_x": 9,  "size_y": 6},   # Recent Stage Transitions
    # Row 4 — bottleneck
    {"col": 0,  "row": 21, "size_x": 18, "size_y": 7},   # Bottleneck Analysis
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
