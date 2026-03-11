"""Build Revenue Pipeline (£) dashboard in Metabase.

Shows pipeline value in pounds at each stage, weighted expected revenue,
pipeline velocity (£/day), and 30/60/90-day forward projection.

Phase 2 deliverable — revenue-denominate everything.

Usage:
    METABASE_API_KEY=mb_... METABASE_COLLECTION_ID=<id> python scripts/build_revenue_pipeline_dashboard.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from _metabase import (
    TENANT_ID, TENANT_NAME, BASELINE_LABEL, require_key, create_question,
    create_dashboard, wire_cards, dashboard_exists, make_date_tags,
)

COLLECTION_ID = int(os.getenv("METABASE_COLLECTION_ID", "5"))
DASHBOARD_NAME = f"{TENANT_NAME}: Revenue Pipeline (£)"
T = TENANT_ID
BL = BASELINE_LABEL

QUESTIONS = [
    # ── Row 1: Pipeline Value by Stage + Weighted Expected Revenue ──
    {
        "name": f"{TENANT_NAME} Rev: Pipeline Value by Stage (£)",
        "display": "bar",
        "sql": f"""
WITH stage_order AS (
    SELECT unnest(ARRAY[
        'lead_created','no_comms','processing','lead_qualified',
        'appointment_booked','appointment_completed','proposal_sent'
    ]) AS stage,
    unnest(ARRAY[1,2,3,4,5,6,7]) AS pos
)
SELECT so.stage AS "Stage",
       COALESCE(sum(l.lead_value), 0) AS "Pipeline Value (£)"
FROM stage_order so
LEFT JOIN engine.leads l
    ON l.tenant_id = '{T}'::uuid
    AND l.current_stage = so.stage
    AND l.is_open = TRUE
GROUP BY so.stage, so.pos
ORDER BY so.pos
""",
        "viz": {
            "graph.dimensions": ["Stage"],
            "graph.metrics": ["Pipeline Value (£)"],
            "graph.x_axis.title_text": "Stage",
            "graph.y_axis.title_text": "£",
        },
        "date_filter": False,
    },
    {
        "name": f"{TENANT_NAME} Rev: Expected Revenue by Stage (£)",
        "display": "bar",
        "sql": f"""
WITH stage_order AS (
    SELECT unnest(ARRAY[
        'lead_created','no_comms','processing','lead_qualified',
        'appointment_booked','appointment_completed','proposal_sent'
    ]) AS stage,
    unnest(ARRAY[1,2,3,4,5,6,7]) AS pos
),
-- Win probability: fraction of leads that reached each stage and eventually won
stage_wins AS (
    SELECT le.canonical_stage,
           count(DISTINCT le.lead_id) AS reached,
           count(DISTINCT le.lead_id) FILTER (
               WHERE l2.current_stage = 'lead_won'
           ) AS won
    FROM engine.lead_events le
    JOIN engine.leads l2 ON l2.lead_id = le.lead_id
    WHERE le.tenant_id = '{T}'::uuid
      AND le.canonical_stage IS NOT NULL
    GROUP BY le.canonical_stage
),
win_prob AS (
    SELECT canonical_stage,
           CASE WHEN reached > 0 THEN won::numeric / reached ELSE 0 END AS wp
    FROM stage_wins
)
SELECT so.stage AS "Stage",
       round(COALESCE(sum(l.lead_value), 0) * COALESCE(wp.wp, 0), 0) AS "Expected Revenue (£)"
FROM stage_order so
LEFT JOIN engine.leads l
    ON l.tenant_id = '{T}'::uuid
    AND l.current_stage = so.stage
    AND l.is_open = TRUE
LEFT JOIN win_prob wp ON wp.canonical_stage = so.stage
GROUP BY so.stage, so.pos, wp.wp
ORDER BY so.pos
""",
        "viz": {
            "graph.dimensions": ["Stage"],
            "graph.metrics": ["Expected Revenue (£)"],
            "graph.x_axis.title_text": "Stage",
            "graph.y_axis.title_text": "£ (weighted)",
        },
        "date_filter": False,
    },

    # ── Row 2: Pipeline Velocity + Total Expected Revenue ──
    {
        "name": f"{TENANT_NAME} Rev: Pipeline Velocity (£/day)",
        "display": "scalar",
        "sql": f"""
WITH open_pipeline AS (
    SELECT count(*) AS opps,
           COALESCE(avg(lead_value), 0) AS avg_val
    FROM engine.leads
    WHERE tenant_id = '{T}'::uuid
      AND is_open = TRUE
),
win_rate AS (
    SELECT CASE WHEN count(*) > 0
           THEN count(*) FILTER (WHERE current_stage = 'lead_won')::numeric / count(*)
           ELSE 0 END AS wr
    FROM engine.leads
    WHERE tenant_id = '{T}'::uuid
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
)
SELECT round(
    (op.opps * op.avg_val * wr.wr) / GREATEST(cl.median_days, 1),
    0
) AS "Pipeline Velocity"
FROM open_pipeline op, win_rate wr, cycle_length cl
""",
        "viz": {"scalar.field": "Pipeline Velocity", "scalar.prefix": "£", "scalar.suffix": "/day"},
        "date_filter": False,
    },
    {
        "name": f"{TENANT_NAME} Rev: Total Expected Pipeline (£)",
        "display": "scalar",
        "sql": f"""
WITH stage_wins AS (
    SELECT le.canonical_stage,
           count(DISTINCT le.lead_id) AS reached,
           count(DISTINCT le.lead_id) FILTER (
               WHERE l2.current_stage = 'lead_won'
           ) AS won
    FROM engine.lead_events le
    JOIN engine.leads l2 ON l2.lead_id = le.lead_id
    WHERE le.tenant_id = '{T}'::uuid
      AND le.canonical_stage IS NOT NULL
    GROUP BY le.canonical_stage
),
win_prob AS (
    SELECT canonical_stage,
           CASE WHEN reached > 0 THEN won::numeric / reached ELSE 0 END AS wp
    FROM stage_wins
)
SELECT round(sum(l.lead_value * COALESCE(wp.wp, 0)), 0) AS "Expected Revenue"
FROM engine.leads l
LEFT JOIN win_prob wp ON wp.canonical_stage = l.current_stage
WHERE l.tenant_id = '{T}'::uuid
  AND l.is_open = TRUE
""",
        "viz": {"scalar.field": "Expected Revenue", "scalar.prefix": "£"},
        "date_filter": False,
    },

    # ── Row 3: 30/60/90-Day Revenue Forecast ──
    {
        "name": f"{TENANT_NAME} Rev: 30/60/90-Day Forecast",
        "display": "table",
        "sql": f"""
WITH stage_order AS (
    SELECT unnest(ARRAY[
        'lead_created','no_comms','processing','lead_qualified',
        'appointment_booked','appointment_completed','proposal_sent'
    ]) AS stage,
    unnest(ARRAY[1,2,3,4,5,6,7]) AS pos
),
-- Win probability per stage
stage_wins AS (
    SELECT le.canonical_stage,
           count(DISTINCT le.lead_id) AS reached,
           count(DISTINCT le.lead_id) FILTER (
               WHERE l2.current_stage = 'lead_won'
           ) AS won
    FROM engine.lead_events le
    JOIN engine.leads l2 ON l2.lead_id = le.lead_id
    WHERE le.tenant_id = '{T}'::uuid
      AND le.canonical_stage IS NOT NULL
    GROUP BY le.canonical_stage
),
win_prob AS (
    SELECT canonical_stage,
           CASE WHEN reached > 0 THEN won::numeric / reached ELSE 0 END AS wp
    FROM stage_wins
),
-- Median days from each stage to won (historical)
stage_to_won AS (
    SELECT le.canonical_stage,
           percentile_cont(0.5) WITHIN GROUP (
               ORDER BY EXTRACT(EPOCH FROM (l2.won_at - le.occurred_at)) / 86400
           ) AS median_days_to_won
    FROM engine.lead_events le
    JOIN engine.leads l2 ON l2.lead_id = le.lead_id
    WHERE le.tenant_id = '{T}'::uuid
      AND l2.current_stage = 'lead_won'
      AND l2.won_at IS NOT NULL
      AND le.canonical_stage IS NOT NULL
    GROUP BY le.canonical_stage
),
-- Open pipeline with expected value and timing
pipeline AS (
    SELECT l.current_stage,
           l.lead_value,
           COALESCE(wp.wp, 0) AS win_prob,
           COALESCE(stw.median_days_to_won, 90) AS days_to_close
    FROM engine.leads l
    LEFT JOIN win_prob wp ON wp.canonical_stage = l.current_stage
    LEFT JOIN stage_to_won stw ON stw.canonical_stage = l.current_stage
    WHERE l.tenant_id = '{T}'::uuid
      AND l.is_open = TRUE
      AND l.lead_value IS NOT NULL
)
SELECT unnest(ARRAY['30 days', '60 days', '90 days']) AS "Horizon",
       unnest(ARRAY[
           round(sum(CASE WHEN days_to_close <= 30 THEN lead_value * win_prob ELSE 0 END), 0),
           round(sum(CASE WHEN days_to_close <= 60 THEN lead_value * win_prob ELSE 0 END), 0),
           round(sum(lead_value * win_prob), 0)
       ]) AS "Projected Revenue (£)"
FROM pipeline
""",
        "viz": {},
        "date_filter": False,
    },

    # ── Row 4: Stage Value Trend Over Time ──
    {
        "name": f"{TENANT_NAME} Rev: Pipeline Value Trend",
        "display": "line",
        "sql": f"""
SELECT date_trunc('month', l.created_at)::date AS "Month",
       l.current_stage AS "Stage",
       sum(COALESCE(l.lead_value, 0)) AS "Pipeline Value (£)"
FROM engine.leads l
WHERE l.tenant_id = '{T}'::uuid
  AND l.is_open = TRUE
[[AND l.created_at >= {{{{start_date}}}}::timestamp]]
[[AND l.created_at <= {{{{end_date}}}}::timestamp]]
GROUP BY 1, 2
ORDER BY 1
""",
        "viz": {
            "graph.dimensions": ["Month", "Stage"],
            "graph.metrics": ["Pipeline Value (£)"],
        },
        "date_filter": True,
    },
]

LAYOUTS = [
    # Row 1 — two bar charts side by side
    {"col": 0, "row": 0, "size_x": 9, "size_y": 7},
    {"col": 9, "row": 0, "size_x": 9, "size_y": 7},
    # Row 2 — velocity scalar + expected total scalar
    {"col": 0, "row": 7, "size_x": 6, "size_y": 4},
    {"col": 6, "row": 7, "size_x": 6, "size_y": 4},
    # Row 3 — 30/60/90 forecast table
    {"col": 0, "row": 11, "size_x": 12, "size_y": 5},
    # Row 4 — value trend line
    {"col": 0, "row": 16, "size_x": 18, "size_y": 6},
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
        tags = make_date_tags() if q.get("date_filter") else None
        cid = create_question(q["name"], q["sql"], q["display"], q.get("viz"), COLLECTION_ID, tags)
        card_ids.append(cid)
        if q.get("date_filter"):
            date_filter_ids.add(cid)

    dash_id = create_dashboard(
        DASHBOARD_NAME, COLLECTION_ID,
        "Pipeline value in £ at each stage, weighted expected revenue, velocity, and 30/60/90-day projection"
    )

    wire_cards(dash_id, card_ids, LAYOUTS, date_filter_ids if date_filter_ids else None)

    from _metabase import MB_URL
    print(f"\n  Dashboard URL: {MB_URL}/dashboard/{dash_id}")
    print("  Done!")


if __name__ == "__main__":
    main()
