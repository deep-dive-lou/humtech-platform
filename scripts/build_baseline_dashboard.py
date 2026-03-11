"""
Build the tenant CRM Baseline dashboard in Metabase via API.

Creates saved questions (native SQL) and arranges them on a dashboard.
Idempotent — checks for existing dashboard by name before creating.

Usage:
    METABASE_API_KEY=mb_... \\
    TENANT_ID=<uuid> \\
    TENANT_NAME=RESG \\
    METABASE_COLLECTION_ID=5 \\
    python scripts/build_baseline_dashboard.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from _metabase import (
    TENANT_ID, TENANT_NAME, MB_URL,
    require_key, create_question, create_dashboard,
    wire_cards, dashboard_exists, make_date_tags,
)

COLLECTION_ID = int(os.getenv("METABASE_COLLECTION_ID", "5"))
DASHBOARD_NAME = f"{TENANT_NAME}: CRM Baseline"
T = TENANT_ID

# ── Questions ──────────────────────────────────────────────────────────────────
# Card order determines layout slot — do not reorder without updating LAYOUTS.
#
# Slot  0   Total Leads              (scalar, row 1)
# Slot  1   Win Rate                 (scalar, row 1)
# Slot  2   Competitive Win Rate     (scalar, row 1)
# Slot  3   Rejection Rate           (scalar, row 1)
# Slot  4   Leads/Month              (scalar, row 1)
# Slot  5   Data Period              (scalar, row 1)
# Slot  6   Baseline Avg Deal Value  (scalar, row 2)
# Slot  7   Baseline Est. Rev/Month  (scalar, row 2)
# Slot  8   Revenue per Lead         (scalar, row 2)
# Slot  9   Pipeline Funnel          (bar,    row 3)
# Slot  10  Lead Outcomes            (bar,    row 3)
# Slot  11  Stage Conversion Rates   (bar,    row 4)
# Slot  12  Monthly Lead Volume      (line,   row 4)
# Slot  13  Leads by Stage           (table,  row 5)

QUESTIONS = [
    # ── Row 1: headline scalars ────────────────────────────────────────────────

    # Slot 0 — Total Leads
    {
        "name": f"{TENANT_NAME}: Total Leads",
        "display": "scalar",
        "sql": f"""
SELECT count(*) AS "Total Leads"
FROM engine.leads
WHERE tenant_id = '{T}'::uuid
[[AND created_at >= {{{{start_date}}}}::timestamp]]
[[AND created_at <= {{{{end_date}}}}::timestamp]]
""",
        "viz": {"scalar.field": "Total Leads"},
        "date_filter": True,
    },

    # Slot 1 — Win Rate (overall: won / all leads)
    {
        "name": f"{TENANT_NAME}: Win Rate",
        "display": "scalar",
        "sql": f"""
SELECT round(
    count(*) FILTER (WHERE current_stage = 'won')::numeric
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

    # Slot 2 — Competitive Win Rate (won / (won + lost) — excludes open/rejected)
    {
        "name": f"{TENANT_NAME}: Competitive Win Rate",
        "display": "scalar",
        "sql": f"""
SELECT round(
    count(*) FILTER (WHERE current_stage = 'won')::numeric
    / NULLIF(count(*) FILTER (WHERE current_stage IN ('won', 'lost')), 0) * 100, 1
) AS "Competitive Win Rate %"
FROM engine.leads
WHERE tenant_id = '{T}'::uuid
[[AND created_at >= {{{{start_date}}}}::timestamp]]
[[AND created_at <= {{{{end_date}}}}::timestamp]]
""",
        "viz": {"scalar.field": "Competitive Win Rate %", "scalar.suffix": "%"},
        "date_filter": True,
    },

    # Slot 3 — Rejection Rate
    {
        "name": f"{TENANT_NAME}: Rejection Rate",
        "display": "scalar",
        "sql": f"""
SELECT round(
    count(*) FILTER (WHERE current_stage = 'rejected')::numeric
    / NULLIF(count(*), 0) * 100, 1
) AS "Rejection Rate %"
FROM engine.leads
WHERE tenant_id = '{T}'::uuid
[[AND created_at >= {{{{start_date}}}}::timestamp]]
[[AND created_at <= {{{{end_date}}}}::timestamp]]
""",
        "viz": {"scalar.field": "Rejection Rate %", "scalar.suffix": "%"},
        "date_filter": True,
    },

    # Slot 4 — Leads per Month (computed from date span in filtered window)
    {
        "name": f"{TENANT_NAME}: Leads per Month",
        "display": "scalar",
        "sql": f"""
SELECT round(
    count(*)::numeric
    / GREATEST(1, EXTRACT(EPOCH FROM (max(created_at) - min(created_at))) / 86400 / 30), 1
) AS "Leads/Month"
FROM engine.leads
WHERE tenant_id = '{T}'::uuid
[[AND created_at >= {{{{start_date}}}}::timestamp]]
[[AND created_at <= {{{{end_date}}}}::timestamp]]
""",
        "viz": {"scalar.field": "Leads/Month"},
        "date_filter": True,
    },

    # Slot 5 — Data Period
    {
        "name": f"{TENANT_NAME}: Data Period",
        "display": "scalar",
        "sql": f"""
SELECT (max(created_at)::date - min(created_at)::date) || ' days' AS "Period"
FROM engine.leads
WHERE tenant_id = '{T}'::uuid
[[AND created_at >= {{{{start_date}}}}::timestamp]]
[[AND created_at <= {{{{end_date}}}}::timestamp]]
""",
        "viz": {},
        "date_filter": True,
    },

    # ── Row 2: baseline scalars (from frozen snapshot — no date filter) ────────

    # Slot 6 — Baseline Avg Deal Value
    {
        "name": f"{TENANT_NAME}: Baseline Avg Deal Value",
        "display": "scalar",
        "sql": f"""
SELECT COALESCE((metrics->>'avg_deal_value_gbp')::numeric, 0) AS "Avg Deal Value"
FROM engine.baselines
WHERE tenant_id = '{T}'::uuid AND is_active = TRUE
LIMIT 1
""",
        "viz": {"scalar.field": "Avg Deal Value", "scalar.prefix": "£"},
        "date_filter": False,
    },

    # Slot 7 — Baseline Est. Revenue/Month
    {
        "name": f"{TENANT_NAME}: Baseline Est. Revenue/Month",
        "display": "scalar",
        "sql": f"""
SELECT round(
    COALESCE((metrics->>'avg_deal_value_gbp')::numeric, 0)
    * COALESCE((metrics->>'lead_volume_per_month')::numeric, 0)
    * COALESCE((metrics->>'win_rate')::numeric, 0),
    0
) AS "Est. Revenue/Month"
FROM engine.baselines
WHERE tenant_id = '{T}'::uuid AND is_active = TRUE
LIMIT 1
""",
        "viz": {"scalar.field": "Est. Revenue/Month", "scalar.prefix": "£"},
        "date_filter": False,
    },

    # Slot 8 — Revenue per Lead (live: total won value / total leads)
    {
        "name": f"{TENANT_NAME}: Revenue per Lead",
        "display": "scalar",
        "sql": f"""
SELECT round(
    COALESCE(
        sum(lead_value) FILTER (WHERE current_stage = 'won')
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

    # ── Row 3: funnel charts ───────────────────────────────────────────────────

    # Slot 9 — Pipeline Funnel (bar)
    {
        "name": f"{TENANT_NAME}: Pipeline Funnel",
        "display": "bar",
        "sql": f"""
WITH stage_order(stage, pos) AS (
    VALUES
        ('lead_created', 1), ('no_comms', 2), ('processing', 3),
        ('lead_qualified', 4), ('won', 5), ('rejected', 6), ('lost', 7)
),
funnel_stages(stage, pos) AS (
    SELECT stage, pos FROM stage_order WHERE stage NOT IN ('won', 'rejected', 'lost')
),
lead_orders AS (
    SELECT l.lead_id, l.current_stage,
           COALESCE(so.pos, 0) AS current_pos
    FROM engine.leads l
    LEFT JOIN stage_order so ON so.stage = l.current_stage
    WHERE l.tenant_id = '{T}'::uuid
    [[AND l.created_at >= {{{{start_date}}}}::timestamp]]
    [[AND l.created_at <= {{{{end_date}}}}::timestamp]]
)
SELECT fs.stage AS "Stage",
       count(*) FILTER (
           WHERE lo.current_stage NOT IN ('lost', 'rejected')
             AND lo.current_pos >= fs.pos
       ) AS "Leads at or beyond"
FROM funnel_stages fs
CROSS JOIN lead_orders lo
GROUP BY fs.stage, fs.pos
ORDER BY fs.pos
""",
        "viz": {
            "graph.dimensions": ["Stage"],
            "graph.metrics": ["Leads at or beyond"],
        },
        "date_filter": True,
    },

    # Slot 10 — Lead Outcomes (bar — not pie, per Cleveland & McGill 1984)
    {
        "name": f"{TENANT_NAME}: Lead Outcomes",
        "display": "bar",
        "sql": f"""
SELECT
    CASE
        WHEN current_stage = 'won'      THEN 'Won'
        WHEN current_stage = 'lost'     THEN 'Lost'
        WHEN current_stage = 'rejected' THEN 'Rejected'
        ELSE 'Open / In Progress'
    END AS "Outcome",
    count(*) AS "Count"
FROM engine.leads
WHERE tenant_id = '{T}'::uuid
[[AND created_at >= {{{{start_date}}}}::timestamp]]
[[AND created_at <= {{{{end_date}}}}::timestamp]]
GROUP BY 1
ORDER BY 2 DESC
""",
        "viz": {
            "graph.dimensions": ["Outcome"],
            "graph.metrics": ["Count"],
        },
        "date_filter": True,
    },

    # ── Row 4: conversion + volume ─────────────────────────────────────────────

    # Slot 11 — Stage Conversion Rates (bar)
    # Includes lead_qualified → won transition (Task 4)
    {
        "name": f"{TENANT_NAME}: Stage Conversion Rates",
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
    a.stage || ' → ' || b.stage AS "Transition",
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

    # Slot 12 — Monthly Lead Volume (line)
    {
        "name": f"{TENANT_NAME}: Monthly Lead Volume",
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
        "viz": {
            "graph.dimensions": ["Month"],
            "graph.metrics": ["Leads"],
        },
        "date_filter": True,
    },

    # ── Row 5: detail table ────────────────────────────────────────────────────

    # Slot 13 — Leads by Stage (table)
    {
        "name": f"{TENANT_NAME}: Leads by Stage",
        "display": "table",
        "sql": f"""
WITH stage_order(stage, pos) AS (
    VALUES
        ('lead_created', 1), ('no_comms', 2), ('processing', 3),
        ('lead_qualified', 4), ('won', 5), ('rejected', 6), ('lost', 7)
)
SELECT so.stage AS "Stage",
       count(*) AS "Count",
       round(count(*)::numeric / NULLIF(sum(count(*)) OVER (), 0) * 100, 1) AS "% of Total"
FROM engine.leads l
JOIN stage_order so ON so.stage = l.current_stage
WHERE l.tenant_id = '{T}'::uuid
[[AND l.created_at >= {{{{start_date}}}}::timestamp]]
[[AND l.created_at <= {{{{end_date}}}}::timestamp]]
GROUP BY so.stage, so.pos
ORDER BY so.pos
""",
        "viz": {},
        "date_filter": True,
    },
]

# ── Layout ─────────────────────────────────────────────────────────────────────
# 18-column grid.
# Row 1 (row=0,  h=3): 6 scalars at 3 cols each
# Row 2 (row=3,  h=4): 3 baseline scalars at 6 cols each
# Row 3 (row=7,  h=6): funnel bar (9) + outcomes bar (9)
# Row 4 (row=13, h=6): conversion rates (9) + monthly volume (9)
# Row 5 (row=19, h=6): stage table (full width 18)

LAYOUTS = [
    # Row 1 — headline scalars
    {"col": 0,  "row": 0,  "size_x": 3, "size_y": 3},   # Slot  0  Total Leads
    {"col": 3,  "row": 0,  "size_x": 3, "size_y": 3},   # Slot  1  Win Rate
    {"col": 6,  "row": 0,  "size_x": 3, "size_y": 3},   # Slot  2  Competitive Win Rate
    {"col": 9,  "row": 0,  "size_x": 3, "size_y": 3},   # Slot  3  Rejection Rate
    {"col": 12, "row": 0,  "size_x": 3, "size_y": 3},   # Slot  4  Leads/Month
    {"col": 15, "row": 0,  "size_x": 3, "size_y": 3},   # Slot  5  Data Period
    # Row 2 — baseline scalars
    {"col": 0,  "row": 3,  "size_x": 6, "size_y": 4},   # Slot  6  Avg Deal Value
    {"col": 6,  "row": 3,  "size_x": 6, "size_y": 4},   # Slot  7  Est. Revenue/Month
    {"col": 12, "row": 3,  "size_x": 6, "size_y": 4},   # Slot  8  Revenue per Lead
    # Row 3 — funnel charts
    {"col": 0,  "row": 7,  "size_x": 9, "size_y": 6},   # Slot  9  Pipeline Funnel
    {"col": 9,  "row": 7,  "size_x": 9, "size_y": 6},   # Slot 10  Lead Outcomes
    # Row 4 — conversion + volume
    {"col": 0,  "row": 13, "size_x": 9, "size_y": 6},   # Slot 11  Stage Conversion Rates
    {"col": 9,  "row": 13, "size_x": 9, "size_y": 6},   # Slot 12  Monthly Lead Volume
    # Row 5 — detail table
    {"col": 0,  "row": 19, "size_x": 18, "size_y": 6},  # Slot 13  Leads by Stage
]


def main():
    require_key()

    existing = dashboard_exists(DASHBOARD_NAME, COLLECTION_ID)
    if existing:
        print(f"Dashboard '{DASHBOARD_NAME}' already exists (id={existing}). Delete it first to rebuild.")
        sys.exit(1)

    print(f"Creating {len(QUESTIONS)} questions in collection {COLLECTION_ID}...")
    card_ids = []
    date_filter_ids = set()
    for q in QUESTIONS:
        tags = make_date_tags() if q["date_filter"] else None
        cid = create_question(q["name"], q["sql"], q["display"], q.get("viz"), COLLECTION_ID, tags)
        card_ids.append(cid)
        if q["date_filter"]:
            date_filter_ids.add(cid)

    dash_id = create_dashboard(
        DASHBOARD_NAME,
        COLLECTION_ID,
        f"{TENANT_NAME} pre-HumTech CRM baseline — lead volume, funnel, conversion rates, and revenue metrics",
    )

    wire_cards(dash_id, card_ids, LAYOUTS, date_filter_ids)

    print(f"\n  Dashboard URL: {MB_URL}/dashboard/{dash_id}")
    print("  Done!")


if __name__ == "__main__":
    main()
