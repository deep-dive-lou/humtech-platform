"""
Build the RESG Baseline dashboard in Metabase via API.

Creates saved questions (native SQL) and arranges them on a dashboard.
Idempotent — checks for existing dashboard by name before creating.

Usage:
    METABASE_API_KEY=mb_... python scripts/build_baseline_dashboard.py
"""
import json
import os
import sys

import httpx

MB_URL = os.getenv("METABASE_URL", "https://metabase.resg.uk")
MB_KEY = os.getenv("METABASE_API_KEY", "")
DB_ID = int(os.getenv("METABASE_DB_ID", "3"))  # HumTech database
COLLECTION_ID = int(os.getenv("METABASE_COLLECTION_ID", "5"))  # RESG collection
TENANT_ID = os.getenv("TENANT_ID", "c545b164-9aad-4edb-a3ba-8820fb5a8037")
DASHBOARD_NAME = os.getenv("DASHBOARD_NAME", "RESG CRM Baseline")

headers = {"x-api-key": MB_KEY, "Content-Type": "application/json"}


def api(method: str, path: str, body: dict | None = None) -> dict:
    with httpx.Client(timeout=30.0) as client:
        resp = client.request(method, f"{MB_URL}{path}", headers=headers, json=body)
        if resp.status_code not in (200, 201, 202):
            print(f"ERROR: {method} {path} -> {resp.status_code}")
            print(resp.text[:500])
            sys.exit(1)
        return resp.json()


def create_question(name: str, sql: str, display: str = "table", viz_settings: dict | None = None) -> int:
    """Create a saved native query question. Returns card ID."""
    body = {
        "name": name,
        "dataset_query": {
            "type": "native",
            "native": {"query": sql},
            "database": DB_ID,
        },
        "display": display,
        "visualization_settings": viz_settings or {},
        "collection_id": COLLECTION_ID,
    }
    card = api("POST", "/api/card", body)
    print(f"  Created question: {name} (id={card['id']})")
    return card["id"]


# ── Questions ──────────────────────────────────────────────────────

QUESTIONS = [
    # 1. Total Leads (scalar)
    {
        "name": "RESG: Total Leads",
        "display": "scalar",
        "sql": f"""
SELECT count(*) AS "Total Leads"
FROM engine.leads
WHERE tenant_id = '{TENANT_ID}'::uuid
""",
        "viz": {"scalar.field": "Total Leads"},
    },
    # 2. Win Rate (scalar)
    {
        "name": "RESG: Win Rate",
        "display": "scalar",
        "sql": f"""
SELECT round(
    count(*) FILTER (WHERE current_stage = 'won')::numeric
    / NULLIF(count(*), 0) * 100, 1
) AS "Win Rate %"
FROM engine.leads
WHERE tenant_id = '{TENANT_ID}'::uuid
""",
        "viz": {"scalar.field": "Win Rate %", "scalar.suffix": "%"},
    },
    # 3. Rejection Rate (scalar)
    {
        "name": "RESG: Rejection Rate",
        "display": "scalar",
        "sql": f"""
SELECT round(
    count(*) FILTER (WHERE current_stage = 'rejected')::numeric
    / NULLIF(count(*), 0) * 100, 1
) AS "Rejection Rate %"
FROM engine.leads
WHERE tenant_id = '{TENANT_ID}'::uuid
""",
        "viz": {"scalar.field": "Rejection Rate %", "scalar.suffix": "%"},
    },
    # 4. Lead Volume per Month (scalar)
    {
        "name": "RESG: Leads per Month",
        "display": "scalar",
        "sql": f"""
SELECT round(
    count(*)::numeric
    / GREATEST(1, EXTRACT(EPOCH FROM (max(created_at) - min(created_at))) / 86400 / 30), 1
) AS "Leads/Month"
FROM engine.leads
WHERE tenant_id = '{TENANT_ID}'::uuid
""",
        "viz": {"scalar.field": "Leads/Month"},
    },
    # 5. Period (scalar)
    {
        "name": "RESG: Data Period",
        "display": "scalar",
        "sql": f"""
SELECT (max(created_at)::date - min(created_at)::date) || ' days' AS "Period"
FROM engine.leads
WHERE tenant_id = '{TENANT_ID}'::uuid
""",
        "viz": {},
    },
    # 6. Stage Funnel (bar chart)
    {
        "name": "RESG: Pipeline Funnel",
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
    WHERE l.tenant_id = '{TENANT_ID}'::uuid
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
    },
    # 7. Lead Outcome Breakdown (pie)
    {
        "name": "RESG: Lead Outcomes",
        "display": "pie",
        "sql": f"""
SELECT
    CASE
        WHEN current_stage = 'won' THEN 'Won'
        WHEN current_stage = 'lost' THEN 'Lost'
        WHEN current_stage = 'rejected' THEN 'Rejected'
        ELSE 'Open / In Progress'
    END AS "Outcome",
    count(*) AS "Count"
FROM engine.leads
WHERE tenant_id = '{TENANT_ID}'::uuid
GROUP BY 1
ORDER BY 2 DESC
""",
        "viz": {
            "pie.dimension": ["Outcome"],
            "pie.metric": "Count",
        },
    },
    # 8. Conversion Rates (bar chart)
    {
        "name": "RESG: Stage Conversion Rates",
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
    WHERE l.tenant_id = '{TENANT_ID}'::uuid
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
    },
    # 9. Monthly Lead Volume Over Time (line)
    {
        "name": "RESG: Monthly Lead Volume",
        "display": "line",
        "sql": f"""
SELECT date_trunc('month', created_at)::date AS "Month",
       count(*) AS "Leads"
FROM engine.leads
WHERE tenant_id = '{TENANT_ID}'::uuid
GROUP BY 1
ORDER BY 1
""",
        "viz": {
            "graph.dimensions": ["Month"],
            "graph.metrics": ["Leads"],
        },
    },
    # 10. Leads by Current Stage (table)
    {
        "name": "RESG: Leads by Stage",
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
WHERE l.tenant_id = '{TENANT_ID}'::uuid
GROUP BY so.stage, so.pos
ORDER BY so.pos
""",
        "viz": {},
    },
]


def main():
    if not MB_KEY:
        print("ERROR: METABASE_API_KEY is required.")
        sys.exit(1)

    # Check existing dashboards to avoid duplicates
    dashboards = api("GET", "/api/dashboard")
    existing = [d for d in dashboards if d["name"] == DASHBOARD_NAME and d.get("collection_id") == COLLECTION_ID]
    if existing:
        print(f"Dashboard '{DASHBOARD_NAME}' already exists (id={existing[0]['id']}). Delete it first to rebuild.")
        sys.exit(1)

    # Create all questions
    print(f"Creating {len(QUESTIONS)} questions in collection {COLLECTION_ID}...")
    card_ids = []
    for q in QUESTIONS:
        card_id = create_question(q["name"], q["sql"], q["display"], q.get("viz"))
        card_ids.append(card_id)

    # Create dashboard
    print(f"\nCreating dashboard: {DASHBOARD_NAME}")
    dash = api("POST", "/api/dashboard", {
        "name": DASHBOARD_NAME,
        "collection_id": COLLECTION_ID,
    })
    dash_id = dash["id"]
    print(f"  Dashboard created (id={dash_id})")

    # Layout: 18-column grid
    # Row 1: 5 scalar cards across the top (each ~3.5 cols wide)
    # Row 2: funnel bar (9 cols) + pie chart (9 cols)
    # Row 3: conversion rates (9 cols) + monthly volume (9 cols)
    # Row 4: stage table (full width)
    layouts = [
        # Row 1 — headline numbers
        {"col": 0,  "row": 0, "size_x": 4, "size_y": 3},   # Total Leads
        {"col": 4,  "row": 0, "size_x": 4, "size_y": 3},   # Win Rate
        {"col": 8,  "row": 0, "size_x": 4, "size_y": 3},   # Rejection Rate
        {"col": 12, "row": 0, "size_x": 3, "size_y": 3},   # Leads/Month
        {"col": 15, "row": 0, "size_x": 3, "size_y": 3},   # Period
        # Row 2 — funnel + outcomes
        {"col": 0,  "row": 3, "size_x": 9, "size_y": 6},   # Pipeline Funnel
        {"col": 9,  "row": 3, "size_x": 9, "size_y": 6},   # Lead Outcomes
        # Row 3 — conversion + volume
        {"col": 0,  "row": 9, "size_x": 9, "size_y": 6},   # Conversion Rates
        {"col": 9,  "row": 9, "size_x": 9, "size_y": 6},   # Monthly Volume
        # Row 4 — detail table
        {"col": 0,  "row": 15, "size_x": 18, "size_y": 6},  # Stage Table
    ]

    dashcards = []
    for i, (card_id, layout) in enumerate(zip(card_ids, layouts)):
        dashcards.append({
            "id": -(i + 1),
            "card_id": card_id,
            **layout,
        })

    api("PUT", f"/api/dashboard/{dash_id}", {"dashcards": dashcards})
    print(f"\n  Added {len(dashcards)} cards to dashboard.")
    print(f"\n  Dashboard URL: {MB_URL}/dashboard/{dash_id}")
    print("  Done!")


if __name__ == "__main__":
    main()
