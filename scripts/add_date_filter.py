"""Add date range filter to RESG Baseline dashboard in Metabase.

Updates all CRM cards with optional date template variables,
then wires a dashboard-level date filter to every card.
"""
import json
import os
import sys
import uuid

import httpx

MB_URL = os.getenv("METABASE_URL", "https://metabase.resg.uk")
MB_KEY = os.getenv("METABASE_API_KEY", "")
DB_ID = 3
DASH_ID = 5
TENANT_ID = "c545b164-9aad-4edb-a3ba-8820fb5a8037"

headers = {"x-api-key": MB_KEY, "Content-Type": "application/json"}


def api(method: str, path: str, body: dict | None = None) -> dict:
    with httpx.Client(timeout=30.0) as c:
        r = c.request(method, f"{MB_URL}{path}", headers=headers, json=body)
        if r.status_code not in (200, 201, 202):
            print(f"ERROR: {method} {path} -> {r.status_code}: {r.text[:500]}")
            sys.exit(1)
        return r.json()


def make_tags():
    return {
        "start_date": {
            "id": str(uuid.uuid4()),
            "name": "start_date",
            "display-name": "Start Date",
            "type": "date",
            "required": False,
        },
        "end_date": {
            "id": str(uuid.uuid4()),
            "name": "end_date",
            "display-name": "End Date",
            "type": "date",
            "required": False,
        },
    }


T = TENANT_ID
DATE_CLAUSE = "[[AND {alias}created_at >= {{{{start_date}}}}::timestamp]]\n[[AND {alias}created_at <= {{{{end_date}}}}::timestamp]]"

# All CRM card SQL with date filter clauses
CARDS_SQL = {
    77: f'SELECT count(*) AS "Total Leads"\nFROM engine.leads\nWHERE tenant_id = \'{T}\'::uuid\n{DATE_CLAUSE.format(alias="")}',

    78: f'SELECT round(\n    count(*) FILTER (WHERE current_stage = \'won\')::numeric\n    / NULLIF(count(*), 0) * 100, 1\n) AS "Win Rate %"\nFROM engine.leads\nWHERE tenant_id = \'{T}\'::uuid\n{DATE_CLAUSE.format(alias="")}',

    79: f'SELECT round(\n    count(*) FILTER (WHERE current_stage = \'rejected\')::numeric\n    / NULLIF(count(*), 0) * 100, 1\n) AS "Rejection Rate %"\nFROM engine.leads\nWHERE tenant_id = \'{T}\'::uuid\n{DATE_CLAUSE.format(alias="")}',

    80: f'SELECT round(\n    count(*)::numeric\n    / GREATEST(1, EXTRACT(EPOCH FROM (max(created_at) - min(created_at))) / 86400 / 30), 1\n) AS "Leads/Month"\nFROM engine.leads\nWHERE tenant_id = \'{T}\'::uuid\n{DATE_CLAUSE.format(alias="")}',

    81: f'SELECT (max(created_at)::date - min(created_at)::date) || \' days\' AS "Period"\nFROM engine.leads\nWHERE tenant_id = \'{T}\'::uuid\n{DATE_CLAUSE.format(alias="")}',

    82: f"""WITH stage_order(stage, pos) AS (
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
ORDER BY fs.pos""",

    83: f"""SELECT
    CASE
        WHEN current_stage = 'won' THEN 'Won'
        WHEN current_stage = 'lost' THEN 'Lost'
        WHEN current_stage = 'rejected' THEN 'Rejected'
        ELSE 'Open / In Progress'
    END AS "Outcome",
    count(*) AS "Count"
FROM engine.leads
WHERE tenant_id = '{T}'::uuid
{DATE_CLAUSE.format(alias="")}
GROUP BY 1
ORDER BY 2 DESC""",

    84: f"""WITH stage_order(stage, pos) AS (
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
ORDER BY a.pos""",

    85: f"""SELECT date_trunc('month', created_at)::date AS "Month",
       count(*) AS "Leads"
FROM engine.leads
WHERE tenant_id = '{T}'::uuid
{DATE_CLAUSE.format(alias="")}
GROUP BY 1
ORDER BY 1""",

    86: f"""WITH stage_order(stage, pos) AS (
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
ORDER BY so.pos""",
}

PARAM_START_ID = "start_date_param"
PARAM_END_ID = "end_date_param"


def main():
    if not MB_KEY:
        print("ERROR: METABASE_API_KEY required.")
        sys.exit(1)

    # Step 1: Update each CRM card SQL with date template tags
    print("Updating cards with date filter variables...")
    for card_id, sql in CARDS_SQL.items():
        tags = make_tags()
        body = {
            "dataset_query": {
                "type": "native",
                "native": {"query": sql, "template-tags": tags},
                "database": DB_ID,
            },
        }
        api("PUT", f"/api/card/{card_id}", body)
        print(f"  Updated card {card_id}")

    # Step 2: Get current dashboard state
    print("\nWiring dashboard filter to all cards...")
    dash = api("GET", f"/api/dashboard/{DASH_ID}")
    existing_cards = dash.get("dashcards", [])

    # Step 3: Build dashcards with parameter mappings
    new_dashcards = []
    for dc in existing_cards:
        card_id = dc.get("card_id")
        entry = {
            "id": dc["id"],
            "card_id": card_id,
            "row": dc["row"],
            "col": dc["col"],
            "size_x": dc["size_x"],
            "size_y": dc["size_y"],
            "visualization_settings": dc.get("visualization_settings", {}),
        }

        if card_id and card_id in CARDS_SQL:
            entry["parameter_mappings"] = [
                {
                    "parameter_id": PARAM_START_ID,
                    "card_id": card_id,
                    "target": ["variable", ["template-tag", "start_date"]],
                },
                {
                    "parameter_id": PARAM_END_ID,
                    "card_id": card_id,
                    "target": ["variable", ["template-tag", "end_date"]],
                },
            ]
        else:
            entry["parameter_mappings"] = dc.get("parameter_mappings", [])

        new_dashcards.append(entry)

    # Step 4: Update dashboard with parameters + mappings
    dash_update = {
        "parameters": [
            {
                "id": PARAM_START_ID,
                "name": "Start Date",
                "slug": "start_date",
                "type": "date/single",
                "sectionId": "date",
            },
            {
                "id": PARAM_END_ID,
                "name": "End Date",
                "slug": "end_date",
                "type": "date/single",
                "sectionId": "date",
            },
        ],
        "dashcards": new_dashcards,
    }

    api("PUT", f"/api/dashboard/{DASH_ID}", dash_update)
    print(f"\nDone! Date range filter added to all CRM cards.")
    print(f"Dashboard: {MB_URL}/dashboard/{DASH_ID}")


if __name__ == "__main__":
    main()
