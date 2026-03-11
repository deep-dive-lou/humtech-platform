"""Build Bot Health dashboard in Metabase.

Cards sourced from monitoring.* views (migration 004).

Usage:
    METABASE_API_KEY=mb_... METABASE_COLLECTION_ID=<id> python scripts/build_bot_health_dashboard.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from _metabase import (
    TENANT_ID, TENANT_NAME, require_key, create_question, create_dashboard,
    wire_cards, dashboard_exists, make_date_tags,
)

COLLECTION_ID = int(os.getenv("METABASE_COLLECTION_ID", "5"))
DASHBOARD_NAME = f"{TENANT_NAME}: Bot Health"
T = TENANT_ID

QUESTIONS = [
    # Row 1 — scalars
    {
        "name": f"{TENANT_NAME} Bot: Conversations Today",
        "display": "scalar",
        "sql": f"""
SELECT count(*) AS "Today"
FROM monitoring.conversation_summary
WHERE tenant_id = '{T}'::uuid
  AND created_at::date = CURRENT_DATE
""",
        "viz": {"scalar.field": "Today"},
        "date_filter": False,
    },
    {
        "name": f"{TENANT_NAME} Bot: Conversations This Week",
        "display": "scalar",
        "sql": f"""
SELECT count(*) AS "This Week"
FROM monitoring.conversation_summary
WHERE tenant_id = '{T}'::uuid
  AND created_at >= date_trunc('week', CURRENT_DATE)
""",
        "viz": {"scalar.field": "This Week"},
        "date_filter": False,
    },
    {
        "name": f"{TENANT_NAME} Bot: Booking Rate",
        "display": "scalar",
        "sql": f"""
SELECT round(
    count(*) FILTER (WHERE has_booking)::numeric
    / NULLIF(count(*), 0) * 100, 1
) AS "Booking Rate %"
FROM monitoring.conversation_summary
WHERE tenant_id = '{T}'::uuid
[[AND created_at >= {{{{start_date}}}}::timestamp]]
[[AND created_at <= {{{{end_date}}}}::timestamp]]
""",
        "viz": {"scalar.field": "Booking Rate %", "scalar.suffix": "%"},
        "date_filter": True,
    },
    {
        "name": f"{TENANT_NAME} Bot: Avg Turns to Book",
        "display": "scalar",
        "sql": f"""
SELECT round(avg(inbound_turns)::numeric, 1) AS "Avg Turns"
FROM monitoring.conversation_summary
WHERE tenant_id = '{T}'::uuid
  AND has_booking = TRUE
[[AND created_at >= {{{{start_date}}}}::timestamp]]
[[AND created_at <= {{{{end_date}}}}::timestamp]]
""",
        "viz": {"scalar.field": "Avg Turns"},
        "date_filter": True,
    },
    {
        "name": f"{TENANT_NAME} Bot: Failed Sends (24h)",
        "display": "scalar",
        "sql": f"""
SELECT count(*) AS "Failed"
FROM monitoring.send_health
WHERE tenant_id = '{T}'::uuid
  AND send_status = 'failed'
  AND created_at >= now() - interval '24 hours'
""",
        "viz": {"scalar.field": "Failed"},
        "date_filter": False,
    },
    # Row 2 — charts
    {
        "name": f"{TENANT_NAME} Bot: Intent Distribution",
        "display": "bar",
        "sql": f"""
SELECT intent AS "Intent",
       sum(count) AS "Count"
FROM monitoring.intent_distribution
WHERE tenant_id = '{T}'::uuid
  AND intent IS NOT NULL
[[AND day >= {{{{start_date}}}}::date]]
[[AND day <= {{{{end_date}}}}::date]]
GROUP BY intent
ORDER BY sum(count) DESC
""",
        "viz": {"graph.dimensions": ["Intent"], "graph.metrics": ["Count"]},
        "date_filter": True,
    },
    {
        "name": f"{TENANT_NAME} Bot: Daily Funnel",
        "display": "line",
        "sql": f"""
SELECT day AS "Day",
       total_conversations AS "Total",
       booked AS "Booked",
       declined AS "Declined",
       wants_human AS "Wants Human",
       engaged AS "Engaged"
FROM monitoring.daily_funnel
WHERE tenant_id = '{T}'::uuid
[[AND day >= {{{{start_date}}}}::date]]
[[AND day <= {{{{end_date}}}}::date]]
ORDER BY day
""",
        "viz": {
            "graph.dimensions": ["Day"],
            "graph.metrics": ["Total", "Booked", "Declined", "Wants Human", "Engaged"],
        },
        "date_filter": True,
    },
    # Row 3 — table
    {
        "name": f"{TENANT_NAME} Bot: Active Alerts",
        "display": "table",
        "sql": f"""
SELECT contact_name AS "Contact",
       alert_type AS "Alert",
       last_intent AS "Last Intent",
       inbound_turns AS "Turns",
       has_booking AS "Booking?",
       status AS "Status",
       last_inbound_at AS "Last Inbound"
FROM monitoring.active_alerts
WHERE tenant_id = '{T}'::uuid
  AND alert_type IS NOT NULL
ORDER BY
  CASE alert_type
    WHEN 'wants_human' THEN 1
    WHEN 'high_turns' THEN 2
    WHEN 'stalled' THEN 3
    ELSE 4
  END
""",
        "viz": {},
        "date_filter": False,
    },
]

LAYOUTS = [
    # Row 1 — scalars
    {"col": 0, "row": 0, "size_x": 4, "size_y": 3},
    {"col": 4, "row": 0, "size_x": 4, "size_y": 3},
    {"col": 8, "row": 0, "size_x": 4, "size_y": 3},
    {"col": 12, "row": 0, "size_x": 3, "size_y": 3},
    {"col": 15, "row": 0, "size_x": 3, "size_y": 3},
    # Row 2 — charts
    {"col": 0, "row": 3, "size_x": 9, "size_y": 6},
    {"col": 9, "row": 3, "size_x": 9, "size_y": 6},
    # Row 3 — table
    {"col": 0, "row": 9, "size_x": 18, "size_y": 6},
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

    dash_id = create_dashboard(DASHBOARD_NAME, COLLECTION_ID, "Bot conversation health, intents, and alerts")

    wire_cards(dash_id, card_ids, LAYOUTS, date_filter_ids)

    from _metabase import MB_URL
    print(f"\n  Dashboard URL: {MB_URL}/dashboard/{dash_id}")
    print("  Done!")


if __name__ == "__main__":
    main()
