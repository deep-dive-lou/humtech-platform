"""Shared Metabase API helpers for dashboard-building scripts."""
import os
import sys
import uuid

import httpx

MB_URL = os.getenv("METABASE_URL", "https://metabase.resg.uk")
MB_KEY = os.getenv("METABASE_API_KEY", "")
DB_ID = int(os.getenv("METABASE_DB_ID", "3"))
TENANT_ID = os.getenv("TENANT_ID", "c545b164-9aad-4edb-a3ba-8820fb5a8037")

_headers = {"x-api-key": MB_KEY, "Content-Type": "application/json"}


def api(method: str, path: str, body: dict | None = None) -> dict:
    """Make a Metabase API call. Exits on error."""
    with httpx.Client(timeout=30.0) as client:
        resp = client.request(method, f"{MB_URL}{path}", headers=_headers, json=body)
        if resp.status_code not in (200, 201, 202):
            print(f"ERROR: {method} {path} -> {resp.status_code}")
            print(resp.text[:500])
            sys.exit(1)
        return resp.json()


def create_question(
    name: str,
    sql: str,
    display: str = "table",
    viz_settings: dict | None = None,
    collection_id: int | None = None,
    template_tags: dict | None = None,
) -> int:
    """Create a saved native query. Returns card ID."""
    native = {"query": sql}
    if template_tags:
        native["template-tags"] = template_tags
    body = {
        "name": name,
        "dataset_query": {"type": "native", "native": native, "database": DB_ID},
        "display": display,
        "visualization_settings": viz_settings or {},
        "collection_id": collection_id,
    }
    card = api("POST", "/api/card", body)
    print(f"  Created question: {name} (id={card['id']})")
    return card["id"]


def make_date_tags() -> dict:
    """Return start_date/end_date template tag definitions."""
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


def dashboard_exists(name: str, collection_id: int) -> int | None:
    """Check if dashboard already exists. Returns dashboard ID or None."""
    dashboards = api("GET", "/api/dashboard")
    for d in dashboards:
        if d["name"] == name and d.get("collection_id") == collection_id:
            return d["id"]
    return None


def create_dashboard(name: str, collection_id: int, description: str = "") -> int:
    """Create a dashboard shell. Returns dashboard ID."""
    dash = api("POST", "/api/dashboard", {
        "name": name,
        "collection_id": collection_id,
        "description": description,
    })
    print(f"  Dashboard created: {name} (id={dash['id']})")
    return dash["id"]


PARAM_START_ID = "start_date_param"
PARAM_END_ID = "end_date_param"


def wire_cards(
    dash_id: int,
    card_ids: list[int],
    layouts: list[dict],
    date_filter_card_ids: set[int] | None = None,
) -> None:
    """Add cards to dashboard with layout. Wire date filter to specified cards only."""
    dashcards = []
    for i, (card_id, layout) in enumerate(zip(card_ids, layouts)):
        entry = {"id": -(i + 1), "card_id": card_id, **layout}
        if date_filter_card_ids and card_id in date_filter_card_ids:
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
        dashcards.append(entry)

    update: dict = {"dashcards": dashcards}
    if date_filter_card_ids:
        update["parameters"] = [
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
        ]

    api("PUT", f"/api/dashboard/{dash_id}", update)
    print(f"  Wired {len(dashcards)} cards to dashboard {dash_id}")


def require_key():
    """Exit if API key not set."""
    if not MB_KEY:
        print("ERROR: METABASE_API_KEY is required.")
        sys.exit(1)


def find_or_create_collection(name: str, parent_id: int | None = None) -> int:
    """Find collection by name+parent, or create it. Returns collection ID."""
    collections = api("GET", "/api/collection")
    for c in collections:
        match_name = c["name"] == name
        if parent_id is None:
            match_parent = c.get("location", "/") == "/"
        else:
            match_parent = c.get("location", "").rstrip("/").endswith(f"/{parent_id}")
        if match_name and match_parent:
            print(f"  Collection exists: {name} (id={c['id']})")
            return c["id"]
    body = {"name": name}
    if parent_id is not None:
        body["parent_id"] = parent_id
    col = api("POST", "/api/collection", body)
    print(f"  Created collection: {name} (id={col['id']})")
    return col["id"]
