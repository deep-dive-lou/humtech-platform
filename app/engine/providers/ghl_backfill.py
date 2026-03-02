"""
GHL Opportunities backfill fetcher.

Paginates through all opportunities for a GHL location and returns
a normalised list. No DB writes — caller handles persistence.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

GHL_API_BASE = "https://services.leadconnectorhq.com"
GHL_VERSION = "2021-07-28"
PAGE_LIMIT = 100


@dataclass
class NormalisedOpportunity:
    id: str
    name: Optional[str]
    stage_name: Optional[str]
    status: str  # open, won, lost, abandoned
    contact_id: Optional[str]
    contact_name: Optional[str]
    monetary_value: Optional[float]
    created_at: datetime
    updated_at: datetime


def _parse_dt(value: Any) -> datetime:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    if isinstance(value, str) and value.strip():
        txt = value.strip()
        if txt.endswith("Z"):
            txt = txt[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(txt)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            pass
    return datetime.now(tz=timezone.utc)


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalise(opp: dict[str, Any]) -> NormalisedOpportunity:
    stage = opp.get("stage") or {}
    stage_name = stage.get("name") if isinstance(stage, dict) else opp.get("pipelineStage")

    contact = opp.get("contact") or {}
    contact_id = contact.get("id") if isinstance(contact, dict) else None

    first = (contact.get("firstName") or "") if isinstance(contact, dict) else ""
    last = (contact.get("lastName") or "") if isinstance(contact, dict) else ""
    full = f"{first} {last}".strip()
    contact_name = full or (contact.get("name") if isinstance(contact, dict) else None)

    return NormalisedOpportunity(
        id=opp["id"],
        name=opp.get("name"),
        stage_name=stage_name,
        status=(opp.get("status") or "open").lower(),
        contact_id=contact_id,
        contact_name=contact_name,
        monetary_value=_to_float(opp.get("monetaryValue")),
        created_at=_parse_dt(opp.get("createdAt")),
        updated_at=_parse_dt(opp.get("updatedAt") or opp.get("createdAt")),
    )


async def fetch_all_opportunities(
    token: str,
    location_id: str,
) -> list[NormalisedOpportunity]:
    """
    Fetch all opportunities for a GHL location via paginated API calls.

    Returns a list of NormalisedOpportunity — no DB writes.
    Paginates using nextPageCursor from meta until exhausted.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Version": GHL_VERSION,
        "Accept": "application/json",
    }

    results: list[NormalisedOpportunity] = []
    cursor: Optional[str] = None
    page = 0

    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            params: dict[str, Any] = {
                "location_id": location_id,
                "limit": PAGE_LIMIT,
            }
            if cursor:
                params["startAfter"] = cursor

            resp = await client.get(
                f"{GHL_API_BASE}/opportunities/search",
                headers=headers,
                params=params,
            )

            if resp.status_code != 200:
                raise RuntimeError(
                    f"GHL opportunities fetch failed: {resp.status_code} {resp.text[:300]}"
                )

            data = resp.json()
            opportunities = data.get("opportunities") or data.get("data") or []
            meta = data.get("meta") or {}

            batch = [_normalise(o) for o in opportunities]
            results.extend(batch)
            page += 1

            logger.info(
                "Fetched page %d: %d opportunities (total so far: %d)",
                page, len(batch), len(results),
            )

            next_cursor = meta.get("nextPageCursor") or meta.get("startAfter")
            if not next_cursor or not batch:
                break

            cursor = next_cursor

    return results
