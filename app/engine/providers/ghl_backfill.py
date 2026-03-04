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


def _normalise(opp: dict[str, Any], stage_id_map: dict[str, str] | None = None) -> NormalisedOpportunity:
    # Resolve stage name: try stage_id_map first, then nested stage.name, then pipelineStage
    stage_id = opp.get("pipelineStageId") or opp.get("pipelineStageUId")
    stage_name = (stage_id_map or {}).get(stage_id) if stage_id else None
    if not stage_name:
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


async def _fetch_stage_id_map(
    client: httpx.AsyncClient,
    headers: dict[str, str],
    location_id: str,
) -> dict[str, str]:
    """Fetch pipeline definitions and build stage_id → stage_name lookup."""
    resp = await client.get(
        f"{GHL_API_BASE}/opportunities/pipelines",
        headers=headers,
        params={"locationId": location_id},
    )
    if resp.status_code != 200:
        logger.warning("Failed to fetch pipelines: %s", resp.status_code)
        return {}

    stage_map: dict[str, str] = {}
    for pipeline in resp.json().get("pipelines", []):
        for stage in pipeline.get("stages", []):
            stage_map[stage["id"]] = stage["name"].strip()
    logger.info("Loaded %d pipeline stages for stage ID resolution.", len(stage_map))
    return stage_map


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
    seen_ids: set[str] = set()
    start_after: Optional[str] = None
    start_after_id: Optional[str] = None
    page = 0

    async with httpx.AsyncClient(timeout=30.0) as client:
        # Pre-fetch pipeline stage ID→name mapping
        stage_id_map = await _fetch_stage_id_map(client, headers, location_id)

        while True:
            params: dict[str, Any] = {
                "location_id": location_id,
                "limit": PAGE_LIMIT,
            }
            if start_after:
                params["startAfter"] = start_after
            if start_after_id:
                params["startAfterId"] = start_after_id

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

            # Deduplicate within this fetch — GHL cursor can repeat
            new_opps = [o for o in opportunities if o.get("id") not in seen_ids]
            if not new_opps:
                logger.info("No new opportunities on page %d — pagination complete.", page + 1)
                break

            for o in new_opps:
                seen_ids.add(o["id"])

            batch = [_normalise(o, stage_id_map) for o in new_opps]
            results.extend(batch)
            page += 1

            logger.info(
                "Fetched page %d: %d new opportunities (total so far: %d)",
                page, len(batch), len(results),
            )

            next_start_after = meta.get("nextPageCursor") or meta.get("startAfter")
            next_start_after_id = meta.get("startAfterId")
            if not next_start_after or not opportunities:
                break

            # Guard against stuck cursor
            if next_start_after == start_after and next_start_after_id == start_after_id:
                logger.warning("Cursor unchanged — stopping pagination.")
                break

            start_after = str(next_start_after)
            start_after_id = next_start_after_id

    return results
