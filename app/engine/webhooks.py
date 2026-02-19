from __future__ import annotations

import json
import logging
from typing import Any, Optional

import asyncpg
from fastapi import APIRouter, HTTPException, Request

from app.db import get_pool
from app.engine.events import resolve_stage_mapping, upsert_lead, write_lead_event
from app.engine.providers.ghl_webhook_parser import (
    NormalizedWebhookEvent,
    parse_ghl_webhook,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/engine/webhooks", tags=["engine"])

LOAD_TENANT_SETTINGS_SQL = """
SELECT tenant_id::text AS tenant_id, settings
FROM core.tenants
WHERE tenant_id = $1::uuid
  AND is_enabled = TRUE
LIMIT 1;
"""

LOAD_ENABLED_TENANTS_SQL = """
SELECT tenant_id::text AS tenant_id, settings
FROM core.tenants
WHERE is_enabled = TRUE;
"""


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            return {}
    return {}


def _from_nested(settings: dict[str, Any], keys: list[str]) -> Optional[str]:
    cur: Any = settings
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    if isinstance(cur, str) and cur.strip():
        return cur.strip()
    return None


def _configured_secret(settings: dict[str, Any], provider: str) -> Optional[str]:
    candidates = [
        _from_nested(settings, ["engine", "webhooks", provider, "secret"]),
        _from_nested(settings, ["webhooks", provider, "secret"]),
        _from_nested(settings, [f"{provider}_webhook_secret"]),
        _from_nested(settings, ["webhook_secret"]),
    ]
    for item in candidates:
        if item:
            return item
    return None


def _configured_location_id(settings: dict[str, Any], provider: str) -> Optional[str]:
    candidates = [
        _from_nested(settings, ["engine", "webhooks", provider, "location_id"]),
        _from_nested(settings, ["engine", "webhooks", provider, "locationId"]),
        _from_nested(settings, ["webhooks", provider, "location_id"]),
        _from_nested(settings, ["webhooks", provider, "locationId"]),
        _from_nested(settings, [f"{provider}_location_id"]),
        _from_nested(settings, ["location_id"]),
        _from_nested(settings, ["locationId"]),
    ]
    for item in candidates:
        if item:
            return item
    return None


def _get_parser(provider: str):
    provider_l = provider.lower()
    if provider_l == "ghl":
        return parse_ghl_webhook
    raise HTTPException(status_code=404, detail=f"Unsupported provider: {provider}")


async def _auth_by_tenant_id(
    conn: asyncpg.Connection,
    *,
    provider: str,
    webhook_secret: str,
    location_id: str,
    tenant_id: str,
) -> Optional[str]:
    row = await conn.fetchrow(LOAD_TENANT_SETTINGS_SQL, tenant_id)
    if not row:
        return None

    settings = _as_dict(row["settings"])
    expected_secret = _configured_secret(settings, provider)
    expected_location = _configured_location_id(settings, provider)
    if not expected_secret or not expected_location:
        return None
    if expected_secret != webhook_secret:
        return None
    if expected_location != location_id:
        return None
    return row["tenant_id"]


async def _auth_by_scan(
    conn: asyncpg.Connection,
    *,
    provider: str,
    webhook_secret: str,
    location_id: str,
) -> Optional[str]:
    rows = await conn.fetch(LOAD_ENABLED_TENANTS_SQL)
    for row in rows:
        settings = _as_dict(row["settings"])
        expected_secret = _configured_secret(settings, provider)
        expected_location = _configured_location_id(settings, provider)
        if expected_secret == webhook_secret and expected_location == location_id:
            return row["tenant_id"]
    return None


async def _authenticate_tenant(
    conn: asyncpg.Connection,
    *,
    provider: str,
    webhook_secret: str,
    location_id: Optional[str],
    tenant_hint: Optional[str],
) -> str:
    if not location_id:
        raise HTTPException(status_code=401, detail="Missing locationId")

    if tenant_hint:
        tenant_id = await _auth_by_tenant_id(
            conn,
            provider=provider,
            webhook_secret=webhook_secret,
            location_id=location_id,
            tenant_id=tenant_hint,
        )
        if tenant_id:
            return tenant_id

    tenant_id = await _auth_by_scan(
        conn,
        provider=provider,
        webhook_secret=webhook_secret,
        location_id=location_id,
    )
    if tenant_id:
        return tenant_id

    raise HTTPException(status_code=401, detail="Webhook authentication failed")


async def _ingest(
    provider: str,
    payload: dict[str, Any],
    webhook_secret: str,
) -> dict[str, Any]:
    parser = _get_parser(provider)
    normalized: NormalizedWebhookEvent = parser(payload)

    pool = await get_pool()
    async with pool.acquire() as conn:
        tenant_id = await _authenticate_tenant(
            conn,
            provider=provider,
            webhook_secret=webhook_secret,
            location_id=normalized.location_id,
            tenant_hint=normalized.tenant_id,
        )

        canonical_stage = None
        if normalized.raw_stage:
            canonical_stage = await resolve_stage_mapping(
                conn,
                tenant_id=tenant_id,
                provider=provider,
                raw_stage=normalized.raw_stage,
            )
            if not canonical_stage:
                logger.warning(
                    "Missing stage mapping for tenant=%s provider=%s raw_stage=%s source_event_id=%s",
                    tenant_id,
                    provider,
                    normalized.raw_stage,
                    normalized.source_event_id,
                )

        lead_id = await upsert_lead(
            conn,
            tenant_id=tenant_id,
            provider=provider,
            external_id=normalized.lead_external_id,
            contact_provider=provider,
            contact_external_id=normalized.contact_external_id,
            current_stage=canonical_stage,
            raw_stage=normalized.raw_stage,
            source=f"webhook:{provider}",
            lead_value=normalized.lead_value,
            metadata={"raw_payload": normalized.raw_payload},
        )

        event_id = await write_lead_event(
            conn,
            lead_id=lead_id,
            tenant_id=tenant_id,
            event_type=normalized.event_type,
            source=f"crm_webhook:{provider}",
            occurred_at=normalized.occurred_at,
            canonical_stage=canonical_stage,
            to_stage=canonical_stage if normalized.event_type == "stage_changed" else None,
            source_event_id=normalized.source_event_id,
            actor=f"webhook:{provider}",
            amount=normalized.lead_value,
            payload={"raw_payload": normalized.raw_payload, "raw_stage": normalized.raw_stage},
        )

    return {
        "ok": True,
        "provider": provider,
        "tenant_id": tenant_id,
        "lead_id": lead_id,
        "event_id": event_id,
        "duplicate": event_id is None,
        "canonical_stage": canonical_stage,
    }


@router.post("/{provider}")
async def ingest_engine_webhook(
    provider: str,
    request: Request,
    webhook_secret: Optional[str] = None,
) -> dict[str, Any]:
    # Compatibility route: allows existing path while still enforcing secret.
    if not webhook_secret:
        raise HTTPException(status_code=401, detail="Missing webhook secret")
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Webhook payload must be a JSON object")
    return await _ingest(provider, payload, webhook_secret)


@router.post("/{provider}/{webhook_secret}")
async def ingest_engine_webhook_with_secret(
    provider: str,
    webhook_secret: str,
    request: Request,
) -> dict[str, Any]:
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Webhook payload must be a JSON object")
    return await _ingest(provider, payload, webhook_secret)
