"""
Bot inbound webhook — receives events from GHL (or any provider) and enqueues them.

Replaces the n8n inbound_capture_v1 workflow.

Endpoint: POST /bot/webhook/inbound/{tenant_slug}

GHL sends two event types:
  - new_lead:        new contact / form submission
  - inbound_message: lead replies to a message

Expected body fields (set in GHL Custom Data):
  event_type        "new_lead" | "inbound_message"
  channel           "sms" | "whatsapp"
  phone             lead's phone number (channel_address)
  text              message body (inbound_message only)
  messageId         GHL message ID (inbound_message only)
  display_name      contact's full name (optional)
  contact_metadata  { "contactId": "..." }  — required for sending replies
"""
from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

from fastapi import APIRouter, Request, Response

from app.db import get_pool

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/bot/webhook", tags=["bot-webhook"])

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

TENANT_LOOKUP_SQL = """
SELECT tenant_id::text, tenant_slug
FROM core.tenants
WHERE tenant_slug = $1
  AND is_enabled = TRUE
LIMIT 1;
"""

INSERT_INBOUND_EVENT_SQL = """
INSERT INTO bot.inbound_events (
    tenant_id, provider, event_type,
    provider_msg_id, channel, channel_address,
    dedupe_key, payload
)
VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8::jsonb)
ON CONFLICT (tenant_id, dedupe_key) DO NOTHING
RETURNING inbound_event_id::text;
"""

INSERT_JOB_SQL = """
INSERT INTO bot.job_queue (tenant_id, job_type, inbound_event_id, status, run_after)
VALUES ($1::uuid, 'process_inbound_event', $2::uuid, 'queued', now())
ON CONFLICT (job_type, inbound_event_id) DO NOTHING
RETURNING job_id::text;
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dedupe_key(tenant_slug: str, provider: str, provider_msg_id: str | None,
                channel: str, channel_address: str, text: str) -> str:
    """Build a stable dedupe key for idempotent event ingestion."""
    if provider_msg_id:
        return f"{tenant_slug}|{provider}|msg|{provider_msg_id}"
    # Fallback: hash of content + 10-second time bucket
    import time
    bucket = int(time.time() // 10)
    raw = f"{tenant_slug}|{provider}|{channel}|{channel_address}|{bucket}|{text}"
    return hashlib.sha256(raw.encode()).hexdigest()[:40]


def _extract_channel_address(body: dict[str, Any]) -> str:
    """Extract the lead's phone/email from the GHL webhook body."""
    for key in ("phone", "from", "email"):
        val = body.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    # Nested under contact
    contact = body.get("contact") or {}
    for key in ("phone", "email"):
        val = contact.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return "unknown"


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

@router.post("/inbound/{tenant_slug}", status_code=200)
async def inbound_webhook(tenant_slug: str, request: Request) -> Response:
    """
    Receive a GHL webhook, validate the tenant, and enqueue a bot job.

    Always returns 200 immediately (even on duplicate / unknown tenant)
    so GHL doesn't retry unnecessarily.
    """
    try:
        body: dict[str, Any] = await request.json()
    except Exception:
        body = {}

    pool = await get_pool()
    async with pool.acquire() as conn:
        # 1. Tenant lookup
        row = await conn.fetchrow(TENANT_LOOKUP_SQL, tenant_slug)
        if not row:
            logger.warning("inbound_webhook: unknown tenant_slug=%s", tenant_slug)
            return Response(content='{"ok":false,"reason":"unknown_tenant"}',
                            media_type="application/json")

        tenant_id = row["tenant_id"]

        # 2. Extract fields from GHL payload
        # GHL Custom Data fields arrive in body.customData — strip whitespace from keys
        # (GHL can insert trailing tabs/spaces into field names)
        raw_custom = body.get("customData") or body.get("custom_data") or {}
        custom_data: dict[str, Any] = {k.strip(): v for k, v in raw_custom.items()}

        event_type = (
            body.get("event_type")          # direct (simulate / non-GHL)
            or custom_data.get("event_type")  # GHL Custom Data field
            or "inbound_message"
        )
        channel = body.get("channel") or custom_data.get("channel") or "sms"
        provider_msg_id = (
            body.get("messageId") or custom_data.get("messageId")
            or body.get("message_id") or body.get("provider_msg_id")
        )
        channel_address = _extract_channel_address(body)
        text = (
            body.get("text")
            or custom_data.get("text")         # GHL Custom Data field
            or body.get("message")
            or body.get("body")
            or ""
        )

        # display_name: prefer customData, fall back to GHL standard fields
        display_name = (
            custom_data.get("display_name")
            or body.get("display_name")
            or body.get("full_name")
            or body.get("name")
            or ""
        )

        # contactId: GHL sends it in customData AND as contact_id at root
        ghl_contact_id = (
            custom_data.get("contactId")
            or custom_data.get("contact_id")
            or body.get("contactId")
            or body.get("contact_id")
        )

        # Build enriched payload for processor
        enriched_payload = dict(body)
        enriched_payload["event_type"] = event_type
        enriched_payload["display_name"] = display_name
        enriched_payload["text"] = text
        if ghl_contact_id:
            enriched_payload["contactId"] = ghl_contact_id

        dedupe = _dedupe_key(tenant_slug, "ghl", provider_msg_id, channel, channel_address, text)

        # 3. Insert inbound event (idempotent)
        inbound_event_id = await conn.fetchval(
            INSERT_INBOUND_EVENT_SQL,
            tenant_id,
            "ghl",
            event_type,
            provider_msg_id,
            channel,
            channel_address,
            dedupe,
            enriched_payload,
        )

        if not inbound_event_id:
            # Duplicate — already processed
            logger.debug("inbound_webhook: duplicate dedupe_key=%s tenant=%s", dedupe, tenant_slug)
            return Response(content='{"ok":true,"queued":false,"reason":"duplicate"}',
                            media_type="application/json")

        # 4. Enqueue job
        job_id = await conn.fetchval(INSERT_JOB_SQL, tenant_id, inbound_event_id)

        logger.info(json.dumps({
            "event": "bot_webhook_received",
            "tenant_slug": tenant_slug,
            "event_type": event_type,
            "channel": channel,
            "channel_address": channel_address,
            "inbound_event_id": inbound_event_id,
            "job_id": job_id,
        }))

    return Response(
        content=json.dumps({"ok": True, "queued": True, "inbound_event_id": inbound_event_id}),
        media_type="application/json",
    )