"""
Bot inbound webhook — receives events from GHL, Twilio, or any provider and enqueues them.

Replaces the n8n inbound_capture_v1 workflow.

Endpoints:
  POST /bot/webhook/inbound/{tenant_slug}   — GHL (JSON body)
  POST /bot/webhook/inbound/twilio          — Twilio (form-encoded, HMAC-SHA1 sig)

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
import hmac
import json
import logging
from base64 import b64encode
from typing import Any

from fastapi import APIRouter, Request, Response

from app.db import get_pool
from app.utils.crypto import decrypt_credentials

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/bot/webhook", tags=["bot-webhook"])

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

TENANT_LOOKUP_SQL = """
SELECT tenant_id::text, tenant_slug, messaging_adapter
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
VALUES ($1::uuid, 'process_inbound_event', $2::uuid, 'queued', now() + ($3::int || ' seconds')::interval)
ON CONFLICT (job_type, inbound_event_id) DO NOTHING
RETURNING job_id::text;
"""

# Debounce delay for inbound_message jobs (seconds).
# new_lead is always instant (0). inbound_message waits to batch rapid-fire texts.
INBOUND_DEBOUNCE_SECONDS = 3

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
# Twilio inbound webhook
# NOTE: Must be registered BEFORE /inbound/{tenant_slug} so FastAPI
#       matches the literal path instead of treating "twilio" as a slug.
# ---------------------------------------------------------------------------

TWILIO_TENANT_LOOKUP_SQL = """
SELECT t.tenant_id::text, t.tenant_slug
FROM core.tenants t
JOIN core.tenant_credentials tc ON tc.tenant_id = t.tenant_id
WHERE tc.provider = 'twilio'
  AND t.is_enabled = TRUE;
"""


def _verify_twilio_signature(
    auth_token: str, signature: str, url: str, params: dict[str, str],
) -> bool:
    """Validate X-Twilio-Signature (HMAC-SHA1) per Twilio's spec."""
    data = url
    for key in sorted(params.keys()):
        data += key + params[key]
    expected = b64encode(
        hmac.new(auth_token.encode("utf-8"), data.encode("utf-8"), "sha1").digest()
    ).decode("utf-8")
    return hmac.compare_digest(expected, signature)


@router.post("/inbound/twilio", status_code=200)
async def twilio_inbound_webhook(request: Request) -> Response:
    """
    Receive a Twilio SMS webhook (form-encoded), verify signature,
    resolve tenant by matching To number, and enqueue a bot job.

    Returns empty TwiML <Response/> so Twilio doesn't send an auto-reply.
    """
    TWIML_EMPTY = '<?xml version="1.0" encoding="UTF-8"?><Response/>'

    form = await request.form()
    params: dict[str, str] = {k: str(v) for k, v in form.items()}

    from_number = params.get("From", "")
    to_number = params.get("To", "")
    text = params.get("Body", "")
    provider_msg_id = params.get("MessageSid", "")

    if not from_number or not text:
        logger.warning("twilio_inbound: missing From or Body")
        return Response(content=TWIML_EMPTY, media_type="application/xml")

    pool = await get_pool()
    async with pool.acquire() as conn:
        # 1. Find tenant by matching To number against Twilio credentials
        rows = await conn.fetch(TWILIO_TENANT_LOOKUP_SQL)
        tenant_id = None
        tenant_slug = None

        for row in rows:
            cred_row = await conn.fetchval(
                "SELECT credentials FROM core.tenant_credentials "
                "WHERE tenant_id = $1::uuid AND provider = 'twilio'",
                row["tenant_id"],
            )
            if not cred_row:
                continue
            creds = decrypt_credentials(bytes(cred_row))
            if creds.get("from_number") == to_number:
                tenant_id = row["tenant_id"]
                tenant_slug = row["tenant_slug"]
                # Verify signature using this tenant's auth_token
                signature = request.headers.get("X-Twilio-Signature", "")
                if signature:
                    webhook_url = str(request.url)
                    if not _verify_twilio_signature(creds["auth_token"], signature, webhook_url, params):
                        logger.warning("twilio_inbound: signature verification failed for tenant=%s", tenant_slug)
                        return Response(content=TWIML_EMPTY, media_type="application/xml")
                break

        if not tenant_id:
            logger.warning("twilio_inbound: no tenant found for To=%s", to_number)
            return Response(content=TWIML_EMPTY, media_type="application/xml")

        # 2. Build payload and ingest
        provider = "twilio"
        channel = "sms"
        channel_address = from_number

        enriched_payload = {
            "event_type": "inbound_message",
            "text": text,
            "channel": channel,
            "From": from_number,
            "To": to_number,
            "MessageSid": provider_msg_id,
            "display_name": "",
        }

        dedupe = _dedupe_key(tenant_slug, provider, provider_msg_id, channel, channel_address, text)

        # 3. Insert inbound event (idempotent)
        inbound_event_id = await conn.fetchval(
            INSERT_INBOUND_EVENT_SQL,
            tenant_id,
            provider,
            "inbound_message",
            provider_msg_id,
            channel,
            channel_address,
            dedupe,
            enriched_payload,
        )

        if not inbound_event_id:
            logger.debug("twilio_inbound: duplicate dedupe_key=%s tenant=%s", dedupe, tenant_slug)
            return Response(content=TWIML_EMPTY, media_type="application/xml")

        # 4. Enqueue job (debounce same as GHL inbound)
        job_id = await conn.fetchval(INSERT_JOB_SQL, tenant_id, inbound_event_id, INBOUND_DEBOUNCE_SECONDS)

        logger.info(json.dumps({
            "event": "twilio_webhook_received",
            "tenant_slug": tenant_slug,
            "channel": channel,
            "channel_address": channel_address,
            "inbound_event_id": inbound_event_id,
            "job_id": job_id,
        }))

    return Response(content=TWIML_EMPTY, media_type="application/xml")


# ---------------------------------------------------------------------------
# GHL Route
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
        provider = row["messaging_adapter"] or "ghl"

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

        dedupe = _dedupe_key(tenant_slug, provider, provider_msg_id, channel, channel_address, text)

        # 3. Insert inbound event (idempotent)
        inbound_event_id = await conn.fetchval(
            INSERT_INBOUND_EVENT_SQL,
            tenant_id,
            provider,
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

        # 4. Enqueue job (debounce inbound replies, instant for new_lead)
        delay = 0 if event_type == "new_lead" else INBOUND_DEBOUNCE_SECONDS
        job_id = await conn.fetchval(INSERT_JOB_SQL, tenant_id, inbound_event_id, delay)

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
