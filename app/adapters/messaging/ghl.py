"""
GHL Conversations messaging adapter.

Sends messages via POST https://services.leadconnectorhq.com/conversations/messages
Falls back to stub mode when MESSAGING_STUB env var is set.
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from typing import Any

import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://services.leadconnectorhq.com"
MESSAGING_STUB_ENABLED_KEY = "MESSAGING_STUB"

# Channel → GHL message type mapping
CHANNEL_TYPE_MAP: dict[str, str] = {
    "sms": "SMS",
    "whatsapp": "WhatsApp",
}

LOAD_CONTACT_SQL = """
SELECT channel_address, metadata
FROM bot.contacts
WHERE contact_id = $1::uuid;
"""


def _resolve_ghl_contact_id(
    contact_row: dict[str, Any] | None,
) -> str | None:
    """Extract GHL contactId from contact metadata."""
    if not contact_row:
        return None
    cmeta = contact_row.get("metadata")
    if isinstance(cmeta, dict):
        for key in ("contactId", "ghl_contact_id", "contact_id"):
            val = cmeta.get(key)
            if isinstance(val, str) and val:
                return val
    return None


async def send_message(
    *,
    tenant_id: str,
    provider: str,
    channel: str,
    to_address: str,
    text: str,
    message_id: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Send a message via the GHL Conversations API.

    POST https://services.leadconnectorhq.com/conversations/messages

    Falls back to stub mode when MESSAGING_STUB env var is set.
    On 401: refreshes the token once and retries.
    """
    from app.db import get_pool
    from app.adapters.ghl.auth import get_valid_token

    # Stub mode for testing
    if os.getenv(MESSAGING_STUB_ENABLED_KEY):
        provider_msg_id = f"ghl-{uuid.uuid4().hex[:16]}"
        return {
            "success": True,
            "provider_msg_id": provider_msg_id,
            "provider": provider,
            "channel": channel,
            "to_address": to_address,
            "raw_response": {
                "status": "sent",
                "message_id": provider_msg_id,
                "stub": True,
            },
        }

    pool = await get_pool()
    async with pool.acquire() as conn:
        # Resolve GHL contactId from contact metadata
        # message_id here is our internal message UUID; we need the contact
        # Look up contact by to_address + tenant
        contact_row = await conn.fetchrow(
            "SELECT contact_id, channel_address, metadata FROM bot.contacts "
            "WHERE tenant_id = $1::uuid AND channel = $2::text AND channel_address = $3::text",
            tenant_id, channel, to_address,
        )
        contact_dict = dict(contact_row) if contact_row else None

        ghl_contact_id = _resolve_ghl_contact_id(contact_dict)
        if not ghl_contact_id:
            return {
                "success": False,
                "error": "no_ghl_contact_id",
                "provider": provider,
                "channel": channel,
                "to_address": to_address,
                "raw_response": {
                    "detail": "Could not resolve GHL contactId from contact metadata",
                },
            }

        access_token = await get_valid_token(conn, tenant_id)

    # GHL message type from channel
    msg_type = CHANNEL_TYPE_MAP.get(channel, "SMS")

    body = {
        "type": msg_type,
        "contactId": ghl_contact_id,
        "message": text,
    }

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    url = f"{BASE_URL}/conversations/messages"

    logger.info(json.dumps({
        "event": "ghl_send_message_request",
        "tenant_id": tenant_id,
        "contact_id": ghl_contact_id,
        "channel": channel,
        "type": msg_type,
        "message_id": message_id,
    }))

    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(url, json=body, headers=headers)

        # On 401: refresh token once and retry
        if resp.status_code == 401:
            logger.info(json.dumps({
                "event": "ghl_send_message_401_retry",
                "tenant_id": tenant_id,
                "message_id": message_id,
            }))
            async with pool.acquire() as conn:
                access_token = await get_valid_token(conn, tenant_id)
            headers["Authorization"] = f"Bearer {access_token}"
            resp = await client.post(url, json=body, headers=headers)

    logger.info(json.dumps({
        "event": "ghl_send_message_response",
        "tenant_id": tenant_id,
        "message_id": message_id,
        "status": resp.status_code,
        "body": resp.text[:500],
    }))

    if resp.status_code not in (200, 201):
        return {
            "success": False,
            "error": f"ghl_api_error:{resp.status_code}",
            "provider": provider,
            "channel": channel,
            "to_address": to_address,
            "raw_response": {
                "status": resp.status_code,
                "detail": resp.text[:300],
            },
        }

    data = resp.json()
    provider_msg_id = (
        data.get("messageId")
        or data.get("id")
        or f"ghl-{uuid.uuid4().hex[:16]}"
    )

    return {
        "success": True,
        "provider_msg_id": provider_msg_id,
        "provider": provider,
        "channel": channel,
        "to_address": to_address,
        "raw_response": data,
    }
