"""
Twilio SMS messaging adapter.

Sends messages via POST https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json
Falls back to stub mode when MESSAGING_STUB env var is set.

Credentials stored in core.tenant_credentials (provider='twilio'):
  { "account_sid": "...", "auth_token": "...", "from_number": "+44..." }
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from typing import Any

import httpx

from app.utils.crypto import decrypt_credentials

logger = logging.getLogger(__name__)

TWILIO_API_BASE = "https://api.twilio.com/2010-04-01"
MESSAGING_STUB_ENABLED_KEY = "MESSAGING_STUB"

LOAD_TWILIO_CREDENTIALS_SQL = """
SELECT credentials
FROM core.tenant_credentials
WHERE tenant_id = $1::uuid
  AND provider = 'twilio';
"""


def _decode_credentials(raw: Any) -> dict[str, Any]:
    """Decrypt the credentials BYTEA column to a dict."""
    if raw is None:
        return {}
    return decrypt_credentials(bytes(raw))


async def _load_twilio_creds(conn: Any, tenant_id: str) -> dict[str, Any]:
    """Load and decrypt Twilio credentials for a tenant."""
    creds: dict[str, Any] = {}
    if conn is not None:
        row = await conn.fetchval(LOAD_TWILIO_CREDENTIALS_SQL, tenant_id)
        creds = _decode_credentials(row)

    if not creds.get("account_sid"):
        # Fallback: env vars (dev/migration path)
        sid = os.getenv("TWILIO_ACCOUNT_SID")
        token = os.getenv("TWILIO_AUTH_TOKEN")
        from_number = os.getenv("TWILIO_FROM_NUMBER")
        if sid and token and from_number:
            return {"account_sid": sid, "auth_token": token, "from_number": from_number}
        raise RuntimeError(f"No Twilio credentials for tenant {tenant_id}")

    return creds


async def send_message(
    *,
    conn: Any,
    tenant_id: str,
    channel: str,
    to_address: str,
    text: str,
    message_id: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Send an SMS via the Twilio Messages API.

    POST https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json
    Auth: HTTP Basic (account_sid:auth_token).

    Falls back to stub mode when MESSAGING_STUB env var is set.
    """
    provider = "twilio"

    # Stub mode for testing
    if os.getenv(MESSAGING_STUB_ENABLED_KEY):
        provider_msg_id = f"twilio-{uuid.uuid4().hex[:16]}"
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

    creds = await _load_twilio_creds(conn, tenant_id)
    account_sid = creds["account_sid"]
    auth_token = creds["auth_token"]
    from_number = creds["from_number"]

    url = f"{TWILIO_API_BASE}/Accounts/{account_sid}/Messages.json"

    # Twilio expects form-encoded body, not JSON
    body = {
        "To": to_address,
        "From": from_number,
        "Body": text,
    }

    logger.info(json.dumps({
        "event": "twilio_send_message_request",
        "tenant_id": tenant_id,
        "channel": channel,
        "to_address": to_address,
        "message_id": message_id,
    }))

    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(
            url,
            data=body,
            auth=(account_sid, auth_token),
        )

    logger.info(json.dumps({
        "event": "twilio_send_message_response",
        "tenant_id": tenant_id,
        "message_id": message_id,
        "status": resp.status_code,
        "body": resp.text[:500],
    }))

    if resp.status_code not in (200, 201):
        return {
            "success": False,
            "error": f"twilio_api_error:{resp.status_code}",
            "provider": provider,
            "channel": channel,
            "to_address": to_address,
            "raw_response": {
                "status": resp.status_code,
                "detail": resp.text[:300],
            },
        }

    data = resp.json()
    provider_msg_id = data.get("sid") or f"twilio-{uuid.uuid4().hex[:16]}"

    return {
        "success": True,
        "provider_msg_id": provider_msg_id,
        "provider": provider,
        "channel": channel,
        "to_address": to_address,
        "raw_response": data,
    }


# ---------------------------------------------------------------------------
# Adapter class -- wraps send_message behind MessagingAdapter protocol
# ---------------------------------------------------------------------------

class TwilioMessagingAdapter:
    """Twilio messaging adapter. Delegates to module-level send_message."""

    def __init__(self, conn: Any, tenant_id: str):
        self.conn = conn
        self.tenant_id = tenant_id

    async def send_message(
        self,
        *,
        channel: str,
        to_address: str,
        text: str,
        message_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await send_message(
            conn=self.conn,
            tenant_id=self.tenant_id,
            channel=channel,
            to_address=to_address,
            text=text,
            message_id=message_id,
            metadata=metadata,
        )
