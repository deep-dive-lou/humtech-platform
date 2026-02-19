"""
GHL OAuth2 token management.

Loads credentials from core.tenant_credentials, checks expiry,
refreshes when needed, and stores the updated blob (Fernet-encrypted).
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Any

import asyncpg
import httpx

from app.utils.crypto import encrypt_credentials, decrypt_credentials

logger = logging.getLogger(__name__)

GHL_TOKEN_URL = "https://services.leadconnectorhq.com/oauth/token"

# Refresh if token expires within this window
EXPIRY_BUFFER = timedelta(minutes=5)

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

LOAD_GHL_CREDENTIALS_SQL = """
SELECT credentials
FROM core.tenant_credentials
WHERE tenant_id = $1::uuid
  AND provider = 'ghl';
"""

UPDATE_GHL_CREDENTIALS_SQL = """
UPDATE core.tenant_credentials
SET credentials = $2::bytea,
    updated_at = now()
WHERE tenant_id = $1::uuid
  AND provider = 'ghl';
"""

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _decode_credentials(raw: Any) -> dict[str, Any]:
    """Decrypt the credentials BYTEA column to a dict."""
    if raw is None:
        return {}
    return decrypt_credentials(bytes(raw))


def _is_expired(creds: dict[str, Any]) -> bool:
    """True if access_token is missing or expires within EXPIRY_BUFFER."""
    expires_at_str = creds.get("expires_at")
    if not expires_at_str:
        return True
    try:
        expires_at = datetime.fromisoformat(expires_at_str)
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) >= (expires_at - EXPIRY_BUFFER)
    except (ValueError, TypeError):
        return True


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def get_valid_token(conn: asyncpg.Connection, tenant_id: str) -> str:
    """
    Return a valid GHL access_token for the tenant.

    Loads credentials from core.tenant_credentials (Fernet-encrypted).
    If the token is expired or within 5 minutes of expiry, refreshes it
    and stores the updated credentials back.

    Falls back to GHL_ACCESS_TOKEN env var if no DB credentials exist
    (migration/dev path — no refresh possible).
    """
    row = await conn.fetchval(LOAD_GHL_CREDENTIALS_SQL, tenant_id)
    creds = _decode_credentials(row)

    # Fallback: env var (dev/migration path — cannot refresh)
    if not creds.get("access_token"):
        env_token = os.getenv("GHL_ACCESS_TOKEN")
        if env_token:
            return env_token
        raise RuntimeError(f"No GHL credentials for tenant {tenant_id}")

    if not _is_expired(creds):
        return creds["access_token"]

    # Token is expired or about to expire — refresh
    refresh_token = creds.get("refresh_token")
    if not refresh_token:
        raise RuntimeError(
            f"GHL token expired and no refresh_token for tenant {tenant_id}"
        )

    new_creds = await refresh_ghl_token(conn, tenant_id, refresh_token, creds)
    return new_creds["access_token"]


async def refresh_ghl_token(
    conn: asyncpg.Connection,
    tenant_id: str,
    refresh_token: str,
    existing_creds: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Call GHL OAuth2 token refresh endpoint and store updated credentials.

    Returns the new credential dict:
    {
        "access_token": "...",
        "refresh_token": "...",
        "expires_at": "2026-02-18T12:00:00+00:00",
        "location_id": "..."
    }
    """
    client_id = os.getenv("GHL_CLIENT_ID")
    client_secret = os.getenv("GHL_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError("GHL_CLIENT_ID and GHL_CLIENT_SECRET must be set")

    request_body = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }

    logger.info(json.dumps({
        "event": "ghl_token_refresh_request",
        "tenant_id": tenant_id,
    }))

    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(
            GHL_TOKEN_URL,
            data=request_body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

    if resp.status_code != 200:
        logger.error(json.dumps({
            "event": "ghl_token_refresh_failed",
            "tenant_id": tenant_id,
            "status": resp.status_code,
            "body": resp.text[:500],
        }))
        raise RuntimeError(
            f"GHL token refresh failed: {resp.status_code} {resp.text[:200]}"
        )

    data = resp.json()

    # Build updated credential blob, preserving fields like location_id
    base = existing_creds.copy() if existing_creds else {}
    expires_in = int(data.get("expires_in", 86400))
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

    base["access_token"] = data["access_token"]
    base["refresh_token"] = data.get("refresh_token", refresh_token)
    base["expires_at"] = expires_at.isoformat()

    logger.info(json.dumps({
        "event": "ghl_token_refresh_success",
        "tenant_id": tenant_id,
        "expires_at": base["expires_at"],
    }))

    # Encrypt and store
    encrypted = encrypt_credentials(base)
    await conn.execute(UPDATE_GHL_CREDENTIALS_SQL, tenant_id, encrypted)

    return base
