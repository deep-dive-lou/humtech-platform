"""
Background token refresh for financial adapters (Xero, QuickBooks).

Runs as a worker loop — check every 15 min, refresh tokens expiring within 10 min.
Both providers use rotating refresh tokens (old one invalidated on each refresh).

Called from the runner alongside bot monitor loop.
"""
from __future__ import annotations

import base64
import json
import logging
import os
from datetime import datetime, timedelta, timezone

import asyncpg
import httpx

from ..utils.crypto import decrypt_credentials, encrypt_credentials

logger = logging.getLogger(__name__)

# Check interval: every 15 minutes
CHECK_INTERVAL_SECONDS = 15 * 60

# Refresh if token expires within this window
EXPIRY_BUFFER = timedelta(minutes=10)

# Provider token endpoints
TOKEN_ENDPOINTS = {
    "xero": "https://identity.xero.com/connect/token",
    "quickbooks": "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
}

CLIENT_ENV_KEYS = {
    "xero": ("XERO_CLIENT_ID", "XERO_CLIENT_SECRET"),
    "quickbooks": ("QBO_CLIENT_ID", "QBO_CLIENT_SECRET"),
}


async def refresh_financial_tokens(pool: asyncpg.Pool) -> None:
    """Check all financial credentials and refresh any that are expiring soon."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT tenant_id, provider, credentials
            FROM core.tenant_credentials
            WHERE provider IN ('xero', 'quickbooks')
        """)

    for row in rows:
        tenant_id = str(row["tenant_id"])
        provider = row["provider"]
        raw_creds = row["credentials"]

        if raw_creds is None:
            continue

        try:
            creds = decrypt_credentials(bytes(raw_creds))
        except Exception:
            logger.error(f"Failed to decrypt {provider} credentials for tenant {tenant_id}")
            continue

        # Check if refresh is needed
        expires_at_str = creds.get("expires_at")
        if not expires_at_str:
            continue

        try:
            expires_at = datetime.fromisoformat(expires_at_str)
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            logger.warning(f"Invalid expires_at for {provider} tenant {tenant_id}")
            continue

        if datetime.now(timezone.utc) < (expires_at - EXPIRY_BUFFER):
            continue  # Token is still fresh

        # Token is expiring soon — refresh it
        refresh_token = creds.get("refresh_token")
        if not refresh_token:
            logger.error(f"{provider} token expiring but no refresh_token for tenant {tenant_id}")
            continue

        try:
            new_creds = await _do_refresh(provider, tenant_id, refresh_token, creds)
            # Store updated credentials
            encrypted = encrypt_credentials(new_creds)
            async with pool.acquire() as conn:
                await conn.execute("""
                    UPDATE core.tenant_credentials
                    SET credentials = $1::bytea, updated_at = now()
                    WHERE tenant_id = $2::uuid AND provider = $3
                """, encrypted, tenant_id, provider)

            logger.info(json.dumps({
                "event": f"{provider}_token_refreshed",
                "tenant_id": tenant_id,
                "new_expires_at": new_creds["expires_at"],
            }))
        except Exception as exc:
            logger.error(json.dumps({
                "event": f"{provider}_token_refresh_failed",
                "tenant_id": tenant_id,
                "error": str(exc),
            }))
            # TODO: Slack alert for refresh failures


async def _do_refresh(
    provider: str,
    tenant_id: str,
    refresh_token: str,
    existing_creds: dict,
) -> dict:
    """Call provider token endpoint to refresh access token."""
    token_url = TOKEN_ENDPOINTS[provider]
    id_env, secret_env = CLIENT_ENV_KEYS[provider]

    client_id = os.getenv(id_env)
    client_secret = os.getenv(secret_env)
    if not client_id or not client_secret:
        raise RuntimeError(f"{id_env} and {secret_env} must be set for token refresh")

    auth_header = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()

    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(
            token_url,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            },
            headers={
                "Authorization": f"Basic {auth_header}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )

    if resp.status_code != 200:
        raise RuntimeError(
            f"{provider} token refresh failed: {resp.status_code} {resp.text[:200]}"
        )

    data = resp.json()
    expires_in = int(data.get("expires_in", 3600))
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

    # Preserve existing fields (xero_tenant_id, realm_id, etc.)
    updated = existing_creds.copy()
    updated["access_token"] = data["access_token"]
    updated["refresh_token"] = data.get("refresh_token", refresh_token)
    updated["expires_at"] = expires_at.isoformat()

    return updated
