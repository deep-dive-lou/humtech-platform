"""
OAuth routes for financial software connections (Xero, QuickBooks).

One-time setup per client:
  1. Staff clicks "Connect Xero" → redirect to provider auth
  2. Client grants access → provider redirects back with code
  3. We exchange code for tokens → store in core.tenant_credentials

Routes:
  GET  /financial/connect/{provider}?tenant_id=...  → redirect to auth URL
  GET  /financial/callback/{provider}               → exchange code, store tokens
"""
from __future__ import annotations

import base64
import json
import logging
import os
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from ..db import get_pool
from ..utils.crypto import encrypt_credentials

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/financial", tags=["financial"])

# ---------------------------------------------------------------------------
# Provider config
# ---------------------------------------------------------------------------

PROVIDERS = {
    "xero": {
        "auth_url": "https://login.xero.com/identity/connect/authorize",
        "token_url": "https://identity.xero.com/connect/token",
        "connections_url": "https://api.xero.com/connections",
        "scopes": "offline_access accounting.reports.read accounting.settings",
        "client_id_env": "XERO_CLIENT_ID",
        "client_secret_env": "XERO_CLIENT_SECRET",
        "redirect_env": "XERO_REDIRECT_URI",
        "default_redirect": "https://api.humtech.ai/financial/callback/xero",
    },
    "quickbooks": {
        "auth_url": "https://appcenter.intuit.com/connect/oauth2",
        "token_url": "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
        "scopes": "com.intuit.quickbooks.accounting",
        "client_id_env": "QBO_CLIENT_ID",
        "client_secret_env": "QBO_CLIENT_SECRET",
        "redirect_env": "QBO_REDIRECT_URI",
        "default_redirect": "https://api.humtech.ai/financial/callback/quickbooks",
    },
}


def _get_provider_config(provider: str) -> dict:
    if provider not in PROVIDERS:
        raise HTTPException(status_code=400, detail=f"Unknown provider: {provider}")
    cfg = PROVIDERS[provider]
    client_id = os.getenv(cfg["client_id_env"])
    client_secret = os.getenv(cfg["client_secret_env"])
    if not client_id or not client_secret:
        raise HTTPException(
            status_code=500,
            detail=f"{cfg['client_id_env']} and {cfg['client_secret_env']} must be set",
        )
    redirect_uri = os.getenv(cfg["redirect_env"], cfg["default_redirect"])
    return {**cfg, "client_id": client_id, "client_secret": client_secret, "redirect_uri": redirect_uri}


# ---------------------------------------------------------------------------
# Connect — redirect to provider auth
# ---------------------------------------------------------------------------

@router.get("/connect/{provider}")
async def connect(provider: str, tenant_id: str = Query(...)):
    """Redirect to provider OAuth authorization page."""
    cfg = _get_provider_config(provider)

    # Store tenant_id in state param for callback
    state = base64.urlsafe_b64encode(
        json.dumps({"tenant_id": tenant_id, "provider": provider}).encode()
    ).decode()

    params = {
        "response_type": "code",
        "client_id": cfg["client_id"],
        "redirect_uri": cfg["redirect_uri"],
        "scope": cfg["scopes"],
        "state": state,
    }

    auth_url = cfg["auth_url"] + "?" + "&".join(f"{k}={v}" for k, v in params.items())
    return RedirectResponse(url=auth_url)


# ---------------------------------------------------------------------------
# Callback — exchange code for tokens
# ---------------------------------------------------------------------------

@router.get("/callback/{provider}")
async def callback(provider: str, code: str = Query(...), state: str = Query(...), realmId: str = Query(None)):
    """Exchange authorization code for tokens and store credentials."""
    cfg = _get_provider_config(provider)

    # Decode state
    try:
        state_data = json.loads(base64.urlsafe_b64decode(state))
        tenant_id = state_data["tenant_id"]
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid state parameter")

    # Exchange code for tokens
    auth_header = base64.b64encode(
        f"{cfg['client_id']}:{cfg['client_secret']}".encode()
    ).decode()

    token_body = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": cfg["redirect_uri"],
    }

    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(
            cfg["token_url"],
            data=token_body,
            headers={
                "Authorization": f"Basic {auth_header}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )

    if resp.status_code != 200:
        logger.error(json.dumps({
            "event": f"{provider}_oauth_token_exchange_failed",
            "tenant_id": tenant_id,
            "status": resp.status_code,
            "body": resp.text[:500],
        }))
        raise HTTPException(status_code=502, detail=f"Token exchange failed: {resp.status_code}")

    data = resp.json()
    expires_in = int(data.get("expires_in", 3600))
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

    # Build credential blob
    creds = {
        "access_token": data["access_token"],
        "refresh_token": data.get("refresh_token", ""),
        "expires_at": expires_at.isoformat(),
    }

    # Provider-specific: get Xero tenant ID from /connections
    if provider == "xero":
        async with httpx.AsyncClient(timeout=15.0) as client:
            conn_resp = await client.get(
                cfg["connections_url"],
                headers={"Authorization": f"Bearer {creds['access_token']}"},
            )
        if conn_resp.status_code == 200:
            connections = conn_resp.json()
            if connections:
                creds["xero_tenant_id"] = connections[0]["tenantId"]
                logger.info(f"Xero tenant ID: {creds['xero_tenant_id']}")
        else:
            logger.warning(f"Could not fetch Xero connections: {conn_resp.status_code}")

    # QuickBooks: store realmId from callback
    if provider == "quickbooks" and realmId:
        creds["realm_id"] = realmId

    # Store credentials
    encrypted = encrypt_credentials(creds)
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Upsert into core.tenant_credentials
        await conn.execute("""
            INSERT INTO core.tenant_credentials (tenant_id, provider, credentials, updated_at)
            VALUES ($1::uuid, $2, $3::bytea, now())
            ON CONFLICT (tenant_id, provider)
            DO UPDATE SET credentials = EXCLUDED.credentials, updated_at = now()
        """, tenant_id, provider, encrypted)

    logger.info(json.dumps({
        "event": f"{provider}_oauth_connected",
        "tenant_id": tenant_id,
    }))

    return HTMLResponse(content=f"""
    <html>
    <body style="font-family: sans-serif; text-align: center; padding: 50px;">
        <h1>Connected!</h1>
        <p>{provider.title()} has been successfully connected for your account.</p>
        <p>You can close this window.</p>
    </body>
    </html>
    """)
