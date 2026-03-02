"""
Store or refresh a GHL token for a tenant.

Two modes:
  1) Re-stamp existing token with a 1-year expiry (if token is still valid):
       python3 store_ghl_token.py

  2) Store a brand new token:
       GHL_ACCESS_TOKEN=<new_token> python3 store_ghl_token.py

Usage: run inside the humtech_api container.
"""
import asyncio
import asyncpg
import os
import sys
from datetime import datetime, timezone, timedelta

from app.utils.crypto import encrypt_credentials, decrypt_credentials

DB = os.getenv("DATABASE_URL")
TENANT_SLUG = "humtech"
EXPIRY_DAYS = 365


async def main():
    conn = await asyncpg.connect(DB)

    tenant_id = await conn.fetchval(
        "SELECT tenant_id::text FROM core.tenants WHERE tenant_slug = $1",
        TENANT_SLUG,
    )
    if not tenant_id:
        print(f"ERROR: tenant '{TENANT_SLUG}' not found.")
        await conn.close()
        sys.exit(1)

    new_token = os.getenv("GHL_ACCESS_TOKEN", "").strip()

    if not new_token:
        # Mode 1: re-stamp existing token
        row = await conn.fetchval(
            "SELECT credentials FROM core.tenant_credentials "
            "WHERE tenant_id = $1::uuid AND provider = 'ghl'",
            tenant_id,
        )
        if not row:
            print("ERROR: No existing credentials found. Set GHL_ACCESS_TOKEN to provide a new token.")
            await conn.close()
            sys.exit(1)
        creds = decrypt_credentials(bytes(row))
        if not creds.get("access_token"):
            print("ERROR: Existing credentials have no access_token. Set GHL_ACCESS_TOKEN to provide a new token.")
            await conn.close()
            sys.exit(1)
        print(f"Re-stamping existing token with {EXPIRY_DAYS}-day expiry...")
    else:
        # Mode 2: store new token
        creds = {}
        creds["access_token"] = new_token
        print(f"Storing new token with {EXPIRY_DAYS}-day expiry...")

    expires_at = (datetime.now(timezone.utc) + timedelta(days=EXPIRY_DAYS)).isoformat()
    creds["expires_at"] = expires_at
    # Remove stale refresh_token if any (Private Integration doesn't use it)
    creds.pop("refresh_token", None)

    encrypted = encrypt_credentials(creds)

    await conn.execute("""
        INSERT INTO core.tenant_credentials (tenant_id, provider, credentials, updated_at)
        VALUES ($1::uuid, 'ghl', $2::bytea, now())
        ON CONFLICT (tenant_id, provider)
        DO UPDATE SET credentials = EXCLUDED.credentials, updated_at = now()
    """, tenant_id, encrypted)

    print(f"Done. Token valid until: {expires_at}")
    await conn.close()


asyncio.run(main())
