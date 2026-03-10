"""
Store Twilio credentials for a tenant.

Env vars:
  TWILIO_ACCOUNT_SID   — Twilio Account SID
  TWILIO_AUTH_TOKEN     — Twilio Auth Token
  TWILIO_FROM_NUMBER    — Twilio phone number (e.g. +44...)
  TENANT_SLUG           — tenant slug (default: humtech)
  DATABASE_URL          — Postgres connection string

Usage: run locally with doadmin DATABASE_URL, or inside container.

  TWILIO_ACCOUNT_SID=AC... TWILIO_AUTH_TOKEN=... TWILIO_FROM_NUMBER=+44... \
    python scripts/store_twilio_creds.py
"""
import asyncio
import os
import sys

import asyncpg

from app.utils.crypto import encrypt_credentials

DB = os.getenv("DATABASE_URL")
TENANT_SLUG = os.getenv("TENANT_SLUG", "humtech")


async def main():
    account_sid = os.getenv("TWILIO_ACCOUNT_SID", "").strip()
    auth_token = os.getenv("TWILIO_AUTH_TOKEN", "").strip()
    from_number = os.getenv("TWILIO_FROM_NUMBER", "").strip()

    if not account_sid or not auth_token or not from_number:
        print("ERROR: Set TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, and TWILIO_FROM_NUMBER")
        sys.exit(1)

    conn = await asyncpg.connect(DB)

    tenant_id = await conn.fetchval(
        "SELECT tenant_id::text FROM core.tenants WHERE tenant_slug = $1",
        TENANT_SLUG,
    )
    if not tenant_id:
        print(f"ERROR: tenant '{TENANT_SLUG}' not found.")
        await conn.close()
        sys.exit(1)

    creds = {
        "account_sid": account_sid,
        "auth_token": auth_token,
        "from_number": from_number,
    }
    encrypted = encrypt_credentials(creds)

    await conn.execute("""
        INSERT INTO core.tenant_credentials (tenant_id, provider, credentials, updated_at)
        VALUES ($1::uuid, 'twilio', $2::bytea, now())
        ON CONFLICT (tenant_id, provider)
        DO UPDATE SET credentials = EXCLUDED.credentials, updated_at = now()
    """, tenant_id, encrypted)

    print(f"Twilio credentials stored for tenant '{TENANT_SLUG}' (from_number={from_number})")
    await conn.close()


asyncio.run(main())
