"""Store GHL location_id in tenant credentials."""
import asyncio, asyncpg, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.utils.crypto import encrypt_credentials, decrypt_credentials

DB = os.getenv("DATABASE_URL")
TENANT_SLUG = "humtech"
LOCATION_ID = "V7m7dlgTERFjn0t4soSv"


async def main():
    conn = await asyncpg.connect(DB)

    tenant_id = await conn.fetchval(
        "SELECT tenant_id::text FROM core.tenants WHERE tenant_slug = $1", TENANT_SLUG
    )
    row = await conn.fetchval(
        "SELECT credentials FROM core.tenant_credentials WHERE tenant_id=$1::uuid AND provider='ghl'",
        tenant_id
    )
    creds = decrypt_credentials(bytes(row))
    creds["location_id"] = LOCATION_ID
    encrypted = encrypt_credentials(creds)

    await conn.execute("""
        INSERT INTO core.tenant_credentials (tenant_id, provider, credentials, updated_at)
        VALUES ($1::uuid, 'ghl', $2::bytea, now())
        ON CONFLICT (tenant_id, provider)
        DO UPDATE SET credentials = EXCLUDED.credentials, updated_at = now()
    """, tenant_id, encrypted)

    print(f"Done. Keys stored: {list(creds.keys())}")
    await conn.close()


asyncio.run(main())