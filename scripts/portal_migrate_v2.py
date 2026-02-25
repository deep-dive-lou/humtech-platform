"""
Portal v2 schema migration — run locally with doadmin DATABASE_URL.

Usage:
  python scripts/portal_migrate_v2.py

Adds:
  - portal.templates table
  - portal.template_items table
  - brand_color, logo_url, brand_name columns on portal.tenants
  - Grants humtech_bot access to new tables
"""
import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

import asyncpg

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set")
    sys.exit(1)


async def migrate():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        print("Running portal v2 migration...")

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS portal.templates (
                id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                tenant_id   UUID NOT NULL REFERENCES portal.tenants(id),
                name        TEXT NOT NULL,
                description TEXT,
                created_by  UUID REFERENCES portal.staff_users(id),
                is_active   BOOLEAN DEFAULT true,
                created_at  TIMESTAMPTZ DEFAULT now()
            )
        """)
        print("OK portal.templates")

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS portal.template_items (
                id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                template_id  UUID NOT NULL REFERENCES portal.templates(id) ON DELETE CASCADE,
                tenant_id    UUID NOT NULL,
                item_type    public.template_item_type NOT NULL DEFAULT 'file_upload',
                title        TEXT NOT NULL,
                instructions TEXT,
                required     BOOLEAN DEFAULT true,
                sort_order   INT DEFAULT 0
            )
        """)
        print("OK portal.template_items")

        await conn.execute("ALTER TABLE portal.tenants ADD COLUMN IF NOT EXISTS brand_color TEXT")
        await conn.execute("ALTER TABLE portal.tenants ADD COLUMN IF NOT EXISTS logo_url    TEXT")
        await conn.execute("ALTER TABLE portal.tenants ADD COLUMN IF NOT EXISTS brand_name  TEXT")
        print("OK portal.tenants branding columns")

        # Grant access to humtech_bot
        await conn.execute("GRANT SELECT, INSERT, UPDATE ON portal.templates TO humtech_bot")
        await conn.execute("GRANT SELECT, INSERT, UPDATE ON portal.template_items TO humtech_bot")
        await conn.execute("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA portal TO humtech_bot")
        print("OK grants to humtech_bot")

        print("\nMigration complete.")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(migrate())
