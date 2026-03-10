"""
Portal v9 schema migration — run locally with doadmin DATABASE_URL.

Usage:
  python scripts/portal_migrate_v9.py

Adds:
  - portal.email_sends.html_body (TEXT, nullable)
  - portal.email_sends.email_type (TEXT, NOT NULL, default 'magic_link')
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
        print("Running portal v9 migration...")

        # 1. html_body column
        exists = await conn.fetchval(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = 'portal' AND table_name = 'email_sends' "
            "AND column_name = 'html_body'",
        )
        if not exists:
            await conn.execute(
                "ALTER TABLE portal.email_sends ADD COLUMN html_body TEXT"
            )
            print("  Added portal.email_sends.html_body")
        else:
            print("  portal.email_sends.html_body already exists")

        # 2. email_type column
        exists = await conn.fetchval(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = 'portal' AND table_name = 'email_sends' "
            "AND column_name = 'email_type'",
        )
        if not exists:
            await conn.execute(
                "ALTER TABLE portal.email_sends "
                "ADD COLUMN email_type TEXT NOT NULL DEFAULT 'magic_link'"
            )
            print("  Added portal.email_sends.email_type")
        else:
            print("  portal.email_sends.email_type already exists")

        print("Portal v9 migration complete.")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(migrate())
