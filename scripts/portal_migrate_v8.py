"""
Portal v8 schema migration — run locally with doadmin DATABASE_URL.

Usage:
  python scripts/portal_migrate_v8.py

Adds:
  - portal.doc_requests.created_by_staff_id (UUID, FK to portal.staff_users)
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
        print("Running portal v8 migration...")

        col = "created_by_staff_id"
        exists = await conn.fetchval(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = 'portal' AND table_name = 'doc_requests' "
            "AND column_name = $1", col,
        )
        if not exists:
            await conn.execute(
                "ALTER TABLE portal.doc_requests "
                "ADD COLUMN created_by_staff_id UUID REFERENCES portal.staff_users(id)"
            )
            print(f"  Added portal.doc_requests.{col}")
        else:
            print(f"  portal.doc_requests.{col} already exists")

        print("Portal v8 migration complete.")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(migrate())
