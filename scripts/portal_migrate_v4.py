"""
Portal Migration v4 — Rename request_status enum value 'cancelled' → 'closed'.

Run:
  python scripts/portal_migrate_v4.py

Requires DATABASE_URL in .env (doadmin connection string).
No data migration needed — portal has no production data yet.
"""
import asyncio
import os

import asyncpg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]


async def main():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        vals = await conn.fetch(
            """SELECT enumlabel FROM pg_enum
               JOIN pg_type ON pg_type.oid = pg_enum.enumtypid
               WHERE pg_type.typname = 'request_status'
               ORDER BY enumsortorder"""
        )
        existing = [r["enumlabel"] for r in vals]
        print(f"Current request_status values: {existing}")

        if "cancelled" in existing and "closed" not in existing:
            await conn.execute(
                "ALTER TYPE public.request_status RENAME VALUE 'cancelled' TO 'closed'"
            )
            print("  Renamed: cancelled -> closed")
        elif "closed" in existing:
            print("  Already renamed -- 'closed' exists")
        else:
            print("  WARNING: 'cancelled' not found in enum. Nothing to rename.")

        vals = await conn.fetch(
            """SELECT enumlabel FROM pg_enum
               JOIN pg_type ON pg_type.oid = pg_enum.enumtypid
               WHERE pg_type.typname = 'request_status'
               ORDER BY enumsortorder"""
        )
        print(f"Final request_status values: {[r['enumlabel'] for r in vals]}")
        print("\nMigration v4 complete.")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
