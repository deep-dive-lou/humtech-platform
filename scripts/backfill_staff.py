"""Backfill created_by_staff_id on existing doc_requests where it's NULL."""
import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()


async def main():
    conn = await asyncpg.connect(os.getenv("DATABASE_URL"))

    admin_id = await conn.fetchval(
        "SELECT id FROM portal.staff_users WHERE role = 'admin' AND is_active = true LIMIT 1"
    )
    print(f"Admin: {admin_id}")

    result = await conn.execute(
        "UPDATE portal.doc_requests SET created_by_staff_id = $1 WHERE created_by_staff_id IS NULL",
        admin_id,
    )
    print(f"Result: {result}")
    await conn.close()


asyncio.run(main())
