"""Reset today's personalisation rows so the pipeline can re-generate them."""
import asyncio
import os
import asyncpg

async def main():
    conn = await asyncpg.connect(os.environ["DATABASE_URL"])
    deleted = await conn.execute(
        "DELETE FROM outreach.personalisation WHERE created_at::date = CURRENT_DATE"
    )
    print(f"Deleted personalisation rows: {deleted}")
    updated = await conn.execute(
        "UPDATE outreach.leads SET status = 'enriched' "
        "WHERE status = 'personalised' AND updated_at::date = CURRENT_DATE"
    )
    print(f"Reset leads to enriched: {updated}")
    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())