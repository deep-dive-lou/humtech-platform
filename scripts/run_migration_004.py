"""Run migration 004: create monitoring views."""
import asyncio
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

SQL_FILE = Path(__file__).parent / "migrations" / "004_monitoring_views.sql"

async def main():
    import asyncpg
    # Use doadmin URL for schema/grant operations
    url = os.environ["DATABASE_URL"].replace("humtech_bot", "doadmin")
    conn = await asyncpg.connect(url)
    try:
        sql = SQL_FILE.read_text()
        await conn.execute(sql)
        print("Migration 004 applied successfully")

        # Verify views exist
        rows = await conn.fetch(
            "SELECT table_name FROM information_schema.views "
            "WHERE table_schema = 'monitoring' ORDER BY table_name"
        )
        print(f"Views created: {[r['table_name'] for r in rows]}")
    finally:
        await conn.close()

asyncio.run(main())
