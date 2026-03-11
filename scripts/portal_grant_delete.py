"""
One-time: grant DELETE on portal.* tables to humtech_bot,
and set default privileges so future tables auto-grant full CRUD.
Run locally with .env loaded.
"""
import asyncio
import os
import asyncpg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"].replace("humtech_bot", "doadmin")


async def main():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        # Catch up: grant DELETE on all existing portal tables
        await conn.execute("GRANT DELETE ON ALL TABLES IN SCHEMA portal TO humtech_bot")
        print("Granted DELETE on all existing portal tables")

        # Prevent regression: future tables created by doadmin auto-grant full CRUD
        await conn.execute(
            "ALTER DEFAULT PRIVILEGES FOR ROLE doadmin IN SCHEMA portal "
            "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO humtech_bot"
        )
        print("Set default table privileges (SELECT, INSERT, UPDATE, DELETE)")

        # Also cover sequences for any future SERIAL/GENERATED columns
        await conn.execute(
            "ALTER DEFAULT PRIVILEGES FOR ROLE doadmin IN SCHEMA portal "
            "GRANT USAGE, SELECT ON SEQUENCES TO humtech_bot"
        )
        print("Set default sequence privileges (USAGE, SELECT)")

        print("\nDone. humtech_bot now has DELETE on portal.* and future tables auto-grant full CRUD.")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
