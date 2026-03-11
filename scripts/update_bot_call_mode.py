"""
Add call_mode to HumTech tenant bot settings.
Run locally with doadmin URL from .env.
"""
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
TENANT_SLUG = "humtech"


async def main():
    import asyncpg

    conn = await asyncpg.connect(DATABASE_URL)
    try:
        row = await conn.fetchrow(
            "SELECT tenant_id::text, settings FROM core.tenants WHERE tenant_slug = $1",
            TENANT_SLUG,
        )
        if not row:
            print(f"ERROR: tenant '{TENANT_SLUG}' not found")
            return

        tenant_id = row["tenant_id"]
        settings = json.loads(row["settings"]) if row["settings"] else {}
        bot = settings.get("bot", {})

        print(f"Tenant: {TENANT_SLUG} ({tenant_id})")

        old = bot.get("call_mode")
        bot["call_mode"] = "phone call"
        print(f"  bot.call_mode: {repr(old)} -> 'phone call'")

        settings["bot"] = bot

        await conn.execute(
            "UPDATE core.tenants SET settings = $1::jsonb WHERE tenant_id = $2::uuid",
            json.dumps(settings),
            tenant_id,
        )
        print("\nDone -- settings updated.")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
