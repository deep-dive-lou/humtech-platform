"""
Update HumTech tenant bot settings for evidence-based prompt overhaul.

Changes:
- Remove first_touch_template (let LLM generate first-touch)
- Scrub em dashes from key_objection_responses
- Scrub em dashes from tone (if present)

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


def scrub_em_dashes(text: str) -> str:
    """Replace em dashes with commas or full stops."""
    # Common patterns: " — " -> ", "
    return text.replace(" \u2014 ", ", ").replace("\u2014", ",")


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
        print()

        # 1. Remove first_touch_template
        if "first_touch_template" in bot:
            old = repr(bot["first_touch_template"])[:80]
            del bot["first_touch_template"]
            print(f"  REMOVED bot.first_touch_template (was: {old})")
        else:
            print("  bot.first_touch_template: already absent")

        # 2. Scrub em dashes from key_objection_responses
        objections = bot.get("key_objection_responses", {})
        for key, value in objections.items():
            if "\u2014" in value:
                new_value = scrub_em_dashes(value)
                print(f"  bot.key_objection_responses.{key}:")
                print(f"    OLD: {value}")
                print(f"    NEW: {new_value}")
                objections[key] = new_value
        bot["key_objection_responses"] = objections

        # 3. Scrub em dashes from tone (if present)
        tone = bot.get("tone", "")
        if tone and "\u2014" in tone:
            new_tone = scrub_em_dashes(tone)
            print(f"  bot.tone:")
            print(f"    OLD: {tone}")
            print(f"    NEW: {new_tone}")
            bot["tone"] = new_tone

        settings["bot"] = bot

        # Write back
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
