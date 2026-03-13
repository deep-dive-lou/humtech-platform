"""Update HumTech tenant bot settings with hooks + key_pain_points for prompt overhaul."""
import asyncio, asyncpg, os, sys, json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv

load_dotenv()

DB = os.getenv("DATABASE_URL")
TENANT_SLUG = "humtech"

# New fields to merge into settings.bot
NEW_BOT_FIELDS = {
    "hooks": [
        "Do you know how long it takes your team to respond to a new enquiry on average?",
        "Do you know what percentage of your booked meetings actually show up?",
        "Do you know which of your marketing channels is actually driving revenue, not just leads?",
        "Do you know how many proposals your team sent last month that never got a follow-up?",
        "Do you know your real cost per acquired customer, not just cost per lead?",
    ],
    "key_pain_points": [
        "Leads going cold because response time is too slow",
        "Sales depends on specific people, not systems",
        "Proposals go quiet with no systematic follow-up",
        "No-shows kill the pipeline (no reminders or rebooking)",
        "Ad spend with no visibility on what actually converts",
        "Been burned by agencies who couldn't prove ROI",
        "No real visibility on conversion rates or revenue per lead",
    ],
    # Also update the business description to the simpler "who" framing
    "business_description": (
        "a multidisciplinary team, sales, tech, and marketing, "
        "that works inside your business to find and fix where revenue is leaking. "
        "Evidence-based approach, measured against your own numbers"
    ),
}


async def main():
    conn = await asyncpg.connect(DB)

    # Get current settings
    row = await conn.fetchrow(
        "SELECT tenant_id::text, settings FROM core.tenants WHERE tenant_slug = $1",
        TENANT_SLUG,
    )
    if not row:
        print(f"Tenant '{TENANT_SLUG}' not found")
        return

    tenant_id = row["tenant_id"]
    settings = row["settings"] if isinstance(row["settings"], dict) else json.loads(row["settings"])

    bot = settings.get("bot", {})

    print(f"Tenant: {TENANT_SLUG} ({tenant_id})")
    print(f"Current bot keys: {list(bot.keys())}")

    # Merge new fields
    for key, value in NEW_BOT_FIELDS.items():
        old = bot.get(key)
        bot[key] = value
        if old:
            print(f"  Updated: {key}")
        else:
            print(f"  Added:   {key}")

    settings["bot"] = bot

    # Write back
    await conn.execute(
        "UPDATE core.tenants SET settings = $1::jsonb WHERE tenant_id = $2::uuid",
        json.dumps(settings),
        tenant_id,
    )

    print("\nDone. Verify with:")
    print(f"  SELECT settings->'bot'->'hooks' FROM core.tenants WHERE tenant_slug = '{TENANT_SLUG}';")

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
