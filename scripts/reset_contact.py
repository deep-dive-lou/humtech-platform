"""Reset a contact's conversation so the bot treats them as a fresh lead."""
import asyncio, asyncpg, os

DB = os.getenv("DATABASE_URL")
PHONE = "+447915262257"

async def main():
    conn = await asyncpg.connect(DB)

    # Clear context (removes lead_touchpoint so new_lead re-sends)
    # and reopen conversation if closed
    result = await conn.execute("""
        UPDATE bot.conversations
        SET context = '{}'::jsonb,
            status = 'open',
            updated_at = now()
        WHERE contact_id = (
            SELECT contact_id FROM bot.contacts
            WHERE channel_address = $1
            LIMIT 1
        )
    """, PHONE)
    print(f"Reset conversation: {result}")

    await conn.close()

asyncio.run(main())