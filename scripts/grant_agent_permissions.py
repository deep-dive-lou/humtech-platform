"""
Grant humtech_bot SELECT on tables needed by the morning briefing agent.

Run once with doadmin credentials:
  .venv/Scripts/python.exe scripts/grant_agent_permissions.py

Idempotent — safe to re-run.
"""
import asyncio
import os

import asyncpg


GRANTS = [
    # Engine
    "GRANT USAGE ON SCHEMA engine TO humtech_bot",
    "GRANT SELECT ON engine.leads TO humtech_bot",
    "GRANT SELECT ON engine.lead_events TO humtech_bot",
    # Outreach
    "GRANT USAGE ON SCHEMA outreach TO humtech_bot",
    "GRANT SELECT ON outreach.leads TO humtech_bot",
    "GRANT SELECT ON outreach.personalisation TO humtech_bot",
    # Optimiser
    "GRANT USAGE ON SCHEMA optimiser TO humtech_bot",
    "GRANT SELECT ON optimiser.experiments TO humtech_bot",
    "GRANT SELECT ON optimiser.daily_stats TO humtech_bot",
    # Portal (may already have access — safe to re-grant)
    "GRANT USAGE ON SCHEMA portal TO humtech_bot",
    "GRANT SELECT ON portal.doc_requests TO humtech_bot",
    "GRANT SELECT ON portal.doc_request_items TO humtech_bot",
    "GRANT SELECT ON portal.clients TO humtech_bot",
    # Monitoring views (should already have access from migration 004)
    "GRANT USAGE ON SCHEMA monitoring TO humtech_bot",
    "GRANT SELECT ON ALL TABLES IN SCHEMA monitoring TO humtech_bot",
]


async def main():
    url = os.environ.get("DATABASE_URL")
    if not url:
        from dotenv import load_dotenv
        load_dotenv()
        url = os.environ.get("DATABASE_URL")

    if not url:
        print("ERROR: DATABASE_URL not set")
        return

    conn = await asyncpg.connect(url)
    try:
        for grant in GRANTS:
            try:
                await conn.execute(grant)
                print(f"  OK: {grant}")
            except Exception as e:
                print(f"  WARN: {grant} — {e}")
    finally:
        await conn.close()

    print("\nDone. All grants applied.")


if __name__ == "__main__":
    asyncio.run(main())
