"""
Test the morning briefing agent — runs it once and prints output.

Usage:
  .venv/Scripts/python.exe scripts/test_briefing.py [--send]

Without --send: prints the briefing to stdout (no Slack).
With --send: posts to Slack webhook from .env.
"""
import asyncio
import os
import sys
from pathlib import Path

# Ensure app package is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Fix Windows console encoding for emoji
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import asyncpg


async def main():
    send_to_slack = "--send" in sys.argv

    from dotenv import load_dotenv
    load_dotenv()

    url = os.environ.get("DATABASE_URL")
    if not url:
        print("ERROR: DATABASE_URL not set")
        return

    conn = await asyncpg.connect(url)

    try:
        # Import after dotenv so env vars are available
        from app.agents.briefing import run as run_morning_briefing
        from app.agents.slack import SlackReporter

        if send_to_slack:
            webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
            if not webhook_url:
                print("ERROR: SLACK_WEBHOOK_URL not set (needed for --send)")
                return
            slack = SlackReporter(webhook_url)
            print("Sending briefing to Slack...")
        else:
            # Print-only reporter
            class PrintReporter:
                async def post(self, text: str) -> bool:
                    print("=" * 60)
                    print(text)
                    print("=" * 60)
                    return True

                async def post_sequence(self, messages: list[str], delay: float = 0) -> int:
                    for msg in messages:
                        await self.post(msg)
                    return len(messages)

            slack = PrintReporter()
            print("Running briefing in print mode (use --send to post to Slack)\n")

        count = await run_morning_briefing(conn, slack)
        print(f"\nDone. {count} conversations included.")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
