"""
Re-engagement Flow Test

Tests the full re-engagement pipeline locally:
1. Creates a conversation via /debug/bot/simulate (new_lead → bot replies)
2. Sends one customer reply to build conversation history
3. Backdates last_outbound_at to simulate hours of silence
4. Calls check_reengagement() → verifies stalled detection + job enqueue
5. Processes the reengage job → verifies LLM composes a follow-up
6. Repeats for bumps 2 and 3, verifying each message is different
7. On final bump, verifies conversation is closed

Prerequisites:
    # Start server with stubs:
    CALENDAR_STUB_SLOTS='["2026-03-03T09:00:00Z","2026-03-03T14:00:00Z"]' \
    BOOKING_STUB=1 MESSAGING_STUB=1 \
    python -m uvicorn app.main:app --port 8000

    # Then run:
    python scripts/test_reengage.py
"""
from __future__ import annotations

import asyncio
import json
import os
import random
import sys
from datetime import datetime, timedelta, timezone

import asyncpg
import httpx
from dotenv import load_dotenv

load_dotenv()

# ── Config ──────────────────────────────────────────────────────────

BASE_URL = os.getenv("TEST_BASE_URL", "http://localhost:8000")
TENANT_ID = os.getenv("TEST_TENANT_ID", "63eef4e2-68d1-4fdf-aa67-938c4008bc89")
DATABASE_URL = os.getenv("DATABASE_URL", "")
SIMULATE_ENDPOINT = "/debug/bot/simulate"

# Test contact details (unique per run to avoid collision)
RUN_ID = random.randint(100000, 999999)
TEST_NAME = f"ReengageTest-{RUN_ID}"
TEST_PHONE = f"+4470000{RUN_ID}"

# Re-engagement settings to apply during test
# Must nest under "reengagement" sub-key — get_bot_settings() reads from bot.reengagement.*
REENGAGE_SETTINGS = {
    "reengagement": {
        "enabled": True,
        "max_attempts": 3,
        "intervals_hours": [0.001, 0.001, 0.001],  # near-zero for testing
        "business_hours": {
            "start": "00:00",
            "end": "23:59",
            "days": [0, 1, 2, 3, 4, 5, 6],  # all days
        },
    },
}


# ── Helpers ─────────────────────────────────────────────────────────

class Colours:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    BOLD = "\033[1m"
    RESET = "\033[0m"


def ok(msg: str) -> None:
    print(f"  {Colours.GREEN}PASS{Colours.RESET}  {msg}")


def fail(msg: str) -> None:
    print(f"  {Colours.RED}FAIL{Colours.RESET}  {msg}")


def info(msg: str) -> None:
    print(f"  {Colours.BLUE}INFO{Colours.RESET}  {msg}")


def header(msg: str) -> None:
    print(f"\n{Colours.BOLD}{Colours.YELLOW}{'=' * 60}{Colours.RESET}")
    print(f"{Colours.BOLD}{Colours.YELLOW}  {msg}{Colours.RESET}")
    print(f"{Colours.BOLD}{Colours.YELLOW}{'=' * 60}{Colours.RESET}")


async def simulate(client: httpx.AsyncClient, event_type: str, text: str = "") -> dict:
    """Call the simulate endpoint and return the response."""
    payload = {
        "tenant_id": TENANT_ID,
        "event_type": event_type,
        "display_name": TEST_NAME,
        "channel_address": TEST_PHONE,
        "channel": "sms",
    }
    if text:
        payload["text"] = text
    resp = await client.post(f"{BASE_URL}{SIMULATE_ENDPOINT}", json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


# ── Main test flow ──────────────────────────────────────────────────

async def run_test():
    if not DATABASE_URL:
        print("ERROR: DATABASE_URL not set. Add it to .env or export it.")
        sys.exit(1)

    conn = await asyncpg.connect(DATABASE_URL)
    # Initialize JSON codec so asyncpg can serialize dicts for JSONB columns
    await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")
    client = httpx.AsyncClient()
    passed = 0
    failed_count = 0
    conversation_id = None
    original_settings = None

    try:
        # ── Step 0: Enable re-engagement on the tenant ──────────────
        header("Step 0: Configure tenant for re-engagement")

        # Save original settings so we can restore them
        tenant_row = await conn.fetchrow(
            "SELECT settings FROM core.tenants WHERE tenant_id = $1::uuid", TENANT_ID
        )
        original_settings = tenant_row["settings"] if tenant_row and tenant_row["settings"] else {}
        if isinstance(original_settings, str):
            original_settings = json.loads(original_settings)

        # Merge reengage settings into bot settings
        updated_settings = dict(original_settings)
        bot = dict(updated_settings.get("bot", {}))
        bot.update(REENGAGE_SETTINGS)
        updated_settings["bot"] = bot

        await conn.execute(
            "UPDATE core.tenants SET settings = $1::jsonb WHERE tenant_id = $2::uuid",
            json.dumps(updated_settings), TENANT_ID,
        )
        ok("Re-engagement enabled on tenant (intervals=[0.001h], max_attempts=3, all-hours)")

        # ── Step 1: Create conversation via simulate ────────────────
        header("Step 1: Create conversation (new_lead)")

        result = await simulate(client, "new_lead")
        bot_reply_1 = result.get("bot_reply", "")
        conversation_id = result.get("conversation_id")

        if conversation_id:
            ok(f"Conversation created: {conversation_id[:12]}...")
        else:
            fail("No conversation_id returned from simulate")
            return

        if bot_reply_1:
            info(f"Bot first-touch: {bot_reply_1[:80]}...")
        else:
            fail("Bot returned empty first-touch")
            return

        # ── Step 2: Send one customer reply ─────────────────────────
        header("Step 2: Customer replies (builds conversation history)")

        result = await simulate(client, "inbound_message", text="Yeah sounds good, when are you free?")
        bot_reply_2 = result.get("bot_reply", "")

        if bot_reply_2:
            ok(f"Bot replied: {bot_reply_2[:80]}...")
        else:
            fail("Bot returned empty reply to customer message")
            return

        # ── Step 3: Backdate last_outbound_at to simulate silence ───
        header("Step 3: Simulate lead silence (backdate timestamps)")

        # Move both timestamps to the past, with outbound AFTER inbound (bot spoke last = stalled)
        backdate_inbound = datetime.now(timezone.utc) - timedelta(hours=4)
        backdate_outbound = datetime.now(timezone.utc) - timedelta(hours=3)
        await conn.execute(
            """UPDATE bot.conversations
               SET last_inbound_at = $1, last_outbound_at = $2, updated_at = $2
               WHERE conversation_id = $3::uuid""",
            backdate_inbound, backdate_outbound, conversation_id,
        )
        ok(f"Backdated: inbound={backdate_inbound.strftime('%H:%M:%S')}, outbound={backdate_outbound.strftime('%H:%M:%S')} UTC (bot spoke last, 3h ago)")

        # Verify conversation is in the right state
        conv = await conn.fetchrow(
            "SELECT status, context FROM bot.conversations WHERE conversation_id = $1::uuid",
            conversation_id,
        )
        ctx = conv["context"] if isinstance(conv["context"], dict) else {}
        if conv["status"] == "open" and not ctx.get("booked_booking") and not ctx.get("declined"):
            ok("Conversation is open, not booked, not declined — eligible for re-engagement")
        else:
            fail(f"Conversation not eligible: status={conv['status']}, ctx={ctx}")
            return

        # ── Bump loop (3 bumps) ─────────────────────────────────────
        bump_messages: list[str] = []

        for bump_num in range(1, 4):
            header(f"Step {bump_num + 3}: Re-engagement bump {bump_num}/3")

            # Import here so the server's app modules are available
            sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            from app.bot.reengage import check_reengagement
            from app.bot.processor import process_reengage_job
            from app.bot.jobs import claim_jobs, mark_done

            # 3a. Run check_reengagement
            enqueued = await check_reengagement(conn)

            if enqueued > 0:
                ok(f"check_reengagement() enqueued {enqueued} job(s)")
            else:
                fail(f"check_reengagement() enqueued 0 jobs (expected 1 for bump {bump_num})")
                failed_count += 1
                # Check why it wasn't found
                stalled = await conn.fetch(
                    """SELECT c.conversation_id::text, c.status, c.last_outbound_at, c.last_inbound_at,
                              c.context
                       FROM bot.conversations c
                       WHERE c.conversation_id = $1::uuid""",
                    conversation_id,
                )
                if stalled:
                    r = stalled[0]
                    info(f"  status={r['status']}, last_out={r['last_outbound_at']}, last_in={r['last_inbound_at']}")
                    info(f"  context={r['context']}")
                break

            # 3b. Find the queued job
            job_row = await conn.fetchrow(
                """SELECT job_id::text FROM bot.job_queue
                   WHERE conversation_id = $1::uuid
                     AND job_type = 'reengage'
                     AND status = 'queued'
                   ORDER BY created_at DESC LIMIT 1""",
                conversation_id,
            )
            if not job_row:
                fail("No queued reengage job found in job_queue")
                failed_count += 1
                break

            job_id = job_row["job_id"]
            ok(f"Found reengage job: {job_id[:12]}...")

            # 3c. Process the job (this calls the LLM)
            info("Calling process_reengage_job() (LLM will compose follow-up)...")
            result = await process_reengage_job(conn, job_id)

            if result.get("route") == "reengage":
                ok(f"Job processed — route=reengage, bump={result['bump_number']}")
                passed += 1
            elif result.get("route") == "reengage_skipped":
                fail(f"Job skipped: {result.get('reason')}")
                failed_count += 1
                break
            else:
                fail(f"Unexpected result: {result}")
                failed_count += 1
                break

            # 3d. Check the outbound message
            out_msg = await conn.fetchrow(
                """SELECT text, payload FROM bot.messages
                   WHERE message_id = $1::uuid""",
                result["out_message_id"],
            )
            if out_msg and out_msg["text"]:
                msg_text = out_msg["text"]
                bump_messages.append(msg_text)
                ok(f"LLM composed message ({len(msg_text)} chars): {msg_text[:100]}...")

                # Check SMS formatting rules
                if "\n" in msg_text:
                    fail("Message contains line breaks (violates SMS formatting rules)")
                    failed_count += 1
                elif "\u2014" in msg_text or "\u2013" in msg_text:
                    fail("Message contains em/en dashes")
                    failed_count += 1
                else:
                    ok("SMS formatting OK (no line breaks, no dashes)")
                    passed += 1

                # Check payload metadata
                payload = out_msg["payload"] if isinstance(out_msg["payload"], dict) else {}
                if payload.get("route") == "reengage" and payload.get("bump_number") == bump_num:
                    ok(f"Payload metadata correct (route=reengage, bump={bump_num})")
                    passed += 1
                else:
                    fail(f"Payload metadata wrong: {payload}")
                    failed_count += 1
            else:
                fail("No outbound message text found")
                failed_count += 1
                break

            # 3e. Check context was updated
            conv = await conn.fetchrow(
                "SELECT context, status FROM bot.conversations WHERE conversation_id = $1::uuid",
                conversation_id,
            )
            ctx = conv["context"] if isinstance(conv["context"], dict) else {}
            if ctx.get("reengage_count") == bump_num:
                ok(f"Context updated: reengage_count={bump_num}")
                passed += 1
            else:
                fail(f"Context reengage_count={ctx.get('reengage_count')}, expected {bump_num}")
                failed_count += 1

            # 3f. Check conversation closure on final bump
            if bump_num == 3:
                if conv["status"] == "closed":
                    ok("Conversation closed after max bumps (3/3)")
                    passed += 1
                else:
                    fail(f"Conversation status={conv['status']}, expected 'closed' after max bumps")
                    failed_count += 1
            else:
                if conv["status"] == "open":
                    ok(f"Conversation still open after bump {bump_num} (correct)")
                    passed += 1
                else:
                    fail(f"Conversation status={conv['status']}, expected 'open' after bump {bump_num}")
                    failed_count += 1

            # Mark the job done so it doesn't block next iteration
            await conn.execute(
                "UPDATE bot.job_queue SET status = 'done' WHERE job_id = $1::uuid", job_id
            )

            # Backdate again for next bump (so elapsed_hours > interval)
            # The reengage job just inserted an outbound message, so last_outbound_at is now().
            # We need last_outbound_at > last_inbound_at AND old enough to trigger next bump.
            if bump_num < 3:
                backdate_inbound = datetime.now(timezone.utc) - timedelta(hours=4)
                backdate_outbound = datetime.now(timezone.utc) - timedelta(hours=3)
                await conn.execute(
                    """UPDATE bot.conversations
                       SET last_inbound_at = $1, last_outbound_at = $2, updated_at = $2
                       WHERE conversation_id = $3::uuid""",
                    backdate_inbound, backdate_outbound, conversation_id,
                )

        # ── Step 7: Verify messages are distinct ────────────────────
        header("Step 7: Message quality checks")

        if len(bump_messages) == 3:
            # Check all 3 messages are different
            if len(set(bump_messages)) == 3:
                ok("All 3 bump messages are unique")
                passed += 1
            else:
                fail(f"Duplicate messages detected: {bump_messages}")
                failed_count += 1

            # Check final message has soft close language
            final = bump_messages[2].lower()
            soft_close_markers = ["no worries", "no problem", "totally fine", "all good",
                                  "not the right time", "whenever", "no rush", "that's ok",
                                  "no pressure", "drop me", "let me know"]
            has_soft_close = any(m in final for m in soft_close_markers)
            if has_soft_close:
                ok("Final bump includes soft close language")
                passed += 1
            else:
                info(f"Final bump may lack soft close (check manually): {final[:100]}")
                # Not a hard fail — LLM might phrase it differently

            # Print all messages for visual review
            print(f"\n{Colours.BOLD}  Message Review:{Colours.RESET}")
            for i, msg in enumerate(bump_messages, 1):
                print(f"    Bump {i}: {msg}")
        else:
            fail(f"Expected 3 bump messages, got {len(bump_messages)}")
            failed_count += 1

        # ── Step 8: Verify no extra job is enqueued for closed conversation ─
        header("Step 8: No re-engagement on closed conversation")

        # Try to enqueue again — should find nothing (conversation is closed)
        extra = await check_reengagement(conn)
        if extra == 0:
            ok("No jobs enqueued for closed conversation (correct)")
            passed += 1
        else:
            fail(f"Enqueued {extra} jobs for already-closed conversation")
            failed_count += 1

    except httpx.ConnectError:
        print(f"\n{Colours.RED}ERROR: Cannot connect to {BASE_URL}. Is the server running?{Colours.RESET}")
        print("Start it with:")
        print(f'  CALENDAR_STUB_SLOTS=\'["2026-03-03T09:00:00Z","2026-03-03T14:00:00Z"]\' \\')
        print("  BOOKING_STUB=1 MESSAGING_STUB=1 \\")
        print("  python -m uvicorn app.main:app --port 8000")
        sys.exit(1)
    except Exception as e:
        print(f"\n{Colours.RED}ERROR: {e}{Colours.RESET}")
        import traceback
        traceback.print_exc()
        failed_count += 1
    finally:
        # ── Cleanup ─────────────────────────────────────────────────
        header("Cleanup")

        # Restore original tenant settings
        if original_settings is not None:
            await conn.execute(
                "UPDATE core.tenants SET settings = $1::jsonb WHERE tenant_id = $2::uuid",
                json.dumps(original_settings), TENANT_ID,
            )
            ok("Restored original tenant settings")

        # Clean up test data
        if conversation_id:
            await conn.execute(
                "DELETE FROM bot.job_queue WHERE conversation_id = $1::uuid", conversation_id
            )
            await conn.execute(
                "DELETE FROM bot.messages WHERE conversation_id = $1::uuid", conversation_id
            )
            await conn.execute(
                "DELETE FROM bot.conversations WHERE conversation_id = $1::uuid", conversation_id
            )
            # Clean up the test contact
            await conn.execute(
                "DELETE FROM bot.contacts WHERE channel_address = $1", TEST_PHONE
            )
            ok(f"Cleaned up test conversation + contact ({TEST_PHONE})")

        await conn.close()
        await client.aclose()

        # ── Summary ─────────────────────────────────────────────────
        total = passed + failed_count
        print(f"\n{Colours.BOLD}{'=' * 60}{Colours.RESET}")
        if failed_count == 0:
            print(f"{Colours.GREEN}{Colours.BOLD}  ALL {passed} CHECKS PASSED{Colours.RESET}")
        else:
            print(f"{Colours.RED}{Colours.BOLD}  {failed_count} FAILED{Colours.RESET} / {total} checks")
        print(f"{Colours.BOLD}{'=' * 60}{Colours.RESET}\n")

        sys.exit(1 if failed_count > 0 else 0)


if __name__ == "__main__":
    asyncio.run(run_test())
