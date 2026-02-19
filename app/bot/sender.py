from __future__ import annotations
from datetime import datetime, timedelta
from typing import Any
import asyncpg
import uuid

from app.adapters.messaging.ghl import send_message
from app.bot.tenants import load_tenant, get_messaging_settings


# Run once to create idempotency index:
# CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS
#   idx_messages_outbound_provider_msg_id
#   ON bot.messages (provider, provider_msg_id)
#   WHERE direction = 'outbound' AND provider_msg_id IS NOT NULL;

# Retry configuration
MAX_SEND_ATTEMPTS = 3
# Backoff schedule in seconds: 30s, 2m, 10m
BACKOFF_SECONDS = [30, 120, 600]


def _get_backoff_seconds(attempt: int) -> int:
    """Get backoff delay in seconds for given attempt number (1-indexed)."""
    idx = min(attempt - 1, len(BACKOFF_SECONDS) - 1)
    return BACKOFF_SECONDS[idx] if idx >= 0 else BACKOFF_SECONDS[0]


# Step 1: Atomically claim messages by setting send_status='sending'
# Only claims messages where send_status='pending' AND (no send_next_at OR send_next_at <= now)
CLAIM_PENDING_OUTBOUND_SQL = """
WITH candidates AS (
  SELECT message_id
  FROM bot.messages
  WHERE direction = 'outbound'
    AND payload->>'send_status' = 'pending'
    AND (
      payload->>'send_next_at' IS NULL
      OR (payload->>'send_next_at')::timestamptz <= now()
    )
  ORDER BY created_at ASC
  LIMIT $1
  FOR UPDATE SKIP LOCKED
)
UPDATE bot.messages m
SET payload = m.payload || '{"send_status": "sending"}'::jsonb
FROM candidates c
WHERE m.message_id = c.message_id
  AND m.payload->>'send_status' = 'pending'  -- Double-check for safety
RETURNING m.message_id::text AS message_id;
"""

# Step 2: Fetch full data for claimed messages (now in 'sending' state)
FETCH_SENDING_MESSAGES_SQL = """
SELECT m.message_id::text AS message_id,
       m.tenant_id::text AS tenant_id,
       m.conversation_id::text AS conversation_id,
       m.contact_id::text AS contact_id,
       m.provider,
       m.channel,
       m.text,
       m.payload,
       c.channel_address
FROM bot.messages m
JOIN bot.contacts c ON c.contact_id = m.contact_id
WHERE m.message_id = ANY($1::uuid[])
  AND m.direction = 'outbound'
  AND m.payload->>'send_status' = 'sending';
"""

MARK_OUTBOUND_SENT_SQL = """
UPDATE bot.messages
SET
  provider_msg_id = $2::text,
  payload = payload
    || jsonb_build_object('send_status', 'sent')
    || jsonb_build_object('sent_at', $3::text)
    || jsonb_build_object('provider_response', $4::jsonb)
    || jsonb_build_object('send_last_error', null)
    || jsonb_build_object('send_next_at', null)
    || jsonb_build_object('send_trace', $5::jsonb)
WHERE message_id = $1::uuid
  AND direction = 'outbound'
  AND payload->>'send_status' = 'sending';
"""

UPDATE_CONVERSATION_LAST_OUTBOUND_SQL = """
UPDATE bot.conversations
SET last_outbound_at = now(), updated_at = now()
WHERE conversation_id = $1::uuid;
"""

# $2 = new_status ('pending' for retry, 'failed' for max attempts reached)
# $3 = new_attempts count
# $4 = send_next_at (ISO string or null)
# $5 = error message
# $6 = send_trace object
MARK_OUTBOUND_FAILED_SQL = """
UPDATE bot.messages
SET payload = payload
    || jsonb_build_object('send_status', $2::text)
    || jsonb_build_object('send_attempts', $3::int)
    || jsonb_build_object('send_next_at', $4::text)
    || jsonb_build_object('send_last_error', $5::text)
    || jsonb_build_object('send_trace', $6::jsonb)
WHERE message_id = $1::uuid
  AND direction = 'outbound'
  AND payload->>'send_status' = 'sending';
"""


async def send_pending_outbound(conn: asyncpg.Connection, limit: int) -> dict[str, Any]:
    """Claim and send pending outbound messages with retry/backoff and dry-run support.

    Idempotency:
    - Only claims messages where send_status='pending'
    - Atomically transitions to 'sending' before processing
    - Guards all updates with send_status='sending' check
    """
    from zoneinfo import ZoneInfo

    tz = ZoneInfo("Europe/London")

    # Step 1: Atomically claim messages (pending -> sending)
    claimed_rows = await conn.fetch(CLAIM_PENDING_OUTBOUND_SQL, limit)
    if not claimed_rows:
        return {"selected": 0, "sent": 0, "failed": 0, "skipped": 0, "dry_run_count": 0}

    claimed_ids = [r["message_id"] for r in claimed_rows]

    # Step 2: Fetch full data for claimed messages
    rows = await conn.fetch(FETCH_SENDING_MESSAGES_SQL, claimed_ids)

    sent = 0
    failed = 0
    skipped = 0
    dry_run_count = 0

    # Cache tenant settings to avoid repeated lookups
    tenant_cache: dict[str, dict[str, Any]] = {}

    for r in rows:
        mid = r["message_id"]
        tenant_id = r["tenant_id"]
        conversation_id = r["conversation_id"]
        payload = r["payload"] if isinstance(r["payload"], dict) else {}
        current_attempts = int(payload.get("send_attempts", 0) or 0)
        now = datetime.now(tz)
        attempted_at = now.isoformat()

        # Guard: skip if not in 'sending' state (already processed)
        if payload.get("send_status") != "sending":
            skipped += 1
            continue

        # Load tenant settings (cached)
        if tenant_id not in tenant_cache:
            try:
                tenant = await load_tenant(conn, tenant_id)
                tenant_cache[tenant_id] = get_messaging_settings(tenant)
            except Exception:
                tenant_cache[tenant_id] = {"dry_run": False, "provider": None}

        messaging_settings = tenant_cache[tenant_id]
        is_dry_run = messaging_settings.get("dry_run", False)

        try:
            if is_dry_run:
                # DRY-RUN MODE: Skip external API, simulate success
                msg_id = f"dryrun-{uuid.uuid4().hex[:16]}"
                provider_response = {
                    "dry_run": True,
                    "status": "sent",
                    "message_id": msg_id,
                }
                send_trace = {
                    "ok": True,
                    "dry_run": True,
                    "attempted_at": attempted_at,
                    "reason": None,
                }

                await conn.execute(
                    MARK_OUTBOUND_SENT_SQL,
                    mid,
                    msg_id,
                    attempted_at,
                    provider_response,
                    send_trace,
                )
                await conn.execute(UPDATE_CONVERSATION_LAST_OUTBOUND_SQL, conversation_id)
                sent += 1
                dry_run_count += 1

            else:
                # LIVE MODE: Call messaging adapter
                result = await send_message(
                    tenant_id=tenant_id,
                    provider=r["provider"],
                    channel=r["channel"],
                    to_address=r["channel_address"],
                    text=r["text"] or "",
                    message_id=mid,
                )

                if result.get("success"):
                    provider_msg_id = result.get("provider_msg_id", "")
                    raw_response = result.get("raw_response", {})

                    # Detect stub/dry-run: adapter stub OR tenant dry_run setting
                    # "No real external send happened" = dry_run
                    adapter_is_stub = raw_response.get("stub", False) is True
                    effective_dry_run = adapter_is_stub or is_dry_run

                    provider_response = {
                        "dry_run": effective_dry_run,
                        "status": "sent",
                        "message_id": provider_msg_id,
                        "raw": raw_response,
                    }
                    send_trace = {
                        "ok": True,
                        "dry_run": effective_dry_run,
                        "attempted_at": attempted_at,
                        "reason": None,
                    }
                    await conn.execute(
                        MARK_OUTBOUND_SENT_SQL,
                        mid,
                        provider_msg_id,
                        attempted_at,
                        provider_response,
                        send_trace,
                    )
                    await conn.execute(UPDATE_CONVERSATION_LAST_OUTBOUND_SQL, conversation_id)
                    sent += 1
                    if effective_dry_run:
                        dry_run_count += 1
                else:
                    # Provider returned failure
                    error_msg = result.get("error", "Unknown provider error")
                    send_trace = {
                        "ok": False,
                        "dry_run": is_dry_run,
                        "attempted_at": attempted_at,
                        "reason": error_msg,
                    }
                    await _mark_failed_with_backoff(conn, mid, current_attempts, error_msg, tz, send_trace)
                    failed += 1

        except Exception as e:
            error_msg = str(e)
            send_trace = {
                "ok": False,
                "dry_run": is_dry_run,
                "attempted_at": attempted_at,
                "reason": error_msg,
            }
            await _mark_failed_with_backoff(conn, mid, current_attempts, error_msg, tz, send_trace)
            failed += 1

    return {
        "selected": len(claimed_ids),
        "sent": sent,
        "failed": failed,
        "skipped": skipped,
        "dry_run_count": dry_run_count,
    }


async def _mark_failed_with_backoff(
    conn: asyncpg.Connection,
    message_id: str,
    current_attempts: int,
    error_msg: str,
    tz: Any,
    send_trace: dict[str, Any],
) -> None:
    """Mark message for retry (pending) or permanently failed after max attempts."""
    new_attempts = current_attempts + 1

    if new_attempts >= MAX_SEND_ATTEMPTS:
        # Max attempts reached - mark as failed (no more retries)
        await conn.execute(
            MARK_OUTBOUND_FAILED_SQL,
            message_id,
            "failed",
            new_attempts,
            None,  # no send_next_at for permanently failed
            error_msg,
            send_trace,
        )
    else:
        # Schedule retry: back to 'pending' with send_next_at for backoff
        backoff_secs = _get_backoff_seconds(new_attempts)
        next_at = datetime.now(tz) + timedelta(seconds=backoff_secs)
        await conn.execute(
            MARK_OUTBOUND_FAILED_SQL,
            message_id,
            "pending",
            new_attempts,
            next_at.isoformat(),
            error_msg,
            send_trace,
        )
