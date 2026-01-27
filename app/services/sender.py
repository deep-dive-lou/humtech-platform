from __future__ import annotations
from datetime import datetime, timedelta
import json
from typing import Any
import asyncpg

from app.adapters.messaging.ghl import send_message


# Run once to create idempotency index:
# CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS
#   idx_messages_outbound_provider_msg_id
#   ON bot.messages (provider, provider_msg_id)
#   WHERE direction = 'outbound' AND provider_msg_id IS NOT NULL;

# Retry configuration
MAX_SEND_ATTEMPTS = 6
# Backoff schedule in seconds: 30s, 2m, 10m, 30m, 2h, 2h (capped)
BACKOFF_SECONDS = [30, 120, 600, 1800, 7200, 7200]


def _get_backoff_seconds(attempt: int) -> int:
    """Get backoff delay in seconds for given attempt number (1-indexed)."""
    idx = min(attempt - 1, len(BACKOFF_SECONDS) - 1)
    return BACKOFF_SECONDS[idx] if idx >= 0 else BACKOFF_SECONDS[0]


CLAIM_PENDING_OUTBOUND_SQL = """
WITH cte AS (
  SELECT message_id
  FROM bot.messages
  WHERE direction = 'outbound'
    AND (
      -- Pending messages (never sent)
      COALESCE(payload->>'send_status', '') = 'pending'
      OR
      -- Failed messages ready for retry
      (
        payload->>'send_status' = 'failed'
        AND (payload->>'send_next_at')::timestamptz <= now()
      )
    )
  ORDER BY created_at ASC
  LIMIT $1
  FOR UPDATE SKIP LOCKED
)
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
JOIN cte ON cte.message_id = m.message_id
JOIN bot.contacts c ON c.contact_id = m.contact_id;
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
WHERE message_id = $1::uuid
  AND direction = 'outbound';
"""

# $2 = new_status ('failed' or 'dead')
# $3 = new_attempts count
# $4 = send_next_at (ISO string or null)
# $5 = error message
MARK_OUTBOUND_FAILED_SQL = """
UPDATE bot.messages
SET payload = payload
    || jsonb_build_object('send_status', $2::text)
    || jsonb_build_object('send_attempts', $3::int)
    || jsonb_build_object('send_next_at', $4::text)
    || jsonb_build_object('send_last_error', $5::text)
WHERE message_id = $1::uuid
  AND direction = 'outbound';
"""


async def send_pending_outbound(conn: asyncpg.Connection, limit: int) -> dict[str, Any]:
    """Claim and send pending outbound messages with retry/backoff."""
    from zoneinfo import ZoneInfo

    tz = ZoneInfo("Europe/London")

    # Lock + fetch messages to send (pending or failed ready for retry)
    rows = await conn.fetch(CLAIM_PENDING_OUTBOUND_SQL, limit)

    sent = 0
    failed = 0
    dead = 0

    for r in rows:
        mid = r["message_id"]
        payload = r["payload"] if isinstance(r["payload"], dict) else {}
        current_attempts = int(payload.get("send_attempts", 0) or 0)

        try:
            # Call messaging adapter
            result = await send_message(
                tenant_id=r["tenant_id"],
                provider=r["provider"],
                channel=r["channel"],
                to_address=r["channel_address"],
                text=r["text"] or "",
                message_id=mid,
            )

            if result.get("success"):
                # Mark as sent with provider_msg_id
                now = datetime.now(tz)
                provider_response_json = json.dumps(result.get("raw_response", {}), ensure_ascii=False)
                await conn.execute(
                    MARK_OUTBOUND_SENT_SQL,
                    mid,
                    result.get("provider_msg_id"),
                    now.isoformat(),
                    provider_response_json,
                )
                sent += 1
            else:
                # Provider returned failure
                error_msg = result.get("error", "Unknown provider error")
                await _mark_failed_with_backoff(conn, mid, current_attempts, error_msg, tz)
                if current_attempts + 1 >= MAX_SEND_ATTEMPTS:
                    dead += 1
                else:
                    failed += 1

        except Exception as e:
            # Exception during send
            await _mark_failed_with_backoff(conn, mid, current_attempts, str(e), tz)
            if current_attempts + 1 >= MAX_SEND_ATTEMPTS:
                dead += 1
            else:
                failed += 1

    return {"selected": len(rows), "sent": sent, "failed": failed, "dead": dead}


async def _mark_failed_with_backoff(
    conn: asyncpg.Connection,
    message_id: str,
    current_attempts: int,
    error_msg: str,
    tz: Any,
) -> None:
    """Mark message as failed with exponential backoff, or dead if max attempts reached."""
    new_attempts = current_attempts + 1

    if new_attempts >= MAX_SEND_ATTEMPTS:
        # Max attempts reached - mark as dead (no more retries)
        await conn.execute(
            MARK_OUTBOUND_FAILED_SQL,
            message_id,
            "dead",
            new_attempts,
            None,  # no send_next_at for dead messages
            error_msg,
        )
    else:
        # Schedule retry with exponential backoff
        backoff_secs = _get_backoff_seconds(new_attempts)
        next_at = datetime.now(tz) + timedelta(seconds=backoff_secs)
        await conn.execute(
            MARK_OUTBOUND_FAILED_SQL,
            message_id,
            "failed",
            new_attempts,
            next_at.isoformat(),
            error_msg,
        )
