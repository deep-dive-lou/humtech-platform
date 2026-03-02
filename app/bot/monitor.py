"""
Bot conversation monitor — periodic health checks with Slack alerts.

Runs as a third loop in the runner. Checks for:
1. High turn count (>6 inbound turns, no booking)
2. Wants human (unactioned for >15 min)
3. Failed sends (permanent delivery failures)
4. Repeated unclear (3+ consecutive unclear intents)
5. Stalled conversations (no reply >2 hours after inbound)
6. Job queue backup (jobs stuck past run_after)
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# ── Dedup state ──────────────────────────────────────────────────────
# In-memory: {alert_key: last_alerted_at}. Resets on container restart.
_alerted: dict[str, datetime] = {}
DEDUP_WINDOW = timedelta(hours=1)


def _should_alert(key: str) -> bool:
    now = datetime.now(timezone.utc)
    last = _alerted.get(key)
    if last and (now - last) < DEDUP_WINDOW:
        return False
    _alerted[key] = now
    return True


# ── Alert queries ────────────────────────────────────────────────────

HIGH_TURNS_SQL = """
WITH turn_counts AS (
    SELECT conversation_id, COUNT(*) AS inbound_turns
    FROM bot.messages WHERE direction = 'inbound'
    GROUP BY conversation_id
    HAVING COUNT(*) > 6
)
SELECT c.conversation_id::text, ct.display_name, ct.channel_address,
       c.last_intent, c.created_at, tc.inbound_turns
FROM bot.conversations c
JOIN bot.contacts ct ON ct.contact_id = c.contact_id
JOIN turn_counts tc ON tc.conversation_id = c.conversation_id
WHERE c.status = 'open'
  AND c.context->'booked_booking'->>'slot' IS NULL;
"""

WANTS_HUMAN_SQL = """
SELECT c.conversation_id::text, ct.display_name, ct.channel_address,
       c.updated_at
FROM bot.conversations c
JOIN bot.contacts ct ON ct.contact_id = c.contact_id
WHERE c.status = 'open'
  AND c.last_intent = 'wants_human'
  AND c.updated_at < now() - interval '15 minutes';
"""

FAILED_SENDS_SQL = """
SELECT m.message_id::text, m.conversation_id::text, ct.display_name,
       m.payload->>'send_last_error' AS error, m.created_at
FROM bot.messages m
JOIN bot.contacts ct ON ct.contact_id = m.contact_id
WHERE m.direction = 'outbound'
  AND m.payload->>'send_status' = 'failed'
  AND m.created_at > now() - interval '1 hour';
"""

REPEATED_UNCLEAR_SQL = """
WITH ranked AS (
    SELECT m.conversation_id, m.payload->>'intent' AS intent,
           ROW_NUMBER() OVER (PARTITION BY m.conversation_id ORDER BY m.created_at DESC) AS rn
    FROM bot.messages m
    WHERE m.direction = 'inbound' AND m.payload->>'intent' IS NOT NULL
)
SELECT r.conversation_id::text, ct.display_name, ct.channel_address
FROM ranked r
JOIN bot.conversations c ON c.conversation_id = r.conversation_id
JOIN bot.contacts ct ON ct.contact_id = c.contact_id
WHERE r.rn <= 3 AND c.status = 'open'
GROUP BY r.conversation_id, ct.display_name, ct.channel_address
HAVING COUNT(*) FILTER (WHERE r.intent = 'unclear') = 3;
"""

STALLED_CONVOS_SQL = """
SELECT c.conversation_id::text, ct.display_name, ct.channel_address,
       c.last_inbound_at, c.last_outbound_at
FROM bot.conversations c
JOIN bot.contacts ct ON ct.contact_id = c.contact_id
WHERE c.status = 'open'
  AND c.last_inbound_at IS NOT NULL
  AND c.last_inbound_at < now() - interval '2 hours'
  AND (c.last_outbound_at IS NULL OR c.last_outbound_at < c.last_inbound_at);
"""

JOB_QUEUE_BACKUP_SQL = """
SELECT COUNT(*) AS stuck_count
FROM bot.job_queue
WHERE status = 'queued'
  AND run_after < now() - interval '5 minutes';
"""


# ── Check runners ────────────────────────────────────────────────────

async def _check_high_turns(conn) -> list[dict]:
    rows = await conn.fetch(HIGH_TURNS_SQL)
    alerts = []
    for r in rows:
        key = f"high_turns:{r['conversation_id']}"
        if _should_alert(key):
            alerts.append({
                "name": r["display_name"] or r["channel_address"],
                "turns": r["inbound_turns"],
                "intent": r["last_intent"],
            })
    return alerts


async def _check_wants_human(conn) -> list[dict]:
    rows = await conn.fetch(WANTS_HUMAN_SQL)
    alerts = []
    for r in rows:
        key = f"wants_human:{r['conversation_id']}"
        if _should_alert(key):
            mins = int((datetime.now(timezone.utc) - r["updated_at"].replace(tzinfo=timezone.utc)).total_seconds() / 60)
            alerts.append({
                "name": r["display_name"] or r["channel_address"],
                "waiting_mins": mins,
            })
    return alerts


async def _check_failed_sends(conn) -> list[dict]:
    rows = await conn.fetch(FAILED_SENDS_SQL)
    alerts = []
    for r in rows:
        key = f"failed_send:{r['message_id']}"
        if _should_alert(key):
            alerts.append({
                "name": r["display_name"] or "unknown",
                "error": (r["error"] or "unknown")[:100],
            })
    return alerts


async def _check_repeated_unclear(conn) -> list[dict]:
    rows = await conn.fetch(REPEATED_UNCLEAR_SQL)
    alerts = []
    for r in rows:
        key = f"unclear:{r['conversation_id']}"
        if _should_alert(key):
            alerts.append({
                "name": r["display_name"] or r["channel_address"],
            })
    return alerts


async def _check_stalled(conn) -> list[dict]:
    rows = await conn.fetch(STALLED_CONVOS_SQL)
    alerts = []
    for r in rows:
        key = f"stalled:{r['conversation_id']}"
        if _should_alert(key):
            hrs = round((datetime.now(timezone.utc) - r["last_inbound_at"].replace(tzinfo=timezone.utc)).total_seconds() / 3600, 1)
            alerts.append({
                "name": r["display_name"] or r["channel_address"],
                "hours_waiting": hrs,
            })
    return alerts


async def _check_job_backup(conn) -> list[dict]:
    row = await conn.fetchrow(JOB_QUEUE_BACKUP_SQL)
    count = row["stuck_count"] if row else 0
    if count > 0 and _should_alert("job_backup:global"):
        return [{"stuck_count": count}]
    return []


# ── Slack posting ────────────────────────────────────────────────────

MAX_ITEMS_PER_SECTION = 5


def _build_slack_message(sections: dict[str, list[dict]]) -> str:
    """Build a plain mrkdwn message from alert sections."""
    label_map = {
        "high_turns": ":warning: *High Turn Count (no booking)*",
        "wants_human": ":raised_hand: *Wants Human (unactioned)*",
        "failed_sends": ":x: *Failed Message Sends*",
        "repeated_unclear": ":question: *Repeated Unclear Intents*",
        "stalled": ":hourglass: *Stalled Conversations*",
        "job_backup": ":rotating_light: *Job Queue Backup*",
    }

    parts: list[str] = []
    for section_key, items in sections.items():
        if not items:
            continue
        label = label_map.get(section_key, section_key)
        lines = [label]
        shown = items[:MAX_ITEMS_PER_SECTION]
        overflow = len(items) - len(shown)

        for item in shown:
            if section_key == "high_turns":
                lines.append(f"  - {item['name']} -- {item['turns']} turns, last intent: {item['intent']}")
            elif section_key == "wants_human":
                lines.append(f"  - {item['name']} -- waiting {item['waiting_mins']} min")
            elif section_key == "failed_sends":
                lines.append(f"  - {item['name']} -- {item['error']}")
            elif section_key == "repeated_unclear":
                lines.append(f"  - {item['name']} -- 3+ unclear in a row")
            elif section_key == "stalled":
                lines.append(f"  - {item['name']} -- {item['hours_waiting']}h since last message")
            elif section_key == "job_backup":
                lines.append(f"  - {item['stuck_count']} jobs stuck in queue")

        if overflow > 0:
            lines.append(f"  _...and {overflow} more_")

        parts.append("\n".join(lines))

    return "\n\n".join(parts)


async def _post_to_slack(webhook_url: str, text: str) -> None:
    payload = {"text": text}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(webhook_url, json=payload)
            if resp.status_code != 200:
                logger.warning("Slack webhook returned %d: %s", resp.status_code, resp.text[:200])
    except Exception as e:
        logger.error("Failed to post to Slack: %s", e)


# ── Main entry point ─────────────────────────────────────────────────

async def run_monitor_check(conn, slack_webhook_url: str) -> int:
    """Run all alert checks and post to Slack. Returns total alert count."""
    sections: dict[str, list[dict]] = {}

    sections["high_turns"] = await _check_high_turns(conn)
    sections["wants_human"] = await _check_wants_human(conn)
    sections["failed_sends"] = await _check_failed_sends(conn)
    sections["repeated_unclear"] = await _check_repeated_unclear(conn)
    sections["stalled"] = await _check_stalled(conn)
    sections["job_backup"] = await _check_job_backup(conn)

    total = sum(len(v) for v in sections.values())

    if total > 0:
        text = _build_slack_message(sections)
        await _post_to_slack(slack_webhook_url, text)
        logger.info("Monitor: %d alerts posted to Slack", total)
    else:
        logger.debug("Monitor: all clear")

    return total
