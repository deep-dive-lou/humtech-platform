"""
Re-engagement checker — finds stalled conversations and enqueues follow-up jobs.

Runs as a periodic loop alongside the monitor. Checks for open conversations
where the bot replied but the lead went silent, and enqueues `reengage` jobs
with appropriate timing and business hours awareness.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Any

import asyncpg

from app.bot.tenants import load_tenant, get_bot_settings, get_booking_config

logger = logging.getLogger(__name__)


# ── SQL ─────────────────────────────────────────────────────────────

STALLED_CONVERSATIONS_SQL = """
SELECT c.conversation_id::text, c.tenant_id::text, c.contact_id::text,
       c.context, c.last_inbound_at, c.last_outbound_at,
       ct.display_name, ct.channel_address, ct.channel
FROM bot.conversations c
JOIN bot.contacts ct ON ct.contact_id = c.contact_id
WHERE c.status = 'open'
  AND c.last_outbound_at IS NOT NULL
  AND c.last_outbound_at > c.last_inbound_at
  AND c.context->>'declined' IS NULL
  AND c.context->>'booked_booking' IS NULL
  AND EXISTS (
    SELECT 1 FROM bot.messages m
    WHERE m.conversation_id = c.conversation_id
      AND m.direction = 'inbound'
  )
  AND NOT EXISTS (
    SELECT 1 FROM bot.job_queue jq
    WHERE jq.conversation_id = c.conversation_id
      AND jq.job_type = 'reengage'
      AND jq.status IN ('queued', 'running')
  )
ORDER BY c.last_outbound_at ASC
LIMIT 50;
"""

INSERT_REENGAGE_JOB_SQL = """
INSERT INTO bot.job_queue (tenant_id, job_type, conversation_id, status, run_after)
VALUES ($1::uuid, 'reengage', $2::uuid, 'queued', now())
ON CONFLICT DO NOTHING
RETURNING job_id::text;
"""


# ── Business hours ──────────────────────────────────────────────────

def is_within_business_hours(
    tz_name: str,
    hours_config: dict[str, Any] | None = None,
) -> bool:
    """Check if the current time is within business hours for the given timezone.

    Default: 09:00–18:00, Monday–Friday.

    hours_config format:
        {"start": "09:00", "end": "18:00", "days": [0, 1, 2, 3, 4]}
        days: 0=Monday, 6=Sunday
    """
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("Europe/London")

    now_local = datetime.now(tz)

    config = hours_config or {}
    start_str = config.get("start", "09:00")
    end_str = config.get("end", "18:00")
    allowed_days = config.get("days", [0, 1, 2, 3, 4])  # Mon-Fri

    if now_local.weekday() not in allowed_days:
        return False

    start_h, start_m = (int(x) for x in start_str.split(":"))
    end_h, end_m = (int(x) for x in end_str.split(":"))

    start_minutes = start_h * 60 + start_m
    end_minutes = end_h * 60 + end_m
    now_minutes = now_local.hour * 60 + now_local.minute

    return start_minutes <= now_minutes < end_minutes


# ── Main checker ────────────────────────────────────────────────────

async def check_reengagement(conn: asyncpg.Connection) -> int:
    """Find stalled conversations and enqueue re-engagement jobs.

    Returns the number of jobs enqueued.
    """
    rows = await conn.fetch(STALLED_CONVERSATIONS_SQL)
    if not rows:
        return 0

    enqueued = 0
    # Cache tenant settings to avoid repeated DB lookups
    tenant_cache: dict[str, dict] = {}

    for row in rows:
        tenant_id = row["tenant_id"]
        conversation_id = row["conversation_id"]
        context = row["context"] if isinstance(row["context"], dict) else {}

        # Load tenant settings (cached)
        if tenant_id not in tenant_cache:
            try:
                tenant = await load_tenant(conn, tenant_id)
                tenant_cache[tenant_id] = tenant
            except Exception as e:
                logger.warning("reengage: failed to load tenant %s: %s", tenant_id, e)
                continue
        tenant = tenant_cache[tenant_id]

        bot_settings = get_bot_settings(tenant)
        booking_config = get_booking_config(tenant)

        # Check if re-engagement is enabled for this tenant
        if not bot_settings.get("reengagement_enabled", False):
            continue

        # Check bump count
        reengage_count = context.get("reengage_count", 0)
        max_attempts = bot_settings.get("reengagement_max_attempts", 3)
        if reengage_count >= max_attempts:
            continue

        # Get the interval for this bump number
        intervals = bot_settings.get("reengagement_intervals_hours")
        if not intervals:
            # Fallback to single delay_hours
            delay_hours = bot_settings.get("reengagement_delay_hours", 6)
            intervals = [delay_hours]

        # Use the interval for the current bump, or the last one if we've exceeded the list
        interval_idx = min(reengage_count, len(intervals) - 1)
        required_hours = intervals[interval_idx]

        # Check elapsed time since last outbound
        last_outbound = row["last_outbound_at"]
        if last_outbound.tzinfo is None:
            last_outbound = last_outbound.replace(tzinfo=timezone.utc)
        elapsed_hours = (datetime.now(timezone.utc) - last_outbound).total_seconds() / 3600

        if elapsed_hours < required_hours:
            continue

        # Check business hours
        tz_name = booking_config.get("timezone", "Europe/London")
        bh_config = bot_settings.get("reengagement_business_hours")
        if not is_within_business_hours(tz_name, bh_config):
            continue

        # Enqueue
        try:
            job_id = await conn.fetchval(INSERT_REENGAGE_JOB_SQL, tenant_id, conversation_id)
            if job_id:
                enqueued += 1
                logger.info(
                    "reengage: enqueued job %s for conversation %s (bump %d)",
                    job_id, conversation_id, reengage_count + 1,
                )
        except Exception as e:
            logger.warning("reengage: failed to enqueue for %s: %s", conversation_id, e)

    return enqueued
