from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Tuple, Optional
import asyncpg
import json
import os
import re

from app.config import settings  # ensures dotenv is loaded
from app.adapters.calendar.ghl import (
    get_free_slots,
    filter_slots_by_signals,
    filter_by_availability_windows,
    format_slots_for_display,
    pick_soonest_two_slots,
    book_slot,
)
from app.bot.routing import route_from_text, compose_reply, route_info_to_dict
from app.bot.tenants import (
    load_tenant,
    load_tenant_credentials,
    get_calendar_settings,
    get_booking_config,
    get_llm_settings,
)
from app.bot.llm import rewrite_outbound_text_llm, classify_confirmation_intent_llm
from app.bot.trace_logger import log_processing_run, build_debug_snapshot
from app.engine.events import resolve_or_create_lead, write_lead_event

# Offer expiry: 2 hours
OFFER_EXPIRY_HOURS = 2

# First-touch message template (no LLM, deterministic)
FIRST_TOUCH_TEMPLATE = (
    "Hey{name_part} — thanks for reaching out. "
    "Want to get you booked in quickly. "
    "What day suits you best, and is morning or afternoon better?"
)

# YES/NO detection patterns
YES_PATTERNS = {"yes", "yep", "yeah", "yup", "sure", "confirm", "ok", "okay", "y", "affirmative", "absolutely", "definitely"}
NO_PATTERNS = {"no", "nope", "nah", "cancel", "n", "negative", "different", "another", "change"}

# Confirmation phrases for auto-confirm during slot selection
CONFIRM_PHRASES = {
    "book it", "book me", "perfect", "great", "sounds good",
    "let's do", "i'll take", "that works", "that's great", "please book",
    "go ahead", "lock it in", "that one", "i want"
}

def _coerce_payload(payload_raw) -> dict:
    if payload_raw is None:
        return {}
    if isinstance(payload_raw, dict):
        return payload_raw
    if isinstance(payload_raw, list):
        # Unwrap single-element list containing a dict
        if payload_raw and isinstance(payload_raw[-1], dict):
            return payload_raw[-1]
        print(f"DEBUG _coerce_payload: unexpected list without dict, returning {{}}")
        return {}
    if isinstance(payload_raw, str):
        s = payload_raw.strip()
        if not s:
            return {}
        try:
            parsed = json.loads(s)
            # json.loads might return a list; unwrap if needed
            if isinstance(parsed, list):
                if parsed and isinstance(parsed[-1], dict):
                    return parsed[-1]
                print(f"DEBUG _coerce_payload: parsed list without dict, returning {{}}")
                return {}
            if isinstance(parsed, dict):
                return parsed
            print(f"DEBUG _coerce_payload: parsed non-dict type {type(parsed).__name__}, returning {{}}")
            return {}
        except json.JSONDecodeError:
            print(f"DEBUG _coerce_payload: JSONDecodeError, returning {{}}")
            return {}
    # asyncpg sometimes returns Record-like mappings; try dict()
    try:
        return dict(payload_raw)
    except Exception:
        print(f"DEBUG _coerce_payload: unexpected type {type(payload_raw).__name__}, returning {{}}")
        return {}

@dataclass
class InboundEvent:
    inbound_event_id: str
    tenant_id: str
    provider: str
    provider_msg_id: Optional[str]
    channel: str
    channel_address: str
    dedupe_key: str
    event_type: str
    payload: dict[str, Any]
    trace_id: str


LOAD_JOB_EVENT_SQL = """
SELECT
  jq.job_id::text,
  jq.tenant_id::text,
  jq.job_type,
  jq.inbound_event_id::text,
  ie.provider,
  ie.event_type,
  ie.provider_msg_id,
  ie.channel,
  ie.channel_address,
  ie.dedupe_key,
  ie.payload,
  COALESCE(jq.trace_id, ie.trace_id)::text AS trace_id
FROM bot.job_queue jq
JOIN bot.inbound_events ie ON ie.inbound_event_id = jq.inbound_event_id
WHERE jq.job_id = $1::uuid;
"""

UPSERT_CONTACT_SQL = """
INSERT INTO bot.contacts (
  tenant_id, channel, channel_address, display_name, metadata, created_at, updated_at
)
VALUES (
  $1::uuid, $2::text, $3::text, $4::text, COALESCE($5::jsonb, '{}'::jsonb), now(), now()
)
ON CONFLICT (tenant_id, channel, channel_address)
DO UPDATE SET
  display_name = COALESCE(EXCLUDED.display_name, bot.contacts.display_name),
  metadata = bot.contacts.metadata || EXCLUDED.metadata,
  updated_at = now()
RETURNING contact_id::text;
"""

# one OPEN per contact enforced by UNIQUE(tenant_id, contact_id, status)
UPSERT_OPEN_CONVERSATION_SQL = """
INSERT INTO bot.conversations (
  tenant_id, contact_id, status, last_step, last_intent, context,
  last_inbound_at, created_at, updated_at
)
VALUES (
  $1::uuid, $2::uuid, 'open', 'start', NULL, '{}'::jsonb,
  now(), now(), now()
)
ON CONFLICT (tenant_id, contact_id, status)
DO UPDATE SET
  last_inbound_at = now(),
  updated_at = now()
RETURNING conversation_id::text;
"""

# Idempotent insert using either provider_msg_id (best) or dedupe_key (fallback).
# We store inbound_event_id + dedupe_key into payload so we can also inspect later.
# $12 = trace_id (propagated from inbound_event)
INSERT_INBOUND_MESSAGE_IDEMPOTENT_SQL = """
WITH existing AS (
  SELECT m.message_id::text AS message_id
  FROM bot.messages m
  WHERE m.tenant_id = $1::uuid
    AND m.direction = 'inbound'
    AND (
      ($6::text IS NOT NULL AND m.provider = $5::text AND m.provider_msg_id = $6::text)
      OR
      ($6::text IS NULL AND (m.payload->>'dedupe_key') = $8::text)
    )
  LIMIT 1
),
ins AS (
  INSERT INTO bot.messages (
    tenant_id, conversation_id, contact_id,
    direction, provider, provider_msg_id, channel, text, payload, created_at, trace_id
  )
  SELECT
    $1::uuid, $2::uuid, $3::uuid,
    'inbound', $5::text, $6::text, $7::text, $4::text,
    COALESCE($9::jsonb, '{}'::jsonb) || jsonb_build_object(
      'inbound_event_id', $10::text,
      'dedupe_key', $8::text,
      'event_type', $11::text
    ),
    now(),
    $12::uuid
  WHERE NOT EXISTS (SELECT 1 FROM existing)
  RETURNING message_id::text AS message_id
)
SELECT COALESCE((SELECT message_id FROM ins), (SELECT message_id FROM existing)) AS message_id;
"""

UPDATE_CONVERSATION_LAST_INBOUND_SQL = """
UPDATE bot.conversations
SET last_inbound_at = now(), updated_at = now()
WHERE conversation_id = $1::uuid;
"""

INSERT_OUTBOUND_MESSAGE_SQL = """
INSERT INTO bot.messages (
  tenant_id, conversation_id, contact_id,
  direction, provider, channel, text, payload, created_at, trace_id
)
VALUES (
  $1::uuid, $2::uuid, $3::uuid,
  'outbound', $4::text, $5::text, $6::text, $7::jsonb, now(), $8::uuid
)
RETURNING message_id::text;
"""

UPDATE_CONVERSATION_CONTEXT_SQL = """
UPDATE bot.conversations
SET context = context || $2::jsonb, updated_at = now()
WHERE conversation_id = $1::uuid;
"""

LOAD_CONVERSATION_CONTEXT_SQL = """
SELECT context FROM bot.conversations WHERE conversation_id = $1::uuid;
"""


def _extract_text(payload: dict[str, Any]) -> str:
    """
    Best-effort extraction. Adjust to your actual webhook schema.
    """
    # Common patterns
    if isinstance(payload.get("text"), str):
        return payload["text"]
    if isinstance(payload.get("message"), str):
        return payload["message"]
    if isinstance(payload.get("body"), str):
        return payload["body"]
    # Nested guess
    msg = payload.get("message")
    if isinstance(msg, dict) and isinstance(msg.get("text"), str):
        return msg["text"]
    return ""


def _extract_display_name(payload: dict[str, Any]) -> Optional[str]:
    for k in ("display_name", "name", "full_name"):
        v = payload.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


# Ordinal patterns: "first", "1st", "the first one", "2nd", "second", etc.
ORDINAL_PATTERNS = [
    (r"\b(1st|first|one)\b", 0),
    (r"\b(2nd|second|two)\b", 1),
    (r"\b(3rd|third|three)\b", 2),
    (r"\b(4th|fourth|four)\b", 3),
    (r"\b(5th|fifth|five)\b", 4),
    (r"\b(6th|sixth|six)\b", 5),
]

# Time patterns: "9:15", "09:15", "915", "9.15", "9am", "9:15am"
TIME_PATTERN = re.compile(
    r"\b(\d{1,2})[:.]?(\d{2})?\s*(am|pm)?\b",
    re.IGNORECASE,
)


def _match_slot_by_ordinal(text: str, slots: list[str]) -> Optional[str]:
    """Match user text to slot by ordinal reference."""
    t = text.lower().strip()
    for pattern, index in ORDINAL_PATTERNS:
        if re.search(pattern, t) and index < len(slots):
            return slots[index]
    return None


def _match_slot_by_time(text: str, slots: list[str]) -> Optional[str]:
    """Match user text to slot by time reference."""
    t = text.lower().strip()
    match = TIME_PATTERN.search(t)
    if not match:
        return None

    hour = int(match.group(1))
    minutes = int(match.group(2)) if match.group(2) else 0
    ampm = (match.group(3) or "").lower()

    # Convert to 24-hour if am/pm specified
    if ampm == "pm" and hour < 12:
        hour += 12
    elif ampm == "am" and hour == 12:
        hour = 0

    # Find matching slot
    for slot_iso in slots:
        slot_dt = datetime.fromisoformat(slot_iso)
        if slot_dt.hour == hour and slot_dt.minute == minutes:
            return slot_iso

    return None


def _match_slot_by_digit(text: str, slots: list[str]) -> Optional[str]:
    """Match exact '1' or '2' input to slot index."""
    t = text.strip()
    if t == "1" and len(slots) >= 1:
        return slots[0]
    if t == "2" and len(slots) >= 2:
        return slots[1]
    return None


def _match_slot_from_text(text: str, slots: list[str]) -> Optional[str]:
    """Try to match user text to one of the offered slots."""
    # Try exact digit first ("1", "2")
    matched = _match_slot_by_digit(text, slots)
    if matched:
        return matched

    # Try ordinal ("the first one", "2nd")
    matched = _match_slot_by_ordinal(text, slots)
    if matched:
        return matched

    # Try time match ("9:15", "9am")
    matched = _match_slot_by_time(text, slots)
    if matched:
        return matched

    return None


async def _handle_new_lead(
    conn: asyncpg.Connection,
    ev: InboundEvent,
    contact_id: str,
    conversation_id: str,
    conv_context: dict[str, Any],
    display_name: Optional[str],
) -> dict[str, Any]:
    """
    Handle new_lead event: send first-touch message, set lead_touchpoint.
    Idempotent: if lead_touchpoint already exists, do nothing.
    """
    from zoneinfo import ZoneInfo

    tz = ZoneInfo("Europe/London")
    now = datetime.now(tz)

    # Idempotency: if lead_touchpoint already exists, skip
    existing_touchpoint = conv_context.get("lead_touchpoint")
    if existing_touchpoint and isinstance(existing_touchpoint, dict):
        # Already processed - return existing data
        return {
            "job_id": None,  # Will be filled by caller
            "tenant_id": ev.tenant_id,
            "inbound_event_id": ev.inbound_event_id,
            "contact_id": contact_id,
            "conversation_id": conversation_id,
            "message_id": None,
            "out_message_id": existing_touchpoint.get("message_id"),
            "route": "new_lead",
            "slot_matched": None,
            "booking_id": None,
            "idempotent_skip": True,
        }

    # Build first-touch message (no LLM)
    name_part = f" {display_name}" if display_name else ""
    out_text = FIRST_TOUCH_TEMPLATE.format(name_part=name_part)

    # Create outbound message
    out_payload_dict: dict[str, Any] = {
        "send_status": "pending",
        "send_attempts": 0,
        "send_last_error": None,
        "route": "new_lead",
        "text_template": out_text,
        "text_final": out_text,
        "event_type": "new_lead",
        "llm": {"enabled": False, "used": False},
    }

    out_message_id = await conn.fetchval(
        INSERT_OUTBOUND_MESSAGE_SQL,
        ev.tenant_id,
        conversation_id,
        contact_id,
        ev.provider,
        ev.channel,
        out_text,
        out_payload_dict,
        ev.trace_id,  # $8 - propagate trace_id
    )

    # Glass-box: debug snapshot for new_lead
    debug_snapshot = build_debug_snapshot(
        route="new_lead",
        signals={},
        slot_count=0,
        chosen_slots=None,
        transition={"from": "start", "to": "new_lead"},
    )

    # Set lead_touchpoint and debug snapshot in context
    lead_touchpoint = {
        "first_touch_at": now.isoformat(),
        "channel": ev.channel,
        "message_id": out_message_id,
    }
    context_updates = {
        "lead_touchpoint": lead_touchpoint,
        "debug": {"last_run": debug_snapshot},
        "_last_step": "new_lead",
    }
    await conn.execute(UPDATE_CONVERSATION_CONTEXT_SQL, conversation_id, context_updates)

    # Glass-box: structured logging for new_lead
    log_processing_run(
        tenant_slug=ev.tenant_id,  # Best-effort, no extra lookup
        contact_id=contact_id,
        conversation_id=conversation_id,
        trace_id=ev.trace_id,
        route="new_lead",
        signals={},
        state_transition={"from": "start", "to": "new_lead"},
    )

    return {
        "job_id": None,  # Will be filled by caller
        "tenant_id": ev.tenant_id,
        "inbound_event_id": ev.inbound_event_id,
        "contact_id": contact_id,
        "conversation_id": conversation_id,
        "message_id": None,
        "out_message_id": out_message_id,
        "route": "new_lead",
        "slot_matched": None,
        "booking_id": None,
        "idempotent_skip": False,
        "trace_id": ev.trace_id,
    }


def _is_offer_expired(offered_at: str, timezone: str = "Europe/London") -> bool:
    """Check if last_offer is expired (older than OFFER_EXPIRY_HOURS)."""
    from zoneinfo import ZoneInfo

    tz = ZoneInfo(timezone)
    now = datetime.now(tz)

    try:
        offer_dt = datetime.fromisoformat(offered_at)
        if offer_dt.tzinfo is None:
            offer_dt = offer_dt.replace(tzinfo=tz)
        return (now - offer_dt) > timedelta(hours=OFFER_EXPIRY_HOURS)
    except (ValueError, TypeError):
        return True  # Treat invalid dates as expired


def _format_slot_for_confirmation(slot_iso: str, timezone: str = "Europe/London") -> str:
    """Format a slot for confirmation message (e.g., 'Friday 09:15')."""
    from zoneinfo import ZoneInfo

    tz = ZoneInfo(timezone)
    slot_dt = datetime.fromisoformat(slot_iso)
    if slot_dt.tzinfo is None:
        slot_dt = slot_dt.replace(tzinfo=tz)
    return slot_dt.strftime("%A %H:%M")


def _detect_yes_no(text: str) -> str | None:
    """Detect YES or NO from user text. Returns 'yes', 'no', or None."""
    t = text.lower().strip()
    # Remove punctuation for matching
    t_clean = re.sub(r"[^\w\s]", "", t)
    words = set(t_clean.split())

    # Check for explicit yes/no
    if words & YES_PATTERNS:
        return "yes"
    if words & NO_PATTERNS:
        return "no"
    return None


async def _detect_confirmation_intent(
    text: str,
    llm_settings: Optional[dict[str, Any]] = None,
) -> bool:
    """
    Detect if text contains confirmation intent for auto-confirm during slot selection.
    Returns True if user expressed confirmation alongside their slot choice.

    Uses pattern matching first (fast path), then LLM fallback if enabled.
    """
    t = text.lower().strip()
    t_clean = re.sub(r"[^\w\s]", "", t)
    words = set(t_clean.split())

    # Fast path: pattern matching
    # Check for YES patterns (e.g., "yes the first one")
    if words & YES_PATTERNS:
        return True

    # Check for confirmation phrases (e.g., "book me for 9:15", "perfect, option 2")
    for phrase in CONFIRM_PHRASES:
        if phrase in t_clean:
            return True

    # LLM fallback (if enabled and pattern matching found nothing)
    if llm_settings and llm_settings.get("enabled"):
        llm_result = await classify_confirmation_intent_llm(text, llm_settings)
        if llm_result.get("has_confirmation") is True:
            return True

    return False


async def _emit_booking_event(
    conn: asyncpg.Connection,
    *,
    tenant_id: str,
    ev: InboundEvent,
    contact_id: str,
    slot_iso: str,
    booking_id: str,
    now: datetime,
) -> None:
    """Emit an appointment_booked event into the engine schema.

    Best-effort: failures are logged but never block the bot response.
    """
    try:
        # Resolve CRM contact ID: prefer payload, fall back to channel address
        contact_external_id = (
            ev.payload.get("contactId")
            or ev.payload.get("contact_id")
            or ev.channel_address
        )
        contact_provider = ev.provider

        lead_id = await resolve_or_create_lead(
            conn,
            tenant_id=tenant_id,
            contact_provider=contact_provider,
            contact_external_id=contact_external_id,
            source="inbound_sms",
        )

        await write_lead_event(
            conn,
            lead_id=lead_id,
            tenant_id=tenant_id,
            event_type="appointment_booked",
            source="bot",
            occurred_at=now,
            canonical_stage="appointment_booked",
            source_event_id=booking_id,
            actor="bot",
            payload={
                "slot_start": slot_iso,
                "booking_id": booking_id,
                "contact_id": contact_id,
            },
        )
    except Exception as exc:
        # Engine write must never break the bot flow
        print(f"WARN engine event failed: {exc}")


async def _handle_offer_slots(
    conn: asyncpg.Connection,
    tenant_id: str,
    route_info: Any,
) -> Tuple[str, Dict[str, Any]]:
    """
    Fetch slots (tenant-configured), filter by signals + availability windows,
    compose reply, return (out_text, last_offer).

    Observability: calendar_check is stored in last_offer and includes:
    ok, calendar_id, checked_range, returned_slots_count, filtered_slots_count, reason, checked_at
    """
    from zoneinfo import ZoneInfo

    # 1) Load tenant + calendar/booking settings from DB
    tenant = await load_tenant(conn, tenant_id)
    cal = get_calendar_settings(tenant)
    booking_cfg = get_booking_config(tenant)

    calendar_id = cal.get("calendar_id")
    timezone = booking_cfg.get("timezone", "Europe/London")
    availability_windows = booking_cfg.get("availability")  # None if not configured

    tz = ZoneInfo(timezone)
    now = datetime.now(tz)

    if not calendar_id:
        out_text = (
            "Quick one — I'm missing calendar setup on our side. "
            "What day works best for you, and would morning, afternoon, or evening be ideal?"
        )
        last_offer = {
            "slots": [],
            "offered_slots": [],
            "constraints": {
                "day": getattr(route_info.signals, "day", None),
                "time_window": getattr(route_info.signals, "time_window", None),
                "explicit_time": getattr(route_info.signals, "explicit_time", None),
            },
            "offered_at": now.isoformat(),
            "timezone": timezone,
            "calendar_check": {
                "ok": False,
                "trace_id": None,
                "calendar_id": None,
                "checked_range": None,
                "returned_slots_count": 0,
                "filtered_slots_count": 0,
                "reason": "missing_calendar_id",
                "checked_at": now.isoformat(),
            },
        }
        return out_text, last_offer

    # 2) Compute range
    start_dt = now
    end_dt = now + timedelta(days=14)

    # 3) Load GHL access token from tenant credentials (with env var fallback)
    credentials = await load_tenant_credentials(conn, tenant_id, provider="ghl")
    ghl_creds = credentials.get("ghl", {})
    access_token = ghl_creds.get("access_token")
    if not access_token:
        out_text = (
            "Quick one — I'm missing calendar credentials on our side. "
            "What day works best for you, and would morning, afternoon, or evening be ideal?"
        )
        last_offer = {
            "slots": [],
            "offered_slots": [],
            "constraints": {
                "day": getattr(route_info.signals, "day", None),
                "time_window": getattr(route_info.signals, "time_window", None),
                "explicit_time": getattr(route_info.signals, "explicit_time", None),
            },
            "offered_at": now.isoformat(),
            "timezone": timezone,
            "calendar_check": {
                "ok": False,
                "trace_id": None,
                "calendar_id": calendar_id,
                "checked_range": {"start": start_dt.isoformat(), "end": end_dt.isoformat()},
                "returned_slots_count": 0,
                "filtered_slots_count": 0,
                "reason": "auth_error",
                "checked_at": now.isoformat(),
            },
        }
        return out_text, last_offer

    # 4) Call GHL calendar API (with error handling for observability)
    try:
        all_slots, trace_id = await get_free_slots(
            access_token=access_token,
            calendar_id=calendar_id,
            start_dt=start_dt,
            end_dt=end_dt,
            timezone=timezone,
        )
    except RuntimeError as e:
        # Auth error (401 from GHL)
        error_msg = str(e)
        reason = "auth_error" if "Unauthorized" in error_msg else "unknown_error"
        out_text = (
            "Quick one — I'm having trouble reaching the calendar right now. "
            "What day works best for you, and would morning, afternoon, or evening be ideal?"
        )
        last_offer = {
            "slots": [],
            "offered_slots": [],
            "constraints": {
                "day": getattr(route_info.signals, "day", None),
                "time_window": getattr(route_info.signals, "time_window", None),
                "explicit_time": getattr(route_info.signals, "explicit_time", None),
            },
            "offered_at": now.isoformat(),
            "timezone": timezone,
            "calendar_check": {
                "ok": False,
                "trace_id": None,
                "calendar_id": calendar_id,
                "checked_range": {"start": start_dt.isoformat(), "end": end_dt.isoformat()},
                "returned_slots_count": 0,
                "filtered_slots_count": 0,
                "reason": reason,
                "checked_at": now.isoformat(),
            },
        }
        return out_text, last_offer
    except Exception as e:
        # HTTP errors or unknown errors
        import httpx
        if isinstance(e, httpx.HTTPStatusError):
            reason = "http_error"
        else:
            reason = "unknown_error"
        out_text = (
            "Quick one — I'm having trouble reaching the calendar right now. "
            "What day works best for you, and would morning, afternoon, or evening be ideal?"
        )
        last_offer = {
            "slots": [],
            "offered_slots": [],
            "constraints": {
                "day": getattr(route_info.signals, "day", None),
                "time_window": getattr(route_info.signals, "time_window", None),
                "explicit_time": getattr(route_info.signals, "explicit_time", None),
            },
            "offered_at": now.isoformat(),
            "timezone": timezone,
            "calendar_check": {
                "ok": False,
                "trace_id": None,
                "calendar_id": calendar_id,
                "checked_range": {"start": start_dt.isoformat(), "end": end_dt.isoformat()},
                "returned_slots_count": 0,
                "filtered_slots_count": 0,
                "reason": reason,
                "checked_at": now.isoformat(),
            },
        }
        return out_text, last_offer

    # 5) Apply optional availability window filtering (only if configured)
    if availability_windows:
        slots_after_windows = filter_by_availability_windows(
            all_slots, availability_windows, timezone=timezone
        )
    else:
        slots_after_windows = all_slots

    # 6) Filter by extracted signals (day, time_window)
    signals = route_info.signals
    filtered = filter_slots_by_signals(
        slots_after_windows,
        day=signals.day,
        time_window=signals.time_window,
        timezone=timezone,
    )

    # 7) Pick exactly 2 slots: A=preference match, B=contrasting or next-closest
    base_slots = filtered if filtered else slots_after_windows
    offered_slots = pick_soonest_two_slots(
        base_slots,
        timezone=timezone,
        contrast_pool=slots_after_windows,  # broader pool for finding contrast
    )

    # 8) Build calendar_check metadata (observability)
    calendar_check = {
        "ok": len(offered_slots) > 0,
        "trace_id": trace_id,
        "calendar_id": calendar_id,
        "checked_range": {"start": start_dt.isoformat(), "end": end_dt.isoformat()},
        "returned_slots_count": len(all_slots),
        "filtered_slots_count": len(slots_after_windows),
        "reason": None,
        "checked_at": now.isoformat(),
    }

    if not all_slots:
        calendar_check["ok"] = False
        calendar_check["reason"] = "no_slots_returned"
    elif not offered_slots:
        calendar_check["ok"] = False
        calendar_check["reason"] = "filtered_out_all"

    # 9) Build constraints for storage
    constraints = {
        "day": signals.day,
        "time_window": signals.time_window,
        "explicit_time": signals.explicit_time,
    }

    # 10) Format for display (in tenant local time)
    display_slots = format_slots_for_display(offered_slots, timezone=timezone)

    # 11) Compose message
    if len(display_slots) == 2:
        out_text = (
            f"I've got two options:\n"
            f"1) {display_slots[0]}\n"
            f"2) {display_slots[1]}\n"
            f"Reply 1 or 2 to choose."
        )
    elif len(display_slots) == 1:
        out_text = (
            f"I've got one available option:\n"
            f"1) {display_slots[0]}\n"
            f"Reply 1 to choose."
        )
    else:
        out_text = (
            "I'm not seeing availability for that window right now. "
            "Would a different day or time work better?"
        )

    last_offer = {
        "slots": offered_slots,
        "offered_slots": offered_slots,  # Duplicate for explicit observability
        "constraints": constraints,
        "offered_at": now.isoformat(),
        "timezone": timezone,
        "calendar_check": calendar_check,
    }

    return out_text, last_offer


async def process_job(conn: asyncpg.Connection, job_id: str) -> dict[str, Any]:
    row = await conn.fetchrow(LOAD_JOB_EVENT_SQL, job_id)
    if not row:
        # This should be rare; job_id exists but join failed
        raise RuntimeError(f"Job not found or missing inbound_event join: {job_id}")

    tenant_id = row["tenant_id"]
    inbound_event_id = row["inbound_event_id"]

    trace_id = row["trace_id"]

    ev = InboundEvent(
        inbound_event_id=inbound_event_id,
        tenant_id=tenant_id,
        provider=row["provider"],
        provider_msg_id=row["provider_msg_id"],
        channel=row["channel"],
        channel_address=row["channel_address"],
        dedupe_key=row["dedupe_key"],
        event_type=row["event_type"],
        payload=_coerce_payload(row["payload"]),
        trace_id=trace_id,
    )

    text = _extract_text(ev.payload)
    display_name = _extract_display_name(ev.payload)

    # Contact metadata must be a dict
    contact_meta = ev.payload.get("contact_metadata")
    if not isinstance(contact_meta, dict):
        contact_meta = {}

    # Contact
    contact_id = await conn.fetchval(
        UPSERT_CONTACT_SQL,
        ev.tenant_id,
        ev.channel,
        ev.channel_address,
        display_name,
        contact_meta,  # Pass dict directly - asyncpg codec handles JSON encoding
    )


    # Conversation (open)
    conversation_id = await conn.fetchval(
        UPSERT_OPEN_CONVERSATION_SQL,
        ev.tenant_id, contact_id,
    )

    # Load conversation context
    context_row = await conn.fetchval(LOAD_CONVERSATION_CONTEXT_SQL, conversation_id)
    conv_context = _coerce_payload(context_row)

    # Handle new_lead event separately (no inbound message, just first-touch outbound)
    if ev.event_type == "new_lead":
        result = await _handle_new_lead(
            conn, ev, contact_id, conversation_id, conv_context, display_name
        )
        result["job_id"] = job_id
        return result

    # Check for existing booking (idempotency)
    booked_booking = conv_context.get("booked_booking")
    existing_pending = conv_context.get("pending_booking")
    last_offer = conv_context.get("last_offer")

    # Route the inbound message (always needed for payload)
    route_info = route_from_text(text)
    route_info_dict = route_info_to_dict(route_info)

    # Build inbound payload with route_info
    inbound_payload = dict(ev.payload)
    inbound_payload["route_info"] = route_info_dict

    # Defensive guard: ensure route_info is always a dict, not a list
    ri = inbound_payload.get("route_info")
    if isinstance(ri, list):
        if len(ri) == 1 and isinstance(ri[0], dict):
            inbound_payload["route_info"] = ri[0]
        else:
            inbound_payload["route_info"] = {
                "route": "unknown",
                "signals": {},
                "confidence": 0.0,
                "error": f"bad_route_info_type:{type(ri).__name__}",
            }
    elif not isinstance(ri, dict):
        inbound_payload["route_info"] = {
            "route": "unknown",
            "signals": {},
            "confidence": 0.0,
            "error": f"bad_route_info_type:{type(ri).__name__}",
        }

    # Defensive guard: ensure inbound_payload is always a dict before insert
    if isinstance(inbound_payload, list):
        inbound_payload = inbound_payload[-1] if inbound_payload else {}
    assert isinstance(inbound_payload, dict), f"inbound_payload must be dict, got {type(inbound_payload)}"

    print("DEBUG route_info typeof:", type(inbound_payload.get("route_info")), inbound_payload.get("route_info"))

    # Pass dict directly - asyncpg codec handles JSON encoding (don't double-encode)
    # Message (idempotent) - $12 = trace_id for glass-box tracing
    message_id = await conn.fetchval(
        INSERT_INBOUND_MESSAGE_IDEMPOTENT_SQL,
        ev.tenant_id,
        conversation_id,
        contact_id,
        text,
        ev.provider,
        ev.provider_msg_id,
        ev.channel,
        ev.dedupe_key,
        inbound_payload,  # Pass dict, not json.dumps() string
        ev.inbound_event_id,
        ev.event_type,
        ev.trace_id,  # $12 - propagate trace_id
    )

    await conn.execute(UPDATE_CONVERSATION_LAST_INBOUND_SQL, conversation_id)

    # State for response generation
    from zoneinfo import ZoneInfo
    tz = ZoneInfo("Europe/London")
    now = datetime.now(tz)

    new_last_offer = None
    context_updates: dict[str, Any] = {}
    out_text: str
    pending_booking = None
    slot_matched = None
    booking_result = None

    # Priority 1: Handle pending_booking confirmation (YES/NO)
    if existing_pending and isinstance(existing_pending.get("slot"), str):
        pending_slot = existing_pending["slot"]

        # Check idempotency: already booked this slot?
        if booked_booking and booked_booking.get("slot") == pending_slot:
            slot_display = _format_slot_for_confirmation(pending_slot)
            out_text = f"Already booked ✅ You're confirmed for {slot_display}."

        else:
            yes_no = _detect_yes_no(text)

            if yes_no == "yes":
                # Book the slot
                booking_result = await book_slot(
                    tenant_id=ev.tenant_id,
                    slot_iso=pending_slot,
                    contact_id=contact_id,
                    conversation_id=conversation_id,
                    metadata={"source": "chatbot"},
                )

                if booking_result.get("success"):
                    # Move to booked_booking, clear pending and last_offer
                    context_updates["booked_booking"] = {
                        "slot": pending_slot,
                        "booking_id": booking_result["booking_id"],
                        "booked_at": now.isoformat(),
                    }
                    context_updates["pending_booking"] = None
                    context_updates["last_offer"] = None

                    slot_display = _format_slot_for_confirmation(pending_slot)
                    out_text = f"Booked ✅ You're confirmed for {slot_display}. See you then!"

                    # Emit engine event
                    await _emit_booking_event(
                        conn,
                        tenant_id=ev.tenant_id,
                        ev=ev,
                        contact_id=contact_id,
                        slot_iso=pending_slot,
                        booking_id=booking_result["booking_id"],
                        now=now,
                    )
                else:
                    out_text = "Sorry, there was an issue booking that slot. Please try again or choose another time."

            elif yes_no == "no":
                # Clear pending_booking, re-offer slots
                context_updates["pending_booking"] = None

                if last_offer and isinstance(last_offer.get("slots"), list) and not _is_offer_expired(last_offer.get("offered_at", "")):
                    # Re-display existing offer
                    display_slots = format_slots_for_display(last_offer["slots"], timezone=last_offer.get("timezone", "Europe/London"))
                    slot_list = "\n".join(f"• {s}" for s in display_slots)
                    out_text = f"No problem! Here are the options again:\n\n{slot_list}\n\nWhich one works best for you?"
                else:
                    # Ask for new preference
                    out_text = "No problem — what day and time would work better for you?"

            else:
                # Neither YES nor NO - remind user
                slot_display = _format_slot_for_confirmation(pending_slot)
                out_text = f"Just to confirm: shall I book you in for {slot_display}? Reply YES to confirm or NO to choose another time."

    # Priority 2: Check for slot selection from last_offer
    elif last_offer and isinstance(last_offer.get("slots"), list):
        offered_at = last_offer.get("offered_at", "")
        offer_expired = _is_offer_expired(offered_at)

        if not offer_expired:
            slot_matched = _match_slot_from_text(text, last_offer["slots"])

        if slot_matched:
            # Check if user also expressed confirmation intent (auto-confirm)
            # Load LLM settings for fallback detection
            try:
                tenant = await load_tenant(conn, ev.tenant_id)
                llm_settings = get_llm_settings(tenant)
            except Exception:
                llm_settings = None

            has_confirmation = await _detect_confirmation_intent(text, llm_settings)

            if has_confirmation:
                # Auto-confirm: book immediately without pending step
                booking_result = await book_slot(
                    tenant_id=ev.tenant_id,
                    slot_iso=slot_matched,
                    contact_id=contact_id,
                    conversation_id=conversation_id,
                    metadata={"source": "chatbot", "auto_confirmed": True},
                )

                if booking_result.get("success"):
                    context_updates["booked_booking"] = {
                        "slot": slot_matched,
                        "booking_id": booking_result["booking_id"],
                        "booked_at": now.isoformat(),
                    }
                    context_updates["pending_booking"] = None
                    context_updates["last_offer"] = None

                    slot_display = _format_slot_for_confirmation(slot_matched)
                    out_text = f"Booked ✅ You're confirmed for {slot_display}. See you then!"

                    # Emit engine event
                    await _emit_booking_event(
                        conn,
                        tenant_id=ev.tenant_id,
                        ev=ev,
                        contact_id=contact_id,
                        slot_iso=slot_matched,
                        booking_id=booking_result["booking_id"],
                        now=now,
                    )
                else:
                    # Booking failed - fall back to pending flow
                    pending_booking = {
                        "slot": slot_matched,
                        "created_at": now.isoformat(),
                    }
                    context_updates["pending_booking"] = pending_booking

                    slot_display = _format_slot_for_confirmation(slot_matched)
                    out_text = f"Perfect — shall I book you in for {slot_display}? Reply YES to confirm or NO to choose another."
            else:
                # No confirmation intent - create pending_booking as before
                pending_booking = {
                    "slot": slot_matched,
                    "created_at": now.isoformat(),
                }
                context_updates["pending_booking"] = pending_booking

                slot_display = _format_slot_for_confirmation(slot_matched)
                out_text = f"Perfect — shall I book you in for {slot_display}? Reply YES to confirm or NO to choose another."

        elif not offer_expired:
            # User has an active offer but didn't match a slot - prompt again
            offer_tz = last_offer.get("timezone", "Europe/London")
            display_slots = format_slots_for_display(last_offer["slots"], timezone=offer_tz)
            if len(display_slots) == 2:
                out_text = f"Reply 1 for {display_slots[0]} or 2 for {display_slots[1]}."
            elif len(display_slots) == 1:
                out_text = f"Reply 1 for {display_slots[0]}."
            else:
                out_text = "I didn't catch which time you'd like. What day and time would work for you?"

        elif route_info.route == "offer_slots":
            # Expired offer but user is asking for slots again
            out_text, new_last_offer = await _handle_offer_slots(conn, ev.tenant_id, route_info)
            context_updates["last_offer"] = new_last_offer

        else:
            # Expired offer, normal routing
            out_text = compose_reply(route_info)

    elif route_info.route == "offer_slots":
        # No last_offer, generate new slots
        out_text, new_last_offer = await _handle_offer_slots(conn, ev.tenant_id, route_info)
        context_updates["last_offer"] = new_last_offer

    else:
        # Normal routing
        out_text = compose_reply(route_info)

    # Store context updates if any
    if context_updates:
        await conn.execute(UPDATE_CONVERSATION_CONTEXT_SQL, conversation_id, context_updates)

    # LLM copy polishing (optional)
    # out_text is now the template_text; we may rewrite it
    template_text = out_text
    final_text = template_text
    llm_metadata: dict[str, Any] = {
        "enabled": False,
        "used": False,
        "model": None,
        "prompt_version": None,
        "error": None,
        "rewritten_at": None,
    }

    # Load tenant for LLM settings
    try:
        tenant = await load_tenant(conn, ev.tenant_id)
        llm_settings = get_llm_settings(tenant)
        llm_metadata["enabled"] = llm_settings["enabled"]

        if llm_settings["enabled"]:
            llm_metadata["model"] = llm_settings["model"]
            llm_metadata["prompt_version"] = llm_settings["prompt_version"]

            # Call LLM rewriter
            llm_result = await rewrite_outbound_text_llm(
                llm_settings=llm_settings,
                template_text=template_text,
            )

            llm_metadata["used"] = llm_result["used"]
            llm_metadata["error"] = llm_result["error"]
            llm_metadata["rewritten_at"] = llm_result["rewritten_at"]

            # Use rewritten text if successful, otherwise fallback to template
            if llm_result["used"] and llm_result["rewritten_text"]:
                final_text = llm_result["rewritten_text"]
            # else: final_text remains template_text (safe fallback)

    except Exception as e:
        # LLM failure should never block message sending
        llm_metadata["error"] = f"llm_load_exception:{str(e)[:100]}"
        # final_text remains template_text

    # Create pending outbound message
    out_payload_dict: dict[str, Any] = {
        "send_status": "pending",
        "send_attempts": 0,
        "send_last_error": None,
        "route": route_info.route,
        "text_template": template_text,
        "text_final": final_text,
        "llm": llm_metadata,
    }
    if new_last_offer:
        out_payload_dict["offered_slots"] = new_last_offer["slots"]
        out_payload_dict["calendar_check"] = new_last_offer.get("calendar_check")
    if pending_booking:
        out_payload_dict["pending_booking"] = pending_booking
    if booking_result:
        out_payload_dict["booking_result"] = booking_result

    out_message_id = await conn.fetchval(
        INSERT_OUTBOUND_MESSAGE_SQL,
        ev.tenant_id,
        conversation_id,
        contact_id,
        ev.provider,
        ev.channel,
        final_text,  # Use final_text (rewritten or template)
        out_payload_dict,  # Pass dict directly - asyncpg codec handles JSON encoding
        ev.trace_id,  # $8 - propagate trace_id
    )

    # Glass-box: build debug snapshot for conversation context
    # Determine state transition
    state_from = conv_context.get("_last_step", "start")
    state_to = route_info.route
    if booking_result and booking_result.get("success"):
        state_to = "booked"
    elif pending_booking:
        state_to = "pending_confirmation"

    debug_snapshot = build_debug_snapshot(
        route=route_info.route,
        signals={
            "day": route_info.signals.day,
            "time_window": route_info.signals.time_window,
            "explicit_time": route_info.signals.explicit_time,
        },
        slot_count=len(new_last_offer["slots"]) if new_last_offer else 0,
        chosen_slots=[
            {"iso": s, "human": _format_slot_for_confirmation(s)}
            for s in (new_last_offer["slots"] if new_last_offer else [])
        ] if new_last_offer else None,
        transition={"from": state_from, "to": state_to},
    )

    # Update conversation context with debug snapshot (overwrite-only)
    debug_context = {"debug": {"last_run": debug_snapshot}, "_last_step": state_to}
    await conn.execute(UPDATE_CONVERSATION_CONTEXT_SQL, conversation_id, debug_context)

    # Glass-box: structured logging
    # Load tenant_slug for logging (best-effort)
    try:
        tenant = await load_tenant(conn, ev.tenant_id)
        tenant_slug = tenant.get("tenant_slug", ev.tenant_id)
    except Exception:
        tenant_slug = ev.tenant_id

    calendar_result = None
    if new_last_offer and new_last_offer.get("calendar_check"):
        cc = new_last_offer["calendar_check"]
        calendar_result = {
            "ok": cc.get("ok"),
            "returned_slots_count": cc.get("returned_slots_count"),
            "provider_trace_id": cc.get("trace_id"),
        }

    log_processing_run(
        tenant_slug=tenant_slug,
        contact_id=contact_id,
        conversation_id=conversation_id,
        trace_id=ev.trace_id,
        route=route_info.route,
        signals={
            "day": route_info.signals.day,
            "time_window": route_info.signals.time_window,
            "explicit_time": route_info.signals.explicit_time,
        },
        calendar_result=calendar_result,
        offered_slots=[
            {"iso": s, "human": _format_slot_for_confirmation(s)}
            for s in (new_last_offer["slots"] if new_last_offer else [])
        ] if new_last_offer else None,
        chosen_slot={"iso": slot_matched, "human": _format_slot_for_confirmation(slot_matched)} if slot_matched else None,
        state_transition={"from": state_from, "to": state_to},
    )

    return {
        "job_id": job_id,
        "tenant_id": ev.tenant_id,
        "inbound_event_id": ev.inbound_event_id,
        "contact_id": contact_id,
        "conversation_id": conversation_id,
        "message_id": message_id,
        "out_message_id": out_message_id,
        "route": route_info.route,
        "slot_matched": slot_matched,
        "booking_id": booking_result.get("booking_id") if booking_result else None,
        "trace_id": ev.trace_id,
    }
