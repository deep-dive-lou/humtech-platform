from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Tuple, Optional
import asyncpg
import json
import os
import re

from app.adapters.calendar.ghl import (
    get_free_slots,
    filter_slots_by_signals,
    format_slots_for_display,
    book_slot,
)
from app.services.routing import route_from_text, compose_reply, route_info_to_dict
from app.services.tenants import load_tenant, get_calendar_settings
from app.services.tenants import load_tenant, get_calendar_settings
from app.adapters.calendar.ghl import get_free_slots, filter_slots_by_signals, format_slots_for_display

# Offer expiry: 2 hours
OFFER_EXPIRY_HOURS = 2

# YES/NO detection patterns
YES_PATTERNS = {"yes", "yep", "yeah", "yup", "sure", "confirm", "ok", "okay", "y", "affirmative", "absolutely", "definitely"}
NO_PATTERNS = {"no", "nope", "nah", "cancel", "n", "negative", "different", "another", "change"}

def _coerce_payload(payload_raw) -> dict:
    if payload_raw is None:
        return {}
    if isinstance(payload_raw, dict):
        return payload_raw
    if isinstance(payload_raw, str):
        s = payload_raw.strip()
        if not s:
            return {}
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            # If it's not valid JSON, keep it wrapped so we don't crash
            return {"_raw": payload_raw}
    # asyncpg sometimes returns Record-like mappings; try dict()
    try:
        return dict(payload_raw)
    except Exception:
        return {"_raw": str(payload_raw)}

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
  ie.payload
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
    direction, provider, provider_msg_id, channel, text, payload, created_at
  )
  SELECT
    $1::uuid, $2::uuid, $3::uuid,
    'inbound', $5::text, $6::text, $7::text, $4::text,
    COALESCE($9::jsonb, '{}'::jsonb) || jsonb_build_object(
      'inbound_event_id', $10::text,
      'dedupe_key', $8::text,
      'event_type', $11::text
    ),
    now()
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
  direction, provider, channel, text, payload, created_at
)
VALUES (
  $1::uuid, $2::uuid, $3::uuid,
  'outbound', $4::text, $5::text, $6::text, $7::jsonb, now()
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


def _match_slot_from_text(text: str, slots: list[str]) -> Optional[str]:
    """Try to match user text to one of the offered slots."""
    # Try ordinal first ("the first one", "2nd")
    matched = _match_slot_by_ordinal(text, slots)
    if matched:
        return matched

    # Try time match ("9:15", "9am")
    matched = _match_slot_by_time(text, slots)
    if matched:
        return matched

    return None


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


async def _handle_offer_slots(
    conn: asyncpg.Connection,
    tenant_id: str,
    route_info: Any,
) -> Tuple[str, Dict[str, Any]]:
    """
    Fetch slots (tenant-configured), filter by signals, compose reply, return (out_text, last_offer).
    """
    from zoneinfo import ZoneInfo

    # 1) Load tenant + calendar settings from DB
    tenant = await load_tenant(conn, tenant_id)
    cal = get_calendar_settings(tenant)
    calendar_id = cal.get("calendar_id")
    timezone = cal.get("timezone", "Europe/London")
    tenant_slug = tenant.get("tenant_slug")  # used for env token lookup in adapter (if you chose that style)

    if not calendar_id:
        out_text = (
            "Quick one — I’m missing calendar setup on our side. "
            "What day works best for you, and would morning, afternoon, or evening be ideal?"
        )
        last_offer = {
            "slots": [],
            "constraints": {
                "day": getattr(route_info.signals, "day", None),
                "time_window": getattr(route_info.signals, "time_window", None),
                "explicit_time": getattr(route_info.signals, "explicit_time", None),
            },
            "offered_at": datetime.utcnow().isoformat(),
            "error": "missing_calendar_id",
        }
        return out_text, last_offer

    # 2) Compute range
    tz = ZoneInfo(timezone)
    now = datetime.now(tz)
    start_dt = now
    end_dt = now + timedelta(days=14)

    # 3) Call adapter (pick ONE signature style and match your ghl.get_free_slots)
    # --- If your ghl.get_free_slots expects tenant_slug + calendar_id:
    all_slots = await get_free_slots(
        tenant_slug=tenant_slug,
        calendar_id=calendar_id,
        start_dt=start_dt,
        end_dt=end_dt,
        timezone=timezone,
    )

    # --- If instead your ghl.get_free_slots expects access_token + calendar_id,
    # load token here and call with access_token=... (not shown).

    # 4) Filter by extracted signals
    signals = route_info.signals
    filtered = filter_slots_by_signals(
        all_slots,
        day=signals.day,
        time_window=signals.time_window,
        timezone=timezone,
    )

    # 5) Pick up to 6 slots
    offered_slots = (filtered[:6] if filtered else all_slots[:6])

    # 6) Build constraints for storage
    constraints = {
        "day": signals.day,
        "time_window": signals.time_window,
        "explicit_time": signals.explicit_time,
    }

    # 7) Format for display
    display_slots = format_slots_for_display(offered_slots, timezone=timezone)

    # 8) Compose message
    if display_slots:
        slot_list = "\n".join(f"• {s}" for s in display_slots)
        out_text = f"Here are the available times:\n\n{slot_list}\n\nWhich one works best for you?"
    else:
        # Safe fallback when API returns nothing
        out_text = (
            "I’m not seeing availability for that window right now. "
            "Would a different day or time work better?"
        )

    last_offer = {
        "slots": offered_slots,
        "constraints": constraints,
        "offered_at": now.isoformat(),
        "calendar_id": calendar_id,
        "timezone": timezone,
    }

    return out_text, last_offer


async def process_job(conn: asyncpg.Connection, job_id: str) -> dict[str, Any]:
    row = await conn.fetchrow(LOAD_JOB_EVENT_SQL, job_id)
    if not row:
        # This should be rare; job_id exists but join failed
        raise RuntimeError(f"Job not found or missing inbound_event join: {job_id}")

    tenant_id = row["tenant_id"]
    inbound_event_id = row["inbound_event_id"]

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

    )

    text = _extract_text(ev.payload)
    display_name = _extract_display_name(ev.payload)

    # Contact metadata must be a dict
    contact_meta = ev.payload.get("contact_metadata")
    if not isinstance(contact_meta, dict):
        contact_meta = {}
    contact_meta_json = json.dumps(contact_meta, ensure_ascii=False)

    # Contact
    contact_id = await conn.fetchval(
        UPSERT_CONTACT_SQL,
        ev.tenant_id,
        ev.channel,
        ev.channel_address,
        display_name,
        contact_meta_json,
    )


    # Conversation (open)
    conversation_id = await conn.fetchval(
        UPSERT_OPEN_CONVERSATION_SQL,
        ev.tenant_id, contact_id,
    )

    # Load conversation context
    context_row = await conn.fetchval(LOAD_CONVERSATION_CONTEXT_SQL, conversation_id)
    conv_context = _coerce_payload(context_row)

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
    payload_json = json.dumps(inbound_payload, ensure_ascii=False)

    # Message (idempotent)
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
        payload_json,
        ev.inbound_event_id,
        ev.event_type,
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
            # User selected a slot - create pending_booking
            pending_booking = {
                "slot": slot_matched,
                "created_at": now.isoformat(),
            }
            context_updates["pending_booking"] = pending_booking

            slot_display = _format_slot_for_confirmation(slot_matched)
            out_text = f"Perfect — shall I book you in for {slot_display}? Reply YES to confirm or NO to choose another."

        elif not offer_expired:
            # User has an active offer but didn't match a slot - prompt again
            display_slots = format_slots_for_display(last_offer["slots"])
            slot_list = "\n".join(f"• {s}" for s in display_slots)
            out_text = f"I didn't catch which time you'd like. Here are the options again:\n\n{slot_list}\n\nWhich one works best for you?"

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
        context_update_json = json.dumps(context_updates, ensure_ascii=False)
        await conn.execute(UPDATE_CONVERSATION_CONTEXT_SQL, conversation_id, context_update_json)

    # Create pending outbound message
    out_payload_dict: dict[str, Any] = {
        "send_status": "pending",
        "send_attempts": 0,
        "send_last_error": None,
        "route": route_info.route,
    }
    if new_last_offer:
        out_payload_dict["offered_slots"] = new_last_offer["slots"]
    if pending_booking:
        out_payload_dict["pending_booking"] = pending_booking
    if booking_result:
        out_payload_dict["booking_result"] = booking_result

    out_payload = json.dumps(out_payload_dict, ensure_ascii=False)
    out_message_id = await conn.fetchval(
        INSERT_OUTBOUND_MESSAGE_SQL,
        ev.tenant_id,
        conversation_id,
        contact_id,
        ev.provider,
        ev.channel,
        out_text,
        out_payload,
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
    }
