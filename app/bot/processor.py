from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Tuple, Optional
import asyncpg
import json
import os

from app.config import settings  # ensures dotenv is loaded
from app.adapters.calendar.ghl import (
    get_free_slots,
    filter_slots_by_signals,
    filter_by_availability_windows,
    format_slots_for_display,
    pick_soonest_two_slots,
    book_slot,
)
from app.bot.routing import route_from_text, route_info_to_dict
from app.bot.tenants import (
    load_tenant,
    load_tenant_credentials,
    get_calendar_settings,
    get_booking_config,
    get_llm_settings,
    get_bot_settings,
)
from app.bot.llm import process_inbound_message
from app.bot.trace_logger import log_processing_run, build_debug_snapshot
from app.engine.events import resolve_or_create_lead, write_lead_event

# Offer expiry: 2 hours
OFFER_EXPIRY_HOURS = 2


class _NullSignals:
    """Null signals for first-touch slot fetching (no day/time preference yet)."""
    day = None
    time_window = None
    explicit_time = None


class _NullRouteInfo:
    """Minimal route_info stub for first-touch slot fetching."""
    route = "offer_slots"
    signals = _NullSignals()


def _build_first_touch_text(
    display_slots: list,
    name_part: str,
    template: Optional[str] = None,
) -> str:
    """Build first-touch greeting with offered slots."""
    if template:
        slot_1 = display_slots[0] if len(display_slots) > 0 else ""
        slot_2 = display_slots[1] if len(display_slots) > 1 else ""
        try:
            return template.format(name_part=name_part, slot_1=slot_1, slot_2=slot_2)
        except (KeyError, IndexError):
            pass  # Fall through to default

    if len(display_slots) >= 2:
        return (
            f"Hey{name_part} — thanks for reaching out. "
            f"Want to get you booked in quickly. "
            f"I've got {display_slots[0]} or {display_slots[1]} free — which works best for you?"
        )
    elif len(display_slots) == 1:
        return (
            f"Hey{name_part} — thanks for reaching out. "
            f"Want to get you booked in quickly. "
            f"I've got {display_slots[0]} free — does that work for you?"
        )
    else:
        return (
            f"Hey{name_part} — thanks for reaching out. "
            f"Want to get you booked in quickly. "
            f"What day and time works best for you?"
        )


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

FIND_OPEN_CONVERSATION_SQL = """
SELECT conversation_id::text
FROM bot.conversations
WHERE tenant_id = $1::uuid
  AND contact_id = $2::uuid
  AND status = 'open'
LIMIT 1;
"""

CLOSE_CONVERSATION_SQL = """
UPDATE bot.conversations
SET status = 'closed', updated_at = now()
WHERE conversation_id = $1::uuid;
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

LOAD_RECENT_MESSAGES_SQL = """
SELECT direction, text
FROM bot.messages
WHERE conversation_id = $1::uuid
ORDER BY created_at ASC
LIMIT 20;
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


async def _handle_new_lead(
    conn: asyncpg.Connection,
    ev: InboundEvent,
    contact_id: str,
    conversation_id: str,
    conv_context: dict[str, Any],
    display_name: Optional[str],
    tenant: dict[str, Any],
) -> dict[str, Any]:
    """
    Handle new_lead event: fetch 2 slots immediately, send first-touch message.
    Idempotent: if lead_touchpoint already exists, do nothing.
    """
    from zoneinfo import ZoneInfo

    bot_settings = get_bot_settings(tenant)
    tz = ZoneInfo("Europe/London")
    now = datetime.now(tz)

    # Idempotency: if lead_touchpoint already exists, skip
    existing_touchpoint = conv_context.get("lead_touchpoint")
    if existing_touchpoint and isinstance(existing_touchpoint, dict):
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

    # Fetch 2 slots for first-touch (no signals — just pick soonest two)
    _, first_touch_offer = await _handle_offer_slots(conn, ev.tenant_id, _NullRouteInfo())
    offered_slots = first_touch_offer.get("offered_slots", [])
    offer_tz = first_touch_offer.get("timezone", "Europe/London")
    display_slots = format_slots_for_display(offered_slots, timezone=offer_tz) if offered_slots else []

    # Build first-touch message (use first name only)
    first_name = display_name.split()[0] if display_name else ""
    name_part = f" {first_name}" if first_name else ""
    out_text = _build_first_touch_text(
        display_slots=display_slots,
        name_part=name_part,
        template=bot_settings.get("first_touch_template"),
    )

    # Create outbound message
    out_payload_dict: dict[str, Any] = {
        "send_status": "pending",
        "send_attempts": 0,
        "send_last_error": None,
        "route": "new_lead",
        "text_final": out_text,
        "event_type": "new_lead",
        "offered_slots": offered_slots,
        "calendar_check": first_touch_offer.get("calendar_check"),
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
        ev.trace_id,
    )

    # Glass-box: debug snapshot for new_lead
    debug_snapshot = build_debug_snapshot(
        route="new_lead",
        signals={},
        slot_count=len(offered_slots),
        chosen_slots=[
            {"iso": s, "human": _format_slot_for_confirmation(s)}
            for s in offered_slots
        ] if offered_slots else None,
        transition={"from": "start", "to": "new_lead"},
    )

    # Set lead_touchpoint, last_offer, and debug snapshot in context
    lead_touchpoint = {
        "first_touch_at": now.isoformat(),
        "channel": ev.channel,
        "message_id": out_message_id,
    }
    context_updates = {
        "lead_touchpoint": lead_touchpoint,
        "last_offer": first_touch_offer if offered_slots else None,
        "debug": {"last_run": debug_snapshot},
        "_last_step": "new_lead",
    }
    await conn.execute(UPDATE_CONVERSATION_CONTEXT_SQL, conversation_id, context_updates)

    # Glass-box: structured logging for new_lead
    log_processing_run(
        tenant_slug=tenant.get("tenant_slug", ev.tenant_id),
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


def _parse_explicit_time_to_hour(explicit_time: str) -> Optional[float]:
    """Parse a time string like '4:35', '9am', '16:00' to a float hour (e.g. 16.583).
    Times < 8 with no am/pm marker are assumed pm (business context)."""
    try:
        raw = str(explicit_time).lower().strip()
        is_pm = "pm" in raw
        is_am = "am" in raw
        t = raw.replace("am", "").replace("pm", "").strip()
        parts = t.split(":")
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        if is_pm and h != 12:
            h += 12
        elif not is_am and not is_pm and h < 8:
            h += 12  # "4:35" without am/pm in business context → 16:35
        return h + m / 60
    except (ValueError, IndexError):
        return None


def _find_nearest_slot(
    slots: list[str],
    preferred_day: Optional[str],
    target_hour: float,
    timezone: str,
    tolerance_minutes: int = 45,
) -> Optional[str]:
    """Find the slot closest to target_hour on preferred_day within tolerance."""
    from zoneinfo import ZoneInfo
    tz = ZoneInfo(timezone)
    utc = ZoneInfo("UTC")
    day_map = {"monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
               "friday": 4, "saturday": 5, "sunday": 6}

    best_slot: Optional[str] = None
    best_diff = float("inf")

    for slot_iso in slots:
        try:
            slot_dt = datetime.fromisoformat(slot_iso.replace("Z", "+00:00"))
            if slot_dt.tzinfo is None:
                slot_dt = slot_dt.replace(tzinfo=utc)
            local_dt = slot_dt.astimezone(tz)
        except ValueError:
            continue

        if preferred_day and preferred_day.lower() in day_map:
            if local_dt.weekday() != day_map[preferred_day.lower()]:
                continue

        slot_hour = local_dt.hour + local_dt.minute / 60
        diff = abs(slot_hour - target_hour)
        if diff < best_diff:
            best_diff = diff
            best_slot = slot_iso

    if best_slot and best_diff <= tolerance_minutes / 60:
        return best_slot
    return None


def _find_two_nearest_slots(
    slots: list[str],
    preferred_day: Optional[str],
    target_hour: float,
    timezone: str,
) -> list[str]:
    """Find the 2 slots nearest to target_hour (no tolerance limit, best effort)."""
    from zoneinfo import ZoneInfo
    tz = ZoneInfo(timezone)
    utc = ZoneInfo("UTC")
    day_map = {"monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
               "friday": 4, "saturday": 5, "sunday": 6}

    candidates: list[tuple[float, datetime, str]] = []
    for slot_iso in slots:
        try:
            slot_dt = datetime.fromisoformat(slot_iso.replace("Z", "+00:00"))
            if slot_dt.tzinfo is None:
                slot_dt = slot_dt.replace(tzinfo=utc)
            local_dt = slot_dt.astimezone(tz)
        except ValueError:
            continue
        if preferred_day and preferred_day.lower() in day_map:
            if local_dt.weekday() != day_map[preferred_day.lower()]:
                continue
        slot_hour = local_dt.hour + local_dt.minute / 60
        candidates.append((abs(slot_hour - target_hour), slot_dt, slot_iso))

    candidates.sort(key=lambda x: x[0])
    result = [iso for _, _, iso in candidates[:2]]
    result.sort(key=lambda iso: datetime.fromisoformat(iso.replace("Z", "+00:00")))
    return result


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
    target_hour: Optional[float] = None,
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

    # 6b) If explicit_time is given (e.g. "2:00" from "between 2-5"), use it as a
    # floor so we only offer slots at or after that hour within the time window.
    explicit_time_signal = getattr(signals, "explicit_time", None)
    if explicit_time_signal and filtered:
        floor_hour = _parse_explicit_time_to_hour(explicit_time_signal)
        if floor_hour is not None:
            _tz_obj = ZoneInfo(timezone)
            _floored = []
            for _iso in filtered:
                try:
                    _dt = datetime.fromisoformat(_iso.replace("Z", "+00:00"))
                    _local = _dt.astimezone(_tz_obj)
                    if _local.hour + _local.minute / 60 >= floor_hour:
                        _floored.append(_iso)
                except ValueError:
                    continue
            if _floored:
                filtered = _floored

    # 7) Pick exactly 2 slots: A=preference match, B=contrasting or next-closest
    base_slots = filtered if filtered else slots_after_windows
    has_time_preference = bool(signals.day or signals.time_window)
    offered_slots = pick_soonest_two_slots(
        base_slots,
        timezone=timezone,
        # When user has a specific preference, stay within that window (don't contrast back to morning)
        contrast_pool=base_slots if has_time_preference else slots_after_windows,
        target_hour=target_hour,
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
            f"I've got {display_slots[0]} or {display_slots[1]} free — which works best for you?"
        )
    elif len(display_slots) == 1:
        out_text = (
            f"I've got {display_slots[0]} free — does that work for you?"
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

    # Build contact metadata — must include contactId so the messaging adapter
    # can resolve the GHL contact when sending outbound messages.
    contact_meta = ev.payload.get("contact_metadata")
    if not isinstance(contact_meta, dict):
        contact_meta = {}
    ghl_contact_id = ev.payload.get("contactId") or ev.payload.get("contact_id")
    if ghl_contact_id:
        contact_meta = {"contactId": ghl_contact_id, **contact_meta}

    # Contact
    contact_id = await conn.fetchval(
        UPSERT_CONTACT_SQL,
        ev.tenant_id,
        ev.channel,
        ev.channel_address,
        display_name,
        contact_meta,  # Pass dict directly - asyncpg codec handles JSON encoding
    )


    # Load tenant settings (needed for both flows)
    try:
        tenant = await load_tenant(conn, ev.tenant_id)
    except Exception as e:
        tenant = {}
        print(f"WARN: Failed to load tenant {ev.tenant_id}: {e}")

    if ev.event_type == "new_lead":
        # new_lead: upsert conversation (creates it if this is the first touch)
        conversation_id = await conn.fetchval(
            UPSERT_OPEN_CONVERSATION_SQL,
            ev.tenant_id, contact_id,
        )
        context_row = await conn.fetchval(LOAD_CONVERSATION_CONTEXT_SQL, conversation_id)
        conv_context = _coerce_payload(context_row)
        result = await _handle_new_lead(
            conn, ev, contact_id, conversation_id, conv_context, display_name, tenant
        )
        result["job_id"] = job_id
        return result

    # inbound_message: only process if an open bot conversation already exists
    conversation_id = await conn.fetchval(
        FIND_OPEN_CONVERSATION_SQL,
        ev.tenant_id, contact_id,
    )
    if not conversation_id:
        return {
            "job_id": job_id,
            "tenant_id": ev.tenant_id,
            "inbound_event_id": ev.inbound_event_id,
            "contact_id": contact_id,
            "conversation_id": None,
            "message_id": None,
            "out_message_id": None,
            "route": "no_active_conversation",
            "slot_matched": None,
            "booking_id": None,
            "trace_id": ev.trace_id,
        }

    # Load conversation context
    context_row = await conn.fetchval(LOAD_CONVERSATION_CONTEXT_SQL, conversation_id)
    conv_context = _coerce_payload(context_row)

    # Route the inbound message (for signal extraction + payload metadata)
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

    route = "unclear"
    new_last_offer = None
    context_updates: dict[str, Any] = {}
    booking_result = None
    slot_matched = None
    llm_result: dict[str, Any] = {"used": False, "error": None, "intent": "unclear",
                                   "reply_text": "Got it — what day and time works best for you?"}

    booked_booking = conv_context.get("booked_booking")
    handoff_info = conv_context.get("handoff_requested")
    last_offer = conv_context.get("last_offer")

    bot_settings = get_bot_settings(tenant)
    llm_settings = get_llm_settings(tenant)

    if booked_booking and isinstance(booked_booking.get("slot"), str):
        # Already booked — idempotent reply
        slot_display = _format_slot_for_confirmation(booked_booking["slot"])
        out_text = f"You're already booked in for {slot_display}. See you then!"
        route = "already_booked"

    elif handoff_info:
        # Handoff already in progress — human will follow up, bot steps back
        out_text = "Someone from the team will be in touch with you shortly."
        route = "handoff_pending"

    else:
        # LLM-driven intent classification + reply composition

        # Load recent conversation history for LLM context
        msg_rows = await conn.fetch(LOAD_RECENT_MESSAGES_SQL, conversation_id)
        conversation_history = [
            {"role": "user" if r["direction"] == "inbound" else "assistant", "text": r["text"]}
            for r in msg_rows
        ]

        # Get active offered slots (only if not expired)
        offered_slots: list[str] = []
        display_slots: list[str] = []
        if last_offer and isinstance(last_offer.get("offered_slots"), list):
            if not _is_offer_expired(last_offer.get("offered_at", "")):
                offered_slots = last_offer["offered_slots"]
                offer_tz = last_offer.get("timezone", "Europe/London")
                display_slots = format_slots_for_display(offered_slots, timezone=offer_tz)

        # LLM classifies intent + composes reply
        llm_result = await process_inbound_message(
            conversation_history=conversation_history,
            offered_slots=offered_slots,
            display_slots=display_slots,
            tenant_context=bot_settings["context"],
            llm_settings=llm_settings,
            persona=bot_settings.get("persona", ""),
        )

        intent = llm_result["intent"]
        route = intent
        out_text = llm_result["reply_text"]

        if intent == "select_slot" and llm_result.get("slot_index") is not None:
            slot_index = llm_result["slot_index"]
            if 0 <= slot_index < len(offered_slots):
                slot_iso = offered_slots[slot_index]
                slot_matched = slot_iso
                booking_result = await book_slot(
                    tenant_id=ev.tenant_id,
                    slot_iso=slot_iso,
                    contact_id=contact_id,
                    conversation_id=conversation_id,
                    metadata={"source": "chatbot"},
                )
                if booking_result.get("success"):
                    slot_display = _format_slot_for_confirmation(slot_iso)
                    out_text = f"Booked ✅ You're confirmed for {slot_display}. See you then!"
                    route = "booked"
                    context_updates["booked_booking"] = {
                        "slot": slot_iso,
                        "booking_id": booking_result["booking_id"],
                        "booked_at": now.isoformat(),
                    }
                    context_updates["last_offer"] = None
                    await _emit_booking_event(
                        conn,
                        tenant_id=ev.tenant_id,
                        ev=ev,
                        contact_id=contact_id,
                        slot_iso=slot_iso,
                        booking_id=booking_result["booking_id"],
                        now=now,
                    )
                else:
                    out_text = "Sorry, I couldn't book that slot — it may have just been taken. Want me to find another time?"
                    route = "booking_failed"

        elif intent == "request_specific_time":
            # Lead asked for a specific time — find it and book if available
            preferred_day = llm_result.get("preferred_day")
            explicit_time = llm_result.get("explicit_time") or ""
            target_hour = _parse_explicit_time_to_hour(explicit_time) if explicit_time else None

            # Fetch all available slots
            tenant_for_slots = await load_tenant(conn, ev.tenant_id)
            cal_for_slots = get_calendar_settings(tenant_for_slots)
            booking_cfg_for_slots = get_booking_config(tenant_for_slots)
            tz_str = booking_cfg_for_slots.get("timezone", "Europe/London")
            credentials_for_slots = await load_tenant_credentials(conn, ev.tenant_id, provider="ghl")
            ghl_creds_for_slots = credentials_for_slots.get("ghl", {})
            access_token_for_slots = ghl_creds_for_slots.get("access_token")
            calendar_id_for_slots = cal_for_slots.get("calendar_id")

            all_slots_for_specific: list[str] = []
            if access_token_for_slots and calendar_id_for_slots:
                _start = now
                _end = now + timedelta(days=14)
                try:
                    all_slots_for_specific, _ = await get_free_slots(
                        access_token=access_token_for_slots,
                        calendar_id=calendar_id_for_slots,
                        start_dt=_start,
                        end_dt=_end,
                        timezone=tz_str,
                    )
                except Exception:
                    pass

            if target_hour is not None and all_slots_for_specific:
                nearest = _find_nearest_slot(
                    all_slots_for_specific, preferred_day, target_hour, tz_str, tolerance_minutes=45
                )
                if nearest:
                    # Found a slot close enough — book it
                    slot_matched = nearest
                    booking_result = await book_slot(
                        tenant_id=ev.tenant_id,
                        slot_iso=nearest,
                        contact_id=contact_id,
                        conversation_id=conversation_id,
                        metadata={"source": "chatbot"},
                    )
                    if booking_result.get("success"):
                        slot_display = _format_slot_for_confirmation(nearest)
                        out_text = f"Booked ✅ You're confirmed for {slot_display}. See you then!"
                        route = "booked"
                        context_updates["booked_booking"] = {
                            "slot": nearest,
                            "booking_id": booking_result["booking_id"],
                            "booked_at": now.isoformat(),
                        }
                        context_updates["last_offer"] = None
                        await _emit_booking_event(
                            conn,
                            tenant_id=ev.tenant_id,
                            ev=ev,
                            contact_id=contact_id,
                            slot_iso=nearest,
                            booking_id=booking_result["booking_id"],
                            now=now,
                        )
                    else:
                        out_text = "Sorry, I couldn't lock that slot in — it may have just gone. Want me to find another time?"
                        route = "booking_failed"
                else:
                    # Nothing close — offer 2 nearest alternatives
                    alts = _find_two_nearest_slots(all_slots_for_specific, preferred_day, target_hour, tz_str)
                    display_alts = format_slots_for_display(alts, timezone=tz_str)
                    if display_alts:
                        if len(display_alts) >= 2:
                            out_text = f"I don't have {explicit_time} I'm afraid. Nearest I've got is {display_alts[0]} or {display_alts[1]} — would either of those work?"
                        else:
                            out_text = f"I don't have {explicit_time} I'm afraid. Nearest I've got is {display_alts[0]} — does that work?"
                        new_last_offer = {"offered_slots": alts, "offered_at": now.isoformat(), "timezone": tz_str}
                        context_updates["last_offer"] = new_last_offer
                    else:
                        out_text = f"I'm afraid I don't have {explicit_time} available. What other times work for you?"
                    route = "offer_slots"
            else:
                # Couldn't parse time or no slots — fall back to broad offer, preserving day preference
                _fallback_day = llm_result.get("preferred_day")
                if _fallback_day:
                    class _FallbackSignals:
                        day = _fallback_day
                        time_window = None
                        explicit_time = None
                    class _FallbackRouteInfo:
                        route = "offer_slots"
                        signals = _FallbackSignals()
                    out_text, new_last_offer = await _handle_offer_slots(conn, ev.tenant_id, _FallbackRouteInfo())
                else:
                    out_text, new_last_offer = await _handle_offer_slots(conn, ev.tenant_id, _NullRouteInfo())
                context_updates["last_offer"] = new_last_offer
                route = "offer_slots"

        elif intent == "request_slots":
            # Broad availability request — offer slots for the requested day/time
            preferred_day = llm_result.get("preferred_day")
            preferred_time = llm_result.get("preferred_time")
            # Inherit time_window and explicit_time from pattern matcher when LLM didn't extract them
            _pm_sig = route_info.signals
            _pm_time_window = getattr(_pm_sig, "time_window", None)
            _pm_explicit_time = getattr(_pm_sig, "explicit_time", None)
            if preferred_day or preferred_time or _pm_time_window or _pm_explicit_time:
                _resolved_day = preferred_day or getattr(_pm_sig, "day", None)
                _resolved_time_window = preferred_time or _pm_time_window
                _resolved_explicit_time = _pm_explicit_time
                class _LLMSignals:
                    day = _resolved_day
                    time_window = _resolved_time_window
                    explicit_time = _resolved_explicit_time
                class _LLMRouteInfo:
                    route = "offer_slots"
                    signals = _LLMSignals()
                slot_route_info = _LLMRouteInfo()
            else:
                slot_route_info = route_info
            slot_text, new_last_offer = await _handle_offer_slots(conn, ev.tenant_id, slot_route_info)
            context_updates["last_offer"] = new_last_offer
            route = "offer_slots"
            # Check if the day preference was satisfied; if not, say so
            # Use resolved_day (which may come from pattern matcher) not just LLM preferred_day
            _check_day = preferred_day or getattr(route_info.signals, "day", None)
            preamble = ""
            if _check_day and new_last_offer.get("offered_slots"):
                offer_tz = new_last_offer.get("timezone", "Europe/London")
                _tz = ZoneInfo(offer_tz)
                slot_days = []
                for _s in new_last_offer["offered_slots"]:
                    _dt = datetime.fromisoformat(_s)
                    if _dt.tzinfo is None:
                        _dt = _dt.replace(tzinfo=_tz)
                    slot_days.append(_dt.strftime("%A").lower())
                if not any(_check_day.lower() in _d for _d in slot_days):
                    preamble = f"I don't have anything on {_check_day.capitalize()} I'm afraid —"
            out_text = f"{preamble} {slot_text}".strip() if preamble else slot_text

        elif intent == "wants_human" and llm_result.get("should_handoff"):
            # Handoff requested — bot steps back, human takes over
            # out_text is already set from LLM reply (natural "I'll get someone from the team...")
            context_updates["handoff_requested"] = {"at": now.isoformat()}
            route = "wants_human"

        elif intent == "decline":
            context_updates["declined"] = {"at": now.isoformat()}
            route = "decline"

        # else intent == "unclear" — LLM reply_text already set

    # Store context updates if any
    if context_updates:
        await conn.execute(UPDATE_CONVERSATION_CONTEXT_SQL, conversation_id, context_updates)

    # Close conversation on terminal outcomes
    if route in ("booked", "wants_human", "decline"):
        await conn.execute(CLOSE_CONVERSATION_SQL, conversation_id)

    # Create pending outbound message
    out_payload_dict: dict[str, Any] = {
        "send_status": "pending",
        "send_attempts": 0,
        "send_last_error": None,
        "route": route,
        "text_final": out_text,
        "llm": {
            "enabled": llm_settings.get("enabled", False),
            "used": llm_result.get("used", False),
            "model": llm_settings.get("model"),
            "error": llm_result.get("error"),
        },
    }
    if new_last_offer:
        out_payload_dict["offered_slots"] = new_last_offer.get("slots", [])
        out_payload_dict["calendar_check"] = new_last_offer.get("calendar_check")
    if booking_result:
        out_payload_dict["booking_result"] = booking_result

    out_message_id = await conn.fetchval(
        INSERT_OUTBOUND_MESSAGE_SQL,
        ev.tenant_id,
        conversation_id,
        contact_id,
        ev.provider,
        ev.channel,
        out_text,
        out_payload_dict,  # Pass dict directly - asyncpg codec handles JSON encoding
        ev.trace_id,  # $8 - propagate trace_id
    )

    # Glass-box: build debug snapshot for conversation context
    state_from = conv_context.get("_last_step", "start")
    state_to = route

    debug_snapshot = build_debug_snapshot(
        route=route,
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

    # Update conversation context with debug snapshot
    debug_context = {"debug": {"last_run": debug_snapshot}, "_last_step": state_to}
    await conn.execute(UPDATE_CONVERSATION_CONTEXT_SQL, conversation_id, debug_context)

    # Glass-box: structured logging
    tenant_slug = tenant.get("tenant_slug", ev.tenant_id)

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
        route=route,
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
        "route": route,
        "slot_matched": slot_matched,
        "booking_id": booking_result.get("booking_id") if booking_result else None,
        "trace_id": ev.trace_id,
    }
