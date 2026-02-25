from __future__ import annotations
import httpx
import json
import os
import uuid
from datetime import datetime, timedelta
from typing import Any, Optional
from zoneinfo import ZoneInfo


BASE_URL = "https://services.leadconnectorhq.com"


def get_stub_slots() -> list[str] | None:
    """
    Return stub slots from CALENDAR_STUB_SLOTS env var if set.

    Format: JSON array of ISO datetime strings, e.g.:
    CALENDAR_STUB_SLOTS='["2026-01-30T09:00:00Z","2026-01-30T14:00:00Z","2026-01-31T10:00:00Z"]'

    Returns None if env var is not set or empty.
    """
    raw = os.getenv("CALENDAR_STUB_SLOTS", "").strip()
    if not raw:
        return None
    try:
        slots = json.loads(raw)
        if isinstance(slots, list) and all(isinstance(s, str) for s in slots):
            return slots
        return None
    except json.JSONDecodeError:
        return None


async def get_free_slots(
    access_token: str,
    calendar_id: str,
    start_dt: datetime,
    end_dt: datetime,
    timezone: str = "Europe/London",
    user_id: Optional[str] = None,
) -> tuple[list[str], str | None]:
    """Fetch free slots from GHL calendar API. Returns (slots, trace_id)."""
    # Check for stub mode first (for deterministic testing)
    stub_slots = get_stub_slots()
    if stub_slots is not None:
        return stub_slots, "stub-trace-id"

    url = f"{BASE_URL}/calendars/{calendar_id}/free-slots"
    # GHL expects Unix timestamps in milliseconds
    params: dict[str, Any] = {
        "startDate": int(start_dt.timestamp() * 1000),
        "endDate": int(end_dt.timestamp() * 1000),
        "timezone": timezone,
    }
    if user_id:
        params["userId"] = user_id

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Version": "2021-07-28",
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(url, params=params, headers=headers)

    if r.status_code == 401:
        raise RuntimeError("Unauthorized: check token + calendars.readonly scope")

    r.raise_for_status()
    data = r.json()
    trace_id = data.get("traceId")

    slots: list[str] = []

    for day, blob in data.items():
        if day == "traceId":
            continue
        if not isinstance(blob, dict):
            continue

        day_slots = blob.get("slots")
        if not isinstance(day_slots, list):
            continue

        # slots are strings like "2026-01-27T20:00:00Z"
        for s in day_slots:
            if isinstance(s, str):
                slots.append(s)

    # de-dupe while preserving order
    seen: set[str] = set()
    slots_out: list[str] = []
    for s in slots:
        if s not in seen:
            seen.add(s)
            slots_out.append(s)

    return slots_out, trace_id


def filter_slots_by_signals(
    slots: list[str],
    day: str | None,
    time_window: str | None,
    timezone: str = "Europe/London",
    explicit_date: int | None = None,
) -> list[str]:
    """
    Filter slots by day, time_window, and optional explicit_date signals.

    day: 'monday', 'tuesday', ..., 'today', 'tomorrow'
    time_window: 'morning' (before 12), 'afternoon' (12-17), 'evening' (17+)
    explicit_date: day-of-month integer (1-31), e.g. 6 from "Friday 6th"

    Slots are ISO strings in UTC (e.g., "2026-01-30T00:00:00Z").
    Filtering is done in tenant timezone, returns original ISO strings sorted chronologically.
    """
    tz = ZoneInfo(timezone)
    utc = ZoneInfo("UTC")
    now = datetime.now(tz)

    # Map day names to weekday integers (0=Monday)
    day_map = {
        "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
        "friday": 4, "saturday": 5, "sunday": 6,
    }

    # Time window hour ranges (in local timezone)
    window_ranges = {
        "morning": (0, 12),
        "afternoon": (12, 17),
        "evening": (17, 24),
    }

    # Parse and convert slots to tenant timezone for filtering
    filtered: list[tuple[datetime, str]] = []
    for slot_iso in slots:
        try:
            slot_dt = datetime.fromisoformat(slot_iso.replace("Z", "+00:00"))
            # Ensure timezone-aware (assume UTC if naive)
            if slot_dt.tzinfo is None:
                slot_dt = slot_dt.replace(tzinfo=utc)
            # Convert to tenant timezone for filtering
            slot_local = slot_dt.astimezone(tz)
        except ValueError:
            continue

        # Filter by day (using local date)
        if day:
            if day == "today":
                if slot_local.date() != now.date():
                    continue
            elif day == "tomorrow":
                if slot_local.date() != (now + timedelta(days=1)).date():
                    continue
            elif day in day_map:
                if slot_local.weekday() != day_map[day]:
                    continue

        # Filter by specific calendar date (day-of-month), e.g. "the 6th"
        if explicit_date is not None:
            if slot_local.day != explicit_date:
                continue

        # Filter by time window (using local hour)
        if time_window and time_window in window_ranges:
            start_hour, end_hour = window_ranges[time_window]
            if not (start_hour <= slot_local.hour < end_hour):
                continue

        filtered.append((slot_dt, slot_iso))

    # Sort chronologically and return original ISO strings
    filtered.sort(key=lambda x: x[0])
    return [slot_iso for _, slot_iso in filtered]


def pick_soonest_two_slots(
    slots: list[str],
    timezone: str = "Europe/London",
    contrast_pool: list[str] | None = None,
    target_hour: float | None = None,
) -> list[str]:
    """
    Pick exactly 2 slots:
      A = closest match to user preference (first from slots)
      B = contrasting morning/afternoon from contrast_pool, else next-closest from slots

    Slots are UTC ISO strings. Categorization uses tenant local time.
    Morning: hour < 12, Afternoon: 12 <= hour < 17, Evening: hour >= 17.
    Returns [] if slots is empty, sorted [A, B] chronologically.
    """
    if not slots:
        return []

    tz = ZoneInfo(timezone)
    utc = ZoneInfo("UTC")

    def parse_slot(slot_iso: str) -> tuple[datetime, datetime, str] | None:
        try:
            slot_dt = datetime.fromisoformat(slot_iso.replace("Z", "+00:00"))
            if slot_dt.tzinfo is None:
                slot_dt = slot_dt.replace(tzinfo=utc)
            local_dt = slot_dt.astimezone(tz)
            return (slot_dt, local_dt, slot_iso)
        except ValueError:
            return None

    def get_time_category(local_dt: datetime) -> str:
        hour = local_dt.hour
        if hour < 12:
            return "morning"
        elif hour < 17:
            return "afternoon"
        return "evening"

    # Parse preference-matched slots
    parsed: list[tuple[datetime, datetime, str]] = []
    for slot_iso in slots:
        p = parse_slot(slot_iso)
        if p:
            parsed.append(p)

    if not parsed:
        return []

    # Sort by UTC time (chronological)
    parsed.sort(key=lambda x: x[0])

    # If target_hour is set, re-sort by proximity to that time of day then pick nearest 2
    if target_hour is not None:
        parsed.sort(key=lambda x: abs(x[1].hour + x[1].minute / 60 - target_hour))
        result = [iso for _, _, iso in parsed[:2]]
        result_parsed = [(datetime.fromisoformat(iso.replace("Z", "+00:00")), iso) for iso in result]
        result_parsed.sort(key=lambda x: x[0])
        return [iso for _, iso in result_parsed]

    # Slot A = first preference-matched slot (closest to user preference)
    slot_a_utc, slot_a_local, slot_a_iso = parsed[0]
    slot_a_category = get_time_category(slot_a_local)

    # Determine contrasting category
    if slot_a_category == "morning":
        contrast_category = "afternoon"
    elif slot_a_category == "afternoon":
        contrast_category = "morning"
    else:  # evening
        contrast_category = "morning"  # contrast evening with morning

    # Parse contrast pool (or use slots if not provided)
    pool = contrast_pool if contrast_pool else slots
    pool_parsed: list[tuple[datetime, datetime, str]] = []
    for slot_iso in pool:
        p = parse_slot(slot_iso)
        if p and p[2] != slot_a_iso:  # exclude slot A
            pool_parsed.append(p)

    pool_parsed.sort(key=lambda x: x[0])

    # Look for contrasting slot in pool
    slot_b_iso: str | None = None
    for utc_dt, local_dt, iso in pool_parsed:
        if get_time_category(local_dt) == contrast_category:
            slot_b_iso = iso
            break

    # Fallback: next chronological from preference-matched slots (excluding A)
    if not slot_b_iso and len(parsed) > 1:
        slot_b_iso = parsed[1][2]

    # Build result
    if slot_b_iso:
        # Sort A and B chronologically
        result = [slot_a_iso, slot_b_iso]
        result_parsed = [(datetime.fromisoformat(iso.replace("Z", "+00:00")), iso) for iso in result]
        result_parsed.sort(key=lambda x: x[0])
        return [iso for _, iso in result_parsed]

    # Only one slot available
    return [slot_a_iso]


def format_slots_for_display(slots: list[str], timezone: str = "Europe/London") -> list[str]:
    """
    Format slots for user-friendly display in tenant local time.

    Slots are UTC ISO strings (e.g., "2026-01-30T14:00:00Z").
    Output: "Friday 09:00" in tenant local time.
    """
    tz = ZoneInfo(timezone)
    utc = ZoneInfo("UTC")
    formatted: list[str] = []
    for slot_iso in slots:
        try:
            slot_dt = datetime.fromisoformat(slot_iso.replace("Z", "+00:00"))
            if slot_dt.tzinfo is None:
                slot_dt = slot_dt.replace(tzinfo=utc)
            # Convert to tenant local time for display
            local_dt = slot_dt.astimezone(tz)
            formatted.append(local_dt.strftime("%A %H:%M"))
        except ValueError:
            continue
    return formatted


def filter_by_availability_windows(
    slots: list[str],
    availability: dict[str, list[dict[str, str]]] | None,
    timezone: str = "Europe/London",
) -> list[str]:
    """
    Filter slots by tenant-configured availability windows.

    availability format:
    {
        "mon": [{"start": "09:00", "end": "17:00"}],
        "tue": [{"start": "09:00", "end": "12:00"}, {"start": "14:00", "end": "17:00"}],
        ...
    }

    If availability is None or empty, returns all slots unfiltered.
    Slots are UTC ISO strings; filtering is done in tenant local time.
    """
    if not availability:
        return slots

    tz = ZoneInfo(timezone)
    utc = ZoneInfo("UTC")

    day_abbrev = {
        0: "mon", 1: "tue", 2: "wed", 3: "thu",
        4: "fri", 5: "sat", 6: "sun",
    }

    filtered: list[tuple[datetime, str]] = []
    for slot_iso in slots:
        try:
            slot_dt = datetime.fromisoformat(slot_iso.replace("Z", "+00:00"))
            if slot_dt.tzinfo is None:
                slot_dt = slot_dt.replace(tzinfo=utc)
            local_dt = slot_dt.astimezone(tz)
        except ValueError:
            continue

        # Get day abbreviation
        day_key = day_abbrev.get(local_dt.weekday())
        if not day_key:
            continue

        # Get windows for this day
        windows = availability.get(day_key)
        if not windows:
            # No windows defined for this day = not available
            continue

        # Check if slot falls within any window
        slot_time = local_dt.strftime("%H:%M")
        in_window = False
        for window in windows:
            start = window.get("start", "00:00")
            end = window.get("end", "23:59")
            if start <= slot_time < end:
                in_window = True
                break

        if in_window:
            filtered.append((slot_dt, slot_iso))

    # Sort chronologically
    filtered.sort(key=lambda x: x[0])
    return [iso for _, iso in filtered]


BOOKING_STUB_ENABLED_KEY = "BOOKING_STUB"

LOAD_CONTACT_SQL = """
SELECT channel_address, metadata
FROM bot.contacts
WHERE contact_id = $1::uuid;
"""

LOAD_TENANT_CALENDAR_SQL = """
SELECT settings
FROM core.tenants
WHERE tenant_id = $1::uuid AND is_enabled = TRUE;
"""


def _resolve_ghl_contact_id(
    contact_row: dict[str, Any] | None,
    metadata: dict[str, Any] | None,
) -> str | None:
    """Extract GHL contactId from contact metadata or booking metadata."""
    # Caller-provided metadata (e.g. from event payload)
    if metadata:
        for key in ("contactId", "ghl_contact_id", "contact_id"):
            val = metadata.get(key)
            if isinstance(val, str) and val:
                return val

    # Contact record metadata
    if contact_row:
        cmeta = contact_row.get("metadata")
        if isinstance(cmeta, dict):
            for key in ("contactId", "ghl_contact_id", "contact_id"):
                val = cmeta.get(key)
                if isinstance(val, str) and val:
                    return val

    return None


async def book_slot(
    tenant_id: str,
    slot_iso: str,
    contact_id: str,
    conversation_id: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Book a slot via the GHL Calendar Events API.

    POST https://services.leadconnectorhq.com/calendars/events

    Falls back to stub mode when BOOKING_STUB env var is set (for testing).
    On 401: refreshes the token once and retries.
    """
    import logging
    from app.db import get_pool
    from app.adapters.ghl.auth import get_valid_token

    logger = logging.getLogger(__name__)

    # Stub mode for testing
    if os.getenv(BOOKING_STUB_ENABLED_KEY):
        booking_id = f"stub-{uuid.uuid4().hex[:12]}"
        return {
            "success": True,
            "booking_id": booking_id,
            "slot": slot_iso,
            "tenant_id": tenant_id,
            "contact_id": contact_id,
            "conversation_id": conversation_id,
        }

    pool = await get_pool()
    async with pool.acquire() as conn:
        # Load contact record for GHL contactId resolution
        contact_row = await conn.fetchrow(LOAD_CONTACT_SQL, contact_id)
        contact_dict = dict(contact_row) if contact_row else None

        ghl_contact_id = _resolve_ghl_contact_id(contact_dict, metadata)
        if not ghl_contact_id:
            return {
                "success": False,
                "error": "no_ghl_contact_id",
                "detail": "Could not resolve GHL contactId from contact metadata",
            }

        # Load calendar settings from tenant
        tenant_row = await conn.fetchrow(LOAD_TENANT_CALENDAR_SQL, tenant_id)
        if not tenant_row:
            return {"success": False, "error": "tenant_not_found"}

        raw_settings = tenant_row["settings"]
        if isinstance(raw_settings, str):
            raw_settings = json.loads(raw_settings)
        settings = raw_settings if isinstance(raw_settings, dict) else {}
        cal = settings.get("calendar") or {}
        calendar_id = cal.get("calendar_id")
        tz = settings.get("timezone") or cal.get("timezone") or "Europe/London"

        if not calendar_id:
            return {"success": False, "error": "missing_calendar_id"}

        # Parse slot times
        slot_dt = datetime.fromisoformat(slot_iso.replace("Z", "+00:00"))
        if slot_dt.tzinfo is None:
            slot_dt = slot_dt.replace(tzinfo=ZoneInfo("UTC"))
        slot_duration = int(cal.get("slot_duration_minutes") or 60)
        end_dt = slot_dt + timedelta(minutes=slot_duration)

        body: dict[str, Any] = {
            "calendarId": calendar_id,
            "contactId": ghl_contact_id,
            "startTime": slot_dt.isoformat(),
            "endTime": end_dt.isoformat(),
            "timezone": tz,
            "title": "Appointment (bot-booked)",
        }

        # Get valid token (handles refresh internally)
        access_token = await get_valid_token(conn, tenant_id)

        # Add locationId only if stored — Private Integration tokens are location-scoped
        # so omitting it lets GHL infer the location from the token
        cred_row = await conn.fetchval(
            "SELECT credentials FROM core.tenant_credentials "
            "WHERE tenant_id = $1::uuid AND provider = 'ghl'",
            tenant_id,
        )
        if cred_row:
            from app.utils.crypto import decrypt_credentials as _decrypt
            try:
                cred_data = _decrypt(bytes(cred_row))
                location_id = cred_data.get("location_id")
                if location_id:
                    body["locationId"] = location_id
            except Exception:
                pass

    # API call (outside DB connection — no need to hold it during HTTP)
    url = f"{BASE_URL}/calendars/events/appointments"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Version": "2021-07-28",
    }

    logger.info(json.dumps({
        "event": "ghl_book_slot_request",
        "tenant_id": tenant_id,
        "calendar_id": calendar_id,
        "contact_id": ghl_contact_id,
        "start_time": body["startTime"],
    }))

    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(url, json=body, headers=headers)

        # On 401: refresh token once and retry
        if resp.status_code == 401:
            logger.info(json.dumps({
                "event": "ghl_book_slot_401_retry",
                "tenant_id": tenant_id,
            }))
            async with pool.acquire() as conn:
                access_token = await get_valid_token(conn, tenant_id)
            headers["Authorization"] = f"Bearer {access_token}"
            resp = await client.post(url, json=body, headers=headers)

    logger.info(json.dumps({
        "event": "ghl_book_slot_response",
        "tenant_id": tenant_id,
        "status": resp.status_code,
        "body": resp.text[:500],
    }))

    if resp.status_code not in (200, 201):
        return {
            "success": False,
            "error": f"ghl_api_error:{resp.status_code}",
            "detail": resp.text[:300],
        }

    data = resp.json()
    booking_id = data.get("id") or data.get("eventId") or f"ghl-{uuid.uuid4().hex[:12]}"

    return {
        "success": True,
        "booking_id": booking_id,
        "slot": slot_iso,
        "tenant_id": tenant_id,
        "contact_id": contact_id,
        "conversation_id": conversation_id,
        "raw_response": data,
    }


async def cancel_booking(
    tenant_id: str,
    booking_id: str,
) -> dict[str, Any]:
    """
    Cancel a booking via the GHL Calendar Events API.

    DELETE https://services.leadconnectorhq.com/calendars/events/{booking_id}

    On 401: refreshes the token once and retries.
    """
    import logging
    from app.db import get_pool
    from app.adapters.ghl.auth import get_valid_token

    logger = logging.getLogger(__name__)

    pool = await get_pool()
    async with pool.acquire() as conn:
        access_token = await get_valid_token(conn, tenant_id)

    url = f"{BASE_URL}/calendars/events/{booking_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Version": "2021-07-28",
    }

    logger.info(json.dumps({
        "event": "ghl_cancel_booking_request",
        "tenant_id": tenant_id,
        "booking_id": booking_id,
    }))

    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.delete(url, headers=headers)

        if resp.status_code == 401:
            logger.info(json.dumps({
                "event": "ghl_cancel_booking_401_retry",
                "tenant_id": tenant_id,
            }))
            async with pool.acquire() as conn:
                access_token = await get_valid_token(conn, tenant_id)
            headers["Authorization"] = f"Bearer {access_token}"
            resp = await client.delete(url, headers=headers)

    logger.info(json.dumps({
        "event": "ghl_cancel_booking_response",
        "tenant_id": tenant_id,
        "booking_id": booking_id,
        "status": resp.status_code,
    }))

    if resp.status_code in (200, 204):
        return {"success": True, "booking_id": booking_id}

    return {
        "success": False,
        "error": f"ghl_api_error:{resp.status_code}",
        "detail": resp.text[:300],
    }
