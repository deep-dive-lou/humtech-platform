from __future__ import annotations
import httpx
import uuid
from datetime import datetime, timedelta
from typing import Any, Optional
from zoneinfo import ZoneInfo


BASE_URL = "https://services.leadconnectorhq.com"


async def get_free_slots(
    access_token: str,
    calendar_id: str,
    start_dt: datetime,
    end_dt: datetime,
    timezone: str = "Europe/London",
    user_id: Optional[str] = None,
) -> list[str]:
    """Fetch free slots from GHL calendar API."""
    url = f"{BASE_URL}/calendars/{calendar_id}/free-slots"
    params: dict[str, Any] = {
        "startDate": start_dt.isoformat(),
        "endDate": end_dt.isoformat(),
        "timezone": timezone,
    }
    if user_id:
        params["userId"] = user_id

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(url, params=params, headers=headers)

    if r.status_code == 401:
        raise RuntimeError("Unauthorized: check token + calendars.readonly scope")

    r.raise_for_status()
    data = r.json()

    # Docs: "Availability map keyed by date (YYYY-MM-DD)"
    availability = data.get("availability") if isinstance(data, dict) else None
    if availability is None:
        # be defensive - sometimes response wrapper differs
        availability = data.get("data") if isinstance(data, dict) else data

    slots: list[str] = []
    if isinstance(availability, dict):
        for _, day_slots in availability.items():
            if isinstance(day_slots, list):
                for s in day_slots:
                    if isinstance(s, str):
                        slots.append(s)
                    elif isinstance(s, dict):
                        # common keys in various APIs
                        for k in ("startTime", "start", "slot", "dateTime"):
                            if k in s and isinstance(s[k], str):
                                slots.append(s[k])
                                break

    return sorted(set(slots))


def filter_slots_by_signals(
    slots: list[str],
    day: str | None,
    time_window: str | None,
    timezone: str = "Europe/London",
) -> list[str]:
    """
    Filter slots by day and time_window signals.

    day: 'monday', 'tuesday', ..., 'today', 'tomorrow'
    time_window: 'morning' (before 12), 'afternoon' (12-17), 'evening' (17+)
    """
    tz = ZoneInfo(timezone)
    now = datetime.now(tz)

    # Map day names to weekday integers (0=Monday)
    day_map = {
        "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
        "friday": 4, "saturday": 5, "sunday": 6,
    }

    # Time window hour ranges
    window_ranges = {
        "morning": (0, 12),
        "afternoon": (12, 17),
        "evening": (17, 24),
    }

    filtered: list[str] = []
    for slot_iso in slots:
        try:
            slot_dt = datetime.fromisoformat(slot_iso)
        except ValueError:
            continue

        # Filter by day
        if day:
            if day == "today":
                if slot_dt.date() != now.date():
                    continue
            elif day == "tomorrow":
                if slot_dt.date() != (now + timedelta(days=1)).date():
                    continue
            elif day in day_map:
                if slot_dt.weekday() != day_map[day]:
                    continue

        # Filter by time window
        if time_window and time_window in window_ranges:
            start_hour, end_hour = window_ranges[time_window]
            if not (start_hour <= slot_dt.hour < end_hour):
                continue

        filtered.append(slot_iso)

    return filtered


def format_slots_for_display(slots: list[str], timezone: str = "Europe/London") -> list[str]:
    """Format slots for user-friendly display."""
    tz = ZoneInfo(timezone)
    formatted: list[str] = []
    for slot_iso in slots:
        try:
            slot_dt = datetime.fromisoformat(slot_iso)
            if slot_dt.tzinfo is None:
                slot_dt = slot_dt.replace(tzinfo=tz)
            # e.g., "Tuesday 14:00"
            formatted.append(slot_dt.strftime("%A %H:%M"))
        except ValueError:
            continue
    return formatted


async def book_slot(
    tenant_id: str,
    slot_iso: str,
    contact_id: str,
    conversation_id: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Book a slot in the calendar.

    Stub: returns success with a fake booking_id.
    TODO: call GHL calendar API when ready.
    """
    # Generate a fake booking ID
    booking_id = f"stub-{uuid.uuid4().hex[:12]}"

    return {
        "success": True,
        "booking_id": booking_id,
        "slot": slot_iso,
        "tenant_id": tenant_id,
        "contact_id": contact_id,
        "conversation_id": conversation_id,
    }
