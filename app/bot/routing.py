from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Optional
import re

# Day patterns with normalization (include plurals)
DAY_PATTERNS = {
    r"\b(mon|monday|mondays)\b": "monday",
    r"\b(tue|tues|tuesday|tuesdays)\b": "tuesday",
    r"\b(wed|wednesday|wednesdays)\b": "wednesday",
    r"\b(thu|thurs|thursday|thursdays)\b": "thursday",
    r"\b(fri|friday|fridays)\b": "friday",
    r"\b(sat|saturday|saturdays)\b": "saturday",
    r"\b(sun|sunday|sundays)\b": "sunday",
    r"\btoday\b": "today",
    r"\btomorrow\b": "tomorrow",
}

TIME_WINDOW_PATTERNS = {
    r"\bmorning\b": "morning",
    r"\bafternoon\b": "afternoon",
    r"\bevening\b": "evening",
}

# Patterns for inferring time window from numeric ranges
# "after 12", "from 12", "between 12 and 3", "12-3"
TIME_RANGE_PATTERN = re.compile(
    r"(?:after|from|between)?\s*(\d{1,2})(?:\s*(?:pm|am))?\s*(?:and|to|but before|-|–)?\s*(\d{1,2})?(?:\s*(?:pm|am))?",
    re.IGNORECASE
)


def _infer_time_window_from_hours(hour_start: int, hour_end: Optional[int] = None) -> Optional[str]:
    """Infer time window from hour range. Hours should be in 24h format or contextual 12h."""
    # Normalize hours (assume PM for 1-6 when no AM/PM given)
    if hour_start < 7:
        hour_start += 12  # 1-6 → 13-18 (afternoon/evening)

    if hour_end is not None and hour_end < 7:
        hour_end += 12

    # Determine window based on start hour
    if 5 <= hour_start < 12:
        return "morning"
    elif 12 <= hour_start < 17:
        return "afternoon"
    elif hour_start >= 17 or hour_start < 5:
        return "evening"
    return None

# Matches times like "2pm", "2:30pm", "14:00", "2 pm"
TIME_REGEX = re.compile(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b", re.IGNORECASE)


# Matches ordinals like "6th", "3rd", "21st"
_ORDINAL_RE = re.compile(r"\b(\d{1,2})(?:st|nd|rd|th)\b", re.IGNORECASE)

# Matches "March 6", "6 March", "6 march", etc.
_MONTH_NAMES = (
    r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?"
    r"|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?"
)
_MONTH_DATE_RE = re.compile(
    rf"\b(?:{_MONTH_NAMES})\s+(\d{{1,2}})\b"          # "March 6"
    rf"|\b(\d{{1,2}})\s+(?:{_MONTH_NAMES})\b",         # "6 March"
    re.IGNORECASE,
)


@dataclass
class Signals:
    day: Optional[str] = None
    time_window: Optional[str] = None
    explicit_time: Optional[str] = None
    explicit_date: Optional[int] = None   # day-of-month from "6th", "March 6", etc.
    raw_text: str = ""


@dataclass
class RouteInfo:
    route: str
    confidence: float
    signals: Signals


_NEGATION_RE = re.compile(
    r"\b(can't|cannot|doesn't|don't|wont|won't|not|no|never|doesnt|"
    r"doesn't work|can't do|won't work|wont work|doesn't suit|not available)\b",
    re.IGNORECASE,
)


def extract_signals(text: str) -> Signals:
    """Extract day, time_window, and explicit time from text."""
    t = (text or "").lower().strip()
    signals = Signals(raw_text=text or "")

    # Extract day — collect all matches, skip negated ones
    # e.g. "Tuesday doesn't work, how about Friday?" → picks Friday, not Tuesday
    day_matches: list[tuple[str, int]] = []  # (day_name, position)
    for pattern, day_name in DAY_PATTERNS.items():
        for m in re.finditer(pattern, t, re.IGNORECASE):
            day_matches.append((day_name, m.start()))

    affirmative: list[tuple[str, int]] = []
    for day_name, pos in day_matches:
        window = t[max(0, pos - 50): pos]
        if not _NEGATION_RE.search(window):
            affirmative.append((day_name, pos))

    if affirmative:
        # Pick earliest affirmative day mention
        signals.day = min(affirmative, key=lambda x: x[1])[0]
    elif day_matches:
        # All mentions are negated — pick the last one (probably what they're pivoting to)
        signals.day = max(day_matches, key=lambda x: x[1])[0]

    # Extract time window (explicit keywords first)
    for pattern, window in TIME_WINDOW_PATTERNS.items():
        if re.search(pattern, t):
            signals.time_window = window
            break

    # Extract explicit time
    time_match = TIME_REGEX.search(t)
    if time_match:
        hour = time_match.group(1)
        minutes = time_match.group(2) or "00"
        ampm = (time_match.group(3) or "").lower()
        signals.explicit_time = f"{hour}:{minutes}{ampm}".strip(":")

    # If no explicit time window found, try to infer from numeric times
    if signals.time_window is None and signals.explicit_time:
        try:
            hour_val = int(signals.explicit_time.split(":")[0])
            signals.time_window = _infer_time_window_from_hours(hour_val)
        except (ValueError, IndexError):
            pass

    # Extract explicit date (day-of-month) from ordinals or "Month day" patterns
    # e.g. "Friday 6th" → explicit_date=6, "March 6" → explicit_date=6
    month_m = _MONTH_DATE_RE.search(t)
    if month_m:
        # Group 1 = "Month D" form, group 2 = "D Month" form
        day_str = month_m.group(1) or month_m.group(2)
        try:
            d = int(day_str)
            if 1 <= d <= 31:
                signals.explicit_date = d
        except (ValueError, TypeError):
            pass
    else:
        ord_m = _ORDINAL_RE.search(t)
        if ord_m:
            try:
                d = int(ord_m.group(1))
                if 1 <= d <= 31:
                    signals.explicit_date = d
            except (ValueError, TypeError):
                pass

    return signals


def route_from_signals(signals: Signals) -> RouteInfo:
    """Determine route based on extracted signals."""
    has_day = signals.day is not None or signals.explicit_date is not None
    has_time_info = signals.time_window is not None or signals.explicit_time is not None

    if has_day and has_time_info:
        return RouteInfo(
            route="offer_slots",
            confidence=0.85,
            signals=signals,
        )
    if has_day:
        return RouteInfo(
            route="clarify_time_window",
            confidence=0.7,
            signals=signals,
        )
    return RouteInfo(
        route="clarify_day_time",
        confidence=0.5,
        signals=signals,
    )


def route_from_text(text: str) -> RouteInfo:
    """Main entry: extract signals and determine route."""
    signals = extract_signals(text)
    return route_from_signals(signals)


def compose_reply(route_info: RouteInfo) -> str:
    """Compose a contextual, one-question reply based on route and signals."""
    signals = route_info.signals
    route = route_info.route

    # Build a reflection of what the user said
    parts = []
    if signals.day:
        parts.append(signals.day.capitalize() if signals.day not in ("today", "tomorrow") else signals.day)
    if signals.time_window:
        parts.append(signals.time_window)
    if signals.explicit_time:
        parts.append(f"around {signals.explicit_time}")

    user_ref = " ".join(parts) if parts else None

    if route == "offer_slots":
        if user_ref:
            return f"Got it — {user_ref}. I'll check what's available and send you the closest options. Does that work?"
        return "Got it — I'll check what's available and send you the closest options. Does that work?"

    if route == "clarify_time_window":
        if user_ref:
            return f"Great, {user_ref} works. Do you prefer morning, afternoon, or evening?"
        return "Great — do you prefer morning, afternoon, or evening?"

    # clarify_day_time
    return "No problem — what day works for you, and would morning, afternoon, or evening be best?"


def route_info_to_dict(route_info: RouteInfo) -> dict[str, Any]:
    """Convert RouteInfo to a dict for storing in payload."""
    return {
        "route": route_info.route,
        "confidence": route_info.confidence,
        "signals": {
            "day": route_info.signals.day,
            "time_window": route_info.signals.time_window,
            "explicit_time": route_info.signals.explicit_time,
            "explicit_date": route_info.signals.explicit_date,
        },
    }
