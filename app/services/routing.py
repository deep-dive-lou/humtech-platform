from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Optional
import re

# Day patterns with normalization
DAY_PATTERNS = {
    r"\b(mon|monday)\b": "monday",
    r"\b(tue|tues|tuesday)\b": "tuesday",
    r"\b(wed|wednesday)\b": "wednesday",
    r"\b(thu|thurs|thursday)\b": "thursday",
    r"\b(fri|friday)\b": "friday",
    r"\b(sat|saturday)\b": "saturday",
    r"\b(sun|sunday)\b": "sunday",
    r"\btoday\b": "today",
    r"\btomorrow\b": "tomorrow",
}

TIME_WINDOW_PATTERNS = {
    r"\bmorning\b": "morning",
    r"\bafternoon\b": "afternoon",
    r"\bevening\b": "evening",
}

# Matches times like "2pm", "2:30pm", "14:00", "2 pm"
TIME_REGEX = re.compile(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b", re.IGNORECASE)


@dataclass
class Signals:
    day: Optional[str] = None
    time_window: Optional[str] = None
    explicit_time: Optional[str] = None
    raw_text: str = ""


@dataclass
class RouteInfo:
    route: str
    confidence: float
    signals: Signals


def extract_signals(text: str) -> Signals:
    """Extract day, time_window, and explicit time from text."""
    t = (text or "").lower().strip()
    signals = Signals(raw_text=text or "")

    # Extract day
    for pattern, day_name in DAY_PATTERNS.items():
        if re.search(pattern, t):
            signals.day = day_name
            break

    # Extract time window
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

    return signals


def route_from_signals(signals: Signals) -> RouteInfo:
    """Determine route based on extracted signals."""
    has_day = signals.day is not None
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
        },
    }
