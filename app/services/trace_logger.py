"""
Structured JSON logging for glass-box mode.

Logs minimal, structured records for each inbound processing run.
All fields are flat and queryable.
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from typing import Any, Optional

# Dedicated logger for trace events (separate from operational logs)
_trace_logger: Optional[logging.Logger] = None


def _get_trace_logger() -> logging.Logger:
    """Get or create the trace logger with JSON formatting."""
    global _trace_logger
    if _trace_logger is not None:
        return _trace_logger

    _trace_logger = logging.getLogger("humtech.trace")
    _trace_logger.setLevel(logging.INFO)
    _trace_logger.propagate = False  # Don't bubble to root logger

    # JSON handler to stdout
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)

    # Custom JSON formatter
    class JsonFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            # record.msg is already a dict for our trace logs
            if isinstance(record.msg, dict):
                return json.dumps(record.msg, default=str, ensure_ascii=False)
            return super().format(record)

    handler.setFormatter(JsonFormatter())
    _trace_logger.addHandler(handler)

    return _trace_logger


def log_processing_run(
    *,
    tenant_slug: str,
    contact_id: str,
    conversation_id: str,
    trace_id: str,
    route: str,
    signals: dict[str, Any],
    calendar_result: Optional[dict[str, Any]] = None,
    offered_slots: Optional[list[dict[str, str]]] = None,
    chosen_slot: Optional[dict[str, str]] = None,
    state_transition: Optional[dict[str, str]] = None,
) -> None:
    """
    Log a single structured record for an inbound processing run.

    Args:
        tenant_slug: Human-readable tenant identifier
        contact_id: UUID of the contact
        conversation_id: UUID of the conversation
        trace_id: UUID for end-to-end tracing
        route: Chosen route (e.g., "offer_slots", "clarify_day_time")
        signals: Extracted signals {day, time_window, explicit_time}
        calendar_result: {ok, returned_slots_count, provider_trace_id}
        offered_slots: List of {iso, human} for offered slots
        chosen_slot: {iso, human} for user-selected slot
        state_transition: {from, to} for state machine transition
    """
    logger = _get_trace_logger()

    record = {
        "type": "processing_run",
        "ts": datetime.utcnow().isoformat() + "Z",
        "tenant_slug": tenant_slug,
        "contact_id": contact_id,
        "conversation_id": conversation_id,
        "trace_id": trace_id,
        "route": route,
        "signals": signals,
    }

    # Optional fields (only include if present)
    if calendar_result is not None:
        record["calendar"] = calendar_result

    if offered_slots is not None:
        record["offered_slots"] = offered_slots

    if chosen_slot is not None:
        record["chosen_slot"] = chosen_slot

    if state_transition is not None:
        record["transition"] = state_transition

    logger.info(record)


def build_debug_snapshot(
    *,
    route: str,
    signals: dict[str, Any],
    slot_count: int,
    chosen_slots: Optional[list[dict[str, str]]] = None,
    transition: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """
    Build a minimal debug snapshot for conversation.context.debug.last_run.

    This is overwrite-only and must not bloat context.
    """
    return {
        "at": datetime.utcnow().isoformat() + "Z",
        "route": route,
        "signals": signals,
        "slot_count": slot_count,
        "chosen_slots": chosen_slots,
        "transition": transition,
    }
