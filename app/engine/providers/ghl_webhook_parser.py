from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional


@dataclass(frozen=True)
class NormalizedWebhookEvent:
    tenant_id: Optional[str]
    provider: str
    lead_external_id: str
    contact_external_id: Optional[str]
    raw_stage: Optional[str]
    event_type: str
    occurred_at: datetime
    source_event_id: str
    lead_value: Optional[float]
    raw_payload: dict[str, Any]
    location_id: Optional[str]


def _deep_get(data: dict[str, Any], *path: str) -> Any:
    cur: Any = data
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _first_non_empty(payload: dict[str, Any], *keys: str) -> Optional[str]:
    for key in keys:
        val = payload.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return None


def _parse_dt(value: Any) -> datetime:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    if isinstance(value, str) and value.strip():
        txt = value.strip()
        if txt.endswith("Z"):
            txt = txt[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(txt)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            return datetime.now(tz=timezone.utc)
    return datetime.now(tz=timezone.utc)


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _deterministic_event_id(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def parse_ghl_webhook(payload: dict[str, Any]) -> NormalizedWebhookEvent:
    """
    Parse a GHL webhook payload into a provider-agnostic internal contract.
    """
    opportunity = payload.get("opportunity")
    contact = payload.get("contact")

    lead_external_id = (
        _first_non_empty(
            payload,
            "opportunityId",
            "opportunity_id",
            "leadId",
            "lead_id",
        )
        or (opportunity.get("id") if isinstance(opportunity, dict) else None)
        or _first_non_empty(payload, "contactId", "contact_id")
    )
    if not lead_external_id:
        raise ValueError("Missing lead external id in GHL webhook payload")

    contact_external_id = (
        _first_non_empty(payload, "contactId", "contact_id")
        or (contact.get("id") if isinstance(contact, dict) else None)
        or _deep_get(opportunity if isinstance(opportunity, dict) else {}, "contact", "id")
    )

    raw_stage = (
        _first_non_empty(payload, "stage", "stageName", "pipelineStage")
        or (opportunity.get("stage") if isinstance(opportunity, dict) else None)
        or _deep_get(payload, "meta", "stage")
    )

    event_type_raw = _first_non_empty(payload, "type", "event", "eventType", "triggerType") or ""
    event_type = "lead_created"
    raw_lower = event_type_raw.lower()
    if "stage" in raw_lower or "status" in raw_lower or "pipeline" in raw_lower:
        event_type = "stage_changed"

    occurred_at = _parse_dt(
        payload.get("occurredAt")
        or payload.get("occurred_at")
        or payload.get("timestamp")
        or payload.get("updatedAt")
        or payload.get("createdAt")
        or _deep_get(payload, "meta", "timestamp")
    )

    source_event_id = (
        _first_non_empty(
            payload,
            "webhookId",
            "webhook_id",
            "eventId",
            "event_id",
            "id",
            "messageId",
            "deliveryId",
        )
        or _deterministic_event_id(payload)
    )

    location_id = (
        _first_non_empty(payload, "locationId", "location_id")
        or _deep_get(payload, "meta", "locationId")
        or _deep_get(opportunity if isinstance(opportunity, dict) else {}, "locationId")
    )

    tenant_id = _first_non_empty(payload, "tenant_id", "tenantId")

    lead_value = _to_float(
        payload.get("lead_value")
        or payload.get("monetaryValue")
        or payload.get("value")
        or payload.get("amount")
        or (opportunity.get("monetaryValue") if isinstance(opportunity, dict) else None)
    )

    return NormalizedWebhookEvent(
        tenant_id=tenant_id,
        provider="ghl",
        lead_external_id=lead_external_id,
        contact_external_id=contact_external_id,
        raw_stage=raw_stage,
        event_type=event_type,
        occurred_at=occurred_at,
        source_event_id=source_event_id,
        lead_value=lead_value,
        raw_payload=payload,
        location_id=location_id,
    )

