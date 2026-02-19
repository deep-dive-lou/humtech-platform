"""
Engine write layer: lead resolution and event recording.

Both the bot (processor.py) and webhook endpoints call these functions.
No imports from app.bot — this module is self-contained.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional
from uuid import uuid4

import asyncpg


# ---------------------------------------------------------------------------
# SQL — Lead resolution
# ---------------------------------------------------------------------------

FIND_OPEN_LEAD_SQL = """
SELECT lead_id::text
FROM engine.leads
WHERE tenant_id = $1::uuid
  AND contact_provider = $2::text
  AND contact_external_id = $3::text
  AND is_open = TRUE
ORDER BY created_at DESC
LIMIT 1;
"""

UPSERT_LEAD_SQL = """
INSERT INTO engine.leads (
    tenant_id, provider, external_id,
    contact_provider, contact_external_id,
    name, pipeline_name, current_stage, raw_stage,
    source, lead_value, currency, metadata
)
VALUES (
    $1::uuid, $2::text, $3::text,
    $4::text, $5::text,
    $6::text, $7::text, COALESCE($8::text, 'lead_created'), $9::text,
    $10::text, $11::numeric, $12::text, COALESCE($13::jsonb, '{}'::jsonb)
)
ON CONFLICT (tenant_id, provider, external_id)
DO UPDATE SET
    contact_provider    = COALESCE(EXCLUDED.contact_provider, engine.leads.contact_provider),
    contact_external_id = COALESCE(EXCLUDED.contact_external_id, engine.leads.contact_external_id),
    name                = COALESCE(EXCLUDED.name, engine.leads.name),
    pipeline_name       = COALESCE(EXCLUDED.pipeline_name, engine.leads.pipeline_name),
    current_stage       = CASE
        WHEN $8::text IS NULL THEN engine.leads.current_stage
        ELSE EXCLUDED.current_stage
    END,
    raw_stage           = COALESCE(EXCLUDED.raw_stage, engine.leads.raw_stage),
    source              = COALESCE(EXCLUDED.source, engine.leads.source),
    lead_value          = COALESCE(EXCLUDED.lead_value, engine.leads.lead_value),
    currency            = COALESCE(EXCLUDED.currency, engine.leads.currency),
    metadata            = engine.leads.metadata || COALESCE(EXCLUDED.metadata, '{}'::jsonb),
    updated_at          = now()
RETURNING lead_id::text;
"""

# ---------------------------------------------------------------------------
# SQL — Lead lifecycle updates
# ---------------------------------------------------------------------------

UPDATE_LEAD_STAGE_SQL = """
UPDATE engine.leads
SET current_stage = $2::text,
    updated_at = now()
WHERE lead_id = $1::uuid;
"""

UPDATE_LEAD_WON_SQL = """
UPDATE engine.leads
SET current_stage = $2::text,
    is_open = FALSE,
    won_at = $3::timestamptz,
    closed_reason = 'won',
    updated_at = now()
WHERE lead_id = $1::uuid;
"""

UPDATE_LEAD_LOST_SQL = """
UPDATE engine.leads
SET current_stage = $2::text,
    is_open = FALSE,
    lost_at = $3::timestamptz,
    closed_reason = 'lost',
    updated_at = now()
WHERE lead_id = $1::uuid;
"""

UPDATE_LEAD_VALUE_SQL = """
UPDATE engine.leads
SET lead_value = $2::numeric,
    currency = COALESCE($3::text, currency),
    updated_at = now()
WHERE lead_id = $1::uuid;
"""

# ---------------------------------------------------------------------------
# SQL — Event recording
# ---------------------------------------------------------------------------

INSERT_LEAD_EVENT_SQL = """
INSERT INTO engine.lead_events (
    tenant_id, lead_id,
    event_type, canonical_stage,
    from_stage, to_stage,
    source, source_event_id, actor,
    amount, currency,
    payload, occurred_at
)
VALUES (
    $1::uuid, $2::uuid,
    $3::text, $4::text,
    $5::text, $6::text,
    $7::text, $8::text, $9::text,
    $10::numeric, $11::text,
    COALESCE($12::jsonb, '{}'::jsonb), $13::timestamptz
)
ON CONFLICT (tenant_id, lead_id, source, source_event_id) DO NOTHING
RETURNING event_id::text;
"""

# ---------------------------------------------------------------------------
# SQL — Stage mapping lookup
# ---------------------------------------------------------------------------

RESOLVE_STAGE_MAPPING_SQL = """
SELECT canonical_stage, stage_order
FROM engine.stage_mappings
WHERE tenant_id = $1::uuid
  AND provider = $2::text
  AND raw_stage = $3::text
  AND is_active = TRUE
LIMIT 1;
"""

# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------


async def resolve_or_create_lead(
    conn: asyncpg.Connection,
    *,
    tenant_id: str,
    contact_provider: str,
    contact_external_id: str,
    provider: Optional[str] = None,
    external_id: Optional[str] = None,
    name: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    current_stage: str = "lead_created",
    raw_stage: Optional[str] = None,
    source: Optional[str] = None,
    lead_value: Optional[float] = None,
    currency: Optional[str] = None,
    metadata: Optional[dict[str, Any]] = None,
) -> str:
    """
    Find an existing open lead for this contact, or create one.

    Resolution order:
    1. Look for an open lead matching (tenant_id, contact_provider, contact_external_id)
    2. If found → return its lead_id
    3. If not found → upsert a new lead and return the lead_id

    When the bot creates a lead (no CRM external_id), provider defaults to
    'bot' and external_id is a generated UUID.

    Returns: lead_id (str)
    """
    # Step 1: try to find an existing open lead by contact
    existing = await conn.fetchval(
        FIND_OPEN_LEAD_SQL,
        tenant_id,
        contact_provider,
        contact_external_id,
    )
    if existing:
        return existing

    # Step 2: no open lead found — upsert a new one
    eff_provider = provider or "bot"
    eff_external_id = external_id or f"bot-{uuid4().hex}"

    lead_id = await conn.fetchval(
        UPSERT_LEAD_SQL,
        tenant_id,
        eff_provider,
        eff_external_id,
        contact_provider,
        contact_external_id,
        name,
        pipeline_name,
        current_stage,
        raw_stage,
        source,
        lead_value,
        currency,
        metadata,
    )
    return lead_id


async def upsert_lead(
    conn: asyncpg.Connection,
    *,
    tenant_id: str,
    provider: str,
    external_id: str,
    contact_provider: Optional[str] = None,
    contact_external_id: Optional[str] = None,
    name: Optional[str] = None,
    pipeline_name: Optional[str] = None,
    current_stage: Optional[str] = None,
    raw_stage: Optional[str] = None,
    source: Optional[str] = None,
    lead_value: Optional[float] = None,
    currency: Optional[str] = None,
    metadata: Optional[dict[str, Any]] = None,
) -> str:
    """
    Upsert a lead by external identity (tenant_id, provider, external_id).

    Unlike resolve_or_create_lead(), this does not attempt contact-based
    lookup first. Use this for CRM webhook ingestion where external lead IDs
    are authoritative.
    """
    lead_id = await conn.fetchval(
        UPSERT_LEAD_SQL,
        tenant_id,
        provider,
        external_id,
        contact_provider,
        contact_external_id,
        name,
        pipeline_name,
        current_stage,
        raw_stage,
        source,
        lead_value,
        currency,
        metadata,
    )
    return lead_id


async def resolve_stage_mapping(
    conn: asyncpg.Connection,
    *,
    tenant_id: str,
    provider: str,
    raw_stage: str,
) -> Optional[str]:
    """
    Resolve a tenant/provider raw stage to canonical stage.
    Returns canonical_stage or None if no active mapping exists.
    """
    row = await conn.fetchrow(
        RESOLVE_STAGE_MAPPING_SQL,
        tenant_id,
        provider,
        raw_stage,
    )
    if not row:
        return None
    return row["canonical_stage"]


async def write_lead_event(
    conn: asyncpg.Connection,
    *,
    lead_id: str,
    tenant_id: str,
    event_type: str,
    source: str,
    occurred_at: datetime,
    canonical_stage: Optional[str] = None,
    from_stage: Optional[str] = None,
    to_stage: Optional[str] = None,
    source_event_id: Optional[str] = None,
    actor: Optional[str] = None,
    amount: Optional[float] = None,
    currency: Optional[str] = None,
    payload: Optional[dict[str, Any]] = None,
) -> Optional[str]:
    """
    Append an event to engine.lead_events.

    Idempotent: if (tenant_id, lead_id, source, source_event_id) already
    exists, the INSERT is skipped (ON CONFLICT DO NOTHING) and None is
    returned. Otherwise returns the new event_id.

    Side effects based on event_type:
    - stage_changed  → updates engine.leads.current_stage
    - lead_won       → closes the lead (is_open=FALSE, won_at)
    - lead_lost      → closes the lead (is_open=FALSE, lost_at)
    - value_changed  → updates engine.leads.lead_value
    """
    # Generate idempotency key if caller didn't provide one
    eff_source_event_id = source_event_id or f"{source}-{uuid4().hex}"

    event_id = await conn.fetchval(
        INSERT_LEAD_EVENT_SQL,
        tenant_id,
        lead_id,
        event_type,
        canonical_stage,
        from_stage,
        to_stage,
        source,
        eff_source_event_id,
        actor,
        amount,
        currency,
        payload,
        occurred_at,
    )

    # If DO NOTHING fired (duplicate), skip side effects
    if event_id is None:
        return None

    # Side effects: update lead state
    if event_type == "stage_changed" and to_stage:
        await conn.execute(UPDATE_LEAD_STAGE_SQL, lead_id, to_stage)

    elif event_type == "lead_won":
        stage = canonical_stage or "lead_won"
        await conn.execute(UPDATE_LEAD_WON_SQL, lead_id, stage, occurred_at)

    elif event_type == "lead_lost":
        stage = canonical_stage or "lead_lost"
        await conn.execute(UPDATE_LEAD_LOST_SQL, lead_id, stage, occurred_at)

    elif event_type == "value_changed" and amount is not None:
        await conn.execute(UPDATE_LEAD_VALUE_SQL, lead_id, amount, currency)

    return event_id
