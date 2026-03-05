from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional
from datetime import date

import asyncpg


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

IS_SUPPRESSED_SQL = """
SELECT 1 FROM outreach.suppressions
WHERE email = $1::text
   OR (domain IS NOT NULL AND domain = $2::text)
LIMIT 1;
"""

INSERT_LEAD_SQL = """
INSERT INTO outreach.leads (
    email, first_name, last_name, title,
    company, company_domain, linkedin_url,
    industry, employee_count, city, apollo_id, batch_date,
    campaign_name
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
ON CONFLICT (email) DO NOTHING
RETURNING lead_id::text;
"""

INSERT_ENRICHMENT_SQL = """
INSERT INTO outreach.enrichment (lead_id, signals)
VALUES ($1::uuid, $2::jsonb);
"""

INSERT_PERSONALISATION_SQL = """
INSERT INTO outreach.personalisation (
    lead_id, opener_first_line, micro_insight, angle_tag,
    confidence_score, evidence_used, risk_flags, rung,
    review_status, prompt_version, model
)
VALUES (
    $1::uuid, $2, $3, $4,
    $5::numeric, $6::jsonb, $7::jsonb, $8,
    $9, $10, $11
)
RETURNING personalisation_id::text;
"""

INSERT_EVENT_SQL = """
INSERT INTO outreach.events (lead_id, event_type, meta)
VALUES ($1::uuid, $2::text, $3::jsonb);
"""

GET_BATCH_SQL = """
SELECT
    l.lead_id::text,
    l.first_name,
    l.last_name,
    l.email,
    l.title,
    l.company,
    l.company_domain,
    l.campaign_name,
    p.personalisation_id::text,
    p.opener_first_line,
    p.edited_opener,
    p.micro_insight,
    p.angle_tag,
    p.confidence_score,
    p.rung,
    p.review_status,
    p.evidence_used,
    p.risk_flags,
    p.removed
FROM outreach.leads l
JOIN outreach.personalisation p ON p.lead_id = l.lead_id
WHERE l.batch_date = $1::date
  AND p.removed = FALSE
ORDER BY
    CASE p.review_status
        WHEN 'needs_review' THEN 0
        WHEN 'auto_send'    THEN 1
        WHEN 'blocked'      THEN 2
    END,
    p.confidence_score DESC;
"""

GET_BATCH_COUNTS_SQL = """
SELECT
    COUNT(*) FILTER (WHERE p.review_status = 'auto_send'    AND p.removed = FALSE) AS auto_send,
    COUNT(*) FILTER (WHERE p.review_status = 'needs_review' AND p.removed = FALSE) AS needs_review,
    COUNT(*) FILTER (WHERE p.review_status = 'blocked'      AND p.removed = FALSE) AS blocked
FROM outreach.leads l
JOIN outreach.personalisation p ON p.lead_id = l.lead_id
WHERE l.batch_date = $1::date;
"""

UPDATE_OPENER_SQL = """
UPDATE outreach.personalisation
SET edited_opener = $2::text
WHERE personalisation_id = $1::uuid;
"""

REMOVE_LEAD_SQL = """
UPDATE outreach.personalisation
SET removed = TRUE
WHERE personalisation_id = $1::uuid;
"""

GET_SENDABLE_LEADS_SQL = """
SELECT
    l.lead_id::text,
    l.first_name,
    l.last_name,
    l.email,
    l.company,
    l.company_domain,
    l.campaign_name,
    COALESCE(p.edited_opener, p.opener_first_line) AS opener
FROM outreach.leads l
JOIN outreach.personalisation p ON p.lead_id = l.lead_id
WHERE l.batch_date = $1::date
  AND p.review_status IN ('auto_send', 'needs_review')
  AND p.removed = FALSE
  AND l.status NOT IN ('sent', 'failed', 'suppressed');
"""

MARK_LEAD_SENT_SQL = """
UPDATE outreach.leads SET status = 'sent', updated_at = now()
WHERE lead_id = $1::uuid;
"""

MARK_LEAD_FAILED_SQL = """
UPDATE outreach.leads SET status = 'failed', updated_at = now()
WHERE lead_id = $1::uuid;
"""

INSERT_SUPPRESSION_SQL = """
INSERT INTO outreach.suppressions (email, domain, reason)
VALUES ($1, $2, $3)
ON CONFLICT (email) DO NOTHING;
"""

# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

async def is_suppressed(conn: asyncpg.Connection, email: str, domain: Optional[str]) -> bool:
    row = await conn.fetchrow(IS_SUPPRESSED_SQL, email, domain)
    return row is not None


async def insert_lead(
    conn: asyncpg.Connection,
    *,
    email: str,
    first_name: str,
    last_name: Optional[str],
    title: Optional[str],
    company: Optional[str],
    company_domain: Optional[str],
    linkedin_url: Optional[str],
    industry: Optional[str],
    employee_count: Optional[int],
    city: Optional[str],
    apollo_id: Optional[str],
    batch_date: date,
    campaign_name: Optional[str] = None,
) -> Optional[str]:
    """Insert lead, returns lead_id or None if email already exists."""
    return await conn.fetchval(
        INSERT_LEAD_SQL,
        email, first_name, last_name, title,
        company, company_domain, linkedin_url,
        industry, employee_count, city, apollo_id, batch_date,
        campaign_name,
    )


async def insert_enrichment(
    conn: asyncpg.Connection, *, lead_id: str, signals: dict[str, Any]
) -> None:
    await conn.execute(INSERT_ENRICHMENT_SQL, lead_id, signals)


async def insert_personalisation(
    conn: asyncpg.Connection,
    *,
    lead_id: str,
    opener_first_line: str,
    micro_insight: Optional[str],
    angle_tag: Optional[str],
    confidence_score: float,
    evidence_used: list,
    risk_flags: list,
    rung: int,
    review_status: str,
    prompt_version: str = "v1.0",
    model: Optional[str] = None,
) -> str:
    return await conn.fetchval(
        INSERT_PERSONALISATION_SQL,
        lead_id, opener_first_line, micro_insight, angle_tag,
        confidence_score, evidence_used, risk_flags, rung,
        review_status, prompt_version, model,
    )


async def log_event(
    conn: asyncpg.Connection,
    *,
    lead_id: str,
    event_type: str,
    meta: Optional[dict[str, Any]] = None,
) -> None:
    await conn.execute(INSERT_EVENT_SQL, lead_id, event_type, meta or {})


async def get_batch(conn: asyncpg.Connection, batch_date: date) -> list[dict]:
    rows = await conn.fetch(GET_BATCH_SQL, batch_date)
    return [dict(r) for r in rows]


async def get_batch_counts(conn: asyncpg.Connection, batch_date: date) -> dict:
    row = await conn.fetchrow(GET_BATCH_COUNTS_SQL, batch_date)
    return dict(row) if row else {"auto_send": 0, "needs_review": 0, "blocked": 0}


async def update_opener(
    conn: asyncpg.Connection, *, personalisation_id: str, opener: str
) -> None:
    await conn.execute(UPDATE_OPENER_SQL, personalisation_id, opener)


async def remove_lead(conn: asyncpg.Connection, *, personalisation_id: str) -> None:
    await conn.execute(REMOVE_LEAD_SQL, personalisation_id)


async def get_sendable_leads(conn: asyncpg.Connection, batch_date: date) -> list[dict]:
    rows = await conn.fetch(GET_SENDABLE_LEADS_SQL, batch_date)
    return [dict(r) for r in rows]


async def mark_lead_sent(conn: asyncpg.Connection, lead_id: str) -> None:
    await conn.execute(MARK_LEAD_SENT_SQL, lead_id)


async def mark_lead_failed(conn: asyncpg.Connection, lead_id: str) -> None:
    await conn.execute(MARK_LEAD_FAILED_SQL, lead_id)


async def insert_suppression(
    conn: asyncpg.Connection,
    *,
    email: Optional[str],
    domain: Optional[str],
    reason: str,
) -> None:
    await conn.execute(INSERT_SUPPRESSION_SQL, email, domain, reason)
