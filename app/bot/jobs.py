from __future__ import annotations
from dataclasses import dataclass
from typing import Any
import asyncpg
import json

@dataclass
class ClaimedJob:
    job_id: str
    tenant_id: str
    job_type: str
    inbound_event_id: str | None
    trace_id: str
    conversation_id: str | None = None

CLAIM_JOBS_SQL = """
WITH cte AS (
  SELECT job_id
  FROM bot.job_queue
  WHERE status = 'queued'
    AND run_after <= now()
  ORDER BY run_after ASC
  LIMIT $1
  FOR UPDATE SKIP LOCKED
)
UPDATE bot.job_queue jq
SET status = 'running',
    locked_at = now(),
    locked_by = $2
FROM cte
WHERE jq.job_id = cte.job_id
RETURNING jq.job_id::text, jq.tenant_id::text, jq.job_type, jq.inbound_event_id::text,
          COALESCE(jq.trace_id,
            (SELECT ie.trace_id FROM bot.inbound_events ie
             WHERE ie.inbound_event_id = jq.inbound_event_id
               AND jq.inbound_event_id IS NOT NULL),
            gen_random_uuid()
          )::text AS trace_id,
          jq.conversation_id::text;
"""

MARK_DONE_SQL = """
UPDATE bot.job_queue
SET status = 'done',
    locked_at = NULL,
    locked_by = NULL,
    last_error = NULL
WHERE job_id = $1::uuid;
"""

MARK_RETRY_SQL = """
UPDATE bot.job_queue
SET status = 'queued',
    attempts = attempts + 1,
    run_after = now() + ($2::int || ' seconds')::interval,
    locked_at = NULL,
    locked_by = NULL,
    last_error = $3::text
WHERE job_id = $1::uuid;
"""

async def claim_jobs(conn: asyncpg.Connection, limit: int, locked_by: str) -> list[ClaimedJob]:
    rows = await conn.fetch(CLAIM_JOBS_SQL, limit, locked_by)
    return [ClaimedJob(**dict(r)) for r in rows]

async def mark_done(conn: asyncpg.Connection, job_id: str) -> None:
    await conn.execute(MARK_DONE_SQL, job_id)

async def mark_retry(conn: asyncpg.Connection, job_id: str, delay_seconds: int, error_obj: dict[str, Any]) -> None:
    await conn.execute(MARK_RETRY_SQL, job_id, delay_seconds, json.dumps(error_obj, ensure_ascii=False))


# ---------------------------------------------------------------------------
# Debounce: find & claim sibling jobs for the same contact
# ---------------------------------------------------------------------------

FIND_SIBLING_JOBS_SQL = """
SELECT jq.job_id::text, ie.inbound_event_id::text, ie.payload->>'text' AS text
FROM bot.job_queue jq
JOIN bot.inbound_events ie ON ie.inbound_event_id = jq.inbound_event_id
WHERE jq.tenant_id = $1::uuid
  AND jq.status = 'queued'
  AND jq.job_id != $2::uuid
  AND ie.channel_address = $3
  AND ie.event_type = 'inbound_message'
  AND jq.run_after <= now() + interval '2 seconds'
ORDER BY jq.created_at ASC
FOR UPDATE OF jq SKIP LOCKED;
"""

MARK_JOBS_DONE_BATCH_SQL = """
UPDATE bot.job_queue
SET status = 'done',
    locked_at = NULL,
    locked_by = NULL,
    last_error = $2::text
WHERE job_id = ANY($1::uuid[]);
"""


async def find_and_claim_siblings(
    conn: asyncpg.Connection,
    tenant_id: str,
    current_job_id: str,
    channel_address: str,
) -> list[dict]:
    """Find other queued inbound_message jobs from the same contact.

    Returns list of {job_id, inbound_event_id, text} for sibling messages.
    The sibling jobs are locked (FOR UPDATE) so no other worker grabs them.
    """
    rows = await conn.fetch(FIND_SIBLING_JOBS_SQL, tenant_id, current_job_id, channel_address)
    return [dict(r) for r in rows]


async def mark_siblings_done(
    conn: asyncpg.Connection,
    sibling_job_ids: list[str],
    aggregated_into: str,
) -> None:
    """Mark sibling jobs as done (they were aggregated into the primary job)."""
    if not sibling_job_ids:
        return
    note = json.dumps({"aggregated_into": aggregated_into}, ensure_ascii=False)
    await conn.execute(MARK_JOBS_DONE_BATCH_SQL, sibling_job_ids, note)

