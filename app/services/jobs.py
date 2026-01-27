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
    inbound_event_id: str

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
RETURNING jq.job_id::text, jq.tenant_id::text, jq.job_type, jq.inbound_event_id::text;
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

