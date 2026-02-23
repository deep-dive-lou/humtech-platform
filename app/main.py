from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
from dotenv import load_dotenv
from .config import settings
from .db import init_db_pool, close_db_pool, get_pool
from .bot.jobs import claim_jobs, mark_done, mark_retry
from .bot.processor import process_job
from .bot.sender import send_pending_outbound
from .bot.tenants import load_tenant_debug
from .engine.webhooks import router as engine_webhooks_router
from .outreach.routes import router as outreach_router
from .bot.webhook import router as bot_webhook_router

app = FastAPI(title="HumTech Platform", version="0.2.0")
load_dotenv()
app.include_router(engine_webhooks_router)
app.include_router(outreach_router)
app.include_router(bot_webhook_router)

@app.on_event("startup")
async def _startup():
    await init_db_pool()

@app.on_event("shutdown")
async def _shutdown():
    await close_db_pool()

@app.get("/health")
async def health():
    return {"ok": True, "service": settings.service_name, "env": settings.env}

@app.get("/debug/tenant/{tenant_id}")
async def debug_tenant(tenant_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            tenant = await load_tenant_debug(conn, tenant_id)
        except RuntimeError as e:
            raise HTTPException(status_code=404, detail=str(e))

    settings_obj = tenant.get("settings") or {}
    calendar = settings_obj.get("calendar") or {}
    settings_keys = list(settings_obj.keys())
    has_calendar_id = bool(calendar.get("calendar_id"))
    has_timezone = bool(settings_obj.get("timezone") or calendar.get("timezone"))

    return {
        "tenant_id": tenant["tenant_id"],
        "tenant_slug": tenant["tenant_slug"],
        "is_enabled": tenant["is_enabled"],
        "messaging_adapter": tenant["messaging_adapter"],
        "calendar_adapter": tenant["calendar_adapter"],
        "settings_keys": settings_keys,
        "has_calendar_id": has_calendar_id,
        "has_timezone": has_timezone,
    }

@app.post("/worker/run")
async def worker_run(limit: int = 50):
    if limit < 1 or limit > 500:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 500")

    pool = await get_pool()
    processed = 0
    claimed = 0
    failures = 0

    async with pool.acquire() as conn:
        async with conn.transaction():
            jobs = await claim_jobs(conn, limit=limit, locked_by=settings.worker_id)
            claimed = len(jobs)

        # Process outside the claim transaction (each job can have its own tx later)
        for job in jobs:
            try:
                async with conn.transaction():
                    result = await process_job(conn, job.job_id)
                    await mark_done(conn, job.job_id)
                processed += 1
            except Exception as e:
                failures += 1
                err = {"error": str(e), "job_id": job.job_id}
                async with conn.transaction():
                    await mark_retry(conn, job.job_id, delay_seconds=30, error_obj=err)

    return {"claimed": claimed, "processed": processed, "failures": failures, "worker_id": settings.worker_id}

class SimulateRequest(BaseModel):
    tenant_id: str
    event_type: str = "inbound_message"   # "new_lead" or "inbound_message"
    text: str = ""
    display_name: Optional[str] = None
    channel_address: str = "+447700000000"  # fake test number
    channel: str = "sms"


INSERT_SIM_EVENT_SQL = """
INSERT INTO bot.inbound_events (
  tenant_id, provider, event_type, provider_msg_id, channel, channel_address,
  dedupe_key, payload, trace_id
) VALUES (
  $1::uuid, 'ghl', $2, gen_random_uuid()::text, $3, $4,
  gen_random_uuid()::text,
  $5::jsonb,
  gen_random_uuid()
) RETURNING inbound_event_id::text, trace_id::text;
"""

INSERT_SIM_JOB_SQL = """
INSERT INTO bot.job_queue (
  tenant_id, job_type, inbound_event_id, status, run_after
) VALUES (
  $1::uuid, 'process_inbound_event', $2::uuid, 'queued', now()
) RETURNING job_id::text;
"""

LOAD_OUTBOUND_TEXT_SQL = """
SELECT text FROM bot.messages
WHERE conversation_id = $1::uuid
  AND direction = 'outbound'
ORDER BY created_at DESC
LIMIT 1;
"""

LOAD_CONVERSATION_ID_SQL = """
SELECT conversation_id::text FROM bot.conversations
WHERE tenant_id = $1::uuid
  AND contact_id = (
    SELECT contact_id FROM bot.contacts
    WHERE tenant_id = $1::uuid AND channel = $2 AND channel_address = $3
    LIMIT 1
  )
  AND status = 'open'
LIMIT 1;
"""


@app.post("/debug/bot/simulate")
async def debug_bot_simulate(body: SimulateRequest):
    """
    Simulate a bot conversation turn.

    Inserts a real inbound event + job, runs process_job, returns the bot reply.
    Does NOT send any SMS (sending only happens via /worker/send).

    Use the same channel_address across calls to simulate a multi-turn conversation.
    The contact and conversation persist in the DB under that number.
    """
    if body.event_type not in ("new_lead", "inbound_message"):
        raise HTTPException(status_code=400, detail="event_type must be 'new_lead' or 'inbound_message'")

    payload = {
        "text": body.text,
        "display_name": body.display_name,
        "sim": True,
    }

    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Insert inbound event
            ev_row = await conn.fetchrow(
                INSERT_SIM_EVENT_SQL,
                body.tenant_id,
                body.event_type,
                body.channel,
                body.channel_address,
                payload,
            )
            inbound_event_id = ev_row["inbound_event_id"]

            # Insert job
            job_row = await conn.fetchrow(
                INSERT_SIM_JOB_SQL,
                body.tenant_id,
                inbound_event_id,
            )
            job_id = job_row["job_id"]

            # Run processor
            try:
                result = await process_job(conn, job_id)
                await mark_done(conn, job_id)
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"process_job failed: {e}")

            # Fetch the bot's reply text
            bot_reply = await conn.fetchval(
                LOAD_OUTBOUND_TEXT_SQL,
                result["conversation_id"],
            )

    return {
        "bot_reply": bot_reply,
        "route": result.get("route"),
        "slot_matched": result.get("slot_matched"),
        "booking_id": result.get("booking_id"),
        "conversation_id": result.get("conversation_id"),
        "contact_id": result.get("contact_id"),
        "trace_id": result.get("trace_id"),
        "tip": "Use the same channel_address to continue the conversation. SMS won't send unless you call /worker/send.",
    }


@app.post("/worker/send")
async def worker_send(limit: int = 50):
    if limit < 1 or limit > 500:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 500")

    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await send_pending_outbound(conn, limit=limit)

    return {"ok": True, **result, "worker_id": settings.worker_id}
