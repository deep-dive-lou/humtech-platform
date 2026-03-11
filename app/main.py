import logging
import os
import sys
import traceback

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import Optional
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stdout,
)
from .config import settings
from .db import init_db_pool, close_db_pool, get_pool
from .bot.jobs import claim_jobs, mark_done, mark_retry
from .bot.processor import process_job
from .bot.sender import send_pending_outbound
from .bot.tenants import load_tenant_debug
from .engine.webhooks import router as engine_webhooks_router
from .financial.routes import router as financial_router
from .outreach.routes import router as outreach_router
from .bot.webhook import router as bot_webhook_router
from .portal.routes import router as portal_router
from .portal.staff_routes import router as portal_staff_router
from .optimiser.routes import router as optimiser_router
from .optimiser.api import router as optimiser_api_router
from .optimiser.auth import router as optimiser_auth_router, OptimiserNotAuthenticated
from .engine.analytics.routes import router as analytics_router
from .engine.analytics.auth import AnalyticsNotAuthenticated
from .portal.auth import NotAuthenticated

app = FastAPI(title="HumTech Platform", version="0.2.0")
load_dotenv()

_error_templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "portal", "templates")
)
logger = logging.getLogger("humtech.errors")
app.include_router(engine_webhooks_router)
app.include_router(financial_router)
app.include_router(outreach_router)
app.include_router(bot_webhook_router)
app.include_router(portal_router)
app.include_router(portal_staff_router)
app.include_router(optimiser_auth_router)
app.include_router(optimiser_router)
app.include_router(optimiser_api_router)
app.include_router(analytics_router)

@app.exception_handler(NotAuthenticated)
async def _portal_auth_handler(request: Request, exc: NotAuthenticated):
    return RedirectResponse(url="/portal/staff/login", status_code=303)

@app.exception_handler(OptimiserNotAuthenticated)
async def _optimiser_auth_handler(request: Request, exc: OptimiserNotAuthenticated):
    return RedirectResponse(url="/optimiser/login", status_code=303)

@app.exception_handler(AnalyticsNotAuthenticated)
async def _analytics_auth_handler(request: Request, exc: AnalyticsNotAuthenticated):
    return RedirectResponse(url="/analytics/login", status_code=303)


async def _send_slack_error(request: Request, status_code: int, detail: str):
    """Fire-and-forget Slack alert for server errors."""
    webhook_url = settings.slack_webhook_url
    if not webhook_url:
        return
    try:
        text = (
            f":rotating_light: *{status_code} Error* on `{request.method} {request.url.path}`\n"
            f"```{detail[:1500]}```"
        )
        async with httpx.AsyncClient(timeout=5) as client:
            await client.post(webhook_url, json={"text": text})
    except Exception:
        logger.warning("Failed to send Slack error alert", exc_info=True)


def _is_portal_request(request: Request) -> bool:
    host = request.headers.get("host", "")
    return host.startswith("portal.") or request.url.path.startswith("/portal/")


def _render_error(request: Request, status_code: int, title: str, message: str):
    return _error_templates.TemplateResponse(
        "error.html",
        {"request": request, "status_code": status_code, "title": title, "message": message},
        status_code=status_code,
    )


@app.exception_handler(500)
async def _server_error_handler(request: Request, exc: Exception):
    tb = traceback.format_exception(type(exc), exc, exc.__traceback__)
    detail = "".join(tb)
    logger.error("Unhandled 500 on %s %s:\n%s", request.method, request.url.path, detail)
    await _send_slack_error(request, 500, detail)
    if _is_portal_request(request):
        return _render_error(
            request, 500,
            "Something went wrong",
            "We hit an unexpected error. The team has been notified and we're looking into it.",
        )
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})


@app.exception_handler(404)
async def _not_found_handler(request: Request, exc: HTTPException):
    if _is_portal_request(request):
        return _render_error(
            request, 404,
            "Page not found",
            "The page you're looking for doesn't exist or has been moved.",
        )
    return JSONResponse(status_code=404, content={"detail": "Not Found"})


@app.on_event("startup")
async def _startup():
    await init_db_pool()

@app.on_event("shutdown")
async def _shutdown():
    await close_db_pool()

@app.get("/", include_in_schema=False)
async def root(request: Request):
    host = request.headers.get("host", "")
    if host.startswith("portal."):
        return RedirectResponse(url="/portal/staff/login", status_code=302)
    return {"service": settings.service_name}

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

LOAD_OUTBOUND_TEXT_BY_ID_SQL = """
SELECT text FROM bot.messages
WHERE message_id = $1::uuid;
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

            # Fetch the bot's reply text from this specific run
            bot_reply = None
            if result.get("out_message_id"):
                bot_reply = await conn.fetchval(
                    LOAD_OUTBOUND_TEXT_BY_ID_SQL,
                    result["out_message_id"],
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
