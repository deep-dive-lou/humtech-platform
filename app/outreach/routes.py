from __future__ import annotations

import logging
import os
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from app.db import get_pool
from app.outreach import models
from app.outreach.pipeline import list_campaigns, load_campaign_config, run_pipeline
from app.outreach.sender import push_to_instantly

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/outreach", tags=["outreach"])

templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "templates")
)


# ---------------------------------------------------------------------------
# Review UI
# ---------------------------------------------------------------------------

@router.get("/review", response_class=HTMLResponse)
async def review_page(request: Request, batch_date: Optional[str] = None):
    today = date.fromisoformat(batch_date) if batch_date else date.today()
    pool = await get_pool()
    async with pool.acquire() as conn:
        leads = await models.get_batch(conn, today)
        counts = await models.get_batch_counts(conn, today)

    all_leads = []
    for l in leads:
        if l["review_status"] not in ("needs_review", "auto_send"):
            continue
        row = dict(l)
        for k, v in row.items():
            if isinstance(v, Decimal):
                row[k] = float(v)
        all_leads.append(row)

    return templates.TemplateResponse("review.html", {
        "request": request,
        "batch_date": today.strftime("%d %b %Y").lstrip("0"),
        "batch_date_iso": today.isoformat(),
        "prev_date": (today - timedelta(days=1)).isoformat(),
        "next_date": (today + timedelta(days=1)).isoformat(),
        "today_iso": date.today().isoformat(),
        "counts": counts,
        "leads": all_leads,
    })


# ---------------------------------------------------------------------------
# Lead actions (called via JS fetch — no page reload)
# ---------------------------------------------------------------------------

class EditOpenerRequest(BaseModel):
    opener: str


@router.post("/lead/{personalisation_id}/edit")
async def edit_opener(personalisation_id: str, body: EditOpenerRequest):
    if not body.opener.strip():
        raise HTTPException(status_code=400, detail="Opener cannot be empty")
    pool = await get_pool()
    async with pool.acquire() as conn:
        await models.update_opener(
            conn,
            personalisation_id=personalisation_id,
            opener=body.opener.strip(),
        )
    return {"ok": True}


@router.post("/lead/{personalisation_id}/remove")
async def remove_lead(personalisation_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await models.remove_lead(conn, personalisation_id=personalisation_id)
    return {"ok": True}


# ---------------------------------------------------------------------------
# Preview & test send
# ---------------------------------------------------------------------------

@router.get("/preview-send")
async def preview_send(batch_date: Optional[str] = None):
    """Show the exact payload that would be sent to Instantly — without sending."""
    today = date.fromisoformat(batch_date) if batch_date else date.today()
    pool = await get_pool()
    async with pool.acquire() as conn:
        leads = await models.get_sendable_leads(conn, today)

    if not leads:
        return {"ok": True, "leads": [], "message": "No sendable leads for this date"}

    instantly_leads = [
        {
            "email": lead["email"],
            "first_name": lead.get("first_name", ""),
            "last_name": lead.get("last_name", ""),
            "company_name": lead.get("company", ""),
            "website": lead.get("company_domain", ""),
            "personalization": lead["opener"],
        }
        for lead in leads
    ]

    # Group by campaign for preview
    campaigns_seen = set(lead.get("campaign_name") or "default" for lead in leads)
    campaign_ids = {}
    for cname in campaigns_seen:
        cfg = load_campaign_config(cname) if cname != "default" else load_campaign_config()
        campaign_ids[cname] = cfg.get("instantly_campaign_id")

    return {
        "ok": True,
        "campaign_ids": campaign_ids,
        "count": len(instantly_leads),
        "leads": instantly_leads,
    }


class TestSendRequest(BaseModel):
    test_email: str


@router.post("/test-send")
async def test_send(body: TestSendRequest, batch_date: Optional[str] = None):
    """Send the first lead to a test email so you can see the rendered email."""
    today = date.fromisoformat(batch_date) if batch_date else date.today()
    pool = await get_pool()
    async with pool.acquire() as conn:
        leads = await models.get_sendable_leads(conn, today)

    if not leads:
        return {"ok": True, "sent": 0, "message": "No sendable leads for this date"}

    # Take just the first lead, override email
    test_lead = dict(leads[0])
    test_lead["email"] = body.test_email

    cname = test_lead.get("campaign_name")
    config = load_campaign_config(cname) if cname else load_campaign_config()
    campaign_id = config.get("instantly_campaign_id")
    result = await push_to_instantly([test_lead], campaign_id=campaign_id)

    return {"ok": True, "test_email": body.test_email, "campaign": cname, **result}


# ---------------------------------------------------------------------------
# Send batch
# ---------------------------------------------------------------------------

@router.post("/send")
async def send_batch(batch_date: Optional[str] = None):
    today = date.fromisoformat(batch_date) if batch_date else date.today()
    pool = await get_pool()

    async with pool.acquire() as conn:
        leads = await models.get_sendable_leads(conn, today)

    if not leads:
        return {"ok": True, "sent": 0, "failed": 0, "message": "No sendable leads for this date"}

    # Group leads by campaign and send to the correct Instantly campaign
    by_campaign: dict[str, list[dict]] = {}
    for lead in leads:
        cname = lead.get("campaign_name") or "default"
        by_campaign.setdefault(cname, []).append(lead)

    total_sent = 0
    total_failed = 0
    for cname, campaign_leads in by_campaign.items():
        config = load_campaign_config(cname) if cname != "default" else load_campaign_config()
        campaign_id = config.get("instantly_campaign_id")
        result = await push_to_instantly(campaign_leads, campaign_id=campaign_id)

        async with pool.acquire() as conn:
            for lead in campaign_leads:
                if result["failed"] == 0:
                    await models.mark_lead_sent(conn, lead["lead_id"])
                    await models.log_event(conn, lead_id=lead["lead_id"], event_type="sent")
                else:
                    await models.mark_lead_failed(conn, lead["lead_id"])
                    await models.log_event(conn, lead_id=lead["lead_id"], event_type="failed")

        total_sent += result.get("sent", 0)
        total_failed += result.get("failed", 0)

    return {"ok": True, "sent": total_sent, "failed": total_failed}


# ---------------------------------------------------------------------------
# Pipeline trigger (for cron / n8n / manual run from VS Code)
# ---------------------------------------------------------------------------

@router.post("/pipeline/run")
async def trigger_pipeline(batch_date: Optional[str] = None, campaign: Optional[str] = None):
    """Run pipeline for a specific campaign, or all campaigns if none specified."""
    today = date.fromisoformat(batch_date) if batch_date else date.today()

    if campaign:
        stats = await run_pipeline(batch_date=today, campaign=campaign)
        return {"ok": True, "stats": stats}

    # No campaign specified — run all available campaigns sequentially
    campaigns = list_campaigns()
    if not campaigns:
        stats = await run_pipeline(batch_date=today)
        return {"ok": True, "stats": stats}

    all_stats = []
    for c in campaigns:
        config = load_campaign_config(c)
        if config.get("instantly_campaign_id") == "PENDING":
            logger.info("Skipping campaign %s — instantly_campaign_id is PENDING", c)
            continue
        stats = await run_pipeline(batch_date=today, campaign=c)
        all_stats.append(stats)

    return {"ok": True, "campaigns_run": len(all_stats), "stats": all_stats}


# ---------------------------------------------------------------------------
# Suppression (called by n8n on unsubscribe/negative reply)
# ---------------------------------------------------------------------------

class SuppressRequest(BaseModel):
    email: Optional[str] = None
    domain: Optional[str] = None
    reason: str = "unsubscribe"


@router.post("/suppress")
async def suppress(body: SuppressRequest):
    if not body.email and not body.domain:
        raise HTTPException(status_code=400, detail="email or domain required")
    pool = await get_pool()
    async with pool.acquire() as conn:
        await models.insert_suppression(
            conn,
            email=body.email,
            domain=body.domain,
            reason=body.reason,
        )
    return {"ok": True}
