from __future__ import annotations

import logging
from typing import Any

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

INSTANTLY_API_V2_URL = "https://api.instantly.ai/api/v2/leads"


async def push_to_instantly(leads: list[dict[str, Any]], campaign_id: str | None = None) -> dict[str, Any]:
    """
    Push a list of approved leads to the Instantly campaign via API v2.

    Each lead dict must have: email, first_name, last_name, company, opener.
    campaign_id: override from campaign.json (falls back to env var).
    Returns summary: {sent: int, failed: int, errors: list}
    """
    if not settings.instantly_api_key:
        raise RuntimeError("INSTANTLY_API_KEY not configured")
    effective_campaign_id = campaign_id or settings.instantly_campaign_id
    if not effective_campaign_id:
        raise RuntimeError("No campaign_id — set INSTANTLY_CAMPAIGN_ID or provide in campaign.json")

    headers = {
        "Authorization": f"Bearer {settings.instantly_api_key}",
        "Content-Type": "application/json",
    }

    sent = 0
    failed = 0
    errors = []

    async with httpx.AsyncClient(timeout=30) as client:
        for lead in leads:
            payload = {
                "email": lead["email"],
                "first_name": lead.get("first_name", ""),
                "last_name": lead.get("last_name", ""),
                "company_name": lead.get("company", ""),
                "website": lead.get("company_domain", ""),
                "campaign": effective_campaign_id,
                "custom_variables": {
                    "personalization": lead["opener"],
                },
            }
            try:
                resp = await client.post(INSTANTLY_API_V2_URL, json=payload, headers=headers)
                resp.raise_for_status()
                sent += 1
            except httpx.HTTPStatusError as e:
                logger.error("Instantly API error for %s: %s — %s", lead["email"], e.response.status_code, e.response.text)
                failed += 1
                errors.append({"email": lead["email"], "status": e.response.status_code, "detail": e.response.text[:200]})
            except Exception as e:
                logger.error("Instantly request failed for %s: %s", lead["email"], e)
                failed += 1
                errors.append({"email": lead["email"], "detail": str(e)})

    if sent:
        logger.info("Instantly: queued %d leads", sent)

    return {"sent": sent, "failed": failed, "errors": errors}
