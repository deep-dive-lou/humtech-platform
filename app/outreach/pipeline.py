from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import date
from pathlib import Path
from typing import Any, Optional

import httpx
from anthropic import AsyncAnthropic

from app.config import settings
from app.db import get_pool
from app.outreach.models import (
    insert_enrichment,
    insert_lead,
    insert_personalisation,
    insert_suppression,
    is_suppressed,
    log_event,
)

logger = logging.getLogger(__name__)

PROMPT_VERSION = "v1.0"
PERSONALISATION_MODEL = "claude-sonnet-4-6"
ENRICHMENT_MODEL = "claude-haiku-4-5-20251001"

# Fallback defaults (used when campaign.json is missing)
TEMPLATE_CONTEXT = (
    "HumTech offers a done-for-you AI Revenue Engine — AI booking bot, "
    "speed-to-lead automation, sales process improvement, and full ad management. "
    "They only get paid when revenue goes up. The email introduces this and asks for a call."
)

APOLLO_TITLES = [
    "CEO", "MD", "Managing Director", "Founder", "Co-Founder",
    "COO", "Commercial Director", "Head of Sales", "Sales Director",
    "VP Sales", "Director of Sales",
]
APOLLO_SENIORITIES = ["owner", "founder", "c_suite", "vp", "director"]

CAMPAIGN_CONFIG_PATH = Path(__file__).parent / "campaign.json"

_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*\n?(.*?)\n?\s*```", re.DOTALL)


def _extract_json(text: str) -> str:
    """Strip markdown code fences from LLM output before JSON parsing."""
    m = _JSON_FENCE_RE.search(text)
    return m.group(1).strip() if m else text.strip()


def load_campaign_config() -> dict[str, Any]:
    """Load active campaign config. Falls back to empty dict if missing/invalid."""
    try:
        if CAMPAIGN_CONFIG_PATH.exists():
            with open(CAMPAIGN_CONFIG_PATH) as f:
                config = json.load(f)
            logger.info("Loaded campaign config: %s", config.get("campaign_name", "unnamed"))
            return config
    except Exception as e:
        logger.error("Failed to load campaign.json: %s — using defaults", e)
    return {}

# ---------------------------------------------------------------------------
# Lead sourcing — Apollo
# ---------------------------------------------------------------------------

async def source_leads(config: dict[str, Any] | None = None, limit: int = 150) -> list[dict[str, Any]]:
    """Pull ICP-matched prospects from Apollo People Search."""
    if not settings.apollo_api_key:
        logger.warning("APOLLO_API_KEY not set — returning empty list")
        return []

    ac = (config or {}).get("apollo", {})

    payload = {
        "person_titles": ac.get("person_titles", APOLLO_TITLES),
        "person_seniorities": ac.get("person_seniorities", APOLLO_SENIORITIES),
        "contact_email_status_v2": ac.get("contact_email_status_v2", ["verified", "likely to engage"]),
        "organization_locations": ac.get("organization_locations", ["United Kingdom"]),
        "organization_num_employees_ranges": ac.get("organization_num_employees_ranges", ["50,500"]),
        "per_page": min(limit, 100),
        "page": 1,
    }

    # Optional filters — only add if present in config
    for key in ("organization_industries", "organization_revenue_ranges"):
        if key in ac:
            payload[key] = ac[key]

    logger.info("Apollo payload filters: %s", {k: v for k, v in payload.items() if k != "per_page" and k != "page"})

    async with httpx.AsyncClient(timeout=20) as client:
        try:
            resp = await client.post(
                "https://api.apollo.io/api/v1/mixed_people/api_search",
                json=payload,
                headers={"Content-Type": "application/json", "X-Api-Key": settings.apollo_api_key},
            )
            resp.raise_for_status()
            data = resp.json()
            people = data.get("people", [])
            logger.info("Apollo returned %d prospects", len(people))
            return people
        except Exception as e:
            logger.error("Apollo sourcing failed: %s", e)
            return []


async def _reveal_contacts(people: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Reveal masked contacts via Apollo bulk_match (batches of 10, 1 credit each)."""
    if not people or not settings.apollo_api_key:
        return []

    revealed = []
    for i in range(0, len(people), 10):
        batch = people[i : i + 10]
        details = [{"id": p["id"]} for p in batch if p.get("id")]
        if not details:
            continue

        async with httpx.AsyncClient(timeout=20) as client:
            try:
                resp = await client.post(
                    "https://api.apollo.io/api/v1/people/bulk_match",
                    json={"details": details},
                    headers={
                        "Content-Type": "application/json",
                        "X-Api-Key": settings.apollo_api_key,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                matches = data.get("matches", [])
                revealed.extend(matches)
                logger.info(
                    "Apollo reveal: %d/%d contacts revealed (batch %d)",
                    len(matches),
                    len(details),
                    i // 10 + 1,
                )
            except Exception as e:
                logger.error("Apollo reveal failed (batch %d): %s", i // 10 + 1, e)

    logger.info("Apollo reveal total: %d contacts with full data", len(revealed))
    return revealed


def _parse_apollo_person(person: dict[str, Any]) -> dict[str, Any]:
    """Normalise an Apollo person record into our lead schema."""
    org = person.get("organization") or {}
    domain = org.get("primary_domain") or person.get("organization_domain", "")
    return {
        "email": person.get("email", ""),
        "first_name": person.get("first_name", ""),
        "last_name": person.get("last_name"),
        "title": person.get("title"),
        "company": org.get("name") or person.get("organization_name"),
        "company_domain": domain,
        "linkedin_url": person.get("linkedin_url"),
        "industry": org.get("industry"),
        "employee_count": org.get("estimated_num_employees"),
        "city": person.get("city"),
        "apollo_id": person.get("id"),
    }


# ---------------------------------------------------------------------------
# Enrichment — Hiring signals (Apify LinkedIn Jobs, batch per pipeline run)
# ---------------------------------------------------------------------------

# Apify actor: worldunboxer/rapid-linkedin-scraper (free, high success rate)
_APIFY_ACTOR_ID = "JkfTWxtpgfvcRQn3p"
_HIRING_SEARCH_TERMS = ["Head of Sales", "Sales Director", "Commercial Director"]
_APIFY_POLL_INTERVAL = 15   # seconds between status checks
_APIFY_TIMEOUT = 480        # 8 minutes max wait per search term


def _normalise_company(name: str) -> str:
    """Lowercase and strip legal suffixes for fuzzy company matching."""
    if not name:
        return ""
    name = name.lower()
    name = re.sub(r"\b(ltd|limited|plc|inc|llc|group|holdings?|uk)\b", "", name)
    name = re.sub(r"[^a-z0-9]", " ", name)
    return " ".join(name.split())


async def _run_apify_job_search(term: str) -> list[dict[str, Any]]:
    """Run one Apify LinkedIn Jobs search and return results."""
    if not settings.apify_api_key:
        return []

    async with httpx.AsyncClient(timeout=20) as client:
        try:
            resp = await client.post(
                f"https://api.apify.com/v2/acts/{_APIFY_ACTOR_ID}/runs",
                params={"token": settings.apify_api_key, "memory": 256},
                json={"searchKeyword": term, "location": "United Kingdom", "limit": 100},
            )
            resp.raise_for_status()
            run_id = resp.json()["data"]["id"]
            dataset_id = resp.json()["data"]["defaultDatasetId"]
        except Exception as e:
            logger.warning("Apify: failed to start run for '%s': %s", term, e)
            return []

    # Poll for completion
    deadline = asyncio.get_event_loop().time() + _APIFY_TIMEOUT
    async with httpx.AsyncClient(timeout=10) as client:
        while asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(_APIFY_POLL_INTERVAL)
            try:
                status_resp = await client.get(
                    f"https://api.apify.com/v2/acts/{_APIFY_ACTOR_ID}/runs/{run_id}",
                    params={"token": settings.apify_api_key},
                )
                status = status_resp.json()["data"]["status"]
                if status == "SUCCEEDED":
                    break
                if status in ("FAILED", "ABORTED", "TIMED-OUT"):
                    logger.warning("Apify: run %s ended with status %s", run_id, status)
                    return []
            except Exception as e:
                logger.warning("Apify: poll error for run %s: %s", run_id, e)
                return []
        else:
            logger.warning("Apify: run %s timed out after %ds", run_id, _APIFY_TIMEOUT)
            return []

    # Fetch results
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            items_resp = await client.get(
                f"https://api.apify.com/v2/datasets/{dataset_id}/items",
                params={"token": settings.apify_api_key, "limit": 100},
            )
            return items_resp.json() if items_resp.status_code == 200 else []
        except Exception as e:
            logger.warning("Apify: failed to fetch dataset %s: %s", dataset_id, e)
            return []


async def fetch_hiring_companies() -> dict[str, dict[str, str]]:
    """
    Batch-fetch UK companies currently hiring for sales roles via Apify.
    Runs once per pipeline invocation, in parallel with Apollo sourcing.
    Returns: {normalised_company_name: {"role": str, "job_url": str}}
    """
    if not settings.apify_api_key:
        logger.info("Apify key not set — skipping hiring signal")
        return {}

    results = await asyncio.gather(
        *[_run_apify_job_search(term) for term in _HIRING_SEARCH_TERMS],
        return_exceptions=True,
    )

    hiring: dict[str, dict[str, str]] = {}
    for term, jobs in zip(_HIRING_SEARCH_TERMS, results):
        if isinstance(jobs, Exception):
            logger.warning("Apify: search '%s' failed: %s", term, jobs)
            continue
        for job in jobs:
            company = _normalise_company(job.get("company_name", ""))
            if company and company not in hiring:
                hiring[company] = {
                    "role": job.get("job_title", term),
                    "job_url": job.get("job_url", ""),
                }

    logger.info("Apify: found %d companies hiring for sales roles", len(hiring))
    return hiring


def _check_hiring_signal(
    company: str, hiring_companies: dict[str, dict[str, str]]
) -> dict[str, Any]:
    """Check if this lead's company appears in the batch hiring lookup."""
    key = _normalise_company(company)
    if not key or key not in hiring_companies:
        return {}
    match = hiring_companies[key]
    return {
        "hiring": {
            "role": match["role"],
            "job_url": match["job_url"],
            "signal": f"Actively hiring for {match['role']} in the UK",
        }
    }


# ---------------------------------------------------------------------------
# Enrichment — Website analysis (Claude)
# ---------------------------------------------------------------------------

async def _analyse_website(domain: str) -> dict[str, Any]:
    """Fetch company homepage and extract signals using Claude Haiku."""
    if not domain or not settings.anthropic_api_key:
        return {}

    url = f"https://{domain}"
    html = ""
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            html = resp.text[:6000]
    except Exception as e:
        logger.warning("Website fetch failed for %s: %s", domain, e)
        return {}

    client = AsyncAnthropic(api_key=settings.anthropic_api_key)
    try:
        msg = await client.messages.create(
            model=ENRICHMENT_MODEL,
            max_tokens=400,
            messages=[{
                "role": "user",
                "content": (
                    f"Analyse this website HTML and return JSON with these fields:\n"
                    f"- has_booking_flow: boolean (is there a book demo/call/meeting CTA?)\n"
                    f"- crm_detected: string or null (HubSpot, Salesforce etc based on scripts)\n"
                    f"- tech_stack: list of strings (detected tools)\n"
                    f"- growth_language: boolean (scaling, growth, expansion language?)\n\n"
                    f"HTML: {html}\n\nReturn ONLY valid JSON, no explanation."
                ),
            }],
        )
        return json.loads(_extract_json(msg.content[0].text))
    except Exception as e:
        logger.warning("Website analysis failed for %s: %s", domain, e)
        return {}


# ---------------------------------------------------------------------------
# Personalisation — Claude
# ---------------------------------------------------------------------------

def _determine_review_status(result: dict[str, Any]) -> str:
    flags = result.get("risk_flags", [])
    confidence = result.get("confidence_score", 0.0)

    if "hallucination_risk" in flags or "privacy_risk" in flags:
        return "blocked"
    if confidence < 0.4 or not result.get("opener_first_line"):
        return "blocked"
    if confidence < 0.7 or "tone_risk" in flags or "duplication_risk" in flags:
        return "needs_review"
    if not result.get("evidence_used"):
        return "needs_review"
    return "auto_send"


async def _generate_personalisation(
    lead: dict[str, Any],
    signals: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run Claude personalisation engine. Returns structured output dict."""
    client = AsyncAnthropic(api_key=settings.anthropic_api_key)

    p_config = (config or {}).get("personalisation", {})
    template_ctx = p_config.get("template_context", TEMPLATE_CONTEXT)

    prompt = f"""You are writing the personalised opening block for a cold email on behalf of HumTech.

The email template is:
  "I saw that [COMPANY] just [SPECIFIC CONTEMPORARY DETAIL].
   This is [COMPLIMENT], and it [SPECIFIC IMPLICATION FOR THEIR REVENUE/SALES]."

Your job: fill in the two bracketed parts to produce two natural sentences that slot into this template.

HumTech context: {template_ctx}

Prospect:
- Name: {lead['first_name']} {lead.get('last_name', '')}
- Title: {lead.get('title', 'unknown')}
- Company: {lead.get('company', 'unknown')}
- Industry: {lead.get('industry', 'unknown')}
- Domain: {lead.get('company_domain', '')}

Available signals (use ONLY what is here — never invent):
{json.dumps(signals, indent=2)}

Rung system (choose highest achievable):
- Rung 5: Specific + evidence-backed (cite real signal with source_url)
- Rung 4: Specific but lighter (category observation with some basis)
- Rung 3: Industry-specific pattern (no personal claim about this company)
- Rung 2: Role-based empathy (title-based, non-assumptive)
- Rung 1: Human neutral (no signals at all)

Signal → template guidance:
- hiring signal → "I saw that [Company] just started recruiting a [role]." / "This is a clear sign of growth ambition, and it usually means [relevant revenue/conversion implication]."
- website growth_language → "I saw that [Company] just [past-tense action from website, e.g. 'launched a new growth push' or 'expanded into new markets']." / "This is ambitious, and it [implication about where systems or AI could help]."
- website has_booking_flow → reference to conversion/booking infrastructure investment
- No strong signals → use industry-level rung 3 observation, do not fabricate a specific detail

UK tone: calm, direct, not salesy. Do NOT mention HumTech — the template body handles that.

Return ONLY valid JSON:
{{
  "opener_first_line": "string — two sentences. Default format: 'I saw that [Company] just [specific contemporary detail]. This is [brief compliment], and it [specific implication for their revenue or sales].' — deviate only if you have a compelling reason and the result is more natural; always stay calm, direct, evidence-backed.",
  "micro_insight": "string or null — internal note on the angle chosen",
  "angle_tag": "speed_to_lead|cac_leak|attribution_gap|sales_ops|conversion_rate",
  "confidence_score": 0.0,
  "evidence_used": [{{"signal_key": "string", "source_url": "string"}}],
  "risk_flags": [],
  "rung": 1
}}

Truth rules — non-negotiable:
1. Only reference signals present in the signals JSON above.
2. Every specific claim needs a source_url in evidence_used.
3. If you reference something without evidence, add "hallucination_risk" to risk_flags.
4. Frame inferences as observations ("usually means", "suggests") not facts.
5. Never invent a contemporary detail — if no strong signal exists, use rung 3 or lower."""

    fallback = {
        "opener_first_line": (
            f"I saw that {lead.get('company', 'your company')} just started expanding its commercial operation. "
            f"This is a strong signal of growth ambition, and it usually surfaces questions about converting that momentum into revenue efficiently."
        ),
        "micro_insight": None,
        "angle_tag": "sales_ops",
        "confidence_score": 0.3,
        "evidence_used": [],
        "risk_flags": [],
        "rung": 1,
    }

    try:
        msg = await client.messages.create(
            model=PERSONALISATION_MODEL,
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}],
        )
        result = json.loads(_extract_json(msg.content[0].text))
        result.setdefault("evidence_used", [])
        result.setdefault("risk_flags", [])
        result.setdefault("rung", 1)
        return result
    except Exception as e:
        logger.warning("Personalisation failed for %s: %s", lead.get("email"), e)
        return fallback


# ---------------------------------------------------------------------------
# Main pipeline entry point
# ---------------------------------------------------------------------------

async def run_pipeline(batch_date: Optional[date] = None) -> dict[str, Any]:
    """
    Full pipeline: source → enrich → personalise → store.
    Returns summary stats.
    """
    config = load_campaign_config()
    today = batch_date or date.today()
    lead_limit = config.get("limits", {}).get("leads_per_run", 150)

    stats = {
        "batch_date": today.isoformat(),
        "campaign": config.get("campaign_name", "default"),
        "sourced": 0,
        "skipped_suppressed": 0,
        "skipped_duplicate": 0,
        "enriched": 0,
        "auto_send": 0,
        "needs_review": 0,
        "blocked": 0,
        "errors": 0,
    }

    # Run Apollo sourcing and Apify hiring fetch concurrently
    search_results, hiring_companies = await asyncio.gather(
        source_leads(config=config, limit=lead_limit),
        fetch_hiring_companies(),
    )
    stats["sourced"] = len(search_results)

    if not search_results:
        logger.warning("Pipeline: no prospects sourced")
        return stats

    # Reveal full contact details (email, last name, linkedin, domain)
    prospects = await _reveal_contacts(search_results)

    if not prospects:
        logger.warning("Pipeline: no prospects sourced")
        return stats

    pool = await get_pool()

    for person in prospects:
        lead = _parse_apollo_person(person)

        if not lead["email"] or not lead["first_name"]:
            stats["errors"] += 1
            continue

        domain = lead.get("company_domain")

        async with pool.acquire() as conn:
            # Suppression check
            if await is_suppressed(conn, lead["email"], domain):
                stats["skipped_suppressed"] += 1
                continue

            # Insert lead (skip if email already exists)
            lead_id = await insert_lead(
                conn,
                batch_date=today,
                **{k: lead[k] for k in lead},
            )
            if not lead_id:
                stats["skipped_duplicate"] += 1
                continue

            await log_event(conn, lead_id=lead_id, event_type="imported")

        # --- Enrichment (outside transaction — slow network calls) ---
        signals: dict[str, Any] = {}

        hiring_signal = _check_hiring_signal(lead.get("company", ""), hiring_companies)
        signals.update(hiring_signal)

        if domain:
            website_signals = await _analyse_website(domain)
            if website_signals:
                signals["website"] = website_signals

        async with pool.acquire() as conn:
            await insert_enrichment(conn, lead_id=lead_id, signals=signals)
            await log_event(conn, lead_id=lead_id, event_type="enriched")
            stats["enriched"] += 1

        # --- Personalisation ---
        p = await _generate_personalisation(lead, signals, config=config)
        review_status = _determine_review_status(p)

        async with pool.acquire() as conn:
            await insert_personalisation(
                conn,
                lead_id=lead_id,
                opener_first_line=p.get("opener_first_line", ""),
                micro_insight=p.get("micro_insight"),
                angle_tag=p.get("angle_tag"),
                confidence_score=float(p.get("confidence_score", 0.0)),
                evidence_used=p.get("evidence_used", []),
                risk_flags=p.get("risk_flags", []),
                rung=int(p.get("rung", 1)),
                review_status=review_status,
                prompt_version=PROMPT_VERSION,
                model=PERSONALISATION_MODEL,
            )
            await conn.execute(
                "UPDATE outreach.leads SET status = 'personalised', updated_at = now() WHERE lead_id = $1::uuid",
                lead_id,
            )
            await log_event(
                conn,
                lead_id=lead_id,
                event_type="personalised",
                meta={"review_status": review_status, "rung": p.get("rung"), "confidence": p.get("confidence_score")},
            )

        stats[review_status] += 1

    logger.info("Pipeline complete: %s", stats)
    return stats
