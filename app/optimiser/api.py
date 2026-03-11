"""
Optimisation Engine — Public Tracking API

Unauthenticated endpoints called from client landing pages.
CORS headers added manually (not app-wide).
"""

from __future__ import annotations

import json
from typing import Optional

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ..db import get_pool
from . import stats
from .queries import (
    INSERT_OBSERVATION, VARIANT_POSTERIORS, GET_EXPERIMENT,
    GET_LATEST_GENERATION,
)
from .rollup import rollup_experiment

router = APIRouter(prefix="/optimiser/api", tags=["optimiser-api"])

# ---------------------------------------------------------------------------
# CORS helper — only on these public routes
# ---------------------------------------------------------------------------

_CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}


def _cors_json(data: dict, status_code: int = 200) -> JSONResponse:
    return JSONResponse(content=data, status_code=status_code, headers=_CORS_HEADERS)


@router.options("/track")
@router.options("/allocate/{experiment_id}")
@router.options("/webhook")
async def cors_preflight():
    return JSONResponse(content={}, headers=_CORS_HEADERS)


# ---------------------------------------------------------------------------
# POST /optimiser/api/track — record impression or conversion
# ---------------------------------------------------------------------------

class TrackEvent(BaseModel):
    experiment_id: str
    variant_id: str
    event_type: str  # 'impression' or 'conversion'
    goal: str = "conversion"  # which conversion goal (ignored for impressions)
    value: Optional[float] = None
    visitor_id: Optional[str] = None
    source: Optional[str] = "js_snippet"


@router.post("/track")
async def track(body: TrackEvent):
    if body.event_type not in ("impression", "conversion"):
        return _cors_json({"error": "event_type must be 'impression' or 'conversion'"}, 400)

    pool = await get_pool()
    async with pool.acquire() as conn:
        # Look up tenant_id from experiment (avoids requiring it in the public API)
        exp = await conn.fetchrow(
            "SELECT tenant_id FROM optimiser.experiments WHERE experiment_id = $1::uuid AND status = 'running'",
            body.experiment_id,
        )
        if not exp:
            return _cors_json({"error": "experiment not found or not running"}, 404)

        # For impressions, goal is stored but irrelevant to rollup
        goal = body.goal if body.event_type == "conversion" else "conversion"
        await conn.execute(
            INSERT_OBSERVATION,
            exp["tenant_id"],
            body.experiment_id,
            body.variant_id,
            body.event_type,
            body.value,
            body.visitor_id,
            body.source,
            goal,
        )

    return _cors_json({"ok": True})


# ---------------------------------------------------------------------------
# GET /optimiser/api/allocate/{experiment_id} — Thompson Sampling allocation
# ---------------------------------------------------------------------------

@router.get("/allocate/{experiment_id}")
async def allocate(experiment_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Verify experiment is running
        exp = await conn.fetchrow(
            "SELECT tenant_id, config FROM optimiser.experiments WHERE experiment_id = $1::uuid AND status = 'running'",
            experiment_id,
        )
        if not exp:
            return _cors_json({"error": "experiment not found or not running"}, 404)

        config = json.loads(exp["config"]) if isinstance(exp["config"], str) else (exp["config"] or {})
        prior_alpha = config.get("prior_alpha", 1)
        prior_beta = config.get("prior_beta", 1)
        goals = config.get("goals", ["conversion"])
        primary_goal = config.get("primary_goal", goals[0] if goals else "conversion")

        # Rollup first to ensure fresh stats
        await rollup_experiment(conn, experiment_id)

        # Fetch posteriors for the primary goal (used for allocation decisions)
        rows = await conn.fetch(VARIANT_POSTERIORS, experiment_id, primary_goal)
        if not rows:
            return _cors_json({"error": "no active variants"}, 404)

        # For evolutionary mode, filter to current generation's variants only
        mode = exp.get("mode") if hasattr(exp, "get") else dict(exp).get("mode")
        if mode == "evolutionary":
            gen = await conn.fetchrow(GET_LATEST_GENERATION, exp["tenant_id"], experiment_id)
            if gen:
                pop = json.loads(gen["population"]) if isinstance(gen["population"], str) else gen["population"]
                gen_vids = {m["variant_id"] for m in pop}
                rows = [r for r in rows if str(r["variant_id"]) in gen_vids]

        variants = []
        for r in rows:
            alpha, beta = stats.beta_posterior(prior_alpha, prior_beta, r["successes"], r["failures"])
            variants.append({
                "variant_id": str(r["variant_id"]),
                "alpha": alpha,
                "beta": beta,
            })

        if not variants:
            return _cors_json({"error": "no active variants"}, 404)

        chosen_id = stats.thompson_allocate(variants)

    return _cors_json({"variant_id": chosen_id})


# ---------------------------------------------------------------------------
# POST /optimiser/api/webhook — CRM inbound webhook for downstream conversions
# ---------------------------------------------------------------------------

class WebhookEvent(BaseModel):
    experiment_id: str
    variant_id: str
    goal: str  # e.g. 'booking', 'proposal', 'won'
    visitor_id: Optional[str] = None
    value: Optional[float] = None


@router.post("/webhook")
async def webhook(body: WebhookEvent, request: Request):
    pool = await get_pool()
    async with pool.acquire() as conn:
        exp = await conn.fetchrow(
            "SELECT tenant_id, config FROM optimiser.experiments WHERE experiment_id = $1::uuid AND status = 'running'",
            body.experiment_id,
        )
        if not exp:
            return _cors_json({"error": "experiment not found or not running"}, 404)

        # Verify webhook key
        config = json.loads(exp["config"]) if isinstance(exp["config"], str) else (exp["config"] or {})
        webhook_key = config.get("webhook_key")
        if webhook_key:
            provided_key = request.headers.get("X-Webhook-Key", "")
            if provided_key != webhook_key:
                return _cors_json({"error": "invalid webhook key"}, 403)

        await conn.execute(
            INSERT_OBSERVATION,
            exp["tenant_id"],
            body.experiment_id,
            body.variant_id,
            "conversion",
            body.value,
            body.visitor_id,
            "webhook",
            body.goal,
        )

    return _cors_json({"ok": True})