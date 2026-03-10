"""
Optimisation Engine — Staff Routes

All routes require optimiser auth (separate from portal).
Prefix: /optimiser
"""

from __future__ import annotations

import json
import os
from typing import Optional

import asyncpg
from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from .auth import get_conn, require_optimiser_user
from . import stats
from .queries import (
    LIST_EXPERIMENTS, GET_EXPERIMENT, INSERT_EXPERIMENT, UPDATE_EXPERIMENT_STATUS,
    SET_WINNER, LIST_VARIANTS, INSERT_VARIANT, VARIANT_TOTALS, DAILY_SERIES,
)
from .rollup import rollup_experiment

router = APIRouter(prefix="/optimiser", tags=["optimiser"])

templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "templates")
)


# ---------------------------------------------------------------------------
# Experiment list
# ---------------------------------------------------------------------------

@router.get("/", response_class=HTMLResponse)
async def experiment_list(
    request: Request,
    staff: dict = Depends(require_optimiser_user),
    conn: asyncpg.Connection = Depends(get_conn),
):
    tenant_id = staff["tenant_id"]
    rows = await conn.fetch(LIST_EXPERIMENTS, tenant_id)
    experiments = [dict(r) for r in rows]

    return templates.TemplateResponse("list.html", {
        "request": request,
        "staff": staff,
        "brand": {},
        "experiments": experiments,
    })


# ---------------------------------------------------------------------------
# Create experiment — form + handler
# ---------------------------------------------------------------------------

@router.get("/new", response_class=HTMLResponse)
async def create_form(
    request: Request,
    staff: dict = Depends(require_optimiser_user),
):
    return templates.TemplateResponse("create.html", {
        "request": request,
        "staff": staff,
        "brand": {},
    })


@router.post("/experiments", response_class=HTMLResponse)
async def create_experiment(
    request: Request,
    staff: dict = Depends(require_optimiser_user),
    conn: asyncpg.Connection = Depends(get_conn),
    name: str = Form(...),
    description: str = Form(""),
    mode: str = Form("bandit"),
    metric_type: str = Form("conversion"),
    p_best_threshold: float = Form(0.95),
    expected_loss_threshold: float = Form(0.01),
    min_impressions: int = Form(100),
    min_days: int = Form(7),
    variant_labels: str = Form(""),  # comma-separated
):
    tenant_id = staff["tenant_id"]

    # Default config per mode
    config = {"prior_alpha": 1, "prior_beta": 1}

    async with conn.transaction():
        row = await conn.fetchrow(
            INSERT_EXPERIMENT,
            tenant_id, name, description, mode, metric_type,
            json.dumps(config),
            p_best_threshold, expected_loss_threshold, min_impressions, min_days,
        )
        experiment_id = str(row["experiment_id"])

        # Create variants from comma-separated labels
        labels = [l.strip() for l in variant_labels.split(",") if l.strip()]
        for i, label in enumerate(labels):
            await conn.execute(
                INSERT_VARIANT,
                tenant_id, experiment_id, label, None,
                i == 0,  # first variant is control
                i,
                json.dumps({}),
            )

    return RedirectResponse(url=f"/optimiser/experiments/{experiment_id}", status_code=303)


# ---------------------------------------------------------------------------
# Experiment detail — dashboard with stats + charts
# ---------------------------------------------------------------------------

@router.get("/experiments/{experiment_id}", response_class=HTMLResponse)
async def experiment_detail(
    request: Request,
    experiment_id: str,
    staff: dict = Depends(require_optimiser_user),
    conn: asyncpg.Connection = Depends(get_conn),
):
    tenant_id = staff["tenant_id"]

    exp = await conn.fetchrow(GET_EXPERIMENT, tenant_id, experiment_id)
    if not exp:
        return HTMLResponse("Experiment not found", status_code=404)
    exp = dict(exp)

    # Rollup observations → daily_stats
    await rollup_experiment(conn, experiment_id)

    # Variant totals
    rows = await conn.fetch(VARIANT_TOTALS, tenant_id, experiment_id)
    variant_rows = [dict(r) for r in rows]

    # Compute Bayesian stats for each variant
    config = json.loads(exp["config"]) if isinstance(exp["config"], str) else (exp["config"] or {})
    prior_alpha = config.get("prior_alpha", 1)
    prior_beta = config.get("prior_beta", 1)

    variants_for_stats = []
    for v in variant_rows:
        successes = v["conversions"]
        failures = v["impressions"] - v["conversions"]
        alpha, beta = stats.beta_posterior(prior_alpha, prior_beta, successes, failures)
        ci_low, ci_high = stats.credible_interval(alpha, beta)
        v["alpha"] = alpha
        v["beta"] = beta
        v["rate"] = successes / v["impressions"] if v["impressions"] > 0 else 0
        v["ci_low"] = ci_low
        v["ci_high"] = ci_high
        variants_for_stats.append({
            "variant_id": str(v["variant_id"]),
            "alpha": alpha,
            "beta": beta,
            "impressions": v["impressions"],
        })

    # P(best) and expected loss
    if len(variants_for_stats) >= 2 and any(v["impressions"] > 0 for v in variant_rows):
        pb = stats.p_best(variants_for_stats)
        el = stats.expected_loss(variants_for_stats)
        for v in variant_rows:
            vid = str(v["variant_id"])
            v["p_best"] = pb.get(vid, 0)
            v["expected_loss"] = el.get(vid, 0)
    else:
        for v in variant_rows:
            v["p_best"] = 0
            v["expected_loss"] = 0

    # Winner check
    winner = None
    if exp["status"] == "running" and variants_for_stats and exp["started_at"]:
        rules = {
            "p_best_threshold": float(exp["p_best_threshold"]),
            "expected_loss_threshold": float(exp["expected_loss_threshold"]),
            "min_impressions": exp["min_impressions"],
            "min_days": exp["min_days"],
        }
        winner = stats.check_winner(variants_for_stats, rules, exp["started_at"])

    # Daily series for convergence chart
    daily_rows = await conn.fetch(DAILY_SERIES, experiment_id)
    daily_data = [dict(r) for r in daily_rows]

    # Total impressions/conversions
    total_impressions = sum(v["impressions"] for v in variant_rows)
    total_conversions = sum(v["conversions"] for v in variant_rows)

    return templates.TemplateResponse("detail.html", {
        "request": request,
        "staff": staff,
        "brand": {},
        "exp": exp,
        "variants": variant_rows,
        "winner": winner,
        "daily_data": daily_data,
        "total_impressions": total_impressions,
        "total_conversions": total_conversions,
    })


# ---------------------------------------------------------------------------
# Status transitions
# ---------------------------------------------------------------------------

@router.post("/experiments/{experiment_id}/start")
async def start_experiment(
    experiment_id: str,
    staff: dict = Depends(require_optimiser_user),
    conn: asyncpg.Connection = Depends(get_conn),
):
    await conn.execute(UPDATE_EXPERIMENT_STATUS, staff["tenant_id"], experiment_id, "running")
    return RedirectResponse(url=f"/optimiser/experiments/{experiment_id}", status_code=303)


@router.post("/experiments/{experiment_id}/pause")
async def pause_experiment(
    experiment_id: str,
    staff: dict = Depends(require_optimiser_user),
    conn: asyncpg.Connection = Depends(get_conn),
):
    await conn.execute(UPDATE_EXPERIMENT_STATUS, staff["tenant_id"], experiment_id, "paused")
    return RedirectResponse(url=f"/optimiser/experiments/{experiment_id}", status_code=303)


@router.post("/experiments/{experiment_id}/complete")
async def complete_experiment(
    experiment_id: str,
    winner_variant_id: str = Form(...),
    staff: dict = Depends(require_optimiser_user),
    conn: asyncpg.Connection = Depends(get_conn),
):
    await conn.execute(SET_WINNER, staff["tenant_id"], experiment_id, winner_variant_id)
    return RedirectResponse(url=f"/optimiser/experiments/{experiment_id}", status_code=303)


# ---------------------------------------------------------------------------
# Add variant
# ---------------------------------------------------------------------------

@router.post("/experiments/{experiment_id}/variants")
async def add_variant(
    experiment_id: str,
    staff: dict = Depends(require_optimiser_user),
    conn: asyncpg.Connection = Depends(get_conn),
    label: str = Form(...),
    description: str = Form(""),
):
    await conn.execute(
        INSERT_VARIANT,
        staff["tenant_id"], experiment_id, label, description or None,
        False, 0, json.dumps({}),
    )
    return RedirectResponse(url=f"/optimiser/experiments/{experiment_id}", status_code=303)