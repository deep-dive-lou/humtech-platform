"""
Optimisation Engine — Staff Routes

All routes require optimiser auth (separate from portal).
Prefix: /optimiser
"""

from __future__ import annotations

import json
import os
import uuid as _uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from uuid import uuid4

import asyncpg
from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from .auth import get_conn, require_optimiser_user
from . import anova, sequential, stats
from . import genome as G
from .evolution import EvolutionConfig, Member, init_population, step_generation
from .queries import (
    LIST_EXPERIMENTS, GET_EXPERIMENT, INSERT_EXPERIMENT, UPDATE_EXPERIMENT_STATUS,
    SET_WINNER, LIST_VARIANTS, LIST_ACTIVE_VARIANTS, INSERT_VARIANT,
    INSERT_FACTOR, INSERT_LEVEL,
    LIST_FACTORS, LIST_LEVELS, VARIANT_TOTALS, DAILY_SERIES, TAGUCHI_OBSERVATIONS,
    INSERT_GENERATION, GET_GENERATIONS, GET_LATEST_GENERATION, DEACTIVATE_VARIANTS,
)
from .rollup import rollup_experiment
from .taguchi import generate_variants, select_array

router = APIRouter(prefix="/optimiser", tags=["optimiser"])

templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "templates")
)


def _sanitize(obj):
    """Recursively convert non-JSON-native types so Jinja tojson works."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, _uuid.UUID):
        return str(obj)
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(i) for i in obj]
    return obj


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
    variant_labels: str = Form(""),  # comma-separated (bandit mode)
    factor_data: str = Form(""),  # JSON array of {name, levels} (taguchi mode)
    goals_input: str = Form(""),  # comma-separated goal names
    primary_goal_input: str = Form(""),  # which goal is primary
):
    tenant_id = staff["tenant_id"]

    # Parse goals
    goal_list = [g.strip() for g in goals_input.split(",") if g.strip()] if goals_input.strip() else ["conversion"]
    p_goal = primary_goal_input.strip() if primary_goal_input.strip() else goal_list[0]
    if p_goal not in goal_list:
        p_goal = goal_list[0]

    # Default config per mode
    config: dict = {
        "prior_alpha": 1,
        "prior_beta": 1,
        "goals": goal_list,
        "primary_goal": p_goal,
        "webhook_key": str(uuid4()),
    }

    async with conn.transaction():
        # For evolutionary mode, parse GA config from form
        if mode == "evolutionary":
            form_data = await request.form()
            config.update({
                "pop_size": int(form_data.get("pop_size", 8)),
                "mutation_rate": float(form_data.get("mutation_rate", 0.1)),
                "gene_mutation_prob": float(form_data.get("gene_mutation_prob", 0.2)),
                "crossover_fn": form_data.get("crossover_fn", "uniform"),
                "elite_pct": float(form_data.get("elite_pct", 0.2)),
                "parent_pct": float(form_data.get("parent_pct", 0.5)),
                "max_generations": int(form_data.get("max_generations", 20)),
                "neighbourhood_radius": int(form_data.get("neighbourhood_radius", 2)),
                "use_relative_fitness": True,
            })

        row = await conn.fetchrow(
            INSERT_EXPERIMENT,
            tenant_id, name, description, mode, metric_type,
            json.dumps(config),
            p_best_threshold, expected_loss_threshold, min_impressions, min_days,
        )
        experiment_id = str(row["experiment_id"])

        if mode == "taguchi" and factor_data:
            # Parse factor definitions from form
            factors_raw = json.loads(factor_data)
            factor_defs = []  # built for generate_variants()

            for sort_idx, fraw in enumerate(factors_raw):
                # Insert factor
                f_row = await conn.fetchrow(
                    INSERT_FACTOR,
                    tenant_id, experiment_id, fraw["name"], sort_idx,
                )
                factor_id = str(f_row["factor_id"])

                levels = []
                for lev_idx, lev_value in enumerate(fraw["levels"]):
                    l_row = await conn.fetchrow(
                        INSERT_LEVEL,
                        tenant_id, factor_id, lev_value, None,
                        json.dumps({"text": lev_value}), lev_idx,
                    )
                    levels.append({
                        "level_id": str(l_row["level_id"]),
                        "value": lev_value,
                    })

                factor_defs.append({
                    "factor_id": factor_id,
                    "name": fraw["name"],
                    "levels": levels,
                })

            # Generate OA-based variant combinations
            oa_variants = generate_variants(factor_defs)

            for i, ov in enumerate(oa_variants):
                # Build human-readable label: "Urgency + Get Started + ..."
                parts = [ov["factor_values"][fd["factor_id"]]["value"] for fd in factor_defs]
                label = " + ".join(parts)
                # Store factor_values as {factor_id: level_id} mapping
                fv_map = {
                    fid: info["level_id"]
                    for fid, info in ov["factor_values"].items()
                }
                await conn.execute(
                    INSERT_VARIANT,
                    tenant_id, experiment_id, label, None,
                    i == 0,  # first variant is control
                    i,
                    json.dumps(fv_map),
                )
        elif mode == "evolutionary" and factor_data:
            # Parse factors/levels (same as Taguchi)
            factors_raw = json.loads(factor_data)
            factor_order = []
            level_map: dict[str, list[str]] = {}
            levels_per_factor: list[int] = []
            factor_names: list[str] = []
            level_labels_map: dict[str, list[str]] = {}

            for sort_idx, fraw in enumerate(factors_raw):
                f_row = await conn.fetchrow(
                    INSERT_FACTOR,
                    tenant_id, experiment_id, fraw["name"], sort_idx,
                )
                factor_id = str(f_row["factor_id"])
                factor_order.append(factor_id)
                factor_names.append(fraw["name"])
                level_ids = []
                level_lbls = []
                for lev_idx, lev_value in enumerate(fraw["levels"]):
                    l_row = await conn.fetchrow(
                        INSERT_LEVEL,
                        tenant_id, factor_id, lev_value, None,
                        json.dumps({"text": lev_value}), lev_idx,
                    )
                    level_ids.append(str(l_row["level_id"]))
                    level_lbls.append(lev_value)
                level_map[factor_id] = level_ids
                level_labels_map[factor_id] = level_lbls
                levels_per_factor.append(len(level_ids))

            # Generate initial population
            pop_size = config.get("pop_size", 8)
            members = init_population(levels_per_factor, pop_size)

            # Create variant rows + control
            population_json = []
            for i, m in enumerate(members):
                fv = G.decode_genome(m.genome, factor_order, level_map)
                label = G.genome_label(m.genome, factor_names, level_labels_map, factor_order)
                v_row = await conn.fetchrow(
                    INSERT_VARIANT,
                    tenant_id, experiment_id, label, None,
                    i == 0,  # first variant is control
                    i,
                    json.dumps(fv),
                )
                vid = str(v_row["variant_id"])
                m.variant_id = vid
                population_json.append({
                    "variant_id": vid,
                    "genome": m.genome,
                    "parent_ids": [],
                    "fitness": None,
                })

            # Store generation 0
            await conn.execute(
                INSERT_GENERATION,
                tenant_id, experiment_id, 0, json.dumps(population_json),
            )

        else:
            # Bandit mode: create variants from comma-separated labels
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

    # Extract goal config
    config = json.loads(exp["config"]) if isinstance(exp["config"], str) else (exp["config"] or {})
    goals = config.get("goals", ["conversion"])
    primary_goal = config.get("primary_goal", goals[0] if goals else "conversion")
    selected_goal = request.query_params.get("goal", primary_goal)
    if selected_goal not in goals:
        selected_goal = primary_goal

    # Variant totals (filtered by selected goal)
    rows = await conn.fetch(VARIANT_TOTALS, tenant_id, experiment_id, selected_goal)
    variant_rows = [dict(r) for r in rows]

    # Compute Bayesian stats for each variant
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

    # Sequential analysis (always-valid inference)
    sequential_data = None
    if len(variant_rows) >= 2 and any(v["impressions"] > 0 for v in variant_rows):
        seq_variants = [{
            "variant_id": str(v["variant_id"]),
            "impressions": v["impressions"],
            "conversions": v["conversions"],
            "is_control": v.get("is_control", False),
        } for v in variant_rows]
        sequential_data = sequential.sequential_status(seq_variants)

        # Attach per-variant sequential results
        for v in variant_rows:
            vid = str(v["variant_id"])
            vr = (sequential_data.get("variant_results") or {}).get(vid, {})
            v["seq_ci_lower"] = vr.get("ci_lower", 0)
            v["seq_ci_upper"] = vr.get("ci_upper", 1)
            v["sprt_decision"] = vr.get("sprt_decision", "n/a")
            v["sprt_statistic"] = vr.get("sprt_statistic", 0)

    # Daily series for convergence chart
    daily_rows = await conn.fetch(DAILY_SERIES, experiment_id, selected_goal)
    daily_data = [dict(r) for r in daily_rows]

    # Build cumulative sequential CI series for convergence chart
    seq_series = {}
    for row in daily_data:
        vid = str(row["variant_id"])
        if vid not in seq_series:
            seq_series[vid] = {"cum_imp": 0, "cum_conv": 0, "points": []}
        s = seq_series[vid]
        s["cum_imp"] += row["impressions"]
        s["cum_conv"] += row["conversions"]
        lo, hi = sequential.confidence_sequence(s["cum_conv"], s["cum_imp"])
        s["points"].append({
            "day": str(row["day"]),
            "ci_lower": lo,
            "ci_upper": hi,
            "rate": s["cum_conv"] / s["cum_imp"] if s["cum_imp"] > 0 else 0,
        })

    # Total impressions/conversions
    total_impressions = sum(v["impressions"] for v in variant_rows)
    total_conversions = sum(v["conversions"] for v in variant_rows)

    # Taguchi ANOVA analysis
    taguchi_data = None
    if exp["mode"] == "taguchi" and total_impressions > 0:
        taguchi_data = await _compute_taguchi_analysis(conn, tenant_id, experiment_id, selected_goal)

    # Evolutionary generation data
    evo_data = None
    if exp["mode"] == "evolutionary":
        evo_data = await _compute_evo_data(conn, tenant_id, experiment_id, variant_rows, config)

    return templates.TemplateResponse("detail.html", {
        "request": request,
        "staff": staff,
        "brand": {},
        "exp": exp,
        "variants": _sanitize(variant_rows),
        "winner": winner,
        "sequential": sequential_data,
        "seq_series": _sanitize(seq_series),
        "daily_data": _sanitize(daily_data),
        "total_impressions": total_impressions,
        "total_conversions": total_conversions,
        "taguchi": _sanitize(taguchi_data),
        "evo": _sanitize(evo_data),
        "goals": goals,
        "selected_goal": selected_goal,
        "primary_goal": primary_goal,
    })


# ---------------------------------------------------------------------------
# Evolutionary data helper
# ---------------------------------------------------------------------------

async def _compute_evo_data(
    conn: asyncpg.Connection,
    tenant_id: str,
    experiment_id: str,
    variant_rows: list[dict],
    config: dict,
) -> dict | None:
    """Build evolutionary dashboard data: generations list, current pop, convergence."""
    gen_rows = await conn.fetch(GET_GENERATIONS, tenant_id, experiment_id)
    if not gen_rows:
        return None

    latest = dict(gen_rows[0])
    pop_raw = latest["population"]
    if isinstance(pop_raw, str):
        pop_raw = json.loads(pop_raw)

    # Build variant lookup
    v_lookup = {str(v["variant_id"]): v for v in variant_rows}

    # Annotate population with live stats
    population = []
    for member in pop_raw:
        vid = member.get("variant_id")
        v = v_lookup.get(vid, {})
        population.append({
            "variant_id": vid,
            "genome": member.get("genome", []),
            "parent_ids": member.get("parent_ids", []),
            "fitness": member.get("fitness"),
            "label": v.get("label", "?"),
            "impressions": v.get("impressions", 0),
            "conversions": v.get("conversions", 0),
            "rate": v.get("rate", 0),
            "is_elite": member.get("is_elite", False),
        })

    # Generation convergence series
    evo_config = EvolutionConfig.from_dict(config)
    gen_series = []
    for gr in reversed(gen_rows):
        gp = gr["population"]
        if isinstance(gp, str):
            gp = json.loads(gp)
        fitnesses = [m.get("fitness") for m in gp if m.get("fitness") is not None]
        gen_series.append({
            "generation": gr["generation_number"],
            "best_fitness": max(fitnesses) if fitnesses else None,
            "avg_fitness": sum(fitnesses) / len(fitnesses) if fitnesses else None,
            "pop_size": len(gp),
        })

    # Check if all variants have minimum impressions (ready to advance)
    min_imp = evo_config.min_impressions
    ready_to_advance = all(
        v_lookup.get(m.get("variant_id"), {}).get("impressions", 0) >= min_imp
        for m in pop_raw
    )

    return {
        "current_generation": latest["generation_number"],
        "max_generations": evo_config.max_generations,
        "population": population,
        "gen_series": gen_series,
        "ready_to_advance": ready_to_advance,
        "min_impressions": min_imp,
        "config": config,
    }


# ---------------------------------------------------------------------------
# Taguchi analysis helper
# ---------------------------------------------------------------------------

async def _compute_taguchi_analysis(
    conn: asyncpg.Connection, tenant_id: str, experiment_id: str, goal: str = "conversion",
) -> dict | None:
    """Compute ANOVA factor contributions and main effects for a Taguchi experiment."""
    # Fetch factors and their levels
    factor_rows = await conn.fetch(LIST_FACTORS, tenant_id, experiment_id)
    if not factor_rows:
        return None

    factors = []
    for fr in factor_rows:
        fid = str(fr["factor_id"])
        level_rows = await conn.fetch(LIST_LEVELS, tenant_id, fid)
        factors.append({
            "factor_id": fid,
            "name": fr["name"],
            "levels": [{"level_id": str(lr["level_id"])} for lr in level_rows],
        })

    # Fetch variant observations with factor_values
    obs_rows = await conn.fetch(TAGUCHI_OBSERVATIONS, tenant_id, experiment_id, goal)
    if not obs_rows:
        return None

    # Build observation list for ANOVA
    observations = []
    for row in obs_rows:
        if row["impressions"] == 0:
            continue
        fv_raw = row["factor_values"]
        if isinstance(fv_raw, str):
            fv_raw = json.loads(fv_raw)
        if not fv_raw:
            continue
        rate = row["conversions"] / row["impressions"]
        observations.append({
            "factor_values": fv_raw,  # {factor_id: level_id}
            "response": rate,
        })

    if len(observations) < 2:
        return None

    # Compute ANOVA
    try:
        contributions = anova.factor_contributions(factors, observations)
        effects = anova.main_effects(factors, observations)
        optimal = anova.optimal_combination(factors, observations)
    except (ValueError, ZeroDivisionError):
        return None

    # Build level label lookup
    level_labels = {}
    for fr in factor_rows:
        fid = str(fr["factor_id"])
        level_rows = await conn.fetch(LIST_LEVELS, tenant_id, fid)
        for lr in level_rows:
            level_labels[str(lr["level_id"])] = lr["label"]

    # Format optimal combination with human-readable names
    optimal_display = []
    for factor in factors:
        fid = factor["factor_id"]
        if fid in optimal:
            lid = optimal[fid]["level_id"]
            optimal_display.append({
                "factor_name": factor["name"],
                "level_label": level_labels.get(lid, lid),
                "mean_response": optimal[fid]["mean_response"],
            })

    # Format main effects with labels
    effects_display = {}
    for fid, level_means in effects.items():
        fname = next((f["name"] for f in factors if f["factor_id"] == fid), fid)
        effects_display[fname] = {
            level_labels.get(lid, lid): mean_val
            for lid, mean_val in level_means.items()
        }

    return {
        "contributions": contributions,
        "effects": effects_display,
        "optimal": optimal_display,
        "factors": factors,
        "level_labels": level_labels,
    }


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
# Advance evolutionary generation
# ---------------------------------------------------------------------------

@router.post("/experiments/{experiment_id}/advance")
async def advance_generation(
    experiment_id: str,
    staff: dict = Depends(require_optimiser_user),
    conn: asyncpg.Connection = Depends(get_conn),
):
    tenant_id = staff["tenant_id"]

    exp = await conn.fetchrow(GET_EXPERIMENT, tenant_id, experiment_id)
    if not exp or exp["mode"] != "evolutionary":
        return RedirectResponse(url=f"/optimiser/experiments/{experiment_id}", status_code=303)

    config_raw = json.loads(exp["config"]) if isinstance(exp["config"], str) else (exp["config"] or {})
    evo_config = EvolutionConfig.from_dict(config_raw)

    # Get current generation
    latest = await conn.fetchrow(GET_LATEST_GENERATION, tenant_id, experiment_id)
    if not latest:
        return RedirectResponse(url=f"/optimiser/experiments/{experiment_id}", status_code=303)

    gen_num = latest["generation_number"]
    pop_raw = latest["population"]
    if isinstance(pop_raw, str):
        pop_raw = json.loads(pop_raw)

    # Load factors to get levels_per_factor
    factor_rows = await conn.fetch(LIST_FACTORS, tenant_id, experiment_id)
    factor_order = []
    level_map: dict[str, list[str]] = {}
    levels_per_factor: list[int] = []
    factor_names: list[str] = []
    level_labels_map: dict[str, list[str]] = {}

    for fr in factor_rows:
        fid = str(fr["factor_id"])
        factor_order.append(fid)
        factor_names.append(fr["name"])
        level_rows = await conn.fetch(LIST_LEVELS, tenant_id, fid)
        lids = [str(lr["level_id"]) for lr in level_rows]
        lbls = [lr["label"] for lr in level_rows]
        level_map[fid] = lids
        level_labels_map[fid] = lbls
        levels_per_factor.append(len(lids))

    # Rebuild Member objects from stored population
    population = []
    for m in pop_raw:
        population.append(Member(
            genome=m["genome"],
            variant_id=m.get("variant_id"),
            parent_ids=m.get("parent_ids", []),
        ))

    # Fetch observations for fitness evaluation (use primary goal)
    goals = config_raw.get("goals", ["conversion"])
    primary_goal = config_raw.get("primary_goal", goals[0] if goals else "conversion")
    variant_totals = await conn.fetch(VARIANT_TOTALS, tenant_id, experiment_id, primary_goal)
    observations = {}
    control_variant_id = None
    for vt in variant_totals:
        vid = str(vt["variant_id"])
        observations[vid] = {
            "impressions": vt["impressions"],
            "conversions": vt["conversions"],
        }
        if vt["is_control"]:
            control_variant_id = vid

    # Run evolution step
    next_gen, gen_stats = step_generation(
        population, observations, levels_per_factor, evo_config,
        control_variant_id=control_variant_id,
    )

    async with conn.transaction():
        # Create new variant rows for bred members
        population_json = []
        keep_variant_ids = []
        for i, m in enumerate(next_gen):
            fv = G.decode_genome(m.genome, factor_order, level_map)
            label = G.genome_label(m.genome, factor_names, level_labels_map, factor_order)

            # Check if this genome already has a variant (elite carried forward)
            existing_vid = m.parent_ids[0] if m.is_elite and m.parent_ids else None
            if existing_vid:
                vid = existing_vid
            else:
                v_row = await conn.fetchrow(
                    INSERT_VARIANT,
                    tenant_id, experiment_id, label, None,
                    i == 0,  # first is control
                    i,
                    json.dumps(fv),
                )
                vid = str(v_row["variant_id"])

            keep_variant_ids.append(vid)
            population_json.append({
                "variant_id": vid,
                "genome": m.genome,
                "parent_ids": m.parent_ids,
                "fitness": None,
                "is_elite": m.is_elite,
            })

        # Deactivate old generation's variants (except those carried forward)
        await conn.execute(DEACTIVATE_VARIANTS, tenant_id, experiment_id, keep_variant_ids)

        # Store new generation
        new_gen_num = gen_num + 1

        # Update previous generation's population with fitness scores
        prev_pop_with_fitness = []
        for m_raw, m_obj in zip(pop_raw, population):
            m_raw["fitness"] = m_obj.fitness
            prev_pop_with_fitness.append(m_raw)
        await conn.execute(
            "UPDATE optimiser.evolutionary_generations SET population = $1::jsonb WHERE generation_id = $2::uuid",
            json.dumps(prev_pop_with_fitness),
            latest["generation_id"],
        )

        await conn.execute(
            INSERT_GENERATION,
            tenant_id, experiment_id, new_gen_num, json.dumps(population_json),
        )

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