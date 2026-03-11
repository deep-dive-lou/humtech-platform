"""
Analytics Command Centre — Routes

Lou's internal analytics UI. Four pages:
  /analytics/          Dashboard summary (KPIs + CIs + sparklines + narrative)
  /analytics/control-charts   P-charts + CUSUM + Western Electric anomaly flags
  /analytics/survival         KM curves + dead deal alerts
  /analytics/bottleneck       Little's Law throughput + constraint detection
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import asyncpg
from fastapi import APIRouter, Cookie, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from ...db import get_pool
from .auth import (
    AnalyticsNotAuthenticated,
    create_jwt,
    require_analytics,
    verify_password,
    _COOKIE_NAME,
    _EXPIRE_HOURS,
)
from . import queries as Q
from .stats import choose_ci, format_ci, two_proportion_z_test
from .anomaly import p_chart, western_electric_rules, cusum
from .survival import kaplan_meier_per_stage, dead_deal_alerts
from .bottleneck import stage_throughput

router = APIRouter(prefix="/analytics", tags=["analytics"])

_templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "templates")
)

# ── Helpers ───────────────────────────────────────────────────────────


async def _get_conn():
    pool = await get_pool()
    async with pool.acquire() as conn:
        yield conn


async def _resolve_tenant(conn: asyncpg.Connection, slug: str) -> str | None:
    """Resolve tenant slug to UUID string."""
    row = await conn.fetchrow(Q.TENANT_BY_SLUG, slug)
    return str(row["tenant_id"]) if row else None


# Rate metrics that have numerator/denominator in snapshot totals
_RATE_METRICS = [
    {
        "key": "lead_to_qualified_rate",
        "label": "Lead to Qualified",
        "num_key": "total_qualified",
        "den_key": "total_leads",
    },
    {
        "key": "qualified_to_booked_rate",
        "label": "Qualified to Booked",
        "num_key": "total_booked",
        "den_key": "total_qualified",
    },
    {
        "key": "show_rate",
        "label": "Show Rate",
        "num_key": "total_completed",
        "den_key": "total_booked",
    },
    {
        "key": "show_to_proposal_rate",
        "label": "Show to Proposal",
        "num_key": "total_proposals",
        "den_key": "total_completed",
    },
    {
        "key": "close_rate",
        "label": "Close Rate",
        "num_key": "total_won",
        "den_key": "total_proposals",
    },
    {
        "key": "competitive_win_rate",
        "label": "Competitive Win Rate",
        "num_key": "total_won",
        "den_key": None,  # won / (won + lost) — computed specially
    },
]


def _parse_metrics(raw) -> dict:
    """Parse metrics from DB — may be JSONB dict or JSON string."""
    if isinstance(raw, str):
        return json.loads(raw)
    if isinstance(raw, dict):
        return raw
    return {}


def _extract_metric(metrics: dict, key: str, default=None):
    """Safely extract a metric value from snapshot."""
    val = metrics.get(key, default)
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


# ── Auth routes ───────────────────────────────────────────────────────


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: Optional[str] = None):
    return _templates.TemplateResponse("login.html", {
        "request": request,
        "error": error,
    })


@router.post("/login", response_class=HTMLResponse)
async def login_submit(request: Request, password: str = Form(...)):
    if not verify_password(password):
        return _templates.TemplateResponse("login.html", {
            "request": request,
            "error": "Invalid password",
        })
    token = create_jwt()
    response = RedirectResponse(url="/analytics/", status_code=303)
    response.set_cookie(
        key=_COOKIE_NAME,
        value=token,
        httponly=True,
        max_age=_EXPIRE_HOURS * 3600,
        samesite="lax",
    )
    return response


@router.get("/logout")
async def logout():
    response = RedirectResponse(url="/analytics/login", status_code=303)
    response.delete_cookie(_COOKIE_NAME)
    return response


# ── Dashboard (Layer 1) ──────────────────────────────────────────────


@router.get("/", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    tenant: str = Query(default="resg"),
    period_type: str = Query(default="monthly"),
    user: dict = Depends(require_analytics),
    conn: asyncpg.Connection = Depends(_get_conn),
):
    tenant_id = await _resolve_tenant(conn, tenant)
    if not tenant_id:
        return _templates.TemplateResponse("dashboard.html", {
            "request": request,
            "error": f"Tenant '{tenant}' not found",
            "tenant": tenant,
            "kpis": [],
            "sparklines": {},
            "narratives": [],
            "anomaly_count": 0,
            "active_page": "dashboard",
        })

    # Fetch snapshot time series
    rows = await conn.fetch(Q.SNAPSHOTS_SERIES, tenant_id, period_type)
    if not rows:
        return _templates.TemplateResponse("dashboard.html", {
            "request": request,
            "error": "No metric snapshots found. Run compute_metric_snapshot.py first.",
            "tenant": tenant,
            "kpis": [],
            "sparklines": {},
            "narratives": [],
            "anomaly_count": 0,
            "active_page": "dashboard",
        })

    snapshots = [{"period_start": r["period_start"], "metrics": _parse_metrics(r["metrics"])} for r in rows]
    latest = snapshots[-1]["metrics"]

    # Build KPIs with CIs
    kpis = []

    # Pipeline win rate
    won = int(latest.get("total_won", 0))
    total = int(latest.get("total_leads", 0))
    pwr = _extract_metric(latest, "pipeline_win_rate", 0)
    ci_lo, ci_hi, ci_method = choose_ci(won, total)
    kpis.append({
        "label": "Pipeline Win Rate",
        "value": f"{pwr * 100:.1f}%",
        "ci": f"({ci_lo * 100:.1f}–{ci_hi * 100:.1f}%)",
        "ci_method": ci_method,
        "icon": "target",
        "sparkline_key": "pipeline_win_rate",
    })

    # Competitive win rate
    lost = int(latest.get("total_lost", 0))
    cwr = _extract_metric(latest, "competitive_win_rate", 0)
    cwr_n = won + lost
    ci_lo, ci_hi, ci_method = choose_ci(won, cwr_n)
    kpis.append({
        "label": "Competitive Win Rate",
        "value": f"{cwr * 100:.1f}%",
        "ci": f"({ci_lo * 100:.1f}–{ci_hi * 100:.1f}%)",
        "ci_method": ci_method,
        "icon": "trophy",
        "sparkline_key": "competitive_win_rate",
    })

    # Pipeline velocity
    velocity = _extract_metric(latest, "pipeline_velocity_gbp_per_day", 0)
    kpis.append({
        "label": "Pipeline Velocity",
        "value": f"£{velocity:,.0f}/day",
        "ci": "",
        "ci_method": "",
        "icon": "trending-up",
        "sparkline_key": "pipeline_velocity_gbp_per_day",
    })

    # Bottleneck stage
    bn_data = await stage_throughput(conn, tenant_id)
    bottleneck_stage = next((s.stage for s in bn_data if s.is_bottleneck), "—")
    kpis.append({
        "label": "Bottleneck Stage",
        "value": bottleneck_stage.replace("_", " ").title(),
        "ci": "",
        "ci_method": "",
        "icon": "alert-triangle",
        "sparkline_key": None,
    })

    # Anomaly count (quick scan of latest period)
    anomaly_count = 0
    for rm in _RATE_METRICS:
        vals = [_extract_metric(s["metrics"], rm["key"], 0) for s in snapshots]
        if len(vals) >= 3:
            chart = p_chart(vals)
            violations = western_electric_rules(vals, chart.center, chart.sigma)
            # Count violations in last period only
            anomaly_count += sum(1 for v in violations if v.index == len(vals) - 1)

    kpis.append({
        "label": "Active Anomalies",
        "value": str(anomaly_count),
        "ci": "",
        "ci_method": "",
        "icon": "bell" if anomaly_count == 0 else "bell-ring",
        "sparkline_key": None,
    })

    # Build sparkline data (last 12 periods)
    sparklines = {}
    for rm in _RATE_METRICS:
        vals = [_extract_metric(s["metrics"], rm["key"], 0) for s in snapshots[-12:]]
        sparklines[rm["key"]] = vals
    # Add velocity sparkline
    sparklines["pipeline_velocity_gbp_per_day"] = [
        _extract_metric(s["metrics"], "pipeline_velocity_gbp_per_day", 0)
        for s in snapshots[-12:]
    ]
    sparklines["pipeline_win_rate"] = [
        _extract_metric(s["metrics"], "pipeline_win_rate", 0)
        for s in snapshots[-12:]
    ]
    sparklines["competitive_win_rate"] = [
        _extract_metric(s["metrics"], "competitive_win_rate", 0)
        for s in snapshots[-12:]
    ]

    # Build narratives
    narratives = _build_narratives(snapshots, latest, anomaly_count, bottleneck_stage)

    return _templates.TemplateResponse("dashboard.html", {
        "request": request,
        "error": None,
        "tenant": tenant,
        "period_type": period_type,
        "kpis": kpis,
        "sparklines": json.dumps(sparklines),
        "narratives": narratives,
        "anomaly_count": anomaly_count,
        "active_page": "dashboard",
    })


def _build_narratives(snapshots, latest, anomaly_count, bottleneck_stage) -> list[dict]:
    """Generate one-sentence narrative per key metric."""
    narratives = []

    # Win rate narrative
    pwr = _extract_metric(latest, "pipeline_win_rate", 0)
    won = int(latest.get("total_won", 0))
    total = int(latest.get("total_leads", 0))
    ci_lo, ci_hi, _ = choose_ci(won, total)
    narratives.append({
        "icon": "target",
        "text": f"Pipeline win rate is {pwr*100:.1f}% (95% CI: {ci_lo*100:.1f}–{ci_hi*100:.1f}%). "
                f"Based on {won:,} wins from {total:,} leads.",
    })

    # Velocity narrative
    velocity = _extract_metric(latest, "pipeline_velocity_gbp_per_day", 0)
    if velocity > 0:
        narratives.append({
            "icon": "trending-up",
            "text": f"Pipeline velocity is £{velocity:,.0f}/day. "
                    f"This is the rate at which pipeline value converts to revenue.",
        })

    # Bottleneck narrative
    if bottleneck_stage and bottleneck_stage != "—":
        narratives.append({
            "icon": "alert-triangle",
            "text": f"The pipeline bottleneck is {bottleneck_stage.replace('_', ' ')}. "
                    f"Improving throughput here has the highest impact on overall pipeline flow.",
        })

    # Anomaly narrative
    if anomaly_count > 0:
        narratives.append({
            "icon": "bell-ring",
            "text": f"{anomaly_count} Western Electric rule violation(s) detected in the latest period. "
                    f"Check control charts for detail.",
        })
    else:
        narratives.append({
            "icon": "check-circle",
            "text": "No anomalies detected. All rate metrics are within normal statistical variation.",
        })

    return narratives


# ── Control Charts (Layer 2) ─────────────────────────────────────────


@router.get("/control-charts", response_class=HTMLResponse)
async def control_charts_page(
    request: Request,
    tenant: str = Query(default="resg"),
    period_type: str = Query(default="monthly"),
    user: dict = Depends(require_analytics),
    conn: asyncpg.Connection = Depends(_get_conn),
):
    tenant_id = await _resolve_tenant(conn, tenant)
    if not tenant_id:
        return _templates.TemplateResponse("control_charts.html", {
            "request": request, "error": f"Tenant '{tenant}' not found",
            "tenant": tenant, "charts": [], "active_page": "control_charts",
        })

    rows = await conn.fetch(Q.SNAPSHOTS_SERIES, tenant_id, period_type)
    snapshots = [{"period_start": r["period_start"], "metrics": _parse_metrics(r["metrics"])} for r in rows]

    charts = []
    for rm in _RATE_METRICS:
        values = [_extract_metric(s["metrics"], rm["key"], 0) for s in snapshots]
        labels = [s["period_start"].strftime("%b %Y") for s in snapshots]

        if len(values) < 3:
            continue

        # P-chart
        chart_data = p_chart(values)
        violations = western_electric_rules(values, chart_data.center, chart_data.sigma)

        # CUSUM
        cusum_data = cusum(values, target=chart_data.center)

        charts.append({
            "key": rm["key"],
            "label": rm["label"],
            "labels": labels,
            "values": values,
            "center": chart_data.center,
            "ucl": chart_data.ucl,
            "lcl": chart_data.lcl,
            "sigma1_upper": chart_data.sigma1_upper,
            "sigma1_lower": chart_data.sigma1_lower,
            "sigma2_upper": chart_data.sigma2_upper,
            "sigma2_lower": chart_data.sigma2_lower,
            "violations": [{"index": v.index, "rule": v.rule, "desc": v.description} for v in violations],
            "cusum_plus": cusum_data.cusum_plus,
            "cusum_minus": cusum_data.cusum_minus,
            "cusum_signals": cusum_data.signals,
        })

    return _templates.TemplateResponse("control_charts.html", {
        "request": request,
        "error": None,
        "tenant": tenant,
        "charts": charts,
        "charts_json": json.dumps(charts, default=str),
        "active_page": "control_charts",
    })


# ── Survival Analysis (Layer 2) ──────────────────────────────────────


@router.get("/survival", response_class=HTMLResponse)
async def survival_page(
    request: Request,
    tenant: str = Query(default="resg"),
    lookback: int = Query(default=90),
    user: dict = Depends(require_analytics),
    conn: asyncpg.Connection = Depends(_get_conn),
):
    tenant_id = await _resolve_tenant(conn, tenant)
    if not tenant_id:
        return _templates.TemplateResponse("survival.html", {
            "request": request, "error": f"Tenant '{tenant}' not found",
            "tenant": tenant, "curves": {}, "dead_deals": [],
            "active_page": "survival",
        })

    km_curves = await kaplan_meier_per_stage(conn, tenant_id, lookback_days=lookback)
    dead_deals = await dead_deal_alerts(conn, tenant_id, lookback_days=lookback)

    # Serialise KM curves for Plotly
    curves_json = {}
    for stage, curve in km_curves.items():
        curves_json[stage] = {
            "times": curve.times,
            "survival": curve.survival,
            "median": curve.median,
            "n_at_risk": curve.n_at_risk,
            "n_events": curve.n_events,
        }

    dead_deals_list = [
        {
            "lead_id": d.lead_id,
            "name": d.name or "—",
            "stage": d.stage.replace("_", " ").title(),
            "days_in_stage": d.days_in_stage,
            "median_for_stage": d.median_for_stage,
            "lead_value": d.lead_value,
            "source": d.source or "—",
        }
        for d in dead_deals
    ]

    return _templates.TemplateResponse("survival.html", {
        "request": request,
        "error": None,
        "tenant": tenant,
        "lookback": lookback,
        "curves_json": json.dumps(curves_json),
        "dead_deals": dead_deals_list,
        "active_page": "survival",
    })


# ── Bottleneck Analysis (Layer 2) ────────────────────────────────────


@router.get("/bottleneck", response_class=HTMLResponse)
async def bottleneck_page(
    request: Request,
    tenant: str = Query(default="resg"),
    lookback: int = Query(default=90),
    user: dict = Depends(require_analytics),
    conn: asyncpg.Connection = Depends(_get_conn),
):
    tenant_id = await _resolve_tenant(conn, tenant)
    if not tenant_id:
        return _templates.TemplateResponse("bottleneck.html", {
            "request": request, "error": f"Tenant '{tenant}' not found",
            "tenant": tenant, "stages": [], "active_page": "bottleneck",
        })

    stages = await stage_throughput(conn, tenant_id, lookback_days=lookback)

    stages_list = [
        {
            "stage": s.stage.replace("_", " ").title(),
            "stage_raw": s.stage,
            "wip": s.wip,
            "throughput_per_week": s.throughput_per_week,
            "median_dwell_days": s.median_dwell_days,
            "arrival_rate_per_week": s.arrival_rate_per_week,
            "rho": s.rho,
            "is_bottleneck": s.is_bottleneck,
            "is_unstable": s.is_unstable,
        }
        for s in stages
    ]

    return _templates.TemplateResponse("bottleneck.html", {
        "request": request,
        "error": None,
        "tenant": tenant,
        "lookback": lookback,
        "stages": stages_list,
        "stages_json": json.dumps(stages_list, default=str),
        "active_page": "bottleneck",
    })
