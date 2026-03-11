"""
Analytics — Survival Analysis

Kaplan-Meier estimator (pure numpy) and dead deal detection.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import numpy as np
import asyncpg

from . import queries as Q


@dataclass
class KMCurve:
    times: list[float]       # event times (days)
    survival: list[float]    # S(t) at each time
    median: float | None     # median survival time (days), None if >50% still alive
    n_at_risk: int           # initial number at risk
    n_events: int            # total observed events


def kaplan_meier(durations: list[float], events: list[int]) -> KMCurve:
    """Compute Kaplan-Meier survival curve from duration/event data.

    Args:
        durations: time-to-event (or censoring) for each subject
        events: 1 = event observed, 0 = right-censored

    Returns KMCurve with step-function data.
    """
    if not durations:
        return KMCurve([], [], None, 0, 0)

    arr = sorted(zip(durations, events), key=lambda x: x[0])
    n = len(arr)
    n_at_risk = n

    times = [0.0]
    survival = [1.0]
    s = 1.0
    total_events = 0

    i = 0
    while i < n:
        t = arr[i][0]
        d = 0  # events at this time
        c = 0  # censored at this time

        while i < n and arr[i][0] == t:
            if arr[i][1] == 1:
                d += 1
            else:
                c += 1
            i += 1

        if d > 0 and n_at_risk > 0:
            s *= (n_at_risk - d) / n_at_risk
            times.append(t)
            survival.append(s)
            total_events += d

        n_at_risk -= (d + c)

    # Median: first time S(t) <= 0.5
    median = None
    for t, sv in zip(times, survival):
        if sv <= 0.5:
            median = t
            break

    return KMCurve(
        times=times,
        survival=survival,
        median=median,
        n_at_risk=n,
        n_events=total_events,
    )


async def kaplan_meier_per_stage(
    conn: asyncpg.Connection,
    tenant_id: str,
    lookback_days: int = 90,
) -> dict[str, KMCurve]:
    """Compute KM curve for each pipeline stage.

    Returns {stage_name: KMCurve}.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    rows = await conn.fetch(Q.STAGE_DWELL_TIMES, tenant_id, cutoff)

    # Group by stage
    stage_data: dict[str, tuple[list[float], list[int]]] = {}
    for row in rows:
        stage = row["stage"]
        dur = float(row["duration_days"])
        event = int(row["event_observed"])
        if dur < 0:
            continue
        if stage not in stage_data:
            stage_data[stage] = ([], [])
        stage_data[stage][0].append(dur)
        stage_data[stage][1].append(event)

    results = {}
    for stage, (durations, events) in stage_data.items():
        results[stage] = kaplan_meier(durations, events)

    return results


@dataclass
class DeadDeal:
    lead_id: str
    name: str | None
    stage: str
    days_in_stage: float
    median_for_stage: float | None
    lead_value: float | None
    source: str | None


async def dead_deal_alerts(
    conn: asyncpg.Connection,
    tenant_id: str,
    threshold_multiplier: float = 3.0,
    lookback_days: int = 90,
) -> list[DeadDeal]:
    """Find open deals exceeding threshold * median dwell for their stage."""
    # Get median dwell times per stage from KM
    km_curves = await kaplan_meier_per_stage(conn, tenant_id, lookback_days)
    medians = {stage: curve.median for stage, curve in km_curves.items() if curve.median}

    # Get candidates
    rows = await conn.fetch(Q.DEAD_DEAL_CANDIDATES, tenant_id)

    alerts = []
    for row in rows:
        stage = row["current_stage"]
        days = float(row["days_in_stage"])
        median = medians.get(stage)
        if median and days > threshold_multiplier * median:
            alerts.append(DeadDeal(
                lead_id=str(row["lead_id"]),
                name=row["name"],
                stage=stage,
                days_in_stage=round(days, 1),
                median_for_stage=round(median, 1),
                lead_value=float(row["lead_value"]) if row["lead_value"] else None,
                source=row["source"],
            ))

    # Sort by value (highest first), then by days. Limit to top 50.
    alerts.sort(key=lambda d: (-(d.lead_value or 0), -d.days_in_stage))
    return alerts[:50]
