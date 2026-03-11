"""
Analytics — Bottleneck Detection

Little's Law: L = lambda * W
The stage with the lowest throughput is the pipeline constraint.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import asyncpg

from . import queries as Q

# Pipeline order for display (non-terminal stages only)
_STAGE_ORDER = [
    "lead_created", "no_comms", "processing", "lead_qualified",
    "appointment_booked", "appointment_completed", "proposal_sent",
    "lead_won", "revenue_collected",
]


@dataclass
class StageMetrics:
    stage: str
    wip: int
    throughput_per_week: float
    median_dwell_days: float | None
    arrival_rate_per_week: float
    rho: float | None        # utilisation = arrival_rate / throughput
    is_bottleneck: bool
    is_unstable: bool         # rho >= 1


async def stage_throughput(
    conn: asyncpg.Connection,
    tenant_id: str,
    lookback_days: int = 90,
) -> list[StageMetrics]:
    """Compute per-stage throughput metrics using Little's Law.

    Returns list ordered by pipeline position.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)

    # Fetch all data in parallel-ish (sequential but fast)
    wip_rows = await conn.fetch(Q.WIP_PER_STAGE, tenant_id)
    exit_rows = await conn.fetch(Q.STAGE_EXITS, tenant_id, cutoff)
    dwell_rows = await conn.fetch(Q.MEDIAN_DWELL_PER_STAGE, tenant_id, cutoff)
    arrival_rows = await conn.fetch(Q.STAGE_ARRIVALS, tenant_id, cutoff)

    # Index by stage
    wip_map = {r["stage"]: int(r["wip"]) for r in wip_rows}
    exit_map = {}
    for r in exit_rows:
        weeks = max(float(r["span_weeks"]), 1.0)
        exit_map[r["stage"]] = float(r["exits"]) / weeks

    dwell_map = {}
    for r in dwell_rows:
        if r["median_dwell_days"] is not None:
            dwell_map[r["stage"]] = float(r["median_dwell_days"])

    arrival_map = {}
    for r in arrival_rows:
        weeks = max(float(r["span_weeks"]), 1.0)
        arrival_map[r["stage"]] = float(r["arrivals"]) / weeks

    # Build metrics for stages that have any activity
    active_stages = set(wip_map) | set(exit_map) | set(dwell_map) | set(arrival_map)

    results = []
    for stage in _STAGE_ORDER:
        if stage not in active_stages:
            continue
        throughput = exit_map.get(stage, 0.0)
        arrival = arrival_map.get(stage, 0.0)
        rho = (arrival / throughput) if throughput > 0 else None

        results.append(StageMetrics(
            stage=stage,
            wip=wip_map.get(stage, 0),
            throughput_per_week=round(throughput, 1),
            median_dwell_days=round(dwell_map[stage], 1) if stage in dwell_map else None,
            arrival_rate_per_week=round(arrival, 1),
            rho=round(rho, 2) if rho is not None else None,
            is_bottleneck=False,  # set below
            is_unstable=rho is not None and rho >= 1.0,
        ))

    # Mark bottleneck: stage with lowest non-zero throughput
    non_zero = [s for s in results if s.throughput_per_week > 0]
    if non_zero:
        bottleneck = min(non_zero, key=lambda s: s.throughput_per_week)
        bottleneck.is_bottleneck = True

    return results
