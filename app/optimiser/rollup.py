"""
Optimisation Engine — Daily Stats Rollup

Aggregates raw observations into daily_stats via upsert.
Two-step: conversions grouped by goal, then impression counts backfilled.
Called on dashboard page load (cheap — two SQL statements).
"""

from __future__ import annotations

import asyncpg

from .queries import ROLLUP_CONVERSIONS, ROLLUP_IMPRESSIONS


async def rollup_experiment(conn: asyncpg.Connection, experiment_id: str) -> None:
    """Recompute daily_stats for one experiment from observations."""
    await conn.execute(ROLLUP_CONVERSIONS, experiment_id)
    await conn.execute(ROLLUP_IMPRESSIONS, experiment_id)
