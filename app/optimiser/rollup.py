"""
Optimisation Engine — Daily Stats Rollup

Aggregates raw observations into daily_stats via upsert.
Called on dashboard page load (cheap — single SQL statement).
"""

from __future__ import annotations

import asyncpg

from .queries import ROLLUP_DAILY_STATS


async def rollup_experiment(conn: asyncpg.Connection, experiment_id: str) -> None:
    """Recompute daily_stats for one experiment from observations."""
    await conn.execute(ROLLUP_DAILY_STATS, experiment_id)