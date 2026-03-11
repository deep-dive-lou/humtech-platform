"""
Analytics — Cohort Analysis

Cohort conversion matrices and Simpson's Paradox detection.
All heavy computation in pure functions, async wrappers at the bottom.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import numpy as np

from . import queries as Q

log = logging.getLogger(__name__)

# ── Dataclasses ──────────────────────────────────────────────────────


@dataclass
class CohortMatrix:
    """Cohort analysis heatmap data."""

    cohort_labels: list[str]                         # ["Aug 2022", "Sep 2022", ...]
    period_offsets: list[int]                         # [0, 1, 2, ..., max_periods-1]
    conversion_rates: list[list[float | None]]        # [cohort][offset]
    cohort_sizes: list[int]
    max_offset_per_cohort: list[int]                  # how many months elapsed for each cohort


@dataclass
class SourceBreakdown:
    source: str
    rate_pre: float
    rate_post: float
    n_pre: int
    n_post: int
    direction: str  # "up" or "down"


@dataclass
class SimpsonCheck:
    """Simpson's Paradox detection result."""

    has_paradox: bool
    aggregate_rate_pre: float
    aggregate_rate_post: float
    aggregate_direction: str
    breakdowns: list[SourceBreakdown]
    explanation: str


# ── Pure Functions ───────────────────────────────────────────────────


def build_cohort_matrix(
    cohort_outcomes: list[dict],
    cohort_sizes: list[dict],
    reference_date: datetime | None = None,
    max_periods: int = 12,
) -> CohortMatrix | None:
    """Build triangular cohort matrix from DB rows.

    cohort_outcomes: rows with cohort_month, win_month, wins, revenue
    cohort_sizes: rows with cohort_month, cohort_size
    """
    if not cohort_sizes:
        return None

    ref = reference_date or datetime.now(timezone.utc)

    # Index sizes by cohort month
    sizes_by_month = {}
    for r in cohort_sizes:
        key = r["cohort_month"]
        sizes_by_month[key] = int(r["cohort_size"])

    # Index wins by (cohort_month, offset_months)
    wins_by_cohort_offset = defaultdict(int)
    for r in cohort_outcomes:
        cm = r["cohort_month"]
        wm = r["win_month"]
        if cm is None or wm is None:
            continue
        offset = (wm.year - cm.year) * 12 + (wm.month - cm.month)
        if 0 <= offset < max_periods:
            wins_by_cohort_offset[(cm, offset)] += int(r["wins"])

    # Sort cohort months
    sorted_months = sorted(sizes_by_month.keys())
    if not sorted_months:
        return None

    cohort_labels = [m.strftime("%b %Y") for m in sorted_months]
    period_offsets = list(range(max_periods))

    # Build matrix: cumulative conversion rate at each offset
    conversion_rates = []
    cohort_sizes_list = []
    max_offset_per_cohort = []

    for cm in sorted_months:
        size = sizes_by_month[cm]
        cohort_sizes_list.append(size)

        # How many full months have elapsed since this cohort started
        months_elapsed = (ref.year - cm.year) * 12 + (ref.month - cm.month)
        max_offset_per_cohort.append(min(months_elapsed, max_periods))

        row = []
        cum_wins = 0
        for offset in range(max_periods):
            if offset > months_elapsed:
                row.append(None)  # hasn't elapsed yet
            else:
                cum_wins += wins_by_cohort_offset.get((cm, offset), 0)
                rate = cum_wins / size if size > 0 else 0.0
                row.append(round(rate, 4))
        conversion_rates.append(row)

    return CohortMatrix(
        cohort_labels=cohort_labels,
        period_offsets=period_offsets,
        conversion_rates=conversion_rates,
        cohort_sizes=cohort_sizes_list,
        max_offset_per_cohort=max_offset_per_cohort,
    )


def detect_simpsons_paradox(
    rates_by_source: list[dict],
    intervention_date: datetime,
) -> SimpsonCheck | None:
    """Check whether aggregate trend reverses at source level.

    rates_by_source: rows with cohort_month, source, total, wins
    """
    if not rates_by_source:
        return None

    # Split pre/post
    pre_totals = defaultdict(lambda: {"wins": 0, "total": 0})
    post_totals = defaultdict(lambda: {"wins": 0, "total": 0})
    agg_pre = {"wins": 0, "total": 0}
    agg_post = {"wins": 0, "total": 0}

    for r in rates_by_source:
        cm = r["cohort_month"]
        src = r["source"]
        wins = int(r["wins"])
        total = int(r["total"])

        if cm < intervention_date:
            pre_totals[src]["wins"] += wins
            pre_totals[src]["total"] += total
            agg_pre["wins"] += wins
            agg_pre["total"] += total
        else:
            post_totals[src]["wins"] += wins
            post_totals[src]["total"] += total
            agg_post["wins"] += wins
            agg_post["total"] += total

    if agg_pre["total"] == 0 or agg_post["total"] == 0:
        return None

    agg_rate_pre = agg_pre["wins"] / agg_pre["total"]
    agg_rate_post = agg_post["wins"] / agg_post["total"]
    agg_direction = "up" if agg_rate_post > agg_rate_pre else "down"

    # Per-source breakdowns
    all_sources = set(pre_totals.keys()) | set(post_totals.keys())
    breakdowns = []
    contrary_count = 0

    for src in sorted(all_sources):
        pre = pre_totals[src]
        post = post_totals[src]
        if pre["total"] < 10 or post["total"] < 10:
            continue  # skip sources with too few observations
        rate_pre = pre["wins"] / pre["total"]
        rate_post = post["wins"] / post["total"]
        direction = "up" if rate_post > rate_pre else "down"

        if direction != agg_direction:
            contrary_count += 1

        breakdowns.append(SourceBreakdown(
            source=src,
            rate_pre=rate_pre,
            rate_post=rate_post,
            n_pre=pre["total"],
            n_post=post["total"],
            direction=direction,
        ))

    if not breakdowns:
        return None

    # Paradox: aggregate goes one way but majority of sources go the other
    has_paradox = contrary_count > len(breakdowns) / 2

    if has_paradox:
        explanation = (
            f"Simpson's Paradox detected: the aggregate win rate went {agg_direction} "
            f"({agg_rate_pre:.1%} → {agg_rate_post:.1%}), but {contrary_count} of "
            f"{len(breakdowns)} lead sources moved in the opposite direction. "
            f"The aggregate shift is driven by a change in source mix, not by a real "
            f"improvement or decline within individual sources."
        )
    else:
        explanation = (
            f"No Simpson's Paradox detected. The aggregate win rate went {agg_direction} "
            f"({agg_rate_pre:.1%} → {agg_rate_post:.1%}), consistent with the majority "
            f"of individual lead sources."
        )

    return SimpsonCheck(
        has_paradox=has_paradox,
        aggregate_rate_pre=agg_rate_pre,
        aggregate_rate_post=agg_rate_post,
        aggregate_direction=agg_direction,
        breakdowns=breakdowns,
        explanation=explanation,
    )


# ── Async wrappers ───────────────────────────────────────────────────


async def run_cohort_analysis(
    conn,
    tenant_id: str,
    lookback_months: int = 24,
) -> CohortMatrix | None:
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_months * 30)
    outcomes = await conn.fetch(Q.COHORT_OUTCOMES, tenant_id, cutoff)
    sizes = await conn.fetch(Q.COHORT_SIZES, tenant_id, cutoff)
    return build_cohort_matrix(
        [dict(r) for r in outcomes],
        [dict(r) for r in sizes],
    )


async def run_simpsons_check(
    conn,
    tenant_id: str,
    lookback_months: int = 24,
) -> SimpsonCheck | None:
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_months * 30)
    rows = await conn.fetch(Q.COHORT_RATES_BY_SOURCE, tenant_id, cutoff)

    # Get intervention date from baseline
    baseline = await conn.fetchrow(Q.ACTIVE_BASELINE, tenant_id)
    if baseline and baseline["period_end"]:
        intervention_date = baseline["period_end"]
    else:
        # No baseline — split at midpoint
        if rows:
            dates = sorted(set(r["cohort_month"] for r in rows))
            intervention_date = dates[len(dates) // 2]
        else:
            return None

    return detect_simpsons_paradox(
        [dict(r) for r in rows],
        intervention_date,
    )
