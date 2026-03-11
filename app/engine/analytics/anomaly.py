"""
Analytics — Anomaly Detection

P-chart control limits, Western Electric rules, and CUSUM.
All pure functions — no DB access.
"""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass
class PChartResult:
    center: float
    ucl: float          # +3 sigma
    lcl: float          # -3 sigma
    sigma1_upper: float # +1 sigma
    sigma1_lower: float # -1 sigma
    sigma2_upper: float # +2 sigma
    sigma2_lower: float # -2 sigma
    sigma: float        # one standard deviation


def p_chart(values: list[float], ns: list[int] | None = None) -> PChartResult:
    """Compute P-chart control limits.

    Args:
        values: observed proportions per period
        ns: sample sizes per period (used for variable-width limits).
            If None, uses mean-based sigma estimate.

    Returns PChartResult with center line and sigma zones.
    """
    if not values:
        return PChartResult(0, 0, 0, 0, 0, 0, 0, 0)

    p_bar = sum(values) / len(values)

    if ns and len(ns) == len(values):
        n_bar = sum(ns) / len(ns)
    else:
        n_bar = 100  # fallback — proportions only

    if n_bar <= 0 or p_bar <= 0 or p_bar >= 1:
        return PChartResult(p_bar, p_bar, p_bar, p_bar, p_bar, p_bar, p_bar, 0)

    sigma = math.sqrt(p_bar * (1 - p_bar) / n_bar)

    return PChartResult(
        center=p_bar,
        ucl=min(1.0, p_bar + 3 * sigma),
        lcl=max(0.0, p_bar - 3 * sigma),
        sigma1_upper=min(1.0, p_bar + sigma),
        sigma1_lower=max(0.0, p_bar - sigma),
        sigma2_upper=min(1.0, p_bar + 2 * sigma),
        sigma2_lower=max(0.0, p_bar - 2 * sigma),
        sigma=sigma,
    )


@dataclass
class WEViolation:
    index: int
    rule: int
    description: str


def western_electric_rules(
    values: list[float], center: float, sigma: float
) -> list[WEViolation]:
    """Apply Western Electric rules for special-cause detection.

    Rules:
        1. One point beyond 3-sigma
        2. Two of three consecutive points beyond 2-sigma (same side)
        3. Four of five consecutive points beyond 1-sigma (same side)
        4. Eight consecutive points on the same side of center
    """
    if sigma <= 0 or not values:
        return []

    violations: list[WEViolation] = []
    n = len(values)

    for i in range(n):
        dev = values[i] - center

        # Rule 1: beyond 3-sigma
        if abs(dev) > 3 * sigma:
            violations.append(WEViolation(i, 1, "Point beyond 3-sigma"))

        # Rule 2: 2 of 3 beyond 2-sigma (same side)
        if i >= 2:
            window = [values[j] - center for j in range(i - 2, i + 1)]
            above_2s = sum(1 for d in window if d > 2 * sigma)
            below_2s = sum(1 for d in window if d < -2 * sigma)
            if above_2s >= 2:
                violations.append(WEViolation(i, 2, "2 of 3 points above 2-sigma"))
            if below_2s >= 2:
                violations.append(WEViolation(i, 2, "2 of 3 points below 2-sigma"))

        # Rule 3: 4 of 5 beyond 1-sigma (same side)
        if i >= 4:
            window = [values[j] - center for j in range(i - 4, i + 1)]
            above_1s = sum(1 for d in window if d > sigma)
            below_1s = sum(1 for d in window if d < -sigma)
            if above_1s >= 4:
                violations.append(WEViolation(i, 3, "4 of 5 points above 1-sigma"))
            if below_1s >= 4:
                violations.append(WEViolation(i, 3, "4 of 5 points below 1-sigma"))

        # Rule 4: 8 consecutive on same side
        if i >= 7:
            window = [values[j] - center for j in range(i - 7, i + 1)]
            if all(d > 0 for d in window):
                violations.append(WEViolation(i, 4, "8 consecutive above center"))
            if all(d < 0 for d in window):
                violations.append(WEViolation(i, 4, "8 consecutive below center"))

    # Deduplicate — keep only the first violation per (index, rule)
    seen = set()
    unique = []
    for v in violations:
        key = (v.index, v.rule)
        if key not in seen:
            seen.add(key)
            unique.append(v)

    return unique


@dataclass
class CUSUMResult:
    cusum_plus: list[float]
    cusum_minus: list[float]
    signals: list[int]  # indices where CUSUM signals


def cusum(
    values: list[float],
    target: float | None = None,
    k: float = 0.5,
    h: float = 5.0,
) -> CUSUMResult:
    """Tabular CUSUM for detecting mean shifts.

    Args:
        values: observed values per period
        target: target/expected value (defaults to mean of values)
        k: allowance parameter (in units of sigma). Default 0.5 = detect 1-sigma shift.
        h: decision threshold (in units of sigma). Default 5.0.

    Returns CUSUMResult with cumulative sums and signal indices.
    """
    if not values:
        return CUSUMResult([], [], [])

    if target is None:
        target = sum(values) / len(values)

    # Estimate sigma from data
    diffs = [abs(values[i] - values[i - 1]) for i in range(1, len(values))]
    sigma = (sum(diffs) / len(diffs)) / 1.128 if diffs else 1.0  # d2 for n=2

    if sigma <= 0:
        sigma = 1e-10

    K = k * sigma
    H = h * sigma

    cp: list[float] = []
    cm: list[float] = []
    signals: list[int] = []

    s_plus = 0.0
    s_minus = 0.0

    for i, val in enumerate(values):
        s_plus = max(0.0, s_plus + (val - target) - K)
        s_minus = max(0.0, s_minus - (val - target) - K)
        cp.append(s_plus)
        cm.append(s_minus)
        if s_plus > H or s_minus > H:
            signals.append(i)

    return CUSUMResult(cusum_plus=cp, cusum_minus=cm, signals=signals)
