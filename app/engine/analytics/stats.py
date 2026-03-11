"""
Analytics — Statistical Functions

Confidence intervals, hypothesis tests, and utility functions.
All pure functions — no DB access, no side effects.
"""

from __future__ import annotations

import math
from scipy import stats as sp_stats


def wilson_ci(successes: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Wilson score interval for a proportion.

    Stable for small n and proportions near 0 or 1.
    Returns (lower, upper) as proportions in [0, 1].
    """
    if n == 0:
        return (0.0, 0.0)
    p = successes / n
    z2 = z * z
    denom = 1 + z2 / n
    centre = p + z2 / (2 * n)
    margin = z * math.sqrt(p * (1 - p) / n + z2 / (4 * n * n))
    lower = max(0.0, (centre - margin) / denom)
    upper = min(1.0, (centre + margin) / denom)
    return (lower, upper)


def beta_binomial_ci(
    successes: int,
    failures: int,
    alpha_prior: float = 1.0,
    beta_prior: float = 1.0,
    ci: float = 0.95,
) -> tuple[float, float]:
    """Bayesian Beta-Binomial credible interval.

    Uses Beta(alpha_prior + successes, beta_prior + failures) posterior.
    Returns (lower, upper) as proportions in [0, 1].
    """
    a = alpha_prior + successes
    b = beta_prior + failures
    tail = (1 - ci) / 2
    lower = float(sp_stats.beta.ppf(tail, a, b))
    upper = float(sp_stats.beta.ppf(1 - tail, a, b))
    return (lower, upper)


def choose_ci(
    successes: int, n: int, ci: float = 0.95
) -> tuple[float, float, str]:
    """Choose the best CI method based on sample size.

    Wilson if n >= 30, Beta-Binomial otherwise.
    Returns (lower, upper, method_name).
    """
    if n == 0:
        return (0.0, 0.0, "none")
    z = sp_stats.norm.ppf(1 - (1 - ci) / 2)
    if n >= 30:
        lower, upper = wilson_ci(successes, n, z=z)
        return (lower, upper, "wilson")
    else:
        failures = n - successes
        lower, upper = beta_binomial_ci(successes, failures, ci=ci)
        return (lower, upper, "beta_binomial")


def two_proportion_z_test(
    p1: float, n1: int, p2: float, n2: int
) -> tuple[float, float]:
    """Two-proportion z-test.

    Tests whether two proportions are significantly different.
    Returns (z_statistic, p_value).
    """
    if n1 == 0 or n2 == 0:
        return (0.0, 1.0)
    x1, x2 = round(p1 * n1), round(p2 * n2)
    p_hat = (x1 + x2) / (n1 + n2)
    if p_hat == 0 or p_hat == 1:
        return (0.0, 1.0)
    se = math.sqrt(p_hat * (1 - p_hat) * (1 / n1 + 1 / n2))
    if se == 0:
        return (0.0, 1.0)
    z = (p1 - p2) / se
    p_value = 2 * (1 - sp_stats.norm.cdf(abs(z)))
    return (z, p_value)


def format_ci(point: float, lower: float, upper: float, as_pct: bool = True) -> str:
    """Format a point estimate with CI for display."""
    if as_pct:
        return f"{point * 100:.1f}% ({lower * 100:.1f}–{upper * 100:.1f}%)"
    return f"{point:.2f} ({lower:.2f}–{upper:.2f})"
