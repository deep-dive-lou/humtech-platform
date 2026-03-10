"""
Optimisation Engine — ANOVA Decomposition Library

Pure math functions for Analysis of Variance and Taguchi factor analysis.
No web dependencies. Computes factor contributions, main effects, and
signal-to-noise ratios for multivariate test results.

References:
- Taguchi, G. (1986). Introduction to Quality Engineering. Asian Productivity Organization.
- Jiang et al. (2018). A Comparison of the Taguchi Method and Evolutionary Optimization
  in Multivariate Testing. arXiv:1808.08347.
"""

from __future__ import annotations

from typing import Any

import numpy as np
from scipy.stats import f as f_dist


# ---------------------------------------------------------------------------
# One-way ANOVA
# ---------------------------------------------------------------------------

def one_way_anova(
    groups: list[list[float]],
) -> dict[str, float]:
    """Perform one-way Analysis of Variance.

    Tests whether the means of two or more groups differ significantly.

    Args:
        groups: List of groups, each a list of observed values.
                E.g. [[0.12, 0.15, 0.11], [0.20, 0.22, 0.19]] for two variants.

    Returns:
        Dict with keys:
            ss_between: Sum of squares between groups
            ss_within:  Sum of squares within groups
            ss_total:   Total sum of squares
            df_between: Degrees of freedom between groups (k - 1)
            df_within:  Degrees of freedom within groups (N - k)
            ms_between: Mean square between groups
            ms_within:  Mean square within groups
            f_statistic: F = MS_between / MS_within
            p_value:    P-value from F-distribution
            eta_squared: Effect size (SS_between / SS_total)

    Raises:
        ValueError: If fewer than 2 groups or any group is empty.
    """
    if len(groups) < 2:
        raise ValueError("ANOVA requires at least 2 groups")
    for i, g in enumerate(groups):
        if len(g) == 0:
            raise ValueError(f"Group {i} is empty")

    k = len(groups)
    all_values = np.concatenate([np.array(g, dtype=np.float64) for g in groups])
    n_total = len(all_values)
    grand_mean = np.mean(all_values)

    # Sum of squares between groups
    ss_between = 0.0
    for g in groups:
        arr = np.array(g, dtype=np.float64)
        ss_between += len(arr) * (np.mean(arr) - grand_mean) ** 2

    # Sum of squares within groups
    ss_within = 0.0
    for g in groups:
        arr = np.array(g, dtype=np.float64)
        ss_within += np.sum((arr - np.mean(arr)) ** 2)

    ss_total = float(np.sum((all_values - grand_mean) ** 2))

    df_between = k - 1
    df_within = n_total - k

    if df_within == 0:
        raise ValueError(
            "Insufficient data: need more observations than groups "
            "for within-group degrees of freedom"
        )

    ms_between = float(ss_between) / df_between
    ms_within = float(ss_within) / df_within

    f_statistic = ms_between / ms_within if ms_within > 0 else float("inf")
    p_value = float(1.0 - f_dist.cdf(f_statistic, df_between, df_within))

    eta_sq = float(ss_between) / ss_total if ss_total > 0 else 0.0

    return {
        "ss_between": float(ss_between),
        "ss_within": float(ss_within),
        "ss_total": ss_total,
        "df_between": df_between,
        "df_within": df_within,
        "ms_between": ms_between,
        "ms_within": ms_within,
        "f_statistic": f_statistic,
        "p_value": p_value,
        "eta_squared": eta_sq,
    }


# ---------------------------------------------------------------------------
# Factor contributions (Taguchi percent contribution)
# ---------------------------------------------------------------------------

def factor_contributions(
    factors: list[dict[str, Any]],
    observations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Compute each factor's percent contribution to total variance.

    Uses ANOVA decomposition on Taguchi results to rank which factors
    have the most influence on the response variable.

    Args:
        factors: List of factor definitions, each with:
            - "factor_id": str
            - "name": str
            - "levels": list[dict] with "level_id" keys
        observations: List of observation dicts, each with:
            - "factor_values": dict mapping factor_id -> level_id
            - "response": float (e.g. conversion rate for that run)

    Returns:
        List of dicts sorted by contribution (descending), each with:
            - "factor_id": str
            - "name": str
            - "ss": float — sum of squares for this factor
            - "contribution_pct": float — percent of total SS
            - "f_statistic": float
            - "p_value": float

    Example:
        If headline accounts for 34% of variance and CTA for 12%, the list
        will show headline first with contribution_pct=34.0.
    """
    if not observations:
        raise ValueError("No observations provided")

    # Grand mean of all responses
    responses = np.array([o["response"] for o in observations], dtype=np.float64)
    grand_mean = float(np.mean(responses))
    ss_total = float(np.sum((responses - grand_mean) ** 2))

    results = []

    for factor in factors:
        fid = factor["factor_id"]

        # Group responses by this factor's level
        level_groups: dict[str, list[float]] = {}
        for obs in observations:
            level_id = obs["factor_values"][fid]
            level_groups.setdefault(level_id, []).append(obs["response"])

        groups = list(level_groups.values())

        if len(groups) < 2:
            results.append({
                "factor_id": fid,
                "name": factor["name"],
                "ss": 0.0,
                "contribution_pct": 0.0,
                "f_statistic": 0.0,
                "p_value": 1.0,
            })
            continue

        anova = one_way_anova(groups)

        results.append({
            "factor_id": fid,
            "name": factor["name"],
            "ss": anova["ss_between"],
            "contribution_pct": (anova["ss_between"] / ss_total * 100.0) if ss_total > 0 else 0.0,
            "f_statistic": anova["f_statistic"],
            "p_value": anova["p_value"],
        })

    # Sort by contribution descending
    results.sort(key=lambda r: r["contribution_pct"], reverse=True)
    return results


# ---------------------------------------------------------------------------
# Main effects — mean response at each level of each factor
# ---------------------------------------------------------------------------

def main_effects(
    factors: list[dict[str, Any]],
    observations: list[dict[str, Any]],
) -> dict[str, dict[str, float]]:
    """Compute main effects: mean response at each level of each factor.

    Used to identify the best level for each factor independently.

    Args:
        factors: Factor definitions (same format as factor_contributions).
        observations: Observation dicts with factor_values and response.

    Returns:
        Nested dict: {factor_id: {level_id: mean_response, ...}, ...}

    Example:
        >>> effects = main_effects(factors, observations)
        >>> effects["headline"]["urgency"]
        0.142  # mean conversion rate when headline=urgency
    """
    result: dict[str, dict[str, float]] = {}

    for factor in factors:
        fid = factor["factor_id"]
        level_responses: dict[str, list[float]] = {}

        for obs in observations:
            level_id = obs["factor_values"][fid]
            level_responses.setdefault(level_id, []).append(obs["response"])

        result[fid] = {
            level_id: float(np.mean(values))
            for level_id, values in level_responses.items()
        }

    return result


# ---------------------------------------------------------------------------
# Optimal combination — best level per factor from main effects
# ---------------------------------------------------------------------------

def optimal_combination(
    factors: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    mode: str = "larger_is_better",
) -> dict[str, dict[str, Any]]:
    """Identify the optimal level for each factor based on main effects.

    Args:
        factors: Factor definitions.
        observations: Observation dicts with factor_values and response.
        mode: "larger_is_better" or "smaller_is_better".

    Returns:
        Dict mapping factor_id -> {"level_id": str, "mean_response": float}
    """
    effects = main_effects(factors, observations)
    result = {}

    for factor in factors:
        fid = factor["factor_id"]
        level_means = effects[fid]
        if mode == "larger_is_better":
            best_level = max(level_means, key=level_means.get)
        else:
            best_level = min(level_means, key=level_means.get)
        result[fid] = {
            "level_id": best_level,
            "mean_response": level_means[best_level],
        }

    return result


# ---------------------------------------------------------------------------
# Taguchi Signal-to-Noise Ratio
# ---------------------------------------------------------------------------

def taguchi_snr(
    values: list[float],
    mode: str = "larger_is_better",
) -> float:
    """Compute Taguchi signal-to-noise ratio for a set of observations.

    Args:
        values: List of observed response values for one experimental run.
        mode: One of:
            - "larger_is_better":  S/N = -10 * log10(mean(1/y^2))
              Used for conversion rates, revenue, click-through rates.
            - "smaller_is_better": S/N = -10 * log10(mean(y^2))
              Used for bounce rates, cost per acquisition, error rates.
            - "nominal_is_best":   S/N = 10 * log10(mean^2 / variance)
              Used when targeting a specific value.

    Returns:
        Signal-to-noise ratio in decibels.

    Raises:
        ValueError: If values is empty or mode is invalid.
    """
    if not values:
        raise ValueError("values must be non-empty")

    arr = np.array(values, dtype=np.float64)

    if mode == "larger_is_better":
        # Guard against zero values
        arr_safe = np.where(arr == 0, 1e-10, arr)
        snr = -10.0 * np.log10(np.mean(1.0 / arr_safe**2))
    elif mode == "smaller_is_better":
        snr = -10.0 * np.log10(np.mean(arr**2))
    elif mode == "nominal_is_best":
        mean_val = np.mean(arr)
        var_val = np.var(arr, ddof=1)
        if var_val == 0:
            return float("inf")  # Perfect consistency
        snr = 10.0 * np.log10(mean_val**2 / var_val)
    else:
        raise ValueError(f"Invalid mode '{mode}'. Use 'larger_is_better', 'smaller_is_better', or 'nominal_is_best'")

    return float(snr)
