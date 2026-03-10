"""
Optimisation Engine — Sequential Statistics Library

Pure math functions for always-valid inference in A/B testing.
No web dependencies. Provides confidence sequences and sequential
probability ratio tests valid at any stopping time.

Unlike fixed-sample confidence intervals, these methods let you check
results after every observation without inflating false positive rates.

References:
- Johari et al. (2015). Always Valid Inference: Continuous Monitoring
  of A/B Tests. arXiv:1512.04922.
- Howard et al. (2021). Time-uniform, nonparametric, nonasymptotic
  confidence sequences. Annals of Statistics.
- Wald, A. (1945). Sequential Tests of Statistical Hypotheses.
  Annals of Mathematical Statistics.
"""

from __future__ import annotations

import math
from typing import Any


# ---------------------------------------------------------------------------
# Confidence Sequence for a Bernoulli proportion
# ---------------------------------------------------------------------------

def confidence_sequence(
    successes: int,
    trials: int,
    alpha: float = 0.05,
) -> tuple[float, float]:
    """Anytime-valid confidence interval for a conversion rate.

    Uses the normal mixture confidence sequence with a stitching boundary
    (Howard et al., 2021). For n >= 1:

        CI = p_hat +/- sqrt( (2 * V_hat / n) * log( sqrt(n+1) / alpha ) )

    where V_hat = p_hat * (1 - p_hat) is the plug-in variance.

    Args:
        successes: Number of conversions observed so far.
        trials: Number of impressions observed so far.
        alpha: Significance level (default 0.05 for 95% intervals).

    Returns:
        (lower, upper) bounds for the true conversion rate.
        Guaranteed: for all stopping times T, P(p in CI_T) >= 1 - alpha.
    """
    if trials < 1:
        return (0.0, 1.0)

    p_hat = successes / trials
    # Plug-in variance with floor to avoid zero-width at extremes
    v_hat = p_hat * (1 - p_hat) + 1e-10

    # Normal mixture boundary (law of the iterated logarithm style)
    boundary = math.sqrt(
        (2 * v_hat / trials) * math.log(math.sqrt(trials + 1) / alpha)
    )

    lower = max(0.0, p_hat - boundary)
    upper = min(1.0, p_hat + boundary)
    return (lower, upper)


# ---------------------------------------------------------------------------
# Confidence Sequence for the difference between two proportions
# ---------------------------------------------------------------------------

def confidence_sequence_difference(
    successes_treatment: int,
    trials_treatment: int,
    successes_control: int,
    trials_control: int,
    alpha: float = 0.05,
) -> tuple[float, float, float]:
    """Anytime-valid confidence interval for (p_treatment - p_control).

    Args:
        successes_treatment: Conversions in treatment arm.
        trials_treatment: Impressions in treatment arm.
        successes_control: Conversions in control arm.
        trials_control: Impressions in control arm.
        alpha: Significance level.

    Returns:
        (delta_hat, lower, upper) where delta_hat is the observed difference
        and [lower, upper] is the anytime-valid CI for the true difference.
        If lower > 0, treatment is significantly better.
        If upper < 0, control is significantly better.
    """
    if trials_treatment < 1 or trials_control < 1:
        return (0.0, -1.0, 1.0)

    p_t = successes_treatment / trials_treatment
    p_c = successes_control / trials_control
    delta_hat = p_t - p_c

    # Variance of the difference (independent arms)
    v_t = p_t * (1 - p_t) + 1e-10
    v_c = p_c * (1 - p_c) + 1e-10
    v_diff = v_t / trials_treatment + v_c / trials_control

    # Effective sample size for the boundary
    n_eff = 1.0 / (1.0 / trials_treatment + 1.0 / trials_control)

    # Anytime-valid boundary for the difference
    boundary = math.sqrt(
        2 * v_diff * math.log(math.sqrt(n_eff + 1) / alpha)
    )

    lower = delta_hat - boundary
    upper = delta_hat + boundary
    return (delta_hat, lower, upper)


# ---------------------------------------------------------------------------
# Mixture Sequential Probability Ratio Test (mSPRT)
# ---------------------------------------------------------------------------

def msprt(
    successes_treatment: int,
    trials_treatment: int,
    successes_control: int,
    trials_control: int,
    alpha: float = 0.05,
    tau: float = 0.03,
) -> dict[str, Any]:
    """Mixture SPRT comparing treatment to control (Johari et al., 2015).

    Tests H0: p_treatment = p_control against H1: p_treatment != p_control.
    Uses a normal mixing distribution with variance tau^2 over the effect
    size, avoiding the need to specify an exact alternative.

    The test statistic:
        Lambda = sqrt(V / (V + tau^2)) * exp(delta^2 * tau^2 / (2 * V * (V + tau^2)))

    Reject H0 when Lambda >= 1/alpha.

    Args:
        successes_treatment: Conversions in treatment arm.
        trials_treatment: Impressions in treatment arm.
        successes_control: Conversions in control arm.
        trials_control: Impressions in control arm.
        alpha: Type I error rate (default 0.05).
        tau: Prior SD on effect size. Controls sensitivity:
             0.01 = detect only large effects, 0.05 = moderate effects.
             Default 0.03 is calibrated for marketing conversion diffs.

    Returns:
        Dict with: statistic, threshold, reject_null, delta_hat,
        p_treatment, p_control.
    """
    threshold = 1.0 / alpha

    if trials_treatment < 1 or trials_control < 1:
        return {
            "statistic": 0.0,
            "threshold": threshold,
            "reject_null": False,
            "delta_hat": 0.0,
            "p_treatment": 0.0,
            "p_control": 0.0,
        }

    p_t = successes_treatment / trials_treatment
    p_c = successes_control / trials_control
    delta_hat = p_t - p_c

    # Pooled rate for variance estimate
    total_s = successes_treatment + successes_control
    total_n = trials_treatment + trials_control
    p_pooled = total_s / total_n
    p_pooled = max(1e-10, min(1 - 1e-10, p_pooled))

    # Variance of delta_hat under H0 (pooled)
    v = p_pooled * (1 - p_pooled) * (1.0 / trials_treatment + 1.0 / trials_control)

    tau_sq = tau * tau

    # mSPRT statistic (Johari et al., 2015, eq. 3)
    # Lambda = sqrt(V / (V + tau^2)) * exp(delta^2 * tau^2 / (2 * V * (V + tau^2)))
    ratio = v / (v + tau_sq)
    exponent = (delta_hat ** 2 * tau_sq) / (2 * v * (v + tau_sq))

    statistic = math.sqrt(ratio) * math.exp(exponent)

    return {
        "statistic": statistic,
        "threshold": threshold,
        "reject_null": statistic >= threshold,
        "delta_hat": delta_hat,
        "p_treatment": p_t,
        "p_control": p_c,
    }


# ---------------------------------------------------------------------------
# SPRT for all variants vs control
# ---------------------------------------------------------------------------

def sprt_all_variants(
    variants: list[dict[str, Any]],
    alpha: float = 0.05,
    tau: float = 0.03,
) -> dict[str, dict[str, Any]]:
    """Run mSPRT for each non-control variant against the control.

    Args:
        variants: [{"variant_id", "impressions", "conversions", "is_control"}, ...]
        alpha: Per-comparison alpha.
        tau: mSPRT mixing parameter.

    Returns:
        {variant_id: msprt_result_dict} for each non-control variant.
    """
    control = None
    others = []
    for v in variants:
        if v.get("is_control"):
            control = v
        else:
            others.append(v)

    if control is None or not others:
        return {}

    results = {}
    for v in others:
        result = msprt(
            successes_treatment=v["conversions"],
            trials_treatment=v["impressions"],
            successes_control=control["conversions"],
            trials_control=control["impressions"],
            alpha=alpha,
            tau=tau,
        )
        results[v["variant_id"]] = result

    return results


# ---------------------------------------------------------------------------
# Sequential status — overall experiment summary
# ---------------------------------------------------------------------------

def sequential_status(
    variants: list[dict[str, Any]],
    alpha: float = 0.05,
    tau: float = 0.03,
) -> dict[str, Any]:
    """Compute overall sequential testing status for an experiment.

    Combines confidence sequences and mSPRT to produce:
    - Per-variant anytime-valid CIs
    - Per-variant SPRT decision vs control
    - Overall recommendation (continue / winner_found / no_effect)

    Args:
        variants: [{"variant_id", "impressions", "conversions", "is_control"}, ...]
        alpha: Significance level.
        tau: mSPRT mixing parameter.

    Returns:
        Dict with: variant_results, recommendation, winner_id, safe_to_stop.
    """
    # Per-variant confidence sequences
    variant_results: dict[str, dict[str, Any]] = {}
    control = None

    for v in variants:
        vid = v["variant_id"]
        ci_lo, ci_hi = confidence_sequence(v["conversions"], v["impressions"], alpha)
        variant_results[vid] = {
            "ci_lower": ci_lo,
            "ci_upper": ci_hi,
            "rate": v["conversions"] / v["impressions"] if v["impressions"] > 0 else 0.0,
        }
        if v.get("is_control"):
            control = v

    # SPRT for non-control variants
    sprt_results = sprt_all_variants(variants, alpha, tau)
    for vid, result in sprt_results.items():
        variant_results[vid]["sprt_statistic"] = result["statistic"]
        variant_results[vid]["sprt_threshold"] = result["threshold"]
        variant_results[vid]["reject_null"] = result["reject_null"]
        variant_results[vid]["delta_hat"] = result["delta_hat"]

        if result["reject_null"]:
            variant_results[vid]["sprt_decision"] = (
                "winner" if result["delta_hat"] > 0 else "loser"
            )
        else:
            variant_results[vid]["sprt_decision"] = "testing"

    # Control gets no SPRT (it's the reference)
    if control:
        cvid = control["variant_id"]
        if cvid in variant_results:
            variant_results[cvid]["sprt_decision"] = "control"

    # Overall recommendation
    winners = [
        vid for vid, r in variant_results.items()
        if r.get("sprt_decision") == "winner"
    ]
    losers = [
        vid for vid, r in variant_results.items()
        if r.get("sprt_decision") == "loser"
    ]
    non_control_count = len([v for v in variants if not v.get("is_control")])

    if winners:
        # Pick the winner with highest observed rate
        best = max(winners, key=lambda vid: variant_results[vid]["rate"])
        return {
            "variant_results": variant_results,
            "recommendation": "winner_found",
            "winner_id": best,
            "safe_to_stop": True,
        }

    if len(losers) == non_control_count and non_control_count > 0:
        # All treatments are worse than control
        return {
            "variant_results": variant_results,
            "recommendation": "no_effect",
            "winner_id": None,
            "safe_to_stop": True,
        }

    return {
        "variant_results": variant_results,
        "recommendation": "continue",
        "winner_id": None,
        "safe_to_stop": False,
    }
