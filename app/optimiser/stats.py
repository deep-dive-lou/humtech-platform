"""
Optimisation Engine — Core Statistics Library

Pure math functions for Bayesian multivariate testing.
No web dependencies. Uses scipy.stats for Beta distribution calculations.

References:
- Thompson, W.R. (1933). On the Likelihood that One Unknown Probability Exceeds Another.
- Miikkulainen et al. (2020). Ascend by Evolv: AI-Based Massively Multivariate CRO.
- Qiu & Miikkulainen (2019). Enhancing Evolutionary CRO via Multi-Armed Bandit Algorithms.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import numpy as np
from scipy.stats import beta as beta_dist


# ---------------------------------------------------------------------------
# Beta posterior update (conjugate prior for Bernoulli likelihood)
# ---------------------------------------------------------------------------

def beta_posterior(
    alpha_prior: float,
    beta_prior: float,
    successes: int,
    failures: int,
) -> tuple[float, float]:
    """Return (alpha_post, beta_post) after observing successes and failures.

    Beta-Binomial conjugate update:
        alpha_post = alpha_prior + successes
        beta_post  = beta_prior + failures
    """
    return (alpha_prior + successes, beta_prior + failures)


# ---------------------------------------------------------------------------
# Credible interval
# ---------------------------------------------------------------------------

def credible_interval(
    alpha: float,
    beta: float,
    ci: float = 0.95,
) -> tuple[float, float]:
    """Return (lower, upper) bounds of the ci% Bayesian credible interval.

    Uses the Beta distribution's percent point function (inverse CDF).
    A 95% credible interval means: given the observed data, there is a 95%
    probability the true conversion rate lies within this range.
    """
    tail = (1.0 - ci) / 2.0
    lower = float(beta_dist.ppf(tail, alpha, beta))
    upper = float(beta_dist.ppf(1.0 - tail, alpha, beta))
    return (lower, upper)


# ---------------------------------------------------------------------------
# P(best) — probability each variant is the true best
# ---------------------------------------------------------------------------

def p_best(
    variants: list[dict[str, Any]],
    n_samples: int = 50_000,
) -> dict[str, float]:
    """Compute P(best) for each variant via Monte Carlo sampling.

    Args:
        variants: [{"variant_id": str, "alpha": float, "beta": float}, ...]
        n_samples: number of Monte Carlo draws (higher = more precise)

    Returns:
        {"variant_id": probability_of_being_best, ...}

    Each variant's posterior is Beta(alpha, beta). We draw n_samples from each,
    then count how often each variant has the highest draw.
    """
    rng = np.random.default_rng()
    ids = [v["variant_id"] for v in variants]
    samples = np.column_stack([
        rng.beta(v["alpha"], v["beta"], size=n_samples)
        for v in variants
    ])
    # For each sample, which column (variant) has the max?
    winners = np.argmax(samples, axis=1)
    counts = np.bincount(winners, minlength=len(variants))
    return {vid: float(counts[i] / n_samples) for i, vid in enumerate(ids)}


# ---------------------------------------------------------------------------
# Expected loss — E[max(others) - this | choosing this]
# ---------------------------------------------------------------------------

def expected_loss(
    variants: list[dict[str, Any]],
    n_samples: int = 50_000,
) -> dict[str, float]:
    """Compute expected loss for each variant.

    Expected loss answers: "if I pick this variant but it's not actually the best,
    how much conversion rate am I leaving on the table on average?"

    A dominant variant will have expected loss approaching 0.
    """
    rng = np.random.default_rng()
    ids = [v["variant_id"] for v in variants]
    samples = np.column_stack([
        rng.beta(v["alpha"], v["beta"], size=n_samples)
        for v in variants
    ])
    max_per_row = np.max(samples, axis=1)
    result = {}
    for i, vid in enumerate(ids):
        loss = np.mean(max_per_row - samples[:, i])
        result[vid] = float(loss)
    return result


# ---------------------------------------------------------------------------
# Winner check — evaluate all declaration rules
# ---------------------------------------------------------------------------

def check_winner(
    variants: list[dict[str, Any]],
    rules: dict[str, Any],
    experiment_started_at: datetime,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    """Check if any variant meets all winner declaration rules.

    Args:
        variants: [{"variant_id", "alpha", "beta", "impressions"}, ...]
        rules: {"p_best_threshold", "expected_loss_threshold",
                "min_impressions", "min_days"}
        experiment_started_at: when the experiment was started
        now: current time (defaults to utcnow, injectable for testing)

    Returns:
        The winning variant dict with added "p_best" and "expected_loss" keys,
        or None if no variant meets all rules.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    # Rule: minimum days elapsed
    days_running = (now - experiment_started_at).total_seconds() / 86400
    if days_running < rules.get("min_days", 7):
        return None

    # Rule: minimum impressions per variant
    min_imp = rules.get("min_impressions", 100)
    for v in variants:
        if v.get("impressions", 0) < min_imp:
            return None

    # Compute P(best) and expected loss
    pb = p_best(variants)
    el = expected_loss(variants)

    p_threshold = rules.get("p_best_threshold", 0.95)
    el_threshold = rules.get("expected_loss_threshold", 0.01)

    # Find a variant that passes both thresholds
    for v in variants:
        vid = v["variant_id"]
        if pb[vid] >= p_threshold and el[vid] <= el_threshold:
            return {
                **v,
                "p_best": pb[vid],
                "expected_loss": el[vid],
            }

    return None


# ---------------------------------------------------------------------------
# Thompson Sampling — allocate one visitor to a variant
# ---------------------------------------------------------------------------

def thompson_allocate(
    variants: list[dict[str, Any]],
) -> str:
    """Draw from each variant's Beta posterior, return the variant_id with
    the highest draw.

    This is Thompson Sampling: the probability of selecting each variant
    equals its P(best), providing an optimal exploration-exploitation trade-off.

    Args:
        variants: [{"variant_id": str, "alpha": float, "beta": float}, ...]

    Returns:
        variant_id of the selected variant
    """
    rng = np.random.default_rng()
    best_id = None
    best_draw = -1.0
    for v in variants:
        draw = rng.beta(v["alpha"], v["beta"])
        if draw > best_draw:
            best_draw = draw
            best_id = v["variant_id"]
    return best_id


# ---------------------------------------------------------------------------
# Combined Bayesian + Sequential winner check
# ---------------------------------------------------------------------------

def check_winner_sequential(
    variants: list[dict[str, Any]],
    rules: dict[str, Any],
    experiment_started_at: datetime,
    now: datetime | None = None,
    alpha: float = 0.05,
    tau: float = 0.03,
) -> dict[str, Any] | None:
    """Enhanced winner check requiring both Bayesian and sequential agreement.

    Winner is declared only when BOTH methods agree on the same variant:
    1. Bayesian: P(best) > threshold AND expected_loss < threshold
    2. Sequential: mSPRT rejects null for that variant vs control

    This prevents false positives from repeated checking (sequential handles it)
    while also ensuring practical significance (Bayesian expected loss).

    Args:
        variants: [{"variant_id", "alpha", "beta", "impressions",
                     "conversions", "is_control"}, ...]
        rules: {"p_best_threshold", "expected_loss_threshold",
                "min_impressions", "min_days"}
        experiment_started_at: when the experiment was started
        now: current time (defaults to utcnow)
        alpha: sequential test significance level
        tau: mSPRT mixing parameter

    Returns:
        Winning variant dict or None.
    """
    from . import sequential

    # First check Bayesian winner (includes min_days / min_impressions guards)
    bayesian_winner = check_winner(variants, rules, experiment_started_at, now)
    if bayesian_winner is None:
        return None

    # Now verify with sequential test
    seq_status = sequential.sequential_status(
        [{"variant_id": v["variant_id"],
          "impressions": v.get("impressions", 0),
          "conversions": v.get("conversions", 0),
          "is_control": v.get("is_control", False)}
         for v in variants],
        alpha=alpha,
        tau=tau,
    )

    winner_vid = bayesian_winner["variant_id"]
    vr = seq_status["variant_results"].get(winner_vid, {})

    if vr.get("sprt_decision") == "winner":
        bayesian_winner["sequential_confirmed"] = True
        return bayesian_winner

    return None
