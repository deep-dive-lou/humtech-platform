"""
Analytics — Causal Attribution

Interrupted Time Series, Bayesian Structural Time Series (CausalImpact),
Doubly Robust estimator, and Bayesian credible intervals on uplift.

All heavy computation is in pure functions (no DB access).
Async wrappers at the bottom fetch data then call pure functions.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
from scipy import stats as sp_stats

try:
    import statsmodels.api as sm
    from statsmodels.tsa.statespace.structural import UnobservedComponents

    HAS_SM = True
except ImportError:
    HAS_SM = False

from . import queries as Q

log = logging.getLogger(__name__)

# ── Dataclasses ──────────────────────────────────────────────────────


@dataclass
class ITSResult:
    """Interrupted Time Series segmented regression results."""

    periods: list[str]
    observed: list[float]
    predicted: list[float]       # fitted line (full length)
    counterfactual: list[float]  # pre-trend projected forward
    intervention_index: int
    level_change: float
    level_change_se: float
    level_change_ci: tuple[float, float]
    level_change_p: float
    slope_change: float
    slope_change_se: float
    slope_change_ci: tuple[float, float]
    slope_change_p: float
    r_squared: float
    narrative: str


@dataclass
class BSTSResult:
    """Bayesian Structural Time Series (CausalImpact) results."""

    periods: list[str]
    observed: list[float]
    predicted: list[float]
    ci_lower: list[float]
    ci_upper: list[float]
    pointwise_effect: list[float]
    cumulative_effect: float
    cumulative_ci_lower: float
    cumulative_ci_upper: float
    relative_effect_pct: float
    prob_causal: float
    intervention_index: int
    narrative: str


@dataclass
class DRResult:
    """Doubly Robust estimator results."""

    ate: float
    ate_se: float
    ate_ci_lower: float
    ate_ci_upper: float
    n_treated: int
    n_control: int
    treated_outcome_mean: float
    control_outcome_mean: float
    propensity_auc: float
    narrative: str


@dataclass
class UpliftSummary:
    """Credible intervals on uplift combining all methods."""

    statements: list[dict]  # [{method, uplift_pct, ci, prob, label}]
    consensus: str


# ── Pure Functions ───────────────────────────────────────────────────


def interrupted_time_series(
    values: list[float],
    periods: list[str],
    intervention_index: int,
    metric_label: str = "metric",
) -> ITSResult | None:
    """Segmented regression: y = b0 + b1*t + b2*D + b3*(D*t) + e

    D = 0 pre-intervention, 1 post.
    (D*t) = 0 pre, 1, 2, 3… post (time since intervention).
    """
    n = len(values)
    if n < 4 or intervention_index < 2 or intervention_index >= n - 1:
        return None

    y = np.array(values, dtype=np.float64)
    t = np.arange(n, dtype=np.float64)
    D = np.where(t >= intervention_index, 1.0, 0.0)
    Dt = np.where(t >= intervention_index, t - intervention_index, 0.0)

    # Design matrix: [intercept, t, D, D*t]
    X = np.column_stack([np.ones(n), t, D, Dt])

    if HAS_SM:
        model = sm.OLS(y, X).fit(cov_type="HC1")
        coefs = model.params
        ses = model.bse
        pvals = model.pvalues
        r2 = model.rsquared
    else:
        # Fallback: numpy OLS
        coefs, residuals, _, _ = np.linalg.lstsq(X, y, rcond=None)
        y_hat = X @ coefs
        resid = y - y_hat
        r2 = 1.0 - np.sum(resid**2) / np.sum((y - y.mean()) ** 2) if np.var(y) > 0 else 0.0
        # HC1 standard errors
        n_obs = len(y)
        leverage = np.diag(X @ np.linalg.inv(X.T @ X) @ X.T)
        hc1_factor = n_obs / (n_obs - X.shape[1])
        S = np.diag(resid**2 * hc1_factor)
        cov = np.linalg.inv(X.T @ X) @ X.T @ S @ X @ np.linalg.inv(X.T @ X)
        ses = np.sqrt(np.diag(cov))
        z = coefs / np.where(ses > 0, ses, 1e-10)
        pvals = 2 * (1 - sp_stats.norm.cdf(np.abs(z)))

    # Fitted values
    predicted = (X @ coefs).tolist()

    # Counterfactual: project pre-trend (D=0 everywhere)
    X_cf = np.column_stack([np.ones(n), t, np.zeros(n), np.zeros(n)])
    counterfactual = (X_cf @ coefs).tolist()

    z95 = 1.96
    level_ci = (coefs[2] - z95 * ses[2], coefs[2] + z95 * ses[2])
    slope_ci = (coefs[3] - z95 * ses[3], coefs[3] + z95 * ses[3])

    # Narrative
    direction = "increase" if coefs[2] > 0 else "decrease"
    sig = "statistically significant" if pvals[2] < 0.05 else "not statistically significant"
    narrative = (
        f"The intervention caused an immediate {direction} of {abs(coefs[2]):.4f} "
        f"in {metric_label} (p={pvals[2]:.3f}, {sig}). "
        f"The monthly trend changed by {coefs[3]:+.4f} per period (p={pvals[3]:.3f}). "
        f"R² = {r2:.3f}."
    )

    return ITSResult(
        periods=periods,
        observed=values,
        predicted=predicted,
        counterfactual=counterfactual,
        intervention_index=intervention_index,
        level_change=float(coefs[2]),
        level_change_se=float(ses[2]),
        level_change_ci=(float(level_ci[0]), float(level_ci[1])),
        level_change_p=float(pvals[2]),
        slope_change=float(coefs[3]),
        slope_change_se=float(ses[3]),
        slope_change_ci=(float(slope_ci[0]), float(slope_ci[1])),
        slope_change_p=float(pvals[3]),
        r_squared=float(r2),
        narrative=narrative,
    )


def causal_impact_bsts(
    values: list[float],
    periods: list[str],
    intervention_index: int,
    metric_label: str = "metric",
    n_samples: int = 1000,
) -> BSTSResult | None:
    """Bayesian Structural Time Series counterfactual.

    Fits local linear trend on pre-period, projects counterfactual into post.
    """
    n = len(values)
    if n < 4 or intervention_index < 2 or intervention_index >= n - 1:
        return None

    y = np.array(values, dtype=np.float64)
    y_pre = y[:intervention_index]
    y_post = y[intervention_index:]
    n_post = len(y_post)

    predicted = np.zeros(n)
    ci_lower = np.zeros(n)
    ci_upper = np.zeros(n)

    try:
        if HAS_SM:
            model = UnobservedComponents(y_pre, level="local linear trend")
            res = model.fit(disp=False, maxiter=200)

            # In-sample fit for pre-period
            predicted[:intervention_index] = res.fittedvalues

            # Forecast post-period (counterfactual)
            forecast = res.get_forecast(steps=n_post)
            predicted[intervention_index:] = forecast.predicted_mean
            fc_ci = forecast.conf_int(alpha=0.05)
            ci_lower[:intervention_index] = res.fittedvalues
            ci_upper[:intervention_index] = res.fittedvalues
            ci_lower[intervention_index:] = fc_ci[:, 0]
            ci_upper[intervention_index:] = fc_ci[:, 1]

            # Estimate forecast variance for posterior simulation
            forecast_var = ((fc_ci[:, 1] - fc_ci[:, 0]) / (2 * 1.96)) ** 2
        else:
            # Fallback: simple linear trend fit on pre-period
            t_pre = np.arange(len(y_pre), dtype=np.float64)
            X_pre = np.column_stack([np.ones(len(y_pre)), t_pre])
            coefs, _, _, _ = np.linalg.lstsq(X_pre, y_pre, rcond=None)
            resid = y_pre - X_pre @ coefs
            sigma2 = np.var(resid, ddof=2) if len(resid) > 2 else np.var(resid)

            t_all = np.arange(n, dtype=np.float64)
            X_all = np.column_stack([np.ones(n), t_all])
            predicted = (X_all @ coefs).tolist()
            predicted = np.array(predicted)

            # CI widens with distance from pre-period
            for i in range(n):
                dist = max(0, i - intervention_index + 1)
                se = np.sqrt(sigma2 * (1 + 1.0 / len(y_pre) + dist * 0.1))
                ci_lower[i] = predicted[i] - 1.96 * se
                ci_upper[i] = predicted[i] + 1.96 * se

            forecast_var = np.array([sigma2 * (1 + 0.1 * (i + 1)) for i in range(n_post)])

    except Exception:
        log.exception("BSTS model fitting failed")
        return None

    # Pointwise effect in post-period
    pointwise = np.zeros(n)
    pointwise[intervention_index:] = y[intervention_index:] - predicted[intervention_index:]

    # Cumulative effect
    cumulative = float(np.sum(pointwise[intervention_index:]))
    counterfactual_sum = float(np.sum(predicted[intervention_index:]))

    # Monte Carlo for cumulative CI and P(causal)
    rng = np.random.default_rng(42)
    cum_samples = []
    for _ in range(n_samples):
        cf_draw = predicted[intervention_index:] + rng.normal(0, np.sqrt(np.maximum(forecast_var, 1e-10)))
        cum_samples.append(float(np.sum(y_post - cf_draw)))
    cum_samples = np.array(cum_samples)
    cum_ci_lo = float(np.percentile(cum_samples, 2.5))
    cum_ci_hi = float(np.percentile(cum_samples, 97.5))
    prob_causal = float(np.mean(cum_samples > 0))

    relative_pct = (cumulative / counterfactual_sum * 100) if abs(counterfactual_sum) > 1e-10 else 0.0

    # Narrative
    prob_label = _prob_label(prob_causal)
    direction = "increased" if cumulative > 0 else "decreased"
    narrative = (
        f"{metric_label.replace('_', ' ').title()} {direction} by a cumulative "
        f"{abs(cumulative):.4f} ({abs(relative_pct):.1f}%) over the post-intervention period. "
        f"Posterior probability of a positive causal effect: {prob_causal:.0%} ({prob_label}). "
        f"95% credible interval: [{cum_ci_lo:.4f}, {cum_ci_hi:.4f}]."
    )

    return BSTSResult(
        periods=periods,
        observed=values,
        predicted=predicted.tolist(),
        ci_lower=ci_lower.tolist(),
        ci_upper=ci_upper.tolist(),
        pointwise_effect=pointwise.tolist(),
        cumulative_effect=cumulative,
        cumulative_ci_lower=cum_ci_lo,
        cumulative_ci_upper=cum_ci_hi,
        relative_effect_pct=relative_pct,
        prob_causal=prob_causal,
        intervention_index=intervention_index,
        narrative=narrative,
    )


def doubly_robust_estimate(
    outcomes: np.ndarray,
    treatments: np.ndarray,
    covariates: np.ndarray,
    n_bootstrap: int = 500,
) -> DRResult | None:
    """Doubly Robust ATE estimator.

    1. Propensity model: P(T=1|X) via logistic regression
    2. Outcome model: E[Y|X,T] via OLS
    3. DR formula for consistency
    4. Bootstrap for CIs
    """
    n = len(outcomes)
    n_treated = int(np.sum(treatments))
    n_control = n - n_treated

    if n_treated < 30 or n_control < 30:
        return None

    # Propensity model
    propensity = _fit_logistic(covariates, treatments)
    propensity = np.clip(propensity, 0.01, 0.99)  # trim extremes

    # Outcome models (separate for treated and control)
    X_with_t = np.column_stack([covariates, treatments])
    X_t1 = np.column_stack([covariates, np.ones(n)])
    X_t0 = np.column_stack([covariates, np.zeros(n)])

    if HAS_SM:
        outcome_model = sm.OLS(outcomes, sm.add_constant(X_with_t)).fit()
        mu1 = outcome_model.predict(sm.add_constant(X_t1))
        mu0 = outcome_model.predict(sm.add_constant(X_t0))
    else:
        X_wc = np.column_stack([np.ones(n), X_with_t])
        coefs, _, _, _ = np.linalg.lstsq(X_wc, outcomes, rcond=None)
        mu1 = np.column_stack([np.ones(n), X_t1]) @ coefs
        mu0 = np.column_stack([np.ones(n), X_t0]) @ coefs

    # DR formula
    dr_scores = _dr_formula(outcomes, treatments, propensity, mu1, mu0)
    ate = float(np.mean(dr_scores))

    # Bootstrap
    rng = np.random.default_rng(42)
    boot_ates = []
    for _ in range(n_bootstrap):
        idx = rng.choice(n, size=n, replace=True)
        p_b = _fit_logistic(covariates[idx], treatments[idx])
        p_b = np.clip(p_b, 0.01, 0.99)

        X_b = np.column_stack([covariates[idx], treatments[idx]])
        X_b1 = np.column_stack([covariates[idx], np.ones(len(idx))])
        X_b0 = np.column_stack([covariates[idx], np.zeros(len(idx))])

        if HAS_SM:
            om_b = sm.OLS(outcomes[idx], sm.add_constant(X_b)).fit()
            m1 = om_b.predict(sm.add_constant(X_b1))
            m0 = om_b.predict(sm.add_constant(X_b0))
        else:
            X_bc = np.column_stack([np.ones(len(idx)), X_b])
            c_b, _, _, _ = np.linalg.lstsq(X_bc, outcomes[idx], rcond=None)
            m1 = np.column_stack([np.ones(len(idx)), X_b1]) @ c_b
            m0 = np.column_stack([np.ones(len(idx)), X_b0]) @ c_b

        dr_b = _dr_formula(outcomes[idx], treatments[idx], p_b, m1, m0)
        boot_ates.append(float(np.mean(dr_b)))

    boot_ates = np.array(boot_ates)
    ate_se = float(np.std(boot_ates, ddof=1))
    ate_ci_lo = float(np.percentile(boot_ates, 2.5))
    ate_ci_hi = float(np.percentile(boot_ates, 97.5))

    # Propensity AUC (concordance)
    auc = _simple_auc(treatments, propensity)

    treated_mean = float(np.mean(outcomes[treatments == 1]))
    control_mean = float(np.mean(outcomes[treatments == 0]))

    # Narrative
    sig = "statistically significant" if ate_ci_lo > 0 or ate_ci_hi < 0 else "not statistically significant"
    auc_note = ""
    if auc < 0.55:
        auc_note = " Warning: propensity model has near-random AUC — treatment assignment may be essentially random."
    elif auc > 0.95:
        auc_note = " Warning: propensity model has very high AUC — treatment is near-deterministic, DR estimates may be unreliable."

    narrative = (
        f"Leads contacted within 24h had a {ate:+.3f} higher conversion probability "
        f"than those contacted later (95% CI: [{ate_ci_lo:.3f}, {ate_ci_hi:.3f}], {sig}). "
        f"Treated: {n_treated:,} leads ({treated_mean:.1%} conversion). "
        f"Control: {n_control:,} leads ({control_mean:.1%} conversion). "
        f"Propensity AUC: {auc:.2f}.{auc_note}"
    )

    return DRResult(
        ate=ate,
        ate_se=ate_se,
        ate_ci_lower=ate_ci_lo,
        ate_ci_upper=ate_ci_hi,
        n_treated=n_treated,
        n_control=n_control,
        treated_outcome_mean=treated_mean,
        control_outcome_mean=control_mean,
        propensity_auc=auc,
        narrative=narrative,
    )


def compute_uplift_summary(
    its: ITSResult | None,
    bsts: BSTSResult | None,
    dr: DRResult | None,
) -> UpliftSummary:
    """Synthesize uplift across all three methods."""
    statements = []

    if its is not None:
        statements.append({
            "method": "Interrupted Time Series",
            "uplift": f"{its.level_change:+.4f}",
            "ci": f"[{its.level_change_ci[0]:.4f}, {its.level_change_ci[1]:.4f}]",
            "p_value": f"{its.level_change_p:.3f}",
            "significant": its.level_change_p < 0.05,
            "label": "level change",
        })

    if bsts is not None:
        statements.append({
            "method": "Bayesian CausalImpact",
            "uplift": f"{bsts.relative_effect_pct:+.1f}%",
            "ci": f"[{bsts.cumulative_ci_lower:.4f}, {bsts.cumulative_ci_upper:.4f}]",
            "prob": f"{bsts.prob_causal:.0%}",
            "significant": bsts.prob_causal > 0.95 or bsts.prob_causal < 0.05,
            "label": _prob_label(bsts.prob_causal),
        })

    if dr is not None:
        sig = dr.ate_ci_lower > 0 or dr.ate_ci_upper < 0
        statements.append({
            "method": "Doubly Robust (lead-level)",
            "uplift": f"{dr.ate:+.3f}",
            "ci": f"[{dr.ate_ci_lower:.3f}, {dr.ate_ci_upper:.3f}]",
            "significant": sig,
            "label": "ATE on conversion probability",
        })

    # Consensus
    if not statements:
        consensus = "Insufficient data for causal analysis."
    else:
        positive_count = sum(
            1 for s in statements
            if s.get("significant") and (
                (s.get("prob") and float(s["prob"].rstrip("%")) / 100 > 0.5)
                or ("+" in s["uplift"])
            )
        )
        if positive_count == len(statements) and len(statements) >= 2:
            prob_str = f" P(positive effect): {bsts.prob_causal:.0%}." if bsts else ""
            consensus = f"All {len(statements)} methods agree on a positive effect.{prob_str}"
        elif positive_count > 0:
            consensus = f"{positive_count} of {len(statements)} methods show a significant positive effect. Evidence is mixed — interpret with caution."
        else:
            consensus = "No method found a statistically significant positive effect."

    return UpliftSummary(statements=statements, consensus=consensus)


# ── Internal helpers ─────────────────────────────────────────────────


def _prob_label(p: float) -> str:
    """IPCC-style probability language."""
    if p > 0.99:
        return "virtually certain"
    if p > 0.95:
        return "very likely"
    if p > 0.90:
        return "likely"
    if p > 0.66:
        return "more likely than not"
    if p > 0.33:
        return "about as likely as not"
    if p > 0.10:
        return "unlikely"
    if p > 0.05:
        return "very unlikely"
    return "exceptionally unlikely"


def _fit_logistic(X: np.ndarray, y: np.ndarray) -> np.ndarray:
    """Fit logistic regression, return predicted probabilities."""
    if HAS_SM:
        try:
            model = sm.Logit(y, sm.add_constant(X)).fit(disp=False, maxiter=100)
            return model.predict(sm.add_constant(X))
        except Exception:
            pass

    # Fallback: scipy minimise
    X_c = np.column_stack([np.ones(len(X)), X])

    def neg_ll(beta):
        z = X_c @ beta
        z = np.clip(z, -500, 500)
        p = 1.0 / (1.0 + np.exp(-z))
        p = np.clip(p, 1e-10, 1 - 1e-10)
        return -np.sum(y * np.log(p) + (1 - y) * np.log(1 - p))

    from scipy.optimize import minimize

    beta0 = np.zeros(X_c.shape[1])
    res = minimize(neg_ll, beta0, method="L-BFGS-B")
    z = X_c @ res.x
    z = np.clip(z, -500, 500)
    return 1.0 / (1.0 + np.exp(-z))


def _dr_formula(
    y: np.ndarray,
    t: np.ndarray,
    e: np.ndarray,
    mu1: np.ndarray,
    mu0: np.ndarray,
) -> np.ndarray:
    """Doubly Robust score per observation."""
    return (
        mu1 - mu0
        + t * (y - mu1) / e
        - (1 - t) * (y - mu0) / (1 - e)
    )


def _simple_auc(labels: np.ndarray, scores: np.ndarray) -> float:
    """Concordance-based AUC (no sklearn needed)."""
    pos = scores[labels == 1]
    neg = scores[labels == 0]
    if len(pos) == 0 or len(neg) == 0:
        return 0.5
    concordant = sum((p > n) + 0.5 * (p == n) for p in pos for n in neg)
    return float(concordant / (len(pos) * len(neg)))


# ── Async wrappers (DB → pure function) ─────────────────────────────


def _parse_metrics(raw) -> dict:
    import json

    if isinstance(raw, str):
        return json.loads(raw)
    if isinstance(raw, dict):
        return raw
    return {}


async def run_its_analysis(
    conn,
    tenant_id: str,
    period_type: str = "monthly",
    metric_key: str = "pipeline_win_rate",
) -> ITSResult | None:
    """Fetch snapshots + baseline, compute ITS."""
    rows = await conn.fetch(Q.SNAPSHOTS_SERIES, tenant_id, period_type)
    if len(rows) < 4:
        return None

    snapshots = [
        {"period_start": r["period_start"], "metrics": _parse_metrics(r["metrics"])}
        for r in rows
    ]

    values = [float(s["metrics"].get(metric_key, 0) or 0) for s in snapshots]
    periods = [s["period_start"].strftime("%b %Y") for s in snapshots]

    # Get intervention index from baseline
    baseline = await conn.fetchrow(Q.ACTIVE_BASELINE, tenant_id)
    if baseline and baseline["period_end"]:
        intervention_date = baseline["period_end"]
        intervention_index = next(
            (i for i, s in enumerate(snapshots) if s["period_start"] >= intervention_date),
            len(snapshots) // 2,
        )
    else:
        intervention_index = len(snapshots) // 2

    return interrupted_time_series(values, periods, intervention_index, metric_key)


async def run_causal_impact(
    conn,
    tenant_id: str,
    period_type: str = "monthly",
    metric_key: str = "pipeline_win_rate",
) -> BSTSResult | None:
    """Fetch snapshots + baseline, compute BSTS counterfactual."""
    rows = await conn.fetch(Q.SNAPSHOTS_SERIES, tenant_id, period_type)
    if len(rows) < 4:
        return None

    snapshots = [
        {"period_start": r["period_start"], "metrics": _parse_metrics(r["metrics"])}
        for r in rows
    ]

    values = [float(s["metrics"].get(metric_key, 0) or 0) for s in snapshots]
    periods = [s["period_start"].strftime("%b %Y") for s in snapshots]

    baseline = await conn.fetchrow(Q.ACTIVE_BASELINE, tenant_id)
    if baseline and baseline["period_end"]:
        intervention_date = baseline["period_end"]
        intervention_index = next(
            (i for i, s in enumerate(snapshots) if s["period_start"] >= intervention_date),
            len(snapshots) // 2,
        )
    else:
        intervention_index = len(snapshots) // 2

    return causal_impact_bsts(values, periods, intervention_index, metric_key)


async def run_dr_estimator(
    conn,
    tenant_id: str,
    lookback_months: int = 24,
) -> DRResult | None:
    """Fetch lead-level data, compute DR estimate."""
    from datetime import datetime, timedelta, timezone

    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_months * 30)
    rows = await conn.fetch(Q.DR_LEAD_FEATURES, tenant_id, cutoff)

    if len(rows) < 100:
        return None

    outcomes = np.array([r["outcome"] for r in rows], dtype=np.float64)
    treatments = np.array([r["treatment"] for r in rows], dtype=np.float64)

    # Build covariate matrix
    dow = np.array([r["day_of_week"] for r in rows], dtype=np.float64)
    hour = np.array([r["hour_of_day"] for r in rows], dtype=np.float64)
    lifecycle = np.array([r["lifecycle_days"] or 0 for r in rows], dtype=np.float64)

    # One-hot encode day of week (7 cols, drop first)
    dow_oh = np.zeros((len(rows), 6))
    for i, d in enumerate(dow):
        if 1 <= d <= 6:
            dow_oh[i, int(d) - 1] = 1.0

    # Hour buckets: 0-6, 6-12, 12-18, 18-24 (3 dummies)
    hour_buck = np.zeros((len(rows), 3))
    for i, h in enumerate(hour):
        if 6 <= h < 12:
            hour_buck[i, 0] = 1.0
        elif 12 <= h < 18:
            hour_buck[i, 1] = 1.0
        elif h >= 18:
            hour_buck[i, 2] = 1.0

    # Source encoding: top 5 sources + other
    sources = [r["source"] or "unknown" for r in rows]
    from collections import Counter

    source_counts = Counter(sources)
    top_sources = [s for s, _ in source_counts.most_common(5)]
    source_oh = np.zeros((len(rows), len(top_sources)))
    for i, s in enumerate(sources):
        if s in top_sources:
            source_oh[i, top_sources.index(s)] = 1.0

    # Normalise lifecycle_days
    lc_mean = np.mean(lifecycle) if len(lifecycle) > 0 else 0
    lc_std = np.std(lifecycle) if len(lifecycle) > 0 else 1
    lifecycle_norm = (lifecycle - lc_mean) / max(lc_std, 1e-10)

    covariates = np.column_stack([dow_oh, hour_buck, lifecycle_norm.reshape(-1, 1), source_oh])

    return doubly_robust_estimate(outcomes, treatments, covariates)
