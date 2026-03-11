"""
AI Digest — generates plain-English interpretations of analytics page data.

Uses Claude Haiku via direct httpx call (no SDK). Results are cached
in memory keyed by (tenant, page_name, date.today()) — clears on restart.
Falls back gracefully (returns None) on any failure.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import date
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# ── In-memory cache ────────────────────────────────────────────────────────────

_cache: dict[tuple, dict | None] = {}

_HAIKU_MODEL = "claude-haiku-4-5-20251001"
_API_URL = "https://api.anthropic.com/v1/messages"
_MAX_TOKENS = 500
_TIMEOUT_SECONDS = 10

_SYSTEM_PROMPT = (
    "You are an analytics advisor for HumTech, a revenue engine company. "
    "You're interpreting pipeline metrics for the founder (Lou) who is learning "
    "these statistical concepts. Be direct, practical, and action-oriented. "
    "Explain what the numbers mean in business terms, not statistical jargon. "
    "Reference specific numbers from the data."
)


def _get_api_key() -> str:
    return os.environ.get("ANTHROPIC_API_KEY", "")


async def _call_haiku(prompt: str) -> dict | None:
    """Call Claude Haiku and return parsed JSON dict, or None on failure."""
    api_key = _get_api_key()
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY not set — skipping digest")
        return None

    payload = {
        "model": _HAIKU_MODEL,
        "max_tokens": _MAX_TOKENS,
        "system": _SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": prompt}],
    }
    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                _API_URL,
                json=payload,
                headers=headers,
                timeout=_TIMEOUT_SECONDS,
            )
        resp.raise_for_status()
        data = resp.json()
        raw_text = data["content"][0]["text"].strip()

        # Strip markdown code fences if present
        if raw_text.startswith("```"):
            raw_text = raw_text.split("```")[1]
            if raw_text.startswith("json"):
                raw_text = raw_text[4:]

        return json.loads(raw_text)
    except Exception as exc:
        logger.warning("Digest generation failed: %s", exc)
        return None


def _cache_key(tenant: str, page: str) -> tuple:
    return (tenant, page, date.today())


def _empty_digest() -> dict:
    return {"summary": "", "highlights": [], "actions": [], "concerns": []}


async def _generate_with_timeout(prompt: str) -> dict | None:
    try:
        return await asyncio.wait_for(_call_haiku(prompt), timeout=_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        logger.warning("Digest timed out after %ss", _TIMEOUT_SECONDS)
        return None


# ── Dashboard digest ────────────────────────────────────────────────────────────


async def generate_dashboard_digest(
    tenant: str,
    kpis: list[dict],
    sparklines_raw: str,
    narratives: list[dict],
    anomaly_count: int,
) -> dict | None:
    key = _cache_key(tenant, "dashboard")
    if key in _cache:
        return _cache[key]

    # Build a concise metrics summary for the prompt
    kpi_lines = []
    for kpi in kpis:
        line = f"  - {kpi['label']}: {kpi['value']}"
        if kpi.get("ci"):
            line += f" (95% CI {kpi['ci']})"
        kpi_lines.append(line)

    narrative_lines = [f"  - {n['text']}" for n in narratives]

    prompt = f"""Interpret these pipeline dashboard metrics for tenant '{tenant}':

KPIs:
{chr(10).join(kpi_lines)}

Active anomalies (Western Electric violations in latest period): {anomaly_count}

Auto-generated insights:
{chr(10).join(narrative_lines)}

Return ONLY valid JSON (no markdown, no prose outside JSON) with this exact structure:
{{
  "summary": "<2-3 sentence overview of what the data shows>",
  "highlights": ["<positive finding 1>", "<positive finding 2>"],
  "actions": ["<recommended next step 1>", "<recommended next step 2>"],
  "concerns": ["<thing to watch 1>"]
}}"""

    result = await _generate_with_timeout(prompt)
    _cache[key] = result
    return result


# ── Control charts digest ───────────────────────────────────────────────────────


async def generate_control_charts_digest(
    tenant: str,
    charts: list[dict],
) -> dict | None:
    key = _cache_key(tenant, "control_charts")
    if key in _cache:
        return _cache[key]

    metrics_with_violations = []
    metrics_clean = []
    cusum_active = []

    for chart in charts:
        violations = chart.get("violations", [])
        label = chart["label"]
        center = chart.get("center", 0)
        if violations:
            v_rules = list({str(v["rule"]) for v in violations})
            metrics_with_violations.append(
                f"  - {label}: {len(violations)} violation(s) — rules: {', '.join(v_rules)}; center={center*100:.1f}%"
            )
        else:
            metrics_clean.append(f"  - {label}: in control (center={center*100:.1f}%)")

        # Check if CUSUM signals any drift
        cusum_signals = chart.get("cusum_signals", [])
        if cusum_signals:
            cusum_active.append(f"  - {label}: CUSUM drift signal at {len(cusum_signals)} point(s)")

    prompt = f"""Interpret these control chart results for pipeline metrics (tenant: '{tenant}'):

Metrics with Western Electric violations (OUT OF CONTROL):
{chr(10).join(metrics_with_violations) if metrics_with_violations else '  None — all metrics in control'}

Metrics in control:
{chr(10).join(metrics_clean) if metrics_clean else '  None'}

CUSUM drift signals (sustained directional trends):
{chr(10).join(cusum_active) if cusum_active else '  None detected'}

Control limits are set at 3-sigma. Violations mean a metric has moved outside normal statistical variation.

Return ONLY valid JSON with this exact structure:
{{
  "summary": "<2-3 sentence overview of what the control chart data shows>",
  "highlights": ["<positive finding>"],
  "actions": ["<recommended action based on violations or trends>"],
  "concerns": ["<specific metric or pattern to investigate>"]
}}"""

    result = await _generate_with_timeout(prompt)
    _cache[key] = result
    return result


# ── Survival digest ─────────────────────────────────────────────────────────────


async def generate_survival_digest(
    tenant: str,
    curves_data: dict[str, Any],
    dead_deals: list[dict],
) -> dict | None:
    key = _cache_key(tenant, "survival")
    if key in _cache:
        return _cache[key]

    stage_lines = []
    for stage, curve in curves_data.items():
        median = curve.get("median")
        n_at_risk = curve.get("n_at_risk", 0)
        initial_n = n_at_risk if isinstance(n_at_risk, int) else (n_at_risk[0] if n_at_risk else 0)
        median_str = f"{median:.0f} days" if median is not None else "not reached"
        stage_lines.append(
            f"  - {stage.replace('_', ' ').title()}: median survival {median_str}, n={initial_n}"
        )

    dead_lines = []
    for d in dead_deals[:10]:  # Cap at 10 for prompt length
        dead_lines.append(
            f"  - {d['name']} in '{d['stage']}': {d['days_in_stage']} days "
            f"(median for stage: {d.get('median_for_stage', '?')} days)"
        )

    prompt = f"""Interpret this survival analysis data for deal aging (tenant: '{tenant}'):

Kaplan-Meier median survival per stage (time until deal exits the stage):
{chr(10).join(stage_lines) if stage_lines else '  No stage data available'}

Statistically dead deals (significantly exceeded stage median, n={len(dead_deals)}):
{chr(10).join(dead_lines) if dead_lines else '  None detected'}

Higher median = deals linger longer in a stage. Dead deals are costing revenue and need intervention.

Return ONLY valid JSON with this exact structure:
{{
  "summary": "<2-3 sentence overview of deal aging patterns>",
  "highlights": ["<positive finding about deal flow>"],
  "actions": ["<specific action to take on dead deals or slow stages>"],
  "concerns": ["<stage or pattern that needs attention>"]
}}"""

    result = await _generate_with_timeout(prompt)
    _cache[key] = result
    return result


# ── Bottleneck digest ───────────────────────────────────────────────────────────


async def generate_bottleneck_digest(
    tenant: str,
    stages: list[dict],
) -> dict | None:
    key = _cache_key(tenant, "bottleneck")
    if key in _cache:
        return _cache[key]

    stage_lines = []
    bottleneck_stage = None
    unstable_stages = []

    for s in stages:
        label = s["stage"]
        rho = s.get("rho") or 0
        wip = s.get("wip") or 0
        throughput = s.get("throughput_per_week") or 0
        dwell = s.get("median_dwell_days") or 0
        flags = []
        if s.get("is_bottleneck"):
            flags.append("BOTTLENECK")
            bottleneck_stage = label
        if s.get("is_unstable"):
            flags.append("UNSTABLE")
            unstable_stages.append(label)
        flag_str = f" [{', '.join(flags)}]" if flags else ""
        stage_lines.append(
            f"  - {label}: WIP={wip}, throughput={throughput:.1f}/week, "
            f"dwell={dwell:.0f}days, utilisation={rho:.2f}{flag_str}"
        )

    prompt = f"""Interpret this Little's Law bottleneck analysis for the pipeline (tenant: '{tenant}'):

Stage metrics (WIP = work in progress, utilisation rho > 1.0 = unstable queue):
{chr(10).join(stage_lines) if stage_lines else '  No stage data available'}

Identified bottleneck: {bottleneck_stage or 'None identified'}
Unstable stages (rho >= 1.0): {', '.join(unstable_stages) if unstable_stages else 'None'}

The bottleneck stage limits total pipeline throughput. Unstable stages are building up backlogs.

Return ONLY valid JSON with this exact structure:
{{
  "summary": "<2-3 sentence overview of pipeline flow and constraint>",
  "highlights": ["<stage that is flowing well>"],
  "actions": ["<specific action to relieve the bottleneck or unstable stage>"],
  "concerns": ["<stage or flow issue to address>"]
}}"""

    result = await _generate_with_timeout(prompt)
    _cache[key] = result
    return result


# ── Causal digest ───────────────────────────────────────────────────────────────


async def generate_causal_digest(
    tenant: str,
    metric: str,
    its: dict | None,
    bsts: dict | None,
    dr: dict | None,
    uplift: dict | None,
) -> dict | None:
    key = _cache_key(tenant, f"causal_{metric}")
    if key in _cache:
        return _cache[key]

    lines = [f"Metric being analysed: {metric.replace('_', ' ')}\n"]

    if its:
        level_p = its.get("level_change_p", 1.0)
        level_ci = its.get("level_change_ci", [0, 0])
        lines.append(
            f"ITS (Interrupted Time Series):\n"
            f"  Level change: {its.get('level_change', 0):.4f} "
            f"(p={level_p:.3f}, CI: {level_ci[0]:.4f} to {level_ci[1]:.4f})\n"
            f"  Slope change: {its.get('slope_change', 0):.4f} "
            f"(p={its.get('slope_change_p', 1.0):.3f})\n"
            f"  Narrative: {its.get('narrative', '')}"
        )

    if bsts:
        lines.append(
            f"\nBSTS (CausalImpact):\n"
            f"  Relative effect: {bsts.get('relative_effect_pct', 0):.1f}%\n"
            f"  P(causal): {bsts.get('prob_causal', 0):.2%}\n"
            f"  Narrative: {bsts.get('narrative', '')}"
        )

    if dr:
        ate_ci = [dr.get("ate_ci_lower", 0), dr.get("ate_ci_upper", 0)]
        lines.append(
            f"\nDoubly Robust estimator:\n"
            f"  ATE: {dr.get('ate', 0):.4f} (CI: {ate_ci[0]:.4f} to {ate_ci[1]:.4f})\n"
            f"  Treated: n={dr.get('n_treated', 0)}, mean outcome={dr.get('treated_outcome_mean', 0):.4f}\n"
            f"  Control: n={dr.get('n_control', 0)}, mean outcome={dr.get('control_outcome_mean', 0):.4f}\n"
            f"  Propensity AUC: {dr.get('propensity_auc', 0):.3f}\n"
            f"  Narrative: {dr.get('narrative', '')}"
        )

    if uplift:
        lines.append(
            f"\nUplift consensus: {uplift.get('consensus', '')}"
        )

    prompt = f"""Interpret this causal attribution analysis for HumTech's revenue engine (tenant: '{tenant}'):

{"".join(lines)}

These methods test whether HumTech's intervention actually caused improvement in the metric, not just correlation.
P(causal) > 0.95 = very likely causal. p < 0.05 = statistically significant.

Return ONLY valid JSON with this exact structure:
{{
  "summary": "<2-3 sentence overview of whether the intervention caused improvement and how confident we are>",
  "highlights": ["<strongest evidence of positive causal impact>"],
  "actions": ["<what to do with this finding — report to client, dig deeper, etc.>"],
  "concerns": ["<any method disagreement, low confidence, or confounds to investigate>"]
}}"""

    result = await _generate_with_timeout(prompt)
    _cache[key] = result
    return result


# ── Cohort digest ───────────────────────────────────────────────────────────────


async def generate_cohort_digest(
    tenant: str,
    matrix: dict | None,
    simpson: dict | None,
) -> dict | None:
    key = _cache_key(tenant, "cohort")
    if key in _cache:
        return _cache[key]

    lines = []

    if matrix:
        cohort_labels = matrix.get("cohort_labels", [])
        cohort_sizes = matrix.get("cohort_sizes", [])
        conversion_rates = matrix.get("conversion_rates", [])

        # Summarise first and last cohort if available
        if cohort_labels and cohort_sizes:
            lines.append(f"Number of cohorts: {len(cohort_labels)}")
            lines.append(f"Most recent cohort: {cohort_labels[-1]} (n={cohort_sizes[-1]})")
            lines.append(f"Oldest cohort: {cohort_labels[0]} (n={cohort_sizes[0]})")

        # Compute avg conversion rate across cohorts where available
        if conversion_rates:
            all_rates = [
                r for row in conversion_rates
                for r in row
                if r is not None and r > 0
            ]
            if all_rates:
                avg_rate = sum(all_rates) / len(all_rates)
                lines.append(f"Average cohort conversion rate (across all periods): {avg_rate*100:.1f}%")

    simpson_lines = []
    if simpson:
        has_paradox = simpson.get("has_paradox", False)
        agg_pre = simpson.get("aggregate_rate_pre", 0)
        agg_post = simpson.get("aggregate_rate_post", 0)
        direction = simpson.get("aggregate_direction", "")
        simpson_lines.append(
            f"Simpson's Paradox detected: {'YES' if has_paradox else 'No'}"
        )
        simpson_lines.append(
            f"Aggregate win rate: pre-HumTech {agg_pre*100:.1f}% → post {agg_post*100:.1f}% ({direction})"
        )
        for b in simpson.get("breakdowns", [])[:5]:
            simpson_lines.append(
                f"  Source '{b['source']}': {b['rate_pre']*100:.1f}% → {b['rate_post']*100:.1f}% "
                f"(n_pre={b['n_pre']}, n_post={b['n_post']}, {b['direction']})"
            )
        if simpson.get("explanation"):
            simpson_lines.append(f"Explanation: {simpson['explanation']}")

    prompt = f"""Interpret this cohort analysis for pipeline conversion (tenant: '{tenant}'):

Cohort summary:
{chr(10).join(lines) if lines else '  No cohort data available'}

Simpson's Paradox check:
{chr(10).join(simpson_lines) if simpson_lines else '  Not enough data for paradox check'}

Cohorts group leads by when they entered. Declining rates in newer cohorts = pipeline quality issue.
Simpson's Paradox = the mix of lead sources is masking or reversing the true conversion trend.

Return ONLY valid JSON with this exact structure:
{{
  "summary": "<2-3 sentence overview of cohort conversion trends and any anomalies>",
  "highlights": ["<cohort that is performing well>"],
  "actions": ["<what to do based on cohort trends or paradox>"],
  "concerns": ["<concerning cohort trend or paradox implication>"]
}}"""

    result = await _generate_with_timeout(prompt)
    _cache[key] = result
    return result
