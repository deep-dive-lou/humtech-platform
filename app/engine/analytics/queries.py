"""
Analytics — SQL Queries

All SQL centralised here. Every query is parameterised and tenant-scoped.
"""

# ── Metric snapshots time series ──────────────────────────────────────

SNAPSHOTS_SERIES = """
SELECT period_start, period_end, metrics
FROM engine.metric_snapshots
WHERE tenant_id = $1::uuid
  AND period_type = $2
ORDER BY period_start;
"""

LATEST_SNAPSHOT = """
SELECT period_start, period_end, metrics
FROM engine.metric_snapshots
WHERE tenant_id = $1::uuid
  AND period_type = $2
ORDER BY period_start DESC
LIMIT 1;
"""

# ── Tenant lookup ─────────────────────────────────────────────────────

TENANT_BY_SLUG = """
SELECT tenant_id FROM core.tenants WHERE tenant_slug = $1;
"""

# ── Active baseline ───────────────────────────────────────────────────

ACTIVE_BASELINE = """
SELECT metrics, period_start, period_end, label
FROM engine.baselines
WHERE tenant_id = $1::uuid AND is_active = TRUE
ORDER BY created_at DESC
LIMIT 1;
"""

# ── Stage dwell times (for KM survival curves) ───────────────────────
# Returns each lead's time in each stage, with censoring indicator.
# Right-censored if the lead is still in that stage (is_open AND current_stage matches).

STAGE_DWELL_TIMES = """
WITH stage_transitions AS (
    SELECT
        le.lead_id,
        le.to_stage AS stage,
        le.occurred_at AS entered_at,
        LEAD(le.occurred_at) OVER (PARTITION BY le.lead_id ORDER BY le.occurred_at) AS exited_at,
        l.is_open,
        l.current_stage
    FROM engine.lead_events le
    JOIN engine.leads l ON l.lead_id = le.lead_id
    WHERE le.tenant_id = $1::uuid
      AND le.event_type = 'stage_changed'
      AND le.occurred_at >= $2::timestamptz
    UNION ALL
    SELECT
        le.lead_id,
        'lead_created' AS stage,
        le.occurred_at AS entered_at,
        (SELECT MIN(le2.occurred_at)
         FROM engine.lead_events le2
         WHERE le2.lead_id = le.lead_id
           AND le2.event_type = 'stage_changed'
           AND le2.occurred_at > le.occurred_at) AS exited_at,
        l.is_open,
        l.current_stage
    FROM engine.lead_events le
    JOIN engine.leads l ON l.lead_id = le.lead_id
    WHERE le.tenant_id = $1::uuid
      AND le.event_type = 'lead_created'
      AND le.occurred_at >= $2::timestamptz
)
SELECT
    stage,
    EXTRACT(EPOCH FROM (COALESCE(exited_at, now()) - entered_at)) / 86400.0 AS duration_days,
    CASE WHEN exited_at IS NULL AND is_open THEN 0 ELSE 1 END AS event_observed
FROM stage_transitions
WHERE stage IS NOT NULL
  AND entered_at IS NOT NULL;
"""

# ── Dead deals (open leads exceeding threshold) ──────────────────────

DEAD_DEAL_CANDIDATES = """
SELECT
    l.lead_id,
    l.name,
    l.current_stage,
    l.lead_value,
    l.source,
    EXTRACT(EPOCH FROM (now() - MAX(le.occurred_at))) / 86400.0 AS days_in_stage
FROM engine.leads l
JOIN engine.lead_events le ON le.lead_id = l.lead_id
WHERE l.tenant_id = $1::uuid
  AND l.is_open = TRUE
  AND l.current_stage NOT IN ('lead_won', 'lead_lost', 'rejected', 'revenue_collected')
GROUP BY l.lead_id, l.name, l.current_stage, l.lead_value, l.source
ORDER BY days_in_stage DESC;
"""

# ── Bottleneck: current WIP per stage ─────────────────────────────────

WIP_PER_STAGE = """
SELECT current_stage AS stage, COUNT(*) AS wip
FROM engine.leads
WHERE tenant_id = $1::uuid AND is_open = TRUE
GROUP BY current_stage
ORDER BY MIN(created_at);
"""

# ── Bottleneck: throughput (exits per stage in period) ────────────────

STAGE_EXITS = """
SELECT
    from_stage AS stage,
    COUNT(*) AS exits,
    EXTRACT(EPOCH FROM (MAX(occurred_at) - MIN(occurred_at))) / 604800.0 AS span_weeks
FROM engine.lead_events
WHERE tenant_id = $1::uuid
  AND event_type = 'stage_changed'
  AND occurred_at >= $2::timestamptz
GROUP BY from_stage;
"""

# ── Bottleneck: median dwell time per stage (completed transitions) ───

MEDIAN_DWELL_PER_STAGE = """
WITH transitions AS (
    SELECT
        from_stage AS stage,
        EXTRACT(EPOCH FROM (occurred_at - LAG(occurred_at) OVER (
            PARTITION BY lead_id ORDER BY occurred_at
        ))) / 86400.0 AS dwell_days
    FROM engine.lead_events
    WHERE tenant_id = $1::uuid
      AND event_type = 'stage_changed'
      AND occurred_at >= $2::timestamptz
)
SELECT
    stage,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dwell_days) AS median_dwell_days
FROM transitions
WHERE dwell_days IS NOT NULL AND dwell_days > 0
GROUP BY stage;
"""

# ── Bottleneck: arrivals per stage ────────────────────────────────────

STAGE_ARRIVALS = """
SELECT
    to_stage AS stage,
    COUNT(*) AS arrivals,
    EXTRACT(EPOCH FROM (MAX(occurred_at) - MIN(occurred_at))) / 604800.0 AS span_weeks
FROM engine.lead_events
WHERE tenant_id = $1::uuid
  AND event_type = 'stage_changed'
  AND occurred_at >= $2::timestamptz
GROUP BY to_stage;
"""

# ── Causal: lead-level features for Doubly Robust estimator ──────────

DR_LEAD_FEATURES = """
SELECT
    l.lead_id,
    l.source,
    l.lead_value,
    l.created_at,
    CASE WHEN l.won_at IS NOT NULL THEN 1 ELSE 0 END AS outcome,
    CASE WHEN EXISTS (
        SELECT 1 FROM engine.lead_events fc
        WHERE fc.lead_id = l.lead_id
          AND fc.event_type = 'first_contact'
          AND fc.occurred_at <= l.created_at + INTERVAL '24 hours'
    ) THEN 1 ELSE 0 END AS treatment,
    EXTRACT(DOW FROM l.created_at) AS day_of_week,
    EXTRACT(HOUR FROM l.created_at) AS hour_of_day,
    EXTRACT(EPOCH FROM (COALESCE(l.won_at, l.lost_at, now()) - l.created_at)) / 86400.0 AS lifecycle_days
FROM engine.leads l
WHERE l.tenant_id = $1::uuid
  AND l.created_at >= $2::timestamptz
  AND (l.won_at IS NOT NULL OR l.lost_at IS NOT NULL);
"""

# ── Cohort: wins grouped by creation month and win month ─────────────

COHORT_OUTCOMES = """
SELECT
    DATE_TRUNC('month', l.created_at) AS cohort_month,
    DATE_TRUNC('month', l.won_at) AS win_month,
    COUNT(*) AS wins,
    COALESCE(SUM(l.lead_value), 0) AS revenue
FROM engine.leads l
WHERE l.tenant_id = $1::uuid
  AND l.created_at >= $2::timestamptz
  AND l.won_at IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2;
"""

# ── Cohort: lead counts per creation month ───────────────────────────

COHORT_SIZES = """
SELECT
    DATE_TRUNC('month', created_at) AS cohort_month,
    COUNT(*) AS cohort_size
FROM engine.leads
WHERE tenant_id = $1::uuid
  AND created_at >= $2::timestamptz
GROUP BY 1
ORDER BY 1;
"""

# ── Cohort: win rates by source for Simpson's Paradox detection ──────

COHORT_RATES_BY_SOURCE = """
SELECT
    DATE_TRUNC('month', l.created_at) AS cohort_month,
    COALESCE(l.source, 'unknown') AS source,
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE l.won_at IS NOT NULL) AS wins
FROM engine.leads l
WHERE l.tenant_id = $1::uuid
  AND l.created_at >= $2::timestamptz
  AND (l.won_at IS NOT NULL OR l.lost_at IS NOT NULL)
GROUP BY 1, 2
ORDER BY 1, 2;
"""
