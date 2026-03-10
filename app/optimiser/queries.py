"""
Optimisation Engine — SQL Constants

All queries parameterised with $1, $2, etc. for asyncpg.
Tenant-scoped by convention: $1 is always tenant_id.
"""

# ---------------------------------------------------------------------------
# Experiments
# ---------------------------------------------------------------------------

LIST_EXPERIMENTS = """
SELECT experiment_id, name, description, mode, status, metric_type,
       started_at, completed_at, created_at, updated_at,
       winner_variant_id
FROM optimiser.experiments
WHERE tenant_id = $1::uuid
ORDER BY created_at DESC;
"""

GET_EXPERIMENT = """
SELECT experiment_id, tenant_id, name, description, mode, status, metric_type,
       config, p_best_threshold, expected_loss_threshold, min_impressions, min_days,
       winner_variant_id, started_at, completed_at, created_at, updated_at
FROM optimiser.experiments
WHERE tenant_id = $1::uuid AND experiment_id = $2::uuid;
"""

INSERT_EXPERIMENT = """
INSERT INTO optimiser.experiments
    (tenant_id, name, description, mode, metric_type, config,
     p_best_threshold, expected_loss_threshold, min_impressions, min_days)
VALUES ($1::uuid, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10)
RETURNING experiment_id;
"""

UPDATE_EXPERIMENT_STATUS = """
UPDATE optimiser.experiments
SET status = $3, updated_at = now(),
    started_at = CASE WHEN $3 = 'running' AND started_at IS NULL THEN now() ELSE started_at END,
    completed_at = CASE WHEN $3 = 'completed' THEN now() ELSE completed_at END
WHERE tenant_id = $1::uuid AND experiment_id = $2::uuid
RETURNING experiment_id;
"""

SET_WINNER = """
UPDATE optimiser.experiments
SET winner_variant_id = $3::uuid, status = 'completed', completed_at = now(), updated_at = now()
WHERE tenant_id = $1::uuid AND experiment_id = $2::uuid
RETURNING experiment_id;
"""

# ---------------------------------------------------------------------------
# Variants
# ---------------------------------------------------------------------------

LIST_VARIANTS = """
SELECT variant_id, label, description, is_control, sort_order, factor_values, is_active, created_at
FROM optimiser.variants
WHERE tenant_id = $1::uuid AND experiment_id = $2::uuid
ORDER BY sort_order, created_at;
"""

LIST_ACTIVE_VARIANTS = """
SELECT variant_id, label, is_control, factor_values
FROM optimiser.variants
WHERE tenant_id = $1::uuid AND experiment_id = $2::uuid AND is_active = TRUE
ORDER BY sort_order, created_at;
"""

INSERT_VARIANT = """
INSERT INTO optimiser.variants
    (tenant_id, experiment_id, label, description, is_control, sort_order, factor_values)
VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7::jsonb)
RETURNING variant_id;
"""

# ---------------------------------------------------------------------------
# Factors & Levels
# ---------------------------------------------------------------------------

LIST_FACTORS = """
SELECT factor_id, name, sort_order, created_at
FROM optimiser.factors
WHERE tenant_id = $1::uuid AND experiment_id = $2::uuid
ORDER BY sort_order, created_at;
"""

INSERT_FACTOR = """
INSERT INTO optimiser.factors (tenant_id, experiment_id, name, sort_order)
VALUES ($1::uuid, $2::uuid, $3, $4)
RETURNING factor_id;
"""

LIST_LEVELS = """
SELECT level_id, factor_id, label, description, content, preview_url, is_active, sort_order
FROM optimiser.levels
WHERE tenant_id = $1::uuid AND factor_id = $2::uuid
ORDER BY sort_order, created_at;
"""

INSERT_LEVEL = """
INSERT INTO optimiser.levels (tenant_id, factor_id, label, description, content, sort_order)
VALUES ($1::uuid, $2::uuid, $3, $4, $5::jsonb, $6)
RETURNING level_id;
"""

# ---------------------------------------------------------------------------
# Observations
# ---------------------------------------------------------------------------

INSERT_OBSERVATION = """
INSERT INTO optimiser.observations
    (tenant_id, experiment_id, variant_id, event_type, value, visitor_id, source)
VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6, $7)
RETURNING observation_id;
"""

# ---------------------------------------------------------------------------
# Daily stats
# ---------------------------------------------------------------------------

ROLLUP_DAILY_STATS = """
INSERT INTO optimiser.daily_stats (tenant_id, experiment_id, variant_id, day, impressions, conversions, total_value, computed_at)
SELECT
    tenant_id, experiment_id, variant_id,
    recorded_at::date AS day,
    COUNT(*) FILTER (WHERE event_type = 'impression'),
    COUNT(*) FILTER (WHERE event_type = 'conversion'),
    COALESCE(SUM(value) FILTER (WHERE event_type = 'conversion'), 0),
    now()
FROM optimiser.observations
WHERE experiment_id = $1::uuid
GROUP BY tenant_id, experiment_id, variant_id, recorded_at::date
ON CONFLICT ON CONSTRAINT uq_daily_stats
DO UPDATE SET
    impressions = EXCLUDED.impressions,
    conversions = EXCLUDED.conversions,
    total_value = EXCLUDED.total_value,
    computed_at = now();
"""

VARIANT_TOTALS = """
SELECT
    v.variant_id, v.label, v.is_control,
    COALESCE(SUM(ds.impressions), 0)::int AS impressions,
    COALESCE(SUM(ds.conversions), 0)::int AS conversions,
    COALESCE(SUM(ds.total_value), 0) AS total_value
FROM optimiser.variants v
LEFT JOIN optimiser.daily_stats ds
    ON ds.variant_id = v.variant_id AND ds.experiment_id = v.experiment_id
WHERE v.tenant_id = $1::uuid AND v.experiment_id = $2::uuid AND v.is_active = TRUE
GROUP BY v.variant_id, v.label, v.is_control
ORDER BY v.sort_order, v.created_at;
"""

DAILY_SERIES = """
SELECT
    v.variant_id, v.label,
    ds.day, ds.impressions, ds.conversions
FROM optimiser.daily_stats ds
JOIN optimiser.variants v ON v.variant_id = ds.variant_id
WHERE ds.experiment_id = $1::uuid
ORDER BY ds.day, v.sort_order;
"""

# ---------------------------------------------------------------------------
# Allocate — fetch posteriors for Thompson Sampling
# ---------------------------------------------------------------------------

VARIANT_POSTERIORS = """
SELECT
    v.variant_id,
    COALESCE(SUM(ds.conversions), 0)::int AS successes,
    COALESCE(SUM(ds.impressions) - SUM(ds.conversions), 0)::int AS failures
FROM optimiser.variants v
LEFT JOIN optimiser.daily_stats ds
    ON ds.variant_id = v.variant_id AND ds.experiment_id = v.experiment_id
WHERE v.experiment_id = $1::uuid AND v.is_active = TRUE
GROUP BY v.variant_id
ORDER BY v.sort_order;
"""