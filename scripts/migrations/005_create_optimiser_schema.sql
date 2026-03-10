-- 005_create_optimiser_schema.sql
-- Optimisation Engine schema — multivariate testing (Bandit / Taguchi / Evolutionary)
-- Zero dependencies on bot, engine, or portal schemas; references core.tenants only.

CREATE SCHEMA IF NOT EXISTS optimiser;

-- ============================================================
-- optimiser.experiments
-- ============================================================
CREATE TABLE optimiser.experiments (
    experiment_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           UUID NOT NULL REFERENCES core.tenants(tenant_id),

    -- Definition
    name                TEXT NOT NULL,
    description         TEXT,
    mode                TEXT NOT NULL CHECK (mode IN ('bandit', 'taguchi', 'evolutionary')),
    status              TEXT NOT NULL DEFAULT 'draft'
                        CHECK (status IN ('draft', 'running', 'paused', 'completed', 'archived')),
    metric_type         TEXT NOT NULL DEFAULT 'conversion'
                        CHECK (metric_type IN ('conversion', 'revenue', 'custom')),

    -- Mode-specific config (genuinely polymorphic — JSONB is correct)
    -- bandit:        { "prior_alpha": 1, "prior_beta": 1 }
    -- taguchi:       { "factors": [{"name": "headline", "levels": ["A","B","C"]}], "array_type": "L9" }
    -- evolutionary:  { "pop_size": 20, "mutation_rate": 0.05, "crossover_rate": 0.7, "elite_pct": 0.2 }
    config              JSONB NOT NULL DEFAULT '{}',

    -- Winner rules — typed columns (same structure for all modes)
    p_best_threshold        NUMERIC NOT NULL DEFAULT 0.95,
    expected_loss_threshold NUMERIC NOT NULL DEFAULT 0.01,
    min_impressions         INT NOT NULL DEFAULT 100,
    min_days                INT NOT NULL DEFAULT 7,

    -- Winner
    winner_variant_id   UUID,

    -- Timestamps
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_experiment_name UNIQUE (tenant_id, name)
);

CREATE INDEX idx_experiments_tenant        ON optimiser.experiments (tenant_id);
CREATE INDEX idx_experiments_tenant_status ON optimiser.experiments (tenant_id, status);

-- ============================================================
-- optimiser.variants
-- ============================================================
CREATE TABLE optimiser.variants (
    variant_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           UUID NOT NULL REFERENCES core.tenants(tenant_id),
    experiment_id       UUID NOT NULL REFERENCES optimiser.experiments(experiment_id)
                        ON DELETE CASCADE,

    -- Definition
    label               TEXT NOT NULL,
    description         TEXT,
    is_control          BOOLEAN NOT NULL DEFAULT FALSE,
    sort_order          INT NOT NULL DEFAULT 0,

    -- Factor values — variable structure per experiment (JSONB correct)
    -- e.g. {"headline": "Free consultation", "cta_color": "green", "image": "hero_v2"}
    factor_values       JSONB NOT NULL DEFAULT '{}',

    is_active           BOOLEAN NOT NULL DEFAULT TRUE,  -- disable combinations without deleting

    -- Timestamps
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_variant_label UNIQUE (experiment_id, label)
);

CREATE INDEX idx_variants_experiment ON optimiser.variants (experiment_id);

-- ============================================================
-- optimiser.observations
-- Append-only event log. High-volume table — no speculative columns.
-- ============================================================
CREATE TABLE optimiser.observations (
    observation_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           UUID NOT NULL REFERENCES core.tenants(tenant_id),
    experiment_id       UUID NOT NULL REFERENCES optimiser.experiments(experiment_id)
                        ON DELETE CASCADE,
    variant_id          UUID NOT NULL REFERENCES optimiser.variants(variant_id)
                        ON DELETE CASCADE,

    -- Event
    event_type          TEXT NOT NULL CHECK (event_type IN ('impression', 'conversion')),
    value               NUMERIC(12,4),          -- NULL for binary conversion, amount for revenue metric
    visitor_id          TEXT,                    -- optional dedup key
    source              TEXT,                    -- 'js_snippet', 'api', 'webhook'

    -- Timestamp
    recorded_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_observations_experiment   ON optimiser.observations (experiment_id, variant_id, event_type);
CREATE INDEX idx_observations_tenant       ON optimiser.observations (tenant_id, experiment_id);
CREATE INDEX idx_observations_recorded     ON optimiser.observations (experiment_id, recorded_at);

-- ============================================================
-- optimiser.daily_stats
-- Materialised rollup — recomputable from observations at any time.
-- ============================================================
CREATE TABLE optimiser.daily_stats (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           UUID NOT NULL REFERENCES core.tenants(tenant_id),
    experiment_id       UUID NOT NULL REFERENCES optimiser.experiments(experiment_id)
                        ON DELETE CASCADE,
    variant_id          UUID NOT NULL REFERENCES optimiser.variants(variant_id)
                        ON DELETE CASCADE,

    -- Rollup
    day                 DATE NOT NULL,
    impressions         INT NOT NULL DEFAULT 0,
    conversions         INT NOT NULL DEFAULT 0,
    total_value         NUMERIC(12,4) NOT NULL DEFAULT 0,

    -- Computation timestamp
    computed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_daily_stats UNIQUE (experiment_id, variant_id, day)
);

CREATE INDEX idx_daily_stats_experiment ON optimiser.daily_stats (experiment_id, day);

-- ============================================================
-- optimiser.evolutionary_generations
-- Defined now to avoid later migration. Used in Phase 4.
-- ============================================================
CREATE TABLE optimiser.evolutionary_generations (
    generation_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           UUID NOT NULL REFERENCES core.tenants(tenant_id),
    experiment_id       UUID NOT NULL REFERENCES optimiser.experiments(experiment_id)
                        ON DELETE CASCADE,

    -- Generation data
    generation_number   INT NOT NULL,
    population          JSONB NOT NULL,         -- array of {variant_id, fitness, parent_ids}

    -- Timestamp
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_generation UNIQUE (experiment_id, generation_number)
);

-- ============================================================
-- optimiser.factors
-- First-class element definitions for Visiopt-style UX.
-- e.g. "headline", "hero image", "CTA button"
-- ============================================================
CREATE TABLE optimiser.factors (
    factor_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           UUID NOT NULL REFERENCES core.tenants(tenant_id),
    experiment_id       UUID NOT NULL REFERENCES optimiser.experiments(experiment_id)
                        ON DELETE CASCADE,

    -- Definition
    name                TEXT NOT NULL,
    sort_order          INT NOT NULL DEFAULT 0,

    -- Timestamp
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_factor_name UNIQUE (experiment_id, name)
);

CREATE INDEX idx_factors_experiment ON optimiser.factors (experiment_id);

-- ============================================================
-- optimiser.levels
-- Versions per factor. e.g. headline A, headline B, headline C.
-- preview_url supports screenshots/thumbnails in the UI.
-- ============================================================
CREATE TABLE optimiser.levels (
    level_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           UUID NOT NULL REFERENCES core.tenants(tenant_id),
    factor_id           UUID NOT NULL REFERENCES optimiser.factors(factor_id)
                        ON DELETE CASCADE,

    -- Definition
    label               TEXT NOT NULL,
    description         TEXT,
    content             JSONB NOT NULL DEFAULT '{}',   -- actual value served: text, image URL, CSS properties
    preview_url         TEXT,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,  -- disable a version without deleting
    sort_order          INT NOT NULL DEFAULT 0,

    -- Timestamp
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_level_label UNIQUE (factor_id, label)
);

CREATE INDEX idx_levels_factor ON optimiser.levels (factor_id);

-- ============================================================
-- Add FK for winner_variant_id (deferred — variants table must exist first)
-- ============================================================
ALTER TABLE optimiser.experiments
    ADD CONSTRAINT fk_winner_variant
    FOREIGN KEY (winner_variant_id)
    REFERENCES optimiser.variants(variant_id);

-- ============================================================
-- optimiser.users
-- Separate auth from portal — own user base for the optimiser.
-- ============================================================
CREATE TABLE optimiser.users (
    user_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           UUID NOT NULL REFERENCES core.tenants(tenant_id),

    email               TEXT NOT NULL,
    password_hash       TEXT NOT NULL,
    full_name           TEXT,
    role                TEXT NOT NULL DEFAULT 'user' CHECK (role IN ('admin', 'user')),
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_optimiser_user_email UNIQUE (tenant_id, email)
);

CREATE INDEX idx_optimiser_users_tenant ON optimiser.users (tenant_id);
