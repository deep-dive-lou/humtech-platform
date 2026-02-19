-- 002_create_engine_schema.sql
-- Revenue Event Engine schema
-- Zero dependencies on bot schema; references core.tenants only.

CREATE SCHEMA IF NOT EXISTS engine;

-- ============================================================
-- engine.leads
-- ============================================================
CREATE TABLE engine.leads (
    lead_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           UUID NOT NULL REFERENCES core.tenants(tenant_id),

    -- External identity (provider-agnostic)
    provider            TEXT NOT NULL,
    external_id         TEXT NOT NULL,

    -- Contact reference (NOT a FK to bot.contacts)
    contact_provider    TEXT,
    contact_external_id TEXT,

    -- Lead data
    name                TEXT,
    pipeline_name       TEXT,
    current_stage       TEXT NOT NULL DEFAULT 'lead_created',
    raw_stage           TEXT,
    source              TEXT,
    lead_value          NUMERIC(12,2),
    currency            TEXT DEFAULT 'GBP',

    -- Lifecycle
    is_open             BOOLEAN NOT NULL DEFAULT TRUE,
    won_at              TIMESTAMPTZ,
    lost_at             TIMESTAMPTZ,
    closed_reason       TEXT,

    -- Metadata
    metadata            JSONB NOT NULL DEFAULT '{}',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_leads_tenant_provider_ext UNIQUE (tenant_id, provider, external_id)
);

CREATE INDEX idx_leads_tenant           ON engine.leads (tenant_id);
CREATE INDEX idx_leads_tenant_contact   ON engine.leads (tenant_id, contact_provider, contact_external_id);
CREATE INDEX idx_leads_tenant_stage     ON engine.leads (tenant_id, current_stage);
CREATE INDEX idx_leads_tenant_open      ON engine.leads (tenant_id, is_open) WHERE is_open = TRUE;
CREATE INDEX idx_leads_created_at       ON engine.leads (tenant_id, created_at);

-- ============================================================
-- engine.lead_events
-- ============================================================
CREATE TABLE engine.lead_events (
    event_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           UUID NOT NULL REFERENCES core.tenants(tenant_id),
    lead_id             UUID NOT NULL REFERENCES engine.leads(lead_id),

    -- Event classification
    event_type          TEXT NOT NULL,
    canonical_stage     TEXT,

    -- Stage transition
    from_stage          TEXT,
    to_stage            TEXT,

    -- Event source
    source              TEXT NOT NULL,
    source_event_id     TEXT,
    actor               TEXT,

    -- Financial data
    amount              NUMERIC(12,2),
    currency            TEXT,

    -- Event payload
    payload             JSONB NOT NULL DEFAULT '{}',

    -- Timestamps
    occurred_at         TIMESTAMPTZ NOT NULL,
    recorded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Idempotency
    CONSTRAINT uq_lead_events_idempotent UNIQUE (tenant_id, lead_id, source, source_event_id)
);

CREATE INDEX idx_lead_events_lead       ON engine.lead_events (lead_id, occurred_at);
CREATE INDEX idx_lead_events_tenant     ON engine.lead_events (tenant_id);
CREATE INDEX idx_lead_events_type       ON engine.lead_events (tenant_id, event_type);
CREATE INDEX idx_lead_events_occurred   ON engine.lead_events (tenant_id, occurred_at);
CREATE INDEX idx_lead_events_source     ON engine.lead_events (tenant_id, source);

-- ============================================================
-- engine.stage_mappings
-- ============================================================
CREATE TABLE engine.stage_mappings (
    mapping_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           UUID NOT NULL REFERENCES core.tenants(tenant_id),

    -- Source identification
    provider            TEXT NOT NULL,
    pipeline_id         TEXT,
    pipeline_name       TEXT,

    -- Mapping
    raw_stage           TEXT NOT NULL,
    canonical_stage     TEXT NOT NULL,
    stage_order         INT NOT NULL,

    -- Control
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_stage_mapping UNIQUE (tenant_id, provider, pipeline_id, raw_stage)
);

CREATE INDEX idx_stage_mappings_lookup ON engine.stage_mappings (tenant_id, provider, raw_stage) WHERE is_active = TRUE;

-- ============================================================
-- engine.baselines
-- ============================================================
CREATE TABLE engine.baselines (
    baseline_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           UUID NOT NULL REFERENCES core.tenants(tenant_id),

    -- Baseline definition
    label               TEXT NOT NULL,
    period_start        TIMESTAMPTZ NOT NULL,
    period_end          TIMESTAMPTZ NOT NULL,

    -- Metrics snapshot
    metrics             JSONB NOT NULL,

    -- Control
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_baseline_label UNIQUE (tenant_id, label)
);

CREATE INDEX idx_baselines_tenant ON engine.baselines (tenant_id) WHERE is_active = TRUE;
