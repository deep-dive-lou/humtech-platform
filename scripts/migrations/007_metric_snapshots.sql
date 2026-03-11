-- Migration 007: Create engine.metric_snapshots table
-- Time-series store for weekly/monthly metric snapshots.
-- Powers ITS, CausalImpact, and trend analysis in Phase 3+.

BEGIN;

CREATE TABLE engine.metric_snapshots (
    snapshot_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES core.tenants(tenant_id),
    period_type     TEXT NOT NULL,           -- 'weekly', 'monthly'
    period_start    TIMESTAMPTZ NOT NULL,
    period_end      TIMESTAMPTZ NOT NULL,
    metrics         JSONB NOT NULL,          -- all 12 metrics + derivatives
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_metric_snapshot UNIQUE (tenant_id, period_type, period_start)
);

CREATE INDEX idx_metric_snapshots_tenant
    ON engine.metric_snapshots (tenant_id, period_type, period_start DESC);

-- Metabase read access (humtech_ro = read-only role used by Metabase)
GRANT SELECT ON engine.metric_snapshots TO humtech_ro;

-- App user access
GRANT SELECT, INSERT, UPDATE ON engine.metric_snapshots TO humtech_bot;

COMMIT;
