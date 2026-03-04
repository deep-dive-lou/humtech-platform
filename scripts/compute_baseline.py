"""
Compute CRM baseline metrics for a tenant from engine.lead_events.

Reads the backfilled lead data and computes funnel conversion rates,
win rate, lead volume, and deal value. Writes a snapshot to engine.baselines.

Idempotent — re-running updates the existing row.

Usage:
    TENANT_SLUG=resg python scripts/compute_baseline.py
    TENANT_SLUG=resg LABEL=crm_pre_humtech python scripts/compute_baseline.py
"""
import asyncio
import asyncpg
import json
import os
import sys
from datetime import datetime, timezone

DB = os.getenv("DATABASE_URL")
TENANT_SLUG = os.getenv("TENANT_SLUG", "").strip()
LABEL_OVERRIDE = os.getenv("LABEL", "").strip()

MIN_LEADS_WARN = 30
MIN_DAYS_WARN = 60


async def main() -> None:
    if not TENANT_SLUG:
        print("ERROR: TENANT_SLUG is required.")
        sys.exit(1)

    conn = await asyncpg.connect(DB)

    try:
        # ------------------------------------------------------------------ #
        # Load tenant
        # ------------------------------------------------------------------ #
        row = await conn.fetchrow(
            "SELECT tenant_id::text, settings FROM core.tenants WHERE tenant_slug = $1",
            TENANT_SLUG,
        )
        if not row:
            print(f"ERROR: Tenant '{TENANT_SLUG}' not found.")
            sys.exit(1)

        tenant_id = row["tenant_id"]
        raw_settings = row["settings"]
        settings = raw_settings if isinstance(raw_settings, dict) else json.loads(raw_settings or "{}")
        crm_provider = settings.get("crm_provider", "ghl")

        label = LABEL_OVERRIDE or f"{TENANT_SLUG}_crm_pre_humtech"
        print(f"Tenant: {TENANT_SLUG} ({tenant_id})")
        print(f"Label:  {label}")

        # ------------------------------------------------------------------ #
        # Load stage mappings (ordered, deduplicated by canonical stage)
        # ------------------------------------------------------------------ #
        stage_rows = await conn.fetch("""
            SELECT canonical_stage, MIN(stage_order) as stage_order
            FROM engine.stage_mappings
            WHERE tenant_id = $1::uuid
              AND provider = $2
              AND is_active = TRUE
            GROUP BY canonical_stage
            ORDER BY MIN(stage_order)
        """, tenant_id, crm_provider)

        if not stage_rows:
            print("ERROR: No stage mappings found. Run onboard_client.py first.")
            sys.exit(1)

        stages = [(r["canonical_stage"], r["stage_order"]) for r in stage_rows]
        # Terminal stages are exits, not funnel progression
        terminal_stages = {"won", "lost", "rejected"}
        funnel_stages = [s for s in stages if s[0] not in terminal_stages]
        print(f"Stages: {[s[0] for s in stages]}")

        # ------------------------------------------------------------------ #
        # Load leads summary (CTE deduplicates many-to-one stage mappings)
        # ------------------------------------------------------------------ #
        leads = await conn.fetch("""
            WITH canonical_orders AS (
                SELECT canonical_stage, MIN(stage_order) as stage_order
                FROM engine.stage_mappings
                WHERE tenant_id = $1::uuid AND provider = $2 AND is_active = TRUE
                GROUP BY canonical_stage
            )
            SELECT
                l.lead_id,
                l.current_stage,
                l.is_open,
                l.lead_value,
                co.stage_order as current_stage_order
            FROM engine.leads l
            LEFT JOIN canonical_orders co
                ON co.canonical_stage = l.current_stage
            WHERE l.tenant_id = $1::uuid
              AND l.provider = $2
        """, tenant_id, crm_provider)

        if not leads:
            print("ERROR: No leads found. Run backfill_crm.py first.")
            sys.exit(1)

        total_leads = len(leads)
        total_won = sum(1 for l in leads if l["current_stage"] == "won")
        total_lost = sum(1 for l in leads if l["current_stage"] == "lost")
        total_rejected = sum(1 for l in leads if l["current_stage"] == "rejected")
        deal_values = [float(l["lead_value"]) for l in leads if l["lead_value"] is not None]
        avg_deal_value = round(sum(deal_values) / len(deal_values), 2) if deal_values else None
        win_rate = round(total_won / total_leads, 4) if total_leads > 0 else 0.0
        rejection_rate = round(total_rejected / total_leads, 4) if total_leads > 0 else 0.0

        # ------------------------------------------------------------------ #
        # Compute funnel: for each non-terminal stage, count leads at or
        # beyond that stage. Lost/rejected leads are exits — they don't count
        # as having progressed. Won leads count as having reached the end.
        # ------------------------------------------------------------------ #
        terminal_losses = {"lost", "rejected"}
        stage_funnel: dict[str, int] = {}
        for stage_name, stage_order in funnel_stages:
            count = sum(
                1 for l in leads
                if l["current_stage"] not in terminal_losses
                and (l["current_stage_order"] or 0) >= stage_order
            )
            stage_funnel[stage_name] = count
        # Add terminal counts separately
        stage_funnel["won"] = total_won
        stage_funnel["rejected"] = total_rejected
        stage_funnel["lost"] = total_lost

        # Conversion rates between consecutive funnel stages
        stage_conversion_rates: dict[str, float] = {}
        for i in range(len(funnel_stages) - 1):
            from_stage, _ = funnel_stages[i]
            to_stage, _ = funnel_stages[i + 1]
            from_count = stage_funnel.get(from_stage, 0)
            to_count = stage_funnel.get(to_stage, 0)
            key = f"{from_stage}_to_{to_stage}"
            stage_conversion_rates[key] = (
                round(to_count / from_count, 4) if from_count > 0 else 0.0
            )

        # ------------------------------------------------------------------ #
        # Period detection from lead_events
        # ------------------------------------------------------------------ #
        period_row = await conn.fetchrow("""
            SELECT
                MIN(occurred_at) as period_start,
                MAX(occurred_at) as period_end
            FROM engine.lead_events
            WHERE tenant_id = $1::uuid
              AND source = 'ghl_backfill'
        """, tenant_id)

        period_start = period_row["period_start"]
        period_end = period_row["period_end"] or datetime.now(timezone.utc)
        period_days = max(1, (period_end - period_start).days) if period_start else 1
        lead_volume_per_month = round(total_leads / period_days * 30, 2)

        # ------------------------------------------------------------------ #
        # Data quality warnings
        # ------------------------------------------------------------------ #
        if total_leads < MIN_LEADS_WARN:
            print(f"WARNING: Only {total_leads} leads — baseline may not be statistically reliable (recommend 30+).")
        if period_days < MIN_DAYS_WARN:
            print(f"WARNING: Only {period_days} days of data — baseline may not be representative (recommend 60+).")

        # ------------------------------------------------------------------ #
        # Build metrics payload
        # ------------------------------------------------------------------ #
        metrics = {
            "total_leads": total_leads,
            "total_won": total_won,
            "total_lost": total_lost,
            "total_rejected": total_rejected,
            "win_rate": win_rate,
            "rejection_rate": rejection_rate,
            "avg_deal_value_gbp": avg_deal_value,
            "stage_funnel": stage_funnel,
            "stage_conversion_rates": stage_conversion_rates,
            "lead_volume_per_month": lead_volume_per_month,
            "period_days": period_days,
            "crm_provider": crm_provider,
        }

        # ------------------------------------------------------------------ #
        # Write to engine.baselines
        # ------------------------------------------------------------------ #
        await conn.execute("""
            INSERT INTO engine.baselines (tenant_id, label, period_start, period_end, metrics)
            VALUES ($1::uuid, $2, $3, $4, $5::jsonb)
            ON CONFLICT (tenant_id, label)
            DO UPDATE SET
                period_start = EXCLUDED.period_start,
                period_end   = EXCLUDED.period_end,
                metrics      = EXCLUDED.metrics
        """,
            tenant_id,
            label,
            period_start,
            period_end,
            json.dumps(metrics),
        )

        print(f"\nBaseline '{label}' written to engine.baselines.")
        print(f"\n--- Metrics ---")
        print(f"  Total leads:           {total_leads}")
        print(f"  Won:                   {total_won} ({win_rate*100:.1f}%)")
        print(f"  Lost:                  {total_lost}")
        print(f"  Rejected:              {total_rejected} ({rejection_rate*100:.1f}%)")
        print(f"  Avg deal value:        £{avg_deal_value:,.2f}" if avg_deal_value else "  Avg deal value:        n/a")
        print(f"  Lead volume/month:     {lead_volume_per_month}")
        print(f"  Period:                {period_days} days")
        print(f"\n  Stage funnel:")
        for stage, count in stage_funnel.items():
            print(f"    {stage:<30} {count}")
        print(f"\n  Conversion rates:")
        for key, rate in stage_conversion_rates.items():
            print(f"    {key:<45} {rate*100:.1f}%")

    finally:
        await conn.close()


asyncio.run(main())
