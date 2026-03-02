"""
CRM backfill — pull historical opportunities and insert into engine tables.

Provider-agnostic entry point. Reads tenant's CRM provider from DB and
dispatches to the right adapter.

Idempotent — safe to re-run. Existing events are silently skipped.

Usage:
    TENANT_SLUG=resg python scripts/backfill_crm.py
"""
import asyncio
import asyncpg
import json
import logging
import os
import sys
from datetime import datetime, timezone

from app.adapters.ghl.auth import get_valid_token
from app.engine.events import resolve_stage_mapping, upsert_lead, write_lead_event

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DB = os.getenv("DATABASE_URL")
TENANT_SLUG = os.getenv("TENANT_SLUG", "").strip()


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
            print(f"ERROR: Tenant '{TENANT_SLUG}' not found. Run onboard_client.py first.")
            sys.exit(1)

        tenant_id = row["tenant_id"]
        raw_settings = row["settings"]
        settings = raw_settings if isinstance(raw_settings, dict) else json.loads(raw_settings or "{}")
        crm_provider = settings.get("crm_provider", "ghl")

        print(f"Tenant: {TENANT_SLUG} ({tenant_id}), CRM provider: {crm_provider}")

        # ------------------------------------------------------------------ #
        # Fetch opportunities via provider adapter
        # ------------------------------------------------------------------ #
        if crm_provider == "ghl":
            from app.engine.providers.ghl_backfill import fetch_all_opportunities

            token = await get_valid_token(conn, tenant_id)

            # Get location_id from credentials
            cred_row = await conn.fetchval(
                "SELECT credentials FROM core.tenant_credentials WHERE tenant_id = $1::uuid AND provider = 'ghl'",
                tenant_id,
            )
            if not cred_row:
                print("ERROR: No GHL credentials found. Run onboard_client.py with GHL_ACCESS_TOKEN.")
                sys.exit(1)

            from app.utils.crypto import decrypt_credentials
            creds = decrypt_credentials(bytes(cred_row))
            location_id = creds.get("location_id")
            if not location_id:
                print("ERROR: No location_id in GHL credentials. Run onboard_client.py with GHL_LOCATION_ID.")
                sys.exit(1)

            print(f"Fetching GHL opportunities for location {location_id}...")
            opportunities = await fetch_all_opportunities(token, location_id)

        else:
            print(f"ERROR: CRM provider '{crm_provider}' not yet supported.")
            print("       Supported providers: ghl")
            sys.exit(1)

        print(f"Fetched {len(opportunities)} opportunities from {crm_provider}.")

        # ------------------------------------------------------------------ #
        # Ingest into engine tables
        # ------------------------------------------------------------------ #
        inserted = 0
        skipped_no_mapping = 0
        skipped_duplicate = 0

        for opp in opportunities:
            # Resolve stage mapping
            canonical = await resolve_stage_mapping(
                conn,
                tenant_id=tenant_id,
                provider=crm_provider,
                raw_stage=opp.stage_name or "",
            )
            if canonical is None:
                logger.warning(
                    "No stage mapping for '%s' (opp %s) — skipping.",
                    opp.stage_name, opp.id,
                )
                skipped_no_mapping += 1
                continue

            # Determine is_open / terminal event type
            is_terminal_won = opp.status in ("won",)
            is_terminal_lost = opp.status in ("lost", "abandoned")
            is_open = not (is_terminal_won or is_terminal_lost)

            # Upsert lead
            lead_id = await upsert_lead(
                conn,
                tenant_id=tenant_id,
                provider=crm_provider,
                external_id=opp.id,
                contact_provider=crm_provider if opp.contact_id else None,
                contact_external_id=opp.contact_id,
                name=opp.name or opp.contact_name,
                current_stage=canonical,
                raw_stage=opp.stage_name,
                lead_value=opp.monetary_value,
                currency="GBP",
            )

            # Write lead_created event
            ev1 = await write_lead_event(
                conn,
                lead_id=lead_id,
                tenant_id=tenant_id,
                event_type="lead_created",
                source="ghl_backfill",
                occurred_at=opp.created_at,
                canonical_stage="new_lead",
                source_event_id=f"backfill-created-{opp.id}",
            )

            # Write terminal or stage_changed event
            if is_terminal_won:
                ev2 = await write_lead_event(
                    conn,
                    lead_id=lead_id,
                    tenant_id=tenant_id,
                    event_type="lead_won",
                    source="ghl_backfill",
                    occurred_at=opp.updated_at,
                    canonical_stage="won",
                    source_event_id=f"backfill-won-{opp.id}",
                    amount=opp.monetary_value,
                    currency="GBP",
                )
            elif is_terminal_lost:
                ev2 = await write_lead_event(
                    conn,
                    lead_id=lead_id,
                    tenant_id=tenant_id,
                    event_type="lead_lost",
                    source="ghl_backfill",
                    occurred_at=opp.updated_at,
                    canonical_stage="lost",
                    source_event_id=f"backfill-lost-{opp.id}",
                )
            else:
                ev2 = await write_lead_event(
                    conn,
                    lead_id=lead_id,
                    tenant_id=tenant_id,
                    event_type="stage_changed",
                    source="ghl_backfill",
                    occurred_at=opp.updated_at,
                    canonical_stage=canonical,
                    to_stage=canonical,
                    source_event_id=f"backfill-stage-{opp.id}",
                )

            # Count: if both events were new inserts, count as inserted; if all skipped, count duplicate
            if ev1 is None and ev2 is None:
                skipped_duplicate += 1
            else:
                inserted += 1

        print(f"\nBackfill complete:")
        print(f"  Inserted:           {inserted}")
        print(f"  Skipped (duplicate): {skipped_duplicate}")
        print(f"  Skipped (no mapping): {skipped_no_mapping}")
        print(f"\nRun compute_baseline.py next.")

    finally:
        await conn.close()


asyncio.run(main())
