"""
Onboard a new client onto the HumTech platform.

Creates the tenant record, stores GHL credentials, and seeds stage mappings.
Idempotent — safe to re-run. Run once per client before backfill.

Usage:
    TENANT_SLUG=resg \\
    TENANT_NAME="RESG" \\
    CRM_PROVIDER=ghl \\
    GHL_ACCESS_TOKEN=<token> \\
    GHL_LOCATION_ID=<location_id> \\
    PIPELINE_STAGES='["New Lead","Appointment Booked","Appointment Completed","Proposal Sent","Won","Lost"]' \\
    python scripts/onboard_client.py
"""
import asyncio
import asyncpg
import json
import os
import sys
from datetime import datetime, timezone, timedelta

from app.utils.crypto import encrypt_credentials

DB = os.getenv("DATABASE_URL")
TENANT_SLUG = os.getenv("TENANT_SLUG", "").strip()
TENANT_NAME = os.getenv("TENANT_NAME", "").strip()
CRM_PROVIDER = os.getenv("CRM_PROVIDER", "ghl").strip().lower()
GHL_ACCESS_TOKEN = os.getenv("GHL_ACCESS_TOKEN", "").strip()
GHL_LOCATION_ID = os.getenv("GHL_LOCATION_ID", "").strip()
PIPELINE_STAGES_RAW = os.getenv("PIPELINE_STAGES", "").strip()
EXPIRY_DAYS = 365

TERMINAL_WON = {"won"}
TERMINAL_LOST = {"lost", "abandoned"}


def _to_canonical(raw: str) -> str:
    lower = raw.lower().strip()
    if lower in TERMINAL_WON:
        return "won"
    if lower in TERMINAL_LOST:
        return "lost"
    return lower.replace(" ", "_")


async def main() -> None:
    if not TENANT_SLUG:
        print("ERROR: TENANT_SLUG is required.")
        sys.exit(1)
    if not TENANT_NAME:
        print("ERROR: TENANT_NAME is required.")
        sys.exit(1)
    if not PIPELINE_STAGES_RAW:
        print("ERROR: PIPELINE_STAGES is required.")
        sys.exit(1)

    try:
        stage_names: list[str] = json.loads(PIPELINE_STAGES_RAW)
    except json.JSONDecodeError as e:
        print(f"ERROR: PIPELINE_STAGES must be a valid JSON array: {e}")
        sys.exit(1)

    if not isinstance(stage_names, list) or not stage_names:
        print("ERROR: PIPELINE_STAGES must be a non-empty JSON array of strings.")
        sys.exit(1)

    conn = await asyncpg.connect(DB)

    try:
        # ------------------------------------------------------------------ #
        # Step 1: Create (or update) tenant
        # ------------------------------------------------------------------ #
        new_settings = json.dumps({"crm_provider": CRM_PROVIDER})

        # INSERT tenant — merge settings if already exists
        await conn.execute("""
            INSERT INTO core.tenants (tenant_slug, name, settings)
            VALUES ($1, $2, $3::jsonb)
            ON CONFLICT (tenant_slug)
            DO UPDATE SET
                name     = EXCLUDED.name,
                settings = core.tenants.settings || EXCLUDED.settings
        """, TENANT_SLUG, TENANT_NAME, new_settings)

        tenant_id = await conn.fetchval(
            "SELECT tenant_id::text FROM core.tenants WHERE tenant_slug = $1",
            TENANT_SLUG,
        )
        print(f"Tenant: {TENANT_NAME} ({TENANT_SLUG}) — id: {tenant_id}")

        # ------------------------------------------------------------------ #
        # Step 2: Store credentials
        # ------------------------------------------------------------------ #
        if CRM_PROVIDER == "ghl":
            if not GHL_ACCESS_TOKEN:
                print("WARNING: GHL_ACCESS_TOKEN not set — skipping credential storage.")
                print("         Run store_ghl_token.py separately to add credentials.")
            else:
                expires_at = (
                    datetime.now(timezone.utc) + timedelta(days=EXPIRY_DAYS)
                ).isoformat()
                creds = {
                    "access_token": GHL_ACCESS_TOKEN,
                    "expires_at": expires_at,
                    "location_id": GHL_LOCATION_ID or None,
                }
                encrypted = encrypt_credentials(creds)
                await conn.execute("""
                    INSERT INTO core.tenant_credentials (tenant_id, provider, credentials, updated_at)
                    VALUES ($1::uuid, 'ghl', $2::bytea, now())
                    ON CONFLICT (tenant_id, provider)
                    DO UPDATE SET credentials = EXCLUDED.credentials, updated_at = now()
                """, tenant_id, encrypted)
                print(f"GHL credentials stored (expires {expires_at[:10]})")

        # ------------------------------------------------------------------ #
        # Step 3: Seed stage mappings (delete + re-insert for clean idempotency)
        # ------------------------------------------------------------------ #
        deleted = await conn.fetchval(
            "DELETE FROM engine.stage_mappings WHERE tenant_id = $1::uuid AND provider = $2 RETURNING count(*)",
            tenant_id, CRM_PROVIDER,
        )
        # fetchval on DELETE ... RETURNING count(*) doesn't work directly — use execute + rowcount
        await conn.execute(
            "DELETE FROM engine.stage_mappings WHERE tenant_id = $1::uuid AND provider = $2",
            tenant_id, CRM_PROVIDER,
        )

        print(f"Seeding {len(stage_names)} stage mappings for provider '{CRM_PROVIDER}':")
        for i, raw_stage in enumerate(stage_names):
            canonical = _to_canonical(raw_stage)
            stage_order = i + 1
            await conn.execute("""
                INSERT INTO engine.stage_mappings (
                    tenant_id, provider, pipeline_id, pipeline_name,
                    raw_stage, canonical_stage, stage_order
                )
                VALUES ($1::uuid, $2, NULL, NULL, $3, $4, $5)
            """, tenant_id, CRM_PROVIDER, raw_stage, canonical, stage_order)
            print(f"  {stage_order}. '{raw_stage}' -> '{canonical}'")

        print(f"\nClient '{TENANT_SLUG}' onboarded. Run backfill_crm.py next.")

    finally:
        await conn.close()


asyncio.run(main())
