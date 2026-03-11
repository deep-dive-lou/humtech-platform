"""Fetch financial data from Xero or QuickBooks and store as a baseline.

Replaces manual set_financial_baseline.py with automated pull from accounting software.

Usage:
    DATABASE_URL=... TENANT_SLUG=resg python scripts/fetch_financial_snapshot.py

Optional env vars:
    PERIOD_MONTHS  — how many months back to pull (default 12)
    BASELINE_LABEL — override label (default: {slug}_financial_pre_humtech)
    PROVIDER       — force xero or quickbooks (default: auto-detect from credentials)
"""
import asyncio
import json
import os
import sys
from datetime import date, datetime, timezone
from uuid import UUID

import asyncpg
from dotenv import load_dotenv

load_dotenv()

# Add parent to path for app imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from app.engine.providers.financial_base import FinancialSnapshot
from app.engine.providers.xero_financial import XeroFinancialAdapter
from app.engine.providers.qbo_financial import QBOFinancialAdapter
from app.utils.crypto import decrypt_credentials


async def main():
    database_url = os.environ["DATABASE_URL"]
    tenant_slug = os.getenv("TENANT_SLUG", "resg")
    period_months = int(os.getenv("PERIOD_MONTHS", "12"))
    forced_provider = os.getenv("PROVIDER")

    conn = await asyncpg.connect(database_url)

    try:
        # Resolve tenant
        row = await conn.fetchrow(
            "SELECT tenant_id FROM core.tenants WHERE slug = $1", tenant_slug
        )
        if not row:
            print(f"ERROR: Tenant '{tenant_slug}' not found")
            sys.exit(1)

        tenant_id = str(row["tenant_id"])
        print(f"Tenant: {tenant_slug} ({tenant_id})")

        # Detect provider from credentials
        provider = forced_provider
        if not provider:
            for p in ("xero", "quickbooks"):
                cred_row = await conn.fetchval(
                    "SELECT credentials FROM core.tenant_credentials WHERE tenant_id = $1::uuid AND provider = $2",
                    tenant_id, p,
                )
                if cred_row:
                    provider = p
                    break

        if not provider:
            print("ERROR: No financial credentials found. Connect Xero or QuickBooks first.")
            print("  Visit: https://api.humtech.ai/financial/connect/xero?tenant_id=" + tenant_id)
            sys.exit(1)

        print(f"Provider: {provider}")

        # Calculate period
        today = date.today()
        period_end = date(today.year, today.month, 1)  # First of current month
        if period_months <= 12:
            start_year = period_end.year - 1 if period_end.month <= period_months - (period_end.month - 1) else period_end.year
            start_month = period_end.month - period_months
            if start_month <= 0:
                start_month += 12
                start_year -= 1
            period_start = date(start_year, start_month, 1)
        else:
            # For > 12 months, go back that many months
            total_months_back = period_months
            start_year = period_end.year
            start_month = period_end.month - total_months_back
            while start_month <= 0:
                start_month += 12
                start_year -= 1
            period_start = date(start_year, start_month, 1)

        print(f"Period: {period_start} → {period_end} ({period_months} months)")

        # Instantiate adapter and fetch
        if provider == "xero":
            adapter = XeroFinancialAdapter(conn, tenant_id)
        else:
            adapter = QBOFinancialAdapter(conn, tenant_id)

        snapshot: FinancialSnapshot = await adapter.fetch_revenue(period_start, period_end)

        print(f"\nResults:")
        print(f"  Annual revenue:  £{snapshot.annual_revenue_gbp:,.2f}")
        print(f"  Monthly average: £{snapshot.monthly_avg_gbp:,.2f}")
        print(f"  Months pulled:   {len(snapshot.monthly_revenue)}")
        print(f"  Method:          {snapshot.accounting_method}")

        # Build metrics JSONB
        monthly_breakdown = {m.month: m.revenue_gbp for m in snapshot.monthly_revenue}
        label = os.getenv("BASELINE_LABEL", f"{tenant_slug}_financial_pre_humtech")

        metrics = {
            "annual_revenue_gbp": snapshot.annual_revenue_gbp,
            "monthly_revenue_gbp": snapshot.monthly_avg_gbp,  # avg monthly
            "revenue_monthly_avg": snapshot.monthly_avg_gbp,   # key used by Revenue Delta dashboard
            "monthly_breakdown": monthly_breakdown,
            "accounting_method": snapshot.accounting_method,
            "source": provider,
            "period_months": len(snapshot.monthly_revenue),
            "pulled_at": snapshot.pulled_at.isoformat(),
        }

        # Upsert baseline
        await conn.execute("""
            INSERT INTO engine.baselines (tenant_id, label, period_start, period_end, metrics, is_active)
            VALUES ($1::uuid, $2, $3, $4, $5::jsonb, TRUE)
            ON CONFLICT (tenant_id, label)
            DO UPDATE SET
                period_start = EXCLUDED.period_start,
                period_end = EXCLUDED.period_end,
                metrics = EXCLUDED.metrics,
                is_active = EXCLUDED.is_active,
                created_at = now()
        """, tenant_id, label, period_start, period_end, json.dumps(metrics))

        print(f"\n  Baseline saved: {label}")
        print(f"  Revenue Delta dashboard will auto-populate with this data.")

        # Print monthly breakdown
        if monthly_breakdown:
            print(f"\n  Monthly breakdown:")
            for month, rev in sorted(monthly_breakdown.items()):
                print(f"    {month}: £{rev:,.2f}")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
