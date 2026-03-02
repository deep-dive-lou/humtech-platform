"""
Set the financial baseline for a client (V1: manual input).

Captures pre-HumTech financial performance from figures provided
by the client in the onboarding meeting.

V1 = manual env var input. Future = Xero/QuickBooks adapter.

Usage:
    TENANT_SLUG=resg \\
    ANNUAL_REVENUE_GBP=850000 \\
    NEW_CUSTOMERS_PER_MONTH=8 \\
    AVG_REVENUE_PER_CUSTOMER_GBP=8850 \\
    CHURN_RATE_MONTHLY=0.03 \\
    PERIOD_MONTHS=12 \\
    python scripts/set_financial_baseline.py
"""
import asyncio
import asyncpg
import json
import os
import sys
from datetime import datetime, timezone, timedelta

DB = os.getenv("DATABASE_URL")
TENANT_SLUG = os.getenv("TENANT_SLUG", "").strip()
LABEL_OVERRIDE = os.getenv("LABEL", "").strip()

ANNUAL_REVENUE_GBP = os.getenv("ANNUAL_REVENUE_GBP", "").strip()
NEW_CUSTOMERS_PER_MONTH = os.getenv("NEW_CUSTOMERS_PER_MONTH", "").strip()
AVG_REVENUE_PER_CUSTOMER_GBP = os.getenv("AVG_REVENUE_PER_CUSTOMER_GBP", "").strip()
CHURN_RATE_MONTHLY = os.getenv("CHURN_RATE_MONTHLY", "").strip()
PERIOD_MONTHS = int(os.getenv("PERIOD_MONTHS", "12"))


def _to_float(val: str, name: str) -> float:
    try:
        return float(val)
    except (ValueError, TypeError):
        print(f"ERROR: {name} must be a number (got '{val}').")
        sys.exit(1)


async def main() -> None:
    if not TENANT_SLUG:
        print("ERROR: TENANT_SLUG is required.")
        sys.exit(1)
    if not ANNUAL_REVENUE_GBP:
        print("ERROR: ANNUAL_REVENUE_GBP is required.")
        sys.exit(1)
    if not NEW_CUSTOMERS_PER_MONTH:
        print("ERROR: NEW_CUSTOMERS_PER_MONTH is required.")
        sys.exit(1)
    if not AVG_REVENUE_PER_CUSTOMER_GBP:
        print("ERROR: AVG_REVENUE_PER_CUSTOMER_GBP is required.")
        sys.exit(1)

    annual_revenue = _to_float(ANNUAL_REVENUE_GBP, "ANNUAL_REVENUE_GBP")
    new_customers = _to_float(NEW_CUSTOMERS_PER_MONTH, "NEW_CUSTOMERS_PER_MONTH")
    avg_revenue = _to_float(AVG_REVENUE_PER_CUSTOMER_GBP, "AVG_REVENUE_PER_CUSTOMER_GBP")
    churn_rate = _to_float(CHURN_RATE_MONTHLY, "CHURN_RATE_MONTHLY") if CHURN_RATE_MONTHLY else None

    conn = await asyncpg.connect(DB)

    try:
        tenant_id = await conn.fetchval(
            "SELECT tenant_id::text FROM core.tenants WHERE tenant_slug = $1",
            TENANT_SLUG,
        )
        if not tenant_id:
            print(f"ERROR: Tenant '{TENANT_SLUG}' not found. Run onboard_client.py first.")
            sys.exit(1)

        label = LABEL_OVERRIDE or f"{TENANT_SLUG}_financial_pre_humtech"

        now = datetime.now(timezone.utc)
        period_end = now
        period_start = now - timedelta(days=PERIOD_MONTHS * 30)

        metrics: dict = {
            "annual_revenue_gbp": annual_revenue,
            "monthly_revenue_gbp": round(annual_revenue / 12, 2),
            "new_customers_per_month": new_customers,
            "avg_revenue_per_customer_gbp": avg_revenue,
            "period_months": PERIOD_MONTHS,
            "source": "manual_input",
        }
        if churn_rate is not None:
            metrics["churn_rate_monthly"] = churn_rate

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

        print(f"Financial baseline '{label}' written to engine.baselines.")
        print(f"\n--- Metrics ---")
        print(f"  Annual revenue:        £{annual_revenue:,.0f}")
        print(f"  Monthly revenue:       £{annual_revenue/12:,.0f}")
        print(f"  New customers/month:   {new_customers}")
        print(f"  Avg revenue/customer:  £{avg_revenue:,.0f}")
        if churn_rate is not None:
            print(f"  Monthly churn rate:    {churn_rate*100:.1f}%")
        print(f"  Period:                {PERIOD_MONTHS} months")
        print(f"  Source:                manual input")

    finally:
        await conn.close()


asyncio.run(main())
