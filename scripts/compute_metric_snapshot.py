"""
Compute the 12 core metrics from the metrics framework and write them to
engine.metric_snapshots for a given tenant and period.

Usage:
    TENANT_SLUG=resg python scripts/compute_metric_snapshot.py

Environment variables:
    DATABASE_URL   — required
    TENANT_SLUG    — tenant slug to compute for (default: resg)
    PERIOD_TYPE    — monthly | weekly (default: monthly)
    PERIOD_START   — ISO timestamp override, e.g. 2025-01-01T00:00:00+00:00
    PERIOD_END     — ISO timestamp override, e.g. 2025-02-01T00:00:00+00:00

If PERIOD_START/PERIOD_END are not provided the script computes the most
recent complete period (last complete calendar month or last complete ISO week).

Idempotent — re-running upserts the row using the UNIQUE constraint on
(tenant_id, period_type, period_start).
"""
import asyncio
import asyncpg
import json
import os
import sys
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
load_dotenv()


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DB = os.getenv("DATABASE_URL")
TENANT_SLUG = os.getenv("TENANT_SLUG", "resg").strip()
PERIOD_TYPE = os.getenv("PERIOD_TYPE", "monthly").strip().lower()
PERIOD_START_OVERRIDE = os.getenv("PERIOD_START", "").strip()
PERIOD_END_OVERRIDE = os.getenv("PERIOD_END", "").strip()


# ---------------------------------------------------------------------------
# Period helpers
# ---------------------------------------------------------------------------

def last_complete_month() -> tuple[datetime, datetime]:
    """Return (period_start, period_end) for the most recent complete calendar month."""
    now = datetime.now(timezone.utc)
    # First day of current month
    first_of_current = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    # period_end = start of current month (exclusive upper bound → last moment of prev month)
    period_end = first_of_current
    # period_start = first day of previous month
    if first_of_current.month == 1:
        period_start = first_of_current.replace(year=first_of_current.year - 1, month=12)
    else:
        period_start = first_of_current.replace(month=first_of_current.month - 1)
    return period_start, period_end


def last_complete_week() -> tuple[datetime, datetime]:
    """Return (period_start, period_end) for the most recent complete ISO week (Mon–Sun)."""
    now = datetime.now(timezone.utc)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    # How many days since last Monday?
    days_since_monday = today.weekday()  # 0=Mon
    # Start of current week
    start_of_current_week = today - timedelta(days=days_since_monday)
    period_end = start_of_current_week
    period_start = start_of_current_week - timedelta(days=7)
    return period_start, period_end


def resolve_period() -> tuple[datetime, datetime]:
    if PERIOD_START_OVERRIDE and PERIOD_END_OVERRIDE:
        ps = datetime.fromisoformat(PERIOD_START_OVERRIDE)
        pe = datetime.fromisoformat(PERIOD_END_OVERRIDE)
        if ps.tzinfo is None:
            ps = ps.replace(tzinfo=timezone.utc)
        if pe.tzinfo is None:
            pe = pe.replace(tzinfo=timezone.utc)
        return ps, pe
    if PERIOD_TYPE == "weekly":
        return last_complete_week()
    return last_complete_month()


# ---------------------------------------------------------------------------
# Individual metric queries
# ---------------------------------------------------------------------------

async def q_lead_volume_weekly(conn, tenant_id: str, ps: datetime, pe: datetime, weeks: float) -> float | None:
    """lead_created events in period / weeks in period."""
    count = await conn.fetchval("""
        SELECT COUNT(*)
        FROM engine.lead_events
        WHERE tenant_id = $1::uuid
          AND event_type = 'lead_created'
          AND occurred_at >= $2 AND occurred_at < $3
    """, tenant_id, ps, pe)
    if weeks <= 0:
        return None
    return round(count / weeks, 4)


async def q_speed_to_first_contact_median_min(conn, tenant_id: str, ps: datetime, pe: datetime) -> float | None:
    """
    Median minutes from lead_created to first first_contact event per lead,
    for leads created in the period.
    """
    val = await conn.fetchval("""
        WITH created AS (
            SELECT lead_id, occurred_at AS created_at
            FROM engine.lead_events
            WHERE tenant_id = $1::uuid
              AND event_type = 'lead_created'
              AND occurred_at >= $2 AND occurred_at < $3
        ),
        first_contacts AS (
            SELECT lead_id, MIN(occurred_at) AS contacted_at
            FROM engine.lead_events
            WHERE tenant_id = $1::uuid
              AND event_type = 'first_contact'
            GROUP BY lead_id
        ),
        gaps AS (
            SELECT
                EXTRACT(EPOCH FROM (fc.contacted_at - c.created_at)) / 60.0 AS minutes
            FROM created c
            JOIN first_contacts fc ON fc.lead_id = c.lead_id
            WHERE fc.contacted_at >= c.created_at
        )
        SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY minutes)
        FROM gaps
    """, tenant_id, ps, pe)
    return round(float(val), 4) if val is not None else None


async def q_lead_to_qualified_rate(conn, tenant_id: str, ps: datetime, pe: datetime) -> float | None:
    """Leads reaching lead_qualified / leads created in period."""
    created = await conn.fetchval("""
        SELECT COUNT(DISTINCT lead_id)
        FROM engine.lead_events
        WHERE tenant_id = $1::uuid
          AND event_type = 'lead_created'
          AND occurred_at >= $2 AND occurred_at < $3
    """, tenant_id, ps, pe)
    if not created:
        return None
    qualified = await conn.fetchval("""
        SELECT COUNT(DISTINCT e.lead_id)
        FROM engine.lead_events e
        WHERE e.tenant_id = $1::uuid
          AND e.canonical_stage = 'lead_qualified'
          AND e.lead_id IN (
              SELECT lead_id FROM engine.lead_events
              WHERE tenant_id = $1::uuid
                AND event_type = 'lead_created'
                AND occurred_at >= $2 AND occurred_at < $3
          )
    """, tenant_id, ps, pe)
    return round(qualified / created, 4)


async def q_qualified_to_booked_rate(conn, tenant_id: str, ps: datetime, pe: datetime) -> float | None:
    """Leads reaching appointment_booked / leads reaching lead_qualified (period cohort)."""
    qualified = await conn.fetchval("""
        SELECT COUNT(DISTINCT lead_id)
        FROM engine.lead_events
        WHERE tenant_id = $1::uuid
          AND canonical_stage = 'lead_qualified'
          AND occurred_at >= $2 AND occurred_at < $3
    """, tenant_id, ps, pe)
    if not qualified:
        return None
    booked = await conn.fetchval("""
        SELECT COUNT(DISTINCT e.lead_id)
        FROM engine.lead_events e
        WHERE e.tenant_id = $1::uuid
          AND e.canonical_stage = 'appointment_booked'
          AND e.lead_id IN (
              SELECT lead_id FROM engine.lead_events
              WHERE tenant_id = $1::uuid
                AND canonical_stage = 'lead_qualified'
                AND occurred_at >= $2 AND occurred_at < $3
          )
    """, tenant_id, ps, pe)
    return round(booked / qualified, 4)


async def q_show_rate(conn, tenant_id: str, ps: datetime, pe: datetime) -> float | None:
    """appointment_completed / appointment_booked events in period."""
    booked = await conn.fetchval("""
        SELECT COUNT(*)
        FROM engine.lead_events
        WHERE tenant_id = $1::uuid
          AND event_type = 'appointment_booked'
          AND occurred_at >= $2 AND occurred_at < $3
    """, tenant_id, ps, pe)
    if not booked:
        return None
    completed = await conn.fetchval("""
        SELECT COUNT(*)
        FROM engine.lead_events
        WHERE tenant_id = $1::uuid
          AND event_type = 'appointment_completed'
          AND occurred_at >= $2 AND occurred_at < $3
    """, tenant_id, ps, pe)
    return round(completed / booked, 4)


async def q_show_to_proposal_rate(conn, tenant_id: str, ps: datetime, pe: datetime) -> float | None:
    """proposal_sent events / appointment_completed events in period."""
    completed = await conn.fetchval("""
        SELECT COUNT(*)
        FROM engine.lead_events
        WHERE tenant_id = $1::uuid
          AND event_type = 'appointment_completed'
          AND occurred_at >= $2 AND occurred_at < $3
    """, tenant_id, ps, pe)
    if not completed:
        return None
    proposals = await conn.fetchval("""
        SELECT COUNT(*)
        FROM engine.lead_events
        WHERE tenant_id = $1::uuid
          AND event_type = 'proposal_sent'
          AND occurred_at >= $2 AND occurred_at < $3
    """, tenant_id, ps, pe)
    return round(proposals / completed, 4)


async def q_close_rate(conn, tenant_id: str, ps: datetime, pe: datetime) -> float | None:
    """lead_won events / proposal_sent events in period."""
    proposals = await conn.fetchval("""
        SELECT COUNT(*)
        FROM engine.lead_events
        WHERE tenant_id = $1::uuid
          AND event_type = 'proposal_sent'
          AND occurred_at >= $2 AND occurred_at < $3
    """, tenant_id, ps, pe)
    if not proposals:
        return None
    won = await conn.fetchval("""
        SELECT COUNT(*)
        FROM engine.lead_events
        WHERE tenant_id = $1::uuid
          AND event_type = 'lead_won'
          AND occurred_at >= $2 AND occurred_at < $3
    """, tenant_id, ps, pe)
    return round(won / proposals, 4)


async def q_avg_deal_value_gbp(conn, tenant_id: str, ps: datetime, pe: datetime) -> float | None:
    """avg(lead_value) for leads won in the period."""
    val = await conn.fetchval("""
        SELECT AVG(l.lead_value)
        FROM engine.leads l
        WHERE l.tenant_id = $1::uuid
          AND l.won_at >= $2 AND l.won_at < $3
          AND l.lead_value IS NOT NULL
    """, tenant_id, ps, pe)
    return round(float(val), 2) if val is not None else None


async def q_sales_cycle_median_days(conn, tenant_id: str, ps: datetime, pe: datetime) -> float | None:
    """Median days from lead_created to lead_won for leads won in the period."""
    val = await conn.fetchval("""
        WITH won_leads AS (
            SELECT lead_id, occurred_at AS won_at
            FROM engine.lead_events
            WHERE tenant_id = $1::uuid
              AND event_type = 'lead_won'
              AND occurred_at >= $2 AND occurred_at < $3
        ),
        created AS (
            SELECT lead_id, MIN(occurred_at) AS created_at
            FROM engine.lead_events
            WHERE tenant_id = $1::uuid
              AND event_type = 'lead_created'
            GROUP BY lead_id
        ),
        cycles AS (
            SELECT
                EXTRACT(EPOCH FROM (w.won_at - c.created_at)) / 86400.0 AS days
            FROM won_leads w
            JOIN created c ON c.lead_id = w.lead_id
            WHERE w.won_at >= c.created_at
        )
        SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY days)
        FROM cycles
    """, tenant_id, ps, pe)
    return round(float(val), 4) if val is not None else None


async def q_revenue_per_lead_gbp(
    conn,
    tenant_id: str,
    ps: datetime,
    pe: datetime,
    total_leads: int,
) -> float | None:
    """Total won value / total leads created in the period."""
    if not total_leads:
        return None
    total_won_value = await conn.fetchval("""
        SELECT COALESCE(SUM(l.lead_value), 0)
        FROM engine.leads l
        WHERE l.tenant_id = $1::uuid
          AND l.won_at >= $2 AND l.won_at < $3
          AND l.lead_value IS NOT NULL
    """, tenant_id, ps, pe)
    if total_won_value is None:
        return None
    return round(float(total_won_value) / total_leads, 2)


async def q_pipeline_value_gbp(conn, tenant_id: str, pe: datetime) -> float | None:
    """sum(lead_value) for open leads at period_end (point-in-time, not filtered by period)."""
    val = await conn.fetchval("""
        SELECT COALESCE(SUM(lead_value), 0)
        FROM engine.leads
        WHERE tenant_id = $1::uuid
          AND is_open = TRUE
          AND created_at <= $2
          AND lead_value IS NOT NULL
    """, tenant_id, pe)
    return round(float(val), 2) if val is not None else None


async def q_pipeline_velocity_gbp_per_day(
    conn,
    tenant_id: str,
    ps: datetime,
    pe: datetime,
) -> float | None:
    """
    (open_opportunities * avg_deal_value * win_rate) / sales_cycle_days.
    Uses period-cohort win_rate and avg_deal_value; open_opportunities and
    sales_cycle_days are point-in-time at period_end.
    """
    open_opportunities = await conn.fetchval("""
        SELECT COUNT(*)
        FROM engine.leads
        WHERE tenant_id = $1::uuid
          AND is_open = TRUE
          AND created_at <= $2
    """, tenant_id, pe)

    if not open_opportunities:
        return None

    avg_deal = await conn.fetchval("""
        SELECT AVG(lead_value)
        FROM engine.leads
        WHERE tenant_id = $1::uuid
          AND won_at IS NOT NULL
          AND lead_value IS NOT NULL
    """, tenant_id)

    if not avg_deal:
        return None

    total_leads = await conn.fetchval("""
        SELECT COUNT(DISTINCT lead_id)
        FROM engine.lead_events
        WHERE tenant_id = $1::uuid
          AND event_type = 'lead_created'
    """, tenant_id)

    total_won = await conn.fetchval("""
        SELECT COUNT(*)
        FROM engine.leads
        WHERE tenant_id = $1::uuid
          AND won_at IS NOT NULL
    """, tenant_id)

    win_rate = (total_won / total_leads) if total_leads and total_won else None
    if win_rate is None:
        return None

    sales_cycle_days = await conn.fetchval("""
        WITH won_leads AS (
            SELECT lead_id, occurred_at AS won_at
            FROM engine.lead_events
            WHERE tenant_id = $1::uuid AND event_type = 'lead_won'
        ),
        created AS (
            SELECT lead_id, MIN(occurred_at) AS created_at
            FROM engine.lead_events
            WHERE tenant_id = $1::uuid AND event_type = 'lead_created'
            GROUP BY lead_id
        ),
        cycles AS (
            SELECT EXTRACT(EPOCH FROM (w.won_at - c.created_at)) / 86400.0 AS days
            FROM won_leads w JOIN created c ON c.lead_id = w.lead_id
            WHERE w.won_at >= c.created_at
        )
        SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY days)
        FROM cycles
    """, tenant_id)

    if not sales_cycle_days or float(sales_cycle_days) <= 0:
        return None

    velocity = (open_opportunities * float(avg_deal) * win_rate) / float(sales_cycle_days)
    return round(velocity, 2)


# ---------------------------------------------------------------------------
# Derivative metrics (totals for context)
# ---------------------------------------------------------------------------

async def q_totals(conn, tenant_id: str, ps: datetime, pe: datetime) -> dict:
    """Return total_leads, total_won, total_lost, total_revenue_gbp for the period."""
    total_leads = await conn.fetchval("""
        SELECT COUNT(DISTINCT lead_id)
        FROM engine.lead_events
        WHERE tenant_id = $1::uuid
          AND event_type = 'lead_created'
          AND occurred_at >= $2 AND occurred_at < $3
    """, tenant_id, ps, pe)

    total_won = await conn.fetchval("""
        SELECT COUNT(*)
        FROM engine.leads
        WHERE tenant_id = $1::uuid
          AND won_at >= $2 AND won_at < $3
    """, tenant_id, ps, pe)

    total_lost = await conn.fetchval("""
        SELECT COUNT(*)
        FROM engine.leads
        WHERE tenant_id = $1::uuid
          AND lost_at >= $2 AND lost_at < $3
    """, tenant_id, ps, pe)

    total_revenue = await conn.fetchval("""
        SELECT COALESCE(SUM(lead_value), 0)
        FROM engine.leads
        WHERE tenant_id = $1::uuid
          AND won_at >= $2 AND won_at < $3
          AND lead_value IS NOT NULL
    """, tenant_id, ps, pe)

    return {
        "total_leads": int(total_leads or 0),
        "total_won": int(total_won or 0),
        "total_lost": int(total_lost or 0),
        "total_revenue_gbp": round(float(total_revenue or 0), 2),
    }


async def q_competitive_win_rate(conn, tenant_id: str, ps: datetime, pe: datetime, total_won: int, total_lost: int) -> float | None:
    """won / (won + lost) for leads closed in the period."""
    denominator = total_won + total_lost
    if not denominator:
        return None
    return round(total_won / denominator, 4)


async def q_pipeline_win_rate(conn, tenant_id: str, ps: datetime, pe: datetime, total_won: int, total_leads: int) -> float | None:
    """won / total leads created in the period."""
    if not total_leads:
        return None
    return round(total_won / total_leads, 4)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    if not DB:
        print("ERROR: DATABASE_URL is required.")
        sys.exit(1)

    period_start, period_end = resolve_period()
    period_days = max(1, (period_end - period_start).days)
    weeks = period_days / 7.0

    print(f"Tenant:       {TENANT_SLUG}")
    print(f"Period type:  {PERIOD_TYPE}")
    print(f"Period:       {period_start.date()} to {period_end.date()} ({period_days} days)")

    conn = await asyncpg.connect(DB)

    try:
        # Resolve tenant_id
        tenant_id = await conn.fetchval(
            "SELECT tenant_id::text FROM core.tenants WHERE tenant_slug = $1",
            TENANT_SLUG,
        )
        if not tenant_id:
            print(f"ERROR: Tenant '{TENANT_SLUG}' not found.")
            sys.exit(1)

        print(f"Tenant ID:    {tenant_id}\n")

        # Compute totals first (reused by derivatives)
        totals = await q_totals(conn, tenant_id, period_start, period_end)
        total_leads = totals["total_leads"]
        total_won = totals["total_won"]
        total_lost = totals["total_lost"]

        # Compute all 12 core metrics
        lead_volume_weekly = await q_lead_volume_weekly(conn, tenant_id, period_start, period_end, weeks)
        speed_to_first_contact = await q_speed_to_first_contact_median_min(conn, tenant_id, period_start, period_end)
        lead_to_qualified_rate = await q_lead_to_qualified_rate(conn, tenant_id, period_start, period_end)
        qualified_to_booked_rate = await q_qualified_to_booked_rate(conn, tenant_id, period_start, period_end)
        show_rate = await q_show_rate(conn, tenant_id, period_start, period_end)
        show_to_proposal_rate = await q_show_to_proposal_rate(conn, tenant_id, period_start, period_end)
        close_rate = await q_close_rate(conn, tenant_id, period_start, period_end)
        avg_deal_value_gbp = await q_avg_deal_value_gbp(conn, tenant_id, period_start, period_end)
        sales_cycle_median_days = await q_sales_cycle_median_days(conn, tenant_id, period_start, period_end)
        revenue_per_lead_gbp = await q_revenue_per_lead_gbp(conn, tenant_id, period_start, period_end, total_leads)
        pipeline_value_gbp = await q_pipeline_value_gbp(conn, tenant_id, period_end)
        pipeline_velocity_gbp_per_day = await q_pipeline_velocity_gbp_per_day(conn, tenant_id, period_start, period_end)

        # Derivative metrics
        competitive_win_rate = await q_competitive_win_rate(conn, tenant_id, period_start, period_end, total_won, total_lost)
        pipeline_win_rate = await q_pipeline_win_rate(conn, tenant_id, period_start, period_end, total_won, total_leads)

        # Build metrics payload
        metrics = {
            # Core 12
            "lead_volume_weekly": lead_volume_weekly,
            "speed_to_first_contact_median_min": speed_to_first_contact,
            "lead_to_qualified_rate": lead_to_qualified_rate,
            "qualified_to_booked_rate": qualified_to_booked_rate,
            "show_rate": show_rate,
            "show_to_proposal_rate": show_to_proposal_rate,
            "close_rate": close_rate,
            "avg_deal_value_gbp": avg_deal_value_gbp,
            "sales_cycle_median_days": sales_cycle_median_days,
            "revenue_per_lead_gbp": revenue_per_lead_gbp,
            "pipeline_value_gbp": pipeline_value_gbp,
            "pipeline_velocity_gbp_per_day": pipeline_velocity_gbp_per_day,
            # Derivatives
            "competitive_win_rate": competitive_win_rate,
            "pipeline_win_rate": pipeline_win_rate,
            "total_leads": total_leads,
            "total_won": total_won,
            "total_lost": total_lost,
            "total_revenue_gbp": totals["total_revenue_gbp"],
        }

        # Upsert to engine.metric_snapshots
        await conn.execute("""
            INSERT INTO engine.metric_snapshots
                (snapshot_id, tenant_id, period_type, period_start, period_end, metrics, created_at)
            VALUES
                (gen_random_uuid(), $1::uuid, $2, $3, $4, $5::jsonb, now())
            ON CONFLICT (tenant_id, period_type, period_start)
            DO UPDATE SET
                metrics    = EXCLUDED.metrics,
                created_at = now()
        """,
            tenant_id,
            PERIOD_TYPE,
            period_start,
            period_end,
            json.dumps(metrics),
        )

        # Print summary
        def fmt_rate(v):
            return f"{v*100:.1f}%" if v is not None else "n/a"

        def fmt_gbp(v):
            return f"£{v:,.2f}" if v is not None else "n/a"

        def fmt_val(v, unit=""):
            return f"{v:,.4g}{unit}" if v is not None else "n/a"

        print("--- Metrics ---")
        print(f"  lead_volume_weekly:                {fmt_val(lead_volume_weekly, ' leads/wk')}")
        print(f"  speed_to_first_contact_median_min: {fmt_val(speed_to_first_contact, ' min')}")
        print(f"  lead_to_qualified_rate:            {fmt_rate(lead_to_qualified_rate)}")
        print(f"  qualified_to_booked_rate:          {fmt_rate(qualified_to_booked_rate)}")
        print(f"  show_rate:                         {fmt_rate(show_rate)}")
        print(f"  show_to_proposal_rate:             {fmt_rate(show_to_proposal_rate)}")
        print(f"  close_rate:                        {fmt_rate(close_rate)}")
        print(f"  avg_deal_value_gbp:                {fmt_gbp(avg_deal_value_gbp)}")
        print(f"  sales_cycle_median_days:           {fmt_val(sales_cycle_median_days, ' days')}")
        print(f"  revenue_per_lead_gbp:              {fmt_gbp(revenue_per_lead_gbp)}")
        print(f"  pipeline_value_gbp:                {fmt_gbp(pipeline_value_gbp)}")
        print(f"  pipeline_velocity_gbp_per_day:     {fmt_gbp(pipeline_velocity_gbp_per_day)}")
        print()
        print("--- Derivatives ---")
        print(f"  competitive_win_rate:              {fmt_rate(competitive_win_rate)}")
        print(f"  pipeline_win_rate:                 {fmt_rate(pipeline_win_rate)}")
        print(f"  total_leads:                       {total_leads}")
        print(f"  total_won:                         {total_won}")
        print(f"  total_lost:                        {total_lost}")
        print(f"  total_revenue_gbp:                 {fmt_gbp(totals['total_revenue_gbp'])}")
        print()
        print(f"Snapshot written to engine.metric_snapshots for tenant={TENANT_SLUG}, period_type={PERIOD_TYPE}, period_start={period_start.date()}.")

    finally:
        await conn.close()


asyncio.run(main())
