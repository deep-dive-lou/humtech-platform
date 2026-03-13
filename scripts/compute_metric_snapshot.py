"""
Compute metrics across CRM funnel, bot, portal, and financials, and write
them to engine.metric_snapshots for a given tenant and period.

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
import math
import os
import sys
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
load_dotenv()

# Add project root so we can import analytics stats
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from app.engine.analytics.stats import choose_ci


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
# Rate result helpers
# ---------------------------------------------------------------------------

def rate_value(result: dict | None) -> float | None:
    """Extract the scalar value from a rate result dict."""
    return result["value"] if result else None


def rate_ci(result: dict | None) -> dict | None:
    """Compute confidence interval for a rate result."""
    if result is None:
        return None
    n, k = result["n"], result["k"]
    lower, upper, method = choose_ci(k, n)
    return {
        "value": result["value"],
        "n": n,
        "k": k,
        "ci_lower": round(lower, 4),
        "ci_upper": round(upper, 4),
        "ci_method": method,
        "low_confidence": n < 30,
    }


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


async def q_lead_to_qualified_rate(conn, tenant_id: str, ps: datetime, pe: datetime) -> dict | None:
    """Leads reaching lead_qualified / leads created in period (both events within period)."""
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
          AND e.occurred_at >= $2 AND e.occurred_at < $3
          AND e.lead_id IN (
              SELECT lead_id FROM engine.lead_events
              WHERE tenant_id = $1::uuid
                AND event_type = 'lead_created'
                AND occurred_at >= $2 AND occurred_at < $3
          )
    """, tenant_id, ps, pe)
    return {"value": round(qualified / created, 4), "n": int(created), "k": int(qualified)}


async def q_qualified_to_booked_rate(conn, tenant_id: str, ps: datetime, pe: datetime) -> dict | None:
    """Leads reaching appointment_booked / leads reaching lead_qualified (both events within period)."""
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
          AND e.occurred_at >= $2 AND e.occurred_at < $3
          AND e.lead_id IN (
              SELECT lead_id FROM engine.lead_events
              WHERE tenant_id = $1::uuid
                AND canonical_stage = 'lead_qualified'
                AND occurred_at >= $2 AND occurred_at < $3
          )
    """, tenant_id, ps, pe)
    return {"value": round(booked / qualified, 4), "n": int(qualified), "k": int(booked)}


async def q_show_rate(conn, tenant_id: str, ps: datetime, pe: datetime) -> dict | None:
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
    return {"value": round(completed / booked, 4), "n": int(booked), "k": int(completed)}


async def q_show_to_proposal_rate(conn, tenant_id: str, ps: datetime, pe: datetime) -> dict | None:
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
    return {"value": round(proposals / completed, 4), "n": int(completed), "k": int(proposals)}


async def q_close_rate(conn, tenant_id: str, ps: datetime, pe: datetime) -> dict | None:
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
    return {"value": round(won / proposals, 4), "n": int(proposals), "k": int(won)}


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
# Bot metrics
# ---------------------------------------------------------------------------

async def q_bot_metrics(conn, tenant_id: str, ps: datetime, pe: datetime) -> dict:
    """Bot conversation metrics for the period."""
    # Conversations opened in period
    conversations_opened = await conn.fetchval("""
        SELECT COUNT(*)
        FROM bot.conversations
        WHERE tenant_id = $1::uuid
          AND created_at >= $2 AND created_at < $3
    """, tenant_id, ps, pe)

    # Conversations closed in period
    conversations_closed = await conn.fetchval("""
        SELECT COUNT(*)
        FROM bot.conversations
        WHERE tenant_id = $1::uuid
          AND status = 'closed'
          AND updated_at >= $2 AND updated_at < $3
    """, tenant_id, ps, pe)

    # Conversations that resulted in a booking (have booked_booking in context)
    bookings_made = await conn.fetchval("""
        SELECT COUNT(*)
        FROM bot.conversations
        WHERE tenant_id = $1::uuid
          AND created_at >= $2 AND created_at < $3
          AND context ? 'booked_booking'
    """, tenant_id, ps, pe)

    # Booking rate
    booking_rate = None
    if conversations_opened:
        booking_rate = round(bookings_made / conversations_opened, 4)

    # Wants_human count (conversations where last_intent was wants_human at any point)
    wants_human_count = await conn.fetchval("""
        SELECT COUNT(DISTINCT conversation_id)
        FROM bot.messages
        WHERE tenant_id = $1::uuid
          AND created_at >= $2 AND created_at < $3
          AND direction = 'inbound'
          AND payload->>'intent' = 'wants_human'
    """, tenant_id, ps, pe)

    # Decline count
    decline_count = await conn.fetchval("""
        SELECT COUNT(DISTINCT conversation_id)
        FROM bot.messages
        WHERE tenant_id = $1::uuid
          AND created_at >= $2 AND created_at < $3
          AND direction = 'inbound'
          AND payload->>'intent' = 'decline'
    """, tenant_id, ps, pe)

    # Avg turns to book (inbound messages per conversation that booked)
    avg_turns_to_book = await conn.fetchval("""
        WITH booked AS (
            SELECT conversation_id
            FROM bot.conversations
            WHERE tenant_id = $1::uuid
              AND created_at >= $2 AND created_at < $3
              AND context ? 'booked_booking'
        ),
        turn_counts AS (
            SELECT m.conversation_id, COUNT(*) AS turns
            FROM bot.messages m
            JOIN booked b ON b.conversation_id = m.conversation_id
            WHERE m.tenant_id = $1::uuid
              AND m.direction = 'inbound'
            GROUP BY m.conversation_id
        )
        SELECT AVG(turns) FROM turn_counts
    """, tenant_id, ps, pe)

    # Failed sends in period
    failed_sends = await conn.fetchval("""
        SELECT COUNT(*)
        FROM bot.messages
        WHERE tenant_id = $1::uuid
          AND created_at >= $2 AND created_at < $3
          AND direction = 'outbound'
          AND payload->>'send_status' = 'failed'
    """, tenant_id, ps, pe)

    # Total messages (inbound + outbound)
    total_inbound = await conn.fetchval("""
        SELECT COUNT(*)
        FROM bot.messages
        WHERE tenant_id = $1::uuid
          AND created_at >= $2 AND created_at < $3
          AND direction = 'inbound'
    """, tenant_id, ps, pe)

    total_outbound = await conn.fetchval("""
        SELECT COUNT(*)
        FROM bot.messages
        WHERE tenant_id = $1::uuid
          AND created_at >= $2 AND created_at < $3
          AND direction = 'outbound'
    """, tenant_id, ps, pe)

    # Re-engagement attempts in period
    reengage_attempts = await conn.fetchval("""
        SELECT COUNT(*)
        FROM bot.job_queue
        WHERE tenant_id = $1::uuid
          AND job_type = 'reengage'
          AND created_at >= $2 AND created_at < $3
    """, tenant_id, ps, pe)

    return {
        "conversations_opened": int(conversations_opened or 0),
        "conversations_closed": int(conversations_closed or 0),
        "bookings_made": int(bookings_made or 0),
        "booking_rate": booking_rate,
        "wants_human_count": int(wants_human_count or 0),
        "decline_count": int(decline_count or 0),
        "avg_turns_to_book": round(float(avg_turns_to_book), 2) if avg_turns_to_book else None,
        "failed_sends": int(failed_sends or 0),
        "total_inbound_messages": int(total_inbound or 0),
        "total_outbound_messages": int(total_outbound or 0),
        "reengage_attempts": int(reengage_attempts or 0),
    }


# ---------------------------------------------------------------------------
# Portal metrics
# ---------------------------------------------------------------------------

async def q_portal_metrics(conn, tenant_id: str, ps: datetime, pe: datetime) -> dict:
    """Document portal metrics for the period."""
    # Check if portal schema exists (not all tenants use portal)
    has_portal = await conn.fetchval("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'portal' AND table_name = 'doc_requests'
        )
    """)
    if not has_portal:
        return {}

    # Requests sent in period
    requests_sent = await conn.fetchval("""
        SELECT COUNT(*)
        FROM portal.doc_requests
        WHERE tenant_id = $1::uuid
          AND sent_at >= $2 AND sent_at < $3
    """, tenant_id, ps, pe)

    # Requests completed in period
    requests_completed = await conn.fetchval("""
        SELECT COUNT(*)
        FROM portal.doc_requests
        WHERE tenant_id = $1::uuid
          AND status = 'completed'
          AND updated_at >= $2 AND updated_at < $3
    """, tenant_id, ps, pe)

    # Requests currently open (point-in-time at period end)
    requests_open = await conn.fetchval("""
        SELECT COUNT(*)
        FROM portal.doc_requests
        WHERE tenant_id = $1::uuid
          AND status NOT IN ('completed', 'closed', 'draft')
          AND created_at <= $2
    """, tenant_id, pe)

    # Items pending review (point-in-time at period end)
    items_pending = await conn.fetchval("""
        SELECT COUNT(*)
        FROM portal.doc_request_items dri
        JOIN portal.doc_requests dr ON dr.id = dri.request_id
        WHERE dr.tenant_id = $1::uuid
          AND dri.status = 'uploaded'
          AND dr.created_at <= $2
    """, tenant_id, pe)

    # Overdue requests (sent but not completed, past due_at)
    requests_overdue = await conn.fetchval("""
        SELECT COUNT(*)
        FROM portal.doc_requests
        WHERE tenant_id = $1::uuid
          AND status NOT IN ('completed', 'closed', 'draft')
          AND due_at IS NOT NULL
          AND due_at < $2
    """, tenant_id, pe)

    # Avg completion time (days from sent_at to completion)
    avg_completion_days = await conn.fetchval("""
        SELECT AVG(EXTRACT(EPOCH FROM (updated_at - sent_at)) / 86400.0)
        FROM portal.doc_requests
        WHERE tenant_id = $1::uuid
          AND status = 'completed'
          AND sent_at IS NOT NULL
          AND updated_at >= $2 AND updated_at < $3
    """, tenant_id, ps, pe)

    # Emails sent in period
    emails_sent = await conn.fetchval("""
        SELECT COUNT(*)
        FROM portal.email_sends
        WHERE request_id IN (
            SELECT id FROM portal.doc_requests WHERE tenant_id = $1::uuid
        )
        AND sent_at >= $2 AND sent_at < $3
    """, tenant_id, ps, pe)

    return {
        "requests_sent": int(requests_sent or 0),
        "requests_completed": int(requests_completed or 0),
        "requests_open": int(requests_open or 0),
        "items_pending_review": int(items_pending or 0),
        "requests_overdue": int(requests_overdue or 0),
        "avg_completion_days": round(float(avg_completion_days), 2) if avg_completion_days else None,
        "emails_sent": int(emails_sent or 0),
    }


# ---------------------------------------------------------------------------
# Financial actuals (from Xero/QuickBooks baselines)
# ---------------------------------------------------------------------------

async def q_financial_actuals(conn, tenant_id: str, ps: datetime, pe: datetime) -> dict:
    """Pull financial actuals from the most recent financial baseline if available."""
    # Find the active financial baseline for this tenant
    baseline = await conn.fetchrow("""
        SELECT metrics
        FROM engine.baselines
        WHERE tenant_id = $1::uuid
          AND is_active = TRUE
          AND label LIKE '%financial%'
        ORDER BY created_at DESC
        LIMIT 1
    """, tenant_id)

    if not baseline:
        return {}

    metrics_raw = baseline["metrics"]
    if isinstance(metrics_raw, str):
        metrics_data = json.loads(metrics_raw)
    else:
        metrics_data = metrics_raw

    # Try to find monthly revenue for this period from the monthly breakdown
    monthly_breakdown = metrics_data.get("monthly_breakdown", [])
    period_month_key = ps.strftime("%Y-%m")

    period_revenue = None
    for entry in monthly_breakdown:
        if entry.get("month", "").startswith(period_month_key):
            period_revenue = entry.get("revenue")
            break

    return {
        "source": metrics_data.get("source"),
        "annual_revenue_gbp": metrics_data.get("annual_revenue_gbp"),
        "monthly_revenue_avg_gbp": metrics_data.get("monthly_revenue_gbp"),
        "period_revenue_gbp": round(float(period_revenue), 2) if period_revenue else None,
        "period_months_covered": metrics_data.get("period_months"),
    }


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
        lead_to_qualified_raw = await q_lead_to_qualified_rate(conn, tenant_id, period_start, period_end)
        qualified_to_booked_raw = await q_qualified_to_booked_rate(conn, tenant_id, period_start, period_end)
        show_rate_raw = await q_show_rate(conn, tenant_id, period_start, period_end)
        show_to_proposal_raw = await q_show_to_proposal_rate(conn, tenant_id, period_start, period_end)
        close_rate_raw = await q_close_rate(conn, tenant_id, period_start, period_end)
        avg_deal_value_gbp = await q_avg_deal_value_gbp(conn, tenant_id, period_start, period_end)
        sales_cycle_median_days = await q_sales_cycle_median_days(conn, tenant_id, period_start, period_end)
        revenue_per_lead_gbp = await q_revenue_per_lead_gbp(conn, tenant_id, period_start, period_end, total_leads)
        pipeline_value_gbp = await q_pipeline_value_gbp(conn, tenant_id, period_end)
        pipeline_velocity_gbp_per_day = await q_pipeline_velocity_gbp_per_day(conn, tenant_id, period_start, period_end)

        # Extract scalar values (backward-compatible)
        lead_to_qualified_rate = rate_value(lead_to_qualified_raw)
        qualified_to_booked_rate = rate_value(qualified_to_booked_raw)
        show_rate = rate_value(show_rate_raw)
        show_to_proposal_rate = rate_value(show_to_proposal_raw)
        close_rate = rate_value(close_rate_raw)

        # Derivative metrics
        competitive_win_rate = await q_competitive_win_rate(conn, tenant_id, period_start, period_end, total_won, total_lost)
        pipeline_win_rate = await q_pipeline_win_rate(conn, tenant_id, period_start, period_end, total_won, total_leads)

        # Bot metrics
        bot = await q_bot_metrics(conn, tenant_id, period_start, period_end)

        # Portal metrics
        portal = await q_portal_metrics(conn, tenant_id, period_start, period_end)

        # Financial actuals
        financial = await q_financial_actuals(conn, tenant_id, period_start, period_end)

        # Build metrics payload (scalar values for backward compatibility)
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
            # Confidence intervals for all rate metrics
            "ci": {
                "lead_to_qualified_rate": rate_ci(lead_to_qualified_raw),
                "qualified_to_booked_rate": rate_ci(qualified_to_booked_raw),
                "show_rate": rate_ci(show_rate_raw),
                "show_to_proposal_rate": rate_ci(show_to_proposal_raw),
                "close_rate": rate_ci(close_rate_raw),
            },
            # Bot metrics
            "bot": bot,
            # Portal metrics
            "portal": portal,
            # Financial actuals (from Xero/QuickBooks)
            "financial": financial,
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

        def fmt_rate_ci(ci_data):
            if ci_data is None:
                return "n/a"
            flag = " [LOW CONFIDENCE]" if ci_data["low_confidence"] else ""
            return (
                f"{ci_data['value']*100:.1f}% "
                f"({ci_data['ci_lower']*100:.1f}–{ci_data['ci_upper']*100:.1f}%) "
                f"n={ci_data['n']}, {ci_data['ci_method']}{flag}"
            )

        def fmt_gbp(v):
            return f"£{v:,.2f}" if v is not None else "n/a"

        def fmt_val(v, unit=""):
            return f"{v:,.4g}{unit}" if v is not None else "n/a"

        ci = metrics["ci"]
        print("--- Metrics (with 95% CIs) ---")
        print(f"  lead_volume_weekly:                {fmt_val(lead_volume_weekly, ' leads/wk')}")
        print(f"  speed_to_first_contact_median_min: {fmt_val(speed_to_first_contact, ' min')}")
        print(f"  lead_to_qualified_rate:            {fmt_rate_ci(ci['lead_to_qualified_rate'])}")
        print(f"  qualified_to_booked_rate:          {fmt_rate_ci(ci['qualified_to_booked_rate'])}")
        print(f"  show_rate:                         {fmt_rate_ci(ci['show_rate'])}")
        print(f"  show_to_proposal_rate:             {fmt_rate_ci(ci['show_to_proposal_rate'])}")
        print(f"  close_rate:                        {fmt_rate_ci(ci['close_rate'])}")
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
        if bot:
            print()
            print("--- Bot ---")
            print(f"  conversations_opened:    {bot['conversations_opened']}")
            print(f"  conversations_closed:    {bot['conversations_closed']}")
            print(f"  bookings_made:           {bot['bookings_made']}")
            print(f"  booking_rate:            {fmt_rate(bot['booking_rate'])}")
            print(f"  wants_human_count:       {bot['wants_human_count']}")
            print(f"  decline_count:           {bot['decline_count']}")
            print(f"  avg_turns_to_book:       {fmt_val(bot['avg_turns_to_book'], ' turns')}")
            print(f"  failed_sends:            {bot['failed_sends']}")
            print(f"  total_inbound_messages:  {bot['total_inbound_messages']}")
            print(f"  total_outbound_messages: {bot['total_outbound_messages']}")
            print(f"  reengage_attempts:       {bot['reengage_attempts']}")
        if portal:
            print()
            print("--- Portal ---")
            print(f"  requests_sent:           {portal['requests_sent']}")
            print(f"  requests_completed:      {portal['requests_completed']}")
            print(f"  requests_open:           {portal['requests_open']}")
            print(f"  items_pending_review:    {portal['items_pending_review']}")
            print(f"  requests_overdue:        {portal['requests_overdue']}")
            print(f"  avg_completion_days:     {fmt_val(portal['avg_completion_days'], ' days')}")
            print(f"  emails_sent:             {portal['emails_sent']}")
        if financial:
            print()
            print("--- Financial ---")
            print(f"  source:                  {financial.get('source', 'n/a')}")
            print(f"  annual_revenue_gbp:      {fmt_gbp(financial.get('annual_revenue_gbp'))}")
            print(f"  monthly_revenue_avg_gbp: {fmt_gbp(financial.get('monthly_revenue_avg_gbp'))}")
            print(f"  period_revenue_gbp:      {fmt_gbp(financial.get('period_revenue_gbp'))}")
        print()
        print(f"Snapshot written to engine.metric_snapshots for tenant={TENANT_SLUG}, period_type={PERIOD_TYPE}, period_start={period_start.date()}.")

    finally:
        await conn.close()


asyncio.run(main())
