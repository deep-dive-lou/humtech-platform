"""
Morning Briefing Agent — comprehensive daily business summary to Slack.

Covers: Bot conversations (with full transcripts), Revenue Engine pipeline,
Outreach pipeline, Document Portal, Optimiser experiments, System health.

Runs daily at 07:00 UK time via runner.py digest_loop.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from app.agents.slack import SlackReporter

logger = logging.getLogger(__name__)

UK_TZ = ZoneInfo("Europe/London")


# ── Bot queries ──────────────────────────────────────────────────────

ACTIVE_CONVERSATIONS_SQL = """
SELECT c.conversation_id::text,
       c.status,
       c.last_intent,
       c.created_at,
       c.updated_at,
       c.context,
       ct.display_name,
       ct.channel_address,
       (SELECT COUNT(*) FROM bot.messages m
        WHERE m.conversation_id = c.conversation_id
          AND m.direction = 'inbound') AS inbound_turns,
       (SELECT COUNT(*) FROM bot.messages m
        WHERE m.conversation_id = c.conversation_id
          AND m.direction = 'outbound') AS outbound_turns
FROM bot.conversations c
JOIN bot.contacts ct ON ct.contact_id = c.contact_id
WHERE c.updated_at >= $1
ORDER BY c.updated_at DESC;
"""

TRANSCRIPT_SQL = """
SELECT direction, text, created_at
FROM bot.messages
WHERE conversation_id = $1::uuid
ORDER BY created_at ASC;
"""

FAILED_SENDS_SQL = """
SELECT ct.display_name, ct.channel_address,
       m.payload->>'send_last_error' AS error,
       m.created_at
FROM bot.messages m
JOIN bot.contacts ct ON ct.contact_id = m.contact_id
WHERE m.direction = 'outbound'
  AND m.payload->>'send_status' = 'failed'
  AND m.created_at >= $1;
"""

REENGAGE_BUMPS_SQL = """
SELECT ct.display_name, ct.channel_address
FROM bot.job_queue jq
JOIN bot.conversations c ON c.conversation_id = jq.conversation_id
JOIN bot.contacts ct ON ct.contact_id = c.contact_id
WHERE jq.job_type = 'reengage'
  AND jq.status = 'done'
  AND jq.created_at >= $1;
"""

STUCK_JOBS_SQL = """
SELECT COUNT(*) AS stuck_count
FROM bot.job_queue
WHERE status = 'queued'
  AND run_after < now() - interval '5 minutes';
"""

# ── Engine queries ───────────────────────────────────────────────────

ENGINE_NEW_LEADS_SQL = """
SELECT COUNT(*) AS new_count,
       COALESCE(SUM(lead_value), 0) AS new_value
FROM engine.leads
WHERE created_at >= $1;
"""

ENGINE_WINS_SQL = """
SELECT COUNT(*) AS win_count,
       COALESCE(SUM(lead_value), 0) AS win_value
FROM engine.leads
WHERE won_at >= $1;
"""

ENGINE_LOSSES_SQL = """
SELECT COUNT(*) AS loss_count
FROM engine.leads
WHERE lost_at >= $1;
"""

ENGINE_OPEN_PIPELINE_SQL = """
SELECT COUNT(*) AS open_count,
       COALESCE(SUM(lead_value), 0) AS open_value
FROM engine.leads
WHERE is_open = TRUE;
"""

ENGINE_STAGE_MOVEMENTS_SQL = """
SELECT from_stage, to_stage, COUNT(*) AS move_count
FROM engine.lead_events
WHERE event_type = 'stage_changed'
  AND occurred_at >= $1
GROUP BY from_stage, to_stage
ORDER BY move_count DESC
LIMIT 10;
"""

# ── Outreach queries ─────────────────────────────────────────────────

OUTREACH_SOURCED_SQL = """
SELECT COUNT(*) AS sourced_today
FROM outreach.leads
WHERE batch_date = CURRENT_DATE;
"""

OUTREACH_SENT_SQL = """
SELECT COUNT(*) AS sent_24h
FROM outreach.leads
WHERE status = 'sent'
  AND updated_at >= $1;
"""

OUTREACH_NEEDS_REVIEW_SQL = """
SELECT COUNT(*) AS needs_review
FROM outreach.personalisation
WHERE review_status = 'needs_review'
  AND removed = FALSE;
"""

# ── Portal queries ───────────────────────────────────────────────────

PORTAL_AWAITING_REVIEW_SQL = """
SELECT COUNT(*) AS awaiting_count
FROM portal.doc_request_items ri
JOIN portal.doc_requests r ON r.id = ri.request_id
WHERE ri.status::text = 'uploaded'
  AND r.status::text NOT IN ('completed', 'closed');
"""

PORTAL_OVERDUE_SQL = """
SELECT c.full_name, r.sent_at
FROM portal.doc_requests r
JOIN portal.clients c ON c.id = r.client_id
WHERE r.status::text = 'sent'
  AND r.sent_at < now() - interval '7 days'
LIMIT 10;
"""

# ── Optimiser queries ────────────────────────────────────────────────

OPTIMISER_RUNNING_SQL = """
SELECT e.name,
       COALESCE(SUM(ds.impressions), 0) AS total_impressions,
       COALESCE(SUM(ds.conversions), 0) AS total_conversions
FROM optimiser.experiments e
LEFT JOIN optimiser.daily_stats ds ON ds.experiment_id = e.experiment_id
WHERE e.status = 'running'
GROUP BY e.experiment_id, e.name;
"""

# ── Monitoring queries ───────────────────────────────────────────────

ALERTS_SQL = """
SELECT alert_type, contact_name, channel_address
FROM monitoring.active_alerts
WHERE alert_type IS NOT NULL;
"""


# ── Outcome detection ────────────────────────────────────────────────

def _outcome_emoji(context: dict | None, status: str, last_intent: str | None) -> str:
    if context and (context.get("booked_booking") or {}).get("slot"):
        return ":white_check_mark: Booked"
    if context and context.get("handoff_redirected"):
        return ":raised_hand: Handoff"
    if status == "closed":
        return ":no_entry_sign: Closed"
    if last_intent == "not_interested":
        return ":x: Not interested"
    return ":speech_balloon: Open"


# ── Transcript formatting ────────────────────────────────────────────

def _format_transcript(rows: list) -> str:
    lines = []
    for r in rows:
        ts = r["created_at"]
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        time_str = ts.astimezone(UK_TZ).strftime("%H:%M")
        direction = "LEAD" if r["direction"] == "inbound" else "BOT"
        text = (r["text"] or "").strip() or "(empty)"
        lines.append(f"  {time_str} [{direction}] {text}")
    return "\n".join(lines) if lines else "  (no messages)"


# ── Section builders ─────────────────────────────────────────────────

def _build_action_items(
    failed_sends: list,
    stuck_jobs: int,
    needs_review: int,
    overdue: list,
    alerts: list,
) -> str:
    items = []
    if failed_sends:
        items.append(f"- {len(failed_sends)} failed send{'s' if len(failed_sends) != 1 else ''}")
    if stuck_jobs > 0:
        items.append(f"- {stuck_jobs} stuck job{'s' if stuck_jobs != 1 else ''} in queue")
    if needs_review > 0:
        items.append(f"- {needs_review} outreach lead{'s' if needs_review != 1 else ''} need review")
    if overdue:
        items.append(f"- {len(overdue)} portal request{'s' if len(overdue) != 1 else ''} overdue (7+ days)")
    if alerts:
        for a in alerts[:5]:
            items.append(f"- Bot alert: {a['alert_type']} — {a['contact_name'] or a['channel_address']}")

    if not items:
        return ":white_check_mark: *Nothing urgent*"
    return ":rotating_light: *Needs Attention*\n" + "\n".join(items)


def _build_bot_section(conversations: list, reengage_bumps: list) -> str:
    total = len(conversations)
    booked = sum(1 for c in conversations if c["outcome"].startswith(":white_check_mark:"))
    dropped = sum(1 for c in conversations if c["outcome"].startswith(":x:") or c["outcome"].startswith(":no_entry_sign:"))
    bumps = len(reengage_bumps)

    parts = [f":speech_balloon: {total} conversation{'s' if total != 1 else ''}"]
    if booked:
        parts.append(f":white_check_mark: {booked} booking{'s' if booked != 1 else ''}")
    if dropped:
        parts.append(f":x: {dropped} dropped")
    if bumps:
        parts.append(f":arrows_counterclockwise: {bumps} bump{'s' if bumps != 1 else ''} sent")

    return ":robot_face: *Bot*\n" + "  |  ".join(parts)


def _build_engine_section(data: dict) -> str:
    nl = data.get("new_leads")
    wins = data.get("wins")
    losses = data.get("losses")
    pipeline = data.get("open_pipeline")
    movements = data.get("movements", [])

    if not nl and not pipeline:
        return ""

    lines = []

    # Activity line
    activity = []
    if nl and nl["new_count"] > 0:
        v = f"£{nl['new_value']:,.0f}" if nl["new_value"] else ""
        activity.append(f":new: {nl['new_count']} new lead{'s' if nl['new_count'] != 1 else ''}" + (f" ({v})" if v else ""))
    if wins and wins["win_count"] > 0:
        v = f"£{wins['win_value']:,.0f}" if wins["win_value"] else ""
        activity.append(f":trophy: {wins['win_count']} won" + (f" ({v})" if v else ""))
    if losses and losses["loss_count"] > 0:
        activity.append(f":no_entry_sign: {losses['loss_count']} lost")
    if activity:
        lines.append("  |  ".join(activity))

    # Pipeline
    if pipeline and pipeline["open_count"] > 0:
        lines.append(f":moneybag: Open pipeline: £{pipeline['open_value']:,.0f} ({pipeline['open_count']} leads)")

    # Movements
    if movements:
        moves = [f"{m['from_stage']} → {m['to_stage']} ({m['move_count']})" for m in movements[:5]]
        lines.append("Movements: " + ", ".join(moves))

    if not lines:
        return ""
    return ":chart_with_upwards_trend: *Pipeline*\n" + "\n".join(lines)


def _build_outreach_section(sourced: int, sent: int, needs_review: int) -> str:
    parts = []
    parts.append(f":incoming_envelope: {sourced} sourced today")
    parts.append(f":outbox_tray: {sent} sent to Instantly")
    if needs_review > 0:
        parts.append(f":eyes: {needs_review} needs review")
    return ":envelope: *Outreach*\n" + "  |  ".join(parts)


def _build_portal_section(awaiting: int, overdue: list) -> str:
    if awaiting == 0 and not overdue:
        return ""
    parts = []
    if awaiting > 0:
        parts.append(f":inbox_tray: {awaiting} item{'s' if awaiting != 1 else ''} awaiting review")
    if overdue:
        oldest = overdue[0]
        days = (datetime.now(timezone.utc) - oldest["sent_at"]).days if oldest["sent_at"] else 0
        parts.append(f":warning: {len(overdue)} request{'s' if len(overdue) != 1 else ''} overdue ({days}d)")
    return ":page_facing_up: *Portal*\n" + "  |  ".join(parts)


def _build_optimiser_section(experiments: list) -> str:
    if not experiments:
        return ""
    lines = []
    for exp in experiments:
        imps = exp["total_impressions"]
        convs = exp["total_conversions"]
        cr = f"{100 * convs / imps:.1f}% CR" if imps > 0 else "no data"
        lines.append(f"  {exp['name']} ({imps:,} impressions, {cr})")
    return ":test_tube: *Optimiser*\n" + "\n".join(lines)


def _build_system_section(stuck_jobs: int, alerts: list) -> str:
    parts = [f"{stuck_jobs} stuck job{'s' if stuck_jobs != 1 else ''}"]
    parts.append(f"{len(alerts)} active alert{'s' if len(alerts) != 1 else ''}")
    return ":wrench: *System*\n" + "  |  ".join(parts)


# ── Message assembly ─────────────────────────────────────────────────

def _build_briefing_message(
    conversations: list,
    reengage_bumps: list,
    failed_sends: list,
    stuck_jobs: int,
    engine_data: dict,
    outreach_sourced: int,
    outreach_sent: int,
    outreach_needs_review: int,
    portal_awaiting: int,
    portal_overdue: list,
    optimiser_experiments: list,
    alerts: list,
    since: datetime,
) -> str:
    now_uk = datetime.now(UK_TZ)
    since_uk = since.astimezone(UK_TZ)

    parts = [
        f":sunrise: *Morning Briefing* — {now_uk.strftime('%A %d %B %Y')}\n"
        f"_Covering {since_uk.strftime('%H:%M %d/%m')} to {now_uk.strftime('%H:%M %d/%m')}_"
    ]

    # Action items
    parts.append(_build_action_items(failed_sends, stuck_jobs, outreach_needs_review, portal_overdue, alerts))

    # Bot
    if conversations or reengage_bumps:
        parts.append("---")
        parts.append(_build_bot_section(conversations, reengage_bumps))

    # Engine
    engine_text = _build_engine_section(engine_data)
    if engine_text:
        parts.append("---")
        parts.append(engine_text)

    # Outreach
    parts.append("---")
    parts.append(_build_outreach_section(outreach_sourced, outreach_sent, outreach_needs_review))

    # Portal
    portal_text = _build_portal_section(portal_awaiting, portal_overdue)
    if portal_text:
        parts.append("---")
        parts.append(portal_text)

    # Optimiser
    opt_text = _build_optimiser_section(optimiser_experiments)
    if opt_text:
        parts.append("---")
        parts.append(opt_text)

    # System
    parts.append("---")
    parts.append(_build_system_section(stuck_jobs, alerts))

    # Failed sends detail
    if failed_sends:
        parts.append("---")
        fail_lines = [":warning: *Failed Sends Detail*"]
        for f in failed_sends[:10]:
            fail_lines.append(f"  - {f['display_name'] or f['channel_address']}: {(f['error'] or 'unknown')[:100]}")
        parts.append("\n".join(fail_lines))

    return "\n\n".join(parts)


def _build_transcript_messages(conversations: list, transcripts: dict) -> list[str]:
    messages = []
    for conv in conversations:
        name = conv["display_name"] or conv["channel_address"] or "Unknown"
        phone = conv["channel_address"] or ""
        total_turns = conv["inbound_turns"] + conv["outbound_turns"]

        header = (
            f"*{name}*  |  {phone}  |  {conv['outcome']}\n"
            f"_Turns: {total_turns} ({conv['inbound_turns']} in / {conv['outbound_turns']} out)_"
        )

        transcript = transcripts.get(conv["conversation_id"], "  (transcript unavailable)")
        messages.append(f"{header}\n```\n{transcript}\n```")

    return messages


# ── Data fetching helpers ────────────────────────────────────────────

async def _fetch_bot_data(conn, since: datetime) -> dict:
    rows = await conn.fetch(ACTIVE_CONVERSATIONS_SQL, since)

    conversations = []
    for r in rows:
        ctx = r["context"] or {}
        if isinstance(ctx, str):
            import json
            ctx = json.loads(ctx)
        conversations.append({
            "conversation_id": r["conversation_id"],
            "status": r["status"],
            "last_intent": r["last_intent"],
            "display_name": r["display_name"],
            "channel_address": r["channel_address"],
            "inbound_turns": r["inbound_turns"],
            "outbound_turns": r["outbound_turns"],
            "outcome": _outcome_emoji(ctx, r["status"], r["last_intent"]),
        })

    transcripts = {}
    for conv in conversations:
        msg_rows = await conn.fetch(TRANSCRIPT_SQL, conv["conversation_id"])
        transcripts[conv["conversation_id"]] = _format_transcript(msg_rows)

    failed_rows = await conn.fetch(FAILED_SENDS_SQL, since)
    failed_sends = [dict(r) for r in failed_rows]

    bump_rows = await conn.fetch(REENGAGE_BUMPS_SQL, since)
    reengage_bumps = [dict(r) for r in bump_rows]

    stuck_row = await conn.fetchrow(STUCK_JOBS_SQL)
    stuck_jobs = stuck_row["stuck_count"] if stuck_row else 0

    return {
        "conversations": conversations,
        "transcripts": transcripts,
        "failed_sends": failed_sends,
        "reengage_bumps": reengage_bumps,
        "stuck_jobs": stuck_jobs,
    }


async def _fetch_engine_data(conn, since: datetime) -> dict:
    data = {}
    try:
        data["new_leads"] = await conn.fetchrow(ENGINE_NEW_LEADS_SQL, since)
        data["wins"] = await conn.fetchrow(ENGINE_WINS_SQL, since)
        data["losses"] = await conn.fetchrow(ENGINE_LOSSES_SQL, since)
        data["open_pipeline"] = await conn.fetchrow(ENGINE_OPEN_PIPELINE_SQL)
        data["movements"] = [dict(r) for r in await conn.fetch(ENGINE_STAGE_MOVEMENTS_SQL, since)]
    except Exception as e:
        logger.warning("Briefing: engine section skipped: %s", e)
    return data


async def _fetch_outreach_data(conn, since: datetime) -> dict:
    data = {"sourced": 0, "sent": 0, "needs_review": 0}
    try:
        row = await conn.fetchrow(OUTREACH_SOURCED_SQL)
        data["sourced"] = row["sourced_today"] if row else 0
        row = await conn.fetchrow(OUTREACH_SENT_SQL, since)
        data["sent"] = row["sent_24h"] if row else 0
        row = await conn.fetchrow(OUTREACH_NEEDS_REVIEW_SQL)
        data["needs_review"] = row["needs_review"] if row else 0
    except Exception as e:
        logger.warning("Briefing: outreach section skipped: %s", e)
    return data


async def _fetch_portal_data(conn) -> dict:
    data = {"awaiting": 0, "overdue": []}
    try:
        row = await conn.fetchrow(PORTAL_AWAITING_REVIEW_SQL)
        data["awaiting"] = row["awaiting_count"] if row else 0
        data["overdue"] = [dict(r) for r in await conn.fetch(PORTAL_OVERDUE_SQL)]
    except Exception as e:
        logger.warning("Briefing: portal section skipped: %s", e)
    return data


async def _fetch_optimiser_data(conn) -> list:
    try:
        rows = await conn.fetch(OPTIMISER_RUNNING_SQL)
        return [dict(r) for r in rows]
    except Exception as e:
        logger.warning("Briefing: optimiser section skipped: %s", e)
        return []


async def _fetch_alerts(conn) -> list:
    try:
        rows = await conn.fetch(ALERTS_SQL)
        return [dict(r) for r in rows]
    except Exception as e:
        logger.warning("Briefing: alerts section skipped: %s", e)
        return []


# ── Main entry point ─────────────────────────────────────────────────

async def run(conn, slack: SlackReporter) -> int:
    """
    Build and send the morning briefing. Returns number of conversations.
    """
    since = datetime.now(timezone.utc) - timedelta(hours=24)

    # Fetch all data
    bot = await _fetch_bot_data(conn, since)
    engine = await _fetch_engine_data(conn, since)
    outreach = await _fetch_outreach_data(conn, since)
    portal = await _fetch_portal_data(conn)
    optimiser = await _fetch_optimiser_data(conn)
    alerts = await _fetch_alerts(conn)

    conversations = bot["conversations"]
    has_activity = (
        conversations
        or bot["failed_sends"]
        or bot["stuck_jobs"] > 0
        or engine.get("new_leads", {}).get("new_count", 0) > 0
        or outreach["sourced"] > 0
        or outreach["sent"] > 0
        or outreach["needs_review"] > 0
        or portal["awaiting"] > 0
        or portal["overdue"]
        or optimiser
        or alerts
    )

    if has_activity:
        # Post the briefing summary
        briefing = _build_briefing_message(
            conversations=conversations,
            reengage_bumps=bot["reengage_bumps"],
            failed_sends=bot["failed_sends"],
            stuck_jobs=bot["stuck_jobs"],
            engine_data=engine,
            outreach_sourced=outreach["sourced"],
            outreach_sent=outreach["sent"],
            outreach_needs_review=outreach["needs_review"],
            portal_awaiting=portal["awaiting"],
            portal_overdue=portal["overdue"],
            optimiser_experiments=optimiser,
            alerts=alerts,
            since=since,
        )
        await slack.post(briefing)

        # Post individual conversation transcripts
        transcript_msgs = _build_transcript_messages(conversations, bot["transcripts"])
        if transcript_msgs:
            await slack.post_sequence(transcript_msgs)

        logger.info(
            "Morning briefing sent: %d conversations, %d failed, %d alerts",
            len(conversations), len(bot["failed_sends"]), len(alerts),
        )
    else:
        now_uk = datetime.now(UK_TZ)
        await slack.post(
            f":sunrise: *Morning Briefing* — {now_uk.strftime('%A %d %B %Y')}\n\n"
            f"No activity in the last 24 hours. All systems quiet."
        )
        logger.info("Morning briefing sent: no activity")

    return len(conversations)
