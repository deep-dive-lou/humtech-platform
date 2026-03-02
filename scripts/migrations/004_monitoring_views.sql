-- 004_monitoring_views.sql
-- Creates monitoring schema with views for Metabase dashboards.
-- Run with doadmin credentials: asyncpg.connect(DOADMIN_DATABASE_URL)

CREATE SCHEMA IF NOT EXISTS monitoring;

-- 1. Per-conversation health snapshot
CREATE OR REPLACE VIEW monitoring.conversation_summary AS
SELECT
    c.conversation_id,
    c.tenant_id,
    c.status,
    ct.display_name AS contact_name,
    ct.channel_address,
    c.last_step,
    c.last_intent,
    (c.context->'booked_booking'->>'slot') IS NOT NULL AS has_booking,
    c.created_at,
    c.updated_at,
    c.last_inbound_at,
    c.last_outbound_at,
    EXTRACT(EPOCH FROM (COALESCE(c.updated_at, c.created_at) - c.created_at)) / 60 AS duration_minutes,
    (SELECT COUNT(*) FROM bot.messages m
     WHERE m.conversation_id = c.conversation_id AND m.direction = 'inbound') AS inbound_turns,
    (SELECT COUNT(*) FROM bot.messages m
     WHERE m.conversation_id = c.conversation_id AND m.direction = 'outbound') AS outbound_turns
FROM bot.conversations c
JOIN bot.contacts ct ON ct.contact_id = c.contact_id;

-- 2. Outbound message delivery health
CREATE OR REPLACE VIEW monitoring.send_health AS
SELECT
    m.message_id,
    m.conversation_id,
    m.contact_id,
    m.payload->>'send_status' AS send_status,
    (m.payload->>'send_attempts')::int AS send_attempts,
    m.payload->>'send_last_error' AS last_error,
    m.payload->>'route' AS route,
    m.created_at
FROM bot.messages m
WHERE m.direction = 'outbound';

-- 3. Daily intent distribution
CREATE OR REPLACE VIEW monitoring.intent_distribution AS
SELECT
    DATE(m.created_at) AS day,
    m.payload->>'intent' AS intent,
    COUNT(*) AS count
FROM bot.messages m
WHERE m.direction = 'inbound'
  AND m.payload->>'intent' IS NOT NULL
GROUP BY DATE(m.created_at), m.payload->>'intent';

-- 4. Daily conversation funnel
CREATE OR REPLACE VIEW monitoring.daily_funnel AS
SELECT
    DATE(c.created_at) AS day,
    COUNT(*) AS total_conversations,
    COUNT(*) FILTER (WHERE (c.context->'booked_booking'->>'slot') IS NOT NULL) AS booked,
    COUNT(*) FILTER (WHERE c.last_intent = 'decline') AS declined,
    COUNT(*) FILTER (WHERE c.last_intent = 'wants_human') AS wants_human,
    COUNT(*) FILTER (WHERE c.last_intent IN ('engage', 'request_slots', 'request_specific_time')) AS engaged
FROM bot.conversations c
GROUP BY DATE(c.created_at);

-- 5. Active alerts (mirrors alert worker checks)
CREATE OR REPLACE VIEW monitoring.active_alerts AS
WITH turn_counts AS (
    SELECT m.conversation_id, COUNT(*) AS inbound_turns
    FROM bot.messages m
    WHERE m.direction = 'inbound'
    GROUP BY m.conversation_id
)
SELECT
    c.conversation_id,
    ct.display_name AS contact_name,
    ct.channel_address,
    c.last_intent,
    c.status,
    tc.inbound_turns,
    (c.context->'booked_booking'->>'slot') IS NOT NULL AS has_booking,
    c.last_inbound_at,
    c.last_outbound_at,
    c.updated_at,
    CASE
        WHEN tc.inbound_turns > 6
             AND (c.context->'booked_booking'->>'slot') IS NULL
             AND c.status = 'open'
            THEN 'high_turns'
        WHEN c.last_intent = 'wants_human'
             AND c.status = 'open'
             AND c.updated_at < now() - interval '15 minutes'
            THEN 'wants_human'
        WHEN c.status = 'open'
             AND c.last_inbound_at < now() - interval '2 hours'
             AND (c.last_outbound_at IS NULL OR c.last_outbound_at < c.last_inbound_at)
            THEN 'stalled'
        ELSE NULL
    END AS alert_type
FROM bot.conversations c
JOIN bot.contacts ct ON ct.contact_id = c.contact_id
LEFT JOIN turn_counts tc ON tc.conversation_id = c.conversation_id
WHERE c.status = 'open';

-- Grant read access to the bot user
GRANT USAGE ON SCHEMA monitoring TO humtech_bot;
GRANT SELECT ON ALL TABLES IN SCHEMA monitoring TO humtech_bot;
