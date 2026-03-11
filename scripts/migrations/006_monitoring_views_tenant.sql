-- Migration 006: Add tenant_id to monitoring views for multi-tenant support
-- All 4 views that lacked tenant_id are recreated with it included
-- Uses DROP + CREATE (not CREATE OR REPLACE) because column order changes

BEGIN;

-- 1. Outbound message delivery health
-- Previously selected from bot.messages without tenant_id.
-- Fixed: JOIN to bot.conversations to pull tenant_id through.
DROP VIEW IF EXISTS monitoring.send_health;
CREATE VIEW monitoring.send_health AS
SELECT
    m.message_id,
    m.conversation_id,
    c.tenant_id,
    m.contact_id,
    m.payload->>'send_status' AS send_status,
    (m.payload->>'send_attempts')::int AS send_attempts,
    m.payload->>'send_last_error' AS last_error,
    m.payload->>'route' AS route,
    m.created_at
FROM bot.messages m
JOIN bot.conversations c ON c.conversation_id = m.conversation_id
WHERE m.direction = 'outbound';

-- 2. Daily intent distribution
-- Previously aggregated from bot.messages without tenant_id.
-- Fixed: JOIN to bot.conversations to pull tenant_id through, add to GROUP BY.
DROP VIEW IF EXISTS monitoring.intent_distribution;
CREATE VIEW monitoring.intent_distribution AS
SELECT
    c.tenant_id,
    DATE(m.created_at) AS day,
    m.payload->>'intent' AS intent,
    count(*) AS count
FROM bot.messages m
JOIN bot.conversations c ON c.conversation_id = m.conversation_id
WHERE m.direction = 'inbound'
  AND m.payload->>'intent' IS NOT NULL
GROUP BY c.tenant_id, DATE(m.created_at), m.payload->>'intent';

-- 3. Daily conversation funnel
-- Previously aggregated from bot.conversations without tenant_id in SELECT or GROUP BY.
-- Fixed: add tenant_id to SELECT and GROUP BY (conversations already has the column).
DROP VIEW IF EXISTS monitoring.daily_funnel;
CREATE VIEW monitoring.daily_funnel AS
SELECT
    c.tenant_id,
    DATE(c.created_at) AS day,
    count(*) AS total_conversations,
    count(*) FILTER (WHERE (c.context->'booked_booking'->>'slot') IS NOT NULL) AS booked,
    count(*) FILTER (WHERE c.last_intent = 'decline') AS declined,
    count(*) FILTER (WHERE c.last_intent = 'wants_human') AS wants_human,
    count(*) FILTER (WHERE c.last_intent IN ('engage', 'request_slots', 'request_specific_time')) AS engaged
FROM bot.conversations c
GROUP BY c.tenant_id, DATE(c.created_at);

-- 4. Active alerts (mirrors alert worker checks)
-- Previously selected from conversations + contacts without tenant_id.
-- Fixed: add tenant_id to SELECT.
DROP VIEW IF EXISTS monitoring.active_alerts;
CREATE VIEW monitoring.active_alerts AS
WITH turn_counts AS (
    SELECT m.conversation_id, count(*) AS inbound_turns
    FROM bot.messages m
    WHERE m.direction = 'inbound'
    GROUP BY m.conversation_id
)
SELECT
    c.conversation_id,
    c.tenant_id,
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

GRANT SELECT ON ALL TABLES IN SCHEMA monitoring TO humtech_bot;

COMMIT;
