-- Migration: 001_add_trace_id
-- Purpose: Add trace_id for end-to-end observability (glass-box mode)
--
-- trace_id is a UUIDv4 generated once per inbound event and propagated
-- through job_queue -> messages -> conversation context.
-- On retries, the same trace_id is reused.

BEGIN;

-- 1) Add trace_id to bot.inbound_events (source of truth)
ALTER TABLE bot.inbound_events
ADD COLUMN IF NOT EXISTS trace_id uuid DEFAULT gen_random_uuid();

-- Backfill existing rows (one-time)
UPDATE bot.inbound_events SET trace_id = gen_random_uuid() WHERE trace_id IS NULL;

-- Make NOT NULL after backfill
ALTER TABLE bot.inbound_events ALTER COLUMN trace_id SET NOT NULL;

-- Index for trace queries
CREATE INDEX IF NOT EXISTS idx_inbound_events_trace_id ON bot.inbound_events (trace_id);

-- 2) Add trace_id to bot.job_queue (copied from inbound_event)
ALTER TABLE bot.job_queue
ADD COLUMN IF NOT EXISTS trace_id uuid;

-- Backfill from inbound_events
UPDATE bot.job_queue jq
SET trace_id = ie.trace_id
FROM bot.inbound_events ie
WHERE jq.inbound_event_id = ie.inbound_event_id
  AND jq.trace_id IS NULL;

-- Index for trace queries
CREATE INDEX IF NOT EXISTS idx_job_queue_trace_id ON bot.job_queue (trace_id);

-- 3) Add trace_id to bot.messages (for both inbound and outbound)
ALTER TABLE bot.messages
ADD COLUMN IF NOT EXISTS trace_id uuid;

-- Index for trace queries
CREATE INDEX IF NOT EXISTS idx_messages_trace_id ON bot.messages (trace_id);

COMMIT;

-- Verification queries (run manually after migration):
-- SELECT COUNT(*) FROM bot.inbound_events WHERE trace_id IS NULL; -- should be 0
-- SELECT COUNT(*) FROM bot.job_queue WHERE trace_id IS NULL AND inbound_event_id IS NOT NULL; -- should be 0
