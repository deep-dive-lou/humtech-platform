-- Migration 008: Re-engagement support
-- Adds conversation_id to job_queue for jobs not tied to an inbound event (e.g., reengage).
-- Makes inbound_event_id nullable since reengage jobs have no inbound event.

ALTER TABLE bot.job_queue ADD COLUMN IF NOT EXISTS conversation_id uuid;

ALTER TABLE bot.job_queue ALTER COLUMN inbound_event_id DROP NOT NULL;

-- Partial index for finding existing reengage jobs for a conversation
CREATE INDEX IF NOT EXISTS idx_job_queue_reengage
  ON bot.job_queue (conversation_id, job_type)
  WHERE status IN ('queued', 'running') AND job_type = 'reengage';
