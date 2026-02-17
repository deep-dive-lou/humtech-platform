# Glass-Box Tracing (v1)

End-to-end observability for debugging and auditing conversation flows.

## Overview

Every inbound event generates a `trace_id` (UUIDv4) that propagates through:
- `bot.inbound_events` (source of truth, auto-generated)
- `bot.job_queue` (copied on job creation)
- `bot.messages` (both inbound and outbound)
- `bot.conversations.context.debug.last_run` (ephemeral snapshot)

The same `trace_id` is reused on job retries to maintain continuity.

## Structured Logging

JSON logs are emitted to stdout for each processing run:

```json
{
  "type": "processing_run",
  "ts": "2025-01-29T12:34:56.789Z",
  "tenant_slug": "demo-clinic",
  "contact_id": "uuid",
  "conversation_id": "uuid",
  "trace_id": "uuid",
  "route": "offer_slots",
  "signals": {"day": "monday", "time_window": "morning", "explicit_time": null},
  "calendar": {"ok": true, "returned_slots_count": 12, "provider_trace_id": "ghl-xyz"},
  "offered_slots": [{"iso": "2025-01-30T09:00:00Z", "human": "Thursday 09:00"}],
  "chosen_slot": null,
  "transition": {"from": "start", "to": "offer_slots"}
}
```

## Debug Snapshot

Stored in `conversation.context.debug.last_run` (overwrite-only):

```json
{
  "at": "2025-01-29T12:34:56.789Z",
  "route": "offer_slots",
  "signals": {"day": "monday", "time_window": "morning"},
  "slot_count": 2,
  "chosen_slots": [{"iso": "...", "human": "..."}],
  "transition": {"from": "start", "to": "offer_slots"}
}
```

## Example Postgres Queries

### 1. Trace a single journey by trace_id

```sql
-- Find all records for a specific trace_id
WITH trace AS (SELECT 'YOUR-TRACE-ID-HERE'::uuid AS tid)

SELECT 'inbound_event' AS source, ie.received_at AS ts, ie.event_type, ie.channel, ie.payload->>'text' AS text
FROM bot.inbound_events ie, trace
WHERE ie.trace_id = trace.tid

UNION ALL

SELECT 'job' AS source, jq.created_at AS ts, jq.job_type, jq.status, jq.last_error
FROM bot.job_queue jq, trace
WHERE jq.trace_id = trace.tid

UNION ALL

SELECT 'message_' || m.direction AS source, m.created_at AS ts, m.direction, m.channel, m.text
FROM bot.messages m, trace
WHERE m.trace_id = trace.tid

ORDER BY ts ASC;
```

### 2. Find conversation by trace_id

```sql
SELECT c.conversation_id, c.contact_id, c.status, c.last_step,
       c.context->'debug'->'last_run' AS debug_snapshot,
       c.context->'last_offer' AS last_offer,
       c.context->'pending_booking' AS pending_booking,
       c.context->'booked_booking' AS booked_booking
FROM bot.conversations c
WHERE c.conversation_id = (
  SELECT m.conversation_id
  FROM bot.messages m
  WHERE m.trace_id = 'YOUR-TRACE-ID-HERE'::uuid
  LIMIT 1
);
```

### 3. Full message timeline for a trace

```sql
SELECT
  m.created_at,
  m.direction,
  m.text,
  m.payload->'route' AS route,
  m.payload->'send_status' AS send_status,
  m.payload->'calendar_check'->'ok' AS calendar_ok,
  m.payload->'booking_result'->'success' AS booking_ok
FROM bot.messages m
WHERE m.trace_id = 'YOUR-TRACE-ID-HERE'::uuid
ORDER BY m.created_at ASC;
```

### 4. Find failed jobs with their trace_ids

```sql
SELECT
  jq.job_id,
  jq.trace_id,
  jq.status,
  jq.attempts,
  jq.last_error,
  jq.created_at,
  ie.channel_address,
  ie.payload->>'text' AS user_text
FROM bot.job_queue jq
JOIN bot.inbound_events ie ON ie.inbound_event_id = jq.inbound_event_id
WHERE jq.status = 'failed'
ORDER BY jq.created_at DESC
LIMIT 20;
```

### 5. Trace a contact's full journey (all trace_ids)

```sql
SELECT DISTINCT ON (ie.trace_id)
  ie.trace_id,
  ie.received_at,
  ie.event_type,
  ie.payload->>'text' AS first_text,
  (SELECT COUNT(*) FROM bot.messages m WHERE m.trace_id = ie.trace_id) AS message_count,
  (SELECT jq.status FROM bot.job_queue jq WHERE jq.trace_id = ie.trace_id LIMIT 1) AS job_status
FROM bot.inbound_events ie
JOIN bot.contacts c ON c.tenant_id = ie.tenant_id
  AND c.channel = ie.channel
  AND c.channel_address = ie.channel_address
WHERE c.contact_id = 'YOUR-CONTACT-ID-HERE'::uuid
ORDER BY ie.trace_id, ie.received_at ASC;
```

### 6. Recent processing runs with calendar failures

```sql
SELECT
  m.trace_id,
  m.created_at,
  m.conversation_id,
  m.payload->'calendar_check'->>'reason' AS failure_reason,
  m.payload->'calendar_check'->>'checked_at' AS checked_at
FROM bot.messages m
WHERE m.direction = 'outbound'
  AND m.payload->'calendar_check'->>'ok' = 'false'
ORDER BY m.created_at DESC
LIMIT 20;
```

## Context Keys Reference

The debug snapshot lives at `context.debug.last_run` and does NOT interfere with:
- `context.preference` - user scheduling preferences
- `context.last_offer` - active slot offer
- `context.pending_booking` - awaiting confirmation
- `context.booked_booking` - immutable booking record
- `context.lead_touchpoint` - new_lead first-touch record
