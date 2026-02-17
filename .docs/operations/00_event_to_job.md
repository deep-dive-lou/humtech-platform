# Event → Job (v1)

n8n responsibilities:
- validate tenant
- insert into bot.inbound_events (idempotent via tenant_id + dedupe_key)
- insert into bot.job_queue (unique job_type + inbound_event_id)
- return 200 OK quickly

Worker responsibilities:
- claim jobs safely (FOR UPDATE SKIP LOCKED)
- process idempotently
