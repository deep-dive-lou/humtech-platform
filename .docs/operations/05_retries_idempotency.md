# Retries & Idempotency (v1)

Idempotency:
- inbound_events unique on (tenant_id, dedupe_key)
- job_queue unique on (job_type, inbound_event_id)
- inbound messages unique on (tenant_id, provider, provider_msg_id, direction='inbound')

Retries:
- jobs use attempts + run_after
- outbound uses send_attempts + send_next_at
- never duplicate side effects
