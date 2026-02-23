# System Overview (v2)

## What this is
A CRM-agnostic, multi-tenant booking agent.

Inbound capture stays in n8n:
webhook → Postgres (bot.inbound_events + bot.job_queue) → 200 OK.

A coded worker service processes jobs:
- loads inbound_event
- upserts contact + conversation
- loads tenant settings (bot config, LLM config, calendar config)
- inserts inbound message
- **new_lead**: fetches 2 calendar slots immediately, sends first-touch with slots included
- **inbound_message**: LLM classifies intent + composes reply (see llm.py: process_inbound_message)
- executes booking immediately when intent is clear (no "Reply YES" confirmation step)
- creates outbound message
- sender sends outbound via messaging adapter
- updates Postgres state

## LLM role (v2)
LLM handles all post-first-touch messages:
- classifies intent: select_slot / request_slots / wants_human / decline / unclear
- composes the reply text (SMS-natural, not template-based)
- booking executes immediately when LLM says should_book=true

Stub mode available for testing without API key (pattern matching fallback in llm.py).

## Guarantees
- Durable state in Postgres
- Idempotent processing (safe for retries + concurrency)
- Adapter pattern for tenant integrations
- LLM failure never blocks message sending (fallback to stub/default reply)

## Tenancy model
Every record is scoped by tenant_id.
Adapters are selected per tenant via core.tenants.
Credentials stored encrypted in core.tenant_credentials (with env var fallback).
Bot behaviour configurable per tenant via core.tenants.settings.bot.

## Entry points
- inbound_message (user text)
- new_lead (form submit / website interaction)
