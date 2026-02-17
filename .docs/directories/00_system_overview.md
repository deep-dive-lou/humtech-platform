# System Overview (v1)

## What this is
A CRM-agnostic, multi-tenant booking agent.

Inbound capture stays in n8n:
webhook → Postgres (bot.inbound_events + bot.job_queue) → 200 OK.

A coded worker service processes jobs:
- loads inbound_event
- upserts contact + conversation
- inserts inbound message
- extracts intent + time constraints (LLM later / heuristic now)
- checks calendar availability (adapter)
- creates outbound message
- sender sends outbound via messaging adapter
- updates Postgres state

## Guarantees
- Durable state in Postgres
- Idempotent processing (safe for retries + concurrency)
- Adapter pattern for tenant integrations
- LLM used only for interpretation (intent/entities), never orchestration

## Tenancy model
Every record is scoped by tenant_id.
Adapters are selected per tenant via core.tenants.
Secrets are env var based, never DB stored.

## Entry points
- inbound_message (user text)
- new_lead (form submit / website interaction)
