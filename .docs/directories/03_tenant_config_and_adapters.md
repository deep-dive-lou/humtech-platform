# Tenant Config & Adapters (v1)

## core.tenants
Adapter routing:
- messaging_adapter (text)
- calendar_adapter (text)
Tenant settings:
- settings (jsonb)

Example settings:
{
  "provider": "ghl",
  "timezone": "Europe/London",
  "calendar_id": "..."
}

## Secrets
Tokens/keys are env vars only (never DB).
Recommended naming:
- GHL_API_KEY__TENANT_<TENANT_SLUG>=...
- GHL_LOCATION_ID__TENANT_<TENANT_SLUG>=...
- (optional) GHL_BASE_URL__TENANT_<TENANT_SLUG>=...

## Adapters
Calendar adapter responsibilities:
- get_free_slots(tenant, start_dt, end_dt)
- book_slot(tenant, slot_iso, ...)

Messaging adapter responsibilities:
- send_message(tenant, to_address, text, ...)

Adapters must be pure execution:
- no orchestration
- return structured success/failure + raw response
