# Intents & Entrypoints (v1)

## Entrypoints
1) inbound_message
- user sends message
- goal: progress toward booking conversationally

2) new_lead
- form submit / website interaction
- SLA: first touch within 60 seconds
- goal: prompt a reply and move toward booking

## Intents (v1)
- booking_request
- booking_confirmation
- booking_change
- greeting_or_unclear
- customer_service (future)

## State-aware intent priority
If pending_booking exists:
- treat message as confirmation OR change_request first

If last_offer exists:
- treat message as slot selection OR change_request first
