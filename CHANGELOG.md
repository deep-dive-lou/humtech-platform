# Changelog

## [Unreleased] — 2026-02-23 — GHL Live Integration

### Bot — End-to-End GHL Flow

**GHL Webhook fixes (`app/bot/webhook.py`)**
- Read `event_type`, `text`, `contactId`, `display_name` from `customData` (GHL nests custom fields there, not at root level)
- Strip whitespace from `customData` keys (GHL appends trailing tabs)

**GHL API fixes (`app/adapters/`)**
- Add `Version: 2021-07-28` header to all GHL API calls (messaging + calendar) — required or API returns 401
- Booking endpoint corrected: `POST /calendars/events/appointments` (was `/calendars/events`)
- Booking body: include `locationId` from credentials — required for 201, without it returns 403
- Token expiry: `_is_expired()` now treats missing `expires_at` as expired

**Slot filtering (`app/adapters/calendar/ghl.py`, `app/bot/processor.py`)**
- Apply `explicit_time` as a floor after `time_window` filter — "between 1-3" returns slots from 13:00, not 12:00
- `request_slots` handler inherits `time_window` + `explicit_time` from pattern matcher when LLM returns null
- LLM prompt updated: time ranges ("between 2-5") → `request_slots`, not `request_specific_time`
- LLM prompt updated: `select_slot` now includes time-matching examples ("1.15", "13:15")
- `request_specific_time` fallback preserves `preferred_day` instead of using null `_NullRouteInfo`

**Booking (`app/bot/processor.py`)**
- `select_slot`: removed `should_book=True` gate — book immediately when `slot_index` is valid (LLM rarely sets `should_book`)
- First name only in greeting (split `display_name` on whitespace)

**DB / Infrastructure**
- `humtech_bot` granted SELECT/INSERT/UPDATE on `core.tenants` + `core.tenant_credentials`
- `location_id` stored in GHL credentials (`V7m7dlgTERFjn0t4soSv`)
- New scripts: `scripts/store_location_id.py`, `scripts/reset_contact.py`, `scripts/reset_failed_messages.py`, `scripts/check_credentials.py`

### Remaining
- Re-engagement worker (follow-up after N hours silence)
- GHL handoff: assign conversation to human on `wants_human` intent
- Disconnect GHL internal booking bot