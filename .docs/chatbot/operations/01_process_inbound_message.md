# Process Inbound Message (v2)

Steps:
1) claim job
2) load inbound_event
3) upsert contact (tenant_id + channel + channel_address)
4) upsert open conversation (one open per contact)
5) load tenant settings (bot config, LLM config, calendar config)
6) **new_lead**: fetch 2 slots (1 morning + 1 afternoon), send first-touch with slots, store last_offer — return
7) insert inbound message (idempotent via provider_msg_id + dedupe_key)
8) **inbound_message**:
   a. if booked_booking exists → reply idempotent confirmation, done
   b. load recent messages (last 20) for LLM context
   c. get active offered_slots from last_offer (empty if expired/none)
   d. call `process_inbound_message()` → intent + reply_text + preferred_day + preferred_time + explicit_time
   e. **select_slot + should_book=true** → book immediately via GHL; reply "Booked ✅ ..."
   f. **request_specific_time** → parse explicit_time to float hour via `_parse_explicit_time_to_hour()`
      → `_find_nearest_slot()` within 45-min tolerance
      → if found: book immediately → "Booked ✅ ..."
      → if not found: `_find_two_nearest_slots()` → "I don't have {time} I'm afraid. Nearest I've got is X or Y"
   g. **request_slots** → `_handle_offer_slots()` with preferred_day + preferred_time signals
      → if preferred_day unavailable: prepend "I don't have anything on {day} I'm afraid —"
      → offer 1 morning + 1 afternoon slot
   h. **wants_human / decline / unclear** → note in context, reply as LLM composed
9) write context updates
10) insert outbound message row (send_status=pending)
11) write debug snapshot to context
12) mark job done or retry

## Time parsing notes

`_parse_explicit_time_to_hour(text)` → float:
- "4:35" → 16.583 (times <8 with no am/pm assumed pm — business hours)
- "9am" → 9.0
- "9:30" → 9.5
- "16:00" → 16.0

`pick_soonest_two_slots(slots, ..., target_hour=None)`:
- When `target_hour` is set, sorts by proximity to that hour → returns 2 nearest (chronologically sorted)
- When None, returns soonest + contrasting morning/afternoon slot (original behaviour)
