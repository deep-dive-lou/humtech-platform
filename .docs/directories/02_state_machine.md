# Conversation State Machine (v1)

State is stored ONLY in `bot.conversations.context` (jsonb).
There is no other memory.

This system is deterministic.
LLMs may interpret language, but code controls state transitions.

---

## Core Context Keys

Only these keys may exist in `context`:

- preference
- last_offer
- pending_booking
- booked_booking
- lead_touchpoint

If a key is not listed here, it must not be created.

---

## Context Shapes (v1)

### preference
Latest user constraints.

```json
{
  "day": "friday",
  "time_window": "morning",
  "explicit_time": null,
  "free_text": "Friday morning",
  "updated_at": "ISO"
}
last_offer
Last calendar offer made to the user.

json
Copy code
{
  "slots": ["ISO", "ISO"],
  "constraints": { "day": "friday", "time_window": "morning", "explicit_time": null },
  "timezone": "Europe/London",
  "offered_at": "ISO",
  "calendar_check": {
    "ok": true,
    "calendar_id": "string",
    "returned_slots_count": 350,
    "filtered_slots_count": 38,
    "checked_range": { "start": "ISO", "end": "ISO" },
    "reason": null
  }
}
Offer expiry: 2 hours

pending_booking
Slot selected but not yet confirmed.

json
Copy code
{
  "slot": "ISO",
  "created_at": "ISO"
}
Expires after 2 hours.

booked_booking
Final confirmed booking.

json
Copy code
{
  "slot": "ISO",
  "booking_id": "string",
  "booked_at": "ISO"
}
Once set, booking is immutable.

lead_touchpoint
Tracks first-touch SLA for new leads.

json
Copy code
{
  "first_touch_at": "ISO",
  "channel": "sms",
  "message_id": "uuid"
}
Entrypoints
inbound_message
Triggered by incoming user message.

Goal: progress toward booking with minimal back-and-forth.

new_lead
Triggered by form / website interaction.

Hard rule:

First outbound message must be sent within 60 seconds

Goal:

Prompt for scheduling intent (not booking confirmation)

Processing Order (Strict)
Inbound messages are processed in this order:

1. Booked idempotency
If user references a slot already in booked_booking → respond with confirmation only.

No further action.

2. Pending booking resolution
If pending_booking exists:

Interpret message as one of:

CONFIRM

DECLINE

CHANGE_REQUEST

UNCLEAR

Rules:

CHANGE_REQUEST overrides all others.

Actions:

CONFIRM → book slot → set booked_booking, clear pending_booking + last_offer

DECLINE → clear pending_booking, re-offer slots

CHANGE_REQUEST → clear pending_booking, update preference, generate new offer

UNCLEAR → short clarification (one question max)

3. Slot selection from last_offer
If last_offer exists and is not expired:

Try to match user text to a slot (ordinal, time, or explicit)

If matched → create pending_booking

If change-of-mind detected → update preference, regenerate offer

Else → re-display 2 options

4. Normal routing
If no active offer or pending booking:

Extract constraints into preference

If enough info → check calendar and generate offer

Else → ask one clarifying question

Slot Offering Rules (Hard)
When generating offers:

Always check calendar first

Offer exactly 2 slots when possible

Prefer:

Closest matching slot

Contrasting slot (morning vs afternoon) or next-closest

Never ask unnecessary follow-up questions if calendar data exists.

Change-of-Mind Rule (Global)
At any time, if user expresses new constraints:

Update preference

Clear pending_booking

Regenerate offer closest to new preference

Diagnostics Requirement
Every calendar check must write calendar_check into last_offer.

This enables full debugging via DB inspection only.

markdown
Copy code
