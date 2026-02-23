# Conversation Flow (v2 — LLM-driven)

State is stored ONLY in `bot.conversations.context` (jsonb).
There is no other memory.

The flow is LLM-driven from first reply onwards.
LLM classifies intent AND composes the outbound reply.
Code handles booking execution and context writes.

---

## Core Context Keys

| Key | Purpose |
|---|---|
| `lead_touchpoint` | Records first-touch time, channel, message_id |
| `last_offer` | The slots currently offered (ISO timestamps + display + calendar_check) |
| `booked_booking` | Final confirmed booking — immutable once set |
| `handoff_requested` | Set when LLM returns wants_human intent |
| `declined` | Set when LLM returns decline intent |
| `debug` | Last run debug snapshot (route, signals, slots, transition) |
| `_last_step` | Last route label for state_transition logging |

`pending_booking` is no longer used — booking is immediate when intent is clear.

---

## Context Shapes

### last_offer
```json
{
  "slots": ["ISO", "ISO"],
  "offered_slots": ["ISO", "ISO"],
  "constraints": { "day": null, "time_window": null, "explicit_time": null },
  "timezone": "Europe/London",
  "offered_at": "ISO",
  "calendar_check": {
    "ok": true,
    "calendar_id": "string",
    "returned_slots_count": 350,
    "filtered_slots_count": 38,
    "checked_range": { "start": "ISO", "end": "ISO" },
    "reason": null,
    "checked_at": "ISO"
  }
}
```

Offer expiry: 2 hours. Expired slots are not sent to LLM as active options.

### booked_booking
```json
{
  "slot": "ISO",
  "booking_id": "string",
  "booked_at": "ISO"
}
```
Once set, booking is immutable.

### lead_touchpoint
```json
{
  "first_touch_at": "ISO",
  "channel": "sms",
  "message_id": "uuid"
}
```

---

## Processing Order

### new_lead
1. Check idempotency (lead_touchpoint already set → skip)
2. Fetch 2 calendar slots (no day/time signals — just soonest two)
3. Build first-touch message with slots using `_build_first_touch_text()`
   - Respects `tenant.settings.bot.first_touch_template` if configured
   - Placeholders: `{name_part}`, `{slot_1}`, `{slot_2}`
4. Store `last_offer` in context so LLM knows what was offered when lead replies
5. Send message

### inbound_message
1. **Booked idempotency** — if `booked_booking` exists, reply with confirmation only. No further action.
2. **LLM processes message** (`process_inbound_message()` in `llm.py`)
   - Input: conversation history (last 20 messages), offered_slots (if any, if not expired), tenant_context
   - Output: `{intent, slot_index, should_book, should_handoff, preferred_day, preferred_time, explicit_time, reply_text}`
   - For `request_specific_time` and `request_slots`, LLM returns `reply_text=""` — system composes the slot response
3. **Handle intent**:
   - `select_slot + should_book=true` → book immediately via GHL API → reply "Booked ✅ ..."
   - `request_specific_time` → parse `explicit_time` → find nearest calendar slot within 45-min tolerance → if found: book immediately; if not found: offer 2 nearest alternatives
   - `request_slots` → fetch fresh slots via `_handle_offer_slots()` with `preferred_day`/`preferred_time` signals → reply with new offer (prepended with preamble if day unavailable)
   - `wants_human + should_handoff=true` → note in context, reply as LLM composed
   - `decline` → note in context, reply as LLM composed
   - `unclear` → reply as LLM composed (clarifying question)
4. Write context updates (booked_booking, last_offer, handoff_requested, declined)
5. Insert outbound message row (send_status=pending)

---

## Slot Offering Rules (unchanged)
- Always check calendar first
- Offer exactly 2 slots when possible (soonest + contrasting)
- `_handle_offer_slots()` handles filtering by day/time signals and availability windows
- `calendar_check` always written to `last_offer` for observability

---

## LLM Config (per tenant)

`tenant.settings.llm`:
```json
{
  "enabled": true,
  "model": "claude-haiku-4-5-20251001",
  "temperature": 0.0
}
```

`tenant.settings.bot`:
```json
{
  "context": "HumTech, a revenue acceleration consultancy",
  "first_touch_template": "Hey{name_part} — ...",
  "reengagement": { "enabled": true, "delay_hours": 6, "max_attempts": 2 },
  "handoff_ghl_user_id": "abc123"
}
```

If `llm.enabled = false` or `model = "stub"`, pattern-based stub mode is used (no API call).
