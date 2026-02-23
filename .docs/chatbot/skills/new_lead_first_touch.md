# Skill: new_lead_first_touch (v2)

Triggered by new_lead event.
SLA: send first message within 60 seconds.

## What changed in v2

First-touch now fetches 2 calendar slots immediately and includes them in the message.
Lead can reply "1" or "2" and the booking is made in the next message — no extra back-and-forth.

## Message goal
- warm, conversational
- offers 2 specific slots immediately (no "what day suits you?" question)
- LLM handles all follow-up from here

## Default message (no custom template)

**2 slots available:**
```
Hey {name} — thanks for reaching out. Want to get you booked in quickly. I've got two slots:
1) Friday 09:15
2) Monday 14:00
Reply 1 or 2 to choose.
```

**1 slot available:**
```
Hey {name} — thanks for reaching out. Want to get you booked in quickly. I've got one slot:
1) Friday 09:15
Reply 1 to book.
```

**No slots (calendar error or no availability):**
```
Hey {name} — thanks for reaching out. Want to get you booked in quickly. What day and time works best for you?
```

## Custom template (per tenant)

Set `tenant.settings.bot.first_touch_template`. Supports:
- `{name_part}` — " Sarah" or "" if no name
- `{slot_1}` — formatted first slot (e.g. "Friday 09:15")
- `{slot_2}` — formatted second slot (e.g. "Monday 14:00")

Example:
```
"Hey{name_part}! I saw your enquiry — want to get you a call booked in. Two options:\n1) {slot_1}\n2) {slot_2}\nWhich works?"
```

## Implementation
- `processor.py: _handle_new_lead()` — fetches slots via `_handle_offer_slots(_NullRouteInfo())`, builds message via `_build_first_touch_text()`
- `last_offer` stored in context so LLM knows offered slots when lead replies
