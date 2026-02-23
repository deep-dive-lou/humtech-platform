# Intents & Entrypoints (v2 — LLM-driven)

## Entrypoints
1) inbound_message
- user sends message
- goal: progress toward booking conversationally

2) new_lead
- form submit / website interaction
- SLA: first touch within 60 seconds
- goal: prompt a reply and move toward booking
- sends 1 morning + 1 afternoon slot on first touch

## Intents (v2 — LLM classified)

All intents are classified by `process_inbound_message()` in `llm.py` using claude-haiku.

| Intent | When | System action |
|---|---|---|
| `select_slot` | Lead picks one of the currently offered slots (by position, day, time) | Book immediately via GHL → "Booked ✅ ..." |
| `request_specific_time` | Lead asks for an exact time ("do you have 4:35?", "can I do Tuesday at 3pm?") | Parse time → find nearest slot within 45 min → book if free; else offer 2 nearest alternatives |
| `request_slots` | Lead wants different/broader availability ("anything Wednesday?", "got an afternoon slot?") | Fetch fresh slots filtered by `preferred_day`/`preferred_time` → offer 1 morning + 1 afternoon |
| `wants_human` | Lead wants to speak to a person | Note in context, reply as LLM composed |
| `decline` | Lead is not interested | Note in context, reply as LLM composed |
| `unclear` | Anything else | LLM composes a clarifying question |

## LLM Response Shape

```json
{
  "intent": "...",
  "slot_index": null,
  "should_book": false,
  "should_handoff": false,
  "preferred_day": null,
  "preferred_time": null,
  "explicit_time": null,
  "reply_text": ""
}
```

- `preferred_day`: the day the lead wants (e.g. "wednesday"), or null. Understands negation — "can't do Monday" → NOT monday.
- `preferred_time`: for `request_slots` — "morning" | "afternoon" | "evening" | null
- `explicit_time`: for `request_specific_time` — exact time string as stated ("4:35", "9:30", "15:00")
- `reply_text`: LLM composes this for `select_slot`, `wants_human`, `decline`, `unclear`. For `request_specific_time` and `request_slots` → LLM returns `""` and system composes the slot text.

## State-Aware Priority

- If `booked_booking` exists → idempotent confirmation only (no further LLM call)
- Otherwise → LLM classifies intent from full conversation history + currently offered slots context
- LLM is told what slots are currently offered, so it correctly routes "the first one" as `select_slot` not `request_slots`
