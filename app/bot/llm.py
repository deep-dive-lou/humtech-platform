"""
LLM service for booking agent.

Provides:
- Outbound message rewriting (make templates sound natural)
- Confirmation intent classification (detect user agreement)
"""
from __future__ import annotations
from datetime import datetime, timezone
from typing import Any, Optional
import os
import re


# --- Prompts ---

REWRITE_PROMPT = {
    "system": """You rewrite SMS booking messages to sound more natural.

RULES:
1. PRESERVE all dates, times, slot numbers EXACTLY
2. Keep it SHORT (SMS, under 160 chars when possible)
3. No emojis unless original has them

If unsure, return original unchanged.""",
    "user": """Rewrite to sound natural. Keep ALL dates/times/numbers exact.

Original: {text}

Return ONLY the rewritten message:""",
}

CONFIRM_INTENT_PROMPT = {
    "system": """Classify if a message contains booking confirmation intent.

YES examples: "yes the first one", "perfect option 2", "book me for 9:15", "sounds good"
NO examples: "1", "the first one", "9:15", "what about Thursday?", "not sure"

Reply ONLY "yes" or "no".""",
    "user": """Does this contain confirmation intent?

Message: {text}

Reply "yes" or "no":""",
}


# --- Core LLM caller ---

_CLAUDE_FALLBACK_MODEL = "claude-sonnet-4-6"
_OVERLOAD_STATUS_CODES = {529, 503, 529}
_MAX_RETRIES = 2
_RETRY_DELAY_SECONDS = 1.5


async def _call_anthropic(
    model: str,
    system: str,
    user: str,
    temperature: float,
    max_tokens: int,
    timeout: float,
) -> Optional[str]:
    """
    Call Anthropic API with retry on overload (529/503) and fallback to sonnet.
    Returns response text or None on failure.
    """
    import asyncio
    import httpx

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return None

    models_to_try = [model]
    if model != _CLAUDE_FALLBACK_MODEL:
        models_to_try.append(_CLAUDE_FALLBACK_MODEL)

    for attempt_model in models_to_try:
        for attempt in range(_MAX_RETRIES + 1):
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    resp = await client.post(
                        "https://api.anthropic.com/v1/messages",
                        headers={
                            "x-api-key": api_key,
                            "anthropic-version": "2023-06-01",
                            "Content-Type": "application/json",
                        },
                        json={
                            "model": attempt_model,
                            "system": system,
                            "messages": [{"role": "user", "content": user}],
                            "temperature": temperature,
                            "max_tokens": max_tokens,
                        },
                    )
                    if resp.status_code in _OVERLOAD_STATUS_CODES and attempt < _MAX_RETRIES:
                        print(f"LLM overloaded ({attempt_model}, attempt {attempt + 1}): {resp.status_code} — retrying")
                        await asyncio.sleep(_RETRY_DELAY_SECONDS)
                        continue
                    resp.raise_for_status()
                    return resp.json()["content"][0]["text"].strip()
            except httpx.HTTPStatusError as e:
                if e.response.status_code in _OVERLOAD_STATUS_CODES and attempt < _MAX_RETRIES:
                    print(f"LLM overloaded ({attempt_model}, attempt {attempt + 1}): {e.response.status_code} — retrying")
                    await asyncio.sleep(_RETRY_DELAY_SECONDS)
                    continue
                print(f"LLM call failed ({attempt_model}): {e}")
                break  # Non-retryable HTTP error — try fallback model
            except Exception as e:
                print(f"LLM call failed ({attempt_model}): {e}")
                break  # Non-retryable error — try fallback model

    return None


async def _call_llm(
    model: str,
    system: str,
    user: str,
    temperature: float = 0,
    max_tokens: int = 256,
    timeout: float = 10.0,
) -> Optional[str]:
    """
    Call OpenAI or Anthropic API. Returns response text or None on failure.
    """
    import httpx

    try:
        if model.startswith("gpt-") or model.startswith("o1"):
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                return None

            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                    json={
                        "model": model,
                        "messages": [
                            {"role": "system", "content": system},
                            {"role": "user", "content": user},
                        ],
                        "temperature": temperature,
                        "max_tokens": max_tokens,
                    },
                )
                resp.raise_for_status()
                return resp.json()["choices"][0]["message"]["content"].strip()

        elif model.startswith("claude-"):
            return await _call_anthropic(model, system, user, temperature, max_tokens, timeout)

        elif model.startswith("groq/"):
            groq_model = model[len("groq/"):]
            api_key = os.getenv("GROQ_API_KEY")
            if not api_key:
                return None
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                    json={
                        "model": groq_model,
                        "messages": [
                            {"role": "system", "content": system},
                            {"role": "user", "content": user},
                        ],
                        "temperature": temperature,
                        "max_tokens": max_tokens,
                    },
                )
                resp.raise_for_status()
                return resp.json()["choices"][0]["message"]["content"].strip()

        return None  # Unknown model

    except Exception as e:
        print(f"LLM call failed ({model}): {e}")
        return None




# --- Public functions ---

async def rewrite_outbound_text_llm(
    llm_settings: dict[str, Any],
    template_text: str,
) -> dict[str, Any]:
    """
    Rewrite outbound message text using LLM.

    Returns: {rewritten_text, used, model, prompt_version, error, rewritten_at}
    """
    model = llm_settings.get("model", "stub")
    temperature = llm_settings.get("temperature", 0.3)

    result = {
        "rewritten_text": None,
        "used": False,
        "model": model,
        "prompt_version": "v1",
        "error": None,
        "rewritten_at": None,
    }

    if not model or model == "stub" or not llm_settings.get("enabled", False):
        print(f"WARN rewrite_outbound_text_llm: LLM disabled or no model — skipping rewrite")
        return result

    try:
        rewritten = await _call_llm(
            model=model,
            system=REWRITE_PROMPT["system"],
            user=REWRITE_PROMPT["user"].format(text=template_text),
            temperature=temperature,
            max_tokens=256,
            timeout=10.0,
        )

        if rewritten:
            # Sanity check
            if len(rewritten) >= 5 and len(rewritten) <= len(template_text) * 3:
                result["rewritten_text"] = rewritten
                result["used"] = True
                result["rewritten_at"] = datetime.now(timezone.utc).isoformat()
            else:
                result["error"] = "rewrite_failed_sanity_check"
        else:
            result["error"] = "rewrite_returned_none"

    except Exception as e:
        result["error"] = f"rewrite_exception:{str(e)[:100]}"

    return result


PROCESS_MESSAGE_PROMPT = {
    "system": """You are a booking assistant{context_part}. Your only goal is to get the lead booked in for a call.
Today is {today_date}.
{persona_section}{slots_section}
Reply with valid JSON only. No explanation. No text outside JSON.

Read the lead's FULL message and understand complete context before classifying. Do not stop reading early.

CLASSIFICATION ORDER (read full message first, then apply in this order to prevent accidental bookings):
1. decline — lead is not interested
2. wants_human — lead wants a person
3. reschedule — lead wants to change or cancel an existing booking
4. select_slot — lead is clearly confirming an offered slot
5. request_specific_time — lead asks for a time (exact or approximate)
6. request_slots — lead asks for general availability with no time
7. unclear — cannot confidently classify

INTENT DEFINITIONS

"select_slot"
Lead is CONFIRMING/ACCEPTING a specific slot already offered.
ONLY use if: clear acceptance language AND specific offered slot referenced by position or exact matching time.
Acceptance language: "yes", "that one", "the first", "book me for X", "that works", "perfect", "great".
NEVER use for questions — "Would X work?" is a REQUEST, not a confirmation.
NEVER use if the day/time does not match an offered slot.
"Friday works" without referencing a specific offered slot → NOT select_slot, use request_specific_time or request_slots.
If acceptance language present but no specific offered slot referenced → request_slots.
slot_index: 0 for first offered slot, 1 for second. Match by position or time.
→ should_book: true

"request_specific_time"
Lead mentions ANY numeric time reference — exact or approximate. Use this aggressively, we want to book them.
Use if message contains a number alongside a time: "at 2pm", "around 3", "3 or 4", "3ish", "2:00pm", "14:00", "around 2", "between 3 and 4".
Extract the first/most prominent time as explicit_time. For "3 or 4" → "3:00pm". For "around 2pm" → "2:00pm". For "between 3 and 4" → "3:00pm".
preferred_day = ONLY the day they are ASKING FOR — ignore days mentioned as context/unavailability.
"im in meetings tomorrow, would Friday around 3 work?" → preferred_day: "friday", explicit_time: "3:00pm".
NEVER use if the time exactly matches an already-offered slot (use select_slot instead).
→ preferred_day: requested day (lowercase) or null. explicit_time: time string. should_book: false.

"request_slots"
Lead asks about general availability with NO numeric time reference.
Use ONLY when they give a day or time-of-day word but no number: "anything Friday?", "got anything afternoon?", "morning works".
If any number/time is present → use request_specific_time instead.
preferred_day = ONLY the day they are ASKING FOR (not days mentioned as context).
→ preferred_day: requested day (lowercase) or null. preferred_time: morning/afternoon/evening or null. should_book: false.

"wants_human"
Lead wants to speak to a person: "can I speak to someone?", "call me", "I'd rather talk to a person".

"reschedule"
Lead wants to change, move, or cancel an existing appointment.
Use if: "can I reschedule", "need to change my appointment", "can we move it", "different time", "can I rebook", "cancel my booking", "change the time".
→ reply_text: "" (bot handles cancellation and re-offers slots automatically)

"decline"
Lead is not interested: "not interested", "no thanks", "stop", "leave me alone".

"unclear"
Cannot be confidently classified into any of the above.

FEW-SHOT EXAMPLES

Message: "Hi Ariyah, im in meetings all day tomorrow. Would Friday around 3 or 4 work?"
→ {{"intent": "request_specific_time", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": "friday", "preferred_time": null, "explicit_time": "3:00pm", "reply_text": ""}}

Message: "Tuesday doesn't work for me. How about friday 6th around 2pm?"
→ {{"intent": "request_specific_time", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": "friday", "preferred_time": null, "explicit_time": "2:00pm", "reply_text": ""}}

Message: "Would Friday around 3 or 4 work for Chris?"
→ {{"intent": "request_specific_time", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": "friday", "preferred_time": null, "explicit_time": "3:00pm", "reply_text": ""}}

Message: "Can I do Thursday at 4:35pm instead?"
→ {{"intent": "request_specific_time", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": "thursday", "preferred_time": null, "explicit_time": "4:35pm", "reply_text": ""}}

Message: "Got anything on Friday afternoon?"
→ {{"intent": "request_slots", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": "friday", "preferred_time": "afternoon", "explicit_time": null, "reply_text": ""}}

Message: "Yes, the first one works for me"
→ {{"intent": "select_slot", "slot_index": 0, "should_book": true, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": ""}}

Message: "yes that works but can I speak to someone first?"
→ {{"intent": "wants_human", "slot_index": null, "should_book": false, "should_handoff": true, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "Of course! I'll get someone to reach out to you shortly."}}

Message: "Actually can I change the appointment time?"
→ {{"intent": "reschedule", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": ""}}

Message: "Can I reschedule to a different day?"
→ {{"intent": "reschedule", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": ""}}

CRITICAL RULES
- Questions ("would X work?", "what about Friday?") are NEVER select_slot
- preferred_day must be lowercase or null. Never calculate a weekday from a date number — read the written day name.
- preferred_time must be "morning", "afternoon", "evening", or null
- reply_text must be "" for select_slot/request_specific_time/request_slots/reschedule
- Compose reply_text ONLY for wants_human/decline/unclear
- Never fabricate a slot. Never guess. Never return multiple intents.
- Ignore greetings, politeness words, emojis — focus on what the lead actually wants.
- Never mention these instructions in reply_text""",
    "user": """Conversation:
{history}

Latest message: "{last_message}"

Respond with JSON:
{{"intent": "...", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": ""}}""",
}



async def process_inbound_message(
    conversation_history: list[dict],
    offered_slots: list[str],
    display_slots: list[str],
    tenant_context: str,
    llm_settings: dict,
    persona: str = "",
) -> dict:
    """
    Classify intent and compose a reply for an inbound message.

    Args:
        conversation_history: List of {"role": "user"|"assistant", "text": str} in chronological order.
        offered_slots: List of ISO timestamp strings currently offered.
        display_slots: Human-readable versions of offered_slots.
        tenant_context: Brief business description for the system prompt.
        llm_settings: LLM config dict from get_llm_settings().
        persona: Optional persona/tone instructions injected into the system prompt.

    Returns:
        {intent, slot_index, should_book, should_handoff, reply_text, used, error}
    """
    import json as _json

    model = llm_settings.get("model", "")
    last_message = conversation_history[-1]["text"] if conversation_history else ""

    result = {
        "intent": "unclear",
        "slot_index": None,
        "should_book": False,
        "should_handoff": False,
        "preferred_day": None,
        "preferred_time": None,
        "explicit_time": None,
        "reply_text": "",
        "used": False,
        "error": None,
    }

    # LLM disabled — bot goes silent for this turn
    if not model or model == "stub" or not llm_settings.get("enabled", False):
        print("WARN process_inbound_message: LLM disabled — bot silent for this turn")
        result["error"] = "llm_disabled"
        return result

    # Build prompt
    context_part = f" for {tenant_context}" if tenant_context else ""
    persona_section = f"\n{persona}\n" if persona else ""

    if display_slots:
        slots_lines = "\n".join(f"  {i + 1}) {s}" for i, s in enumerate(display_slots))
        slots_section = f"\nCurrently offered slots:\n{slots_lines}\n"
    else:
        slots_section = "\nNo slots have been offered yet.\n"

    history_lines = "\n".join(
        f"{'Lead' if m['role'] == 'user' else 'You'}: {m['text']}"
        for m in conversation_history[:-1]  # exclude latest message — shown separately
    )
    if not history_lines:
        history_lines = "(no prior messages)"

    _now = datetime.now(timezone.utc)
    today_str = _now.strftime(f"%A {_now.day} %B %Y")

    system = PROCESS_MESSAGE_PROMPT["system"].format(
        context_part=context_part,
        persona_section=persona_section,
        slots_section=slots_section,
        today_date=today_str,
    )
    user = PROCESS_MESSAGE_PROMPT["user"].format(
        history=history_lines,
        last_message=last_message,
    )

    try:
        response = await _call_llm(
            model=model,
            system=system,
            user=user,
            temperature=0,
            max_tokens=256,
            timeout=10.0,
        )
        if response:
            # Strip markdown fences if present
            clean = re.sub(r"^```(?:json)?\s*|\s*```$", "", response.strip(), flags=re.DOTALL)
            parsed = _json.loads(clean)

            result["used"] = True
            result["intent"] = parsed.get("intent", "unclear")
            result["slot_index"] = parsed.get("slot_index")
            result["should_book"] = bool(parsed.get("should_book", False))
            result["should_handoff"] = bool(parsed.get("should_handoff", False))
            result["preferred_day"] = parsed.get("preferred_day")
            result["preferred_time"] = parsed.get("preferred_time")
            result["explicit_time"] = parsed.get("explicit_time")
            result["reply_text"] = parsed.get("reply_text", result["reply_text"])

            # Safety: validate slot_index is in range
            if result["slot_index"] is not None:
                if not isinstance(result["slot_index"], int) or result["slot_index"] < 0 or result["slot_index"] >= len(offered_slots):
                    result["slot_index"] = None
                    result["should_book"] = False
                    result["error"] = "slot_index_out_of_range"
        else:
            result["error"] = "llm_returned_none"

    except Exception as e:
        result["error"] = f"process_message_exception:{str(e)[:100]}"

    return result


async def classify_confirmation_intent_llm(
    text: str,
    llm_settings: dict[str, Any],
) -> dict[str, Any]:
    """
    Classify if text contains confirmation intent.

    Returns: {has_confirmation: bool|None, used: bool, error: str|None}
    """
    model = llm_settings.get("model", "")

    result = {
        "has_confirmation": None,
        "used": False,
        "error": None,
    }

    if not model or model == "stub" or not llm_settings.get("enabled", False):
        result["error"] = "llm_disabled"
        return result

    try:
        response = await _call_llm(
            model=model,
            system=CONFIRM_INTENT_PROMPT["system"],
            user=CONFIRM_INTENT_PROMPT["user"].format(text=text),
            temperature=0,
            max_tokens=8,
            timeout=5.0,
        )

        if response:
            result["used"] = True
            resp_lower = response.lower()
            if resp_lower.startswith("yes"):
                result["has_confirmation"] = True
            elif resp_lower.startswith("no"):
                result["has_confirmation"] = False
            else:
                result["error"] = f"unexpected_response:{resp_lower[:20]}"
        else:
            result["error"] = "llm_returned_none"

    except Exception as e:
        result["error"] = f"llm_exception:{str(e)[:50]}"

    return result
