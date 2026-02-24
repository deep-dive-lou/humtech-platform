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
Reply with JSON only — no explanation, no markdown.

Intent options:
- "select_slot": lead is accepting one of the currently offered slots. Use when they reference an offered slot by position or by a day/time that matches one of the offered slots (e.g. "the first", "that one", "1.15 is great", "13:15"). Match by time if mentioned — slot_index=0 for first offered slot, 1 for second. If they say a time like "1.15" or "13:15", find which offered slot has that time and use its index. Do NOT use if they mention a day or time that does not match any of the currently offered slots.
- "request_specific_time": lead is asking for a SINGLE exact time (e.g. "do you have 4:35?", "can I do Tuesday at 3pm?", "what about 9:30 on Friday?"). Use preferred_day + explicit_time. Do NOT use for time ranges like "between 2-5" or "sometime this afternoon".
- "request_slots": lead gives broad availability — a day, time of day, or time range (e.g. "anything Wednesday?", "got anything in the afternoon?", "I can do Tuesday afternoon between 2-5", "between 2 and 5", "different day?", "what about Friday?"). Also use this when they propose a day that was NOT in the offered slots. Use preferred_day + preferred_time.
- "wants_human": lead wants to speak to a person or has a complex question
- "decline": lead is not interested
- "unclear": anything else → compose a clarifying reply

Rules:
- If offered slots are active and lead accepts or references a matching offered slot → "select_slot"
- If lead mentions a DIFFERENT day or time not in the current offered slots → "request_slots" or "request_specific_time"
- slot_index is 0, 1, or null
- preferred_day: the day they ARE requesting, or null. Ignore rejected/negated days — if they say "Tuesday doesn't work, how about Friday?" → preferred_day is "friday". If they say "not Monday, what about Wednesday?" → preferred_day is "wednesday".
- explicit_time: for request_specific_time only — the exact time they asked for as a string (e.g. "4:35", "9:30", "15:00"). Use 24h if obvious, otherwise as stated.
- preferred_time: for request_slots only — "morning", "afternoon", "evening", or null
- reply_text: for select_slot/wants_human/decline/unclear → full reply. For request_specific_time and request_slots → leave as empty string "" — the system will compose the slot response.
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
