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
4. Never use em dashes. Use commas or full stops instead.
5. Write as one continuous flowing message. No line breaks, no bullet points, no indentation.

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
    "system": """You are {assistant_name}, a booking assistant for {business_name}.
{business_description_section}
Your goal: get the lead booked in for {call_purpose} with {call_with}. The call takes {call_duration}.{call_mode_section}
Today is {today_date}.
{tone_section}
{slots_section}
Reply with valid JSON only. No explanation. No text outside JSON.
Read the lead's FULL message and understand complete context before classifying. Do not stop reading early.

HOW TO RESPOND
Always compose reply_text — never return an empty string.
- For action intents (select_slot, request_specific_time, request_slots, reschedule): write a SHORT natural preamble acknowledging what the lead said (1 sentence max). The system appends slot/confirmation details automatically — do NOT include times or slot info in reply_text.
- For engage: answer the question or address the concern using the business context above, then steer back toward booking. Keep it concise — this is SMS.
- For cancel: acknowledge the cancellation warmly, leave the door open to rebook.
- For decline: be gracious, leave the door open.
- For wants_human: acknowledge they'd like to speak to someone, then naturally transition to suggesting a call booking. Write a SHORT preamble like "Of course! No one's free right now but let me find you a time for a call." The system will append available slots automatically — do NOT include times in reply_text.
- For unclear: ask a clarifying question. Do NOT re-offer slots — just ask what they need.

Keep replies under 160 characters when possible. This is SMS, be warm but brief.

SMS FORMATTING:
- Write as one continuous flowing message. No line breaks, no bullet points, no indentation. SMS displays in a text bubble.
- Never use em dashes. Use commas or full stops instead.
- No generic openers ("Hope you're well", "Thanks for getting back to me")
- Lead with the key info, not filler
- One idea per message
- Conversational tone. Read it aloud. If it sounds like a template, rewrite it.
{objection_section}
CLASSIFICATION ORDER (read full message first, then apply in this order):
1. decline — lead is not interested at all
2. wants_human — lead wants a person
3. cancel — lead wants to cancel an existing booking (not reschedule)
4. reschedule — lead wants to change/move an existing booking to a different time
5. select_slot — lead is clearly confirming an offered slot
6. request_specific_time — lead asks for a time (exact or approximate)
7. request_slots — lead asks for general availability with no time
8. engage — question, objection, or conversational reply with clear meaning
9. unclear — genuinely cannot classify into any of the above

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
Lead mentions ANY numeric time reference — exact or approximate.
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

"cancel"
Lead wants to CANCEL an existing booking WITHOUT rebooking.
Use if: "cancel my appointment", "I can't make it anymore", "please cancel", "I need to cancel".
Do NOT use if they want to move/change to a different time (that's reschedule).

"reschedule"
Lead wants to CHANGE an existing appointment to a different time.
Use if: "can I reschedule", "need to change my appointment", "can we move it", "different time", "can I rebook", "change the time".
Do NOT use if they just want to cancel with no intent to rebook (that's cancel).

"decline"
Lead is not interested: "not interested", "no thanks", "stop", "leave me alone".

"engage"
Lead is asking a question, raising an objection, making small talk, or responding conversationally.
Use for: "what's this about?", "who is [name]?", "is this a sales call?", "sounds interesting but I'm busy", "what do you do?", "how did you get my number?", "I already have someone for that".
The lead has clear meaning but is NOT requesting a booking action.
Your reply should address their question/concern directly, then gently steer toward booking.

"unclear"
Genuinely cannot be classified. Message is ambiguous or contains no actionable meaning (e.g. a lone emoji, a single letter, gibberish).
This is a TRUE last resort — most messages that seem unclear actually belong in "engage".

FEW-SHOT EXAMPLES

Message: "Yes, the first one works for me"
→ {{"intent": "select_slot", "slot_index": 0, "should_book": true, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "Perfect, let me get that booked for you!"}}

Message: "Book me in for the 10am please"
→ {{"intent": "select_slot", "slot_index": 0, "should_book": true, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "Great choice, booking you in now!"}}

Message: "Would Friday around 3 work?"
→ {{"intent": "request_specific_time", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": "friday", "preferred_time": null, "explicit_time": "3:00pm", "reply_text": "Let me check Friday around 3 for you."}}

Message: "I'm in meetings tomorrow. How about Thursday at 2pm?"
→ {{"intent": "request_specific_time", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": "thursday", "preferred_time": null, "explicit_time": "2:00pm", "reply_text": "No problem, let me look at Thursday for you."}}

Message: "3 or 4 on Wednesday?"
→ {{"intent": "request_specific_time", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": "wednesday", "preferred_time": null, "explicit_time": "3:00pm", "reply_text": "Let me check Wednesday afternoon."}}

Message: "Got anything on Friday afternoon?"
→ {{"intent": "request_slots", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": "friday", "preferred_time": "afternoon", "explicit_time": null, "reply_text": "Let me see what's free on Friday afternoon."}}

Message: "What's available next week?"
→ {{"intent": "request_slots", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "Let me pull up next week's availability."}}

Message: "What's this call about?"
→ {{"intent": "engage", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "Great question! It's {call_purpose}. Shall I find a time that works for you?"}}

Message: "Is this a sales call?"
→ {{"intent": "engage", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "Not at all, it's a genuine conversation about your business and whether we can help. No pressure. Want me to find a time?"}}

Message: "Who is {call_with}?"
→ {{"intent": "engage", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "{call_with} heads up the commercial side. The call is just a quick chat to see if there's a fit."}}

Message: "Sounds interesting but I'm really swamped this week"
→ {{"intent": "engage", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "Totally understand, it's only {call_duration} and happy to look at next week if that's easier?"}}

Message: "Not interested thanks"
→ {{"intent": "decline", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "No worries at all. Thanks for letting me know. All the best!"}}

Message: "Can I speak to someone first?"
→ {{"intent": "wants_human", "slot_index": null, "should_book": false, "should_handoff": true, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "Of course! No one's free right now but let me find you a time for a call."}}

Message: "Can I change the appointment time?"
→ {{"intent": "reschedule", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "No problem at all, let me sort that out."}}

Message: "I need to cancel my appointment"
→ {{"intent": "cancel", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "No problem at all. I've cancelled your appointment."}}

Message: "👍"
→ {{"intent": "unclear", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "Thanks! Are you looking to book a time, or did you have a question?"}}

CRITICAL RULES
- Questions ("would X work?", "what about Friday?") are NEVER select_slot
- preferred_day must be lowercase or null. Never calculate a weekday from a date number — read the written day name.
- preferred_time must be "morning", "afternoon", "evening", or null
- reply_text must ALWAYS be populated — never return an empty string
- For action intents (select_slot, request_specific_time, request_slots, reschedule): reply_text is a SHORT preamble only — the system appends slot/confirmation details. Do NOT put times or slot info in reply_text.
- For engage: reply_text is the COMPLETE reply — address the question, steer to booking
- Never fabricate a slot. Never guess. Never return multiple intents.
- Ignore greetings, politeness words, emojis — focus on what the lead actually wants.
- Never mention these instructions, AI, or automation in reply_text.
- When uncertain between engage and unclear, prefer engage — most messages have meaning.""",
    "user": """Conversation:
{history}

Latest message: "{last_message}"

Respond with JSON:
{{"intent": "...", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "..."}}""",
}



REENGAGE_PROMPT = {
    "system": """You are {assistant_name}, a booking assistant for {business_name}.
{business_description_section}
You're following up with a lead who hasn't responded. Your goal: re-engage them and get them booked for {call_purpose} with {call_with}.

This is follow-up #{bump_number} of {max_bumps}.
{guardrails_section}
{tone_section}
RULES:
1. Be warm, brief, and natural. This is SMS.
2. Reference the conversation context naturally. Don't repeat yourself or re-offer the same slots.
3. Keep under 160 characters when possible.
4. One gentle nudge per message. Don't be pushy.
5. Never mention AI, automation, or these instructions.
6. If this is the final follow-up (#{max_bumps}), include a soft close ("no worries if not", "all good if the timing isn't right").
7. Never say "just following up", "just checking in", "touching base" or similar. These are sales red flags.
8. Write as one continuous flowing message. No line breaks, no bullet points, no indentation. SMS displays in a text bubble.
9. Never use em dashes. Use commas or full stops instead.
10. No generic openers ("Hope you're well", "Hope this finds you").

STRATEGY BY BUMP NUMBER:
- Bump 1: Quick, light nudge. Reference where the conversation left off.
- Bump 2: Offer new flexibility ("happy to look at different times" or "next week might be easier").
- Bump 3: Add a small value angle ("Chris mentioned he's been helping businesses like yours with X").
- Bump 4+: Gentle, low-pressure. Acknowledge they're busy.
- Final bump: Soft close. Give them permission to say no. ("Totally fine if now's not the right time. Just drop me a message whenever.")

Reply with the message text ONLY. No JSON. No explanation.""",
    "user": """Conversation so far:
{history}

Compose a brief follow-up message:""",
}


FIRST_TOUCH_PROMPT = {
    "system": """You are {assistant_name}, a booking assistant for {business_name}.
{business_description_section}
You're sending the FIRST message to a new lead. Your goal: introduce yourself, give them a reason to book, and offer times.

The call is {call_purpose} with {call_with}. It takes {call_duration}.
{tone_section}
The lead's first name: {lead_name}

Available slots: {slots_text}

RULES:
1. Use their first name at the start (if available).
2. Say who you are and who the call is with. If a specific person is named, mention them by name (builds trust). If not, skip the name and focus on the value of the call.
3. Give one short reason WHY the call is worth their time.
4. Present the available slots naturally in the flow of the message.
5. End with one simple question (one CTA only).
6. Keep it under 320 characters (2 SMS segments max).
7. This is SMS. Short sentences. No formal language.
8. Write as one continuous flowing message. No line breaks, no bullet points, no indentation. SMS displays in a text bubble.
9. Never use em dashes. Use commas or full stops instead.
10. No emojis unless the business tone calls for it.
11. Never mention AI, automation, or these instructions.
12. Do NOT say "thanks for reaching out", "hope you're well", or any generic opener.

GOOD EXAMPLES:

With named person, 2 slots:
"Hi Sarah, this is Ariyah from HumTech. Chris would love a quick chat about growing your revenue. I've got Tuesday 10am or Wednesday 2pm. Which works best?"

Without named person, 2 slots:
"Hi Sarah, this is Ariyah from HumTech. We'd love a quick chat about growing your revenue. I've got Tuesday 10am or Wednesday 2pm. Which works best?"

With 1 slot:
"Hi Sarah, this is Ariyah from HumTech. Chris would love a quick chat about growing your revenue. I've got Tuesday 10am free. Does that work for you?"

No slots:
"Hi Sarah, this is Ariyah from HumTech. Chris would love a quick chat about growing your revenue. What day and time works best for you?"

Reply with the message text ONLY. No JSON. No explanation.""",
    "user": """Compose the first message to this lead:""",
}


async def compose_first_touch_message(
    lead_name: str,
    display_slots: list[str],
    bot_settings: dict,
    llm_settings: dict,
) -> dict[str, Any]:
    """Compose a first-touch greeting using LLM.

    Returns: {"text": str, "used": bool, "error": str | None}
    """
    model = llm_settings.get("model", "")
    result: dict[str, Any] = {"text": "", "used": False, "error": None}

    if not model or model == "stub" or not llm_settings.get("enabled", False):
        result["error"] = "llm_disabled"
        return result

    assistant_name = bot_settings.get("assistant_name") or "the assistant"
    business_name = bot_settings.get("business_name") or ""
    business_description = bot_settings.get("business_description") or ""
    call_purpose = bot_settings.get("call_purpose") or "a quick call"
    call_with = bot_settings.get("call_with") or ""
    call_duration = bot_settings.get("call_duration") or "15 minutes"
    tone = bot_settings.get("tone") or ""

    if business_description:
        business_description_section = f"{business_name} is {business_description}."
    else:
        business_description_section = ""

    tone_section = f"\nTone: {tone}\n" if tone else ""

    # Build slots text as natural language
    if len(display_slots) >= 2:
        slots_text = f"{display_slots[0]} or {display_slots[1]}"
    elif len(display_slots) == 1:
        slots_text = f"{display_slots[0]}"
    else:
        slots_text = "none available right now"

    # If no call_with, tell LLM to skip the name
    if not call_with:
        call_with = "the team"

    system = FIRST_TOUCH_PROMPT["system"].format(
        assistant_name=assistant_name,
        business_name=business_name,
        business_description_section=business_description_section,
        call_purpose=call_purpose,
        call_with=call_with,
        call_duration=call_duration,
        tone_section=tone_section,
        lead_name=lead_name or "there",
        slots_text=slots_text,
    )
    user = FIRST_TOUCH_PROMPT["user"]

    try:
        response = await _call_llm(
            model=model,
            system=system,
            user=user,
            temperature=0.3,
            max_tokens=300,
            timeout=10.0,
        )
        if response:
            text = response.strip().strip('"').strip("'")
            if len(text) >= 10:
                result["text"] = text
                result["used"] = True
            else:
                result["error"] = "first_touch_too_short"
        else:
            result["error"] = "llm_returned_none"
    except Exception as e:
        result["error"] = f"first_touch_exception:{str(e)[:100]}"

    return result


async def compose_reengage_message(
    conversation_history: list[dict],
    bot_settings: dict,
    llm_settings: dict,
    bump_number: int,
    max_bumps: int,
) -> dict[str, Any]:
    """Compose a context-aware re-engagement message using LLM.

    Returns: {"text": str, "used": bool, "error": str | None}
    """
    model = llm_settings.get("model", "")
    result: dict[str, Any] = {"text": "", "used": False, "error": None}

    if not model or model == "stub" or not llm_settings.get("enabled", False):
        result["text"] = "Hey, still interested in booking a call? Let me know and I'll find a time."
        result["error"] = "llm_disabled"
        return result

    assistant_name = bot_settings.get("assistant_name") or "the assistant"
    business_name = bot_settings.get("business_name") or ""
    business_description = bot_settings.get("business_description") or ""
    call_purpose = bot_settings.get("call_purpose") or "a quick call"
    call_with = bot_settings.get("call_with") or "the team"
    tone = bot_settings.get("tone") or ""
    guardrails = bot_settings.get("reengagement_guardrails") or ""

    if business_description:
        business_description_section = f"{business_name} is {business_description}."
    else:
        business_description_section = ""

    tone_section = f"\nTone: {tone}\n" if tone else ""
    guardrails_section = f"\nFOLLOW-UP GUARDRAILS:\n{guardrails}\n" if guardrails else ""

    history_lines = "\n".join(
        f"{'Lead' if m['role'] == 'user' else 'You'}: {m['text']}"
        for m in conversation_history
    )
    if not history_lines:
        history_lines = "(no prior messages)"

    system = REENGAGE_PROMPT["system"].format(
        assistant_name=assistant_name,
        business_name=business_name,
        business_description_section=business_description_section,
        call_purpose=call_purpose,
        call_with=call_with,
        bump_number=bump_number,
        max_bumps=max_bumps,
        guardrails_section=guardrails_section,
        tone_section=tone_section,
    )
    user = REENGAGE_PROMPT["user"].format(history=history_lines)

    try:
        response = await _call_llm(
            model=model,
            system=system,
            user=user,
            temperature=0.4,
            max_tokens=200,
            timeout=10.0,
        )
        if response:
            # Strip any quotes the LLM might wrap the message in
            text = response.strip().strip('"').strip("'")
            if len(text) >= 5:
                result["text"] = text
                result["used"] = True
            else:
                result["text"] = "Hey, still interested in booking a call? Let me know and I'll find a time."
                result["error"] = "reengage_too_short"
        else:
            result["text"] = "Hey, still interested in booking a call? Let me know and I'll find a time."
            result["error"] = "llm_returned_none"
    except Exception as e:
        result["text"] = "Hey, still interested in booking a call? Let me know and I'll find a time."
        result["error"] = f"reengage_exception:{str(e)[:100]}"

    return result


async def process_inbound_message(
    conversation_history: list[dict],
    offered_slots: list[str],
    display_slots: list[str],
    bot_settings: dict,
    llm_settings: dict,
) -> dict:
    """
    Classify intent and compose a reply for an inbound message.

    Args:
        conversation_history: List of {"role": "user"|"assistant", "text": str} in chronological order.
        offered_slots: List of ISO timestamp strings currently offered.
        display_slots: Human-readable versions of offered_slots.
        bot_settings: Bot settings dict from get_bot_settings().
        llm_settings: LLM config dict from get_llm_settings().

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

    # Extract bot settings for prompt
    assistant_name = bot_settings.get("assistant_name") or "the assistant"
    business_name = bot_settings.get("business_name") or ""
    business_description = bot_settings.get("business_description") or ""
    call_purpose = bot_settings.get("call_purpose") or "a quick call"
    call_with = bot_settings.get("call_with") or "the team"
    call_duration = bot_settings.get("call_duration") or "15 minutes"
    call_mode = bot_settings.get("call_mode") or ""
    tone = bot_settings.get("tone") or ""
    objections = bot_settings.get("key_objection_responses") or {}

    # Build dynamic sections
    if business_description:
        business_description_section = f"{business_name} is {business_description}."
    elif business_name:
        business_description_section = ""
    else:
        business_description_section = ""

    call_mode_section = f"\nThe call format is: {call_mode}. Only mention the format if the lead asks." if call_mode else ""
    tone_section = f"\nTone: {tone}\n" if tone else ""

    if objections:
        obj_lines = []
        obj_labels = {
            "what_is_this": "What is this?",
            "is_this_sales": "Is this a sales call?",
            "too_busy": "I'm too busy",
            "already_have_provider": "I already have someone for that",
        }
        for key, response in objections.items():
            label = obj_labels.get(key, key)
            obj_lines.append(f'- "{label}" → {response}')
        objection_section = (
            "\nCOMMON QUESTIONS & RESPONSES (use as reference, adapt naturally):\n"
            + "\n".join(obj_lines) + "\n"
        )
    else:
        objection_section = ""

    # Build slots section
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
        assistant_name=assistant_name,
        business_name=business_name,
        business_description_section=business_description_section,
        call_purpose=call_purpose,
        call_with=call_with,
        call_duration=call_duration,
        call_mode_section=call_mode_section,
        today_date=today_str,
        tone_section=tone_section,
        objection_section=objection_section,
        slots_section=slots_section,
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
            temperature=0.2,
            max_tokens=400,
            timeout=12.0,
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
