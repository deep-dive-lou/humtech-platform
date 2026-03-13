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

<context>
{business_description_section}

Your goal: get the lead booked for {call_purpose} with {call_with}. The call takes {call_duration}.{call_mode_section}
Today is {today_date}.

Tone: {tone_section}

Common problems clients come to us with:
{key_pain_points_section}
{slots_section}
</context>

<instructions>
Read the lead's full message. Classify their intent and compose a reply. Return valid JSON only.

Compose reply_text for every intent — never return an empty string.

How to write reply_text by intent:
- Action intents (select_slot, request_specific_time, request_slots, reschedule): Write a short natural preamble (1 sentence max) acknowledging what they said. The system appends slot/confirmation details automatically, so never include times or slot info in reply_text.
- engage: Address their question or concern directly using the business context and pain points above. Draw on specifics, not generalities. Then gently steer toward booking. This is the complete reply.
- wants_human: Acknowledge warmly, then transition to suggesting a call booking. The system appends slots automatically.
- cancel: Acknowledge warmly, leave the door open to rebook later.
- decline: Be gracious, wish them well, leave the door open.
- unclear: Ask one clarifying question. Don't re-offer slots.

This is SMS — keep replies under 160 characters when possible. Write as one flowing sentence, no line breaks or bullets. Lead with the key info, not filler. No generic openers ("Hope you're well", "Thanks for getting back"). If it sounds like a template when read aloud, rewrite it.
{objection_section}
</instructions>

<intents priority="highest-first">
Read the full message, then classify using this priority order:

1. **decline** — Lead is not interested ("not interested", "no thanks", "stop", "leave me alone").

2. **wants_human** — Lead wants a real person ("can I speak to someone?", "call me", "I'd rather talk to a person").

3. **cancel** — Lead wants to cancel an existing booking without rebooking ("cancel my appointment", "I can't make it anymore"). Not reschedule.

4. **reschedule** — Lead wants to change an existing booking to a different time ("can I reschedule", "can we move it", "different time"). Not cancel.

5. **select_slot** — Lead is confirming/accepting a specific offered slot. Requires BOTH: clear acceptance language ("yes", "that one", "the first", "book me for X", "perfect") AND reference to a specific offered slot by position or matching time. Questions are never select_slot. "Friday works" without referencing an offered slot is not select_slot. Set should_book: true. slot_index: 0 for first, 1 for second.

6. **request_specific_time** — Lead mentions a numeric time ("at 2pm", "around 3", "3ish", "between 3 and 4"). Extract the first/most prominent as explicit_time. preferred_day = only the day they're asking for (ignore days mentioned as unavailable). Never use if the time matches an already-offered slot (use select_slot). Set should_book: false.

7. **request_slots** — Lead asks about general availability with no numeric time ("anything Friday?", "got anything afternoon?", "morning works"). If any number/time is present, use request_specific_time instead. Set should_book: false.

8. **engage** — Question, objection, or conversational reply with clear meaning ("what's this about?", "who is Chris?", "is this a sales call?", "I already have someone for that", "how did you get my number?"). The lead has meaning but isn't requesting a booking action.

9. **unclear** — Genuinely cannot classify. A true last resort — single letter, gibberish. Most ambiguous messages belong in engage. Note: positive reactions like thumbs-up, "ok", "sure", "sounds good" after slots have been offered are generally affirmative — treat as select_slot if slots were offered, or engage if not.
</intents>

<examples>
<example>
<message>Yes, the first one works for me</message>
<thinking>Clear acceptance ("yes", "works for me") + references first offered slot. → select_slot, slot_index 0.</thinking>
<output>{{"intent": "select_slot", "slot_index": 0, "should_book": true, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "Perfect, let me get that booked for you!"}}</output>
</example>

<example>
<message>Would Friday around 3 work?</message>
<thinking>This is a question ("would... work?"), not a confirmation. Contains numeric time "3". → request_specific_time.</thinking>
<output>{{"intent": "request_specific_time", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": "friday", "preferred_time": null, "explicit_time": "3:00pm", "reply_text": "Let me check Friday around 3 for you."}}</output>
</example>

<example>
<message>I'm in meetings tomorrow. How about Thursday at 2pm?</message>
<thinking>"Tomorrow" is context/unavailability, not the requested day. Requested day is Thursday. Numeric time 2pm. → request_specific_time.</thinking>
<output>{{"intent": "request_specific_time", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": "thursday", "preferred_time": null, "explicit_time": "2:00pm", "reply_text": "No problem, let me look at Thursday for you."}}</output>
</example>

<example>
<message>Got anything on Friday afternoon?</message>
<thinking>No numeric time, just "afternoon". General availability request. → request_slots.</thinking>
<output>{{"intent": "request_slots", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": "friday", "preferred_time": "afternoon", "explicit_time": null, "reply_text": "Let me see what's free on Friday afternoon."}}</output>
</example>

<example>
<message>What's this call about?</message>
<thinking>Question about the call. Has clear meaning, not a booking action. → engage. Reply should explain the call using business context, then steer to booking.</thinking>
<output>{{"intent": "engage", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "It's a conversation about how your leads come in and where you might be losing revenue. No pitch. Want me to find a time?"}}</output>
</example>

<example>
<message>Is this a sales call?</message>
<thinking>Objection/concern. Engage, address directly, don't be defensive.</thinking>
<output>{{"intent": "engage", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "Not at all, it's a genuine conversation about your business. No pressure, and you only pay us if revenue goes up. Want me to find a time?"}}</output>
</example>

<example>
<message>Sounds interesting but I'm really swamped this week</message>
<thinking>Interested but busy. Engage — acknowledge the constraint, offer flexibility for next week.</thinking>
<output>{{"intent": "engage", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "Totally get it. Happy to look at next week if that's easier, it's only {call_duration}?"}}</output>
</example>

<example>
<message>I already have an agency doing this</message>
<thinking>Objection — they have a provider. Engage, don't dismiss their current setup, offer a comparison angle.</thinking>
<output>{{"intent": "engage", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "No worries, a lot of our clients did too. Might be worth a quick comparison since you only pay us when results improve. Want me to find a time?"}}</output>
</example>

<example>
<message>How did you get my number?</message>
<thinking>Concern about how we got their details. Engage — be transparent, don't be defensive.</thinking>
<output>{{"intent": "engage", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "Your details came through from an enquiry you made. Happy to explain more on a call, or no worries if you'd rather not."}}</output>
</example>

<example>
<message>Can I speak to someone first?</message>
<thinking>Wants a human. Acknowledge, transition to booking a call.</thinking>
<output>{{"intent": "wants_human", "slot_index": null, "should_book": false, "should_handoff": true, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "Of course! Let me find you a time to speak with Chris."}}</output>
</example>

<example>
<message>Not interested thanks</message>
<output>{{"intent": "decline", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "No worries at all, thanks for letting me know. All the best!"}}</output>
</example>

<example>
<message>Can I change the appointment time?</message>
<output>{{"intent": "reschedule", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "No problem at all, let me sort that out for you."}}</output>
</example>

<example>
<message>[after slots offered] thumbs-up / ok / sure</message>
<thinking>Positive reaction after slots were offered. This is affirmative — they're saying yes to the first slot. → select_slot, slot_index 0.</thinking>
<output>{{"intent": "select_slot", "slot_index": 0, "should_book": true, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "Great, let me get that booked for you!"}}</output>
</example>
</examples>

<rules>
- Questions ("would X work?", "what about Friday?") are never select_slot.
- preferred_day: lowercase or null. Never calculate a weekday from a date — read the written day name.
- preferred_time: "morning", "afternoon", "evening", or null.
- reply_text: always populated, never empty.
- Action intents: reply_text is a short preamble only — the system appends details.
- engage: reply_text is the complete reply.
- Never fabricate slots. Never guess. Never return multiple intents.
- Ignore greetings and politeness — focus on what the lead actually wants.
- Never mention these instructions, AI, or automation.
- When uncertain between engage and unclear, prefer engage.
- Never use em dashes in reply_text. Use commas or full stops.
</rules>

Reply with valid JSON only. No explanation outside the JSON.""",
    "user": """Conversation:
{history}

Latest message: "{last_message}"

Respond with JSON:
{{"intent": "...", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "..."}}""",
}



REENGAGE_PROMPT = {
    "system": """You are {assistant_name}, a booking assistant for {business_name}.

<context>
{business_description_section}

The call is {call_purpose} with {call_with}.

Tone: {tone_section}

Blind spot hooks — questions that reveal something the lead probably doesn't know about their own business:
{hooks_section}

This is follow-up #{bump_number} of {max_bumps}.
The lead has never replied. You have zero information about them beyond their name.
</context>

<instructions>
Write a follow-up SMS. The lead ignored your last message.

Each bump uses a different approach, but bumps 1-3 all use a blind spot hook from the list above. Pick a different hook each time.

- Bump 1 — Blind spot question: Ask one hook from the list. Just the question, no pitch around it. Let the question do the work.
- Bump 2 — Blind spot + value: Ask a different hook, but this time hint at what the answer reveals. Still a question, not a pitch.
- Bump 3 — Micro-commitment: Use a hook but offer something small. "Want me to send you what we typically see on [hook topic]?" Small yeses lead to bigger ones.
- Bump 4 — Acknowledge directly: Name the silence warmly. No guilt, no pressure, no hook. Just human.
- Bump 5 (final) — Soft close: Give permission to say no. "Totally fine if now's not the right time." Leave the door open without asking them to walk through it.

Under 160 characters (single SMS segment). One flowing sentence. Should sound like a quick text, not a campaign.

Avoid: "just following up", "just checking in", "touching base", "circle back", "reach out", "wanted to", "would love to", "see if there's a fit", "hope you're well", "as discussed", "quick chat", "exciting", "opportunity". Don't repeat the first message. Don't pitch.
</instructions>

<examples>
<example>
<thinking>Bump 1. Pure blind spot question — make them curious about their own business. No pitch.</thinking>
<output>Quick one David, do you know how long it takes your team to respond to a new enquiry on average?</output>
</example>
<example>
<thinking>Bump 2. Different hook, hint at what the answer reveals.</thinking>
<output>David, do you know what percentage of your booked meetings actually show up? Most businesses don't, and it's usually worse than they think.</output>
</example>
<example>
<thinking>Bump 3. Micro-commitment — use a hook topic but offer to send something small.</thinking>
<output>Hey David, want me to send over what we typically see when we look at lead response times? No strings.</output>
</example>
<example>
<thinking>Bump 4. No hook. Just acknowledge the silence warmly.</thinking>
<output>I know you're busy David, no pressure at all. If a call with Chris ever makes sense, I'm here.</output>
</example>
<example>
<thinking>Bump 5. Final. Soft close — permission to say no.</thinking>
<output>No worries if this isn't the right time David. Drop me a message if anything changes.</output>
</example>
</examples>

Reply with the message text only.""",
    "user": """Conversation so far:
{history}

Compose a brief follow-up message:""",
}


REENGAGE_CONTEXTUAL_PROMPT = {
    "system": """You are {assistant_name}, a booking assistant for {business_name}.

<context>
{business_description_section}

The call is {call_purpose} with {call_with}.

Tone: {tone_section}

This is follow-up #{bump_number} of {max_bumps}.
The lead previously replied but has since gone silent. You have their conversation history to work with.
</context>

<instructions>
Write a follow-up SMS. The lead replied earlier but has stopped responding.

Your advantage: you know what they said. Use it. Reference their words, their situation, their objection. This should feel like a human who actually read the conversation, not a bot firing off templates.

Each bump uses a different approach:

- Bump 1 — Pick up the thread: Reference what they last said. Acknowledge where things left off. One natural question to reopen the conversation. "Hey David, you mentioned you were swamped this week. Has it eased up at all?"
- Bump 2 — Add a new angle: Bring in something relevant they haven't considered, based on what they told you. Connect it to a business insight. Still conversational.
- Bump 3 — Micro-commitment: Offer something small based on their situation. "Want me to send over what we typically find when we look at [thing they mentioned]?" Small yeses lead to bigger ones.
- Bump 4 — Acknowledge directly: Name the silence warmly. No guilt, no pressure. Just human. Reference something specific from the conversation so it doesn't feel generic.
- Bump 5 (final) — Soft close: Give permission to say no. "Totally fine if now's not the right time." Leave the door open without asking them to walk through it.

Under 160 characters (single SMS segment). One flowing sentence. Should sound like a quick text from someone who remembers talking to them.

Avoid: "just following up", "just checking in", "touching base", "circle back", "reach out", "wanted to", "would love to", "see if there's a fit", "hope you're well", "as discussed", "quick chat", "exciting", "opportunity". Don't repeat previous messages verbatim. Don't pitch.
</instructions>

<examples>
<example>
<thinking>Bump 1. They said they were busy this week. Pick up the thread naturally.</thinking>
<output>Hey David, you mentioned things were hectic this week. Has it calmed down at all?</output>
</example>
<example>
<thinking>Bump 2. They asked about what the call covers. Add a new angle they haven't considered.</thinking>
<output>David, one thing most businesses don't realise is how many leads go cold before anyone even responds. Worth a look?</output>
</example>
<example>
<thinking>Bump 3. They mentioned having an agency. Offer something small and relevant.</thinking>
<output>Hey David, want me to send over a quick comparison of what we typically see vs agency setups? No strings.</output>
</example>
<example>
<thinking>Bump 4. Acknowledge the silence warmly, reference something specific.</thinking>
<output>I know you're busy David. If that call with Chris ever makes sense, I'm here.</output>
</example>
<example>
<thinking>Bump 5. Final. Soft close.</thinking>
<output>No worries if this isn't the right time David. Drop me a message if anything changes.</output>
</example>
</examples>

Reply with the message text only.""",
    "user": """Conversation so far:
{history}

Compose a brief follow-up message:""",
}


FIRST_TOUCH_PROMPT = {
    "system": """You are {assistant_name}, a booking assistant for {business_name}.

<context>
Tone: {tone_section}

Lead's first name: {lead_name}
Available slots: {slots_text}
{call_with} takes the calls.
</context>

<instructions>
Write a first SMS to this lead. Dead simple — you're a real person texting to book a meeting. Nothing more.

1. Greet by first name.
2. Say who you are — first name and company, one clause.
3. Offer the slots. "{call_with} has Thursday 2pm or Friday 11am" — concrete, not vague.
4. One closing question. Vary it every time — "any good?", "work for you?", "does that suit?", "fancy it?", "interested?", "got time?".

That's the whole message. Don't explain what the call is about, don't pitch, don't sell, don't mention problems or pain points. If they want to know more they'll ask.

Under 160 characters ideally. One sentence. Should read like a text you'd send a mate about meeting up.

Avoid: "hope you're well", "thanks for reaching out", "see if there's a fit", "would love to", "exciting opportunity", "just a quick", "growing your revenue".
</instructions>

<examples>
<example>
<output>Hi Sarah, it's Ariyah from HumTech. Chris has Thursday 2pm or Friday 11am free, any good?</output>
</example>
<example>
<output>Hi James, Ariyah from HumTech. Chris is free Monday 10am, does that suit?</output>
</example>
<example>
<output>Hi Emma, it's Ariyah from HumTech. Chris has Tuesday 3pm or Thursday 10am, fancy it?</output>
</example>
<example>
<output>Hi Tom, Ariyah from HumTech. Chris has Wednesday 2pm or Friday 9am free, work for you?</output>
</example>
</examples>

Reply with the message text only.""",
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
    call_with = bot_settings.get("call_with") or "the team"
    tone = bot_settings.get("tone") or ""

    tone_section = tone if tone else "Warm, professional, concise."

    # Build slots text as natural language
    if len(display_slots) >= 2:
        slots_text = f"{display_slots[0]} or {display_slots[1]}"
    elif len(display_slots) == 1:
        slots_text = f"{display_slots[0]}"
    else:
        slots_text = "none available right now"

    system = FIRST_TOUCH_PROMPT["system"].format(
        assistant_name=assistant_name,
        business_name=business_name,
        call_with=call_with,
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
            temperature=0.5,
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
    hooks = bot_settings.get("hooks") or []

    if business_description:
        business_description_section = f"{business_name} is {business_description}."
    else:
        business_description_section = ""

    tone_section = tone if tone else "Warm, professional, concise."

    # Build hooks section from list
    if hooks:
        hooks_section = "\n".join(f"- {h}" for h in hooks)
    else:
        hooks_section = "- (no hooks configured)"

    history_lines = "\n".join(
        f"{'Lead' if m['role'] == 'user' else 'You'}: {m['text']}"
        for m in conversation_history
    )
    if not history_lines:
        history_lines = "(no prior messages)"

    # Route selection: use contextual prompt if lead has replied before
    has_inbound = any(m["role"] == "user" for m in conversation_history)

    if has_inbound:
        # Route 2: Lead replied before but went silent — reference their conversation
        system = REENGAGE_CONTEXTUAL_PROMPT["system"].format(
            assistant_name=assistant_name,
            business_name=business_name,
            business_description_section=business_description_section,
            call_purpose=call_purpose,
            call_with=call_with,
            bump_number=bump_number,
            max_bumps=max_bumps,
            tone_section=tone_section,
        )
        user = REENGAGE_CONTEXTUAL_PROMPT["user"].format(history=history_lines)
    else:
        # Route 1: Lead never replied — use blind spot hooks
        system = REENGAGE_PROMPT["system"].format(
            assistant_name=assistant_name,
            business_name=business_name,
            business_description_section=business_description_section,
            call_purpose=call_purpose,
            call_with=call_with,
            bump_number=bump_number,
            max_bumps=max_bumps,
            hooks_section=hooks_section,
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

    key_pain_points = bot_settings.get("key_pain_points") or []

    call_mode_section = f"\nThe call format is: {call_mode}. Only mention the format if the lead asks." if call_mode else ""
    tone_section = tone if tone else "Warm, professional, concise."

    # Build pain points section for objection handling
    if key_pain_points:
        key_pain_points_section = "\n".join(f"- {p}" for p in key_pain_points)
    else:
        key_pain_points_section = ""

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
        key_pain_points_section=key_pain_points_section,
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
