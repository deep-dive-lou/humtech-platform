"""Simulate full bot conversations end-to-end using the actual prompts from llm.py."""
import asyncio
import os
import sys
import json
import re
import httpx
from dotenv import load_dotenv

load_dotenv()
sys.stdout.reconfigure(encoding='utf-8')

API_KEY = os.environ["ANTHROPIC_API_KEY"]
MODEL = "claude-sonnet-4-6"

# Import the actual prompts from llm.py
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from scripts.test_prompt_v3 import FIRST_TOUCH_V3, REENGAGE_V3, PROCESS_MESSAGE_V3

# ── Tenant settings (matching what's now in DB) ──────────────────────

BOT_SETTINGS = {
    "assistant_name": "Ariyah",
    "business_name": "HumTech",
    "business_description": (
        "a multidisciplinary team, sales, tech, and marketing, "
        "that works inside your business to find and fix where revenue is leaking. "
        "Evidence-based approach, measured against your own numbers"
    ),
    "call_purpose": (
        "a call to look at how your leads come in, how they're handled, "
        "and whether anything's falling through the cracks"
    ),
    "call_with": "Chris, our commercial director",
    "call_duration": "30 minutes",
    "tone": (
        "Warm, professional, concise. Friendly but not overly casual. "
        "Never pushy, guide, don't pressure."
    ),
    "hooks": [
        "Do you know how long it takes your team to respond to a new enquiry on average?",
        "Do you know what percentage of your booked meetings actually show up?",
        "Do you know which of your marketing channels is actually driving revenue, not just leads?",
        "Do you know how many proposals your team sent last month that never got a follow-up?",
        "Do you know your real cost per acquired customer, not just cost per lead?",
    ],
    "key_pain_points": [
        "Leads going cold because response time is too slow",
        "Sales depends on specific people, not systems",
        "Proposals go quiet with no systematic follow-up",
        "No-shows kill the pipeline (no reminders or rebooking)",
        "Ad spend with no visibility on what actually converts",
        "Been burned by agencies who couldn't prove ROI",
        "No real visibility on conversion rates or revenue per lead",
    ],
}

SLOTS = ["Thursday 2:00 PM", "Friday 11:00 AM"]
SLOTS_TEXT = "Thursday 2pm or Friday 11am"


async def call_claude(system: str, user: str, temperature: float = 0.3, max_tokens: int = 400) -> str:
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": API_KEY,
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json",
            },
            json={
                "model": MODEL,
                "system": system,
                "messages": [{"role": "user", "content": user}],
                "temperature": temperature,
                "max_tokens": max_tokens,
            },
        )
        resp.raise_for_status()
        return resp.json()["content"][0]["text"].strip()


def build_first_touch_prompt(lead_name: str) -> tuple[str, str]:
    system = FIRST_TOUCH_V3.format(
        assistant_name=BOT_SETTINGS["assistant_name"],
        business_name=BOT_SETTINGS["business_name"],
        tone_section=BOT_SETTINGS["tone"],
        lead_name=lead_name,
        slots_text=SLOTS_TEXT,
        call_with=BOT_SETTINGS["call_with"],
    )
    return system, "Compose the first message to this lead:"


def build_process_message_prompt(history: list[dict], last_message: str) -> tuple[str, str]:
    bs = BOT_SETTINGS
    biz_desc = f"{bs['business_name']} is {bs['business_description']}."
    pain_points = "\n".join(f"- {p}" for p in bs["key_pain_points"])
    slots_lines = "\n".join(f"  {i+1}) {s}" for i, s in enumerate(SLOTS))
    slots_section = f"\nCurrently offered slots:\n{slots_lines}\n"

    history_lines = "\n".join(
        f"{'Lead' if m['role'] == 'user' else 'You'}: {m['text']}"
        for m in history
    )
    if not history_lines:
        history_lines = "(no prior messages)"

    system = PROCESS_MESSAGE_V3.format(
        assistant_name=bs["assistant_name"],
        business_name=bs["business_name"],
        business_description_section=biz_desc,
        call_purpose=bs["call_purpose"],
        call_with=bs["call_with"],
        call_duration=bs["call_duration"],
        call_mode_section="",
        today_date="Thursday 13 March 2026",
        tone_section=bs["tone"],
        key_pain_points_section=pain_points,
        objection_section="",
        slots_section=slots_section,
    )
    user = f'Conversation:\n{history_lines}\n\nLatest message: "{last_message}"\n\nRespond with JSON:\n{{"intent": "...", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "..."}}'
    return system, user


REENGAGE_CONTEXTUAL = """You are {assistant_name}, a booking assistant for {business_name}.

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

Reply with the message text only."""


def build_reengage_prompt(history: list[dict], bump: int) -> tuple[str, str]:
    bs = BOT_SETTINGS
    biz_desc = f"{bs['business_name']} is {bs['business_description']}."
    hooks_section = "\n".join(f"- {h}" for h in bs["hooks"])
    max_bumps = 5

    history_lines = "\n".join(
        f"{'Lead' if m['role'] == 'user' else 'You'}: {m['text']}"
        for m in history
    )

    # Route selection: contextual if lead has replied, hooks if never replied
    has_inbound = any(m["role"] == "user" for m in history)

    if has_inbound:
        system = REENGAGE_CONTEXTUAL.format(
            assistant_name=bs["assistant_name"],
            business_name=bs["business_name"],
            business_description_section=biz_desc,
            call_purpose=bs["call_purpose"],
            call_with=bs["call_with"],
            tone_section=bs["tone"],
            bump_number=bump,
            max_bumps=max_bumps,
        )
    else:
        system = REENGAGE_V3.format(
            assistant_name=bs["assistant_name"],
            business_name=bs["business_name"],
            business_description_section=biz_desc,
            call_purpose=bs["call_purpose"],
            call_with=bs["call_with"],
            tone_section=bs["tone"],
            hooks_section=hooks_section,
            first_message=history_lines.split("\n")[0] if history_lines else "",
        )
        system = system.replace("{bump}", str(bump))

    user = f"Conversation so far:\n{history_lines}\n\nCompose a brief follow-up message:"
    return system, user


def parse_json_response(text: str) -> dict:
    clean = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.DOTALL)
    return json.loads(clean)


async def simulate_conversation(name: str, lead_name: str, lead_messages: list[str]):
    """Simulate a full conversation: first touch, then lead replies, bot responds."""
    print(f"\n{'='*70}")
    print(f"  SCENARIO: {name}")
    print(f"{'='*70}")

    history = []

    # 1. First touch
    sys_prompt, user_prompt = build_first_touch_prompt(lead_name)
    first_touch = await call_claude(sys_prompt, user_prompt, temperature=0.5)
    history.append({"role": "assistant", "text": first_touch})
    print(f"\n  Bot: {first_touch}")

    # 2. Lead replies (or doesn't)
    for msg in lead_messages:
        if msg == "[SILENCE]":
            # Simulate re-engagement
            bump = sum(1 for m in history if m["role"] == "assistant")
            print(f"\n  ... (lead goes silent, bump {bump}) ...")
            sys_prompt, user_prompt = build_reengage_prompt(history, bump)
            reengage = await call_claude(sys_prompt, user_prompt, temperature=0.4, max_tokens=200)
            history.append({"role": "assistant", "text": reengage})
            print(f"  Bot: {reengage}")
            continue

        # Process lead's inbound message
        print(f"\n  Lead: {msg}")
        history.append({"role": "user", "text": msg})

        sys_prompt, user_prompt = build_process_message_prompt(history[:-1], msg)
        raw = await call_claude(sys_prompt, user_prompt, temperature=0.2)

        try:
            parsed = parse_json_response(raw)
            intent = parsed.get("intent", "?")
            reply = parsed.get("reply_text", "?")
            extras = ""
            if parsed.get("slot_index") is not None:
                extras += f" slot={parsed['slot_index']}"
            if parsed.get("should_book"):
                extras += " BOOK"
            if parsed.get("should_handoff"):
                extras += " HANDOFF"
            if parsed.get("preferred_day"):
                extras += f" day={parsed['preferred_day']}"
            if parsed.get("explicit_time"):
                extras += f" time={parsed['explicit_time']}"

            print(f"  --> [{intent}{extras}]")

            # Simulate system appending slot info for action intents
            if intent == "select_slot" and parsed.get("should_book"):
                full_reply = f"{reply} You're booked for {SLOTS[parsed.get('slot_index', 0)]}."
            elif intent in ("request_slots", "request_specific_time"):
                full_reply = f"{reply} I've got {SLOTS_TEXT}."
            elif intent == "wants_human":
                full_reply = f"{reply} I've got {SLOTS_TEXT}."
            else:
                full_reply = reply

            print(f"  Bot: {full_reply}")
            history.append({"role": "assistant", "text": full_reply})

            # If booked or declined, conversation ends
            if intent in ("decline", "select_slot"):
                print(f"\n  [Conversation ended: {intent}]")
                return

        except Exception as e:
            print(f"  --> PARSE ERROR: {e}")
            print(f"  RAW: {raw[:200]}")
            history.append({"role": "assistant", "text": raw[:100]})


async def main():
    print("\n" + "#"*70)
    print("  BOT CONVERSATION SIMULATOR")
    print("#"*70)

    # Scenario 1: Happy path — lead books immediately
    await simulate_conversation(
        "Happy path — books on first reply",
        "Sarah",
        ["Yeah Thursday works"],
    )

    # Scenario 2: Lead asks questions then books
    await simulate_conversation(
        "Curious lead — asks questions, then books",
        "James",
        [
            "Who is Chris?",
            "What would we actually talk about?",
            "Ok go on then, Thursday 2pm",
        ],
    )

    # Scenario 3: Sceptical lead — objections
    await simulate_conversation(
        "Sceptical lead — pushback then books",
        "Emma",
        [
            "Is this a sales call?",
            "I've already got an agency doing this",
            "How is yours different?",
            "Fine, Friday then",
        ],
    )

    # Scenario 4: Lead goes silent — re-engage sequence
    await simulate_conversation(
        "Silent lead — full re-engage sequence",
        "Tom",
        [
            "[SILENCE]",  # bump 1
            "[SILENCE]",  # bump 2
            "[SILENCE]",  # bump 3
            "[SILENCE]",  # bump 4
            "[SILENCE]",  # bump 5
        ],
    )

    # Scenario 5: Lead goes silent, then responds to re-engage
    await simulate_conversation(
        "Silent then responds to blind spot hook",
        "David",
        [
            "[SILENCE]",  # bump 1 (blind spot question)
            "Actually no idea, probably a few hours?",  # responds to hook
            "Yeah go on then, book me in",
        ],
    )

    # Scenario 6: Thumbs up after first touch
    await simulate_conversation(
        "Thumbs up after first touch",
        "Lucy",
        ["\U0001f44d"],
    )

    # Scenario 7: Decline
    await simulate_conversation(
        "Immediate decline",
        "Mike",
        ["Not interested thanks"],
    )

    # Scenario 8: Busy lead, reschedule
    await simulate_conversation(
        "Busy lead — wants different time",
        "Rachel",
        [
            "Sounds interesting but I'm swamped this week",
            "Could we do next Wednesday around 3?",
        ],
    )

    # Scenario 9: Route 2 — Lead replies then goes silent (contextual re-engage)
    await simulate_conversation(
        "Route 2 — replied then silent (busy objection)",
        "Hannah",
        [
            "Sounds interesting but I'm really busy right now",
            "[SILENCE]",  # bump 1 — should reference being busy
            "[SILENCE]",  # bump 2 — new angle based on their situation
            "[SILENCE]",  # bump 3 — micro-commitment
        ],
    )

    # Scenario 10: Route 2 — Lead asked a question then went silent
    await simulate_conversation(
        "Route 2 — asked question then silent",
        "Alex",
        [
            "What would we actually talk about on the call?",
            "[SILENCE]",  # bump 1 — should pick up from their question
            "[SILENCE]",  # bump 2 — add new angle
        ],
    )

    # Scenario 11: Route 2 — Lead had agency objection then went silent
    await simulate_conversation(
        "Route 2 — agency objection then silent",
        "Mark",
        [
            "I've already got an agency doing this",
            "[SILENCE]",  # bump 1 — reference the agency comment
            "[SILENCE]",  # bump 2
            "[SILENCE]",  # bump 3 — micro-commitment
        ],
    )


if __name__ == "__main__":
    asyncio.run(main())
