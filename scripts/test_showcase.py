"""Quick showcase of v3 prompts — clean output for review."""
import asyncio
import os
import sys
import httpx
from dotenv import load_dotenv

load_dotenv()

# Force UTF-8 output
sys.stdout.reconfigure(encoding='utf-8')

API_KEY = os.environ["ANTHROPIC_API_KEY"]
MODEL = "claude-sonnet-4-6"

# Import prompts from the v3 test script
sys.path.insert(0, os.path.dirname(__file__))
from test_prompt_v3 import FIRST_TOUCH_V3, REENGAGE_V3, PROCESS_MESSAGE_V3

# ── TENANT SETTINGS ────────────────────────────────────────────────────

VARS = {
    "assistant_name": "Ariyah",
    "business_name": "HumTech",
    "business_description_section": (
        "HumTech is a multidisciplinary team — sales, tech, and marketing — "
        "that works inside your business to find and fix where revenue is leaking. "
        "Evidence-based approach, measured against your own numbers."
    ),
    "call_purpose": (
        "a call to look at how your leads come in, how they're handled, "
        "and whether anything's falling through the cracks"
    ),
    "call_with": "Chris, our commercial director",
    "call_duration": "30 minutes",
    "call_mode_section": "",
    "tone_section": (
        "Warm, professional, concise. Friendly but not overly casual. "
        "Never pushy, guide, don't pressure."
    ),
    # Pain points — used by PROCESS_MESSAGE for objection handling
    "key_pain_points_section": "\n".join([
        "- Leads going cold because response time is too slow",
        "- Sales depends on specific people, not systems",
        "- Proposals go quiet with no systematic follow-up",
        "- No-shows kill the pipeline (no reminders or rebooking)",
        "- Ad spend with no visibility on what actually converts",
        "- Been burned by agencies who couldn't prove ROI",
        "- No real visibility on conversion rates or revenue per lead",
    ]),
    # Hooks — blind spot questions for re-engagement
    "hooks_section": "\n".join([
        "- Do you know how long it takes your team to respond to a new enquiry on average?",
        "- Do you know what percentage of your booked meetings actually show up?",
        "- Do you know which of your marketing channels is actually driving revenue, not just leads?",
        "- Do you know how many proposals your team sent last month that never got a follow-up?",
        "- Do you know your real cost per acquired customer, not just cost per lead?",
    ]),
    # First message — shown in re-engage conversation history
    "first_message": (
        "You: Hi David, it's Ariyah from HumTech. Chris has Thursday 2pm or "
        "Friday 11am free for a call about how your leads are handled, any good?"
    ),
    "lead_name": "David",
    "slots_text": "Thursday 2pm or Friday 11am",
    "today_date": "Thursday 13 March 2026",
    "objection_section": "",
    "slots_section": "\nCurrently offered slots:\n  1) Thursday 2:00 PM\n  2) Friday 11:00 AM\n",
}


async def call_claude(system: str, user: str = "Compose the message:") -> str:
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
                "temperature": 0.5,
                "max_tokens": 400,
            },
        )
        resp.raise_for_status()
        return resp.json()["content"][0]["text"].strip()


async def main():
    # ── FIRST TOUCH ──
    print("=" * 60)
    print("  FIRST TOUCH (4 samples, should each be different)")
    print("=" * 60)

    system = FIRST_TOUCH_V3.format(**VARS)
    for i in range(4):
        result = await call_claude(system, "Compose the first message to this lead:")
        print(f"\n  {i+1}. ({len(result)} chars)")
        print(f"     {result}")

    # ── RE-ENGAGE (all 5 bumps) ──
    print("\n" + "=" * 60)
    print("  RE-ENGAGE (bumps 1-5)")
    print("=" * 60)

    for bump in [1, 2, 3, 4, 5]:
        system = REENGAGE_V3.format(**VARS).replace("{bump}", str(bump))
        result = await call_claude(system, "Compose the follow-up message:")
        print(f"\n  Bump {bump}. ({len(result)} chars)")
        print(f"     {result}")

    # ── PROCESS MESSAGE (tricky inbound) ──
    print("\n" + "=" * 60)
    print("  INBOUND MESSAGE HANDLING")
    print("=" * 60)

    sys_prompt = PROCESS_MESSAGE_V3.format(**VARS)

    tests = [
        ("What exactly do you do?", "(no prior messages)"),
        ("Is this a sales call?", "(no prior messages)"),
        ("I've already got an agency thanks", "(no prior messages)"),
        ("How did you get my number?", "(no prior messages)"),
        ("Sounds interesting but I'm swamped right now", "(no prior messages)"),
        ("Maybe next month", "(no prior messages)"),
        ("Friday works", "(no prior messages)"),
        ("Yeah go on then", "You: Hi David, it's Ariyah from HumTech. Chris has Thursday 2pm or Friday 11am, any good?"),
        ("Could we do 3ish on Wednesday instead?", "You: Hi David, Chris has Thursday 2pm or Friday 11am.\nLead: Got anything earlier in the week?"),
        ("What would we actually talk about on the call?", "You: Hi David, Chris has Thursday 2pm or Friday 11am, any good?"),
        ("Sorry who is Chris?", "You: Hi David, Chris can walk you through how we help. Thursday 2pm or Friday 11am?"),
        ("\U0001f44d", "You: I've got Thursday 2pm or Friday 11am, any good?"),
        ("ok sure", "You: I've got Thursday 2pm or Friday 11am, any good?"),
        ("Not interested thanks", "(no prior messages)"),
        ("Can I speak to someone?", "(no prior messages)"),
    ]

    for msg, history in tests:
        user_prompt = f'Conversation:\n{history}\n\nLatest message: "{msg}"\n\nRespond with JSON:\n{{"intent": "...", "slot_index": null, "should_book": false, "should_handoff": false, "preferred_day": null, "preferred_time": null, "explicit_time": null, "reply_text": "..."}}'

        result = await call_claude(sys_prompt, user_prompt)

        # Extract intent and reply_text for clean display
        import json, re
        clean = re.sub(r"^```(?:json)?\s*|\s*```$", "", result.strip(), flags=re.DOTALL)
        try:
            parsed = json.loads(clean)
            intent = parsed.get("intent", "?")
            reply = parsed.get("reply_text", "?")
            extras = ""
            if parsed.get("slot_index") is not None:
                extras += f" slot={parsed['slot_index']}"
            if parsed.get("preferred_day"):
                extras += f" day={parsed['preferred_day']}"
            if parsed.get("explicit_time"):
                extras += f" time={parsed['explicit_time']}"
            if parsed.get("should_book"):
                extras += " BOOK"
            if parsed.get("should_handoff"):
                extras += " HANDOFF"

            print(f'\n  Lead: "{msg}"')
            print(f"  --> [{intent}{extras}]")
            print(f'  Bot: "{reply}"')
        except:
            print(f'\n  Lead: "{msg}"')
            print(f"  --> RAW: {result[:200]}")


if __name__ == "__main__":
    asyncio.run(main())
