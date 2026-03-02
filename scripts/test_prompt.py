"""
Test the new prompt with sample messages against the live LLM.
Run locally — uses ANTHROPIC_API_KEY from .env.
"""
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv

load_dotenv()

from app.bot.llm import process_inbound_message

# Simulate bot_settings as they come from get_bot_settings()
BOT_SETTINGS = {
    "assistant_name": "Ariyah",
    "business_name": "HumTech",
    "business_description": "a revenue acceleration consultancy that helps B2B businesses grow through AI-powered systems",
    "call_purpose": "a short discovery call to understand your current setup and see if there's a fit",
    "call_with": "Chris, our commercial director",
    "call_duration": "15 minutes",
    "tone": "Warm, professional, concise. Friendly but not overly casual. Never pushy — guide, don't pressure.",
    "key_objection_responses": {
        "what_is_this": "We help businesses like yours accelerate revenue using AI-powered systems. The call is just a quick chat to see if there's a fit — no pressure.",
        "is_this_sales": "It's not a sales pitch — it's a genuine conversation about your business and whether we can help. Takes about 15 minutes.",
        "too_busy": "Totally understand — we keep it short, just 15 minutes. Happy to find a time that works around your schedule.",
        "already_have_provider": "No worries — a lot of our clients had existing setups too. Might still be worth a quick chat to compare approaches.",
    },
    "context": "HumTech, a revenue acceleration consultancy",
    "persona": "",
    "first_touch_template": None,
    "booking_confirmation_template": "Booked \u2705 You're confirmed for {day} {date} {month} at {time}. See you then!",
}

LLM_SETTINGS = {
    "enabled": True,
    "model": "claude-haiku-4-5-20251001",
    "temperature": 0.2,
}

# Test messages — each with expected intent
TEST_CASES = [
    {
        "label": "engage: what's this about?",
        "history": [
            {"role": "assistant", "text": "Hey Sarah — thanks for reaching out. Want to get you booked in quickly. I've got Tuesday 10am or Wednesday 2pm free — which works best for you?"},
            {"role": "user", "text": "What's this call about?"},
        ],
        "display_slots": ["Tuesday 4th March at 10:00am", "Wednesday 5th March at 2:00pm"],
        "offered_slots": ["2026-03-04T10:00:00+00:00", "2026-03-05T14:00:00+00:00"],
        "expected_intent": "engage",
    },
    {
        "label": "engage: is this a sales call?",
        "history": [
            {"role": "assistant", "text": "Hey — thanks for reaching out. Want to get you booked in quickly. I've got Tuesday 10am or Wednesday 2pm free — which works best for you?"},
            {"role": "user", "text": "Is this a sales call?"},
        ],
        "display_slots": ["Tuesday 4th March at 10:00am", "Wednesday 5th March at 2:00pm"],
        "offered_slots": ["2026-03-04T10:00:00+00:00", "2026-03-05T14:00:00+00:00"],
        "expected_intent": "engage",
    },
    {
        "label": "engage: too busy this week",
        "history": [
            {"role": "assistant", "text": "Hey — thanks for reaching out. Want to get you booked in quickly. I've got Tuesday 10am or Wednesday 2pm free — which works best for you?"},
            {"role": "user", "text": "Sounds interesting but I'm really swamped this week"},
        ],
        "display_slots": ["Tuesday 4th March at 10:00am", "Wednesday 5th March at 2:00pm"],
        "offered_slots": ["2026-03-04T10:00:00+00:00", "2026-03-05T14:00:00+00:00"],
        "expected_intent": "engage",
    },
    {
        "label": "select_slot: yes the first one",
        "history": [
            {"role": "assistant", "text": "Hey Sarah — I've got Tuesday 10am or Wednesday 2pm free — which works best for you?"},
            {"role": "user", "text": "Yes, the first one works for me"},
        ],
        "display_slots": ["Tuesday 4th March at 10:00am", "Wednesday 5th March at 2:00pm"],
        "offered_slots": ["2026-03-04T10:00:00+00:00", "2026-03-05T14:00:00+00:00"],
        "expected_intent": "select_slot",
    },
    {
        "label": "request_specific_time: Friday around 3",
        "history": [
            {"role": "assistant", "text": "Hey — I've got Tuesday 10am or Wednesday 2pm free — which works best for you?"},
            {"role": "user", "text": "Neither work for me. Would Friday around 3 work?"},
        ],
        "display_slots": ["Tuesday 4th March at 10:00am", "Wednesday 5th March at 2:00pm"],
        "offered_slots": ["2026-03-04T10:00:00+00:00", "2026-03-05T14:00:00+00:00"],
        "expected_intent": "request_specific_time",
    },
    {
        "label": "request_slots: anything Thursday afternoon",
        "history": [
            {"role": "assistant", "text": "Hey — I've got Tuesday 10am or Wednesday 2pm free — which works best for you?"},
            {"role": "user", "text": "Got anything on Thursday afternoon?"},
        ],
        "display_slots": ["Tuesday 4th March at 10:00am", "Wednesday 5th March at 2:00pm"],
        "offered_slots": ["2026-03-04T10:00:00+00:00", "2026-03-05T14:00:00+00:00"],
        "expected_intent": "request_slots",
    },
    {
        "label": "decline: not interested",
        "history": [
            {"role": "assistant", "text": "Hey — thanks for reaching out. Want to get you booked in quickly. I've got Tuesday 10am or Wednesday 2pm free — which works best for you?"},
            {"role": "user", "text": "Not interested thanks"},
        ],
        "display_slots": ["Tuesday 4th March at 10:00am", "Wednesday 5th March at 2:00pm"],
        "offered_slots": ["2026-03-04T10:00:00+00:00", "2026-03-05T14:00:00+00:00"],
        "expected_intent": "decline",
    },
    {
        "label": "wants_human: speak to someone",
        "history": [
            {"role": "assistant", "text": "Hey — I've got Tuesday 10am or Wednesday 2pm free — which works best for you?"},
            {"role": "user", "text": "Can I speak to someone first?"},
        ],
        "display_slots": ["Tuesday 4th March at 10:00am", "Wednesday 5th March at 2:00pm"],
        "offered_slots": ["2026-03-04T10:00:00+00:00", "2026-03-05T14:00:00+00:00"],
        "expected_intent": "wants_human",
    },
]


async def main():
    passed = 0
    failed = 0

    for tc in TEST_CASES:
        result = await process_inbound_message(
            conversation_history=tc["history"],
            offered_slots=tc["offered_slots"],
            display_slots=tc["display_slots"],
            bot_settings=BOT_SETTINGS,
            llm_settings=LLM_SETTINGS,
        )

        intent = result["intent"]
        reply = result["reply_text"]
        ok = intent == tc["expected_intent"]

        status = "PASS" if ok else "FAIL"
        if ok:
            passed += 1
        else:
            failed += 1

        print(f"\n{status} | {tc['label']}")
        print(f"  Intent: {intent} (expected: {tc['expected_intent']})")
        print(f"  Reply:  {reply[:120]}")
        if result.get("error"):
            print(f"  Error:  {result['error']}")

    print(f"\n{'='*60}")
    print(f"Results: {passed}/{passed+failed} passed")
    if failed:
        print(f"FAILED: {failed} test(s)")


if __name__ == "__main__":
    asyncio.run(main())
