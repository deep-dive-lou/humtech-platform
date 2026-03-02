"""
Bot-to-Bot Conversation Simulator

Runs 12 customer personas against the real bot processor via /debug/bot/simulate.
Customer side is played by Claude Haiku. Bot side uses the full processor pipeline.

Usage:
    # Start server with stubs first:
    CALENDAR_STUB_SLOTS='["2026-03-03T09:00:00Z","2026-03-03T09:15:00Z","2026-03-03T14:00:00Z","2026-03-03T14:30:00Z","2026-03-04T10:00:00Z","2026-03-04T15:00:00Z","2026-03-05T09:00:00Z","2026-03-05T11:00:00Z"]' BOOKING_STUB=1 MESSAGING_STUB=1 python -m uvicorn app.main:app --port 8000

    # Then run:
    python scripts/bot_simulator.py
    python scripts/bot_simulator.py --persona happy_booker
    python scripts/bot_simulator.py --output report.json
"""

import argparse
import asyncio
import json
import os
import random
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime

import anthropic
import httpx

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class Config:
    base_url: str = "http://localhost:8000"
    tenant_id: str = "63eef4e2-68d1-4fdf-aa67-938c4008bc89"
    simulate_endpoint: str = "/debug/bot/simulate"
    anthropic_api_key: str = ""
    customer_model: str = "claude-haiku-4-5-20251001"
    max_turns: int = 10
    concurrency: int = 3
    run_id: int = 0  # randomised per run for phone isolation


# ---------------------------------------------------------------------------
# Persona definitions
# ---------------------------------------------------------------------------

PERSONAS = [
    {
        "id": "happy_booker",
        "name": "Sarah Mitchell",
        "goal": "Accept the first offered slot immediately",
        "expected_terminal": "booked",
        "system_prompt": (
            "You are Sarah Mitchell, a friendly business owner who just filled out a form. "
            "You want to book a call as soon as possible. When the bot offers you time slots, "
            "pick the first one enthusiastically. Be warm and direct. "
            "Example: 'Yes the first one works great!' or 'Perfect, book me in for that one!'"
        ),
    },
    {
        "id": "vague_lead",
        "name": "Dave Thompson",
        "goal": "Give low-signal responses that require clarification, eventually book",
        "expected_terminal": "booked",
        "system_prompt": (
            "You are Dave Thompson. You're somewhat interested but vague. "
            "Your first response should be non-committal like 'yeah maybe' or 'sounds alright'. "
            "When pushed for a day/time, give vague answers like 'sometime this week I suppose'. "
            "After 3-4 exchanges, finally agree to a specific slot when offered. "
            "Never volunteer a specific day or time unprompted."
        ),
    },
    {
        "id": "time_specific",
        "name": "Emma Clarke",
        "goal": "Request a specific time window (mornings before 10am only)",
        "expected_terminal": "booked",
        "system_prompt": (
            "You are Emma Clarke, a busy professional. You can ONLY do mornings before 10am. "
            "Say something like 'I can only do mornings, ideally before 10'. "
            "If offered afternoon slots, reject them politely and restate your morning preference. "
            "If offered a morning slot, accept it. Be professional and concise."
        ),
    },
    {
        "id": "rescheduler",
        "name": "James Wilson",
        "goal": "Book a slot then immediately ask to reschedule",
        "expected_terminal": "booked",
        "system_prompt": (
            "You are James Wilson. You're eager to book. When offered slots, pick the first one. "
            "IMPORTANT: After the bot confirms your booking, say 'Actually, can I change the time? "
            "Something came up.' Then when offered new slots, pick the second option. "
            "You must book first, then reschedule to a different slot."
        ),
    },
    {
        "id": "human_seeker",
        "name": "Linda Brown",
        "goal": "Ask to speak to a real person",
        "expected_terminal": "wants_human",
        "system_prompt": (
            "You are Linda Brown. You don't like automated systems. "
            "When the bot messages you, respond with something like "
            "'Can I speak to an actual person please?' or 'I'd rather talk to someone directly'. "
            "Be polite but firm. Do NOT engage with slot selection at all."
        ),
    },
    {
        "id": "decliner",
        "name": "Mark Stevens",
        "goal": "Decline the service",
        "expected_terminal": "decline",
        "system_prompt": (
            "You are Mark Stevens. You are not interested in the service at all. "
            "Respond with a clear rejection like 'No thanks, not interested' or "
            "'Please stop contacting me'. Be brief. One or two words is fine."
        ),
    },
    {
        "id": "confused",
        "name": "Brenda Hughes",
        "goal": "Send unrelated messages (thinks they're texting someone else)",
        "expected_terminal": "unclear",
        "system_prompt": (
            "You are Brenda Hughes. You think you're texting your daughter about dinner plans. "
            "Send messages like 'Did you pick up the chicken?', 'What time is nan coming over?', "
            "'The dog needs walking'. Never acknowledge the booking context. "
            "Stay completely off-topic for the entire conversation."
        ),
    },
    {
        "id": "negation_multi",
        "name": "Tom Price",
        "goal": "Use negation with day preferences then book",
        "expected_terminal": "booked",
        "system_prompt": (
            "You are Tom Price. You have complex scheduling constraints. "
            "Your first message should be: 'Monday doesn't work for me, and mornings are out. "
            "Could do Tuesday afternoon maybe, around 2 or 3?' "
            "If offered matching slots, pick one. If not matching, restate your preference."
        ),
    },
    {
        "id": "minimal_responder",
        "name": "Chris Ward",
        "goal": "Give one-word or ultra-minimal responses, eventually book",
        "expected_terminal": "booked",
        "system_prompt": (
            "You are Chris Ward. You respond with as few words as possible. "
            "Examples: 'k', 'sure', 'friday', 'afternoon', 'yeah first one', '1'. "
            "Never write more than 3 words. When a slot is offered, accept with 'yes' or '1'."
        ),
    },
    {
        "id": "aggressive",
        "name": "Gary Marsh",
        "goal": "Be impatient and demand to speak to someone",
        "expected_terminal": "wants_human",
        "system_prompt": (
            "You are Gary Marsh. You are frustrated and impatient. "
            "You want someone to call you RIGHT NOW. Your first message: 'Just call me back'. "
            "If the bot tries to offer slots, respond: 'I don't want to book online, "
            "get someone to phone me'. Escalate to wanting a human after 2 messages."
        ),
    },
    {
        "id": "post_booking",
        "name": "Rachel Green",
        "goal": "Keep messaging after booking is confirmed",
        "expected_terminal": "booked",
        "system_prompt": (
            "You are Rachel Green. When offered slots, immediately accept the first one. "
            "After the bot confirms the booking, send follow-up messages: "
            "'What is the meeting about exactly?', 'Will it be on Zoom?', 'How long will it be?'. "
            "Keep asking questions even after the booking is done."
        ),
    },
    {
        "id": "emoji_gibberish",
        "name": "Zoe X",
        "goal": "Send emoji-only or gibberish messages",
        "expected_terminal": "unclear",
        "system_prompt": (
            "You are testing edge cases. Send ONLY emojis or gibberish. "
            "Message 1: '!!!'. "
            "Message 2: 'asdfjkl'. "
            "Message 3: '???'. "
            "Never send real words or coherent sentences."
        ),
    },
]


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class TurnRecord:
    turn_number: int
    event_type: str
    customer_text: str
    bot_reply: str
    route: str


@dataclass
class ConversationResult:
    persona_id: str
    persona_name: str
    expected_terminal: str
    channel_address: str
    turns: list = field(default_factory=list)
    final_route: str = ""
    goal_achieved: bool = False
    routes_hit: list = field(default_factory=list)
    conversation_id: str | None = None
    error: str | None = None


# ---------------------------------------------------------------------------
# Customer simulator (Claude plays the customer)
# ---------------------------------------------------------------------------

TERMINAL_ROUTES = {"booked", "wants_human", "decline", "booking_failed"}

# post_booking persona keeps going after booked/already_booked
POST_BOOKING_CONTINUE_ROUTES = {"booked", "already_booked"}


async def simulate_customer_response(
    client: anthropic.AsyncAnthropic,
    persona: dict,
    conversation_history: list[dict],
    model: str,
) -> str:
    """Ask Claude-as-customer to generate the next SMS response."""
    messages = []
    for turn in conversation_history:
        if turn["role"] == "bot":
            messages.append({"role": "user", "content": f"{turn['text']}"})
        else:
            messages.append({"role": "assistant", "content": turn["text"]})

    system = (
        persona["system_prompt"]
        + "\n\nYou are texting via SMS. Keep responses SHORT (1-2 sentences max). "
        "Respond ONLY with your message text, nothing else. No quotes, no labels."
    )

    response = await client.messages.create(
        model=model,
        max_tokens=150,
        temperature=0.7,
        system=system,
        messages=messages,
    )
    return response.content[0].text.strip()


# ---------------------------------------------------------------------------
# Conversation runner (single persona)
# ---------------------------------------------------------------------------

async def run_conversation(
    http_client: httpx.AsyncClient,
    anthropic_client: anthropic.AsyncAnthropic,
    persona: dict,
    config: Config,
    persona_index: int,
) -> ConversationResult:
    """Run a full multi-turn conversation for one persona."""
    channel_address = f"+4477{config.run_id:05d}{persona_index:02d}"
    conversation_history: list[dict] = []
    turns: list[TurnRecord] = []
    routes_hit: list[str] = []
    conversation_id = None
    final_route = ""
    booked_once = False

    print(f"  [{persona['id']}] Starting conversation as {persona['name']}...")

    for turn_num in range(config.max_turns):
        # Turn 0 = new_lead (bot initiates), Turn 1+ = inbound_message
        if turn_num == 0:
            event_type = "new_lead"
            customer_text = ""
        else:
            event_type = "inbound_message"
            customer_text = await simulate_customer_response(
                anthropic_client, persona, conversation_history, config.customer_model
            )
            conversation_history.append({"role": "customer", "text": customer_text})

        # Call the simulate endpoint
        payload = {
            "tenant_id": config.tenant_id,
            "event_type": event_type,
            "text": customer_text,
            "display_name": persona["name"],
            "channel_address": channel_address,
            "channel": "sms",
        }
        resp = await http_client.post(
            f"{config.base_url}{config.simulate_endpoint}",
            json=payload,
            timeout=60.0,
        )
        resp.raise_for_status()
        data = resp.json()

        bot_reply = data.get("bot_reply") or ""
        route = data.get("route", "unknown")
        conversation_id = data.get("conversation_id") or conversation_id

        if bot_reply:
            conversation_history.append({"role": "bot", "text": bot_reply})

        routes_hit.append(route)
        turns.append(TurnRecord(
            turn_number=turn_num,
            event_type=event_type,
            customer_text=customer_text,
            bot_reply=bot_reply,
            route=route,
        ))

        final_route = route

        if route == "booked":
            booked_once = True

        # Check terminal state
        if route in TERMINAL_ROUTES:
            # post_booking persona continues after booking
            if persona["id"] == "post_booking" and route in POST_BOOKING_CONTINUE_ROUTES:
                continue
            break

        # already_booked is terminal for everyone except post_booking
        if route == "already_booked":
            if persona["id"] != "post_booking":
                break
            # post_booking continues but cap at 3 post-booking turns
            post_booking_turns = sum(1 for r in routes_hit if r == "already_booked")
            if post_booking_turns >= 3:
                break

    # For post_booking persona, goal is "booked" — check if it booked at least once
    if persona["id"] == "post_booking":
        goal_achieved = booked_once
    else:
        goal_achieved = final_route == persona["expected_terminal"]
        # confused/emoji personas may never hit terminal — "stuck" is expected
        if persona["expected_terminal"] == "unclear" and final_route in ("unclear", "offer_slots"):
            goal_achieved = True

    turn_summary = " -> ".join(routes_hit)
    status = "PASS" if goal_achieved else "FAIL"
    print(f"  [{persona['id']}] {status} ({len(turns)} turns) {turn_summary}")

    return ConversationResult(
        persona_id=persona["id"],
        persona_name=persona["name"],
        expected_terminal=persona["expected_terminal"],
        channel_address=channel_address,
        turns=turns,
        final_route=final_route,
        goal_achieved=goal_achieved,
        routes_hit=routes_hit,
        conversation_id=conversation_id,
    )


# ---------------------------------------------------------------------------
# Run all conversations with concurrency control
# ---------------------------------------------------------------------------

async def run_all_conversations(
    config: Config,
    personas: list[dict],
) -> list[ConversationResult]:
    semaphore = asyncio.Semaphore(config.concurrency)

    async with httpx.AsyncClient() as http_client:
        anthropic_client = anthropic.AsyncAnthropic(api_key=config.anthropic_api_key)

        async def run_with_semaphore(persona, index):
            async with semaphore:
                try:
                    return await run_conversation(
                        http_client, anthropic_client, persona, config, index
                    )
                except Exception as e:
                    print(f"  [{persona['id']}] ERROR: {e}")
                    return ConversationResult(
                        persona_id=persona["id"],
                        persona_name=persona["name"],
                        expected_terminal=persona["expected_terminal"],
                        channel_address=f"+4477{config.run_id:05d}{index:02d}",
                        error=str(e),
                    )

        tasks = [
            run_with_semaphore(persona, i)
            for i, persona in enumerate(personas)
        ]
        return await asyncio.gather(*tasks)


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

ALL_INTENTS = {"select_slot", "request_specific_time", "request_slots",
               "wants_human", "reschedule", "decline", "unclear"}

ALL_ROUTES = {"new_lead", "offer_slots", "booked", "booking_failed",
              "already_booked", "reschedule_offer", "reschedule_failed",
              "wants_human", "decline", "unclear", "handoff_pending",
              "request_specific_time", "request_slots"}

# Map: routes that prove an intent was triggered
ROUTE_TO_INTENT = {
    "booked": "select_slot",
    "booking_failed": "select_slot",
    "offer_slots": "request_slots",
    "request_specific_time": "request_specific_time",
    "wants_human": "wants_human",
    "decline": "decline",
    "unclear": "unclear",
    "reschedule_offer": "reschedule",
    "reschedule_failed": "reschedule",
}


def generate_report(results: list[ConversationResult]) -> dict:
    # Outcomes
    outcomes = {
        "booked": 0, "declined": 0, "wants_human": 0,
        "unclear": 0, "stuck": 0, "error": 0, "booking_failed": 0,
    }
    for r in results:
        if r.error:
            outcomes["error"] += 1
        elif r.final_route == "booked":
            outcomes["booked"] += 1
        elif r.final_route == "decline":
            outcomes["declined"] += 1
        elif r.final_route == "wants_human":
            outcomes["wants_human"] += 1
        elif r.final_route == "booking_failed":
            outcomes["booking_failed"] += 1
        elif r.final_route == "unclear":
            outcomes["unclear"] += 1
        else:
            outcomes["stuck"] += 1

    # Coverage
    routes_triggered = set()
    intents_triggered = set()
    for r in results:
        for route in r.routes_hit:
            routes_triggered.add(route)
            if route in ROUTE_TO_INTENT:
                intents_triggered.add(ROUTE_TO_INTENT[route])

    # Edge cases
    edge_cases = []
    for r in results:
        if r.error:
            continue
        unclear_count = sum(1 for route in r.routes_hit if route == "unclear")
        if unclear_count > 2:
            edge_cases.append({
                "persona": r.persona_id,
                "issue": f"Hit 'unclear' {unclear_count} times",
            })
        if len(r.turns) >= 10 and r.final_route not in TERMINAL_ROUTES:
            edge_cases.append({
                "persona": r.persona_id,
                "issue": f"Stuck at max turns (final route: {r.final_route})",
            })
        if not r.goal_achieved:
            edge_cases.append({
                "persona": r.persona_id,
                "issue": f"Goal missed: expected '{r.expected_terminal}', got '{r.final_route}'",
            })

    # Per-persona
    persona_details = []
    for r in results:
        persona_details.append({
            "persona_id": r.persona_id,
            "persona_name": r.persona_name,
            "turns": len(r.turns),
            "final_route": r.final_route,
            "expected_terminal": r.expected_terminal,
            "goal_achieved": r.goal_achieved,
            "route_sequence": r.routes_hit,
            "conversation_id": r.conversation_id,
            "error": r.error,
        })

    # Full transcripts
    transcripts = []
    for r in results:
        transcripts.append({
            "persona_id": r.persona_id,
            "persona_name": r.persona_name,
            "channel_address": r.channel_address,
            "turns": [
                {
                    "turn": t.turn_number,
                    "event_type": t.event_type,
                    "customer": t.customer_text,
                    "bot": t.bot_reply,
                    "route": t.route,
                }
                for t in r.turns
            ],
        })

    return {
        "run_at": datetime.utcnow().isoformat() + "Z",
        "summary": {
            "total_conversations": len(results),
            "outcomes": outcomes,
            "goals_achieved": sum(1 for r in results if r.goal_achieved),
            "goals_missed": sum(1 for r in results if not r.goal_achieved),
        },
        "coverage": {
            "intents_triggered": sorted(intents_triggered),
            "intents_missed": sorted(ALL_INTENTS - intents_triggered),
            "intent_coverage": f"{len(intents_triggered)}/{len(ALL_INTENTS)}",
            "routes_triggered": sorted(routes_triggered),
            "routes_missed": sorted(ALL_ROUTES - routes_triggered),
            "route_coverage": f"{len(routes_triggered & ALL_ROUTES)}/{len(ALL_ROUTES)}",
        },
        "personas": persona_details,
        "edge_cases": edge_cases,
        "transcripts": transcripts,
    }


def print_report(report: dict):
    s = report["summary"]
    c = report["coverage"]

    print(f"\n{'=' * 65}")
    print(f"  BOT SIMULATOR REPORT")
    print(f"{'=' * 65}")
    print(f"  Run at:         {report['run_at']}")
    print(f"  Conversations:  {s['total_conversations']}")
    print(f"  Goals achieved: {s['goals_achieved']}/{s['total_conversations']}")
    print()

    print("  Outcomes:")
    for k, v in s["outcomes"].items():
        if v > 0:
            print(f"    {k:<15} {v}")
    print()

    print(f"  Intent coverage: {c['intent_coverage']}")
    if c["intents_missed"]:
        print(f"    Missing: {', '.join(c['intents_missed'])}")
    print(f"  Route coverage:  {c['route_coverage']}")
    if c["routes_missed"]:
        print(f"    Missing: {', '.join(c['routes_missed'])}")
    print()

    print(f"  {'Persona':<20} {'Turns':>5}  {'Final Route':<22} {'Result':>6}")
    print(f"  {'-' * 20} {'-' * 5}  {'-' * 22} {'-' * 6}")
    for p in report["personas"]:
        status = "PASS" if p["goal_achieved"] else "FAIL"
        err = " (ERROR)" if p["error"] else ""
        print(f"  {p['persona_id']:<20} {p['turns']:>5}  {p['final_route']:<22} {status:>6}{err}")
    print()

    if report["edge_cases"]:
        print(f"  Edge Cases ({len(report['edge_cases'])}):")
        for ec in report["edge_cases"]:
            print(f"    - [{ec['persona']}] {ec['issue']}")
        print()

    print(f"{'=' * 65}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Bot-to-bot conversation simulator")
    parser.add_argument("--base-url", default="http://localhost:8000",
                        help="Server base URL (default: http://localhost:8000)")
    parser.add_argument("--tenant-id", default="63eef4e2-68d1-4fdf-aa67-938c4008bc89",
                        help="Tenant UUID to test against")
    parser.add_argument("--max-turns", type=int, default=10,
                        help="Max turns per conversation (default: 10)")
    parser.add_argument("--concurrency", type=int, default=3,
                        help="Parallel conversations (default: 3)")
    parser.add_argument("--model", default="claude-haiku-4-5-20251001",
                        help="Model for customer simulation (default: claude-haiku-4-5-20251001)")
    parser.add_argument("--output", default=None,
                        help="JSON report output path (default: auto-generated)")
    parser.add_argument("--persona", default=None,
                        help="Run only this persona ID (default: all)")
    args = parser.parse_args()

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY environment variable required")
        sys.exit(1)

    config = Config(
        base_url=args.base_url,
        tenant_id=args.tenant_id,
        max_turns=args.max_turns,
        concurrency=args.concurrency,
        customer_model=args.model,
        anthropic_api_key=api_key,
        run_id=random.randint(10000, 99999),
    )

    # Filter personas if --persona specified
    personas = PERSONAS
    if args.persona:
        personas = [p for p in PERSONAS if p["id"] == args.persona]
        if not personas:
            print(f"ERROR: Unknown persona '{args.persona}'")
            print(f"Available: {', '.join(p['id'] for p in PERSONAS)}")
            sys.exit(1)

    print(f"\nBot Simulator starting...")
    print(f"  Server:    {config.base_url}")
    print(f"  Tenant:    {config.tenant_id}")
    print(f"  Personas:  {len(personas)}")
    print(f"  Max turns: {config.max_turns}")
    print(f"  Run ID:    {config.run_id}")
    print()

    results = asyncio.run(run_all_conversations(config, personas))
    report = generate_report(results)
    print_report(report)

    # Write JSON report
    output_path = args.output or f"bot_sim_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"  Full report: {output_path}\n")


if __name__ == "__main__":
    main()
