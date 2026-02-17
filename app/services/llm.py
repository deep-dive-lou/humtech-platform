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
            api_key = os.getenv("ANTHROPIC_API_KEY")
            if not api_key:
                return None

            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": api_key,
                        "anthropic-version": "2023-06-01",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": model,
                        "system": system,
                        "messages": [{"role": "user", "content": user}],
                        "temperature": temperature,
                        "max_tokens": max_tokens,
                    },
                )
                resp.raise_for_status()
                return resp.json()["content"][0]["text"].strip()

        return None  # Unknown model

    except Exception as e:
        print(f"LLM call failed ({model}): {e}")
        return None


# --- Stub rewriter (for testing without API) ---

STUB_SUBSTITUTIONS = [
    (r"^I've got two options:", "Here are two options:"),
    (r"^I've got one available option:", "Here's one available option:"),
    (r"Reply 1 or 2 to choose\.$", "Just reply 1 or 2 to pick one."),
    (r"Reply 1 to choose\.$", "Just reply 1 to pick it."),
    (r"^Perfect —", "Great —"),
    (r"Reply YES to confirm or NO to choose another\.$", "Reply YES to confirm, or NO for another option."),
    (r"^Booked ✅", "All set! ✅"),
    (r"See you then!$", "We'll see you then!"),
]


def _stub_rewrite(text: str) -> str:
    """Deterministic stub rewriter for testing."""
    for pattern, replacement in STUB_SUBSTITUTIONS:
        text = re.sub(pattern, replacement, text, flags=re.MULTILINE)
    return text


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

    try:
        if model == "stub":
            rewritten = _stub_rewrite(template_text)
        else:
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


async def classify_confirmation_intent_llm(
    text: str,
    llm_settings: dict[str, Any],
) -> dict[str, Any]:
    """
    Classify if text contains confirmation intent.

    Returns: {has_confirmation: bool|None, used: bool, error: str|None}
    """
    model = llm_settings.get("model", "stub")

    result = {
        "has_confirmation": None,
        "used": False,
        "error": None,
    }

    if model == "stub":
        result["error"] = "stub_mode"
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
