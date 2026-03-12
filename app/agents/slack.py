"""
Shared Slack reporter for HumTech agents.

Handles message posting, rate limiting, and overflow protection.
All agents use this to communicate results.
"""
from __future__ import annotations

import asyncio
import logging

import httpx

logger = logging.getLogger(__name__)

SLACK_MAX_CHARS = 39_000  # Slack truncates at 40k — leave buffer


class SlackReporter:
    """Post messages to a Slack webhook with rate limiting and overflow guard."""

    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    async def post(self, text: str) -> bool:
        """Post a single message. Returns True on success."""
        if len(text) > SLACK_MAX_CHARS:
            text = text[:SLACK_MAX_CHARS] + "\n\n_(message truncated)_"
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(self.webhook_url, json={"text": text})
                if resp.status_code != 200:
                    logger.warning("Slack webhook returned %d: %s", resp.status_code, resp.text[:200])
                    return False
            return True
        except Exception as e:
            logger.error("Failed to post to Slack: %s", e)
            return False

    async def post_sequence(self, messages: list[str], delay: float = 0.5) -> int:
        """Post multiple messages with delay between each. Returns count sent."""
        sent = 0
        for msg in messages:
            if await self.post(msg):
                sent += 1
            if sent < len(messages):
                await asyncio.sleep(delay)
        return sent
