from __future__ import annotations
from typing import Any
import uuid


async def send_message(
    *,
    tenant_id: str,
    provider: str,
    channel: str,
    to_address: str,
    text: str,
    message_id: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Send a message via GHL messaging API.

    Stub: returns success with a fake provider_msg_id.
    TODO: call GHL messaging API when ready.
    """
    # Generate a fake provider message ID
    provider_msg_id = f"ghl-{uuid.uuid4().hex[:16]}"

    return {
        "success": True,
        "provider_msg_id": provider_msg_id,
        "provider": provider,
        "channel": channel,
        "to_address": to_address,
        "raw_response": {
            "status": "sent",
            "message_id": provider_msg_id,
            "stub": True,
        },
    }
