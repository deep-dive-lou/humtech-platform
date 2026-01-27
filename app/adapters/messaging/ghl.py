from typing import Any

async def send_message(*, tenant_settings: dict[str, Any], to_address: str, text: str) -> dict[str, Any]:
    # TODO: call GHL messaging later
    # For now, just pretend we sent it.
    return {"provider_msg_id": None, "status": "stored_only", "meta": {}}
