from typing import Any

async def get_free_slots(*, tenant_settings: dict[str, Any], entities: dict[str, Any]) -> dict[str, Any]:
    # TODO: call GHL calendar API later
    # For now, return empty slots to prove wiring.
    return {"slots": [], "timezone": tenant_settings.get("timezone", "Europe/London"), "meta": {}}
