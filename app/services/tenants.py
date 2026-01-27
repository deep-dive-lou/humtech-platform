from __future__ import annotations
from typing import Any
import asyncpg


LOAD_TENANT_SETTINGS_SQL = """
SELECT tenant_id::text, tenant_slug, calendar_adapter, messaging_adapter, settings
FROM core.tenants
WHERE tenant_id = $1::uuid
  AND is_enabled = TRUE;
"""


async def load_tenant(conn: asyncpg.Connection, tenant_id: str) -> dict[str, Any]:
    """Load tenant settings from core.tenants."""
    row = await conn.fetchrow(LOAD_TENANT_SETTINGS_SQL, tenant_id)
    if not row:
        raise RuntimeError(f"Tenant not found or disabled: {tenant_id}")

    settings = row["settings"] if isinstance(row["settings"], dict) else {}
    return {
        "tenant_id": row["tenant_id"],
        "tenant_slug": row["tenant_slug"],
        "calendar_adapter": row["calendar_adapter"],
        "messaging_adapter": row["messaging_adapter"],
        "settings": settings,
    }


def get_calendar_settings(tenant: dict[str, Any]) -> dict[str, Any]:
    """Extract calendar settings from tenant with defaults."""
    settings = tenant.get("settings") or {}
    cal = settings.get("calendar") or {}
    return {
        "calendar_id": cal.get("calendar_id"),
        "timezone": cal.get("timezone", "Europe/London"),
        "provider": tenant.get("calendar_adapter", "ghl"),
    }
