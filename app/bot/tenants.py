from __future__ import annotations
from typing import Any, Optional
import asyncpg
import json
import os

from app.utils.crypto import decrypt_credentials


LOAD_TENANT_SETTINGS_SQL = """
SELECT tenant_id::text, tenant_slug, calendar_adapter, messaging_adapter, settings
FROM core.tenants
WHERE tenant_id = $1::uuid
  AND is_enabled = TRUE;
"""

LOAD_TENANT_DEBUG_SQL = """
SELECT tenant_id::text, tenant_slug, is_enabled, calendar_adapter, messaging_adapter, settings
FROM core.tenants
WHERE tenant_id = $1::uuid;
"""

LOAD_TENANT_CREDENTIALS_SQL = """
SELECT provider, credentials
FROM core.tenant_credentials
WHERE tenant_id = $1::uuid;
"""


async def load_tenant(conn: asyncpg.Connection, tenant_id: str) -> dict[str, Any]:
    """Load tenant settings from core.tenants."""
    row = await conn.fetchrow(LOAD_TENANT_SETTINGS_SQL, tenant_id)
    if not row:
        raise RuntimeError(f"Tenant not found or disabled: {tenant_id}")

    # Handle settings as dict, JSON string, or None
    raw_settings = row["settings"]
    if isinstance(raw_settings, dict):
        settings = raw_settings
    elif isinstance(raw_settings, str):
        try:
            settings = json.loads(raw_settings)
        except json.JSONDecodeError:
            settings = {}
    else:
        settings = {}

    return {
        "tenant_id": row["tenant_id"],
        "tenant_slug": row["tenant_slug"],
        "calendar_adapter": row["calendar_adapter"],
        "messaging_adapter": row["messaging_adapter"],
        "settings": settings,
    }


async def load_tenant_debug(conn: asyncpg.Connection, tenant_id: str) -> dict[str, Any]:
    """Load tenant settings (including disabled) from core.tenants for debug."""
    row = await conn.fetchrow(LOAD_TENANT_DEBUG_SQL, tenant_id)
    if not row:
        raise RuntimeError(f"Tenant not found: {tenant_id}")

    # Handle settings as dict, JSON string, or None
    raw_settings = row["settings"]
    if isinstance(raw_settings, dict):
        settings = raw_settings
    elif isinstance(raw_settings, str):
        try:
            settings = json.loads(raw_settings)
        except json.JSONDecodeError:
            settings = {}
    else:
        settings = {}

    return {
        "tenant_id": row["tenant_id"],
        "tenant_slug": row["tenant_slug"],
        "is_enabled": row["is_enabled"],
        "calendar_adapter": row["calendar_adapter"],
        "messaging_adapter": row["messaging_adapter"],
        "settings": settings,
    }


async def load_tenant_credentials(
    conn: asyncpg.Connection,
    tenant_id: str,
    provider: Optional[str] = None,
) -> dict[str, dict[str, Any]]:
    """
    Load and decrypt tenant credentials from core.tenant_credentials.

    Args:
        conn: Database connection.
        tenant_id: Tenant UUID.
        provider: Optional filter for specific provider (e.g., "ghl").

    Returns:
        Dict keyed by provider, e.g.:
        {
            "ghl": {"access_token": "...", "location_id": "..."},
        }

    Falls back to global env vars if no credentials found in DB (migration path).
    """
    rows = await conn.fetch(LOAD_TENANT_CREDENTIALS_SQL, tenant_id)

    credentials: dict[str, dict[str, Any]] = {}

    for row in rows:
        row_provider = row["provider"]
        if provider and row_provider != provider:
            continue

        encrypted = row["credentials"]
        if encrypted:
            try:
                decrypted = decrypt_credentials(bytes(encrypted))
                credentials[row_provider] = decrypted
            except Exception as e:
                # Log but don't fail - allows fallback to env vars
                print(f"WARN: Failed to decrypt credentials for tenant={tenant_id} provider={row_provider}: {e}")

    # Fallback to global env vars if no DB credentials found (migration path)
    if not credentials.get("ghl"):
        env_token = os.getenv("GHL_ACCESS_TOKEN")
        if env_token:
            credentials["ghl"] = {"access_token": env_token}

    return credentials


def get_calendar_settings(tenant: dict[str, Any]) -> dict[str, Any]:
    """Extract calendar settings from tenant with defaults."""
    settings = tenant.get("settings") or {}
    cal = settings.get("calendar") or {}
    # Timezone fallback chain: settings.timezone -> settings.calendar.timezone -> Europe/London
    timezone = settings.get("timezone") or cal.get("timezone") or "Europe/London"
    return {
        "calendar_id": cal.get("calendar_id"),
        "timezone": timezone,
        "provider": tenant.get("calendar_adapter", "ghl"),
    }


def get_messaging_settings(tenant: dict[str, Any]) -> dict[str, Any]:
    """
    Extract messaging settings from tenant with defaults.

    Returns:
        {
            "dry_run": bool,  # If True, skip external API calls (default: False)
            "provider": str,  # messaging_adapter from tenant
        }
    """
    settings = tenant.get("settings") or {}
    messaging = settings.get("messaging") or {}
    return {
        "dry_run": bool(messaging.get("dry_run", False)),
        "provider": tenant.get("messaging_adapter", "ghl"),
    }


def get_booking_config(tenant: dict[str, Any]) -> dict[str, Any]:
    """
    Extract booking configuration from tenant settings.

    Returns:
        {
            "timezone": str,  # Fallback: settings.timezone -> settings.calendar.timezone -> Europe/London
            "availability": dict | None,  # Optional: settings.booking.availability
        }

    availability format (if present):
        {
            "mon": [{"start": "09:00", "end": "17:00"}],
            "tue": [{"start": "09:00", "end": "17:00"}],
            ...
        }
    """
    settings = tenant.get("settings") or {}
    cal = settings.get("calendar") or {}
    booking = settings.get("booking") or {}

    # Timezone fallback chain
    timezone = settings.get("timezone") or cal.get("timezone") or "Europe/London"

    # Optional availability windows
    availability = booking.get("availability")
    if availability and not isinstance(availability, dict):
        availability = None

    return {
        "timezone": timezone,
        "availability": availability,
    }


def get_bot_settings(tenant: dict[str, Any]) -> dict[str, Any]:
    """
    Extract bot behaviour settings from tenant.

    Tenant settings.bot structure:
        {
            "first_touch_template": "Hey{name_part}...",  # optional, uses default if absent
            "context": "HumTech, a revenue acceleration consultancy",  # injected into LLM prompt
            "reengagement": {
                "enabled": true,
                "delay_hours": 6,
                "max_attempts": 2
            },
            "handoff_ghl_user_id": "abc123"  # GHL user to assign conversation to on handoff
        }
    """
    settings = tenant.get("settings") or {}
    bot = settings.get("bot") or {}
    reengagement = bot.get("reengagement") or {}
    return {
        "first_touch_template": bot.get("first_touch_template"),
        "context": bot.get("context", ""),
        "persona": bot.get("persona", ""),
        "reengagement_enabled": bool(reengagement.get("enabled", True)),
        "reengagement_delay_hours": int(reengagement.get("delay_hours", 6)),
        "reengagement_max_attempts": int(reengagement.get("max_attempts", 2)),
        "handoff_ghl_user_id": bot.get("handoff_ghl_user_id"),
    }


def get_llm_settings(tenant: dict[str, Any]) -> dict[str, Any]:
    """
    Extract LLM rewriting settings from tenant.

    Returns:
        {
            "enabled": bool,       # Whether LLM rewriting is enabled (default: False)
            "model": str,          # Model to use (default: "stub")
            "temperature": float,  # Temperature for generation (default: 0.3)
            "prompt_version": str, # Version of prompt to use (default: "v1")
        }

    Tenant settings.llm structure:
        {
            "enabled": true,
            "model": "gpt-4o-mini",  # or "stub", "claude-3-haiku-20240307"
            "temperature": 0.3,
            "prompt_version": "v1"
        }
    """
    settings = tenant.get("settings") or {}
    llm = settings.get("llm") or {}
    return {
        "enabled": bool(llm.get("enabled", False)),
        "model": llm.get("model", "stub"),
        "temperature": float(llm.get("temperature", 0.3)),
        "prompt_version": llm.get("prompt_version", "v1"),
    }
