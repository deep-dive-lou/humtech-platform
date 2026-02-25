import asyncpg
import bcrypt
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator, Optional

from fastapi import Cookie, Depends, HTTPException
from jose import JWTError, jwt

from ..config import settings
from ..db import get_pool

_ALGORITHM = "HS256"
_EXPIRE_HOURS = 24


class NotAuthenticated(Exception):
    """Raised by require_staff when no valid JWT cookie is present."""


# ---------------------------------------------------------------------------
# Password helpers
# ---------------------------------------------------------------------------

def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()


def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode(), hashed.encode())


# ---------------------------------------------------------------------------
# JWT helpers
# ---------------------------------------------------------------------------

def create_jwt(staff_id: str, tenant_id: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(hours=_EXPIRE_HOURS)
    payload = {"sub": str(staff_id), "tenant_id": str(tenant_id), "exp": expire}
    return jwt.encode(payload, settings.portal_jwt_secret, algorithm=_ALGORITHM)


# ---------------------------------------------------------------------------
# Shared DB dependency (used by both routes.py and staff_routes.py)
# ---------------------------------------------------------------------------

async def get_conn() -> AsyncGenerator[asyncpg.Connection, None]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        yield conn


# ---------------------------------------------------------------------------
# Staff auth dependency
# ---------------------------------------------------------------------------

async def require_staff(
    portal_token: Optional[str] = Cookie(default=None),
    conn: asyncpg.Connection = Depends(get_conn),
) -> dict:
    """Decodes JWT cookie and fetches current role from DB. Raises NotAuthenticated if missing/invalid."""
    if not portal_token:
        raise NotAuthenticated()
    try:
        payload = jwt.decode(
            portal_token, settings.portal_jwt_secret, algorithms=[_ALGORITHM]
        )
        staff_id = payload["sub"]
        tenant_id = payload["tenant_id"]
    except JWTError:
        raise NotAuthenticated()

    row = await conn.fetchrow(
        "SELECT role FROM portal.staff_users WHERE id = $1::uuid AND is_active = true",
        staff_id,
    )
    if not row:
        raise NotAuthenticated()

    return {"staff_id": staff_id, "tenant_id": tenant_id, "role": row["role"]}


async def require_admin(staff: dict = Depends(require_staff)) -> dict:
    """Extends require_staff — raises 403 if role is not admin."""
    if staff["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return staff


async def get_tenant_brand(conn: asyncpg.Connection, tenant_id: str) -> dict:
    """Returns brand settings for the tenant (color, logo, name). Safe to call on every request."""
    row = await conn.fetchrow(
        "SELECT brand_color, logo_url, brand_name FROM portal.tenants WHERE id = $1::uuid",
        tenant_id,
    )
    return dict(row) if row else {}


# ---------------------------------------------------------------------------
# Audit helper
# ---------------------------------------------------------------------------

async def log_audit(
    conn: asyncpg.Connection,
    *,
    tenant_id,
    event_type: str,
    actor: str,
    actor_id=None,
    request_id=None,
    request_item_id=None,
    file_id=None,
    metadata: dict | None = None,
) -> None:
    await conn.execute(
        """
        INSERT INTO portal.audit_events
            (tenant_id, request_id, request_item_id, file_id,
             actor, actor_id, event_type, metadata)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """,
        tenant_id,
        request_id,
        request_item_id,
        file_id,
        actor,
        actor_id,
        event_type,
        metadata or {},
    )
