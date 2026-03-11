"""
Analytics Command Centre — Authentication

Single-password auth for Lou's internal analytics UI.
No DB user table — password stored as bcrypt hash in env var.
"""

from __future__ import annotations

import base64
import bcrypt
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Cookie
from jose import JWTError, jwt

from ...config import settings

_ALGORITHM = "HS256"
_EXPIRE_HOURS = 72
_COOKIE_NAME = "analytics_token"


class AnalyticsNotAuthenticated(Exception):
    """Raised when no valid analytics JWT cookie is present."""


def verify_password(plain: str) -> bool:
    stored = settings.analytics_password_hash
    if not stored:
        return False
    # Hash may be base64-encoded to avoid Docker Compose $ interpolation
    if not stored.startswith("$"):
        stored = base64.b64decode(stored).decode()
    return bcrypt.checkpw(plain.encode(), stored.encode())


def create_jwt() -> str:
    expire = datetime.now(timezone.utc) + timedelta(hours=_EXPIRE_HOURS)
    payload = {"sub": "analytics", "exp": expire}
    return jwt.encode(payload, settings.analytics_jwt_secret, algorithm=_ALGORITHM)


def require_analytics(
    analytics_token: Optional[str] = Cookie(default=None),
) -> dict:
    """Dependency — verifies JWT cookie. Returns {"user": "analytics"}."""
    if not analytics_token:
        raise AnalyticsNotAuthenticated()
    try:
        payload = jwt.decode(
            analytics_token, settings.analytics_jwt_secret, algorithms=[_ALGORITHM]
        )
        if payload.get("sub") != "analytics":
            raise AnalyticsNotAuthenticated()
    except JWTError:
        raise AnalyticsNotAuthenticated()
    return {"user": "analytics"}
