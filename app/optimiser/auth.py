"""
Optimisation Engine — Authentication

Fully separate from portal auth. Own JWT cookie, own user table, own login/logout.
"""

from __future__ import annotations

import bcrypt
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator, Optional

import asyncpg
from fastapi import APIRouter, Cookie, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from jose import JWTError, jwt

from ..config import settings
from ..db import get_pool

_ALGORITHM = "HS256"
_EXPIRE_HOURS = 24
_COOKIE_NAME = "optimiser_token"

router = APIRouter(prefix="/optimiser", tags=["optimiser-auth"])


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class OptimiserNotAuthenticated(Exception):
    """Raised when no valid optimiser JWT cookie is present."""


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

def _create_jwt(user_id: str, tenant_id: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(hours=_EXPIRE_HOURS)
    payload = {"sub": str(user_id), "tenant_id": str(tenant_id), "exp": expire}
    return jwt.encode(payload, settings.optimiser_jwt_secret, algorithm=_ALGORITHM)


# ---------------------------------------------------------------------------
# DB dependency
# ---------------------------------------------------------------------------

async def get_conn() -> AsyncGenerator[asyncpg.Connection, None]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        yield conn


# ---------------------------------------------------------------------------
# Auth dependency — inject into protected routes
# ---------------------------------------------------------------------------

async def require_optimiser_user(
    optimiser_token: Optional[str] = Cookie(default=None),
    conn: asyncpg.Connection = Depends(get_conn),
) -> dict:
    """Decodes optimiser JWT cookie and fetches user from DB.

    Returns: {"user_id", "tenant_id", "role", "full_name", "email"}
    """
    if not optimiser_token:
        raise OptimiserNotAuthenticated()
    try:
        payload = jwt.decode(
            optimiser_token, settings.optimiser_jwt_secret, algorithms=[_ALGORITHM]
        )
        user_id = payload["sub"]
        tenant_id = payload["tenant_id"]
    except JWTError:
        raise OptimiserNotAuthenticated()

    row = await conn.fetchrow(
        "SELECT role, full_name, email FROM optimiser.users WHERE user_id = $1::uuid AND is_active = true",
        user_id,
    )
    if not row:
        raise OptimiserNotAuthenticated()

    return {
        "user_id": user_id,
        "tenant_id": tenant_id,
        "role": row["role"],
        "full_name": row["full_name"],
        "email": row["email"],
    }


# ---------------------------------------------------------------------------
# Login / Logout routes
# ---------------------------------------------------------------------------

import os
from fastapi.templating import Jinja2Templates

_templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "templates")
)


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: Optional[str] = None):
    return _templates.TemplateResponse("login.html", {
        "request": request,
        "error": error,
    })


@router.post("/login", response_class=HTMLResponse)
async def login_submit(
    request: Request,
    conn: asyncpg.Connection = Depends(get_conn),
    email: str = Form(...),
    password: str = Form(...),
):
    row = await conn.fetchrow(
        "SELECT user_id, tenant_id, password_hash, full_name, role FROM optimiser.users WHERE email = $1 AND is_active = true",
        email.strip().lower(),
    )
    if not row or not verify_password(password, row["password_hash"]):
        return _templates.TemplateResponse("login.html", {
            "request": request,
            "error": "Invalid email or password",
        })

    token = _create_jwt(str(row["user_id"]), str(row["tenant_id"]))
    response = RedirectResponse(url="/optimiser/", status_code=303)
    response.set_cookie(
        key=_COOKIE_NAME,
        value=token,
        httponly=True,
        max_age=_EXPIRE_HOURS * 3600,
        samesite="lax",
    )
    return response


@router.get("/logout")
async def logout():
    response = RedirectResponse(url="/optimiser/login", status_code=303)
    response.delete_cookie(_COOKIE_NAME)
    return response