import hashlib
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

import asyncpg
from fastapi import APIRouter, Body, Depends, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from ..config import settings
from .auth import NotAuthenticated, create_jwt, get_conn, hash_password, log_audit, require_staff, verify_password
from .storage import presign_get

router = APIRouter(prefix="/portal/staff", tags=["portal-staff"])

templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "templates")
)

TENANT_SLUG = settings.portal_tenant_slug


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Login
# ---------------------------------------------------------------------------

@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: Optional[str] = None):
    return templates.TemplateResponse("staff_login.html", {
        "request": request,
        "error": error,
    })


@router.post("/login")
async def login(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    conn: asyncpg.Connection = Depends(get_conn),
):
    row = await conn.fetchrow(
        """
        SELECT su.id, su.password_hash, su.tenant_id, su.is_active
        FROM portal.staff_users su
        JOIN portal.tenants t ON t.id = su.tenant_id
        WHERE su.email = $1 AND t.slug = $2
        """,
        email.lower().strip(),
        TENANT_SLUG,
    )

    if not row or not row["is_active"] or not verify_password(password, row["password_hash"]):
        return RedirectResponse(
            url="/portal/staff/login?error=Invalid+email+or+password",
            status_code=303,
        )

    await conn.execute(
        "UPDATE portal.staff_users SET last_login_at = now() WHERE id = $1",
        row["id"],
    )

    token = create_jwt(str(row["id"]), str(row["tenant_id"]))
    response = RedirectResponse(url="/portal/staff/requests", status_code=303)
    response.set_cookie(
        key="portal_token",
        value=token,
        httponly=True,
        samesite="lax",
        max_age=86400,
    )
    return response


@router.get("/logout")
async def logout():
    response = RedirectResponse(url="/portal/staff/login", status_code=303)
    response.delete_cookie("portal_token")
    return response


@router.get("/", response_class=HTMLResponse)
async def staff_root():
    return RedirectResponse(url="/portal/staff/requests", status_code=303)


# ---------------------------------------------------------------------------
# Request list
# ---------------------------------------------------------------------------

@router.get("/requests", response_class=HTMLResponse)
async def staff_requests(
    request: Request,
    staff: dict = Depends(require_staff),
    conn: asyncpg.Connection = Depends(get_conn),
):
    rows = await conn.fetch(
        """
        SELECT r.id, r.status::text AS status, r.due_at, r.created_at, r.sent_at,
               c.full_name AS client_name, c.email AS client_email,
               COUNT(ri.id) AS item_total,
               COUNT(CASE WHEN ri.status = 'approved' THEN 1 END) AS item_done
        FROM portal.doc_requests r
        JOIN portal.clients c ON c.id = r.client_id
        LEFT JOIN portal.doc_request_items ri ON ri.request_id = r.id
        WHERE r.tenant_id = $1::uuid
        GROUP BY r.id, r.status, r.due_at, r.created_at, r.sent_at, c.full_name, c.email
        ORDER BY r.created_at DESC
        """,
        staff["tenant_id"],
    )
    return templates.TemplateResponse("staff_list.html", {
        "request": request,
        "requests": [dict(r) for r in rows],
        "staff": staff,
    })


# ---------------------------------------------------------------------------
# Request detail
# ---------------------------------------------------------------------------

@router.get("/requests/{request_id}", response_class=HTMLResponse)
async def staff_request_detail(
    request_id: str,
    request: Request,
    staff: dict = Depends(require_staff),
    conn: asyncpg.Connection = Depends(get_conn),
):
    req = await conn.fetchrow(
        """
        SELECT r.id, r.status::text AS status, r.due_at, r.created_at,
               r.sent_at, r.last_viewed_at,
               c.full_name AS client_name, c.email AS client_email,
               c.phone_e164 AS client_phone
        FROM portal.doc_requests r
        JOIN portal.clients c ON c.id = r.client_id
        WHERE r.id = $1::uuid AND r.tenant_id = $2::uuid
        """,
        request_id,
        staff["tenant_id"],
    )
    if not req:
        return RedirectResponse(url="/portal/staff/requests", status_code=303)

    items = await conn.fetch(
        """
        SELECT id, title, item_type::text AS item_type, required,
               status::text AS status, sort_order, instructions
        FROM portal.doc_request_items
        WHERE request_id = $1::uuid
        ORDER BY sort_order ASC
        """,
        request_id,
    )

    files = await conn.fetch(
        """
        SELECT f.id, f.request_item_id, f.original_filename,
               f.size_bytes, f.storage_key, f.created_at
        FROM portal.files f
        WHERE f.request_id = $1::uuid
        ORDER BY f.created_at DESC
        """,
        request_id,
    )

    # Build presigned download URLs and group files by item_id
    files_by_item: dict = {}
    for f in files:
        f_dict = dict(f)
        try:
            f_dict["download_url"] = presign_get(f["storage_key"])
        except Exception:
            f_dict["download_url"] = None
        item_id = str(f["request_item_id"])
        files_by_item.setdefault(item_id, []).append(f_dict)

    return templates.TemplateResponse("staff_detail.html", {
        "request": request,
        "req": dict(req),
        "items": [dict(i) for i in items],
        "files_by_item": files_by_item,
        "staff": staff,
    })


# ---------------------------------------------------------------------------
# Generate magic link (AJAX — returns JSON)
# ---------------------------------------------------------------------------

@router.post("/requests/{request_id}/link")
async def generate_link(
    request_id: str,
    staff: dict = Depends(require_staff),
    conn: asyncpg.Connection = Depends(get_conn),
    expires_days: int = 30,
):
    async with conn.transaction():
        req = await conn.fetchrow(
            """
            SELECT id FROM portal.doc_requests
            WHERE id = $1::uuid AND tenant_id = $2::uuid
            """,
            request_id,
            staff["tenant_id"],
        )
        if not req:
            return JSONResponse({"error": "Request not found"}, status_code=404)

        raw_token = secrets.token_urlsafe(32)
        token_hash = _hash_token(raw_token)
        expires_at = _now_utc() + timedelta(days=expires_days)

        await conn.execute(
            """
            INSERT INTO portal.request_access_tokens
                (tenant_id, request_id, token_hash, expires_at)
            VALUES ($1::uuid, $2::uuid, $3, $4)
            """,
            staff["tenant_id"],
            request_id,
            token_hash,
            expires_at,
        )

        await log_audit(
            conn,
            tenant_id=staff["tenant_id"],
            event_type="access_link_created",
            actor="staff",
            actor_id=staff["staff_id"],
            request_id=request_id,
        )

    link = f"{settings.portal_base_url}/portal/r/{raw_token}/view"
    return {"link": link, "expires_at": expires_at.isoformat()}


# ---------------------------------------------------------------------------
# Item review — approve or reject (AJAX — returns JSON)
# ---------------------------------------------------------------------------

@router.post("/items/{item_id}/review")
async def review_item(
    item_id: str,
    action: str = Body(..., embed=True),
    staff: dict = Depends(require_staff),
    conn: asyncpg.Connection = Depends(get_conn),
):
    if action not in ("approve", "reject"):
        return JSONResponse({"error": "action must be approve or reject"}, status_code=400)

    new_status = "approved" if action == "approve" else "missing"

    async with conn.transaction():
        item = await conn.fetchrow(
            """
            SELECT id, request_id, tenant_id
            FROM portal.doc_request_items
            WHERE id = $1::uuid AND tenant_id = $2::uuid
            """,
            item_id,
            staff["tenant_id"],
        )
        if not item:
            return JSONResponse({"error": "Item not found"}, status_code=404)

        await conn.execute(
            """
            UPDATE portal.doc_request_items
            SET status = $1::public.request_item_status
            WHERE id = $2::uuid
            """,
            new_status,
            item_id,
        )

        event_type = "item_approved" if action == "approve" else "item_rejected"
        await log_audit(
            conn,
            tenant_id=staff["tenant_id"],
            event_type=event_type,
            actor="staff",
            actor_id=staff["staff_id"],
            request_id=str(item["request_id"]),
            request_item_id=item_id,
        )

    return {"item_id": item_id, "status": new_status}
