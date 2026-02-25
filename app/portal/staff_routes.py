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
from .auth import (
    NotAuthenticated, create_jwt, get_conn, get_tenant_brand,
    hash_password, log_audit, require_admin, require_staff, verify_password,
)
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
# Login / logout
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
# Templates CRUD (admin only)
# NOTE: all /templates/* routes must come before /requests/* to avoid conflicts
# ---------------------------------------------------------------------------

@router.get("/templates", response_class=HTMLResponse)
async def staff_templates_list(
    request: Request,
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    rows = await conn.fetch(
        """
        SELECT t.id, t.name, t.description, t.is_active, t.created_at,
               COUNT(ti.id) AS item_count
        FROM portal.templates t
        LEFT JOIN portal.template_items ti ON ti.template_id = t.id
        WHERE t.tenant_id = $1::uuid AND t.is_active = true
        GROUP BY t.id
        ORDER BY t.created_at DESC
        """,
        staff["tenant_id"],
    )
    brand = await get_tenant_brand(conn, staff["tenant_id"])
    return templates.TemplateResponse("staff_templates.html", {
        "request": request,
        "tmplts": [dict(r) for r in rows],
        "staff": staff,
        "brand": brand,
    })


@router.get("/templates/new", response_class=HTMLResponse)
async def staff_template_new(
    request: Request,
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    brand = await get_tenant_brand(conn, staff["tenant_id"])
    return templates.TemplateResponse("staff_template_edit.html", {
        "request": request,
        "tmpl": None,
        "items": [],
        "staff": staff,
        "brand": brand,
    })


@router.post("/templates")
async def create_template(
    request: Request,
    name: str = Form(...),
    description: str = Form(default=""),
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    template_id = await conn.fetchval(
        """
        INSERT INTO portal.templates (tenant_id, name, description, created_by)
        VALUES ($1::uuid, $2, $3, $4::uuid)
        RETURNING id
        """,
        staff["tenant_id"],
        name.strip(),
        description.strip() or None,
        staff["staff_id"],
    )
    return RedirectResponse(
        url=f"/portal/staff/templates/{template_id}/edit",
        status_code=303,
    )


@router.get("/templates/{template_id}/edit", response_class=HTMLResponse)
async def staff_template_edit(
    template_id: str,
    request: Request,
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    tmpl = await conn.fetchrow(
        "SELECT id, name, description FROM portal.templates WHERE id = $1::uuid AND tenant_id = $2::uuid",
        template_id,
        staff["tenant_id"],
    )
    if not tmpl:
        return RedirectResponse(url="/portal/staff/templates", status_code=303)

    items = await conn.fetch(
        "SELECT id, title, instructions, required, sort_order FROM portal.template_items WHERE template_id = $1::uuid ORDER BY sort_order",
        template_id,
    )
    brand = await get_tenant_brand(conn, staff["tenant_id"])
    return templates.TemplateResponse("staff_template_edit.html", {
        "request": request,
        "tmpl": dict(tmpl),
        "items": [dict(i) for i in items],
        "staff": staff,
        "brand": brand,
    })


@router.post("/templates/{template_id}")
async def update_template(
    template_id: str,
    name: str = Form(...),
    description: str = Form(default=""),
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    await conn.execute(
        "UPDATE portal.templates SET name = $1, description = $2 WHERE id = $3::uuid AND tenant_id = $4::uuid",
        name.strip(),
        description.strip() or None,
        template_id,
        staff["tenant_id"],
    )
    return RedirectResponse(
        url=f"/portal/staff/templates/{template_id}/edit",
        status_code=303,
    )


@router.get("/templates/{template_id}/items")
async def get_template_items(
    template_id: str,
    staff: dict = Depends(require_staff),
    conn: asyncpg.Connection = Depends(get_conn),
):
    items = await conn.fetch(
        """
        SELECT id, title, instructions, required, sort_order
        FROM portal.template_items
        WHERE template_id = $1::uuid AND tenant_id = $2::uuid
        ORDER BY sort_order
        """,
        template_id,
        staff["tenant_id"],
    )
    return [dict(i) for i in items]


@router.post("/templates/{template_id}/items")
async def add_template_item(
    template_id: str,
    body: dict = Body(...),
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    title = (body.get("title") or "").strip()
    if not title:
        return JSONResponse({"error": "title required"}, status_code=400)

    # Next sort_order
    max_order = await conn.fetchval(
        "SELECT COALESCE(MAX(sort_order), -1) FROM portal.template_items WHERE template_id = $1::uuid",
        template_id,
    )
    item_id = await conn.fetchval(
        """
        INSERT INTO portal.template_items
            (template_id, tenant_id, title, instructions, required, sort_order)
        VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6)
        RETURNING id
        """,
        template_id,
        staff["tenant_id"],
        title,
        (body.get("instructions") or "").strip() or None,
        bool(body.get("required", True)),
        max_order + 1,
    )
    return {
        "id": str(item_id),
        "title": title,
        "instructions": body.get("instructions"),
        "required": bool(body.get("required", True)),
        "sort_order": max_order + 1,
    }


@router.delete("/templates/{template_id}/items/{item_id}")
async def delete_template_item(
    template_id: str,
    item_id: str,
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    await conn.execute(
        "DELETE FROM portal.template_items WHERE id = $1::uuid AND template_id = $2::uuid AND tenant_id = $3::uuid",
        item_id,
        template_id,
        staff["tenant_id"],
    )
    return {"ok": True}


# ---------------------------------------------------------------------------
# Settings — branding (admin only)
# ---------------------------------------------------------------------------

@router.get("/settings", response_class=HTMLResponse)
async def staff_settings(
    request: Request,
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    brand = await get_tenant_brand(conn, staff["tenant_id"])
    return templates.TemplateResponse("staff_settings.html", {
        "request": request,
        "staff": staff,
        "brand": brand,
        "saved": request.query_params.get("saved"),
    })


@router.post("/settings")
async def update_settings(
    brand_name: str = Form(default=""),
    brand_color: str = Form(default=""),
    logo_url: str = Form(default=""),
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    await conn.execute(
        """
        UPDATE portal.tenants
        SET brand_name = $1, brand_color = $2, logo_url = $3
        WHERE id = $4::uuid
        """,
        brand_name.strip() or None,
        brand_color.strip() or None,
        logo_url.strip() or None,
        staff["tenant_id"],
    )
    return RedirectResponse(url="/portal/staff/settings?saved=1", status_code=303)


# ---------------------------------------------------------------------------
# New request form + create — MUST come before /requests/{request_id}
# ---------------------------------------------------------------------------

@router.get("/requests/new", response_class=HTMLResponse)
async def staff_request_new_form(
    request: Request,
    staff: dict = Depends(require_staff),
    conn: asyncpg.Connection = Depends(get_conn),
):
    tmplts = await conn.fetch(
        "SELECT id, name FROM portal.templates WHERE tenant_id = $1::uuid AND is_active = true ORDER BY name",
        staff["tenant_id"],
    )
    brand = await get_tenant_brand(conn, staff["tenant_id"])
    return templates.TemplateResponse("staff_request_new.html", {
        "request": request,
        "templates": [dict(t) for t in tmplts],
        "staff": staff,
        "brand": brand,
    })


@router.post("/requests")
async def create_request(
    request: Request,
    full_name: str = Form(...),
    email: str = Form(...),
    phone: str = Form(default=""),
    due_at: str = Form(default=""),
    template_id: str = Form(default=""),
    staff: dict = Depends(require_staff),
    conn: asyncpg.Connection = Depends(get_conn),
):
    email = email.lower().strip()
    phone_val = phone.strip() or None
    due_val = None
    if due_at.strip():
        try:
            from datetime import date
            due_val = datetime.strptime(due_at.strip(), "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            pass

    async with conn.transaction():
        # Upsert client
        client_id = await conn.fetchval(
            """
            INSERT INTO portal.clients (tenant_id, full_name, email, phone_e164)
            VALUES ($1::uuid, $2, $3, $4)
            ON CONFLICT DO NOTHING
            RETURNING id
            """,
            staff["tenant_id"],
            full_name.strip(),
            email,
            phone_val,
        )
        if not client_id:
            client_id = await conn.fetchval(
                "SELECT id FROM portal.clients WHERE tenant_id = $1::uuid AND email = $2",
                staff["tenant_id"],
                email,
            )
            # Update name if client exists
            await conn.execute(
                "UPDATE portal.clients SET full_name = $1 WHERE id = $2",
                full_name.strip(),
                client_id,
            )

        # Create request
        request_id = await conn.fetchval(
            """
            INSERT INTO portal.doc_requests (tenant_id, client_id, due_at, status)
            VALUES ($1::uuid, $2::uuid, $3, 'draft')
            RETURNING id
            """,
            staff["tenant_id"],
            client_id,
            due_val,
        )

        # Clone items from template
        if template_id.strip():
            items = await conn.fetch(
                "SELECT * FROM portal.template_items WHERE template_id = $1::uuid ORDER BY sort_order",
                template_id,
            )
            for item in items:
                await conn.execute(
                    """
                    INSERT INTO portal.doc_request_items
                        (tenant_id, request_id, item_type, title, instructions, required, sort_order)
                    VALUES ($1::uuid, $2::uuid, $3::public.template_item_type, $4, $5, $6, $7)
                    """,
                    staff["tenant_id"],
                    request_id,
                    item["item_type"],
                    item["title"],
                    item["instructions"],
                    item["required"],
                    item["sort_order"],
                )

    return RedirectResponse(
        url=f"/portal/staff/requests/{request_id}",
        status_code=303,
    )


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
    brand = await get_tenant_brand(conn, staff["tenant_id"])
    return templates.TemplateResponse("staff_list.html", {
        "request": request,
        "requests": [dict(r) for r in rows],
        "staff": staff,
        "brand": brand,
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

    files_by_item: dict = {}
    for f in files:
        f_dict = dict(f)
        try:
            f_dict["download_url"] = presign_get(f["storage_key"])
        except Exception:
            f_dict["download_url"] = None
        item_id = str(f["request_item_id"])
        files_by_item.setdefault(item_id, []).append(f_dict)

    brand = await get_tenant_brand(conn, staff["tenant_id"])
    return templates.TemplateResponse("staff_detail.html", {
        "request": request,
        "req": dict(req),
        "items": [dict(i) for i in items],
        "files_by_item": files_by_item,
        "staff": staff,
        "brand": brand,
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
            "SELECT id FROM portal.doc_requests WHERE id = $1::uuid AND tenant_id = $2::uuid",
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
            "SELECT id, request_id, tenant_id FROM portal.doc_request_items WHERE id = $1::uuid AND tenant_id = $2::uuid",
            item_id,
            staff["tenant_id"],
        )
        if not item:
            return JSONResponse({"error": "Item not found"}, status_code=404)

        await conn.execute(
            "UPDATE portal.doc_request_items SET status = $1::public.request_item_status WHERE id = $2::uuid",
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
