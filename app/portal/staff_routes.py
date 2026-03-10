import hashlib
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

import asyncpg
from fastapi import APIRouter, Body, Depends, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from ..config import settings
from .auth import (
    NotAuthenticated, create_jwt, get_conn, get_tenant_brand,
    hash_password, log_audit, require_admin, require_staff, verify_password,
)
from .storage import presign_get
from .email import (
    check_domain_verification, render_email, render_subject, send_email,
    verify_domain, DEFAULT_BODY,
)

router = APIRouter(prefix="/portal/staff", tags=["portal-staff"])

templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "templates")
)

TENANT_SLUG = settings.portal_tenant_slug


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _relative_time(dt) -> str:
    """Convert a datetime to a human-readable relative time string."""
    if not dt:
        return ""
    now = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = now - dt
    if delta.days > 30:
        return dt.strftime("%d %b")
    if delta.days > 0:
        return f"{delta.days}d ago"
    hours = delta.seconds // 3600
    if hours > 0:
        return f"{hours}h ago"
    mins = delta.seconds // 60
    return f"{mins}m ago" if mins > 1 else "just now"


# ---------------------------------------------------------------------------
# Login / logout
# ---------------------------------------------------------------------------

@router.get("/login", response_class=HTMLResponse)
async def login_page(
    request: Request,
    error: Optional[str] = None,
    conn: asyncpg.Connection = Depends(get_conn),
):
    row = await conn.fetchrow(
        "SELECT brand_color, logo_url, brand_name FROM portal.tenants WHERE slug = $1",
        TENANT_SLUG,
    )
    brand = dict(row) if row else {}
    return templates.TemplateResponse("staff_login.html", {
        "request": request,
        "error": error,
        "brand": brand,
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
    response = RedirectResponse(url="/portal/staff/", status_code=303)
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


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

@router.get("/", response_class=HTMLResponse)
async def staff_dashboard(
    request: Request,
    scope: str = "all",
    staff: dict = Depends(require_staff),
    conn: asyncpg.Connection = Depends(get_conn),
):
    tid = staff["tenant_id"]
    mine = scope == "mine"
    staff_filter = " AND r.created_by_staff_id = $2::uuid" if mine else ""
    params: list = [tid]
    if mine:
        params.append(staff["staff_id"])

    stats = await conn.fetchrow(
        f"""
        SELECT
            COUNT(*) FILTER (WHERE r.status NOT IN ('completed', 'closed')) AS open_count,
            COUNT(*) FILTER (
                WHERE r.status NOT IN ('completed', 'closed')
                AND r.due_at IS NOT NULL AND r.due_at < now()
            ) AS overdue_count,
            COUNT(*) FILTER (
                WHERE r.status = 'completed'
                AND r.created_at >= now() - interval '7 days'
            ) AS completed_week
        FROM portal.doc_requests r
        WHERE r.tenant_id = $1::uuid{staff_filter}
        """,
        *params,
    )

    awaiting = await conn.fetchval(
        f"""
        SELECT COUNT(DISTINCT ri.request_id)
        FROM portal.doc_request_items ri
        JOIN portal.doc_requests r ON r.id = ri.request_id
        WHERE ri.tenant_id = $1::uuid
          AND ri.status = 'uploaded'
          AND r.status NOT IN ('completed', 'closed')
          {staff_filter}
        """,
        *params,
    )

    recent = await conn.fetch(
        f"""
        SELECT r.id, r.status::text AS status, r.due_at, r.created_at,
               r.last_viewed_at, r.sent_at,
               c.full_name AS client_name, c.email AS client_email,
               COUNT(ri.id) AS item_total,
               COUNT(CASE WHEN ri.status = 'approved' THEN 1 END) AS item_done,
               COUNT(CASE WHEN ri.status = 'uploaded' THEN 1 END) AS item_awaiting
        FROM portal.doc_requests r
        JOIN portal.clients c ON c.id = r.client_id
        LEFT JOIN portal.doc_request_items ri ON ri.request_id = r.id
        WHERE r.tenant_id = $1::uuid
          AND r.status NOT IN ('completed', 'closed')
          AND EXISTS (
              SELECT 1 FROM portal.doc_request_items ri2
              WHERE ri2.request_id = r.id AND ri2.status = 'uploaded'
          )
          {staff_filter}
        GROUP BY r.id, r.status, r.due_at, r.created_at, r.last_viewed_at,
                 r.sent_at, c.full_name, c.email
        ORDER BY r.created_at DESC
        LIMIT 5
        """,
        *params,
    )

    # Activity feed — last 10 audit events across the tenant
    activity = await conn.fetch(
        """
        SELECT ae.event_type, ae.actor, ae.metadata, ae.event_time,
               COALESCE(su.full_name, 'Client') AS actor_name,
               cl.full_name AS client_name
        FROM portal.audit_events ae
        LEFT JOIN portal.staff_users su
            ON ae.actor = 'staff' AND ae.actor_id = su.id
        LEFT JOIN portal.doc_requests dr ON ae.request_id = dr.id
        LEFT JOIN portal.clients cl ON dr.client_id = cl.id
        WHERE ae.tenant_id = $1::uuid
        ORDER BY ae.event_time DESC
        LIMIT 10
        """,
        tid,
    )

    # Stale requests — sent 7+ days ago, never viewed
    stale_count = await conn.fetchval(
        """
        SELECT COUNT(*)
        FROM portal.doc_requests
        WHERE tenant_id = $1::uuid
          AND status = 'sent'
          AND sent_at < now() - interval '7 days'
          AND last_viewed_at IS NULL
        """,
        tid,
    )

    # Average client response time (sent → first view)
    avg_response = await conn.fetchval(
        """
        SELECT ROUND(EXTRACT(EPOCH FROM AVG(last_viewed_at - sent_at)) / 3600.0, 1)
        FROM portal.doc_requests
        WHERE tenant_id = $1::uuid
          AND sent_at IS NOT NULL
          AND last_viewed_at IS NOT NULL
        """,
        tid,
    )

    brand = await get_tenant_brand(conn, tid)
    return templates.TemplateResponse("staff_dashboard.html", {
        "request": request,
        "staff": staff,
        "brand": brand,
        "scope": scope,
        "stats": dict(stats),
        "awaiting_review": awaiting or 0,
        "recent": [dict(r) for r in recent],
        "activity": [dict(a) for a in activity],
        "stale_count": stale_count or 0,
        "avg_response_hours": float(avg_response) if avg_response else None,
        "now": datetime.now(timezone.utc),
    })


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


@router.post("/templates/quick", response_class=JSONResponse)
async def create_template_quick(
    body: dict = Body(...),
    staff: dict = Depends(require_staff),
    conn: asyncpg.Connection = Depends(get_conn),
):
    """Create a hidden template for one-time custom requests (AJAX)."""
    template_id = await conn.fetchval(
        """INSERT INTO portal.templates (tenant_id, name, description, is_active, created_by)
           VALUES ($1::uuid, $2, NULL, false, $3::uuid) RETURNING id""",
        staff["tenant_id"],
        body.get("name", "Custom request"),
        staff["staff_id"],
    )
    return {"id": str(template_id)}


@router.get("/templates/{template_id}/edit", response_class=HTMLResponse)
async def staff_template_edit(
    template_id: str,
    request: Request,
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    tmpl = await conn.fetchrow(
        "SELECT id, name, description, email_subject, email_body FROM portal.templates WHERE id = $1::uuid AND tenant_id = $2::uuid",
        template_id,
        staff["tenant_id"],
    )
    if not tmpl:
        return RedirectResponse(url="/portal/staff/templates", status_code=303)

    items = await conn.fetch(
        """SELECT ti.id, ti.title, ti.instructions, ti.required, ti.sort_order,
                  ti.item_type::text AS item_type, ti.file_key,
                  ti.sig_page, ti.sig_x, ti.sig_y, ti.sig_w, ti.sig_h,
                  COALESCE(zc.zone_count, 0)::int AS zone_count
           FROM portal.template_items ti
           LEFT JOIN (
               SELECT template_item_id, COUNT(*) AS zone_count
               FROM portal.template_item_zones
               GROUP BY template_item_id
           ) zc ON zc.template_item_id = ti.id
           WHERE ti.template_id = $1::uuid
           ORDER BY ti.sort_order""",
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
    email_subject: str = Form(default=""),
    email_body: str = Form(default=""),
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    await conn.execute(
        "UPDATE portal.templates SET name = $1, description = $2, email_subject = $3, email_body = $4 WHERE id = $5::uuid AND tenant_id = $6::uuid",
        name.strip(),
        description.strip() or None,
        email_subject.strip() or None,
        email_body.strip() or None,
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
        SELECT id, title, instructions, required, sort_order,
               item_type::text AS item_type, file_key,
               sig_page, sig_x, sig_y, sig_w, sig_h
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

    item_type = body.get("item_type", "file_upload")
    if item_type not in ("file_upload", "signature"):
        return JSONResponse({"error": "invalid item_type"}, status_code=400)

    file_key = None
    if item_type == "signature":
        file_key = (body.get("file_key") or "").strip() or None

    # Next sort_order
    max_order = await conn.fetchval(
        "SELECT COALESCE(MAX(sort_order), -1) FROM portal.template_items WHERE template_id = $1::uuid",
        template_id,
    )
    sig_page = body.get("sig_page")
    sig_x = body.get("sig_x")
    sig_y = body.get("sig_y")
    sig_w = body.get("sig_w")
    sig_h = body.get("sig_h")

    item_id = await conn.fetchval(
        """
        INSERT INTO portal.template_items
            (template_id, tenant_id, item_type, title, instructions, required, sort_order, file_key,
             sig_page, sig_x, sig_y, sig_w, sig_h)
        VALUES ($1::uuid, $2::uuid, $3::public.template_item_type, $4, $5, $6, $7, $8,
                $9, $10, $11, $12, $13)
        RETURNING id
        """,
        template_id,
        staff["tenant_id"],
        item_type,
        title,
        (body.get("instructions") or "").strip() or None,
        bool(body.get("required", True)),
        max_order + 1,
        file_key,
        sig_page, sig_x, sig_y, sig_w, sig_h,
    )
    return {
        "id": str(item_id),
        "title": title,
        "item_type": item_type,
        "instructions": body.get("instructions"),
        "file_key": file_key,
        "required": bool(body.get("required", True)),
        "sort_order": max_order + 1,
        "sig_page": sig_page,
        "sig_x": sig_x,
        "sig_y": sig_y,
        "sig_w": sig_w,
        "sig_h": sig_h,
    }


@router.post("/templates/{template_id}/items/{item_id}")
async def update_template_item(
    template_id: str,
    item_id: str,
    body: dict = Body(...),
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    item_type = body.get("item_type", "file_upload")
    if item_type not in ("file_upload", "signature"):
        item_type = "file_upload"
    file_key = (body.get("file_key") or "").strip() or None
    sig_page = body.get("sig_page")
    sig_x = body.get("sig_x")
    sig_y = body.get("sig_y")
    sig_w = body.get("sig_w")
    sig_h = body.get("sig_h")

    await conn.execute(
        """
        UPDATE portal.template_items
        SET title = $1, instructions = $2, required = $3,
            item_type = $4::public.template_item_type, file_key = $5,
            sig_page = $6, sig_x = $7, sig_y = $8, sig_w = $9, sig_h = $10
        WHERE id = $11::uuid AND template_id = $12::uuid AND tenant_id = $13::uuid
        """,
        (body.get("title") or "").strip(),
        (body.get("instructions") or "").strip() or None,
        bool(body.get("required", True)),
        item_type,
        file_key,
        sig_page, sig_x, sig_y, sig_w, sig_h,
        item_id,
        template_id,
        staff["tenant_id"],
    )
    return {"ok": True}


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


@router.post("/templates/{template_id}/upload-doc")
async def template_upload_doc(
    template_id: str,
    file: UploadFile = File(...),
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    """Upload a document-to-sign directly through the server (avoids CORS)."""
    filename = (file.filename or "document.pdf").strip()
    content_type = file.content_type or "application/pdf"
    data = await file.read()
    if not data:
        return JSONResponse({"error": "Empty file"}, status_code=400)

    from .storage import upload_object
    object_key = f"templates/{staff['tenant_id']}/{template_id}/{filename}"
    upload_object(object_key, data, content_type)
    return {"file_key": object_key}


@router.get("/templates/{template_id}/items/{item_id}/pdf-bytes")
async def template_pdf_bytes(
    template_id: str,
    item_id: str,
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    """Stream the uploaded PDF through the server for PDF.js rendering."""
    item = await conn.fetchrow(
        "SELECT file_key FROM portal.template_items WHERE id=$1::uuid AND template_id=$2::uuid AND tenant_id=$3::uuid",
        item_id, template_id, staff["tenant_id"],
    )
    if not item or not item["file_key"]:
        return JSONResponse({"error": "No document"}, status_code=404)
    from .storage import download_object
    from fastapi.responses import Response
    pdf_data = download_object(item["file_key"])
    return Response(content=pdf_data, media_type="application/pdf")


# ---------------------------------------------------------------------------
# Template item zones (multi-zone builder)
# ---------------------------------------------------------------------------

_VALID_ZONE_TYPES = ("signature", "text", "date")


@router.get("/templates/{template_id}/items/{item_id}/zones")
async def list_zones(
    template_id: str,
    item_id: str,
    staff: dict = Depends(require_staff),
    conn: asyncpg.Connection = Depends(get_conn),
):
    # Verify item belongs to this template + tenant
    item = await conn.fetchrow(
        "SELECT id FROM portal.template_items WHERE id=$1::uuid AND template_id=$2::uuid AND tenant_id=$3::uuid",
        item_id, template_id, staff["tenant_id"],
    )
    if not item:
        return JSONResponse({"error": "Item not found"}, status_code=404)

    rows = await conn.fetch(
        """SELECT id, zone_type::text AS zone_type, label, page, x, y, w, h,
                  sort_order, required
           FROM portal.template_item_zones
           WHERE template_item_id = $1::uuid AND tenant_id = $2::uuid
           ORDER BY sort_order ASC, created_at ASC""",
        item_id, staff["tenant_id"],
    )
    return {"zones": [dict(r) for r in rows]}


@router.post("/templates/{template_id}/items/{item_id}/zones")
async def create_zone(
    template_id: str,
    item_id: str,
    body: dict = Body(...),
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    # Verify item
    item = await conn.fetchrow(
        "SELECT id FROM portal.template_items WHERE id=$1::uuid AND template_id=$2::uuid AND tenant_id=$3::uuid",
        item_id, template_id, staff["tenant_id"],
    )
    if not item:
        return JSONResponse({"error": "Item not found"}, status_code=404)

    zone_type = body.get("zone_type", "")
    if zone_type not in _VALID_ZONE_TYPES:
        return JSONResponse({"error": f"zone_type must be one of {_VALID_ZONE_TYPES}"}, status_code=400)

    label = (body.get("label") or "").strip()
    if not label:
        return JSONResponse({"error": "label required"}, status_code=400)

    page = int(body.get("page", 0))
    x = float(body.get("x", 0))
    y = float(body.get("y", 0))
    w = float(body.get("w", 20))
    h = float(body.get("h", 5))
    required = bool(body.get("required", True))

    max_order = await conn.fetchval(
        "SELECT COALESCE(MAX(sort_order), -1) FROM portal.template_item_zones WHERE template_item_id = $1::uuid",
        item_id,
    )

    zone_id = await conn.fetchval(
        """INSERT INTO portal.template_item_zones
               (template_item_id, tenant_id, zone_type, label, page, x, y, w, h, sort_order, required)
           VALUES ($1::uuid, $2::uuid, $3::public.zone_type, $4, $5, $6, $7, $8, $9, $10, $11)
           RETURNING id""",
        item_id, staff["tenant_id"], zone_type, label, page, x, y, w, h, max_order + 1, required,
    )
    return {
        "id": str(zone_id), "zone_type": zone_type, "label": label,
        "page": page, "x": x, "y": y, "w": w, "h": h,
        "sort_order": max_order + 1, "required": required,
    }


@router.put("/templates/{template_id}/items/{item_id}/zones/{zone_id}")
async def update_zone(
    template_id: str,
    item_id: str,
    zone_id: str,
    body: dict = Body(...),
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    zone_type = body.get("zone_type", "")
    if zone_type not in _VALID_ZONE_TYPES:
        return JSONResponse({"error": f"zone_type must be one of {_VALID_ZONE_TYPES}"}, status_code=400)

    await conn.execute(
        """UPDATE portal.template_item_zones
           SET zone_type = $1::public.zone_type, label = $2, page = $3,
               x = $4, y = $5, w = $6, h = $7, required = $8
           WHERE id = $9::uuid AND template_item_id = $10::uuid AND tenant_id = $11::uuid""",
        zone_type,
        (body.get("label") or "").strip(),
        int(body.get("page", 0)),
        float(body.get("x", 0)),
        float(body.get("y", 0)),
        float(body.get("w", 20)),
        float(body.get("h", 5)),
        bool(body.get("required", True)),
        zone_id, item_id, staff["tenant_id"],
    )
    return {"ok": True}


@router.delete("/templates/{template_id}/items/{item_id}/zones/{zone_id}")
async def delete_zone(
    template_id: str,
    item_id: str,
    zone_id: str,
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    await conn.execute(
        "DELETE FROM portal.template_item_zones WHERE id=$1::uuid AND template_item_id=$2::uuid AND tenant_id=$3::uuid",
        zone_id, item_id, staff["tenant_id"],
    )
    return {"ok": True}


# ---------------------------------------------------------------------------
# Settings — branding (admin only)
# ---------------------------------------------------------------------------

@router.get("/settings", response_class=HTMLResponse)
async def staff_settings(
    request: Request,
    tab: Optional[str] = "branding",
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    brand = await get_tenant_brand(conn, staff["tenant_id"])
    users = []
    email_config = None
    if tab == "users":
        rows = await conn.fetch(
            """SELECT id, email, full_name, role, is_active, last_login_at, created_at
               FROM portal.staff_users WHERE tenant_id = $1::uuid ORDER BY created_at ASC""",
            staff["tenant_id"],
        )
        users = [dict(r) for r in rows]
    elif tab == "email":
        row = await conn.fetchrow(
            "SELECT sending_domain, sending_from_email, domain_verified FROM portal.tenants WHERE id = $1::uuid",
            staff["tenant_id"],
        )
        email_config = dict(row) if row else {}
    return templates.TemplateResponse("staff_settings.html", {
        "request": request,
        "staff": staff,
        "brand": brand,
        "saved": request.query_params.get("saved"),
        "tab": tab or "branding",
        "users": users,
        "email_config": email_config,
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


@router.post("/settings/email")
async def update_email_settings(
    sending_domain: str = Form(default=""),
    sending_from_email: str = Form(default=""),
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    domain = sending_domain.strip().lower()
    from_email = sending_from_email.strip().lower()

    dkim_records = []
    if domain:
        dkim_records = verify_domain(domain)

    await conn.execute(
        """
        UPDATE portal.tenants
        SET sending_domain = $1, sending_from_email = $2, domain_verified = false
        WHERE id = $3::uuid
        """,
        domain or None,
        from_email or None,
        staff["tenant_id"],
    )

    if dkim_records:
        import json
        return RedirectResponse(
            url=f"/portal/staff/settings?tab=email&saved=1&dns={json.dumps(dkim_records)}",
            status_code=303,
        )
    return RedirectResponse(url="/portal/staff/settings?tab=email&saved=1", status_code=303)


@router.post("/settings/email/check-verification")
async def check_email_verification_route(
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    tenant = await conn.fetchrow(
        "SELECT sending_domain FROM portal.tenants WHERE id = $1::uuid",
        staff["tenant_id"],
    )
    if not tenant or not tenant["sending_domain"]:
        return JSONResponse({"error": "No domain configured"}, status_code=400)

    verified = check_domain_verification(tenant["sending_domain"])
    if verified:
        await conn.execute(
            "UPDATE portal.tenants SET domain_verified = true WHERE id = $1::uuid",
            staff["tenant_id"],
        )
    return {"verified": verified}


# ---------------------------------------------------------------------------
# User management (admin only)
# ---------------------------------------------------------------------------

@router.get("/users", response_class=HTMLResponse)
async def staff_users_list(
    request: Request,
    staff: dict = Depends(require_admin),
):
    return RedirectResponse(url="/portal/staff/settings?tab=users", status_code=303)


@router.get("/users/new", response_class=HTMLResponse)
async def staff_user_new_form(
    request: Request,
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    brand = await get_tenant_brand(conn, staff["tenant_id"])
    return templates.TemplateResponse("staff_user_edit.html", {
        "request": request,
        "user": None,
        "staff": staff,
        "brand": brand,
        "error": None,
    })


@router.post("/users")
async def create_user(
    request: Request,
    full_name: str = Form(...),
    email: str = Form(...),
    role: str = Form(...),
    password: str = Form(...),
    confirm_password: str = Form(...),
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    email = email.lower().strip()
    error = None

    if role not in ("admin", "staff"):
        error = "Role must be admin or staff."
    elif not password.strip():
        error = "Password is required."
    elif password != confirm_password:
        error = "Passwords do not match."
    else:
        existing = await conn.fetchval(
            "SELECT id FROM portal.staff_users WHERE tenant_id = $1::uuid AND email = $2",
            staff["tenant_id"],
            email,
        )
        if existing:
            error = "A user with this email already exists."

    if error:
        brand = await get_tenant_brand(conn, staff["tenant_id"])
        return templates.TemplateResponse("staff_user_edit.html", {
            "request": request,
            "user": None,
            "staff": staff,
            "brand": brand,
            "error": error,
        })

    await conn.execute(
        """
        INSERT INTO portal.staff_users (tenant_id, email, full_name, role, password_hash, is_active)
        VALUES ($1::uuid, $2, $3, $4, $5, true)
        """,
        staff["tenant_id"],
        email,
        full_name.strip(),
        role,
        hash_password(password),
    )
    return RedirectResponse(url="/portal/staff/settings?tab=users", status_code=303)


@router.get("/users/{user_id}/edit", response_class=HTMLResponse)
async def staff_user_edit_form(
    user_id: str,
    request: Request,
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    user = await conn.fetchrow(
        "SELECT id, email, full_name, role, is_active FROM portal.staff_users WHERE id = $1::uuid AND tenant_id = $2::uuid",
        user_id,
        staff["tenant_id"],
    )
    if not user:
        return RedirectResponse(url="/portal/staff/settings?tab=users", status_code=303)

    brand = await get_tenant_brand(conn, staff["tenant_id"])
    return templates.TemplateResponse("staff_user_edit.html", {
        "request": request,
        "user": dict(user),
        "staff": staff,
        "brand": brand,
        "error": None,
    })


@router.post("/users/{user_id}")
async def update_user(
    user_id: str,
    request: Request,
    full_name: str = Form(...),
    role: str = Form(...),
    is_active: Optional[str] = Form(default=None),
    password: str = Form(default=""),
    confirm_password: str = Form(default=""),
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    active = is_active == "on"
    error = None

    if role not in ("admin", "staff"):
        error = "Role must be admin or staff."
    elif password and password != confirm_password:
        error = "Passwords do not match."

    # Prevent deactivating yourself
    if user_id == staff["staff_id"] and not active:
        error = "You cannot deactivate your own account."

    # Prevent demoting yourself if you're the last admin
    if user_id == staff["staff_id"] and role != "admin":
        admin_count = await conn.fetchval(
            "SELECT COUNT(*) FROM portal.staff_users WHERE tenant_id = $1::uuid AND role = 'admin' AND is_active = true",
            staff["tenant_id"],
        )
        if admin_count <= 1:
            error = "Cannot demote — you are the only admin."

    if error:
        user = await conn.fetchrow(
            "SELECT id, email, full_name, role, is_active FROM portal.staff_users WHERE id = $1::uuid AND tenant_id = $2::uuid",
            user_id,
            staff["tenant_id"],
        )
        brand = await get_tenant_brand(conn, staff["tenant_id"])
        return templates.TemplateResponse("staff_user_edit.html", {
            "request": request,
            "user": dict(user) if user else None,
            "staff": staff,
            "brand": brand,
            "error": error,
        })

    await conn.execute(
        """
        UPDATE portal.staff_users
        SET full_name = $1, role = $2, is_active = $3
        WHERE id = $4::uuid AND tenant_id = $5::uuid
        """,
        full_name.strip(),
        role,
        active,
        user_id,
        staff["tenant_id"],
    )

    if password.strip():
        await conn.execute(
            "UPDATE portal.staff_users SET password_hash = $1 WHERE id = $2::uuid",
            hash_password(password),
            user_id,
        )

    return RedirectResponse(url="/portal/staff/settings?tab=users", status_code=303)


# ---------------------------------------------------------------------------
# Change own password — available to all staff
# ---------------------------------------------------------------------------

@router.get("/change-password", response_class=HTMLResponse)
async def change_password_form(
    request: Request,
    staff: dict = Depends(require_staff),
    conn: asyncpg.Connection = Depends(get_conn),
):
    brand = await get_tenant_brand(conn, staff["tenant_id"])
    return templates.TemplateResponse("staff_change_password.html", {
        "request": request, "staff": staff, "brand": brand,
    })


@router.post("/change-password", response_class=HTMLResponse)
async def change_password(
    request: Request,
    current_password: str = Form(...),
    new_password: str = Form(...),
    confirm_password: str = Form(...),
    staff: dict = Depends(require_staff),
    conn: asyncpg.Connection = Depends(get_conn),
):
    brand = await get_tenant_brand(conn, staff["tenant_id"])
    ctx = {"request": request, "staff": staff, "brand": brand}

    row = await conn.fetchrow(
        "SELECT password_hash FROM portal.staff_users WHERE id = $1::uuid",
        staff["staff_id"],
    )
    if not row or not verify_password(current_password, row["password_hash"]):
        ctx["error"] = "Current password is incorrect."
        return templates.TemplateResponse("staff_change_password.html", ctx)

    if new_password != confirm_password:
        ctx["error"] = "New passwords do not match."
        return templates.TemplateResponse("staff_change_password.html", ctx)

    if len(new_password) < 8:
        ctx["error"] = "Password must be at least 8 characters."
        return templates.TemplateResponse("staff_change_password.html", ctx)

    await conn.execute(
        "UPDATE portal.staff_users SET password_hash = $1 WHERE id = $2::uuid",
        hash_password(new_password),
        staff["staff_id"],
    )
    await log_audit(conn, staff["tenant_id"], "password_changed", "staff", actor_id=staff["staff_id"])

    ctx["success"] = "Password updated successfully."
    return templates.TemplateResponse("staff_change_password.html", ctx)


# ---------------------------------------------------------------------------
# New request form + create — MUST come before /requests/{request_id}
# ---------------------------------------------------------------------------

@router.get("/requests/new", response_class=HTMLResponse)
async def staff_request_new_form(
    request: Request,
    staff: dict = Depends(require_staff),
    conn: asyncpg.Connection = Depends(get_conn),
):
    # Clean up orphaned hidden templates from abandoned custom requests
    orphan_ids = [
        r["id"]
        for r in await conn.fetch(
            """SELECT id FROM portal.templates
               WHERE created_by = $1::uuid AND is_active = false
               AND id NOT IN (SELECT template_id FROM portal.doc_requests WHERE template_id IS NOT NULL)""",
            staff["staff_id"],
        )
    ]
    if orphan_ids:
        await conn.execute(
            "DELETE FROM portal.template_item_zones WHERE template_item_id IN (SELECT id FROM portal.template_items WHERE template_id = ANY($1::uuid[]))",
            orphan_ids,
        )
        await conn.execute(
            "DELETE FROM portal.template_items WHERE template_id = ANY($1::uuid[])",
            orphan_ids,
        )
        await conn.execute(
            "DELETE FROM portal.templates WHERE id = ANY($1::uuid[])",
            orphan_ids,
        )

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
    first_name: str = Form(...),
    last_name: str = Form(...),
    email: str = Form(...),
    phone: str = Form(default=""),
    due_at: str = Form(default=""),
    template_id: str = Form(default=""),
    manual_items: str = Form(default=""),
    staff: dict = Depends(require_staff),
    conn: asyncpg.Connection = Depends(get_conn),
):
    full_name = f"{first_name.strip()} {last_name.strip()}"
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
            INSERT INTO portal.doc_requests (tenant_id, client_id, due_at, status, created_by_staff_id)
            VALUES ($1::uuid, $2::uuid, $3, 'draft', $4::uuid)
            RETURNING id
            """,
            staff["tenant_id"],
            client_id,
            due_val,
            staff["staff_id"],
        )

        # Store template reference for email customisation
        if template_id.strip():
            await conn.execute(
                "UPDATE portal.doc_requests SET template_id = $1::uuid WHERE id = $2::uuid",
                template_id, request_id,
            )

        # Clone items from template
        if template_id.strip():
            items = await conn.fetch(
                "SELECT * FROM portal.template_items WHERE template_id = $1::uuid ORDER BY sort_order",
                template_id,
            )
            for item in items:
                req_item_id = await conn.fetchval(
                    """
                    INSERT INTO portal.doc_request_items
                        (tenant_id, request_id, item_type, title, instructions, required, sort_order, file_key,
                         sig_page, sig_x, sig_y, sig_w, sig_h)
                    VALUES ($1::uuid, $2::uuid, $3::public.template_item_type, $4, $5, $6, $7, $8,
                            $9, $10, $11, $12, $13)
                    RETURNING id
                    """,
                    staff["tenant_id"],
                    request_id,
                    item["item_type"],
                    item["title"],
                    item["instructions"],
                    item["required"],
                    item["sort_order"],
                    item.get("file_key"),
                    item.get("sig_page"),
                    item.get("sig_x"),
                    item.get("sig_y"),
                    item.get("sig_w"),
                    item.get("sig_h"),
                )
                # Clone zones from template item to request item
                zones = await conn.fetch(
                    "SELECT * FROM portal.template_item_zones WHERE template_item_id = $1::uuid",
                    item["id"],
                )
                for z in zones:
                    await conn.execute(
                        """INSERT INTO portal.request_item_zones
                               (request_item_id, tenant_id, zone_type, label, page,
                                x, y, w, h, sort_order, required)
                           VALUES ($1::uuid, $2::uuid, $3::public.zone_type, $4, $5,
                                   $6, $7, $8, $9, $10, $11)""",
                        req_item_id, staff["tenant_id"],
                        z["zone_type"], z["label"], z["page"],
                        z["x"], z["y"], z["w"], z["h"],
                        z["sort_order"], z["required"],
                    )

            # Delete hidden template after cloning (one-time custom requests)
            is_hidden = await conn.fetchval(
                "SELECT NOT is_active FROM portal.templates WHERE id = $1::uuid",
                template_id,
            )
            if is_hidden:
                await conn.execute(
                    "DELETE FROM portal.template_item_zones WHERE template_item_id IN (SELECT id FROM portal.template_items WHERE template_id = $1::uuid)",
                    template_id,
                )
                await conn.execute(
                    "DELETE FROM portal.template_items WHERE template_id = $1::uuid",
                    template_id,
                )
                await conn.execute(
                    "DELETE FROM portal.templates WHERE id = $1::uuid",
                    template_id,
                )
                # Clear template reference since the template no longer exists
                await conn.execute(
                    "UPDATE portal.doc_requests SET template_id = NULL WHERE id = $1::uuid",
                    request_id,
                )

        # Create manual items (if no template or in addition to template)
        if manual_items.strip():
            import json as _json
            try:
                items_data = _json.loads(manual_items)
            except _json.JSONDecodeError:
                items_data = []
            # Start sort_order after any template items
            max_order_row = await conn.fetchval(
                "SELECT COALESCE(MAX(sort_order), -1) FROM portal.doc_request_items WHERE request_id = $1::uuid",
                request_id,
            )
            sort_start = (max_order_row or -1) + 1
            for idx, item in enumerate(items_data):
                item_type = item.get("item_type", "file_upload")
                if item_type not in ("file_upload", "signature"):
                    item_type = "file_upload"
                title = (item.get("title") or "").strip()
                if not title:
                    continue
                await conn.execute(
                    """
                    INSERT INTO portal.doc_request_items
                        (tenant_id, request_id, item_type, title, instructions, required, sort_order)
                    VALUES ($1::uuid, $2::uuid, $3::public.template_item_type, $4, $5, $6, $7)
                    """,
                    staff["tenant_id"],
                    request_id,
                    item_type,
                    title,
                    (item.get("instructions") or "").strip() or None,
                    bool(item.get("required", True)),
                    sort_start + idx,
                )

    return RedirectResponse(
        url=f"/portal/staff/requests/{request_id}",
        status_code=303,
    )


# ---------------------------------------------------------------------------
# Bulk actions on requests (AJAX — returns JSON)
# ---------------------------------------------------------------------------

@router.post("/requests/bulk")
async def bulk_action(
    body: dict = Body(...),
    staff: dict = Depends(require_staff),
    conn: asyncpg.Connection = Depends(get_conn),
):
    action = body.get("action")
    request_ids = body.get("request_ids", [])

    if action not in ("complete", "close", "delete"):
        return JSONResponse({"error": "action must be 'complete', 'close', or 'delete'"}, status_code=400)
    if not request_ids or not isinstance(request_ids, list):
        return JSONResponse({"error": "request_ids required"}, status_code=400)

    if action == "delete":
        eligible = await conn.fetch(
            """
            SELECT id FROM portal.doc_requests
            WHERE id = ANY($1::uuid[])
              AND tenant_id = $2::uuid
            """,
            request_ids,
            staff["tenant_id"],
        )
        eligible_ids = [str(row["id"]) for row in eligible]
        if eligible_ids:
            for rid in eligible_ids:
                await conn.execute("DELETE FROM portal.files WHERE request_id = $1::uuid", rid)
                await conn.execute("DELETE FROM portal.doc_request_items WHERE request_id = $1::uuid", rid)
                await conn.execute(
                    "DELETE FROM portal.doc_requests WHERE id = $1::uuid AND tenant_id = $2::uuid",
                    rid, staff["tenant_id"],
                )
        return {"deleted": len(eligible_ids)}

    new_status = "completed" if action == "complete" else "closed"

    updated = await conn.fetch(
        """
        UPDATE portal.doc_requests
        SET status = $1::public.request_status
        WHERE id = ANY($2::uuid[])
          AND tenant_id = $3::uuid
          AND status NOT IN ('completed', 'closed')
        RETURNING id
        """,
        new_status,
        request_ids,
        staff["tenant_id"],
    )

    for row in updated:
        await log_audit(
            conn,
            tenant_id=staff["tenant_id"],
            event_type=f"request_{new_status}",
            actor="staff",
            actor_id=staff["staff_id"],
            request_id=str(row["id"]),
        )

    return {"updated": len(updated), "status": new_status}


# ---------------------------------------------------------------------------
# Request list (with filtering + search)
# ---------------------------------------------------------------------------

@router.get("/requests", response_class=HTMLResponse)
async def staff_requests(
    request: Request,
    q: Optional[str] = None,
    users: Optional[str] = None,
    filters: Optional[str] = None,
    staff: dict = Depends(require_staff),
    conn: asyncpg.Connection = Depends(get_conn),
):
    conditions = ["r.tenant_id = $1::uuid"]
    params: list = [staff["tenant_id"]]

    # Parse comma-separated filter tags
    active_filters = set()
    if filters:
        active_filters = {f.strip() for f in filters.split(",") if f.strip()}

    # User filter (comma-separated UUIDs)
    active_user_ids: list[str] = []
    if users:
        active_user_ids = [u.strip() for u in users.split(",") if u.strip()]
        placeholders = ", ".join(
            f"${len(params) + i + 1}::uuid" for i in range(len(active_user_ids))
        )
        params.extend(active_user_ids)
        conditions.append(f"r.created_by_staff_id IN ({placeholders})")

    # Status filters (can combine)
    status_parts = []
    if "open" in active_filters:
        status_parts.append("r.status NOT IN ('completed', 'closed')")
    if "closed" in active_filters:
        status_parts.append("r.status IN ('completed', 'closed')")
    if "review" in active_filters:
        status_parts.append(
            "EXISTS (SELECT 1 FROM portal.doc_request_items ri2 "
            "WHERE ri2.request_id = r.id AND ri2.status = 'uploaded')"
        )
    if "overdue" in active_filters:
        status_parts.append(
            "(r.due_at < now() AND r.status NOT IN ('completed', 'closed'))"
        )
    if status_parts:
        conditions.append(f"({' OR '.join(status_parts)})")

    if q and q.strip():
        params.append(f"%{q.strip()}%")
        conditions.append(
            f"(c.full_name ILIKE ${len(params)} OR c.email ILIKE ${len(params)})"
        )

    where = " AND ".join(conditions)

    rows = await conn.fetch(
        f"""
        SELECT r.id, r.status::text AS status, r.due_at, r.created_at,
               r.sent_at, r.last_viewed_at,
               c.full_name AS client_name, c.email AS client_email,
               su.full_name AS created_by_name,
               COUNT(ri.id) AS item_total,
               COUNT(CASE WHEN ri.status = 'approved' THEN 1 END) AS item_done,
               COUNT(CASE WHEN ri.status = 'uploaded' THEN 1 END) AS item_awaiting
        FROM portal.doc_requests r
        JOIN portal.clients c ON c.id = r.client_id
        LEFT JOIN portal.staff_users su ON su.id = r.created_by_staff_id
        LEFT JOIN portal.doc_request_items ri ON ri.request_id = r.id
        WHERE {where}
        GROUP BY r.id, r.status, r.due_at, r.created_at, r.sent_at,
                 r.last_viewed_at, c.full_name, c.email, su.full_name
        ORDER BY r.created_at DESC
        """,
        *params,
    )

    requests_out = []
    for r in rows:
        d = dict(r)
        d["last_viewed_rel"] = _relative_time(r["last_viewed_at"])
        requests_out.append(d)

    # Load staff users for the filter dropdown
    staff_users = await conn.fetch(
        "SELECT id::text, full_name FROM portal.staff_users "
        "WHERE tenant_id = $1::uuid AND is_active = true ORDER BY full_name",
        staff["tenant_id"],
    )

    brand = await get_tenant_brand(conn, staff["tenant_id"])
    return templates.TemplateResponse("staff_list.html", {
        "request": request,
        "requests": requests_out,
        "staff": staff,
        "brand": brand,
        "staff_users": [dict(u) for u in staff_users],
        "active_filters": active_filters,
        "active_user_ids": active_user_ids,
        "q": q or "",
        "now": datetime.now(timezone.utc),
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
               status::text AS status, sort_order, instructions,
               file_key, signature_file_key, signed_pdf_key
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

    # Fetch submitted zone values
    item_ids = [i["id"] for i in items]
    zones_by_item: dict = {}
    if item_ids:
        zones = await conn.fetch("""
            SELECT id, request_item_id, zone_type::text AS zone_type, label,
                   page, x, y, w, h, sort_order, required,
                   value, signature_file_key, filled_at
            FROM portal.request_item_zones
            WHERE request_item_id = ANY($1::uuid[])
            ORDER BY sort_order ASC
        """, item_ids)
        for z in zones:
            rid = str(z["request_item_id"])
            zd = dict(z)
            zd["id"] = str(zd["id"])
            zd["request_item_id"] = rid
            # Convert datetime for JSON serialization
            if zd.get("filled_at"):
                zd["filled_at"] = zd["filled_at"].isoformat()
            # Presign signature images
            if zd.get("signature_file_key"):
                try:
                    zd["signature_url"] = presign_get(zd["signature_file_key"])
                except Exception:
                    zd["signature_url"] = None
            zones_by_item.setdefault(rid, []).append(zd)

    # Check if all items are approved (for "ready to close" banner)
    all_approved = (
        len(items) > 0
        and all(i["status"] == "approved" for i in items)
    )

    # Presigned URLs for signature documents, client signatures, and signed PDFs
    signature_doc_urls: dict = {}
    signature_img_urls: dict = {}
    signed_pdf_urls: dict = {}
    for item in items:
        iid = str(item["id"])
        if item.get("file_key"):
            try:
                signature_doc_urls[iid] = presign_get(item["file_key"])
            except Exception:
                pass
        if item.get("signature_file_key"):
            try:
                signature_img_urls[iid] = presign_get(item["signature_file_key"])
            except Exception:
                pass
        if item.get("signed_pdf_key"):
            try:
                signed_pdf_urls[iid] = presign_get(item["signed_pdf_key"])
            except Exception:
                pass

    brand = await get_tenant_brand(conn, staff["tenant_id"])

    # Check if email is configured for this tenant
    email_cfg = await conn.fetchrow(
        "SELECT sending_from_email, domain_verified FROM portal.tenants WHERE id = $1::uuid",
        staff["tenant_id"],
    )
    email_enabled = bool(email_cfg and email_cfg["domain_verified"] and email_cfg["sending_from_email"])

    # Fetch audit timeline
    audit_events = await conn.fetch("""
        SELECT event_type, actor::text AS actor, metadata::text AS metadata,
               event_time AS created_at
        FROM portal.audit_events
        WHERE request_id = $1::uuid AND tenant_id = $2::uuid
        ORDER BY event_time DESC
        LIMIT 50
    """, request_id, staff["tenant_id"])

    return templates.TemplateResponse("staff_detail.html", {
        "request": request,
        "req": dict(req),
        "items": [dict(i) for i in items],
        "files_by_item": files_by_item,
        "signature_doc_urls": signature_doc_urls,
        "signature_img_urls": signature_img_urls,
        "signed_pdf_urls": signed_pdf_urls,
        "zones_by_item": zones_by_item,
        "staff": staff,
        "brand": brand,
        "all_approved": all_approved,
        "email_enabled": email_enabled,
        "client_email": req["client_email"] or "",
        "sending_from_email": (email_cfg["sending_from_email"] or "") if email_cfg else "",
        "audit_events": [dict(e) for e in audit_events],
    })


# ---------------------------------------------------------------------------
# Staff: stream request item PDF bytes (for PDF.js rendering)
# ---------------------------------------------------------------------------

@router.get("/requests/{request_id}/items/{item_id}/pdf-bytes")
async def staff_item_pdf_bytes(
    request_id: str,
    item_id: str,
    staff: dict = Depends(require_staff),
    conn: asyncpg.Connection = Depends(get_conn),
):
    item = await conn.fetchrow(
        """SELECT file_key FROM portal.doc_request_items
           WHERE id = $1::uuid AND request_id = $2::uuid""",
        item_id, request_id,
    )
    if not item or not item["file_key"]:
        raise HTTPException(status_code=404, detail="Document not found")

    from .storage import download_object
    from fastapi.responses import Response
    pdf_data = download_object(item["file_key"])
    return Response(content=pdf_data, media_type="application/pdf")


@router.get("/requests/{request_id}/items/{item_id}/download-completed")
async def staff_download_completed(
    request_id: str,
    item_id: str,
    staff: dict = Depends(require_staff),
    conn: asyncpg.Connection = Depends(get_conn),
):
    """Download the completed PDF with all filled zones burned in."""
    item = await conn.fetchrow(
        """SELECT file_key, title FROM portal.doc_request_items
           WHERE id = $1::uuid AND request_id = $2::uuid""",
        item_id, request_id,
    )
    if not item or not item["file_key"]:
        raise HTTPException(status_code=404, detail="Document not found")

    zones = await conn.fetch(
        """SELECT zone_type::text AS zone_type, page, x, y, w, h,
                  value, signature_file_key
           FROM portal.request_item_zones
           WHERE request_item_id = $1::uuid AND filled_at IS NOT NULL""",
        item_id,
    )

    from .storage import download_object
    from .pdf_merge import merge_zones_onto_pdf
    from fastapi.responses import Response

    pdf_data = download_object(item["file_key"])
    merged = merge_zones_onto_pdf(pdf_data, [dict(z) for z in zones], download_object)

    filename = (item["title"] or "document").replace('"', "") + " - completed.pdf"
    return Response(
        content=merged,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# Generate magic link (AJAX — returns JSON)
# ---------------------------------------------------------------------------

async def _create_access_token(
    conn: asyncpg.Connection, tenant_id: str, request_id: str, staff_id: str, expires_days: int = 30,
) -> tuple[str, datetime]:
    """Create a magic-link token for a request. Returns (raw_token, expires_at)."""
    raw_token = secrets.token_urlsafe(32)
    token_hash = _hash_token(raw_token)
    expires_at = _now_utc() + timedelta(days=expires_days)
    await conn.execute(
        """
        INSERT INTO portal.request_access_tokens
            (tenant_id, request_id, token_hash, expires_at)
        VALUES ($1::uuid, $2::uuid, $3, $4)
        """,
        tenant_id, request_id, token_hash, expires_at,
    )
    # Advance draft → sent on first link
    await conn.execute(
        """
        UPDATE portal.doc_requests
        SET status = 'sent'::public.request_status,
            sent_at = CASE WHEN sent_at IS NULL THEN now() ELSE sent_at END
        WHERE id = $1::uuid AND tenant_id = $2::uuid
          AND status = 'draft'::public.request_status
        """,
        request_id, tenant_id,
    )
    await log_audit(
        conn, tenant_id=tenant_id, event_type="access_link_created",
        actor="staff", actor_id=staff_id, request_id=request_id,
    )
    return raw_token, expires_at


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
            request_id, staff["tenant_id"],
        )
        if not req:
            return JSONResponse({"error": "Request not found"}, status_code=404)
        raw_token, expires_at = await _create_access_token(
            conn, staff["tenant_id"], request_id, staff["staff_id"], expires_days,
        )
    link = f"{settings.portal_base_url}/portal/r/{raw_token}/view"
    return {"link": link, "expires_at": expires_at.isoformat()}


@router.post("/requests/{request_id}/send-email")
async def send_email_to_client(
    request_id: str,
    staff: dict = Depends(require_staff),
    conn: asyncpg.Connection = Depends(get_conn),
):
    """Generate a magic link and email it to the client."""
    async with conn.transaction():
        row = await conn.fetchrow(
            """
            SELECT r.id, r.due_at, r.template_id,
                   c.full_name AS client_name, c.email AS client_email,
                   t.sending_from_email, t.domain_verified,
                   t.brand_name, t.brand_color, t.logo_url
            FROM portal.doc_requests r
            JOIN portal.clients c ON c.id = r.client_id
            JOIN portal.tenants t ON t.id = r.tenant_id
            WHERE r.id = $1::uuid AND r.tenant_id = $2::uuid
            """,
            request_id, staff["tenant_id"],
        )
        if not row:
            return JSONResponse({"error": "Request not found"}, status_code=404)
        if not row["domain_verified"] or not row["sending_from_email"]:
            return JSONResponse(
                {"error": "Email not configured. Go to Settings → Email to set up your sending domain."},
                status_code=400,
            )

        raw_token, expires_at = await _create_access_token(
            conn, staff["tenant_id"], request_id, staff["staff_id"],
        )

    magic_link = f"{settings.portal_base_url}/portal/r/{raw_token}/view"

    # Look up template email customisation
    custom_subject = None
    custom_body = None
    if row["template_id"]:
        tmpl = await conn.fetchrow(
            "SELECT email_subject, email_body FROM portal.templates WHERE id = $1::uuid",
            row["template_id"],
        )
        if tmpl:
            custom_subject = tmpl["email_subject"]
            custom_body = tmpl["email_body"]

    brand_name = row["brand_name"] or "HumTech"
    client_name = (row["client_name"] or "").split()[0] if row["client_name"] else "there"
    due_str = row["due_at"].strftime("%d %b %Y") if row["due_at"] else None

    subject = render_subject(
        custom_subject=custom_subject,
        brand_name=brand_name,
        client_name=row["client_name"] or "",
    )
    html_body = render_email(
        client_name=client_name,
        magic_link=magic_link,
        due_date=due_str,
        body_text=custom_body or DEFAULT_BODY,
        brand_name=brand_name,
        brand_color=row["brand_color"] or "#111827",
        logo_url=row["logo_url"],
    )

    ses_message_id = send_email(
        to_email=row["client_email"],
        from_email=row["sending_from_email"],
        subject=subject,
        html_body=html_body,
    )

    # Log the send
    await conn.execute(
        """
        INSERT INTO portal.email_sends
            (tenant_id, request_id, recipient_email, from_email, subject, ses_message_id, html_body, email_type)
        VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, 'magic_link')
        """,
        staff["tenant_id"], request_id,
        row["client_email"], row["sending_from_email"],
        subject, ses_message_id, html_body,
    )
    await log_audit(
        conn, tenant_id=staff["tenant_id"], event_type="magic_link_emailed",
        actor="staff", actor_id=staff["staff_id"], request_id=request_id,
    )

    return {"success": True, "email": row["client_email"]}


# ---------------------------------------------------------------------------
# Email log + compose (AJAX)
# ---------------------------------------------------------------------------


@router.get("/requests/{request_id}/emails")
async def list_request_emails(
    request_id: str,
    staff: dict = Depends(require_staff),
    conn: asyncpg.Connection = Depends(get_conn),
):
    """Return email history for a request as JSON."""
    rows = await conn.fetch(
        """
        SELECT id, email_type, recipient_email, from_email, subject,
               html_body, ses_message_id, sent_at
        FROM portal.email_sends
        WHERE request_id = $1::uuid AND tenant_id = $2::uuid
        ORDER BY sent_at DESC
        """,
        request_id, staff["tenant_id"],
    )
    return [
        {
            "id": str(r["id"]),
            "email_type": r["email_type"],
            "recipient_email": r["recipient_email"],
            "from_email": r["from_email"],
            "subject": r["subject"],
            "html_body": r["html_body"],
            "sent_at": r["sent_at"].isoformat() if r["sent_at"] else None,
        }
        for r in rows
    ]


@router.post("/requests/{request_id}/compose-email")
async def compose_email(
    request_id: str,
    body: dict = Body(...),
    staff: dict = Depends(require_staff),
    conn: asyncpg.Connection = Depends(get_conn),
):
    """Compose and send a custom email to the client for this request."""
    subject = (body.get("subject") or "").strip()
    body_text = (body.get("body_text") or "").strip()
    include_magic_link = body.get("include_magic_link", True)

    if not subject or not body_text:
        return JSONResponse({"error": "Subject and body are required."}, status_code=400)

    row = await conn.fetchrow(
        """
        SELECT r.id, r.due_at,
               c.full_name AS client_name, c.email AS client_email,
               t.sending_from_email, t.domain_verified,
               t.brand_name, t.brand_color, t.logo_url
        FROM portal.doc_requests r
        JOIN portal.clients c ON c.id = r.client_id
        JOIN portal.tenants t ON t.id = r.tenant_id
        WHERE r.id = $1::uuid AND r.tenant_id = $2::uuid
        """,
        request_id, staff["tenant_id"],
    )
    if not row:
        return JSONResponse({"error": "Request not found"}, status_code=404)
    if not row["domain_verified"] or not row["sending_from_email"]:
        return JSONResponse(
            {"error": "Email not configured. Go to Settings → Email to set up your sending domain."},
            status_code=400,
        )

    magic_link = ""
    if include_magic_link:
        async with conn.transaction():
            raw_token, _ = await _create_access_token(
                conn, staff["tenant_id"], request_id, staff["staff_id"],
            )
        magic_link = f"{settings.portal_base_url}/portal/r/{raw_token}/view"

    brand_name = row["brand_name"] or "HumTech"
    client_name = (row["client_name"] or "").split()[0] if row["client_name"] else "there"
    due_str = row["due_at"].strftime("%d %b %Y") if row["due_at"] else None

    html_body = render_email(
        client_name=client_name,
        magic_link=magic_link,
        due_date=due_str,
        body_text=body_text,
        brand_name=brand_name,
        brand_color=row["brand_color"] or "#111827",
        logo_url=row["logo_url"],
    )

    ses_message_id = send_email(
        to_email=row["client_email"],
        from_email=row["sending_from_email"],
        subject=subject,
        html_body=html_body,
    )

    await conn.execute(
        """
        INSERT INTO portal.email_sends
            (tenant_id, request_id, recipient_email, from_email, subject, ses_message_id, html_body, email_type)
        VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, 'custom')
        """,
        staff["tenant_id"], request_id,
        row["client_email"], row["sending_from_email"],
        subject, ses_message_id, html_body,
    )
    await log_audit(
        conn, tenant_id=staff["tenant_id"], event_type="custom_email_sent",
        actor="staff", actor_id=staff["staff_id"], request_id=request_id,
    )

    return {"success": True, "email": row["client_email"]}


# ---------------------------------------------------------------------------
# Close / complete request (AJAX — returns JSON)
# ---------------------------------------------------------------------------

@router.post("/requests/{request_id}/close")
async def close_request(
    request_id: str,
    body: dict = Body(...),
    staff: dict = Depends(require_staff),
    conn: asyncpg.Connection = Depends(get_conn),
):
    action = body.get("action")
    if action not in ("complete", "close"):
        return JSONResponse(
            {"error": "action must be 'complete' or 'close'"}, status_code=400
        )

    new_status = "completed" if action == "complete" else "closed"

    result = await conn.fetchval(
        """
        UPDATE portal.doc_requests
        SET status = $1::public.request_status
        WHERE id = $2::uuid AND tenant_id = $3::uuid
          AND status NOT IN ('completed', 'closed')
        RETURNING id
        """,
        new_status,
        request_id,
        staff["tenant_id"],
    )

    if not result:
        return JSONResponse(
            {"error": "Request not found or already closed"}, status_code=404
        )

    await log_audit(
        conn,
        tenant_id=staff["tenant_id"],
        event_type=f"request_{new_status}",
        actor="staff",
        actor_id=staff["staff_id"],
        request_id=request_id,
    )

    return {"request_id": request_id, "status": new_status}


@router.post("/requests/{request_id}/reopen")
async def reopen_request(
    request_id: str,
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    # Reopen to 'viewed' if it was previously viewed, else 'draft'
    result = await conn.fetchrow(
        """
        UPDATE portal.doc_requests
        SET status = CASE
            WHEN last_viewed_at IS NOT NULL THEN 'viewed'::public.request_status
            WHEN sent_at IS NOT NULL THEN 'sent'::public.request_status
            ELSE 'draft'::public.request_status
        END
        WHERE id = $1::uuid AND tenant_id = $2::uuid
          AND status IN ('completed', 'closed')
        RETURNING id, status::text AS status
        """,
        request_id,
        staff["tenant_id"],
    )

    if not result:
        return JSONResponse(
            {"error": "Request not found or not closed"}, status_code=404
        )

    await log_audit(
        conn,
        tenant_id=staff["tenant_id"],
        event_type="request_reopened",
        actor="staff",
        actor_id=staff["staff_id"],
        request_id=request_id,
    )

    return {"request_id": request_id, "status": result["status"]}


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


# ---------------------------------------------------------------------------
# Delete: Templates
# ---------------------------------------------------------------------------

@router.delete("/templates/{template_id}")
async def delete_template(
    template_id: str,
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    """Delete a template and all its items. Admin only."""
    tpl = await conn.fetchrow(
        "SELECT id FROM portal.templates WHERE id = $1::uuid AND tenant_id = $2::uuid",
        template_id, staff["tenant_id"],
    )
    if not tpl:
        return JSONResponse({"error": "Template not found"}, status_code=404)

    await conn.execute(
        "DELETE FROM portal.template_items WHERE template_id = $1::uuid", template_id
    )
    await conn.execute(
        "DELETE FROM portal.templates WHERE id = $1::uuid AND tenant_id = $2::uuid",
        template_id, staff["tenant_id"],
    )
    return {"ok": True}


# ---------------------------------------------------------------------------
# Reset password: Staff Users
# ---------------------------------------------------------------------------

@router.post("/users/{user_id}/reset-password")
async def reset_user_password(
    user_id: str,
    body: dict = Body(...),
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    """Admin resets another user's password."""
    password = body.get("password", "").strip()
    if len(password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters")

    user = await conn.fetchrow(
        "SELECT id FROM portal.staff_users WHERE id = $1::uuid AND tenant_id = $2::uuid",
        user_id, staff["tenant_id"],
    )
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    await conn.execute(
        "UPDATE portal.staff_users SET password_hash = $1 WHERE id = $2::uuid",
        hash_password(password), user_id,
    )
    await log_audit(
        conn,
        tenant_id=staff["tenant_id"],
        event_type="password_reset",
        actor="staff",
        actor_id=staff["staff_id"],
        metadata={"target_user_id": user_id, "reset_by": staff["full_name"]},
    )
    return {"ok": True}


# ---------------------------------------------------------------------------
# Delete: Staff Users
# ---------------------------------------------------------------------------

@router.delete("/users/{user_id}")
async def delete_user(
    user_id: str,
    staff: dict = Depends(require_admin),
    conn: asyncpg.Connection = Depends(get_conn),
):
    """Delete a staff user. Cannot delete self or last admin."""
    if user_id == staff["staff_id"]:
        return JSONResponse({"error": "Cannot delete your own account"}, status_code=400)

    user = await conn.fetchrow(
        "SELECT id, role FROM portal.staff_users WHERE id = $1::uuid AND tenant_id = $2::uuid",
        user_id, staff["tenant_id"],
    )
    if not user:
        return JSONResponse({"error": "User not found"}, status_code=404)

    if user["role"] == "admin":
        admin_count = await conn.fetchval(
            "SELECT COUNT(*) FROM portal.staff_users WHERE tenant_id = $1::uuid AND role = 'admin' AND is_active = true",
            staff["tenant_id"],
        )
        if admin_count <= 1:
            return JSONResponse({"error": "Cannot delete the only admin"}, status_code=400)

    await conn.execute(
        "DELETE FROM portal.staff_users WHERE id = $1::uuid AND tenant_id = $2::uuid",
        user_id, staff["tenant_id"],
    )
    return {"ok": True}


# ---------------------------------------------------------------------------
# Delete: Requests
# ---------------------------------------------------------------------------

@router.delete("/requests/{request_id}")
async def delete_request(
    request_id: str,
    staff: dict = Depends(require_staff),
    conn: asyncpg.Connection = Depends(get_conn),
):
    """Delete a request, its items, and files. Only draft/closed."""
    req = await conn.fetchrow(
        "SELECT id, status FROM portal.doc_requests WHERE id = $1::uuid AND tenant_id = $2::uuid",
        request_id, staff["tenant_id"],
    )
    if not req:
        return JSONResponse({"error": "Request not found"}, status_code=404)

    await conn.execute("DELETE FROM portal.files WHERE request_id = $1::uuid", request_id)
    await conn.execute("DELETE FROM portal.doc_request_items WHERE request_id = $1::uuid", request_id)
    await conn.execute(
        "DELETE FROM portal.doc_requests WHERE id = $1::uuid AND tenant_id = $2::uuid",
        request_id, staff["tenant_id"],
    )
    return {"ok": True}
