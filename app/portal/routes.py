import hashlib
import os
import secrets
from datetime import datetime, timedelta, timezone

import asyncpg
from fastapi import APIRouter, Body, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from ..config import settings
from .auth import get_conn, get_tenant_brand, log_audit
from .storage import SPACES_BUCKET, head_object, presign_get, presign_put

router = APIRouter(prefix="/portal", tags=["portal"])

templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "templates")
)

TENANT_SLUG = settings.portal_tenant_slug


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _resolve_token_sql() -> str:
    return """
        SELECT t.request_id, t.tenant_id, t.expires_at, t.revoked_at
        FROM portal.request_access_tokens t
        WHERE t.token_hash = $1
        LIMIT 1
    """


# ---------------------------------------------------------------------------
# JSON routes (existing — unchanged behaviour)
# ---------------------------------------------------------------------------

@router.get("/requests")
async def list_requests(conn: asyncpg.Connection = Depends(get_conn)):
    rows = await conn.fetch("""
        SELECT r.id, r.status::text AS status, r.due_at, r.created_at,
               c.full_name AS client_name, c.email AS client_email,
               COUNT(ri.id) AS item_count
        FROM portal.doc_requests r
        JOIN portal.clients c ON c.id = r.client_id
        LEFT JOIN portal.doc_request_items ri ON ri.request_id = r.id
        WHERE r.tenant_id = (SELECT id FROM portal.tenants WHERE slug=$1)
        GROUP BY r.id, r.status, r.due_at, r.created_at, c.full_name, c.email
        ORDER BY r.created_at DESC
    """, TENANT_SLUG)
    return {"requests": [dict(r) for r in rows]}


@router.get("/requests/{request_id}")
async def get_request(request_id: str, conn: asyncpg.Connection = Depends(get_conn)):
    req = await conn.fetchrow("""
        SELECT r.id, r.status::text AS status, r.due_at, r.created_at, r.sent_at,
               c.full_name AS client_name, c.email AS client_email, c.phone_e164 AS client_phone
        FROM portal.doc_requests r
        JOIN portal.clients c ON c.id = r.client_id
        WHERE r.id = $1::uuid
          AND r.tenant_id = (SELECT id FROM portal.tenants WHERE slug=$2)
    """, request_id, TENANT_SLUG)
    if not req:
        raise HTTPException(status_code=404, detail="Request not found")

    items = await conn.fetch("""
        SELECT id, title, item_type::text AS item_type, required, status::text AS status, sort_order, instructions
        FROM portal.doc_request_items
        WHERE request_id = $1::uuid
        ORDER BY sort_order ASC
    """, request_id)

    return {"request": dict(req), "items": [dict(i) for i in items]}


@router.post("/requests/{request_id}/access-link")
async def create_access_link(
    request_id: str,
    expires_days: int = 30,
    conn: asyncpg.Connection = Depends(get_conn),
):
    async with conn.transaction():
        req = await conn.fetchrow("""
            SELECT id FROM portal.doc_requests
            WHERE id = $1::uuid
              AND tenant_id = (SELECT id FROM portal.tenants WHERE slug=$2)
        """, request_id, TENANT_SLUG)
        if not req:
            raise HTTPException(status_code=404, detail="Request not found")

        raw_token = secrets.token_urlsafe(32)
        token_hash = _hash_token(raw_token)
        expires_at = _now_utc() + timedelta(days=expires_days)

        await conn.execute("""
            INSERT INTO portal.request_access_tokens (tenant_id, request_id, token_hash, expires_at)
            VALUES (
                (SELECT id FROM portal.tenants WHERE slug=$1),
                $2::uuid,
                $3,
                $4
            )
        """, TENANT_SLUG, request_id, token_hash, expires_at)

    link = f"{settings.portal_base_url}/portal/r/{raw_token}/view"
    return {"request_id": request_id, "expires_at": expires_at.isoformat(), "link": link}


@router.get("/r/{token}")
async def resolve_magic_link(token: str):
    """Redirect bare token URL to the HTML view."""
    return RedirectResponse(url=f"/portal/r/{token}/view", status_code=302)


# ---------------------------------------------------------------------------
# Client portal HTML view
# ---------------------------------------------------------------------------

@router.get("/r/{token}/view", response_class=HTMLResponse)
async def client_portal_view(
    token: str,
    request: Request,
    conn: asyncpg.Connection = Depends(get_conn),
):
    token_hash = _hash_token(token)

    async with conn.transaction():
        tok = await conn.fetchrow(_resolve_token_sql(), token_hash)

        if not tok:
            return templates.TemplateResponse("client.html", {
                "request": request, "error": "This link is invalid or has expired.",
                "req": None, "items": [], "token": token, "brand": {},
            })
        if tok["revoked_at"] is not None:
            return templates.TemplateResponse("client.html", {
                "request": request, "error": "This link has been revoked.",
                "req": None, "items": [], "token": token, "brand": {},
            })
        if tok["expires_at"] is not None and tok["expires_at"] < _now_utc():
            return templates.TemplateResponse("client.html", {
                "request": request, "error": "This link has expired. Please contact your advisor.",
                "req": None, "items": [], "token": token, "brand": {},
            })

        await conn.execute("""
            UPDATE portal.request_access_tokens
            SET last_used_at = now(), use_count = use_count + 1
            WHERE token_hash = $1
        """, token_hash)

        await conn.execute("""
            UPDATE portal.doc_requests
            SET last_viewed_at = now(),
                status = CASE
                    WHEN status IN ('sent') THEN 'viewed'::public.request_status
                    ELSE status
                END
            WHERE id = $1
        """, tok["request_id"])

        await log_audit(
            conn,
            tenant_id=tok["tenant_id"],
            event_type="magic_link_resolved",
            actor="client",
            request_id=str(tok["request_id"]),
        )

        req = await conn.fetchrow("""
            SELECT r.id, r.status::text AS status, r.due_at,
                   c.full_name AS client_name, c.email AS client_email
            FROM portal.doc_requests r
            JOIN portal.clients c ON c.id = r.client_id
            WHERE r.id = $1
        """, tok["request_id"])

        items = await conn.fetch("""
            SELECT id, title, item_type::text AS item_type, required,
                   status::text AS status, sort_order, instructions, file_key,
                   sig_page, sig_x, sig_y, sig_w, sig_h
            FROM portal.doc_request_items
            WHERE request_id = $1
            ORDER BY sort_order ASC
        """, tok["request_id"])

        # Fetch zones for all items
        item_ids = [i["id"] for i in items]
        zones = []
        if item_ids:
            zones = await conn.fetch("""
                SELECT id, request_item_id, zone_type::text AS zone_type, label,
                       page, x, y, w, h, sort_order, required, value, signature_file_key
                FROM portal.request_item_zones
                WHERE request_item_id = ANY($1::uuid[])
                ORDER BY sort_order ASC
            """, item_ids)

    # Attach zones to items
    zones_by_item = {}
    for z in zones:
        rid = str(z["request_item_id"])
        zd = dict(z)
        zd["id"] = str(zd["id"])
        zd["request_item_id"] = rid
        zones_by_item.setdefault(rid, []).append(zd)
    items_list = [dict(i) for i in items]
    for item in items_list:
        item["zones"] = zones_by_item.get(str(item["id"]), [])

    brand = await get_tenant_brand(conn, str(tok["tenant_id"]))
    return templates.TemplateResponse("client.html", {
        "request": request,
        "error": None,
        "req": dict(req),
        "items": items_list,
        "token": token,
        "brand": brand,
    })


# ---------------------------------------------------------------------------
# Upload flow
# ---------------------------------------------------------------------------

@router.post("/r/{token}/items/{item_id}/upload-url")
async def create_upload_url(
    token: str,
    item_id: str,
    filename: str = Body(..., embed=True),
    content_type: str = Body(..., embed=True),
    conn: asyncpg.Connection = Depends(get_conn),
):
    token_hash = _hash_token(token)

    async with conn.transaction():
        tok = await conn.fetchrow("""
            SELECT request_id FROM portal.request_access_tokens
            WHERE token_hash=$1
              AND revoked_at IS NULL
              AND (expires_at IS NULL OR expires_at > now())
            LIMIT 1
        """, token_hash)
        if not tok:
            raise HTTPException(status_code=404, detail="Invalid or expired link")

        request_id = tok["request_id"]

        item = await conn.fetchrow("""
            SELECT id, tenant_id, request_id FROM portal.doc_request_items
            WHERE id=$1::uuid AND request_id=$2
        """, item_id, request_id)
        if not item:
            raise HTTPException(status_code=404, detail="Item not found for this request")

        tenant_id = item["tenant_id"]
        object_key = f"{tenant_id}/{request_id}/{item_id}/{filename}"
        upload_url = presign_put(object_key, content_type)

        file_row = await conn.fetchrow("""
            INSERT INTO portal.files (
                tenant_id, request_id, request_item_id, purpose,
                storage_provider, storage_bucket, storage_key,
                original_filename, mime_type, scan_status,
                uploaded_by_actor, uploaded_by_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            RETURNING id
        """, tenant_id, request_id, item["id"], "client_upload",
            "do_spaces", SPACES_BUCKET, object_key,
            filename, content_type, "pending", "client", None)

    return {
        "file_id": str(file_row["id"]),
        "upload_url": upload_url,
        "storage_key": object_key,
        "expires_in_seconds": 600,
    }


@router.post("/r/{token}/files/{file_id}/confirm")
async def confirm_upload(
    token: str,
    file_id: str,
    conn: asyncpg.Connection = Depends(get_conn),
):
    token_hash = _hash_token(token)

    async with conn.transaction():
        tok = await conn.fetchrow("""
            SELECT t.request_id, t.tenant_id FROM portal.request_access_tokens t
            WHERE t.token_hash = $1
              AND t.revoked_at IS NULL
              AND (t.expires_at IS NULL OR t.expires_at > now())
            LIMIT 1
        """, token_hash)
        if not tok:
            raise HTTPException(status_code=404, detail="Invalid or expired link")

        request_id = tok["request_id"]
        tenant_id = tok["tenant_id"]

        file_row = await conn.fetchrow("""
            SELECT id, storage_key, request_item_id, scan_status::text AS scan_status
            FROM portal.files
            WHERE id = $1::uuid
              AND request_id = $2
              AND tenant_id = $3
        """, file_id, request_id, tenant_id)
        if not file_row:
            raise HTTPException(status_code=404, detail="File not found for this request")

        obj_meta = head_object(file_row["storage_key"])
        if not obj_meta:
            raise HTTPException(status_code=400, detail="Object not found in storage")

        size_bytes = obj_meta["size_bytes"]

        await conn.execute("""
            UPDATE portal.files
            SET size_bytes = $1, scan_status = 'clean'::public.scan_status
            WHERE id = $2::uuid
        """, size_bytes, file_id)

        await conn.execute("""
            UPDATE portal.doc_request_items
            SET status = 'uploaded'::public.request_item_status
            WHERE id = $1
              AND tenant_id = $2
        """, file_row["request_item_id"], tenant_id)

        await log_audit(
            conn,
            tenant_id=tenant_id,
            event_type="file_confirmed",
            actor="client",
            request_id=str(request_id),
            request_item_id=str(file_row["request_item_id"]),
            file_id=file_id,
        )

    return {
        "file_id": file_id,
        "size_bytes": size_bytes,
        "scan_status": "clean",
        "item_status": "uploaded",
    }


# ---------------------------------------------------------------------------
# Signature flow (document to sign)
# ---------------------------------------------------------------------------

@router.get("/r/{token}/signature/{item_id}/pdf-bytes")
async def client_signature_pdf_bytes(
    token: str,
    item_id: str,
    conn: asyncpg.Connection = Depends(get_conn),
):
    """Stream the PDF through for client-side PDF.js rendering."""
    token_hash = _hash_token(token)
    tok = await conn.fetchrow("""
        SELECT request_id, tenant_id FROM portal.request_access_tokens
        WHERE token_hash=$1 AND revoked_at IS NULL
          AND (expires_at IS NULL OR expires_at > now())
        LIMIT 1
    """, token_hash)
    if not tok:
        raise HTTPException(status_code=404, detail="Invalid or expired link")

    item = await conn.fetchrow(
        "SELECT file_key FROM portal.doc_request_items WHERE id = $1::uuid AND request_id = $2",
        item_id, tok["request_id"],
    )
    if not item or not item["file_key"]:
        raise HTTPException(status_code=404, detail="Document not found")

    from .storage import download_object
    from fastapi.responses import Response
    pdf_data = download_object(item["file_key"])
    return Response(content=pdf_data, media_type="application/pdf")


@router.get("/r/{token}/signature/{item_id}/document")
async def signature_document(
    token: str,
    item_id: str,
    conn: asyncpg.Connection = Depends(get_conn),
):
    """Redirect to presigned GET URL for the document the client needs to sign."""
    token_hash = _hash_token(token)
    tok = await conn.fetchrow("""
        SELECT request_id, tenant_id FROM portal.request_access_tokens
        WHERE token_hash=$1 AND revoked_at IS NULL
          AND (expires_at IS NULL OR expires_at > now())
        LIMIT 1
    """, token_hash)
    if not tok:
        raise HTTPException(status_code=404, detail="Invalid or expired link")

    item = await conn.fetchrow(
        "SELECT file_key FROM portal.doc_request_items WHERE id = $1::uuid AND request_id = $2",
        item_id, tok["request_id"],
    )
    if not item or not item["file_key"]:
        raise HTTPException(status_code=404, detail="Document not found")

    url = presign_get(item["file_key"])
    return RedirectResponse(url=url, status_code=302)


@router.post("/r/{token}/items/{item_id}/upload-sig")
async def signature_upload(
    token: str,
    item_id: str,
    body: dict = Body(...),
    conn: asyncpg.Connection = Depends(get_conn),
):
    """Upload signature PNG through the server (avoids CORS with Spaces)."""
    token_hash = _hash_token(token)
    tok = await conn.fetchrow("""
        SELECT request_id, tenant_id FROM portal.request_access_tokens
        WHERE token_hash=$1 AND revoked_at IS NULL
          AND (expires_at IS NULL OR expires_at > now())
        LIMIT 1
    """, token_hash)
    if not tok:
        raise HTTPException(status_code=404, detail="Invalid or expired link")

    item = await conn.fetchrow(
        "SELECT id, tenant_id FROM portal.doc_request_items WHERE id = $1::uuid AND request_id = $2",
        item_id, tok["request_id"],
    )
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")

    import base64
    data_url = body.get("image", "")
    if "," in data_url:
        data_url = data_url.split(",", 1)[1]
    png_bytes = base64.b64decode(data_url)

    object_key = f"{item['tenant_id']}/{tok['request_id']}/{item_id}/signature.png"
    from .storage import upload_object
    upload_object(object_key, png_bytes, "image/png")
    return {"file_key": object_key}


@router.post("/r/{token}/items/{item_id}/acknowledge")
async def acknowledge_signature(
    token: str,
    item_id: str,
    body: dict = Body(default={}),
    conn: asyncpg.Connection = Depends(get_conn),
):
    """Confirm client has signed — save signature_file_key and set status to uploaded."""
    token_hash = _hash_token(token)

    async with conn.transaction():
        tok = await conn.fetchrow("""
            SELECT request_id, tenant_id FROM portal.request_access_tokens
            WHERE token_hash=$1 AND revoked_at IS NULL
              AND (expires_at IS NULL OR expires_at > now())
            LIMIT 1
        """, token_hash)
        if not tok:
            raise HTTPException(status_code=404, detail="Invalid or expired link")

        item = await conn.fetchrow(
            """SELECT id, item_type::text AS item_type, status::text AS status,
                      file_key, sig_page, sig_x, sig_y, sig_w, sig_h
               FROM portal.doc_request_items
               WHERE id = $1::uuid AND request_id = $2""",
            item_id, tok["request_id"],
        )
        if not item or item["item_type"] != "signature":
            raise HTTPException(status_code=400, detail="Not a signature item")
        if item["status"] not in ("missing", "pending"):
            return {"item_id": item_id, "status": item["status"]}

        signature_file_key = (body.get("signature_file_key") or "").strip() or None

        # Server-side PDF merge if position data exists
        signed_pdf_key = None
        if signature_file_key and item["file_key"] and item["sig_page"] is not None:
            try:
                import asyncio
                from .storage import download_object, upload_object
                from .pdf_merge import merge_signature_onto_pdf

                pdf_bytes = download_object(item["file_key"])
                sig_bytes = download_object(signature_file_key)

                merged = await asyncio.to_thread(
                    merge_signature_onto_pdf,
                    pdf_bytes, sig_bytes,
                    item["sig_page"],
                    item["sig_x"], item["sig_y"],
                    item["sig_w"], item["sig_h"],
                )

                signed_pdf_key = f"{tok['tenant_id']}/{tok['request_id']}/{item_id}/signed.pdf"
                upload_object(signed_pdf_key, merged, "application/pdf")
            except Exception:
                pass  # Merge failed — still save signature, just no merged PDF

        await conn.execute(
            """UPDATE portal.doc_request_items
               SET status = 'uploaded'::public.request_item_status,
                   signature_file_key = $1,
                   signed_pdf_key = $2
               WHERE id = $3::uuid AND tenant_id = $4""",
            signature_file_key,
            signed_pdf_key,
            item_id,
            tok["tenant_id"],
        )

        await log_audit(
            conn,
            tenant_id=tok["tenant_id"],
            event_type="signature_acknowledged",
            actor="client",
            request_id=str(tok["request_id"]),
        )

    return {"item_id": item_id, "status": "uploaded"}


@router.post("/r/{token}/items/{item_id}/zones/submit")
async def submit_zone_values(
    token: str,
    item_id: str,
    body: dict = Body(default={}),
    conn: asyncpg.Connection = Depends(get_conn),
):
    """Save filled zone values (text, date) from client."""
    token_hash = _hash_token(token)

    tok = await conn.fetchrow("""
        SELECT request_id, tenant_id FROM portal.request_access_tokens
        WHERE token_hash=$1 AND revoked_at IS NULL
          AND (expires_at IS NULL OR expires_at > now())
        LIMIT 1
    """, token_hash)
    if not tok:
        raise HTTPException(status_code=404, detail="Invalid or expired link")

    # Verify item belongs to this request
    item = await conn.fetchrow(
        "SELECT id FROM portal.doc_request_items WHERE id=$1::uuid AND request_id=$2",
        item_id, tok["request_id"],
    )
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")

    zone_values = body.get("zone_values", {})
    sig_keys = body.get("signature_keys", {})

    for zone_id, value in zone_values.items():
        await conn.execute(
            """UPDATE portal.request_item_zones
               SET value = $1, filled_at = now()
               WHERE id = $2::uuid AND request_item_id = $3::uuid AND tenant_id = $4""",
            str(value).strip() if value else None,
            zone_id, item_id, tok["tenant_id"],
        )

    for zone_id, file_key in sig_keys.items():
        await conn.execute(
            """UPDATE portal.request_item_zones
               SET signature_file_key = $1, filled_at = now()
               WHERE id = $2::uuid AND request_item_id = $3::uuid AND tenant_id = $4""",
            file_key, zone_id, item_id, tok["tenant_id"],
        )

    # Mark item as uploaded
    await conn.execute(
        """UPDATE portal.doc_request_items
           SET status = 'uploaded'::public.request_item_status
           WHERE id = $1::uuid""",
        item_id,
    )

    await log_audit(
        conn,
        tenant_id=str(tok["tenant_id"]),
        event_type="zones_submitted",
        actor="client",
        request_id=str(tok["request_id"]),
    )

    return {"item_id": item_id, "status": "uploaded"}


@router.get("/r/{token}/items/{item_id}/download-completed")
async def client_download_completed(
    token: str,
    item_id: str,
    conn: asyncpg.Connection = Depends(get_conn),
):
    """Download the completed PDF with all filled zones burned in."""
    token_hash = _hash_token(token)
    tok = await conn.fetchrow("""
        SELECT request_id, tenant_id FROM portal.request_access_tokens
        WHERE token_hash=$1 AND revoked_at IS NULL
          AND (expires_at IS NULL OR expires_at > now())
        LIMIT 1
    """, token_hash)
    if not tok:
        raise HTTPException(status_code=404, detail="Invalid or expired link")

    item = await conn.fetchrow(
        """SELECT file_key, label FROM portal.doc_request_items
           WHERE id = $1::uuid AND request_id = $2::uuid""",
        item_id, tok["request_id"],
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

    filename = (item["label"] or "document").replace('"', "") + " - completed.pdf"
    return Response(
        content=merged,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
