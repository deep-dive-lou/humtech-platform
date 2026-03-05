"""Merge zones (signatures, text, dates) onto PDF pages."""
import io
from typing import Callable, Optional

from PIL import Image
from pypdf import PdfReader, PdfWriter
from reportlab.lib.colors import Color
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen.canvas import Canvas


def merge_signature_onto_pdf(
    pdf_bytes: bytes,
    sig_png_bytes: bytes,
    page_index: int,
    x_pct: float,
    y_pct: float,
    w_pct: float,
    h_pct: float,
) -> bytes:
    """
    Composite a signature PNG onto a specific page of a PDF.

    Coordinates are percentages of the page dimensions (0-100).
    y_pct is measured from the TOP of the page (matching browser convention).
    Returns the merged PDF as bytes.
    """
    reader = PdfReader(io.BytesIO(pdf_bytes))
    page = reader.pages[page_index]
    media = page.mediabox

    page_w = float(media.width)
    page_h = float(media.height)

    # Convert percentages to PDF points
    x_pts = x_pct / 100.0 * page_w
    w_pts = w_pct / 100.0 * page_w
    h_pts = h_pct / 100.0 * page_h

    # PDF origin is bottom-left; y_pct is top-down, so flip
    y_pts = page_h - (y_pct / 100.0 * page_h) - h_pts

    # Build a single-page overlay PDF with the signature image
    overlay_buf = io.BytesIO()
    c = Canvas(overlay_buf, pagesize=(page_w, page_h))

    sig_image = ImageReader(io.BytesIO(sig_png_bytes))
    c.drawImage(
        sig_image,
        x_pts, y_pts, w_pts, h_pts,
        mask="auto",  # preserve PNG transparency
    )
    c.save()

    # Merge overlay onto the target page
    overlay_reader = PdfReader(overlay_buf)
    page.merge_page(overlay_reader.pages[0])

    # Write the full PDF out
    writer = PdfWriter()
    for p in reader.pages:
        writer.add_page(p)

    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


def merge_zones_onto_pdf(
    pdf_bytes: bytes,
    zones: list[dict],
    download_fn: Callable[[str], bytes],
) -> bytes:
    """
    Burn all filled zones onto a PDF and return the result as bytes.

    Each zone dict must have: zone_type, page, x, y, w, h (percentages 0-100).
    Text/date zones use 'value'. Signature zones use 'signature_file_key'.
    download_fn(key) -> bytes is called to fetch signature PNGs from storage.
    """
    reader = PdfReader(io.BytesIO(pdf_bytes))
    num_pages = len(reader.pages)

    # Group zones by page
    by_page: dict[int, list[dict]] = {}
    for z in zones:
        pg = int(z.get("page", 0))
        if pg < 0 or pg >= num_pages:
            continue
        by_page.setdefault(pg, []).append(z)

    for page_idx, page_zones in by_page.items():
        page = reader.pages[page_idx]
        media = page.mediabox
        page_w = float(media.width)
        page_h = float(media.height)

        overlay_buf = io.BytesIO()
        c = Canvas(overlay_buf, pagesize=(page_w, page_h))

        for z in page_zones:
            x_pct = float(z.get("x", 0))
            y_pct = float(z.get("y", 0))
            w_pct = float(z.get("w", 20))
            h_pct = float(z.get("h", 5))

            x_pts = x_pct / 100.0 * page_w
            w_pts = w_pct / 100.0 * page_w
            h_pts = h_pct / 100.0 * page_h
            y_pts = page_h - (y_pct / 100.0 * page_h) - h_pts

            zone_type = z.get("zone_type", "")
            value = z.get("value") or ""

            if zone_type == "signature":
                file_key = z.get("signature_file_key")
                if not file_key:
                    continue
                try:
                    sig_bytes = download_fn(file_key)
                    sig_image = ImageReader(io.BytesIO(sig_bytes))
                    c.drawImage(
                        sig_image, x_pts, y_pts, w_pts, h_pts,
                        mask="auto",
                    )
                except Exception:
                    continue

            elif zone_type in ("text", "date") and value:
                # Size font to fit zone height (~60% of box height)
                font_size = min(h_pts * 0.6, 14)
                if font_size < 4:
                    font_size = 4
                c.setFont("Helvetica", font_size)
                c.setFillColor(Color(0.1, 0.1, 0.1))
                # Draw text vertically centered in the zone
                text_y = y_pts + (h_pts - font_size) / 2
                c.drawString(x_pts + 2, text_y, value)

        c.save()

        overlay_reader = PdfReader(overlay_buf)
        page.merge_page(overlay_reader.pages[0])

    writer = PdfWriter()
    for p in reader.pages:
        writer.add_page(p)

    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()
