"""
Portal seed script — run locally with doadmin DATABASE_URL.

Usage:
  python scripts/portal_seed.py --email admin@humtech.ai --password MyPassword123

Options:
  --email     Staff user email (required)
  --password  Staff user password (required)
  --test      Also seed a test client + doc request with 3 items
"""
import argparse
import asyncio
import os
import sys
import uuid
from pathlib import Path

# Load .env so we pick up DATABASE_URL
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

import asyncpg

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set")
    sys.exit(1)

# Import password hasher from the platform
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.portal.auth import hash_password

TENANT_SLUG = os.getenv("PORTAL_TENANT_SLUG", "humtech")
TENANT_NAME = "HumTech"


async def seed(email: str, password: str, with_test_data: bool):
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        # ---------------------------------------------------------------
        # 1. Ensure portal.tenants row exists
        # ---------------------------------------------------------------
        tenant = await conn.fetchrow(
            "SELECT id FROM portal.tenants WHERE slug = $1", TENANT_SLUG
        )
        if tenant:
            tenant_id = tenant["id"]
            print(f"OK Tenant already exists: {TENANT_SLUG} ({tenant_id})")
        else:
            tenant_id = await conn.fetchval(
                """
                INSERT INTO portal.tenants (name, slug, status)
                VALUES ($1, $2, 'active')
                RETURNING id
                """,
                TENANT_NAME,
                TENANT_SLUG,
            )
            print(f"OK Created tenant: {TENANT_SLUG} ({tenant_id})")

        # ---------------------------------------------------------------
        # 2. Create or update staff user
        # ---------------------------------------------------------------
        email_lower = email.lower().strip()
        pw_hash = hash_password(password)

        existing = await conn.fetchrow(
            "SELECT id FROM portal.staff_users WHERE tenant_id = $1 AND email = $2",
            tenant_id,
            email_lower,
        )
        if existing:
            await conn.execute(
                "UPDATE portal.staff_users SET password_hash = $1, is_active = true WHERE id = $2",
                pw_hash,
                existing["id"],
            )
            print(f"OK Updated staff user: {email_lower} (password reset)")
        else:
            staff_id = await conn.fetchval(
                """
                INSERT INTO portal.staff_users (tenant_id, email, full_name, role, password_hash, is_active)
                VALUES ($1, $2, $3, 'admin', $4, true)
                RETURNING id
                """,
                tenant_id,
                email_lower,
                email_lower.split("@")[0].title(),
                pw_hash,
            )
            print(f"OK Created staff user: {email_lower} (id: {staff_id})")

        # ---------------------------------------------------------------
        # 3. Optional test data
        # ---------------------------------------------------------------
        if with_test_data:
            client_id = await conn.fetchval(
                """
                INSERT INTO portal.clients (tenant_id, full_name, email)
                VALUES ($1, 'Test Client', 'test@example.com')
                ON CONFLICT DO NOTHING
                RETURNING id
                """,
                tenant_id,
            )
            if not client_id:
                client_id = await conn.fetchval(
                    "SELECT id FROM portal.clients WHERE tenant_id = $1 AND email = 'test@example.com'",
                    tenant_id,
                )

            request_id = await conn.fetchval(
                """
                INSERT INTO portal.doc_requests (tenant_id, client_id, status)
                VALUES ($1, $2, 'draft')
                RETURNING id
                """,
                tenant_id,
                client_id,
            )

            items = [
                ("Proof of Identity", "file_upload", "A valid passport or driving licence"),
                ("Bank Statement (last 3 months)", "file_upload", "PDF or scanned copy"),
                ("Proof of Address", "file_upload", "Utility bill or bank letter dated within 90 days"),
            ]
            for i, (title, itype, instructions) in enumerate(items):
                await conn.execute(
                    """
                    INSERT INTO portal.doc_request_items
                        (tenant_id, request_id, item_type, title, instructions, required, sort_order)
                    VALUES ($1, $2, $3::public.template_item_type, $4, $5, true, $6)
                    """,
                    tenant_id,
                    request_id,
                    itype,
                    title,
                    instructions,
                    i,
                )

            print(f"OK Created test client + request ({request_id}) with {len(items)} items")
            print(f"  View at: /portal/staff/requests/{request_id}")

        print("\nDone. Login at /portal/staff/login")

    finally:
        await conn.close()


def main():
    parser = argparse.ArgumentParser(description="Seed the HumTech document portal")
    parser.add_argument("--email", required=True, help="Staff user email")
    parser.add_argument("--password", required=True, help="Staff user password")
    parser.add_argument("--test", action="store_true", help="Also seed test client + request")
    args = parser.parse_args()
    asyncio.run(seed(args.email, args.password, args.test))


if __name__ == "__main__":
    main()
