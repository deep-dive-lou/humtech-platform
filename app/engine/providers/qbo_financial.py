"""
QuickBooks Online P&L financial adapter.

Pulls monthly revenue totals from the QBO ProfitAndLoss report.
Handles OAuth2 token refresh and Fernet-encrypted credential storage.
"""
from __future__ import annotations

import base64
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import asyncpg
import httpx

from app.utils.crypto import decrypt_credentials, encrypt_credentials
from app.engine.providers.financial_base import FinancialAdapter, FinancialSnapshot, MonthlyRevenue

logger = logging.getLogger(__name__)

QBO_TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
QBO_API_BASE = "https://quickbooks.api.intuit.com"
QBO_SANDBOX_BASE = "https://sandbox-quickbooks.api.intuit.com"

# Refresh if token expires within this window
EXPIRY_BUFFER = timedelta(minutes=5)

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

LOAD_QBO_CREDENTIALS_SQL = """
SELECT credentials
FROM core.tenant_credentials
WHERE tenant_id = $1::uuid
  AND provider = 'quickbooks';
"""

UPDATE_QBO_CREDENTIALS_SQL = """
UPDATE core.tenant_credentials
SET credentials = $2::bytea,
    updated_at = now()
WHERE tenant_id = $1::uuid
  AND provider = 'quickbooks';
"""


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class QBOFinancialAdapter:
    """
    Fetches monthly revenue from the QuickBooks Online ProfitAndLoss report.

    Credentials are stored Fernet-encrypted in core.tenant_credentials
    under provider='quickbooks'. Expected blob fields:
        access_token, refresh_token, expires_at,
        realm_id, accounting_method
    """

    def __init__(self, conn: asyncpg.Connection, tenant_id: str) -> None:
        self._conn = conn
        self._tenant_id = tenant_id
        self._sandbox = os.getenv("QBO_SANDBOX", "").lower() in ("1", "true", "yes")

    # ------------------------------------------------------------------
    # Public interface (FinancialAdapter protocol)
    # ------------------------------------------------------------------

    async def fetch_revenue(
        self,
        period_start: date,
        period_end: date,
    ) -> FinancialSnapshot:
        """
        Pull monthly revenue totals for the given period from QuickBooks Online.

        No DB writes — caller handles persistence.
        """
        creds = await self._load_credentials()
        access_token = await self._ensure_valid_token(creds)
        realm_id = creds.get("realm_id", "")
        accounting_method = creds.get("accounting_method", "accrual")

        monthly_revenues = await self._fetch_pl(
            access_token=access_token,
            realm_id=realm_id,
            period_start=period_start,
            period_end=period_end,
            accounting_method=accounting_method,
        )

        annual_revenue = sum(m.revenue_gbp for m in monthly_revenues)
        monthly_avg = annual_revenue / len(monthly_revenues) if monthly_revenues else 0.0

        return FinancialSnapshot(
            tenant_id=UUID(self._tenant_id),
            provider="quickbooks",
            period_start=period_start,
            period_end=period_end,
            monthly_revenue=monthly_revenues,
            annual_revenue_gbp=annual_revenue,
            monthly_avg_gbp=monthly_avg,
            accounting_method=accounting_method,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @property
    def _api_base(self) -> str:
        return QBO_SANDBOX_BASE if self._sandbox else QBO_API_BASE

    async def _load_credentials(self) -> dict[str, Any]:
        """Load and decrypt QBO credentials from DB."""
        raw = await self._conn.fetchval(LOAD_QBO_CREDENTIALS_SQL, self._tenant_id)
        if raw is None:
            raise RuntimeError(f"No QuickBooks credentials for tenant {self._tenant_id}")
        return decrypt_credentials(bytes(raw))

    def _is_expired(self, creds: dict[str, Any]) -> bool:
        """True if access_token is missing or expires within EXPIRY_BUFFER."""
        expires_at_str = creds.get("expires_at")
        if not expires_at_str:
            return True
        try:
            expires_at = datetime.fromisoformat(expires_at_str)
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            return datetime.now(timezone.utc) >= (expires_at - EXPIRY_BUFFER)
        except (ValueError, TypeError):
            return True

    async def _ensure_valid_token(self, creds: dict[str, Any]) -> str:
        """Return a valid access_token, refreshing if expired."""
        if not self._is_expired(creds):
            return creds["access_token"]

        refresh_token = creds.get("refresh_token")
        if not refresh_token:
            raise RuntimeError(
                f"QBO token expired and no refresh_token for tenant {self._tenant_id}"
            )

        updated_creds = await self._refresh_token(creds, refresh_token)
        return updated_creds["access_token"]

    async def _refresh_token(
        self,
        existing_creds: dict[str, Any],
        refresh_token: str,
    ) -> dict[str, Any]:
        """
        Call QBO token refresh endpoint and store updated credentials.

        Uses HTTP Basic auth with base64(client_id:client_secret).
        """
        client_id = os.getenv("QUICKBOOKS_CLIENT_ID")
        client_secret = os.getenv("QUICKBOOKS_CLIENT_SECRET")
        if not client_id or not client_secret:
            raise RuntimeError("QUICKBOOKS_CLIENT_ID and QUICKBOOKS_CLIENT_SECRET must be set")

        credentials_b64 = base64.b64encode(
            f"{client_id}:{client_secret}".encode()
        ).decode()

        logger.info(json.dumps({
            "event": "qbo_token_refresh_request",
            "tenant_id": self._tenant_id,
        }))

        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                QBO_TOKEN_URL,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                },
                headers={
                    "Authorization": f"Basic {credentials_b64}",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json",
                },
            )

        if resp.status_code != 200:
            logger.error(json.dumps({
                "event": "qbo_token_refresh_failed",
                "tenant_id": self._tenant_id,
                "status": resp.status_code,
                "body": resp.text[:500],
            }))
            raise RuntimeError(
                f"QBO token refresh failed: {resp.status_code} {resp.text[:200]}"
            )

        data = resp.json()

        base = existing_creds.copy()
        expires_in = int(data.get("expires_in", 3600))
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

        base["access_token"] = data["access_token"]
        base["refresh_token"] = data.get("refresh_token", refresh_token)
        base["expires_at"] = expires_at.isoformat()

        logger.info(json.dumps({
            "event": "qbo_token_refresh_success",
            "tenant_id": self._tenant_id,
            "expires_at": base["expires_at"],
        }))

        encrypted = encrypt_credentials(base)
        await self._conn.execute(UPDATE_QBO_CREDENTIALS_SQL, self._tenant_id, encrypted)

        return base

    async def _fetch_pl(
        self,
        access_token: str,
        realm_id: str,
        period_start: date,
        period_end: date,
        accounting_method: str,
    ) -> list[MonthlyRevenue]:
        """Fetch the ProfitAndLoss report from QBO and return monthly revenue rows."""
        url = f"{self._api_base}/v3/company/{realm_id}/reports/ProfitAndLoss"

        # QBO expects Accrual or Cash (capitalised)
        qbo_method = "Accrual" if accounting_method.lower() == "accrual" else "Cash"

        params = {
            "start_date": period_start.strftime("%Y-%m-%d"),
            "end_date": period_end.strftime("%Y-%m-%d"),
            "summarize_column_by": "Month",
            "accounting_method": qbo_method,
            "minorversion": "73",
        }

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }

        logger.info(json.dumps({
            "event": "qbo_pl_request",
            "tenant_id": self._tenant_id,
            "realm_id": realm_id,
            "start_date": params["start_date"],
            "end_date": params["end_date"],
            "sandbox": self._sandbox,
        }))

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(url, params=params, headers=headers)

        if resp.status_code != 200:
            logger.error(json.dumps({
                "event": "qbo_pl_failed",
                "tenant_id": self._tenant_id,
                "status": resp.status_code,
                "body": resp.text[:500],
            }))
            raise RuntimeError(
                f"QBO P&L request failed: {resp.status_code} {resp.text[:200]}"
            )

        return _parse_qbo_pl(resp.json(), self._tenant_id)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def _parse_qbo_pl(payload: dict[str, Any], tenant_id: str) -> list[MonthlyRevenue]:
    """
    Extract monthly Income totals from a QBO ProfitAndLoss report response.

    Structure:
        Columns.Column[] → ColTitle (month labels, index 0 = "")
        Rows.Row[] → group=="Income" → Summary.ColData[] (index 0 = label, rest = amounts)
    """
    try:
        columns = payload["Columns"]["Column"]
        rows = payload["Rows"]["Row"]
    except (KeyError, TypeError) as exc:
        raise RuntimeError(f"Unexpected QBO P&L response structure: {exc}") from exc

    # Extract month labels from column headers (skip index 0 — empty label column)
    month_labels: list[str] = []
    for col in columns[1:]:
        # ColTitle is typically "Jan 2026" or an ISO date string
        month_labels.append(col.get("ColTitle", ""))

    if not month_labels:
        logger.warning(json.dumps({
            "event": "qbo_pl_no_columns",
            "tenant_id": tenant_id,
        }))
        return []

    # Find the Income group row
    income_col_data: list[dict[str, Any]] = []
    for row in rows:
        if row.get("group") == "Income":
            summary = row.get("Summary", {})
            col_data = summary.get("ColData", [])
            if col_data:
                income_col_data = col_data
            break

    if not income_col_data:
        logger.warning(json.dumps({
            "event": "qbo_pl_no_income_group",
            "tenant_id": tenant_id,
        }))
        return []

    # income_col_data[0] is the label ("Total Income") — skip it
    value_entries = income_col_data[1:]

    monthly_revenues: list[MonthlyRevenue] = []
    for label, entry in zip(month_labels, value_entries):
        raw_value = entry.get("value", "0")
        try:
            amount = float(
                raw_value.replace(",", "") if isinstance(raw_value, str) else raw_value
            )
        except (ValueError, AttributeError):
            amount = 0.0

        month_key = _parse_month_label(label)

        monthly_revenues.append(MonthlyRevenue(
            month=month_key,
            revenue_gbp=amount,
            source="quickbooks",
        ))

    return monthly_revenues


def _parse_month_label(label: str) -> str:
    """
    Convert a QBO month column label to YYYY-MM.

    QBO returns labels like "Jan 2026" or ISO date strings "2026-01-01".
    Falls back to the raw label if parsing fails.
    """
    _MONTH_MAP = {
        "jan": "01", "feb": "02", "mar": "03", "apr": "04",
        "may": "05", "jun": "06", "jul": "07", "aug": "08",
        "sep": "09", "oct": "10", "nov": "11", "dec": "12",
    }
    label = label.strip()

    # ISO date format: "2026-01-01"
    if len(label) == 10 and label[4] == "-" and label[7] == "-":
        return label[:7]  # YYYY-MM

    parts = label.split()
    try:
        if len(parts) == 2:
            month_abbr = parts[0].lower()
            year = parts[1]
            month_num = _MONTH_MAP.get(month_abbr, "01")
            return f"{year}-{month_num}"
    except Exception:
        pass

    return label
