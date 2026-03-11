"""
Xero P&L financial adapter.

Pulls monthly revenue totals from the Xero ProfitAndLoss report.
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

XERO_TOKEN_URL = "https://identity.xero.com/connect/token"
XERO_PL_URL = "https://api.xero.com/api.xro/2.0/Reports/ProfitAndLoss"

# Refresh if token expires within this window
EXPIRY_BUFFER = timedelta(minutes=5)

# Xero P&L API supports a maximum 1-year range per request
MAX_RANGE_DAYS = 365

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

LOAD_XERO_CREDENTIALS_SQL = """
SELECT credentials
FROM core.tenant_credentials
WHERE tenant_id = $1::uuid
  AND provider = 'xero';
"""

UPDATE_XERO_CREDENTIALS_SQL = """
UPDATE core.tenant_credentials
SET credentials = $2::bytea,
    updated_at = now()
WHERE tenant_id = $1::uuid
  AND provider = 'xero';
"""


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class XeroFinancialAdapter:
    """
    Fetches monthly revenue from Xero ProfitAndLoss report.

    Credentials are stored Fernet-encrypted in core.tenant_credentials
    under provider='xero'. Expected blob fields:
        access_token, refresh_token, expires_at,
        tenant_id (Xero org UUID), accounting_method
    """

    def __init__(self, conn: asyncpg.Connection, tenant_id: str) -> None:
        self._conn = conn
        self._tenant_id = tenant_id

    # ------------------------------------------------------------------
    # Public interface (FinancialAdapter protocol)
    # ------------------------------------------------------------------

    async def fetch_revenue(
        self,
        period_start: date,
        period_end: date,
    ) -> FinancialSnapshot:
        """
        Pull monthly revenue totals for the given period from Xero.

        Makes multiple API calls if the range exceeds 1 year.
        No DB writes — caller handles persistence.
        """
        creds = await self._load_credentials()
        access_token = await self._ensure_valid_token(creds)
        xero_tenant_id = creds.get("tenant_id", "")
        accounting_method = creds.get("accounting_method", "accrual")

        monthly_revenues: list[MonthlyRevenue] = []

        # Split into <=365-day windows if needed
        windows = _build_date_windows(period_start, period_end)
        for window_start, window_end in windows:
            months = await self._fetch_pl_window(
                access_token, xero_tenant_id, window_start, window_end
            )
            monthly_revenues.extend(months)

        annual_revenue = sum(m.revenue_gbp for m in monthly_revenues)
        monthly_avg = annual_revenue / len(monthly_revenues) if monthly_revenues else 0.0

        return FinancialSnapshot(
            tenant_id=UUID(self._tenant_id),
            provider="xero",
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

    async def _load_credentials(self) -> dict[str, Any]:
        """Load and decrypt Xero credentials from DB."""
        raw = await self._conn.fetchval(LOAD_XERO_CREDENTIALS_SQL, self._tenant_id)
        if raw is None:
            raise RuntimeError(f"No Xero credentials for tenant {self._tenant_id}")
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
                f"Xero token expired and no refresh_token for tenant {self._tenant_id}"
            )

        updated_creds = await self._refresh_token(creds, refresh_token)
        return updated_creds["access_token"]

    async def _refresh_token(
        self,
        existing_creds: dict[str, Any],
        refresh_token: str,
    ) -> dict[str, Any]:
        """
        Call Xero token refresh endpoint and store updated credentials.

        Uses HTTP Basic auth with base64(client_id:client_secret).
        """
        client_id = os.getenv("XERO_CLIENT_ID")
        client_secret = os.getenv("XERO_CLIENT_SECRET")
        if not client_id or not client_secret:
            raise RuntimeError("XERO_CLIENT_ID and XERO_CLIENT_SECRET must be set")

        credentials_b64 = base64.b64encode(
            f"{client_id}:{client_secret}".encode()
        ).decode()

        logger.info(json.dumps({
            "event": "xero_token_refresh_request",
            "tenant_id": self._tenant_id,
        }))

        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                XERO_TOKEN_URL,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                },
                headers={
                    "Authorization": f"Basic {credentials_b64}",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )

        if resp.status_code != 200:
            logger.error(json.dumps({
                "event": "xero_token_refresh_failed",
                "tenant_id": self._tenant_id,
                "status": resp.status_code,
                "body": resp.text[:500],
            }))
            raise RuntimeError(
                f"Xero token refresh failed: {resp.status_code} {resp.text[:200]}"
            )

        data = resp.json()

        base = existing_creds.copy()
        expires_in = int(data.get("expires_in", 1800))
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

        base["access_token"] = data["access_token"]
        base["refresh_token"] = data.get("refresh_token", refresh_token)
        base["expires_at"] = expires_at.isoformat()

        logger.info(json.dumps({
            "event": "xero_token_refresh_success",
            "tenant_id": self._tenant_id,
            "expires_at": base["expires_at"],
        }))

        encrypted = encrypt_credentials(base)
        await self._conn.execute(UPDATE_XERO_CREDENTIALS_SQL, self._tenant_id, encrypted)

        return base

    async def _fetch_pl_window(
        self,
        access_token: str,
        xero_tenant_id: str,
        window_start: date,
        window_end: date,
    ) -> list[MonthlyRevenue]:
        """
        Fetch ProfitAndLoss report for a single <=365-day window.

        The undocumented `date` param (set to last day of first month) is required
        to prevent Xero returning cumulative YTD amounts instead of monthly columns.
        """
        # Number of full months between start and end (periods = months - 1)
        months_count = _count_months(window_start, window_end)
        periods = max(months_count - 1, 0)

        # `date` param: last day of the first month in the range
        first_month_end = _last_day_of_month(window_start)

        params = {
            "fromDate": window_start.strftime("%Y-%m-%d"),
            "toDate": window_end.strftime("%Y-%m-%d"),
            "periods": str(periods),
            "timeframe": "MONTH",
            "date": first_month_end.strftime("%Y-%m-%d"),
            "standardLayout": "true",
        }

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Xero-Tenant-Id": xero_tenant_id,
            "Accept": "application/json",
        }

        logger.info(json.dumps({
            "event": "xero_pl_request",
            "tenant_id": self._tenant_id,
            "from": params["fromDate"],
            "to": params["toDate"],
            "periods": periods,
        }))

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(XERO_PL_URL, params=params, headers=headers)

        if resp.status_code != 200:
            logger.error(json.dumps({
                "event": "xero_pl_failed",
                "tenant_id": self._tenant_id,
                "status": resp.status_code,
                "body": resp.text[:500],
            }))
            raise RuntimeError(
                f"Xero P&L request failed: {resp.status_code} {resp.text[:200]}"
            )

        return _parse_xero_pl(resp.json(), self._tenant_id)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def _parse_xero_pl(payload: dict[str, Any], tenant_id: str) -> list[MonthlyRevenue]:
    """
    Extract monthly Income totals from a Xero ProfitAndLoss report response.

    Structure:
        Reports[0].Rows → Section with Title=="Income" → SummaryRow → Cells
        Reports[0].Rows[0] (HeaderRow) → Cells → Value (month labels)
    """
    try:
        report = payload["Reports"][0]
    except (KeyError, IndexError) as exc:
        raise RuntimeError(f"Unexpected Xero P&L response structure: {exc}") from exc

    rows = report.get("Rows", [])

    # Extract month labels from the header row
    month_labels: list[str] = []
    for row in rows:
        if row.get("RowType") == "Header":
            cells = row.get("Cells", [])
            # First cell is the label column ("Account") — skip it
            for cell in cells[1:]:
                month_labels.append(cell.get("Value", ""))
            break

    if not month_labels:
        logger.warning(json.dumps({
            "event": "xero_pl_no_header",
            "tenant_id": tenant_id,
        }))
        return []

    # Find the Income section and its SummaryRow
    income_cells: list[dict[str, Any]] = []
    for row in rows:
        if row.get("RowType") == "Section" and row.get("Title") == "Income":
            for sub_row in row.get("Rows", []):
                if sub_row.get("RowType") == "SummaryRow":
                    income_cells = sub_row.get("Cells", [])
                    break
            break

    if not income_cells:
        logger.warning(json.dumps({
            "event": "xero_pl_no_income_section",
            "tenant_id": tenant_id,
        }))
        return []

    # income_cells[0] is the label ("Total Income") — skip it
    value_cells = income_cells[1:]

    monthly_revenues: list[MonthlyRevenue] = []
    for label, cell in zip(month_labels, value_cells):
        raw_value = cell.get("Value", "0")
        try:
            amount = float(raw_value.replace(",", "") if isinstance(raw_value, str) else raw_value)
        except (ValueError, AttributeError):
            amount = 0.0

        # Convert "01 Jan 2026" or "Jan 2026" style labels to YYYY-MM
        month_key = _parse_month_label(label)

        monthly_revenues.append(MonthlyRevenue(
            month=month_key,
            revenue_gbp=amount,
            source="xero",
        ))

    return monthly_revenues


def _parse_month_label(label: str) -> str:
    """
    Convert a Xero month header label to YYYY-MM.

    Xero returns labels like "01 Jan 2026" or "Jan 2026".
    Falls back to the raw label if parsing fails.
    """
    _MONTH_MAP = {
        "jan": "01", "feb": "02", "mar": "03", "apr": "04",
        "may": "05", "jun": "06", "jul": "07", "aug": "08",
        "sep": "09", "oct": "10", "nov": "11", "dec": "12",
    }
    parts = label.strip().split()
    # "01 Jan 2026" → parts = ["01", "Jan", "2026"]
    # "Jan 2026"    → parts = ["Jan", "2026"]
    try:
        if len(parts) == 3:
            month_abbr = parts[1].lower()
            year = parts[2]
        elif len(parts) == 2:
            month_abbr = parts[0].lower()
            year = parts[1]
        else:
            return label
        month_num = _MONTH_MAP.get(month_abbr, "01")
        return f"{year}-{month_num}"
    except Exception:
        return label


# ---------------------------------------------------------------------------
# Date utilities
# ---------------------------------------------------------------------------


def _build_date_windows(
    period_start: date,
    period_end: date,
) -> list[tuple[date, date]]:
    """
    Split a date range into windows of at most MAX_RANGE_DAYS days.

    Xero P&L API rejects ranges longer than 1 year.
    """
    windows: list[tuple[date, date]] = []
    current_start = period_start
    while current_start < period_end:
        current_end = min(
            current_start + timedelta(days=MAX_RANGE_DAYS - 1),
            period_end,
        )
        windows.append((current_start, current_end))
        current_start = current_end + timedelta(days=1)
    return windows


def _count_months(start: date, end: date) -> int:
    """Count the number of calendar months fully or partially spanned."""
    return (end.year - start.year) * 12 + (end.month - start.month) + 1


def _last_day_of_month(d: date) -> date:
    """Return the last day of the month containing d."""
    if d.month == 12:
        return date(d.year, 12, 31)
    return date(d.year, d.month + 1, 1) - timedelta(days=1)
