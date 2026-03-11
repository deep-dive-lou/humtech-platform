"""Financial adapter protocol — all financial adapters implement this interface."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Protocol
from uuid import UUID


@dataclass
class MonthlyRevenue:
    month: str          # "2026-01" (YYYY-MM)
    revenue_gbp: float
    source: str         # "xero" | "quickbooks"


@dataclass
class FinancialSnapshot:
    tenant_id: UUID
    provider: str                      # "xero" | "quickbooks"
    period_start: date
    period_end: date
    monthly_revenue: list[MonthlyRevenue] = field(default_factory=list)
    annual_revenue_gbp: float = 0.0
    monthly_avg_gbp: float = 0.0
    accounting_method: str = "accrual"
    pulled_at: datetime = field(default_factory=datetime.utcnow)


class FinancialAdapter(Protocol):
    async def fetch_revenue(
        self,
        period_start: date,
        period_end: date,
    ) -> FinancialSnapshot:
        """Pull monthly revenue totals for the given period. No DB writes — caller handles persistence."""
        ...
