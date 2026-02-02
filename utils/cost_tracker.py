"""Budget and cost tracking for enrichment API usage."""

import logging
from datetime import datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from config import PROVIDER_COSTS, settings
from core.models import CostSummary, EnrichmentLog

logger = logging.getLogger(__name__)


class CostTracker:
    """Track API costs and enforce budget limits."""

    def __init__(self):
        self._session_costs: dict[str, float] = {}
        self._request_counts: dict[str, int] = {}

    async def log_request(
        self,
        session: AsyncSession,
        provider: str,
        endpoint: str,
        entity_type: str | None = None,
        entity_id: int | None = None,
        request_params: dict[str, Any] | None = None,
        success: bool = True,
        status_code: int | None = None,
        response_data: dict[str, Any] | None = None,
        error_message: str | None = None,
        request_time_ms: int | None = None,
        cost_override: float | None = None,
    ) -> EnrichmentLog:
        """Log an enrichment API request."""
        # Calculate cost
        cost = cost_override if cost_override is not None else PROVIDER_COSTS.get(provider, 0.0)

        # Create log entry
        log = EnrichmentLog(
            provider=provider,
            endpoint=endpoint,
            entity_type=entity_type,
            entity_id=entity_id,
            request_params=request_params or {},
            success=success,
            status_code=status_code,
            response_data=response_data or {},
            error_message=error_message,
            cost_usd=cost if success else 0.0,  # Only charge for successful requests
            request_time_ms=request_time_ms,
        )

        session.add(log)

        # Track in-memory for session
        if success:
            self._session_costs[provider] = self._session_costs.get(provider, 0.0) + cost
            self._request_counts[provider] = self._request_counts.get(provider, 0) + 1

        # Update cost summary
        await self._update_cost_summary(session, provider, success, cost if success else 0.0)

        return log

    async def _update_cost_summary(
        self,
        session: AsyncSession,
        provider: str,
        success: bool,
        cost: float,
    ) -> None:
        """Update monthly cost summary for a provider."""
        now = datetime.utcnow()
        year = now.year
        month = now.month

        # Try to find existing summary
        result = await session.execute(
            select(CostSummary).where(
                CostSummary.provider == provider,
                CostSummary.year == year,
                CostSummary.month == month,
            )
        )
        summary = result.scalar_one_or_none()

        if summary:
            summary.total_requests += 1
            if success:
                summary.successful_requests += 1
                summary.total_cost_usd += cost
            else:
                summary.failed_requests += 1
        else:
            summary = CostSummary(
                provider=provider,
                year=year,
                month=month,
                total_requests=1,
                successful_requests=1 if success else 0,
                failed_requests=0 if success else 1,
                total_cost_usd=cost if success else 0.0,
            )
            session.add(summary)

    async def get_monthly_spend(self, session: AsyncSession) -> float:
        """Get total spend for current month."""
        now = datetime.utcnow()
        year = now.year
        month = now.month

        result = await session.execute(
            select(func.sum(CostSummary.total_cost_usd)).where(
                CostSummary.year == year,
                CostSummary.month == month,
            )
        )
        total = result.scalar_one_or_none()
        return total or 0.0

    async def get_provider_spend(
        self, session: AsyncSession, provider: str
    ) -> dict[str, Any]:
        """Get spend details for a specific provider."""
        now = datetime.utcnow()
        year = now.year
        month = now.month

        result = await session.execute(
            select(CostSummary).where(
                CostSummary.provider == provider,
                CostSummary.year == year,
                CostSummary.month == month,
            )
        )
        summary = result.scalar_one_or_none()

        if summary:
            return {
                "provider": provider,
                "total_requests": summary.total_requests,
                "successful_requests": summary.successful_requests,
                "failed_requests": summary.failed_requests,
                "total_cost_usd": summary.total_cost_usd,
                "success_rate": (
                    summary.successful_requests / summary.total_requests * 100
                    if summary.total_requests > 0
                    else 0.0
                ),
            }

        return {
            "provider": provider,
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "total_cost_usd": 0.0,
            "success_rate": 0.0,
        }

    async def get_budget_status(self, session: AsyncSession) -> dict[str, Any]:
        """Get current budget status."""
        monthly_budget = settings.budget.monthly_budget
        spent = await self.get_monthly_spend(session)
        remaining = monthly_budget - spent
        percentage_used = (spent / monthly_budget * 100) if monthly_budget > 0 else 0.0

        # Get breakdown by provider
        now = datetime.utcnow()
        result = await session.execute(
            select(CostSummary).where(
                CostSummary.year == now.year,
                CostSummary.month == now.month,
            )
        )
        summaries = result.scalars().all()

        breakdown = {s.provider: s.total_cost_usd for s in summaries}

        return {
            "monthly_budget": monthly_budget,
            "spent_this_month": round(spent, 4),
            "remaining": round(remaining, 4),
            "percentage_used": round(percentage_used, 2),
            "is_over_budget": spent >= monthly_budget,
            "alert_threshold_reached": percentage_used >= settings.budget.alert_threshold * 100,
            "breakdown_by_provider": breakdown,
        }

    async def check_budget(self, session: AsyncSession) -> bool:
        """Check if budget allows more requests. Returns True if OK."""
        if not settings.budget.hard_stop:
            return True

        spent = await self.get_monthly_spend(session)
        return spent < settings.budget.monthly_budget

    async def can_afford(
        self, session: AsyncSession, provider: str, count: int = 1
    ) -> bool:
        """Check if we can afford a specific number of requests."""
        if not settings.budget.hard_stop:
            return True

        cost_per_request = PROVIDER_COSTS.get(provider, 0.0)
        total_cost = cost_per_request * count

        spent = await self.get_monthly_spend(session)
        return (spent + total_cost) <= settings.budget.monthly_budget

    async def get_recent_logs(
        self,
        session: AsyncSession,
        provider: str | None = None,
        limit: int = 100,
    ) -> list[EnrichmentLog]:
        """Get recent enrichment logs."""
        query = select(EnrichmentLog).order_by(EnrichmentLog.created_at.desc())

        if provider:
            query = query.where(EnrichmentLog.provider == provider)

        query = query.limit(limit)
        result = await session.execute(query)
        return list(result.scalars().all())

    def get_session_stats(self) -> dict[str, Any]:
        """Get stats for the current session (in-memory)."""
        total_cost = sum(self._session_costs.values())
        total_requests = sum(self._request_counts.values())

        return {
            "session_total_cost": round(total_cost, 4),
            "session_total_requests": total_requests,
            "by_provider": {
                provider: {
                    "cost": round(cost, 4),
                    "requests": self._request_counts.get(provider, 0),
                }
                for provider, cost in self._session_costs.items()
            },
        }

    def reset_session_stats(self) -> None:
        """Reset in-memory session stats."""
        self._session_costs.clear()
        self._request_counts.clear()


# Global cost tracker instance
cost_tracker = CostTracker()


class BudgetExceeded(Exception):
    """Raised when budget is exceeded."""

    def __init__(self, message: str, spent: float, budget: float):
        super().__init__(message)
        self.spent = spent
        self.budget = budget
