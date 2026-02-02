"""Abstract base class for enrichment providers."""

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from core.schemas import CompanyEnrichment, ContactEnrichment, EmailVerificationResult
from utils.cost_tracker import cost_tracker
from utils.rate_limiter import RateLimitError, rate_limiter

logger = logging.getLogger(__name__)

T = TypeVar("T", CompanyEnrichment, ContactEnrichment, EmailVerificationResult)


@dataclass
class EnrichmentResult(Generic[T]):
    """Result of an enrichment operation."""

    success: bool
    data: T | None = None
    raw_response: dict[str, Any] | None = None
    error: str | None = None
    provider: str = ""
    cost: float = 0.0
    request_time_ms: int = 0


class BaseEnrichmentProvider(ABC):
    """Abstract base class for enrichment providers."""

    # Class attributes to be set by subclasses
    PROVIDER_NAME: str = "base"
    BASE_URL: str = ""
    COST_PER_REQUEST: float = 0.0

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._client: httpx.AsyncClient | None = None

    @property
    def client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.BASE_URL,
                headers=self._get_headers(),
                timeout=30.0,
            )
        return self._client

    def _get_headers(self) -> dict[str, str]:
        """Get default headers for API requests."""
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
        session: AsyncSession | None = None,
        entity_type: str | None = None,
        entity_id: int | None = None,
    ) -> dict[str, Any]:
        """Make an API request with rate limiting and cost tracking."""
        # Acquire rate limit
        if not await rate_limiter.acquire(self.PROVIDER_NAME):
            raise RateLimitError(f"Rate limit exceeded for {self.PROVIDER_NAME}")

        start_time = time.monotonic()
        success = False
        status_code = None
        response_data: dict[str, Any] = {}
        error_message: str | None = None

        try:
            response = await self.client.request(
                method=method,
                url=endpoint,
                params=params,
                json=json_data,
            )
            status_code = response.status_code

            # Check for rate limit response
            if status_code == 429:
                retry_after = float(response.headers.get("Retry-After", "60"))
                raise RateLimitError(
                    f"Rate limited by {self.PROVIDER_NAME}", retry_after=retry_after
                )

            response.raise_for_status()
            response_data = response.json()
            success = True

        except httpx.HTTPStatusError as e:
            error_message = f"HTTP {e.response.status_code}: {e.response.text[:200]}"
            logger.error(f"{self.PROVIDER_NAME} API error: {error_message}")
            raise

        except httpx.RequestError as e:
            error_message = str(e)
            logger.error(f"{self.PROVIDER_NAME} request error: {error_message}")
            raise

        finally:
            elapsed_ms = int((time.monotonic() - start_time) * 1000)

            # Log to database if session provided
            if session:
                await cost_tracker.log_request(
                    session=session,
                    provider=self.PROVIDER_NAME,
                    endpoint=endpoint,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    request_params=params or json_data,
                    success=success,
                    status_code=status_code,
                    response_data=response_data if success else None,
                    error_message=error_message,
                    request_time_ms=elapsed_ms,
                    cost_override=self.COST_PER_REQUEST,
                )

        return response_data

    @abstractmethod
    async def enrich_company(
        self,
        domain: str,
        session: AsyncSession | None = None,
        company_id: int | None = None,
    ) -> EnrichmentResult[CompanyEnrichment]:
        """Enrich company data by domain."""
        pass

    @abstractmethod
    async def enrich_contact(
        self,
        email: str | None = None,
        linkedin_url: str | None = None,
        first_name: str | None = None,
        last_name: str | None = None,
        domain: str | None = None,
        session: AsyncSession | None = None,
        contact_id: int | None = None,
    ) -> EnrichmentResult[ContactEnrichment]:
        """Enrich contact data."""
        pass

    def _normalize_title(self, title: str | None) -> tuple[str | None, str | None, str | None]:
        """
        Normalize a job title to extract seniority and department.
        Returns: (normalized_title, seniority_level, department)
        """
        if not title:
            return None, None, None

        title_lower = title.lower()

        # Seniority mapping
        seniority = None
        if any(x in title_lower for x in ["chief", "c-level", "ceo", "cto", "cfo", "cmo", "cro", "coo"]):
            seniority = "c_level"
        elif any(x in title_lower for x in ["vp", "vice president"]):
            seniority = "vp"
        elif any(x in title_lower for x in ["director"]):
            seniority = "director"
        elif any(x in title_lower for x in ["head of", "head,"]):
            seniority = "head"
        elif any(x in title_lower for x in ["manager", "lead"]):
            seniority = "manager"
        elif any(x in title_lower for x in ["senior", "sr."]):
            seniority = "senior"
        elif any(x in title_lower for x in ["junior", "jr.", "associate"]):
            seniority = "junior"
        else:
            seniority = "individual_contributor"

        # Department mapping
        department = None
        if any(x in title_lower for x in ["sales", "revenue", "business development", "account"]):
            department = "sales"
        elif any(x in title_lower for x in ["marketing", "growth", "brand", "content", "seo", "demand"]):
            department = "marketing"
        elif any(x in title_lower for x in ["engineering", "developer", "software", "tech", "devops"]):
            department = "engineering"
        elif any(x in title_lower for x in ["product", "pm "]):
            department = "product"
        elif any(x in title_lower for x in ["design", "ux", "ui", "creative"]):
            department = "design"
        elif any(x in title_lower for x in ["hr", "human", "people", "talent", "recruiting"]):
            department = "hr"
        elif any(x in title_lower for x in ["finance", "accounting", "controller"]):
            department = "finance"
        elif any(x in title_lower for x in ["operations", "ops", "logistics"]):
            department = "operations"
        elif any(x in title_lower for x in ["customer", "success", "support", "service"]):
            department = "customer_success"
        elif any(x in title_lower for x in ["legal", "compliance", "counsel"]):
            department = "legal"

        # Normalize title (capitalize first letters)
        normalized = title.title()

        return normalized, seniority, department


class CompanyEnrichmentProvider(BaseEnrichmentProvider):
    """Provider that only supports company enrichment."""

    async def enrich_contact(
        self,
        email: str | None = None,
        linkedin_url: str | None = None,
        first_name: str | None = None,
        last_name: str | None = None,
        domain: str | None = None,
        session: AsyncSession | None = None,
        contact_id: int | None = None,
    ) -> EnrichmentResult[ContactEnrichment]:
        """Not supported by this provider."""
        return EnrichmentResult(
            success=False,
            error=f"{self.PROVIDER_NAME} does not support contact enrichment",
            provider=self.PROVIDER_NAME,
        )


class ContactEnrichmentProvider(BaseEnrichmentProvider):
    """Provider that only supports contact enrichment."""

    async def enrich_company(
        self,
        domain: str,
        session: AsyncSession | None = None,
        company_id: int | None = None,
    ) -> EnrichmentResult[CompanyEnrichment]:
        """Not supported by this provider."""
        return EnrichmentResult(
            success=False,
            error=f"{self.PROVIDER_NAME} does not support company enrichment",
            provider=self.PROVIDER_NAME,
        )


class EmailVerificationProvider(BaseEnrichmentProvider):
    """Provider that only supports email verification."""

    async def enrich_company(
        self,
        domain: str,
        session: AsyncSession | None = None,
        company_id: int | None = None,
    ) -> EnrichmentResult[CompanyEnrichment]:
        """Not supported by this provider."""
        return EnrichmentResult(
            success=False,
            error=f"{self.PROVIDER_NAME} does not support company enrichment",
            provider=self.PROVIDER_NAME,
        )

    async def enrich_contact(
        self,
        email: str | None = None,
        linkedin_url: str | None = None,
        first_name: str | None = None,
        last_name: str | None = None,
        domain: str | None = None,
        session: AsyncSession | None = None,
        contact_id: int | None = None,
    ) -> EnrichmentResult[ContactEnrichment]:
        """Not supported by this provider."""
        return EnrichmentResult(
            success=False,
            error=f"{self.PROVIDER_NAME} does not support contact enrichment",
            provider=self.PROVIDER_NAME,
        )

    @abstractmethod
    async def verify_email(
        self,
        email: str,
        session: AsyncSession | None = None,
        contact_id: int | None = None,
    ) -> EnrichmentResult[EmailVerificationResult]:
        """Verify an email address."""
        pass
