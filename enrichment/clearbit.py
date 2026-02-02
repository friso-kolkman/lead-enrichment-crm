"""Clearbit enrichment provider."""

import base64
import logging
from datetime import datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from core.schemas import CompanyEnrichment, ContactEnrichment
from enrichment.base import BaseEnrichmentProvider, EnrichmentResult

logger = logging.getLogger(__name__)


class ClearbitProvider(BaseEnrichmentProvider):
    """Clearbit enrichment provider for company data."""

    PROVIDER_NAME = "clearbit"
    BASE_URL = "https://company.clearbit.com/v2"
    COST_PER_REQUEST = 0.10

    def _get_headers(self) -> dict[str, str]:
        # Clearbit uses basic auth with API key as username
        auth = base64.b64encode(f"{self.api_key}:".encode()).decode()
        return {
            "Accept": "application/json",
            "Authorization": f"Basic {auth}",
        }

    async def enrich_company(
        self,
        domain: str,
        session: AsyncSession | None = None,
        company_id: int | None = None,
    ) -> EnrichmentResult[CompanyEnrichment]:
        """Enrich company data using Clearbit Company API."""
        try:
            response = await self._make_request(
                method="GET",
                endpoint=f"/companies/find?domain={domain}",
                session=session,
                entity_type="company",
                entity_id=company_id,
            )

            if not response:
                return EnrichmentResult(
                    success=False,
                    error="No company data returned",
                    provider=self.PROVIDER_NAME,
                    raw_response=response,
                )

            enrichment = self._parse_company_response(response)
            return EnrichmentResult(
                success=True,
                data=enrichment,
                raw_response=response,
                provider=self.PROVIDER_NAME,
                cost=self.COST_PER_REQUEST,
            )

        except Exception as e:
            logger.error(f"Clearbit company enrichment error: {e}")
            return EnrichmentResult(
                success=False,
                error=str(e),
                provider=self.PROVIDER_NAME,
            )

    def _parse_company_response(self, data: dict[str, Any]) -> CompanyEnrichment:
        """Parse Clearbit company response to CompanyEnrichment."""
        # Parse tech stack
        tech_stack = {}
        tech = data.get("tech", []) or []
        for t in tech:
            if "other" not in tech_stack:
                tech_stack["other"] = []
            tech_stack["other"].append(t)

        # Parse category for industry
        category = data.get("category", {}) or {}
        industry = category.get("industry")
        sub_industry = category.get("subIndustry")

        # Parse metrics
        metrics = data.get("metrics", {}) or {}

        # Parse geo
        geo = data.get("geo", {}) or {}

        # Parse funding
        crunchbase = data.get("crunchbase", {}) or {}

        # Detect CRM and marketing automation
        all_tech = [t.lower() for t in tech]
        crm_platform = None
        marketing_automation = None

        crm_keywords = ["salesforce", "hubspot", "pipedrive", "zoho", "dynamics"]
        for crm in crm_keywords:
            if any(crm in t for t in all_tech):
                crm_platform = crm.title()
                break

        ma_keywords = ["marketo", "hubspot", "pardot", "mailchimp", "klaviyo", "braze"]
        for ma in ma_keywords:
            if any(ma in t for t in all_tech):
                marketing_automation = ma.title()
                break

        # Parse funding date
        last_funding_date = None
        if crunchbase.get("lastFundingDate"):
            try:
                last_funding_date = datetime.fromisoformat(
                    crunchbase["lastFundingDate"].replace("Z", "+00:00")
                )
            except (ValueError, AttributeError):
                pass

        return CompanyEnrichment(
            industry=industry,
            sub_industry=sub_industry,
            employee_count=metrics.get("employees"),
            employee_range=metrics.get("employeesRange"),
            revenue=metrics.get("estimatedAnnualRevenue"),
            founded_year=data.get("foundedYear"),
            hq_city=geo.get("city"),
            hq_state=geo.get("state"),
            hq_country=geo.get("country"),
            hq_region=geo.get("subRegion"),
            total_funding=crunchbase.get("totalFunding"),
            last_funding_date=last_funding_date,
            last_funding_amount=crunchbase.get("lastFundingAmount"),
            last_funding_type=crunchbase.get("lastFundingType"),
            tech_stack=tech_stack if tech_stack else None,
            crm_platform=crm_platform,
            marketing_automation=marketing_automation,
            linkedin_url=data.get("linkedin", {}).get("handle"),
            twitter_url=data.get("twitter", {}).get("handle"),
            website_description=data.get("description"),
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
        """
        Enrich contact using Clearbit Person API.
        Note: Clearbit person enrichment requires email.
        """
        if not email:
            return EnrichmentResult(
                success=False,
                error="Clearbit requires email for contact enrichment",
                provider=self.PROVIDER_NAME,
            )

        try:
            # Use combined endpoint for person + company
            response = await self._make_request(
                method="GET",
                endpoint=f"https://person.clearbit.com/v2/combined/find?email={email}",
                session=session,
                entity_type="contact",
                entity_id=contact_id,
            )

            person = response.get("person", {})
            if not person:
                return EnrichmentResult(
                    success=False,
                    error="No person data returned",
                    provider=self.PROVIDER_NAME,
                    raw_response=response,
                )

            enrichment = self._parse_contact_response(person)
            return EnrichmentResult(
                success=True,
                data=enrichment,
                raw_response=response,
                provider=self.PROVIDER_NAME,
                cost=self.COST_PER_REQUEST,
            )

        except Exception as e:
            logger.error(f"Clearbit contact enrichment error: {e}")
            return EnrichmentResult(
                success=False,
                error=str(e),
                provider=self.PROVIDER_NAME,
            )

    def _parse_contact_response(self, person: dict[str, Any]) -> ContactEnrichment:
        """Parse Clearbit person response to ContactEnrichment."""
        name = person.get("name", {}) or {}
        employment = person.get("employment", {}) or {}

        title = employment.get("title")
        normalized_title, seniority, department = self._normalize_title(title)

        # Build social URLs
        linkedin = person.get("linkedin", {}) or {}
        twitter = person.get("twitter", {}) or {}

        linkedin_url = None
        if linkedin.get("handle"):
            linkedin_url = f"https://linkedin.com/in/{linkedin['handle']}"

        twitter_url = None
        if twitter.get("handle"):
            twitter_url = f"https://twitter.com/{twitter['handle']}"

        return ContactEnrichment(
            first_name=name.get("givenName"),
            last_name=name.get("familyName"),
            full_name=name.get("fullName"),
            title=title,
            normalized_title=normalized_title,
            seniority_level=seniority or employment.get("seniority"),
            department=department or employment.get("role"),
            email=person.get("email"),
            linkedin_url=linkedin_url,
            twitter_url=twitter_url,
        )
