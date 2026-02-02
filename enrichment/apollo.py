"""Apollo.io enrichment provider."""

import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from core.schemas import CompanyEnrichment, ContactEnrichment
from enrichment.base import BaseEnrichmentProvider, EnrichmentResult

logger = logging.getLogger(__name__)


class ApolloProvider(BaseEnrichmentProvider):
    """Apollo.io enrichment provider for company and contact data."""

    PROVIDER_NAME = "apollo"
    BASE_URL = "https://api.apollo.io/v1"
    COST_PER_REQUEST = 0.03

    def _get_headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
        }

    async def enrich_company(
        self,
        domain: str,
        session: AsyncSession | None = None,
        company_id: int | None = None,
    ) -> EnrichmentResult[CompanyEnrichment]:
        """Enrich company data using Apollo.io organization endpoint."""
        try:
            response = await self._make_request(
                method="POST",
                endpoint="/organizations/enrich",
                json_data={
                    "api_key": self.api_key,
                    "domain": domain,
                },
                session=session,
                entity_type="company",
                entity_id=company_id,
            )

            org = response.get("organization", {})
            if not org:
                return EnrichmentResult(
                    success=False,
                    error="No organization data returned",
                    provider=self.PROVIDER_NAME,
                    raw_response=response,
                )

            enrichment = self._parse_company_response(org)
            return EnrichmentResult(
                success=True,
                data=enrichment,
                raw_response=response,
                provider=self.PROVIDER_NAME,
                cost=self.COST_PER_REQUEST,
            )

        except Exception as e:
            logger.error(f"Apollo company enrichment error: {e}")
            return EnrichmentResult(
                success=False,
                error=str(e),
                provider=self.PROVIDER_NAME,
            )

    def _parse_company_response(self, org: dict[str, Any]) -> CompanyEnrichment:
        """Parse Apollo organization response to CompanyEnrichment."""
        # Parse tech stack
        tech_stack = {}
        technologies = org.get("technologies", []) or []
        for tech in technologies:
            if isinstance(tech, dict):
                category = tech.get("category", "other")
                name = tech.get("name", "")
                if category not in tech_stack:
                    tech_stack[category] = []
                tech_stack[category].append(name)
            elif isinstance(tech, str):
                if "other" not in tech_stack:
                    tech_stack["other"] = []
                tech_stack["other"].append(tech)

        # Detect specific platforms
        all_tech = [t.lower() for t in (org.get("technologies_names", []) or [])]
        crm_platform = None
        marketing_automation = None

        crm_keywords = ["salesforce", "hubspot", "pipedrive", "zoho crm", "dynamics"]
        for crm in crm_keywords:
            if any(crm in t for t in all_tech):
                crm_platform = crm.title()
                break

        ma_keywords = ["marketo", "hubspot", "pardot", "mailchimp", "klaviyo", "braze"]
        for ma in ma_keywords:
            if any(ma in t for t in all_tech):
                marketing_automation = ma.title()
                break

        return CompanyEnrichment(
            industry=org.get("industry"),
            sub_industry=org.get("subindustry"),
            employee_count=org.get("estimated_num_employees"),
            employee_range=org.get("employee_count_range"),
            revenue=self._parse_revenue(org.get("annual_revenue")),
            revenue_range=org.get("annual_revenue_printed"),
            founded_year=org.get("founded_year"),
            hq_city=org.get("city"),
            hq_state=org.get("state"),
            hq_country=org.get("country"),
            total_funding=org.get("total_funding"),
            last_funding_date=None,  # Would need to parse from funding_rounds
            last_funding_amount=org.get("latest_funding_amount"),
            last_funding_type=org.get("latest_funding_round_type"),
            funding_stage=org.get("funding_stage"),
            tech_stack=tech_stack,
            crm_platform=crm_platform,
            marketing_automation=marketing_automation,
            is_hiring=bool(org.get("job_postings")),
            open_positions=len(org.get("job_postings", []) or []),
            linkedin_url=org.get("linkedin_url"),
            twitter_url=org.get("twitter_url"),
            website_description=org.get("short_description"),
        )

    def _parse_revenue(self, revenue: Any) -> float | None:
        """Parse revenue from various formats."""
        if revenue is None:
            return None
        if isinstance(revenue, (int, float)):
            return float(revenue)
        if isinstance(revenue, str):
            # Remove currency symbols and convert
            cleaned = revenue.replace("$", "").replace(",", "").strip()
            multipliers = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}
            for suffix, mult in multipliers.items():
                if cleaned.lower().endswith(suffix):
                    try:
                        return float(cleaned[:-1]) * mult
                    except ValueError:
                        return None
            try:
                return float(cleaned)
            except ValueError:
                return None
        return None

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
        """Enrich contact data using Apollo.io people endpoint."""
        try:
            # Build request params
            params: dict[str, Any] = {"api_key": self.api_key}

            if email:
                params["email"] = email
            if linkedin_url:
                params["linkedin_url"] = linkedin_url
            if first_name:
                params["first_name"] = first_name
            if last_name:
                params["last_name"] = last_name
            if domain:
                params["organization_domain"] = domain

            response = await self._make_request(
                method="POST",
                endpoint="/people/match",
                json_data=params,
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
            logger.error(f"Apollo contact enrichment error: {e}")
            return EnrichmentResult(
                success=False,
                error=str(e),
                provider=self.PROVIDER_NAME,
            )

    def _parse_contact_response(self, person: dict[str, Any]) -> ContactEnrichment:
        """Parse Apollo person response to ContactEnrichment."""
        title = person.get("title")
        normalized_title, seniority, department = self._normalize_title(title)

        # Get phone numbers
        phone_numbers = person.get("phone_numbers", []) or []
        mobile_phone = None
        work_phone = None
        for phone in phone_numbers:
            if isinstance(phone, dict):
                phone_type = phone.get("type", "").lower()
                number = phone.get("sanitized_number") or phone.get("raw_number")
                if "mobile" in phone_type and not mobile_phone:
                    mobile_phone = number
                elif not work_phone:
                    work_phone = number

        return ContactEnrichment(
            first_name=person.get("first_name"),
            last_name=person.get("last_name"),
            full_name=person.get("name"),
            title=title,
            normalized_title=normalized_title,
            seniority_level=seniority,
            department=department,
            email=person.get("email"),
            mobile_phone=mobile_phone,
            work_phone=work_phone,
            linkedin_url=person.get("linkedin_url"),
            twitter_url=person.get("twitter_url"),
        )

    async def search_people(
        self,
        domain: str,
        titles: list[str] | None = None,
        seniority_levels: list[str] | None = None,
        limit: int = 10,
        session: AsyncSession | None = None,
    ) -> list[ContactEnrichment]:
        """Search for people at a company with optional filters."""
        try:
            params: dict[str, Any] = {
                "api_key": self.api_key,
                "organization_domains": [domain],
                "page": 1,
                "per_page": limit,
            }

            if titles:
                params["person_titles"] = titles
            if seniority_levels:
                params["person_seniorities"] = seniority_levels

            response = await self._make_request(
                method="POST",
                endpoint="/mixed_people/search",
                json_data=params,
                session=session,
                entity_type="search",
            )

            people = response.get("people", [])
            return [self._parse_contact_response(p) for p in people]

        except Exception as e:
            logger.error(f"Apollo people search error: {e}")
            return []
