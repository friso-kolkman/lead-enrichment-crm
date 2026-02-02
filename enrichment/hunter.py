"""Hunter.io enrichment provider."""

import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from core.schemas import CompanyEnrichment, ContactEnrichment
from enrichment.base import ContactEnrichmentProvider, EnrichmentResult

logger = logging.getLogger(__name__)


class HunterProvider(ContactEnrichmentProvider):
    """Hunter.io provider for email finding and verification."""

    PROVIDER_NAME = "hunter"
    BASE_URL = "https://api.hunter.io/v2"
    COST_PER_REQUEST = 0.01

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
        Enrich contact using Hunter.io.
        Can find email by name + domain, or enrich existing email.
        """
        try:
            # If we have an email, use email finder for verification/enrichment
            if email:
                return await self._enrich_by_email(email, session, contact_id)

            # If we have name + domain, find email
            if first_name and last_name and domain:
                return await self._find_email(
                    first_name, last_name, domain, session, contact_id
                )

            return EnrichmentResult(
                success=False,
                error="Hunter requires email OR (first_name + last_name + domain)",
                provider=self.PROVIDER_NAME,
            )

        except Exception as e:
            logger.error(f"Hunter contact enrichment error: {e}")
            return EnrichmentResult(
                success=False,
                error=str(e),
                provider=self.PROVIDER_NAME,
            )

    async def _enrich_by_email(
        self,
        email: str,
        session: AsyncSession | None = None,
        contact_id: int | None = None,
    ) -> EnrichmentResult[ContactEnrichment]:
        """Look up contact data by email."""
        response = await self._make_request(
            method="GET",
            endpoint="/email-finder",
            params={
                "api_key": self.api_key,
                "email": email,
            },
            session=session,
            entity_type="contact",
            entity_id=contact_id,
        )

        data = response.get("data", {})
        if not data:
            return EnrichmentResult(
                success=False,
                error="No data returned for email",
                provider=self.PROVIDER_NAME,
                raw_response=response,
            )

        enrichment = self._parse_response(data)
        return EnrichmentResult(
            success=True,
            data=enrichment,
            raw_response=response,
            provider=self.PROVIDER_NAME,
            cost=self.COST_PER_REQUEST,
        )

    async def _find_email(
        self,
        first_name: str,
        last_name: str,
        domain: str,
        session: AsyncSession | None = None,
        contact_id: int | None = None,
    ) -> EnrichmentResult[ContactEnrichment]:
        """Find email by name and domain."""
        response = await self._make_request(
            method="GET",
            endpoint="/email-finder",
            params={
                "api_key": self.api_key,
                "domain": domain,
                "first_name": first_name,
                "last_name": last_name,
            },
            session=session,
            entity_type="contact",
            entity_id=contact_id,
        )

        data = response.get("data", {})
        if not data or not data.get("email"):
            return EnrichmentResult(
                success=False,
                error="No email found for this person",
                provider=self.PROVIDER_NAME,
                raw_response=response,
            )

        enrichment = self._parse_response(data)
        return EnrichmentResult(
            success=True,
            data=enrichment,
            raw_response=response,
            provider=self.PROVIDER_NAME,
            cost=self.COST_PER_REQUEST,
        )

    def _parse_response(self, data: dict[str, Any]) -> ContactEnrichment:
        """Parse Hunter response to ContactEnrichment."""
        title = data.get("position")
        normalized_title, seniority, department = self._normalize_title(title)

        # Build LinkedIn URL if handle provided
        linkedin_url = None
        if data.get("linkedin_url"):
            linkedin_url = data["linkedin_url"]
        elif data.get("linkedin"):
            linkedin_url = f"https://linkedin.com/in/{data['linkedin']}"

        return ContactEnrichment(
            first_name=data.get("first_name"),
            last_name=data.get("last_name"),
            full_name=f"{data.get('first_name', '')} {data.get('last_name', '')}".strip() or None,
            title=title,
            normalized_title=normalized_title,
            seniority_level=seniority,
            department=department or data.get("department"),
            email=data.get("email"),
            mobile_phone=data.get("phone_number"),
            linkedin_url=linkedin_url,
            twitter_url=data.get("twitter"),
        )

    async def domain_search(
        self,
        domain: str,
        limit: int = 10,
        department: str | None = None,
        seniority: str | None = None,
        session: AsyncSession | None = None,
    ) -> list[ContactEnrichment]:
        """
        Search for all contacts at a domain.
        Returns list of contacts with emails.
        """
        try:
            params: dict[str, Any] = {
                "api_key": self.api_key,
                "domain": domain,
                "limit": limit,
            }

            if department:
                params["department"] = department
            if seniority:
                params["seniority"] = seniority

            response = await self._make_request(
                method="GET",
                endpoint="/domain-search",
                params=params,
                session=session,
                entity_type="search",
            )

            emails = response.get("data", {}).get("emails", [])
            return [self._parse_response(e) for e in emails]

        except Exception as e:
            logger.error(f"Hunter domain search error: {e}")
            return []

    async def verify_email(
        self,
        email: str,
        session: AsyncSession | None = None,
    ) -> dict[str, Any]:
        """
        Verify an email address.
        Returns verification status and details.
        """
        try:
            response = await self._make_request(
                method="GET",
                endpoint="/email-verifier",
                params={
                    "api_key": self.api_key,
                    "email": email,
                },
                session=session,
                entity_type="verification",
            )

            data = response.get("data", {})
            return {
                "email": email,
                "status": data.get("status"),  # valid, invalid, accept_all, webmail, disposable, unknown
                "result": data.get("result"),  # deliverable, undeliverable, risky
                "score": data.get("score"),  # 0-100
                "regexp": data.get("regexp"),
                "gibberish": data.get("gibberish"),
                "disposable": data.get("disposable"),
                "webmail": data.get("webmail"),
                "mx_records": data.get("mx_records"),
                "smtp_server": data.get("smtp_server"),
                "smtp_check": data.get("smtp_check"),
                "accept_all": data.get("accept_all"),
                "block": data.get("block"),
            }

        except Exception as e:
            logger.error(f"Hunter email verification error: {e}")
            return {
                "email": email,
                "status": "error",
                "error": str(e),
            }
