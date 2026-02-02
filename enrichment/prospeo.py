"""Prospeo enrichment provider."""

import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from core.schemas import CompanyEnrichment, ContactEnrichment
from enrichment.base import ContactEnrichmentProvider, EnrichmentResult

logger = logging.getLogger(__name__)


class ProspeoProvider(ContactEnrichmentProvider):
    """Prospeo provider for LinkedIn-based contact enrichment."""

    PROVIDER_NAME = "prospeo"
    BASE_URL = "https://api.prospeo.io"
    COST_PER_REQUEST = 0.05

    def _get_headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-KEY": self.api_key,
        }

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
        Enrich contact using Prospeo.
        Prioritizes LinkedIn URL, then falls back to email lookup.
        """
        try:
            # LinkedIn URL is the primary enrichment method for Prospeo
            if linkedin_url:
                return await self._enrich_by_linkedin(linkedin_url, session, contact_id)

            # Fall back to email finder if we have name + domain
            if first_name and last_name and domain:
                return await self._find_email(
                    first_name, last_name, domain, session, contact_id
                )

            # Can also enrich by email
            if email:
                return await self._enrich_by_email(email, session, contact_id)

            return EnrichmentResult(
                success=False,
                error="Prospeo requires linkedin_url, email, or (first_name + last_name + domain)",
                provider=self.PROVIDER_NAME,
            )

        except Exception as e:
            logger.error(f"Prospeo contact enrichment error: {e}")
            return EnrichmentResult(
                success=False,
                error=str(e),
                provider=self.PROVIDER_NAME,
            )

    async def _enrich_by_linkedin(
        self,
        linkedin_url: str,
        session: AsyncSession | None = None,
        contact_id: int | None = None,
    ) -> EnrichmentResult[ContactEnrichment]:
        """Enrich contact data from LinkedIn profile."""
        response = await self._make_request(
            method="POST",
            endpoint="/linkedin-email-finder",
            json_data={"url": linkedin_url},
            session=session,
            entity_type="contact",
            entity_id=contact_id,
        )

        if not response.get("success") or response.get("error"):
            return EnrichmentResult(
                success=False,
                error=response.get("message", "LinkedIn enrichment failed"),
                provider=self.PROVIDER_NAME,
                raw_response=response,
            )

        enrichment = self._parse_linkedin_response(response, linkedin_url)
        return EnrichmentResult(
            success=True,
            data=enrichment,
            raw_response=response,
            provider=self.PROVIDER_NAME,
            cost=self.COST_PER_REQUEST,
        )

    async def _enrich_by_email(
        self,
        email: str,
        session: AsyncSession | None = None,
        contact_id: int | None = None,
    ) -> EnrichmentResult[ContactEnrichment]:
        """Enrich contact data from email."""
        response = await self._make_request(
            method="POST",
            endpoint="/email-finder",
            json_data={"email": email},
            session=session,
            entity_type="contact",
            entity_id=contact_id,
        )

        if not response.get("success"):
            return EnrichmentResult(
                success=False,
                error=response.get("message", "Email enrichment failed"),
                provider=self.PROVIDER_NAME,
                raw_response=response,
            )

        enrichment = self._parse_email_response(response, email)
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
        """Find email by name and company domain."""
        response = await self._make_request(
            method="POST",
            endpoint="/email-finder",
            json_data={
                "first_name": first_name,
                "last_name": last_name,
                "company": domain,
            },
            session=session,
            entity_type="contact",
            entity_id=contact_id,
        )

        if not response.get("success") or not response.get("email"):
            return EnrichmentResult(
                success=False,
                error=response.get("message", "Email not found"),
                provider=self.PROVIDER_NAME,
                raw_response=response,
            )

        enrichment = ContactEnrichment(
            first_name=first_name,
            last_name=last_name,
            full_name=f"{first_name} {last_name}",
            email=response.get("email"),
        )

        return EnrichmentResult(
            success=True,
            data=enrichment,
            raw_response=response,
            provider=self.PROVIDER_NAME,
            cost=self.COST_PER_REQUEST,
        )

    def _parse_linkedin_response(
        self, response: dict[str, Any], linkedin_url: str
    ) -> ContactEnrichment:
        """Parse Prospeo LinkedIn enrichment response."""
        # Prospeo returns profile data along with email
        profile = response.get("profile", {}) or {}

        title = profile.get("title") or profile.get("headline")
        normalized_title, seniority, department = self._normalize_title(title)

        # Parse name
        first_name = profile.get("first_name") or response.get("first_name")
        last_name = profile.get("last_name") or response.get("last_name")
        full_name = profile.get("full_name")
        if not full_name and first_name and last_name:
            full_name = f"{first_name} {last_name}"

        return ContactEnrichment(
            first_name=first_name,
            last_name=last_name,
            full_name=full_name,
            title=title,
            normalized_title=normalized_title,
            seniority_level=seniority,
            department=department,
            email=response.get("email"),
            mobile_phone=profile.get("phone"),
            linkedin_url=linkedin_url,
            twitter_url=profile.get("twitter_url"),
        )

    def _parse_email_response(
        self, response: dict[str, Any], email: str
    ) -> ContactEnrichment:
        """Parse Prospeo email enrichment response."""
        profile = response.get("profile", {}) or {}

        title = profile.get("title")
        normalized_title, seniority, department = self._normalize_title(title)

        return ContactEnrichment(
            first_name=profile.get("first_name"),
            last_name=profile.get("last_name"),
            full_name=profile.get("full_name"),
            title=title,
            normalized_title=normalized_title,
            seniority_level=seniority,
            department=department,
            email=email,
            mobile_phone=profile.get("phone"),
            linkedin_url=profile.get("linkedin_url"),
        )

    async def bulk_linkedin_enrich(
        self,
        linkedin_urls: list[str],
        session: AsyncSession | None = None,
    ) -> list[EnrichmentResult[ContactEnrichment]]:
        """
        Bulk enrich contacts from LinkedIn URLs.
        More cost-effective for large batches.
        """
        results = []
        for url in linkedin_urls:
            result = await self._enrich_by_linkedin(url, session)
            results.append(result)
        return results
