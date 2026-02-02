"""Dropcontact enrichment provider (GDPR-compliant)."""

import asyncio
import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from core.schemas import CompanyEnrichment, ContactEnrichment
from enrichment.base import ContactEnrichmentProvider, EnrichmentResult

logger = logging.getLogger(__name__)


class DropcontactProvider(ContactEnrichmentProvider):
    """Dropcontact provider for GDPR-compliant contact enrichment."""

    PROVIDER_NAME = "dropcontact"
    BASE_URL = "https://api.dropcontact.io"
    COST_PER_REQUEST = 0.04

    def _get_headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Access-Token": self.api_key,
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
        Enrich contact using Dropcontact.
        Dropcontact is async - submits job and polls for results.
        """
        try:
            # Build request data
            contact_data: dict[str, Any] = {}

            if email:
                contact_data["email"] = email
            if first_name:
                contact_data["first_name"] = first_name
            if last_name:
                contact_data["last_name"] = last_name
            if domain:
                contact_data["company"] = domain
            if linkedin_url:
                contact_data["linkedin"] = linkedin_url

            if not contact_data:
                return EnrichmentResult(
                    success=False,
                    error="Dropcontact requires at least one of: email, linkedin_url, or (name + domain)",
                    provider=self.PROVIDER_NAME,
                )

            # Submit enrichment request
            request_id = await self._submit_request([contact_data], session, contact_id)
            if not request_id:
                return EnrichmentResult(
                    success=False,
                    error="Failed to submit enrichment request",
                    provider=self.PROVIDER_NAME,
                )

            # Poll for results
            result = await self._poll_for_results(request_id, session)
            if not result:
                return EnrichmentResult(
                    success=False,
                    error="Enrichment request timed out",
                    provider=self.PROVIDER_NAME,
                )

            # Parse first contact from results
            contacts = result.get("data", [])
            if not contacts:
                return EnrichmentResult(
                    success=False,
                    error="No contact data returned",
                    provider=self.PROVIDER_NAME,
                    raw_response=result,
                )

            enrichment = self._parse_contact_response(contacts[0])
            return EnrichmentResult(
                success=True,
                data=enrichment,
                raw_response=result,
                provider=self.PROVIDER_NAME,
                cost=self.COST_PER_REQUEST,
            )

        except Exception as e:
            logger.error(f"Dropcontact contact enrichment error: {e}")
            return EnrichmentResult(
                success=False,
                error=str(e),
                provider=self.PROVIDER_NAME,
            )

    async def _submit_request(
        self,
        contacts: list[dict[str, Any]],
        session: AsyncSession | None = None,
        contact_id: int | None = None,
    ) -> str | None:
        """Submit enrichment request to Dropcontact."""
        response = await self._make_request(
            method="POST",
            endpoint="/batch",
            json_data={
                "data": contacts,
                "siren": True,  # Include French company ID
                "language": "en",
            },
            session=session,
            entity_type="contact",
            entity_id=contact_id,
        )

        return response.get("request_id")

    async def _poll_for_results(
        self,
        request_id: str,
        session: AsyncSession | None = None,
        max_attempts: int = 30,
        poll_interval: float = 2.0,
    ) -> dict[str, Any] | None:
        """Poll for enrichment results."""
        for _ in range(max_attempts):
            response = await self._make_request(
                method="GET",
                endpoint=f"/batch/{request_id}",
                session=session,
                entity_type="poll",
            )

            if response.get("success") and response.get("data"):
                return response

            # Check if still processing
            if response.get("error") == "Request is being processed":
                await asyncio.sleep(poll_interval)
                continue

            # Check for error
            if response.get("error"):
                logger.error(f"Dropcontact error: {response.get('error')}")
                return None

            await asyncio.sleep(poll_interval)

        logger.warning(f"Dropcontact polling timed out for request {request_id}")
        return None

    def _parse_contact_response(self, data: dict[str, Any]) -> ContactEnrichment:
        """Parse Dropcontact response to ContactEnrichment."""
        title = data.get("job")
        normalized_title, seniority, department = self._normalize_title(title)

        # Get email (Dropcontact may return multiple)
        email = data.get("email")
        if isinstance(email, list) and email:
            email = email[0].get("email") if isinstance(email[0], dict) else email[0]

        # Get phone
        phone = data.get("phone")
        mobile_phone = data.get("mobile_phone")

        # Parse LinkedIn URL
        linkedin = data.get("linkedin")
        linkedin_url = None
        if linkedin:
            if linkedin.startswith("http"):
                linkedin_url = linkedin
            else:
                linkedin_url = f"https://linkedin.com/in/{linkedin}"

        return ContactEnrichment(
            first_name=data.get("first_name"),
            last_name=data.get("last_name"),
            full_name=data.get("full_name"),
            title=title,
            normalized_title=normalized_title,
            seniority_level=seniority or data.get("seniority"),
            department=department,
            email=email,
            mobile_phone=mobile_phone or phone,
            work_phone=phone if mobile_phone else None,
            linkedin_url=linkedin_url,
        )

    async def bulk_enrich(
        self,
        contacts: list[dict[str, Any]],
        session: AsyncSession | None = None,
    ) -> list[ContactEnrichment]:
        """
        Bulk enrich multiple contacts.
        More efficient than single requests for large batches.

        Each contact dict should have some combination of:
        - email
        - first_name + last_name + company
        - linkedin
        """
        try:
            # Submit batch request
            request_id = await self._submit_request(contacts, session)
            if not request_id:
                logger.error("Failed to submit bulk enrichment request")
                return []

            # Poll for results
            result = await self._poll_for_results(request_id, session)
            if not result:
                return []

            # Parse all contacts
            return [
                self._parse_contact_response(c)
                for c in result.get("data", [])
            ]

        except Exception as e:
            logger.error(f"Dropcontact bulk enrichment error: {e}")
            return []

    async def verify_email(
        self,
        email: str,
        session: AsyncSession | None = None,
    ) -> dict[str, Any]:
        """
        Verify an email address using Dropcontact.
        Dropcontact includes email verification in enrichment.
        """
        try:
            request_id = await self._submit_request(
                [{"email": email}],
                session,
            )
            if not request_id:
                return {"email": email, "status": "error", "error": "Failed to submit"}

            result = await self._poll_for_results(request_id, session)
            if not result:
                return {"email": email, "status": "error", "error": "Timeout"}

            contacts = result.get("data", [])
            if not contacts:
                return {"email": email, "status": "unknown"}

            contact = contacts[0]
            email_data = contact.get("email", {})

            # Dropcontact returns email qualification
            qualification = email_data.get("qualification") if isinstance(email_data, dict) else None

            return {
                "email": email,
                "status": qualification or "unknown",
                "is_valid": qualification in ["valid", "professional"],
                "is_professional": email_data.get("is_pro") if isinstance(email_data, dict) else None,
            }

        except Exception as e:
            logger.error(f"Dropcontact email verification error: {e}")
            return {"email": email, "status": "error", "error": str(e)}
