"""Attio CRM REST client."""

import logging
from typing import Any

import httpx

from config import settings
from utils.rate_limiter import rate_limiter

logger = logging.getLogger(__name__)


class AttioClient:
    """Client for Attio CRM API."""

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or settings.attio.api_key
        self.base_url = settings.attio.base_url
        self._client: httpx.AsyncClient | None = None

    @property
    def client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                timeout=30.0,
            )
        return self._client

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _request(
        self,
        method: str,
        endpoint: str,
        json_data: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Make an API request with rate limiting."""
        await rate_limiter.acquire("attio")

        try:
            response = await self.client.request(
                method=method,
                url=endpoint,
                json=json_data,
                params=params,
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"Attio API error: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Attio request error: {e}")
            raise

    # Company operations

    async def upsert_company(
        self,
        domain: str,
        name: str | None = None,
        industry: str | None = None,
        employee_count: int | None = None,
        revenue: float | None = None,
        hq_city: str | None = None,
        hq_country: str | None = None,
        tech_stack: dict | list | None = None,
        is_hiring: bool | None = None,
        linkedin_url: str | None = None,
    ) -> str:
        """
        Create or update a company in Attio.

        Args:
            domain: Company domain (used as unique identifier)
            name: Company name
            industry: Industry
            employee_count: Number of employees
            revenue: Annual revenue
            hq_city: HQ city
            hq_country: HQ country
            tech_stack: Technology stack
            is_hiring: Whether actively hiring
            linkedin_url: LinkedIn URL

        Returns:
            Attio record ID
        """
        # Build attribute values
        values: dict[str, Any] = {
            "domains": [{"domain": domain}],
        }

        if name:
            values["name"] = name
        if industry:
            values["categories"] = [industry]
        if employee_count:
            values["employee_count"] = employee_count
        if hq_city or hq_country:
            location = {}
            if hq_city:
                location["city"] = hq_city
            if hq_country:
                location["country"] = hq_country
            values["primary_location"] = location
        if linkedin_url:
            values["linkedin"] = linkedin_url

        # Custom attributes (if configured in Attio)
        # These would need to be created in Attio first
        # values["revenue"] = revenue
        # values["is_hiring"] = is_hiring
        # values["tech_stack"] = tech_stack

        data = {
            "data": {
                "values": values,
            },
            "matching_attribute": "domains",
        }

        try:
            response = await self._request(
                method="PUT",
                endpoint="/objects/companies/records",
                json_data=data,
            )
            record_id = response.get("data", {}).get("id", {}).get("record_id", "")
            logger.debug(f"Upserted company {domain}: {record_id}")
            return record_id
        except Exception as e:
            logger.error(f"Failed to upsert company {domain}: {e}")
            raise

    async def get_company(self, record_id: str) -> dict[str, Any]:
        """Get a company by record ID."""
        return await self._request(
            method="GET",
            endpoint=f"/objects/companies/records/{record_id}",
        )

    async def search_companies(
        self,
        domain: str | None = None,
        name: str | None = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Search for companies."""
        filters = []
        if domain:
            filters.append({
                "attribute": "domains",
                "condition": "contains",
                "value": domain,
            })
        if name:
            filters.append({
                "attribute": "name",
                "condition": "contains",
                "value": name,
            })

        data = {
            "filter": {"and": filters} if len(filters) > 1 else filters[0] if filters else {},
            "limit": limit,
        }

        response = await self._request(
            method="POST",
            endpoint="/objects/companies/records/query",
            json_data=data,
        )
        return response.get("data", [])

    # Contact operations

    async def upsert_contact(
        self,
        email: str | None = None,
        first_name: str | None = None,
        last_name: str | None = None,
        title: str | None = None,
        phone: str | None = None,
        linkedin_url: str | None = None,
        company_record_id: str | None = None,
    ) -> str:
        """
        Create or update a contact (person) in Attio.

        Args:
            email: Email address (used as unique identifier)
            first_name: First name
            last_name: Last name
            title: Job title
            phone: Phone number
            linkedin_url: LinkedIn URL
            company_record_id: Associated company record ID

        Returns:
            Attio record ID
        """
        values: dict[str, Any] = {}

        if email:
            values["email_addresses"] = [{"email_address": email}]
        if first_name:
            values["first_name"] = first_name
        if last_name:
            values["last_name"] = last_name
        if title:
            values["job_title"] = title
        if phone:
            values["phone_numbers"] = [{"phone_number": phone}]
        if linkedin_url:
            values["linkedin"] = linkedin_url
        if company_record_id:
            values["company"] = [{"target_record_id": company_record_id}]

        data = {
            "data": {
                "values": values,
            },
            "matching_attribute": "email_addresses",
        }

        try:
            response = await self._request(
                method="PUT",
                endpoint="/objects/people/records",
                json_data=data,
            )
            record_id = response.get("data", {}).get("id", {}).get("record_id", "")
            logger.debug(f"Upserted contact {email}: {record_id}")
            return record_id
        except Exception as e:
            logger.error(f"Failed to upsert contact {email}: {e}")
            raise

    async def get_contact(self, record_id: str) -> dict[str, Any]:
        """Get a contact by record ID."""
        return await self._request(
            method="GET",
            endpoint=f"/objects/people/records/{record_id}",
        )

    async def search_contacts(
        self,
        email: str | None = None,
        name: str | None = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Search for contacts."""
        filters = []
        if email:
            filters.append({
                "attribute": "email_addresses",
                "condition": "contains",
                "value": email,
            })
        if name:
            filters.append({
                "attribute": "name",
                "condition": "contains",
                "value": name,
            })

        data = {
            "filter": {"and": filters} if len(filters) > 1 else filters[0] if filters else {},
            "limit": limit,
        }

        response = await self._request(
            method="POST",
            endpoint="/objects/people/records/query",
            json_data=data,
        )
        return response.get("data", [])

    # Lead/Deal operations (if you have a custom object)

    async def upsert_lead(
        self,
        lead_data: dict[str, Any],
    ) -> str:
        """
        Create or update a lead in Attio.

        Note: This assumes you have a custom "Leads" or "Deals" object in Attio.
        The actual implementation depends on your Attio workspace configuration.

        Args:
            lead_data: Lead data dict

        Returns:
            Attio record ID
        """
        # This is a placeholder - actual implementation depends on your Attio setup
        # You may need to use a custom object like "deals" or "opportunities"

        values: dict[str, Any] = {}

        if lead_data.get("company_record_id"):
            values["company"] = [{"target_record_id": lead_data["company_record_id"]}]
        if lead_data.get("contact_record_id"):
            values["person"] = [{"target_record_id": lead_data["contact_record_id"]}]
        if lead_data.get("score"):
            values["score"] = lead_data["score"]
        if lead_data.get("tier"):
            values["tier"] = lead_data["tier"]
        if lead_data.get("status"):
            values["status"] = lead_data["status"]

        # For now, we'll store lead info as a note on the company
        # In a real implementation, you'd create a custom object for leads

        logger.info(f"Lead data prepared for sync: {values}")

        # Return a placeholder ID - in real implementation, this would be the record ID
        return f"lead_{lead_data.get('company_record_id', 'unknown')}"

    # Note operations

    async def create_note(
        self,
        parent_object: str,
        parent_record_id: str,
        title: str,
        content: str,
    ) -> str:
        """
        Create a note attached to a record.

        Args:
            parent_object: Object type (e.g., "companies", "people")
            parent_record_id: Record ID to attach note to
            title: Note title
            content: Note content

        Returns:
            Note ID
        """
        data = {
            "data": {
                "parent_object": parent_object,
                "parent_record_id": parent_record_id,
                "title": title,
                "content": content,
            }
        }

        response = await self._request(
            method="POST",
            endpoint="/notes",
            json_data=data,
        )
        return response.get("data", {}).get("id", {}).get("note_id", "")

    # List management

    async def add_to_list(
        self,
        list_id: str,
        record_id: str,
    ) -> bool:
        """Add a record to a list."""
        data = {
            "data": {
                "record_id": record_id,
            }
        }

        try:
            await self._request(
                method="POST",
                endpoint=f"/lists/{list_id}/entries",
                json_data=data,
            )
            return True
        except Exception as e:
            logger.error(f"Failed to add record to list: {e}")
            return False

    async def get_lists(self) -> list[dict[str, Any]]:
        """Get all lists."""
        response = await self._request(
            method="GET",
            endpoint="/lists",
        )
        return response.get("data", [])


# Global Attio client instance
attio_client = AttioClient()
