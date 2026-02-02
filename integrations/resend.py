"""Resend email client."""

import logging
from typing import Any

import httpx

from config import settings
from utils.rate_limiter import rate_limiter

logger = logging.getLogger(__name__)


class ResendClient:
    """Client for Resend email API."""

    def __init__(
        self,
        api_key: str | None = None,
        from_email: str | None = None,
        from_name: str | None = None,
    ):
        self.api_key = api_key or settings.resend.api_key
        self.from_email = from_email or settings.resend.from_email
        self.from_name = from_name or settings.resend.from_name
        self.base_url = "https://api.resend.com"
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

    async def send_email(
        self,
        to_email: str,
        subject: str,
        html_body: str | None = None,
        text_body: str | None = None,
        from_email: str | None = None,
        from_name: str | None = None,
        reply_to: str | None = None,
        cc: list[str] | None = None,
        bcc: list[str] | None = None,
        tags: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        """
        Send an email via Resend.

        Args:
            to_email: Recipient email address
            subject: Email subject
            html_body: HTML body content
            text_body: Plain text body content
            from_email: Sender email (overrides default)
            from_name: Sender name (overrides default)
            reply_to: Reply-to address
            cc: CC recipients
            bcc: BCC recipients
            tags: Tags for tracking

        Returns:
            Dict with message ID or error
        """
        # Acquire rate limit (Resend free tier: 100/day, ~7/min)
        await rate_limiter.acquire("resend")

        # Build from address
        sender = from_email or self.from_email
        sender_name = from_name or self.from_name
        from_address = f"{sender_name} <{sender}>" if sender_name else sender

        # Build request payload
        payload: dict[str, Any] = {
            "from": from_address,
            "to": [to_email],
            "subject": subject,
        }

        if html_body:
            payload["html"] = html_body
        if text_body:
            payload["text"] = text_body
        if reply_to:
            payload["reply_to"] = reply_to
        if cc:
            payload["cc"] = cc
        if bcc:
            payload["bcc"] = bcc
        if tags:
            payload["tags"] = tags

        try:
            response = await self.client.post("/emails", json=payload)
            response.raise_for_status()
            data = response.json()

            logger.info(f"Email sent to {to_email}: {data.get('id')}")
            return {
                "success": True,
                "id": data.get("id"),
            }

        except httpx.HTTPStatusError as e:
            error_data = e.response.json() if e.response.content else {}
            error_message = error_data.get("message", str(e))
            logger.error(f"Resend API error: {e.response.status_code} - {error_message}")
            return {
                "success": False,
                "error": error_message,
                "status_code": e.response.status_code,
            }
        except Exception as e:
            logger.error(f"Resend request error: {e}")
            return {
                "success": False,
                "error": str(e),
            }

    async def send_batch(
        self,
        emails: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Send multiple emails.

        Note: Resend doesn't have a true batch endpoint on free tier,
        so this sends emails sequentially.

        Args:
            emails: List of email dicts with to, subject, html_body, etc.

        Returns:
            List of results for each email
        """
        results = []
        for email in emails:
            result = await self.send_email(
                to_email=email["to"],
                subject=email["subject"],
                html_body=email.get("html_body"),
                text_body=email.get("text_body"),
                reply_to=email.get("reply_to"),
            )
            results.append({
                "to": email["to"],
                **result,
            })
        return results

    async def get_email(self, email_id: str) -> dict[str, Any]:
        """
        Get email details by ID.

        Args:
            email_id: Resend email ID

        Returns:
            Email details dict
        """
        try:
            response = await self.client.get(f"/emails/{email_id}")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get email {email_id}: {e}")
            return {"error": str(e)}

    async def create_contact(
        self,
        email: str,
        first_name: str | None = None,
        last_name: str | None = None,
        unsubscribed: bool = False,
        audience_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Create a contact in Resend audience.

        Args:
            email: Contact email
            first_name: First name
            last_name: Last name
            unsubscribed: Whether contact is unsubscribed
            audience_id: Audience ID to add contact to

        Returns:
            Contact creation result
        """
        if not audience_id:
            return {"error": "audience_id is required"}

        payload: dict[str, Any] = {
            "email": email,
            "unsubscribed": unsubscribed,
        }
        if first_name:
            payload["first_name"] = first_name
        if last_name:
            payload["last_name"] = last_name

        try:
            response = await self.client.post(
                f"/audiences/{audience_id}/contacts",
                json=payload,
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to create contact {email}: {e}")
            return {"error": str(e)}

    async def get_audiences(self) -> list[dict[str, Any]]:
        """Get all audiences."""
        try:
            response = await self.client.get("/audiences")
            response.raise_for_status()
            return response.json().get("data", [])
        except Exception as e:
            logger.error(f"Failed to get audiences: {e}")
            return []

    async def create_audience(self, name: str) -> dict[str, Any]:
        """Create a new audience."""
        try:
            response = await self.client.post(
                "/audiences",
                json={"name": name},
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to create audience {name}: {e}")
            return {"error": str(e)}


# Global Resend client instance
resend_client = ResendClient()
