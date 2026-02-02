"""ZeroBounce email verification provider."""

import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from core.schemas import EmailVerificationResult
from enrichment.base import EmailVerificationProvider, EnrichmentResult

logger = logging.getLogger(__name__)


class ZeroBounceProvider(EmailVerificationProvider):
    """ZeroBounce provider for email verification."""

    PROVIDER_NAME = "zerobounce"
    BASE_URL = "https://api.zerobounce.net/v2"
    COST_PER_REQUEST = 0.008

    async def verify_email(
        self,
        email: str,
        session: AsyncSession | None = None,
        contact_id: int | None = None,
    ) -> EnrichmentResult[EmailVerificationResult]:
        """Verify an email address using ZeroBounce."""
        try:
            response = await self._make_request(
                method="GET",
                endpoint="/validate",
                params={
                    "api_key": self.api_key,
                    "email": email,
                },
                session=session,
                entity_type="verification",
                entity_id=contact_id,
            )

            result = self._parse_verification_response(email, response)
            return EnrichmentResult(
                success=True,
                data=result,
                raw_response=response,
                provider=self.PROVIDER_NAME,
                cost=self.COST_PER_REQUEST,
            )

        except Exception as e:
            logger.error(f"ZeroBounce verification error: {e}")
            return EnrichmentResult(
                success=False,
                error=str(e),
                provider=self.PROVIDER_NAME,
            )

    def _parse_verification_response(
        self, email: str, response: dict[str, Any]
    ) -> EmailVerificationResult:
        """Parse ZeroBounce response to EmailVerificationResult."""
        status = response.get("status", "unknown").lower()
        sub_status = response.get("sub_status")

        # Map ZeroBounce status to our status
        status_mapping = {
            "valid": "valid",
            "invalid": "invalid",
            "catch-all": "catch_all",
            "unknown": "unknown",
            "spamtrap": "invalid",
            "abuse": "invalid",
            "do_not_mail": "invalid",
        }

        normalized_status = status_mapping.get(status, "unknown")

        return EmailVerificationResult(
            email=email,
            status=normalized_status,
            sub_status=sub_status,
            free_email=response.get("free_email"),
            disposable=response.get("disposable") == "true" or response.get("disposable") is True,
            smtp_provider=response.get("smtp_provider"),
        )

    async def bulk_verify(
        self,
        emails: list[str],
        session: AsyncSession | None = None,
    ) -> list[EmailVerificationResult]:
        """
        Verify multiple emails.
        Note: ZeroBounce has a separate bulk API endpoint for large batches.
        This implementation uses single validation for simplicity.
        """
        results = []
        for email in emails:
            result = await self.verify_email(email, session)
            if result.success and result.data:
                results.append(result.data)
            else:
                results.append(
                    EmailVerificationResult(
                        email=email,
                        status="unknown",
                    )
                )
        return results

    async def get_credits(self) -> dict[str, Any]:
        """Get remaining ZeroBounce credits."""
        try:
            response = await self._make_request(
                method="GET",
                endpoint="/getcredits",
                params={"api_key": self.api_key},
            )
            return {
                "credits": response.get("Credits", 0),
                "provider": self.PROVIDER_NAME,
            }
        except Exception as e:
            logger.error(f"ZeroBounce get credits error: {e}")
            return {"credits": 0, "error": str(e), "provider": self.PROVIDER_NAME}

    async def get_api_usage(self) -> dict[str, Any]:
        """Get API usage statistics."""
        try:
            response = await self._make_request(
                method="GET",
                endpoint="/getapiusage",
                params={"api_key": self.api_key},
            )
            return {
                "total": response.get("total", 0),
                "status_valid": response.get("status_valid", 0),
                "status_invalid": response.get("status_invalid", 0),
                "status_catch_all": response.get("status_catch_all", 0),
                "status_unknown": response.get("status_unknown", 0),
                "status_spamtrap": response.get("status_spamtrap", 0),
                "status_abuse": response.get("status_abuse", 0),
                "status_do_not_mail": response.get("status_do_not_mail", 0),
                "provider": self.PROVIDER_NAME,
            }
        except Exception as e:
            logger.error(f"ZeroBounce get usage error: {e}")
            return {"error": str(e), "provider": self.PROVIDER_NAME}
