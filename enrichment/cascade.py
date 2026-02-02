"""Cascade manager for waterfall enrichment across providers."""

import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from core.schemas import CompanyEnrichment, ContactEnrichment, EmailVerificationResult
from enrichment.apollo import ApolloProvider
from enrichment.base import (
    BaseEnrichmentProvider,
    EnrichmentResult,
)
from enrichment.clearbit import ClearbitProvider
from enrichment.dropcontact import DropcontactProvider
from enrichment.hunter import HunterProvider
from enrichment.prospeo import ProspeoProvider
from enrichment.zerobounce import ZeroBounceProvider

logger = logging.getLogger(__name__)


class CascadeManager:
    """Manages waterfall enrichment across multiple providers."""

    def __init__(self):
        self._providers: dict[str, BaseEnrichmentProvider] = {}
        self._initialize_providers()

    def _initialize_providers(self) -> None:
        """Initialize all configured providers."""
        # Company + Contact providers
        if settings.apollo.api_key and settings.apollo.enabled:
            self._providers["apollo"] = ApolloProvider(settings.apollo.api_key)

        if settings.clearbit.api_key and settings.clearbit.enabled:
            self._providers["clearbit"] = ClearbitProvider(settings.clearbit.api_key)

        # Contact-only providers
        if settings.hunter.api_key and settings.hunter.enabled:
            self._providers["hunter"] = HunterProvider(settings.hunter.api_key)

        if settings.prospeo.api_key and settings.prospeo.enabled:
            self._providers["prospeo"] = ProspeoProvider(settings.prospeo.api_key)

        if settings.dropcontact.api_key and settings.dropcontact.enabled:
            self._providers["dropcontact"] = DropcontactProvider(settings.dropcontact.api_key)

        # Email verification
        if settings.zerobounce.api_key and settings.zerobounce.enabled:
            self._providers["zerobounce"] = ZeroBounceProvider(settings.zerobounce.api_key)

        logger.info(f"Initialized {len(self._providers)} enrichment providers: {list(self._providers.keys())}")

    def get_provider(self, name: str) -> BaseEnrichmentProvider | None:
        """Get a specific provider by name."""
        return self._providers.get(name)

    async def close_all(self) -> None:
        """Close all provider HTTP clients."""
        for provider in self._providers.values():
            await provider.close()

    async def enrich_company(
        self,
        domain: str,
        session: AsyncSession | None = None,
        company_id: int | None = None,
        providers: list[str] | None = None,
    ) -> tuple[CompanyEnrichment | None, list[str]]:
        """
        Enrich company data using cascade of providers.

        Args:
            domain: Company domain to enrich
            session: Database session for logging
            company_id: Company ID for logging
            providers: Optional list of providers to use (overrides config)

        Returns:
            Tuple of (enrichment_data, list_of_successful_providers)
        """
        provider_order = providers or settings.cascade.company_order
        successful_providers: list[str] = []
        merged_data: dict[str, Any] = {}

        for provider_name in provider_order:
            provider = self._providers.get(provider_name)
            if not provider:
                logger.debug(f"Provider {provider_name} not configured, skipping")
                continue

            logger.info(f"Trying company enrichment with {provider_name} for {domain}")

            result = await provider.enrich_company(
                domain=domain,
                session=session,
                company_id=company_id,
            )

            if result.success and result.data:
                successful_providers.append(provider_name)

                # Merge data (later providers fill gaps, don't overwrite)
                data_dict = result.data.model_dump(exclude_none=True)
                for key, value in data_dict.items():
                    if key not in merged_data or merged_data[key] is None:
                        merged_data[key] = value

                logger.info(f"Got company data from {provider_name}")

                # Stop if configured to stop on success
                if settings.cascade.stop_on_success:
                    break
            else:
                logger.warning(f"Provider {provider_name} failed: {result.error}")

        if not merged_data:
            return None, []

        return CompanyEnrichment(**merged_data), successful_providers

    async def enrich_contact(
        self,
        email: str | None = None,
        linkedin_url: str | None = None,
        first_name: str | None = None,
        last_name: str | None = None,
        domain: str | None = None,
        session: AsyncSession | None = None,
        contact_id: int | None = None,
        providers: list[str] | None = None,
    ) -> tuple[ContactEnrichment | None, list[str]]:
        """
        Enrich contact data using cascade of providers.

        Args:
            email: Contact email
            linkedin_url: LinkedIn profile URL
            first_name: First name
            last_name: Last name
            domain: Company domain
            session: Database session for logging
            contact_id: Contact ID for logging
            providers: Optional list of providers to use (overrides config)

        Returns:
            Tuple of (enrichment_data, list_of_successful_providers)
        """
        provider_order = providers or settings.cascade.contact_order
        successful_providers: list[str] = []
        merged_data: dict[str, Any] = {}

        for provider_name in provider_order:
            provider = self._providers.get(provider_name)
            if not provider:
                logger.debug(f"Provider {provider_name} not configured, skipping")
                continue

            logger.info(f"Trying contact enrichment with {provider_name}")

            result = await provider.enrich_contact(
                email=email,
                linkedin_url=linkedin_url,
                first_name=first_name,
                last_name=last_name,
                domain=domain,
                session=session,
                contact_id=contact_id,
            )

            if result.success and result.data:
                successful_providers.append(provider_name)

                # Merge data
                data_dict = result.data.model_dump(exclude_none=True)
                for key, value in data_dict.items():
                    if key not in merged_data or merged_data[key] is None:
                        merged_data[key] = value

                logger.info(f"Got contact data from {provider_name}")

                # Update email for subsequent providers
                if not email and merged_data.get("email"):
                    email = merged_data["email"]

                # Stop if we have all critical fields
                if settings.cascade.stop_on_success and self._has_required_contact_fields(merged_data):
                    break
            else:
                logger.warning(f"Provider {provider_name} failed: {result.error}")

        if not merged_data:
            return None, []

        return ContactEnrichment(**merged_data), successful_providers

    def _has_required_contact_fields(self, data: dict[str, Any]) -> bool:
        """Check if we have the minimum required contact fields."""
        has_name = data.get("first_name") and data.get("last_name")
        has_email = bool(data.get("email"))
        has_title = bool(data.get("title"))
        return has_name and has_email and has_title

    async def verify_email(
        self,
        email: str,
        session: AsyncSession | None = None,
        contact_id: int | None = None,
    ) -> EmailVerificationResult | None:
        """
        Verify an email using the configured verification provider.

        Args:
            email: Email address to verify
            session: Database session for logging
            contact_id: Contact ID for logging

        Returns:
            EmailVerificationResult or None if verification fails
        """
        provider_name = settings.cascade.email_verification
        provider = self._providers.get(provider_name)

        if not provider:
            logger.error(f"Email verification provider {provider_name} not configured")
            return None

        if not isinstance(provider, ZeroBounceProvider):
            logger.error(f"Provider {provider_name} does not support email verification")
            return None

        logger.info(f"Verifying email {email} with {provider_name}")

        result = await provider.verify_email(
            email=email,
            session=session,
            contact_id=contact_id,
        )

        if result.success and result.data:
            logger.info(f"Email {email} status: {result.data.status}")
            return result.data

        logger.warning(f"Email verification failed: {result.error}")
        return None

    def get_provider_status(self) -> dict[str, dict[str, Any]]:
        """Get status of all configured providers."""
        status = {}
        for name, provider in self._providers.items():
            status[name] = {
                "configured": True,
                "enabled": True,
                "base_url": provider.BASE_URL,
                "cost_per_request": provider.COST_PER_REQUEST,
            }

        # Add unconfigured providers
        all_providers = ["apollo", "clearbit", "hunter", "prospeo", "dropcontact", "zerobounce"]
        for name in all_providers:
            if name not in status:
                status[name] = {
                    "configured": False,
                    "enabled": False,
                }

        return status


# Global cascade manager instance
cascade_manager = CascadeManager()
