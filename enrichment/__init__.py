"""Enrichment providers module."""

from enrichment.base import (
    BaseEnrichmentProvider,
    CompanyEnrichmentProvider,
    ContactEnrichmentProvider,
    EmailVerificationProvider,
    EnrichmentResult,
)
from enrichment.apollo import ApolloProvider
from enrichment.clearbit import ClearbitProvider
from enrichment.hunter import HunterProvider
from enrichment.prospeo import ProspeoProvider
from enrichment.dropcontact import DropcontactProvider
from enrichment.zerobounce import ZeroBounceProvider
from enrichment.cascade import CascadeManager, cascade_manager

__all__ = [
    # Base classes
    "BaseEnrichmentProvider",
    "CompanyEnrichmentProvider",
    "ContactEnrichmentProvider",
    "EmailVerificationProvider",
    "EnrichmentResult",
    # Providers
    "ApolloProvider",
    "ClearbitProvider",
    "HunterProvider",
    "ProspeoProvider",
    "DropcontactProvider",
    "ZeroBounceProvider",
    # Cascade
    "CascadeManager",
    "cascade_manager",
]
