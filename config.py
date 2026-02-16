"""Configuration for lead enrichment pipeline."""

import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

load_dotenv()


class DatabaseConfig(BaseModel):
    """Database configuration."""

    url: str = Field(
        default="postgresql+asyncpg://localhost/lead_enrichment",
        description="PostgreSQL connection URL",
    )
    echo: bool = Field(default=False, description="Echo SQL queries")
    pool_size: int = Field(default=5, description="Connection pool size")


class EnrichmentProviderConfig(BaseModel):
    """Configuration for a single enrichment provider."""

    api_key: str = ""
    enabled: bool = True
    priority: int = 1
    rate_limit: int = 100  # requests per minute
    cost_per_request: float = 0.0


class CascadeConfig(BaseModel):
    """Enrichment cascade configuration."""

    company_order: list[str] = Field(
        default=["apollo", "clearbit"],
        description="Order of providers for company enrichment",
    )
    contact_order: list[str] = Field(
        default=["apollo", "prospeo", "hunter", "dropcontact"],
        description="Order of providers for contact enrichment",
    )
    email_verification: str = Field(
        default="zerobounce",
        description="Email verification provider",
    )
    stop_on_success: bool = Field(
        default=True,
        description="Stop cascade when data is found",
    )


class ICPCriteria(BaseModel):
    """Ideal Customer Profile criteria for scoring."""

    # Industry targeting
    target_industries: list[str] = Field(
        default=[
            "Software",
            "Technology",
            "SaaS",
            "FinTech",
            "E-commerce",
            "Digital Marketing",
        ]
    )
    excluded_industries: list[str] = Field(
        default=["Government", "Non-profit", "Education"]
    )

    # Company size
    min_employees: int = 50
    max_employees: int = 1000
    min_revenue: float = 5_000_000  # $5M
    max_revenue: float = 100_000_000  # $100M

    # Geography
    target_countries: list[str] = Field(default=["US", "UK", "CA", "DE", "NL"])
    target_regions: list[str] = Field(
        default=["North America", "Europe", "Western Europe"]
    )

    # Tech stack signals
    target_technologies: list[str] = Field(
        default=[
            "Salesforce",
            "HubSpot",
            "Marketo",
            "Segment",
            "Mixpanel",
            "Amplitude",
        ]
    )

    # Title targeting
    target_titles: list[str] = Field(
        default=[
            "VP",
            "Director",
            "Head",
            "Chief",
            "Founder",
            "CEO",
            "CTO",
            "CMO",
            "CRO",
        ]
    )
    target_departments: list[str] = Field(
        default=["Sales", "Marketing", "Revenue", "Growth", "Operations"]
    )


class ScoringWeights(BaseModel):
    """Weights for ICP scoring."""

    # Base ICP fit weights (total 100)
    industry_match: int = 25
    revenue_fit: int = 20
    tech_stack_match: int = 20
    employee_fit: int = 15
    geography_match: int = 10
    title_match: int = 10

    # Intent signal bonuses
    recent_funding_bonus: int = 15  # Funding < 6 months
    hiring_bonus: int = 10  # is_hiring = true
    open_positions_bonus: int = 10  # 5+ open positions
    news_mentions_bonus: int = 5  # Recent news

    # Score cap
    max_score: int = 100


class TierConfig(BaseModel):
    """Lead tier configuration."""

    high_touch_min: int = 80
    standard_min: int = 50
    # Below standard_min = nurture tier


class BudgetConfig(BaseModel):
    """Budget tracking configuration."""

    monthly_budget: float = Field(default=100.0, description="Monthly budget in USD")
    alert_threshold: float = Field(
        default=0.8, description="Alert when budget reaches this percentage"
    )
    hard_stop: bool = Field(
        default=True, description="Stop enrichment when budget exhausted"
    )


class AIConfig(BaseModel):
    """AI provider configuration."""

    openai_api_key: str = ""
    perplexity_api_key: str = ""
    model: str = "gpt-4o-mini"
    max_tokens: int = 1000
    temperature: float = 0.7


class ResendConfig(BaseModel):
    """Resend email configuration."""

    api_key: str = ""
    from_email: str = "noreply@example.com"
    from_name: str = "Your Name"
    reply_to_email: str = ""  # Where replies go (important for deliverability)
    daily_limit: int = 100  # Free tier limit
    # CAN-SPAM required: physical address in footer
    company_name: str = ""
    company_address: str = ""  # e.g. "123 Main St, City, Country"
    unsubscribe_url: str = ""  # Base URL for unsubscribe links


class AttioConfig(BaseModel):
    """Attio CRM configuration."""

    api_key: str = ""
    base_url: str = "https://api.attio.com/v2"


class Settings(BaseSettings):
    """Main application settings."""

    # Database
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)

    # Enrichment providers
    apollo: EnrichmentProviderConfig = Field(default_factory=EnrichmentProviderConfig)
    clearbit: EnrichmentProviderConfig = Field(default_factory=EnrichmentProviderConfig)
    hunter: EnrichmentProviderConfig = Field(default_factory=EnrichmentProviderConfig)
    prospeo: EnrichmentProviderConfig = Field(default_factory=EnrichmentProviderConfig)
    dropcontact: EnrichmentProviderConfig = Field(
        default_factory=EnrichmentProviderConfig
    )
    zerobounce: EnrichmentProviderConfig = Field(
        default_factory=EnrichmentProviderConfig
    )

    # Cascade configuration
    cascade: CascadeConfig = Field(default_factory=CascadeConfig)

    # ICP and scoring
    icp: ICPCriteria = Field(default_factory=ICPCriteria)
    scoring: ScoringWeights = Field(default_factory=ScoringWeights)
    tiers: TierConfig = Field(default_factory=TierConfig)

    # Budget
    budget: BudgetConfig = Field(default_factory=BudgetConfig)

    # AI
    ai: AIConfig = Field(default_factory=AIConfig)

    # Integrations
    resend: ResendConfig = Field(default_factory=ResendConfig)
    attio: AttioConfig = Field(default_factory=AttioConfig)

    model_config = {"env_prefix": "", "env_nested_delimiter": "__"}

    @classmethod
    def from_env(cls) -> "Settings":
        """Load settings from environment variables."""
        return cls(
            database=DatabaseConfig(
                url=os.getenv(
                    "DATABASE_URL", "postgresql+asyncpg://localhost/lead_enrichment"
                ),
            ),
            apollo=EnrichmentProviderConfig(
                api_key=os.getenv("APOLLO_API_KEY", ""),
                cost_per_request=0.03,
                rate_limit=100,
            ),
            clearbit=EnrichmentProviderConfig(
                api_key=os.getenv("CLEARBIT_API_KEY", ""),
                cost_per_request=0.10,
                rate_limit=60,
                priority=2,
            ),
            hunter=EnrichmentProviderConfig(
                api_key=os.getenv("HUNTER_API_KEY", ""),
                cost_per_request=0.01,
                rate_limit=100,
                priority=3,
            ),
            prospeo=EnrichmentProviderConfig(
                api_key=os.getenv("PROSPEO_API_KEY", ""),
                cost_per_request=0.05,
                rate_limit=60,
                priority=2,
            ),
            dropcontact=EnrichmentProviderConfig(
                api_key=os.getenv("DROPCONTACT_API_KEY", ""),
                cost_per_request=0.04,
                rate_limit=50,
                priority=4,
            ),
            zerobounce=EnrichmentProviderConfig(
                api_key=os.getenv("ZEROBOUNCE_API_KEY", ""),
                cost_per_request=0.008,
                rate_limit=200,
            ),
            budget=BudgetConfig(
                monthly_budget=float(os.getenv("MONTHLY_BUDGET", "100.0")),
            ),
            ai=AIConfig(
                openai_api_key=os.getenv("OPENAI_API_KEY", ""),
                perplexity_api_key=os.getenv("PERPLEXITY_API_KEY", ""),
            ),
            resend=ResendConfig(
                api_key=os.getenv("RESEND_API_KEY", ""),
                from_email=os.getenv("RESEND_FROM_EMAIL", "noreply@example.com"),
                from_name=os.getenv("RESEND_FROM_NAME", "Your Name"),
                reply_to_email=os.getenv("RESEND_REPLY_TO", ""),
                company_name=os.getenv("COMPANY_NAME", ""),
                company_address=os.getenv("COMPANY_ADDRESS", ""),
                unsubscribe_url=os.getenv("UNSUBSCRIBE_BASE_URL", ""),
            ),
            attio=AttioConfig(
                api_key=os.getenv("ATTIO_API_KEY", ""),
            ),
        )


# Global settings instance
settings = Settings.from_env()


# Provider cost map for budget tracking
PROVIDER_COSTS: dict[str, float] = {
    "apollo": 0.03,
    "clearbit": 0.10,
    "hunter": 0.01,
    "prospeo": 0.05,
    "dropcontact": 0.04,
    "zerobounce": 0.008,
    "openai": 0.002,  # Approximate per request
    "perplexity": 0.005,
}

# Lead statuses
LEAD_STATUSES = [
    "NEW",
    "ENRICHING",
    "ENRICHED",
    "SCORED",
    "RESEARCHED",
    "READY",
    "SYNCED",
    "CONTACTED",
]

# Lead tiers
LEAD_TIERS = ["high_touch", "standard", "nurture"]
