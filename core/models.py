"""SQLAlchemy models for lead enrichment pipeline."""

from datetime import datetime
from typing import Optional
from enum import Enum as PyEnum

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for all models."""

    pass


class LeadStatus(str, PyEnum):
    """Lead status enum."""

    NEW = "NEW"
    ENRICHING = "ENRICHING"
    ENRICHED = "ENRICHED"
    SCORED = "SCORED"
    RESEARCHED = "RESEARCHED"
    READY = "READY"
    SYNCED = "SYNCED"
    CONTACTED = "CONTACTED"


class LeadTier(str, PyEnum):
    """Lead tier enum."""

    HIGH_TOUCH = "high_touch"
    STANDARD = "standard"
    NURTURE = "nurture"


class EmailStatus(str, PyEnum):
    """Email verification status."""

    PENDING = "pending"
    VALID = "valid"
    INVALID = "invalid"
    CATCH_ALL = "catch_all"
    UNKNOWN = "unknown"


class Company(Base):
    """Company model with firmographics and technographics."""

    __tablename__ = "companies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    domain: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    name: Mapped[Optional[str]] = mapped_column(String(255))

    # Firmographics
    industry: Mapped[Optional[str]] = mapped_column(String(255))
    sub_industry: Mapped[Optional[str]] = mapped_column(String(255))
    employee_count: Mapped[Optional[int]] = mapped_column(Integer)
    employee_range: Mapped[Optional[str]] = mapped_column(String(50))
    revenue: Mapped[Optional[float]] = mapped_column(Float)
    revenue_range: Mapped[Optional[str]] = mapped_column(String(50))
    founded_year: Mapped[Optional[int]] = mapped_column(Integer)

    # Location
    hq_city: Mapped[Optional[str]] = mapped_column(String(255))
    hq_state: Mapped[Optional[str]] = mapped_column(String(100))
    hq_country: Mapped[Optional[str]] = mapped_column(String(100))
    hq_region: Mapped[Optional[str]] = mapped_column(String(100))

    # Funding
    total_funding: Mapped[Optional[float]] = mapped_column(Float)
    last_funding_date: Mapped[Optional[datetime]] = mapped_column(DateTime)
    last_funding_amount: Mapped[Optional[float]] = mapped_column(Float)
    last_funding_type: Mapped[Optional[str]] = mapped_column(String(100))
    funding_stage: Mapped[Optional[str]] = mapped_column(String(50))

    # Technographics
    tech_stack: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict)
    crm_platform: Mapped[Optional[str]] = mapped_column(String(100))
    marketing_automation: Mapped[Optional[str]] = mapped_column(String(100))

    # Signals
    is_hiring: Mapped[bool] = mapped_column(Boolean, default=False)
    open_positions: Mapped[Optional[int]] = mapped_column(Integer)
    hiring_departments: Mapped[Optional[list]] = mapped_column(JSONB, default=list)

    # Social
    linkedin_url: Mapped[Optional[str]] = mapped_column(String(500))
    twitter_url: Mapped[Optional[str]] = mapped_column(String(500))
    website_description: Mapped[Optional[str]] = mapped_column(Text)

    # Metadata
    enriched_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    enrichment_sources: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    raw_data: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    contacts: Mapped[list["Contact"]] = relationship("Contact", back_populates="company")
    leads: Mapped[list["Lead"]] = relationship("Lead", back_populates="company")

    __table_args__ = (
        Index("ix_companies_industry", "industry"),
        Index("ix_companies_employee_count", "employee_count"),
        Index("ix_companies_hq_country", "hq_country"),
    )


class Contact(Base):
    """Contact model with personal and professional info."""

    __tablename__ = "contacts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"), nullable=False)

    # Personal info
    first_name: Mapped[Optional[str]] = mapped_column(String(100))
    last_name: Mapped[Optional[str]] = mapped_column(String(100))
    full_name: Mapped[Optional[str]] = mapped_column(String(255))

    # Professional info
    title: Mapped[Optional[str]] = mapped_column(String(255))
    normalized_title: Mapped[Optional[str]] = mapped_column(String(100))
    seniority_level: Mapped[Optional[str]] = mapped_column(String(50))
    department: Mapped[Optional[str]] = mapped_column(String(100))

    # Contact info
    email: Mapped[Optional[str]] = mapped_column(String(255))
    email_status: Mapped[EmailStatus] = mapped_column(
        Enum(EmailStatus), default=EmailStatus.PENDING
    )
    email_verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    mobile_phone: Mapped[Optional[str]] = mapped_column(String(50))
    work_phone: Mapped[Optional[str]] = mapped_column(String(50))

    # Social
    linkedin_url: Mapped[Optional[str]] = mapped_column(String(500))
    twitter_url: Mapped[Optional[str]] = mapped_column(String(500))

    # Metadata
    enriched_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    enrichment_sources: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    raw_data: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    company: Mapped["Company"] = relationship("Company", back_populates="contacts")
    leads: Mapped[list["Lead"]] = relationship("Lead", back_populates="contact")

    __table_args__ = (
        UniqueConstraint("email", name="uq_contacts_email"),
        Index("ix_contacts_company_id", "company_id"),
        Index("ix_contacts_email_status", "email_status"),
        Index("ix_contacts_seniority_level", "seniority_level"),
    )


class Lead(Base):
    """Lead model combining company and contact with scoring."""

    __tablename__ = "leads"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"), nullable=False)
    contact_id: Mapped[Optional[int]] = mapped_column(ForeignKey("contacts.id"))

    # Status tracking
    status: Mapped[LeadStatus] = mapped_column(
        Enum(LeadStatus), default=LeadStatus.NEW
    )
    tier: Mapped[Optional[LeadTier]] = mapped_column(Enum(LeadTier))

    # Scoring
    total_score: Mapped[Optional[int]] = mapped_column(Integer)
    score_breakdown: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict)
    icp_fit_score: Mapped[Optional[int]] = mapped_column(Integer)
    intent_score: Mapped[Optional[int]] = mapped_column(Integer)

    # AI Research
    research_summary: Mapped[Optional[str]] = mapped_column(Text)
    kpis: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    trigger_events: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    linkedin_posts: Mapped[Optional[list]] = mapped_column(JSONB, default=list)

    # Generated content
    icebreakers: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    email_variants: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict)
    linkedin_message: Mapped[Optional[str]] = mapped_column(Text)

    # CRM sync
    attio_record_id: Mapped[Optional[str]] = mapped_column(String(255))
    synced_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Campaign tracking
    campaign_id: Mapped[Optional[int]] = mapped_column(ForeignKey("campaigns.id"))
    contacted_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    last_email_sent: Mapped[Optional[datetime]] = mapped_column(DateTime)
    emails_sent: Mapped[int] = mapped_column(Integer, default=0)
    opens: Mapped[int] = mapped_column(Integer, default=0)
    clicks: Mapped[int] = mapped_column(Integer, default=0)
    replies: Mapped[int] = mapped_column(Integer, default=0)

    # Metadata
    source: Mapped[Optional[str]] = mapped_column(String(100))
    source_file: Mapped[Optional[str]] = mapped_column(String(500))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    company: Mapped["Company"] = relationship("Company", back_populates="leads")
    contact: Mapped[Optional["Contact"]] = relationship("Contact", back_populates="leads")
    campaign: Mapped[Optional["Campaign"]] = relationship("Campaign", back_populates="leads")

    __table_args__ = (
        Index("ix_leads_status", "status"),
        Index("ix_leads_tier", "tier"),
        Index("ix_leads_total_score", "total_score"),
        Index("ix_leads_company_contact", "company_id", "contact_id"),
    )


class Campaign(Base):
    """Email campaign model."""

    __tablename__ = "campaigns"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)

    # Targeting
    target_tier: Mapped[Optional[LeadTier]] = mapped_column(Enum(LeadTier))
    min_score: Mapped[Optional[int]] = mapped_column(Integer)
    max_score: Mapped[Optional[int]] = mapped_column(Integer)
    filters: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict)

    # Templates
    email_subject_template: Mapped[Optional[str]] = mapped_column(Text)
    email_body_template: Mapped[Optional[str]] = mapped_column(Text)
    email_variants: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict)

    # Schedule
    start_date: Mapped[Optional[datetime]] = mapped_column(DateTime)
    end_date: Mapped[Optional[datetime]] = mapped_column(DateTime)
    send_times: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    timezone: Mapped[str] = mapped_column(String(50), default="UTC")

    # Limits
    daily_limit: Mapped[int] = mapped_column(Integer, default=50)
    total_limit: Mapped[Optional[int]] = mapped_column(Integer)

    # Status
    is_active: Mapped[bool] = mapped_column(Boolean, default=False)
    is_paused: Mapped[bool] = mapped_column(Boolean, default=False)

    # Stats
    total_sent: Mapped[int] = mapped_column(Integer, default=0)
    total_delivered: Mapped[int] = mapped_column(Integer, default=0)
    total_opens: Mapped[int] = mapped_column(Integer, default=0)
    total_clicks: Mapped[int] = mapped_column(Integer, default=0)
    total_replies: Mapped[int] = mapped_column(Integer, default=0)
    total_bounces: Mapped[int] = mapped_column(Integer, default=0)

    # Metadata
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Relationships
    leads: Mapped[list["Lead"]] = relationship("Lead", back_populates="campaign")

    __table_args__ = (Index("ix_campaigns_is_active", "is_active"),)


class EnrichmentLog(Base):
    """Log of all enrichment API calls for auditing and cost tracking."""

    __tablename__ = "enrichment_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Request info
    provider: Mapped[str] = mapped_column(String(50), nullable=False)
    endpoint: Mapped[str] = mapped_column(String(255), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(50))  # company, contact, email
    entity_id: Mapped[Optional[int]] = mapped_column(Integer)

    # Request details
    request_params: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict)

    # Response
    success: Mapped[bool] = mapped_column(Boolean, default=False)
    status_code: Mapped[Optional[int]] = mapped_column(Integer)
    response_data: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict)
    error_message: Mapped[Optional[str]] = mapped_column(Text)

    # Cost
    cost_usd: Mapped[float] = mapped_column(Float, default=0.0)
    credits_used: Mapped[Optional[int]] = mapped_column(Integer)

    # Timing
    request_time_ms: Mapped[Optional[int]] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_enrichment_logs_provider", "provider"),
        Index("ix_enrichment_logs_created_at", "created_at"),
        Index("ix_enrichment_logs_entity", "entity_type", "entity_id"),
    )


class CostSummary(Base):
    """Monthly cost aggregation per provider."""

    __tablename__ = "cost_summaries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    provider: Mapped[str] = mapped_column(String(50), nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    month: Mapped[int] = mapped_column(Integer, nullable=False)

    # Aggregations
    total_requests: Mapped[int] = mapped_column(Integer, default=0)
    successful_requests: Mapped[int] = mapped_column(Integer, default=0)
    failed_requests: Mapped[int] = mapped_column(Integer, default=0)
    total_cost_usd: Mapped[float] = mapped_column(Float, default=0.0)
    total_credits_used: Mapped[int] = mapped_column(Integer, default=0)

    # Metadata
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        UniqueConstraint("provider", "year", "month", name="uq_cost_summary_period"),
        Index("ix_cost_summaries_period", "year", "month"),
    )
