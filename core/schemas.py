"""Pydantic schemas for validation and serialization."""

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, EmailStr, Field, HttpUrl, field_validator


class CompanyBase(BaseModel):
    """Base company schema."""

    domain: str
    name: Optional[str] = None


class CompanyCreate(CompanyBase):
    """Schema for creating a company."""

    pass


class CompanyEnrichment(BaseModel):
    """Schema for company enrichment data."""

    # Firmographics
    industry: Optional[str] = None
    sub_industry: Optional[str] = None
    employee_count: Optional[int] = None
    employee_range: Optional[str] = None
    revenue: Optional[float] = None
    revenue_range: Optional[str] = None
    founded_year: Optional[int] = None

    # Location
    hq_city: Optional[str] = None
    hq_state: Optional[str] = None
    hq_country: Optional[str] = None
    hq_region: Optional[str] = None

    # Funding
    total_funding: Optional[float] = None
    last_funding_date: Optional[datetime] = None
    last_funding_amount: Optional[float] = None
    last_funding_type: Optional[str] = None
    funding_stage: Optional[str] = None

    # Technographics
    tech_stack: Optional[dict[str, Any]] = None
    crm_platform: Optional[str] = None
    marketing_automation: Optional[str] = None

    # Signals
    is_hiring: Optional[bool] = None
    open_positions: Optional[int] = None
    hiring_departments: Optional[list[str]] = None

    # Social
    linkedin_url: Optional[str] = None
    twitter_url: Optional[str] = None
    website_description: Optional[str] = None


class CompanyResponse(CompanyBase):
    """Schema for company response."""

    id: int
    industry: Optional[str] = None
    employee_count: Optional[int] = None
    revenue: Optional[float] = None
    hq_country: Optional[str] = None
    tech_stack: Optional[dict[str, Any]] = None
    is_hiring: bool = False
    enriched_at: Optional[datetime] = None
    created_at: datetime

    model_config = {"from_attributes": True}


class ContactBase(BaseModel):
    """Base contact schema."""

    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[EmailStr] = None


class ContactCreate(ContactBase):
    """Schema for creating a contact."""

    company_id: int
    title: Optional[str] = None
    linkedin_url: Optional[str] = None


class ContactEnrichment(BaseModel):
    """Schema for contact enrichment data."""

    first_name: Optional[str] = None
    last_name: Optional[str] = None
    full_name: Optional[str] = None
    title: Optional[str] = None
    normalized_title: Optional[str] = None
    seniority_level: Optional[str] = None
    department: Optional[str] = None
    email: Optional[str] = None
    mobile_phone: Optional[str] = None
    work_phone: Optional[str] = None
    linkedin_url: Optional[str] = None
    twitter_url: Optional[str] = None


class ContactResponse(ContactBase):
    """Schema for contact response."""

    id: int
    company_id: int
    full_name: Optional[str] = None
    title: Optional[str] = None
    seniority_level: Optional[str] = None
    email_status: str = "pending"
    linkedin_url: Optional[str] = None
    enriched_at: Optional[datetime] = None
    created_at: datetime

    model_config = {"from_attributes": True}


class LeadBase(BaseModel):
    """Base lead schema."""

    company_id: int
    contact_id: Optional[int] = None


class LeadCreate(LeadBase):
    """Schema for creating a lead."""

    source: Optional[str] = None
    source_file: Optional[str] = None


class LeadScoring(BaseModel):
    """Schema for lead scoring results."""

    total_score: int = Field(ge=0, le=100)
    icp_fit_score: int = Field(ge=0, le=100)
    intent_score: int = Field(ge=0, le=50)
    tier: str
    score_breakdown: dict[str, int]


class LeadResearch(BaseModel):
    """Schema for AI research results."""

    research_summary: str
    kpis: list[str]
    trigger_events: list[dict[str, Any]]
    linkedin_posts: Optional[list[dict[str, Any]]] = None


class LeadMessaging(BaseModel):
    """Schema for generated messaging."""

    icebreakers: list[str] = Field(min_length=1, max_length=5)
    email_variants: dict[str, str]  # tier -> email content
    linkedin_message: Optional[str] = None


class LeadResponse(LeadBase):
    """Schema for lead response."""

    id: int
    status: str
    tier: Optional[str] = None
    total_score: Optional[int] = None
    research_summary: Optional[str] = None
    icebreakers: Optional[list[str]] = None
    attio_record_id: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class LeadDetailResponse(LeadResponse):
    """Detailed lead response with company and contact."""

    company: CompanyResponse
    contact: Optional[ContactResponse] = None
    score_breakdown: Optional[dict[str, int]] = None
    email_variants: Optional[dict[str, str]] = None
    trigger_events: Optional[list[dict[str, Any]]] = None


class CampaignBase(BaseModel):
    """Base campaign schema."""

    name: str
    description: Optional[str] = None


class CampaignCreate(CampaignBase):
    """Schema for creating a campaign."""

    target_tier: Optional[str] = None
    min_score: Optional[int] = None
    max_score: Optional[int] = None
    daily_limit: int = 50
    email_subject_template: Optional[str] = None
    email_body_template: Optional[str] = None


class CampaignResponse(CampaignBase):
    """Schema for campaign response."""

    id: int
    target_tier: Optional[str] = None
    is_active: bool
    is_paused: bool
    daily_limit: int
    total_sent: int
    total_opens: int
    total_clicks: int
    total_replies: int
    created_at: datetime

    model_config = {"from_attributes": True}


class EnrichmentLogResponse(BaseModel):
    """Schema for enrichment log response."""

    id: int
    provider: str
    endpoint: str
    entity_type: Optional[str] = None
    success: bool
    cost_usd: float
    request_time_ms: Optional[int] = None
    created_at: datetime

    model_config = {"from_attributes": True}


class CostSummaryResponse(BaseModel):
    """Schema for cost summary response."""

    provider: str
    year: int
    month: int
    total_requests: int
    successful_requests: int
    failed_requests: int
    total_cost_usd: float

    model_config = {"from_attributes": True}


class BudgetStatus(BaseModel):
    """Schema for budget status."""

    monthly_budget: float
    spent_this_month: float
    remaining: float
    percentage_used: float
    is_over_budget: bool
    breakdown_by_provider: dict[str, float]


class PipelineStatus(BaseModel):
    """Schema for pipeline status."""

    total_leads: int
    by_status: dict[str, int]
    by_tier: dict[str, int]
    enrichment_progress: float
    scoring_progress: float
    research_progress: float


class LeadImportRequest(BaseModel):
    """Schema for lead import request."""

    file_path: str
    source: str = "import"
    skip_duplicates: bool = True


class LeadImportResult(BaseModel):
    """Schema for lead import result."""

    total_rows: int
    imported: int
    skipped: int
    errors: list[str]


class EmailVerificationResult(BaseModel):
    """Schema for email verification result."""

    email: str
    status: str  # valid, invalid, catch_all, unknown
    sub_status: Optional[str] = None
    free_email: Optional[bool] = None
    disposable: Optional[bool] = None
    smtp_provider: Optional[str] = None


class SyncResult(BaseModel):
    """Schema for CRM sync result."""

    total: int
    created: int
    updated: int
    failed: int
    errors: list[str]


class EmailSendRequest(BaseModel):
    """Schema for sending an email."""

    lead_id: int
    variant: str = "standard"  # high_touch, standard, nurture
    custom_subject: Optional[str] = None
    custom_body: Optional[str] = None


class EmailSendResult(BaseModel):
    """Schema for email send result."""

    success: bool
    message_id: Optional[str] = None
    error: Optional[str] = None


# --- Sequence schemas ---


class SequenceStepCreate(BaseModel):
    """Schema for creating a sequence step."""

    step_number: int = Field(ge=1)
    delay_days: int = Field(ge=0, default=1)
    subject_template: str
    body_template: str


class SequenceStepResponse(BaseModel):
    """Schema for sequence step response."""

    id: int
    step_number: int
    delay_days: int
    subject_template: str
    body_template: str

    model_config = {"from_attributes": True}


class SequenceCreate(BaseModel):
    """Schema for creating a sequence with steps."""

    name: str
    description: Optional[str] = None
    target_tier: Optional[str] = None
    min_score: Optional[int] = None
    max_score: Optional[int] = None
    steps: list[SequenceStepCreate] = Field(min_length=1)


class SequenceResponse(BaseModel):
    """Schema for sequence response."""

    id: int
    name: str
    description: Optional[str] = None
    target_tier: Optional[str] = None
    is_active: bool
    is_paused: bool
    steps: list[SequenceStepResponse] = []
    created_at: datetime

    model_config = {"from_attributes": True}


class SequenceEnrollmentResponse(BaseModel):
    """Schema for enrollment response."""

    id: int
    lead_id: int
    sequence_id: int
    current_step: int
    status: str
    enrolled_at: datetime
    last_step_sent_at: Optional[datetime] = None
    next_send_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class SequenceEnrollRequest(BaseModel):
    """Schema for enrolling leads into a sequence."""

    lead_ids: Optional[list[int]] = None
    target_tier: Optional[str] = None
    min_score: Optional[int] = None
    max_score: Optional[int] = None
    limit: int = 50
