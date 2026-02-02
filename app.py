"""FastAPI web dashboard for lead enrichment pipeline."""

import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from core.database import db, get_session, init_db
from core.models import (
    Campaign,
    Company,
    Contact,
    Lead,
    LeadStatus,
    LeadTier,
)
from core.schemas import (
    BudgetStatus,
    CampaignCreate,
    CampaignResponse,
    LeadDetailResponse,
    LeadResponse,
    PipelineStatus,
)
from pipeline.orchestrator import PipelineOrchestrator
from utils.cost_tracker import cost_tracker
from utils.rate_limiter import configure_provider_limits

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    # Startup
    await init_db()
    configure_provider_limits()
    logger.info("Application started")

    yield

    # Shutdown
    await db.close()
    logger.info("Application shutdown")


app = FastAPI(
    title="Lead Enrichment Pipeline",
    description="9-stage waterfall pipeline for lead enrichment and outbound campaigns",
    version="1.0.0",
    lifespan=lifespan,
)

# Templates
templates = Jinja2Templates(directory="templates")


# Dashboard route
@app.get("/", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    """Main dashboard page."""
    # Get pipeline status
    orch = PipelineOrchestrator()
    pipeline_status = await orch.get_pipeline_status(session)

    # Get budget status
    budget_status = await cost_tracker.get_budget_status(session)

    # Get recent leads
    result = await session.execute(
        select(Lead)
        .order_by(Lead.created_at.desc())
        .limit(10)
    )
    recent_leads = result.scalars().all()

    # Load companies for leads
    if recent_leads:
        company_ids = list({l.company_id for l in recent_leads})
        company_result = await session.execute(
            select(Company).where(Company.id.in_(company_ids))
        )
        companies = {c.id: c for c in company_result.scalars().all()}
    else:
        companies = {}

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "pipeline_status": pipeline_status,
            "budget_status": budget_status,
            "recent_leads": recent_leads,
            "companies": companies,
            "lead_status_enum": LeadStatus,
            "lead_tier_enum": LeadTier,
        },
    )


@app.get("/leads", response_class=HTMLResponse)
async def leads_page(
    request: Request,
    session: AsyncSession = Depends(get_session),
    status: LeadStatus | None = None,
    tier: LeadTier | None = None,
    min_score: int | None = None,
    limit: int = Query(default=50, le=500),
    offset: int = 0,
):
    """Leads list page with filtering."""
    # Build query
    query = select(Lead)

    if status:
        query = query.where(Lead.status == status)
    if tier:
        query = query.where(Lead.tier == tier)
    if min_score is not None:
        query = query.where(Lead.total_score >= min_score)

    # Get total count
    count_query = select(func.count()).select_from(query.subquery())
    total_count = await session.scalar(count_query)

    # Get leads with pagination
    query = query.order_by(Lead.total_score.desc().nullslast())
    query = query.offset(offset).limit(limit)
    result = await session.execute(query)
    leads = result.scalars().all()

    # Load companies
    if leads:
        company_ids = list({l.company_id for l in leads})
        company_result = await session.execute(
            select(Company).where(Company.id.in_(company_ids))
        )
        companies = {c.id: c for c in company_result.scalars().all()}

        # Load contacts
        contact_ids = [l.contact_id for l in leads if l.contact_id]
        if contact_ids:
            contact_result = await session.execute(
                select(Contact).where(Contact.id.in_(contact_ids))
            )
            contacts = {c.id: c for c in contact_result.scalars().all()}
        else:
            contacts = {}
    else:
        companies = {}
        contacts = {}

    # Get status counts
    status_counts = {}
    for status_enum in LeadStatus:
        count_result = await session.execute(
            select(func.count()).select_from(Lead).where(Lead.status == status_enum)
        )
        status_counts[status_enum.value] = count_result.scalar()

    return templates.TemplateResponse(
        "leads.html",
        {
            "request": request,
            "leads": leads,
            "companies": companies,
            "contacts": contacts,
            "total_count": total_count,
            "limit": limit,
            "offset": offset,
            "selected_status": status.value if status else None,
            "selected_tier": tier.value if tier else None,
            "min_score": min_score,
            "status_counts": status_counts,
        },
    )


@app.get("/leads/{lead_id}", response_class=HTMLResponse)
async def lead_detail_page(
    lead_id: int,
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    """Lead detail page."""
    result = await session.execute(
        select(Lead).where(Lead.id == lead_id)
    )
    lead = result.scalar_one_or_none()

    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    # Load company
    company_result = await session.execute(
        select(Company).where(Company.id == lead.company_id)
    )
    lead.company = company_result.scalar_one()

    # Load contact if exists
    if lead.contact_id:
        contact_result = await session.execute(
            select(Contact).where(Contact.id == lead.contact_id)
        )
        lead.contact = contact_result.scalar_one_or_none()
    else:
        lead.contact = None

    return templates.TemplateResponse(
        "lead_detail.html",
        {
            "request": request,
            "lead": lead,
        },
    )


@app.get("/campaigns", response_class=HTMLResponse)
async def campaigns_page(
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    """Campaigns management page."""
    result = await session.execute(
        select(Campaign).order_by(Campaign.created_at.desc())
    )
    campaigns = result.scalars().all()

    return templates.TemplateResponse(
        "campaigns.html",
        {
            "request": request,
            "campaigns": campaigns,
        },
    )


@app.get("/analytics", response_class=HTMLResponse)
async def analytics_page(request: Request):
    """Analytics page (placeholder)."""
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request},
    )


# API routes

@app.get("/api/status", response_model=PipelineStatus)
async def get_status(session: AsyncSession = Depends(get_session)):
    """Get pipeline status."""
    orch = PipelineOrchestrator()
    status = await orch.get_pipeline_status(session)
    return PipelineStatus(**status)


@app.get("/api/budget", response_model=BudgetStatus)
async def get_budget(session: AsyncSession = Depends(get_session)):
    """Get budget status."""
    status = await cost_tracker.get_budget_status(session)
    return BudgetStatus(**status)


@app.get("/api/leads", response_model=list[LeadResponse])
async def list_leads(
    session: AsyncSession = Depends(get_session),
    status: LeadStatus | None = None,
    tier: LeadTier | None = None,
    min_score: int | None = None,
    limit: int = Query(default=50, le=500),
    offset: int = 0,
):
    """List leads with optional filters."""
    query = select(Lead)

    if status:
        query = query.where(Lead.status == status)
    if tier:
        query = query.where(Lead.tier == tier)
    if min_score is not None:
        query = query.where(Lead.total_score >= min_score)

    query = query.order_by(Lead.total_score.desc().nullslast())
    query = query.offset(offset).limit(limit)

    result = await session.execute(query)
    return result.scalars().all()


@app.get("/api/leads/{lead_id}", response_model=LeadDetailResponse)
async def get_lead(
    lead_id: int,
    session: AsyncSession = Depends(get_session),
):
    """Get lead details."""
    result = await session.execute(
        select(Lead).where(Lead.id == lead_id)
    )
    lead = result.scalar_one_or_none()

    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    # Load company
    company_result = await session.execute(
        select(Company).where(Company.id == lead.company_id)
    )
    company = company_result.scalar_one()

    # Load contact
    contact = None
    if lead.contact_id:
        contact_result = await session.execute(
            select(Contact).where(Contact.id == lead.contact_id)
        )
        contact = contact_result.scalar_one_or_none()

    return LeadDetailResponse(
        id=lead.id,
        company_id=lead.company_id,
        contact_id=lead.contact_id,
        status=lead.status.value,
        tier=lead.tier.value if lead.tier else None,
        total_score=lead.total_score,
        research_summary=lead.research_summary,
        icebreakers=lead.icebreakers,
        attio_record_id=lead.attio_record_id,
        created_at=lead.created_at,
        updated_at=lead.updated_at,
        company=company,
        contact=contact,
        score_breakdown=lead.score_breakdown,
        email_variants=lead.email_variants,
        trigger_events=lead.trigger_events,
    )


@app.post("/api/pipeline/run")
async def run_pipeline(
    session: AsyncSession = Depends(get_session),
    start_stage: int = Query(default=1, ge=1, le=9),
    end_stage: int = Query(default=9, ge=1, le=9),
    limit: int = Query(default=100, le=1000),
    dry_run: bool = False,
):
    """Run pipeline stages."""
    orch = PipelineOrchestrator()
    result = await orch.run_stages(
        session=session,
        start=start_stage,
        end=end_stage,
        limit=limit,
        dry_run=dry_run,
    )
    return result


@app.get("/api/campaigns", response_model=list[CampaignResponse])
async def list_campaigns(
    session: AsyncSession = Depends(get_session),
):
    """List all campaigns."""
    result = await session.execute(select(Campaign))
    return result.scalars().all()


@app.post("/api/campaigns", response_model=CampaignResponse)
async def create_campaign_api(
    request: Request,
    session: AsyncSession = Depends(get_session),
    name: str = Query(...),
    description: str | None = Query(default=None),
    target_tier: str | None = Query(default=None),
    min_score: int | None = Query(default=None),
    max_score: int | None = Query(default=None),
    daily_limit: int = Query(default=50),
    email_subject_template: str | None = Query(default=None),
    email_body_template: str | None = Query(default=None),
):
    """Create a new campaign."""
    from pipeline.stages import campaign as campaign_module

    # Check if request is form data
    content_type = request.headers.get("content-type", "")
    if "application/x-www-form-urlencoded" in content_type or "multipart/form-data" in content_type:
        form_data = await request.form()
        name = form_data.get("name")
        description = form_data.get("description") or None
        target_tier_str = form_data.get("target_tier") or None
        min_score = int(form_data.get("min_score")) if form_data.get("min_score") else None
        max_score = int(form_data.get("max_score")) if form_data.get("max_score") else None
        daily_limit = int(form_data.get("daily_limit", 50))
        email_subject_template = form_data.get("email_subject_template") or None
        email_body_template = form_data.get("email_body_template") or None
    else:
        target_tier_str = target_tier

    tier = None
    if target_tier_str:
        tier = LeadTier(target_tier_str)

    campaign = await campaign_module.create_campaign(
        session=session,
        name=name,
        description=description,
        target_tier=tier,
        min_score=min_score,
        max_score=max_score,
        daily_limit=daily_limit,
        email_subject_template=email_subject_template,
        email_body_template=email_body_template,
    )
    return campaign


@app.post("/api/campaigns/{campaign_id}/launch")
async def launch_campaign_api(
    campaign_id: int,
    session: AsyncSession = Depends(get_session),
    limit: int | None = None,
    dry_run: bool = False,
):
    """Launch a campaign."""
    from pipeline.stages import campaign as campaign_module

    result = await campaign_module.launch_campaign(
        session=session,
        campaign_id=campaign_id,
        limit=limit,
        dry_run=dry_run,
    )
    return result


@app.post("/api/campaigns/{campaign_id}/activate")
async def activate_campaign_api(
    campaign_id: int,
    session: AsyncSession = Depends(get_session),
):
    """Activate a campaign."""
    from pipeline.stages import campaign as campaign_module

    campaign = await campaign_module.activate_campaign(session, campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return {"status": "activated"}


@app.post("/api/campaigns/{campaign_id}/pause")
async def pause_campaign_api(
    campaign_id: int,
    session: AsyncSession = Depends(get_session),
):
    """Pause a campaign."""
    from pipeline.stages import campaign as campaign_module

    campaign = await campaign_module.pause_campaign(session, campaign_id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return {"status": "paused"}


@app.get("/api/providers")
async def get_providers():
    """Get configured enrichment providers."""
    from enrichment.cascade import cascade_manager
    return cascade_manager.get_provider_status()


@app.get("/api/stats/scoring")
async def get_scoring_stats(session: AsyncSession = Depends(get_session)):
    """Get scoring statistics."""
    from pipeline.stages import scoring
    return await scoring.get_scoring_stats(session)


@app.get("/api/stats/verification")
async def get_verification_stats(session: AsyncSession = Depends(get_session)):
    """Get email verification statistics."""
    from pipeline.stages import email_verification
    return await email_verification.get_verification_stats(session)


@app.get("/api/stats/messaging")
async def get_messaging_stats(session: AsyncSession = Depends(get_session)):
    """Get messaging generation statistics."""
    from pipeline.stages import messaging
    return await messaging.get_messaging_stats(session)


@app.get("/api/stats/sync")
async def get_sync_stats(session: AsyncSession = Depends(get_session)):
    """Get CRM sync statistics."""
    from pipeline.stages import crm_sync
    return await crm_sync.get_sync_stats(session)
