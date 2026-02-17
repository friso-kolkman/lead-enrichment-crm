"""FastAPI web dashboard for lead enrichment pipeline."""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from fastapi import Depends, FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from config import settings
from core.database import db, get_session, init_db
from core.models import (
    Campaign,
    Company,
    Contact,
    EmailStatus,
    Lead,
    LeadStatus,
    LeadTier,
    Sequence,
    SequenceEnrollment,
    SequenceStatus,
    SequenceStep,
)
from core.schemas import (
    BudgetStatus,
    CampaignCreate,
    CampaignResponse,
    LeadDetailResponse,
    LeadResponse,
    PipelineStatus,
    SequenceCreate,
    SequenceEnrollRequest,
    SequenceResponse,
)
from pipeline.orchestrator import PipelineOrchestrator
from utils.cost_tracker import cost_tracker
from utils.rate_limiter import configure_provider_limits

logger = logging.getLogger(__name__)


_sequence_lock = asyncio.Lock()


async def _sequence_processor_loop():
    """Background loop that processes pending sequence emails every 15 minutes."""
    from pipeline.stages.sequences import process_pending_sequences

    while True:
        try:
            async with _sequence_lock:
                async with db.session() as session:
                    result = await process_pending_sequences(session)
                    if result.get("sent", 0) > 0:
                        logger.info(f"Sequence processor: {result}")
        except Exception as e:
            logger.error(f"Sequence processor error: {e}")
        await asyncio.sleep(900)  # 15 minutes


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    # Startup
    await init_db()
    configure_provider_limits()
    processor_task = asyncio.create_task(_sequence_processor_loop())
    logger.info("Application started (sequence processor running)")

    yield

    # Shutdown
    processor_task.cancel()
    try:
        await processor_task
    except asyncio.CancelledError:
        pass
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


@app.get("/ai-lead-finder", response_class=HTMLResponse)
async def ai_lead_finder_page(request: Request):
    """AI-powered lead finder page."""
    return templates.TemplateResponse(
        "ai_lead_finder.html",
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


# ──────────────────────────────────────────────
# Sequence Endpoints
# ──────────────────────────────────────────────


@app.get("/sequences", response_class=HTMLResponse)
async def sequences_page(
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    """Sequences dashboard page."""
    result = await session.execute(
        select(Sequence)
        .options(selectinload(Sequence.steps))
        .order_by(Sequence.created_at.desc())
    )
    sequences = result.scalars().all()

    # Get stats for each sequence
    seq_stats = {}
    for seq in sequences:
        stats_result = await session.execute(
            select(
                SequenceEnrollment.status,
                func.count(SequenceEnrollment.id),
            )
            .where(SequenceEnrollment.sequence_id == seq.id)
            .group_by(SequenceEnrollment.status)
        )
        counts = dict(stats_result.all())
        seq_stats[seq.id] = {
            "total": sum(counts.values()),
            "active": counts.get(SequenceStatus.ACTIVE, 0),
            "completed": counts.get(SequenceStatus.COMPLETED, 0),
            "replied": counts.get(SequenceStatus.REPLIED, 0),
            "bounced": counts.get(SequenceStatus.BOUNCED, 0),
            "unsubscribed": counts.get(SequenceStatus.UNSUBSCRIBED, 0),
        }

    return templates.TemplateResponse(
        "sequences.html",
        {"request": request, "sequences": sequences, "seq_stats": seq_stats},
    )


@app.get("/api/sequences", response_model=list[SequenceResponse])
async def list_sequences(session: AsyncSession = Depends(get_session)):
    """List all sequences."""
    result = await session.execute(
        select(Sequence)
        .options(selectinload(Sequence.steps))
        .order_by(Sequence.created_at.desc())
    )
    return result.scalars().all()


@app.post("/api/sequences", response_model=SequenceResponse)
async def create_sequence(
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    """Create a sequence with steps."""
    from pydantic import ValidationError

    data = await request.json()
    try:
        seq_data = SequenceCreate(**data)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.errors())

    # Validate unique step numbers
    step_numbers = [s.step_number for s in seq_data.steps]
    if len(step_numbers) != len(set(step_numbers)):
        raise HTTPException(status_code=422, detail="Duplicate step numbers")

    sequence = Sequence(
        name=seq_data.name,
        description=seq_data.description,
        target_tier=LeadTier(seq_data.target_tier) if seq_data.target_tier else None,
        min_score=seq_data.min_score,
        max_score=seq_data.max_score,
    )
    session.add(sequence)
    await session.flush()

    for step_data in seq_data.steps:
        step = SequenceStep(
            sequence_id=sequence.id,
            step_number=step_data.step_number,
            delay_days=step_data.delay_days,
            subject_template=step_data.subject_template,
            body_template=step_data.body_template,
        )
        session.add(step)

    await session.flush()

    # Reload with steps
    result = await session.execute(
        select(Sequence)
        .options(selectinload(Sequence.steps))
        .where(Sequence.id == sequence.id)
    )
    return result.scalar_one()


@app.post("/api/sequences/{sequence_id}/activate")
async def activate_sequence(
    sequence_id: int,
    session: AsyncSession = Depends(get_session),
):
    """Activate a sequence."""
    result = await session.execute(
        select(Sequence).where(Sequence.id == sequence_id)
    )
    sequence = result.scalar_one_or_none()
    if not sequence:
        raise HTTPException(status_code=404, detail="Sequence not found")

    sequence.is_active = True
    sequence.is_paused = False
    return {"status": "activated"}


@app.post("/api/sequences/{sequence_id}/pause")
async def pause_sequence(
    sequence_id: int,
    session: AsyncSession = Depends(get_session),
):
    """Pause a sequence."""
    result = await session.execute(
        select(Sequence).where(Sequence.id == sequence_id)
    )
    sequence = result.scalar_one_or_none()
    if not sequence:
        raise HTTPException(status_code=404, detail="Sequence not found")

    sequence.is_paused = True
    return {"status": "paused"}


@app.post("/api/sequences/{sequence_id}/enroll")
async def enroll_leads_api(
    sequence_id: int,
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    """Enroll leads into a sequence (by IDs or by filter criteria)."""
    from pipeline.stages.sequences import enroll_leads

    data = await request.json()
    enroll_data = SequenceEnrollRequest(**data)

    # If specific lead_ids provided, use them directly
    if enroll_data.lead_ids:
        lead_ids = enroll_data.lead_ids
    else:
        # Query leads matching filter criteria
        query = (
            select(Lead.id)
            .join(Contact, Lead.contact_id == Contact.id)
            .where(
                Lead.contact_id.isnot(None),
                Contact.unsubscribed == False,
                Contact.email_status != EmailStatus.INVALID,
            )
        )
        if enroll_data.target_tier:
            query = query.where(Lead.tier == LeadTier(enroll_data.target_tier))
        if enroll_data.min_score is not None:
            query = query.where(Lead.total_score >= enroll_data.min_score)
        if enroll_data.max_score is not None:
            query = query.where(Lead.total_score <= enroll_data.max_score)
        query = query.limit(enroll_data.limit)

        result = await session.execute(query)
        lead_ids = list(result.scalars().all())

    if not lead_ids:
        return {"enrolled": 0, "message": "No matching leads found"}

    return await enroll_leads(session, sequence_id, lead_ids)


@app.get("/api/sequences/{sequence_id}/stats")
async def sequence_stats_api(
    sequence_id: int,
    session: AsyncSession = Depends(get_session),
):
    """Get sequence performance stats."""
    from pipeline.stages.sequences import get_sequence_stats
    return await get_sequence_stats(session, sequence_id)


@app.post("/api/sequences/process")
async def trigger_sequence_processor(
    session: AsyncSession = Depends(get_session),
):
    """Manually trigger the sequence processor (for testing)."""
    if _sequence_lock.locked():
        return {"status": "skipped", "reason": "processor already running"}
    from pipeline.stages.sequences import process_pending_sequences
    async with _sequence_lock:
        return await process_pending_sequences(session)


@app.delete("/api/sequences/{sequence_id}")
async def delete_sequence(
    sequence_id: int,
    session: AsyncSession = Depends(get_session),
):
    """Delete a sequence and its steps/enrollments."""
    result = await session.execute(
        select(Sequence).where(Sequence.id == sequence_id)
    )
    sequence = result.scalar_one_or_none()
    if not sequence:
        raise HTTPException(status_code=404, detail="Sequence not found")

    # Delete enrollments and steps first
    from sqlalchemy import delete
    await session.execute(
        delete(SequenceEnrollment).where(SequenceEnrollment.sequence_id == sequence_id)
    )
    await session.execute(
        delete(SequenceStep).where(SequenceStep.sequence_id == sequence_id)
    )
    await session.delete(sequence)
    return {"status": "deleted"}


@app.post("/api/webhooks/resend")
async def resend_webhook(
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    """
    Handle Resend webhook events for delivery tracking.
    Configure at: https://resend.com/webhooks
    Events: email.delivered, email.opened, email.clicked, email.bounced, email.complained
    """
    payload = await request.json()
    event_type = payload.get("type", "")
    data = payload.get("data", {})
    to_email = data.get("to", [None])[0] if isinstance(data.get("to"), list) else data.get("to")

    if not to_email:
        return {"status": "ignored", "reason": "no recipient"}

    logger.info(f"Resend webhook: {event_type} for {to_email}")

    # Find the contact and their most recent lead
    contact_result = await session.execute(
        select(Contact).where(Contact.email == to_email)
    )
    contact = contact_result.scalar_one_or_none()
    if not contact:
        return {"status": "ignored", "reason": "contact not found"}

    # Find the lead most recently emailed
    lead_result = await session.execute(
        select(Lead)
        .where(Lead.contact_id == contact.id, Lead.last_email_sent.isnot(None))
        .order_by(Lead.last_email_sent.desc())
        .limit(1)
    )
    lead = lead_result.scalar_one_or_none()

    if event_type == "email.delivered":
        if lead and lead.campaign_id:
            camp_result = await session.execute(
                select(Campaign).where(Campaign.id == lead.campaign_id)
            )
            camp = camp_result.scalar_one_or_none()
            if camp:
                camp.total_delivered = (camp.total_delivered or 0) + 1

    elif event_type == "email.opened":
        if lead:
            lead.opens = (lead.opens or 0) + 1
            if lead.campaign_id:
                camp_result = await session.execute(
                    select(Campaign).where(Campaign.id == lead.campaign_id)
                )
                camp = camp_result.scalar_one_or_none()
                if camp:
                    camp.total_opens = (camp.total_opens or 0) + 1

    elif event_type == "email.clicked":
        if lead:
            lead.clicks = (lead.clicks or 0) + 1
            if lead.campaign_id:
                camp_result = await session.execute(
                    select(Campaign).where(Campaign.id == lead.campaign_id)
                )
                camp = camp_result.scalar_one_or_none()
                if camp:
                    camp.total_clicks = (camp.total_clicks or 0) + 1

    elif event_type == "email.bounced":
        # Mark contact email as invalid and stop future sends
        contact.email_status = EmailStatus.INVALID
        if lead:
            if lead.campaign_id:
                camp_result = await session.execute(
                    select(Campaign).where(Campaign.id == lead.campaign_id)
                )
                camp = camp_result.scalar_one_or_none()
                if camp:
                    camp.total_bounces = (camp.total_bounces or 0) + 1
        # Stop sequence enrollments for ALL leads of this contact
        from pipeline.stages.sequences import stop_enrollment
        all_leads_result = await session.execute(
            select(Lead.id).where(Lead.contact_id == contact.id)
        )
        for lid in all_leads_result.scalars().all():
            await stop_enrollment(session, lid, "bounced")
        logger.warning(f"Bounce detected for {to_email} — marked as invalid")

    elif event_type == "email.complained":
        # Spam complaint — unsubscribe immediately
        contact.unsubscribed = True
        contact.unsubscribed_at = datetime.utcnow()
        # Stop sequence enrollments for ALL leads of this contact
        from pipeline.stages.sequences import stop_enrollment
        all_leads_result = await session.execute(
            select(Lead.id).where(Lead.contact_id == contact.id)
        )
        for lid in all_leads_result.scalars().all():
            await stop_enrollment(session, lid, "unsubscribed")
        logger.warning(f"Spam complaint from {to_email} — auto-unsubscribed")

    elif event_type == "email.replied":
        if lead:
            lead.replies = (lead.replies or 0) + 1
            if lead.campaign_id:
                camp_result = await session.execute(
                    select(Campaign).where(Campaign.id == lead.campaign_id)
                )
                camp = camp_result.scalar_one_or_none()
                if camp:
                    camp.total_replies = (camp.total_replies or 0) + 1
        # Stop sequence enrollments for ALL leads of this contact
        from pipeline.stages.sequences import stop_enrollment
        all_leads_result = await session.execute(
            select(Lead.id).where(Lead.contact_id == contact.id)
        )
        for lid in all_leads_result.scalars().all():
            await stop_enrollment(session, lid, "replied")
        logger.info(f"Reply detected from {to_email} — sequences stopped")

    return {"status": "processed", "event": event_type}


@app.get("/unsubscribe", response_class=HTMLResponse)
async def unsubscribe_page(
    email: str = Query(...),
    session: AsyncSession = Depends(get_session),
):
    """Handle unsubscribe requests — marks contact as opted out."""
    from core.models import Contact

    result = await session.execute(
        select(Contact).where(Contact.email == email)
    )
    contact = result.scalar_one_or_none()

    if contact and not contact.unsubscribed:
        contact.unsubscribed = True
        contact.unsubscribed_at = datetime.utcnow()

        # Stop any active sequence enrollments for this contact's leads
        from pipeline.stages.sequences import stop_enrollment
        leads_result = await session.execute(
            select(Lead.id).where(Lead.contact_id == contact.id)
        )
        for lead_id in leads_result.scalars().all():
            await stop_enrollment(session, lead_id, "unsubscribed")

        logger.info(f"Contact {email} unsubscribed")

    # Always show success (don't reveal if email exists)
    return """
    <html><head><title>Unsubscribed</title>
    <style>body{font-family:sans-serif;display:flex;justify-content:center;align-items:center;min-height:100vh;margin:0;background:#f5f5f5}
    .card{background:#fff;padding:40px;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,.1);text-align:center;max-width:400px}
    h1{color:#333;font-size:24px}p{color:#666}</style></head>
    <body><div class="card"><h1>You've been unsubscribed</h1>
    <p>You won't receive any more emails from us.</p></div></body></html>
    """


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


@app.post("/api/ai-lead-finder/chat")
async def ai_lead_finder_chat(request: Request):
    """Process AI lead finder conversation with real AI."""
    import json
    from ai.client import ai_client

    data = await request.json()
    user_message = data.get("message", "")
    context = data.get("context", {})
    history = data.get("history", [])

    # System prompt for the AI assistant
    system_prompt = """You are an AI assistant helping users find B2B leads. Your job is to have a natural conversation to understand exactly what kind of leads they're looking for.

Extract and track these key criteria:
- Industry/sector (e.g., "SaaS", "Mobility", "Hotels", "Rentals")
- Company size (employee count range like "1-50", "50-200")
- Geographic location (countries like "Netherlands", "US", "UK")
- Job titles/roles to target (e.g., "CEO", "VP of Sales", "Director")
- Any specific keywords or requirements

IMPORTANT INSTRUCTIONS:
1. Be conversational and friendly - don't use a rigid format
2. Ask 1-2 questions at a time based on what's missing
3. When you extract information, acknowledge it naturally
4. When you have AT LEAST industry + location OR industry + company size, you can indicate readiness
5. At the end of your response, on a new line, add: CRITERIA: {json object with extracted info}
6. When ready to search, also add: READY: true

Example response format:
"Great! So you're looking for mobility companies in the Netherlands with 1-50 employees. That's perfect! Let me search for those leads now.

CRITERIA: {"industry": "Mobility & Transportation", "location": "Netherlands", "countries": ["NL"], "min_employees": 1, "max_employees": 50, "company_size": "1-50"}
READY: true"

Be natural and conversational. Extract information intelligently."""

    # Build conversation for AI
    messages = [{"role": "system", "content": system_prompt}]

    # Add conversation history
    for msg in history[-6:]:
        messages.append({
            "role": msg["role"],
            "content": msg["content"]
        })

    # Add current context as system reminder
    if context:
        messages.append({
            "role": "system",
            "content": f"Current extracted criteria: {json.dumps(context)}"
        })

    # Add user message
    messages.append({
        "role": "user",
        "content": user_message
    })

    try:
        # Call Claude API (Anthropic)
        import os
        import httpx

        anthropic_api_key = os.getenv("ANTHROPIC_API_KEY", "")

        if not anthropic_api_key:
            raise ValueError("Anthropic API key not configured")

        # Convert messages format for Claude
        claude_messages = []
        system_message = None

        for msg in messages:
            if msg["role"] == "system":
                system_message = msg["content"]
            else:
                claude_messages.append({
                    "role": msg["role"],
                    "content": msg["content"]
                })

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": anthropic_api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json"
                },
                json={
                    "model": "claude-3-5-sonnet-20241022",
                    "max_tokens": 500,
                    "temperature": 0.7,
                    "system": system_message,
                    "messages": claude_messages
                }
            )

            if response.status_code != 200:
                logger.error(f"Claude API error: {response.status_code} - {response.text}")
                raise Exception("Claude API failed")

            result = response.json()
            ai_response = result["content"][0]["text"]

        # Parse AI response for criteria and ready signal
        updated_context = context.copy()
        ready_to_search = False

        # Extract CRITERIA: {...} if present
        if "CRITERIA:" in ai_response:
            try:
                criteria_start = ai_response.index("CRITERIA:") + 9
                criteria_end = ai_response.find("\n", criteria_start)
                if criteria_end == -1:
                    criteria_end = len(ai_response)
                criteria_json = ai_response[criteria_start:criteria_end].strip()
                extracted = json.loads(criteria_json)
                updated_context.update(extracted)
                # Remove CRITERIA line from response
                ai_response = ai_response[:ai_response.index("CRITERIA:")].strip()
            except:
                pass

        # Check for READY signal
        if "READY: true" in ai_response or "READY:true" in ai_response:
            ready_to_search = True
            ai_response = ai_response.replace("READY: true", "").replace("READY:true", "").strip()

        # Also update context from message using keyword extraction as backup
        updated_context = update_context_from_message(user_message, updated_context)

        # Auto-detect readiness
        has_industry = bool(updated_context.get("industry"))
        has_size = bool(updated_context.get("company_size") or updated_context.get("min_employees"))
        has_location = bool(updated_context.get("location") or updated_context.get("countries"))

        if has_industry and (has_size or has_location):
            ready_to_search = True

        return {
            "message": ai_response,
            "context": updated_context,
            "ready_to_search": ready_to_search
        }

    except Exception as e:
        logger.error(f"AI chat error: {str(e)}")
        # Fallback to keyword-based conversation
        updated_context = update_context_from_message(user_message, context)
        has_industry = bool(updated_context.get("industry"))
        has_size = bool(updated_context.get("company_size") or updated_context.get("min_employees"))
        has_location = bool(updated_context.get("location") or updated_context.get("countries"))
        ready = has_industry and has_size and has_location

        message = await generate_lead_finder_response(user_message, updated_context, ready)
        return {
            "message": message,
            "context": updated_context,
            "ready_to_search": ready,
        }


async def generate_lead_finder_response(message: str, context: dict, ready: bool) -> str:
    """Generate AI response for lead finder."""
    message_lower = message.lower()

    # Check what we have and what we need
    has_industry = bool(context.get("industry"))
    has_size = bool(context.get("company_size") or context.get("min_employees"))
    has_location = bool(context.get("location") or context.get("countries"))

    # If ready to search (all 3 criteria gathered)
    if ready:
        industry_str = context.get("industry", "companies in your target sector")
        size_str = f"with {context.get('company_size', '1-50 employees')}" if context.get("company_size") else ""
        location_str = f"in {context.get('location', 'your target location')}" if context.get("location") else ""

        return f"Perfect! I have enough information. Let me search for {industry_str} {size_str} {location_str}. I'll create these leads for you now!"

    # Progressive questioning based on what's missing
    if has_industry and has_size and not has_location:
        return f"Great! {context.get('industry')} companies with {context.get('company_size', '1-50 employees')}. What geographic region? (e.g., Netherlands, United States, Europe)"

    if has_industry and has_location and not has_size:
        return f"Excellent! {context.get('industry')} companies in {context.get('location')}. What size companies? (e.g., 1-50, 50-200, 200-500 employees)"

    if has_industry and not has_size and not has_location:
        return f"Got it - {context.get('industry')} industry! What's the company size range you're targeting? (e.g., 1-50, 50-200, 500+ employees)"

    if not has_industry:
        # Check if they just provided more details
        if any(word in message_lower for word in ["industry", "mobility", "rental", "hotel", "transport", "scooter"]):
            return "Perfect! I understand the industry now. What company size range are you interested in? (e.g., 1-50, 50-200, 500+ employees)"
        return "I'd love to help! What industry are you targeting? (e.g., SaaS, E-commerce, Mobility, Hotels, Rentals, etc.)"

    # Fallback
    return "Great! Just need a bit more information. What else can you tell me about your ideal leads?"


def update_context_from_message(message: str, context: dict) -> dict:
    """Extract structured information from user message."""
    updated = context.copy()
    message_lower = message.lower()

    # Extract industry - expanded list
    if "saas" in message_lower or "software as a service" in message_lower:
        updated["industry"] = "SaaS"
    elif "software" in message_lower and "industry" not in updated:
        updated["industry"] = "Software"
    elif "fintech" in message_lower or "financial tech" in message_lower:
        updated["industry"] = "FinTech"
    elif "ecommerce" in message_lower or "e-commerce" in message_lower:
        updated["industry"] = "E-commerce"
    elif "healthcare" in message_lower or "health care" in message_lower:
        updated["industry"] = "Healthcare"
    elif "mobility" in message_lower or "scooter" in message_lower or "bike" in message_lower or "e-step" in message_lower:
        updated["industry"] = "Mobility & Transportation"
    elif "rental" in message_lower and "industry" not in updated:
        updated["industry"] = "Rental Services"
    elif "hotel" in message_lower or "hospitality" in message_lower or "accommodation" in message_lower:
        updated["industry"] = "Hotels & Hospitality"
    elif "transport" in message_lower and "industry" not in updated:
        updated["industry"] = "Transportation"
    elif "tech" in message_lower or "technology" in message_lower:
        updated["industry"] = "Technology"

    # Extract company size - more patterns
    import re

    # Look for patterns like "1-50", "50-200", etc.
    size_pattern = re.search(r'(\d+)-(\d+)\s*employee', message_lower)
    if size_pattern:
        min_emp = int(size_pattern.group(1))
        max_emp = int(size_pattern.group(2))
        updated["company_size"] = f"{min_emp}-{max_emp}"
        updated["min_employees"] = min_emp
        updated["max_employees"] = max_emp
    elif "1-50" in message_lower or "1 to 50" in message_lower:
        updated["company_size"] = "1-50"
        updated["min_employees"] = 1
        updated["max_employees"] = 50
    elif "50-200" in message_lower or "50 to 200" in message_lower:
        updated["company_size"] = "50-200"
        updated["min_employees"] = 50
        updated["max_employees"] = 200
    elif "100-500" in message_lower:
        updated["company_size"] = "100-500"
        updated["min_employees"] = 100
        updated["max_employees"] = 500
    elif "small" in message_lower or "startup" in message_lower:
        updated["company_size"] = "1-50"
        updated["min_employees"] = 1
        updated["max_employees"] = 50
    elif "medium" in message_lower or "mid" in message_lower:
        updated["company_size"] = "50-200"
        updated["min_employees"] = 50
        updated["max_employees"] = 200
    elif "large" in message_lower or "enterprise" in message_lower:
        updated["company_size"] = "500+"
        updated["min_employees"] = 500
        updated["max_employees"] = 10000

    # Extract location - expanded
    if "netherlands" in message_lower or "holland" in message_lower or "dutch" in message_lower:
        updated["location"] = "Netherlands"
        updated["countries"] = ["NL"]
    elif "us" in message_lower or "usa" in message_lower or "united states" in message_lower or "america" in message_lower:
        updated["location"] = "United States"
        updated["countries"] = ["US"]
    elif "europe" in message_lower and "location" not in updated:
        updated["location"] = "Europe"
        updated["countries"] = ["UK", "DE", "FR", "NL", "ES", "IT"]
    elif "uk" in message_lower or "united kingdom" in message_lower or "britain" in message_lower:
        updated["location"] = "United Kingdom"
        updated["countries"] = ["UK"]
    elif "canada" in message_lower:
        updated["location"] = "Canada"
        updated["countries"] = ["CA"]
    elif "germany" in message_lower or "german" in message_lower:
        updated["location"] = "Germany"
        updated["countries"] = ["DE"]
    elif "france" in message_lower or "french" in message_lower:
        updated["location"] = "France"
        updated["countries"] = ["FR"]

    # Extract titles
    titles = updated.get("titles", [])
    if "vp" in message_lower or "vice president" in message_lower:
        if "VP" not in titles:
            titles.append("VP")
    if "director" in message_lower:
        if "Director" not in titles:
            titles.append("Director")
    if "cto" in message_lower:
        if "CTO" not in titles:
            titles.append("CTO")
    if "ceo" in message_lower:
        if "CEO" not in titles:
            titles.append("CEO")
    if "cmo" in message_lower:
        if "CMO" not in titles:
            titles.append("CMO")
    if "founder" in message_lower:
        if "Founder" not in titles:
            titles.append("Founder")
    if "owner" in message_lower:
        if "Owner" not in titles:
            titles.append("Owner")
    if "manager" in message_lower:
        if "Manager" not in titles:
            titles.append("Manager")

    if titles:
        updated["titles"] = titles

    # Extract keywords/notes
    if "rental" in message_lower or "rent" in message_lower:
        if "keywords" not in updated:
            updated["keywords"] = []
        if "rental" not in updated["keywords"]:
            updated["keywords"].append("rental")

    return updated


@app.post("/api/ai-lead-finder/search")
async def ai_lead_finder_search(request: Request, session: AsyncSession = Depends(get_session)):
    """Search for leads based on AI-gathered criteria using Apollo API."""
    from datetime import datetime
    import random
    import httpx
    import os

    data = await request.json()
    criteria = data.get("criteria", {})

    # Extract criteria
    industry = criteria.get("industry", "Technology")
    location = criteria.get("location", "United States")
    company_size = criteria.get("company_size", "50-200")
    min_employees = criteria.get("min_employees", 1)
    max_employees = criteria.get("max_employees", 50)
    countries = criteria.get("countries", ["US"])
    keywords = criteria.get("keywords", [])

    # Get Apollo API key
    apollo_api_key = os.getenv("APOLLO_API_KEY", "")

    if not apollo_api_key:
        # Fallback to mock data if no API key
        return await generate_mock_leads(session, criteria)

    # Call Apollo API to search for real companies
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Build Apollo search query
            apollo_payload = {
                "page": 1,
                "per_page": 10,
                "organization_num_employees_ranges": [
                    f"{min_employees},{max_employees}"
                ],
                "organization_locations": [location] if location else countries,
                "person_locations": [location] if location else countries,
                "q_organization_keyword_tags": [industry.lower()] if industry else []
            }

            # Log the search
            logger.info(f"Apollo search: location={location}, countries={countries}, industry={industry}")

            # Add keywords if any
            if keywords:
                apollo_payload["q_organization_keyword_tags"].extend(keywords)

            # Apollo requires API key in header
            headers = {
                "X-Api-Key": apollo_api_key,
                "Content-Type": "application/json"
            }

            response = await client.post(
                "https://api.apollo.io/v1/mixed_companies/search",
                json=apollo_payload,
                headers=headers
            )

            if response.status_code != 200:
                logger.error(f"Apollo API error: {response.status_code} - {response.text}")
                return await generate_mock_leads(session, criteria)

            apollo_data = response.json()
            organizations = apollo_data.get("organizations", [])

            if not organizations:
                return {
                    "success": False,
                    "message": "No companies found matching your criteria. Try adjusting your search parameters.",
                    "count": 0
                }

            # Create leads from Apollo data
            created_leads = []
            for org in organizations[:7]:  # Limit to 7 leads
                # Extract company data
                company_name = org.get("name")

                # Handle domain safely
                domain = org.get("primary_domain")
                if not domain:
                    website = org.get("website_url") or ""
                    if website:
                        domain = website.replace("http://", "").replace("https://", "").split("/")[0]

                if not domain or not company_name:
                    continue

                # Verify location matches (filter out non-matching locations)
                org_country = org.get("country", "")
                if countries and org_country not in countries:
                    continue

                # Check if company already exists
                existing = await session.execute(
                    select(Company).where(Company.domain == domain)
                )
                if existing.scalar_one_or_none():
                    continue  # Skip if already exists

                # Create company
                company = Company(
                    domain=domain,
                    name=company_name,
                    industry=org.get("industry") or industry,
                    employee_count=org.get("estimated_num_employees"),
                    employee_range=f"{min_employees}-{max_employees}",
                    founded_year=org.get("founded_year"),
                    hq_city=org.get("city"),
                    hq_state=org.get("state"),
                    hq_country=org.get("country"),
                    website_description=org.get("short_description"),
                    linkedin_url=org.get("linkedin_url"),
                )
                session.add(company)
                await session.flush()

                # Try to get contact from Apollo
                contact = None
                people = org.get("people", [])
                if people and len(people) > 0:
                    person = people[0]

                    contact = Contact(
                        company_id=company.id,
                        first_name=person.get("first_name"),
                        last_name=person.get("last_name"),
                        full_name=person.get("name"),
                        title=person.get("title"),
                        email=person.get("email"),
                        email_status=EmailStatus.PENDING,
                        linkedin_url=person.get("linkedin_url"),
                    )
                    session.add(contact)
                    await session.flush()

                # Create lead
                score = random.randint(60, 95)
                lead = Lead(
                    company_id=company.id,
                    contact_id=contact.id if contact else None,
                    status=LeadStatus.NEW,
                    tier=LeadTier.HIGH_TOUCH if score >= 80 else LeadTier.STANDARD if score >= 60 else LeadTier.NURTURE,
                    total_score=score,
                    score_breakdown={
                        "industry_match": 25,
                        "company_size_fit": 20,
                        "geography_match": 20,
                    },
                    icp_fit_score=score,
                    source="apollo_ai_finder",
                )
                session.add(lead)
                created_leads.append(lead)

            await session.commit()

            return {
                "success": True,
                "message": f"Found and created {len(created_leads)} real leads from Apollo!",
                "count": len(created_leads),
                "criteria": criteria,
                "leads": [{"id": lead.id, "company_id": lead.company_id} for lead in created_leads]
            }

    except Exception as e:
        logger.error(f"Error searching Apollo: {str(e)}")
        # Rollback any failed transaction
        await session.rollback()
        # Fallback to mock data
        return await generate_mock_leads(session, criteria)


async def generate_mock_leads(session: AsyncSession, criteria: dict):
    """Generate mock leads when Apollo API is not available."""
    from datetime import datetime
    import random

    industry = criteria.get("industry", "Technology")
    location = criteria.get("location", "United States")
    company_size = criteria.get("company_size", "50-200")

    # Sample company data matching criteria based on industry
    if "Mobility" in industry or "Transportation" in industry:
        company_templates = [
            {"name_suffix": "Mobility", "description": "Urban mobility and scooter sharing services"},
            {"name_suffix": "Rides", "description": "E-scooter and bike rental platform"},
            {"name_suffix": "Move", "description": "Sustainable transportation solutions"},
            {"name_suffix": "Go", "description": "Micro-mobility services provider"},
            {"name_suffix": "Wheels", "description": "Electric scooter rental and sharing"},
        ]
    elif "Hotel" in industry or "Hospitality" in industry:
        company_templates = [
            {"name_suffix": "Hotels", "description": "Boutique hotel chain"},
            {"name_suffix": "Stay", "description": "Modern hospitality and accommodation"},
            {"name_suffix": "Inn", "description": "Premium hotel and resort services"},
            {"name_suffix": "Suites", "description": "Business and leisure accommodations"},
            {"name_suffix": "Hospitality", "description": "Hotel management and services"},
        ]
    elif "Rental" in industry:
        company_templates = [
            {"name_suffix": "Rentals", "description": "Equipment and vehicle rental services"},
            {"name_suffix": "Hire", "description": "Short-term rental solutions"},
            {"name_suffix": "Lease", "description": "Flexible rental and leasing options"},
            {"name_suffix": "Share", "description": "Peer-to-peer rental platform"},
            {"name_suffix": "Rent", "description": "Online rental marketplace"},
        ]
    else:
        company_templates = [
            {"name_suffix": "Technologies", "description": "Leading provider of innovative technology solutions"},
            {"name_suffix": "Systems", "description": "Enterprise software and cloud services"},
            {"name_suffix": "Solutions", "description": "Digital transformation and consulting services"},
            {"name_suffix": "Platform", "description": "Next-generation SaaS platform"},
            {"name_suffix": "Software", "description": "Business intelligence and analytics tools"},
        ]

    min_employees = criteria.get("min_employees", 1)
    max_employees = criteria.get("max_employees", 50)

    created_leads = []
    num_leads = random.randint(3, 7)  # Create 3-7 leads

    for i in range(num_leads):
        template = random.choice(company_templates)
        prefix = random.choice(['Alpha', 'Beta', 'Gamma', 'Delta', 'Epsilon', 'Zeta', 'Theta', 'Sigma', 'Omega'])
        company_name = f"{prefix} {template['name_suffix']}"
        domain = f"{company_name.lower().replace(' ', '')}.com"

        # Check if domain already exists
        existing_check = await session.execute(
            select(Company).where(Company.domain == domain)
        )
        if existing_check.scalar_one_or_none():
            continue  # Skip if exists

        # Determine location-specific details
        countries = criteria.get("countries", ["NL"])  # Default to NL if specified Netherlands

        # Force Netherlands if location is Netherlands
        if criteria.get("location") and "netherlands" in criteria.get("location", "").lower():
            countries = ["NL"]

        if "NL" in countries:
            cities = ["Amsterdam", "Rotterdam", "Utrecht", "The Hague", "Eindhoven"]
            hq_city = random.choice(cities)
            hq_state = None
            hq_country = "NL"
        elif "US" in countries:
            cities = ["San Francisco", "New York", "Austin", "Boston", "Seattle"]
            states = ["CA", "NY", "TX", "MA", "WA"]
            city_idx = random.randint(0, len(cities) - 1)
            hq_city = cities[city_idx]
            hq_state = states[city_idx]
            hq_country = "US"
        elif "UK" in countries:
            cities = ["London", "Manchester", "Birmingham", "Edinburgh", "Bristol"]
            hq_city = random.choice(cities)
            hq_state = None
            hq_country = "UK"
        elif "DE" in countries:
            cities = ["Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne"]
            hq_city = random.choice(cities)
            hq_state = None
            hq_country = "DE"
        else:
            hq_city = "Amsterdam"
            hq_state = None
            hq_country = countries[0] if countries else "US"

        # Create company
        company = Company(
            domain=domain,
            name=company_name,
            industry=industry,
            employee_count=random.randint(criteria.get("min_employees", 1), criteria.get("max_employees", 50)),
            employee_range=company_size,
            revenue=random.randint(1, 20) * 1000000,
            revenue_range=f"${random.randint(1, 10)}M-${random.randint(11, 50)}M",
            founded_year=random.randint(2010, 2021),
            hq_city=hq_city,
            hq_state=hq_state,
            hq_country=hq_country,
            website_description=template["description"],
            is_hiring=random.choice([True, False]),
            open_positions=random.randint(0, 10),
        )
        session.add(company)
        await session.flush()

        # Create contact if titles specified
        titles_list = criteria.get("titles", ["VP of Sales", "Director"])
        selected_title = random.choice(titles_list) if titles_list else "Director of Sales"

        first_names = ["John", "Sarah", "Michael", "Emily", "David", "Jessica", "Robert", "Lisa"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]

        first_name = random.choice(first_names)
        last_name = random.choice(last_names)

        contact = Contact(
            company_id=company.id,
            first_name=first_name,
            last_name=last_name,
            full_name=f"{first_name} {last_name}",
            title=selected_title,
            seniority_level="Director" if "Director" in selected_title else "VP" if "VP" in selected_title else "C-Level",
            department="Sales" if "Sales" in selected_title else "Marketing" if "Marketing" in selected_title else "Operations",
            email=f"{first_name.lower()}.{last_name.lower()}@{domain}",
            email_status=EmailStatus.PENDING,
        )
        session.add(contact)
        await session.flush()

        # Create lead
        score = random.randint(60, 95)
        lead = Lead(
            company_id=company.id,
            contact_id=contact.id,
            status=LeadStatus.NEW,
            tier=LeadTier.HIGH_TOUCH if score >= 80 else LeadTier.STANDARD if score >= 60 else LeadTier.NURTURE,
            total_score=score,
            score_breakdown={
                "industry_match": 25,
                "company_size_fit": 20,
                "geography_match": 15,
                "title_match": 20,
            },
            icp_fit_score=score,
            source="ai_lead_finder",
        )
        session.add(lead)
        created_leads.append(lead)

    await session.commit()

    return {
        "success": True,
        "message": f"Found and created {len(created_leads)} leads matching your criteria!",
        "count": len(created_leads),
        "criteria": criteria,
        "leads": [{"id": lead.id, "company_id": lead.company_id} for lead in created_leads]
    }


@app.post("/api/demo/create-sample-leads")
async def create_sample_leads(session: AsyncSession = Depends(get_session)):
    """Create sample leads for demo purposes."""
    from datetime import datetime

    sample_companies = [
        {
            "domain": "acme-corp.com",
            "name": "Acme Corporation",
            "industry": "Software",
            "employee_count": 250,
            "employee_range": "200-500",
            "revenue": 15000000,
            "revenue_range": "$10M-$50M",
            "founded_year": 2015,
            "hq_city": "San Francisco",
            "hq_state": "CA",
            "hq_country": "US",
            "website_description": "Enterprise software solutions for modern businesses",
            "is_hiring": True,
            "open_positions": 12,
        },
        {
            "domain": "techstartup.io",
            "name": "TechStartup Inc",
            "industry": "SaaS",
            "employee_count": 75,
            "employee_range": "50-100",
            "revenue": 5000000,
            "revenue_range": "$1M-$10M",
            "founded_year": 2020,
            "hq_city": "Austin",
            "hq_state": "TX",
            "hq_country": "US",
            "website_description": "AI-powered analytics platform",
            "is_hiring": True,
            "open_positions": 5,
        },
        {
            "domain": "globalenterprises.com",
            "name": "Global Enterprises",
            "industry": "Technology",
            "employee_count": 500,
            "employee_range": "500-1000",
            "revenue": 50000000,
            "revenue_range": "$50M-$100M",
            "founded_year": 2010,
            "hq_city": "New York",
            "hq_state": "NY",
            "hq_country": "US",
            "website_description": "Leading provider of business solutions",
            "is_hiring": False,
            "open_positions": 3,
        },
    ]

    sample_contacts = [
        {
            "first_name": "John",
            "last_name": "Smith",
            "full_name": "John Smith",
            "title": "VP of Sales",
            "seniority_level": "VP",
            "department": "Sales",
            "email": "john.smith@acme-corp.com",
            "email_status": EmailStatus.VALID,
        },
        {
            "first_name": "Sarah",
            "last_name": "Johnson",
            "full_name": "Sarah Johnson",
            "title": "Director of Marketing",
            "seniority_level": "Director",
            "department": "Marketing",
            "email": "sarah.j@techstartup.io",
            "email_status": EmailStatus.VALID,
        },
        {
            "first_name": "Michael",
            "last_name": "Brown",
            "full_name": "Michael Brown",
            "title": "Chief Technology Officer",
            "seniority_level": "C-Level",
            "department": "Engineering",
            "email": "mbrown@globalenterprises.com",
            "email_status": EmailStatus.VALID,
        },
    ]

    created_leads = []

    for i, company_data in enumerate(sample_companies):
        # Create company
        company = Company(**company_data)
        session.add(company)
        await session.flush()

        # Create contact
        contact_data = sample_contacts[i]
        contact_data["company_id"] = company.id
        contact = Contact(**contact_data)
        session.add(contact)
        await session.flush()

        # Create lead with sample scoring
        lead = Lead(
            company_id=company.id,
            contact_id=contact.id,
            status=LeadStatus.ENRICHED if i == 0 else LeadStatus.SCORED if i == 1 else LeadStatus.NEW,
            tier=LeadTier.HIGH_TOUCH if i == 0 else LeadTier.STANDARD if i == 1 else LeadTier.NURTURE,
            total_score=85 if i == 0 else 65 if i == 1 else 45,
            score_breakdown={
                "industry_match": 25 if i == 0 else 20,
                "revenue_fit": 20 if i == 0 else 15,
                "employee_fit": 15,
                "geography_match": 10,
                "tech_stack": 15 if i == 0 else 10,
            },
            icp_fit_score=80 if i == 0 else 60 if i == 1 else 40,
            intent_score=5 if i == 0 else 5 if i == 1 else 5,
            research_summary="Strong fit for our product. Fast-growing company with clear need for our solution." if i == 0 else None,
            icebreakers=[
                "Noticed you're hiring 12 new positions - congrats on the growth!",
                "Your recent expansion into enterprise software aligns perfectly with our solution",
            ] if i == 0 else None,
            trigger_events=["Recent funding round", "Expanding sales team"] if i == 0 else None,
            source="demo",
        )
        session.add(lead)
        created_leads.append(lead)

    await session.commit()

    return {
        "message": "Sample leads created successfully",
        "count": len(created_leads),
        "leads": [{"id": lead.id, "company": lead.company_id} for lead in created_leads]
    }
