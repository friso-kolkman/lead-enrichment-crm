"""Stage 6: AI research - LinkedIn posts, KPIs, triggers, summaries."""

import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ai.generator import (
    analyze_linkedin_posts,
    research_company,
    research_contact,
)
from core.models import Company, Contact, Lead, LeadStatus
from utils.cost_tracker import BudgetExceeded, cost_tracker

logger = logging.getLogger(__name__)


async def research_lead(
    session: AsyncSession,
    lead: Lead,
    company: Company | None = None,
    contact: Contact | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """
    Conduct AI research for a lead.

    Args:
        session: Database session
        lead: Lead to research
        company: Company (optional, will load if needed)
        contact: Contact (optional, will load if needed)
        force: Re-research even if already researched

    Returns:
        Dict with research results
    """
    # Check if already researched
    if lead.research_summary and not force:
        logger.debug(f"Lead {lead.id} already researched, skipping")
        return {
            "research_summary": lead.research_summary,
            "kpis": lead.kpis,
            "trigger_events": lead.trigger_events,
        }

    # Check budget
    if not await cost_tracker.check_budget(session):
        raise BudgetExceeded(
            "Monthly budget exceeded",
            spent=await cost_tracker.get_monthly_spend(session),
            budget=(await cost_tracker.get_budget_status(session))["monthly_budget"],
        )

    # Load company if needed
    if not company:
        result = await session.execute(
            select(Company).where(Company.id == lead.company_id)
        )
        company = result.scalar_one_or_none()

    if not company:
        logger.error(f"Company not found for lead {lead.id}")
        return {"error": "Company not found"}

    # Load contact if needed
    if not contact and lead.contact_id:
        result = await session.execute(
            select(Contact).where(Contact.id == lead.contact_id)
        )
        contact = result.scalar_one_or_none()

    logger.info(f"Researching lead {lead.id} ({company.domain})")

    # Research company
    company_research = await research_company(company)

    # Research contact if available
    contact_research = {}
    if contact:
        contact_research = await research_contact(contact, company)

    # Combine research
    research_summary_parts = []
    if company_research.get("summary"):
        research_summary_parts.append(company_research["summary"])
    if contact_research.get("role_summary"):
        research_summary_parts.append(contact_research["role_summary"])

    research_summary = " ".join(research_summary_parts)

    # Combine KPIs
    kpis = list(set(
        company_research.get("kpis", []) +
        contact_research.get("kpis", [])
    ))

    # Build trigger events from company data
    trigger_events = _build_trigger_events(company)

    # Update lead
    lead.research_summary = research_summary
    lead.kpis = kpis
    lead.trigger_events = trigger_events
    lead.status = LeadStatus.RESEARCHED

    # Log cost (approximate - depends on model)
    await cost_tracker.log_request(
        session=session,
        provider="openai",
        endpoint="chat/completions",
        entity_type="lead",
        entity_id=lead.id,
        success=True,
        cost_override=0.002,  # Approximate cost
    )

    logger.info(f"Research complete for lead {lead.id}")

    return {
        "research_summary": research_summary,
        "kpis": kpis,
        "trigger_events": trigger_events,
        "company_research": company_research,
        "contact_research": contact_research,
    }


def _build_trigger_events(company: Company) -> list[dict[str, Any]]:
    """Build trigger events list from company data."""
    events: list[dict[str, Any]] = []

    # Recent funding
    if company.last_funding_date and company.last_funding_amount:
        events.append({
            "type": "funding",
            "description": f"Raised ${company.last_funding_amount:,.0f} ({company.last_funding_type or 'funding'})",
            "date": company.last_funding_date.isoformat() if company.last_funding_date else None,
        })

    # Hiring activity
    if company.is_hiring and company.open_positions:
        events.append({
            "type": "hiring",
            "description": f"Actively hiring with {company.open_positions} open positions",
            "departments": company.hiring_departments or [],
        })

    return events


async def research_leads_batch(
    session: AsyncSession,
    leads: list[Lead] | None = None,
    status_filter: LeadStatus | None = LeadStatus.SCORED,
    limit: int = 50,
    force: bool = False,
) -> dict[str, Any]:
    """
    Research a batch of leads.

    Args:
        session: Database session
        leads: Specific leads to process (optional)
        status_filter: Only process leads with this status
        limit: Maximum leads to process
        force: Re-research even if already researched

    Returns:
        Dict with research statistics
    """
    # Get leads to process
    if leads is None:
        query = select(Lead)
        if status_filter:
            query = query.where(Lead.status == status_filter)
        if not force:
            query = query.where(Lead.research_summary.is_(None))
        query = query.limit(limit)
        result = await session.execute(query)
        leads = list(result.scalars().all())

    if not leads:
        logger.info("No leads to research")
        return {"total": 0, "researched": 0, "failed": 0}

    logger.info(f"Researching {len(leads)} leads")

    # Load all companies
    company_ids = list({lead.company_id for lead in leads})
    result = await session.execute(
        select(Company).where(Company.id.in_(company_ids))
    )
    companies = {c.id: c for c in result.scalars().all()}

    # Load all contacts
    contact_ids = [lead.contact_id for lead in leads if lead.contact_id]
    contacts: dict[int, Contact] = {}
    if contact_ids:
        result = await session.execute(
            select(Contact).where(Contact.id.in_(contact_ids))
        )
        contacts = {c.id: c for c in result.scalars().all()}

    researched = 0
    failed = 0
    errors: list[str] = []

    for lead in leads:
        try:
            company = companies.get(lead.company_id)
            contact = contacts.get(lead.contact_id) if lead.contact_id else None

            result = await research_lead(session, lead, company, contact, force=force)

            if "error" not in result:
                researched += 1
            else:
                failed += 1
                errors.append(f"Lead {lead.id}: {result['error']}")

        except BudgetExceeded as e:
            logger.error(f"Budget exceeded: {e}")
            errors.append(f"Budget exceeded after {researched} leads")
            break
        except Exception as e:
            logger.error(f"Error researching lead {lead.id}: {e}")
            failed += 1
            errors.append(f"Lead {lead.id}: {str(e)}")

    return {
        "total": len(leads),
        "researched": researched,
        "failed": failed,
        "errors": errors[:20],
    }


async def analyze_lead_linkedin(
    session: AsyncSession,
    lead: Lead,
    contact: Contact | None = None,
    company: Company | None = None,
    posts: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Analyze LinkedIn posts for a lead's contact.

    Note: This requires LinkedIn posts to be provided externally
    (e.g., from a LinkedIn scraping service or manual input).

    Args:
        session: Database session
        lead: Lead
        contact: Contact (optional, will load if needed)
        company: Company (optional, will load if needed)
        posts: List of LinkedIn post dicts with 'text' and optionally 'date'

    Returns:
        Dict with LinkedIn analysis results
    """
    # Load contact if needed
    if not contact and lead.contact_id:
        result = await session.execute(
            select(Contact).where(Contact.id == lead.contact_id)
        )
        contact = result.scalar_one_or_none()

    if not contact:
        return {"error": "No contact found"}

    # Load company if needed
    if not company:
        result = await session.execute(
            select(Company).where(Company.id == lead.company_id)
        )
        company = result.scalar_one_or_none()

    if not company:
        return {"error": "No company found"}

    if not posts:
        return {
            "themes": [],
            "interests": [],
            "tone": "unknown",
            "engagement_style": "unknown",
            "personalization_hooks": [],
            "note": "No LinkedIn posts provided",
        }

    # Analyze posts
    analysis = await analyze_linkedin_posts(contact, company, posts)

    # Store in lead
    lead.linkedin_posts = posts[:10]  # Store up to 10 posts

    return analysis


async def get_research_stats(session: AsyncSession) -> dict[str, Any]:
    """Get research statistics."""
    from sqlalchemy import func

    # Total researched
    researched_result = await session.execute(
        select(func.count(Lead.id)).where(Lead.research_summary.isnot(None))
    )
    total_researched = researched_result.scalar_one()

    # Total leads
    total_result = await session.execute(select(func.count(Lead.id)))
    total_leads = total_result.scalar_one()

    # Average KPIs per lead
    leads_with_kpis_result = await session.execute(
        select(Lead.kpis).where(Lead.kpis.isnot(None))
    )
    all_kpis = [len(row[0]) for row in leads_with_kpis_result.fetchall() if row[0]]
    avg_kpis = sum(all_kpis) / len(all_kpis) if all_kpis else 0

    return {
        "total_leads": total_leads,
        "total_researched": total_researched,
        "research_rate": (total_researched / total_leads * 100) if total_leads > 0 else 0,
        "average_kpis_per_lead": round(avg_kpis, 1),
    }
