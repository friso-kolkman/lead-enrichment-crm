"""Stage 7: AI-generated messaging - icebreakers, email variants, LinkedIn messages."""

import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ai.generator import (
    generate_all_content,
    generate_email_variants,
    generate_icebreakers,
    generate_linkedin_message,
)
from core.models import Company, Contact, Lead, LeadStatus
from utils.cost_tracker import BudgetExceeded, cost_tracker

logger = logging.getLogger(__name__)


async def generate_lead_messaging(
    session: AsyncSession,
    lead: Lead,
    company: Company | None = None,
    contact: Contact | None = None,
    value_prop: str = "We help companies like yours improve efficiency and drive growth.",
    force: bool = False,
) -> dict[str, Any]:
    """
    Generate all messaging for a lead.

    Args:
        session: Database session
        lead: Lead to generate messaging for
        company: Company (optional, will load if needed)
        contact: Contact (optional, will load if needed)
        value_prop: Value proposition to include in emails
        force: Re-generate even if already exists

    Returns:
        Dict with generated messaging
    """
    # Check if already generated
    if lead.icebreakers and lead.email_variants and not force:
        logger.debug(f"Lead {lead.id} already has messaging, skipping")
        return {
            "icebreakers": lead.icebreakers,
            "email_variants": lead.email_variants,
            "linkedin_message": lead.linkedin_message,
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

    if not contact:
        logger.error(f"Contact not found for lead {lead.id}")
        return {"error": "Contact not found"}

    logger.info(f"Generating messaging for lead {lead.id} ({company.domain})")

    # Generate all content
    content = await generate_all_content(
        lead=lead,
        contact=contact,
        company=company,
        value_prop=value_prop,
    )

    # Update lead
    lead.icebreakers = content.get("icebreakers", [])
    lead.email_variants = content.get("email_variants", {})
    lead.linkedin_message = content.get("linkedin_message", "")
    lead.status = LeadStatus.READY

    # If research wasn't done yet, update that too
    if not lead.research_summary and content.get("research_summary"):
        lead.research_summary = content["research_summary"]
        lead.kpis = content.get("kpis", [])

    # Log cost (approximate - multiple API calls)
    await cost_tracker.log_request(
        session=session,
        provider="openai",
        endpoint="chat/completions",
        entity_type="lead",
        entity_id=lead.id,
        success=True,
        cost_override=0.01,  # Approximate cost for all messaging
    )

    logger.info(f"Messaging generated for lead {lead.id}")

    return content


async def generate_icebreakers_only(
    session: AsyncSession,
    lead: Lead,
    company: Company | None = None,
    contact: Contact | None = None,
    force: bool = False,
) -> list[str]:
    """
    Generate only icebreakers for a lead.

    Args:
        session: Database session
        lead: Lead
        company: Company (optional)
        contact: Contact (optional)
        force: Re-generate even if exists

    Returns:
        List of icebreaker strings
    """
    if lead.icebreakers and not force:
        return lead.icebreakers

    # Load company if needed
    if not company:
        result = await session.execute(
            select(Company).where(Company.id == lead.company_id)
        )
        company = result.scalar_one_or_none()

    if not company:
        return []

    # Load contact if needed
    if not contact and lead.contact_id:
        result = await session.execute(
            select(Contact).where(Contact.id == lead.contact_id)
        )
        contact = result.scalar_one_or_none()

    if not contact:
        return []

    icebreakers = await generate_icebreakers(
        lead=lead,
        contact=contact,
        company=company,
        research_summary=lead.research_summary or "",
        trigger_events=lead.trigger_events,
    )

    lead.icebreakers = icebreakers
    return icebreakers


async def generate_emails_only(
    session: AsyncSession,
    lead: Lead,
    company: Company | None = None,
    contact: Contact | None = None,
    value_prop: str = "We help companies like yours improve efficiency and drive growth.",
    force: bool = False,
) -> dict[str, dict[str, str]]:
    """
    Generate only email variants for a lead.

    Args:
        session: Database session
        lead: Lead
        company: Company (optional)
        contact: Contact (optional)
        value_prop: Value proposition
        force: Re-generate even if exists

    Returns:
        Dict mapping tier to email dict
    """
    if lead.email_variants and not force:
        return lead.email_variants

    # Load company if needed
    if not company:
        result = await session.execute(
            select(Company).where(Company.id == lead.company_id)
        )
        company = result.scalar_one_or_none()

    if not company:
        return {}

    # Load contact if needed
    if not contact and lead.contact_id:
        result = await session.execute(
            select(Contact).where(Contact.id == lead.contact_id)
        )
        contact = result.scalar_one_or_none()

    if not contact:
        return {}

    # Build research data from lead
    research_data = {
        "summary": lead.research_summary or "",
        "kpis": lead.kpis or [],
        "pain_points": [],
        "trigger_events": lead.trigger_events or [],
    }

    email_variants = await generate_email_variants(
        lead=lead,
        contact=contact,
        company=company,
        research_data=research_data,
        icebreaker=lead.icebreakers[0] if lead.icebreakers else None,
        value_prop=value_prop,
    )

    lead.email_variants = email_variants
    return email_variants


async def generate_messaging_batch(
    session: AsyncSession,
    leads: list[Lead] | None = None,
    status_filter: LeadStatus | None = LeadStatus.RESEARCHED,
    limit: int = 50,
    value_prop: str = "We help companies like yours improve efficiency and drive growth.",
    force: bool = False,
) -> dict[str, Any]:
    """
    Generate messaging for a batch of leads.

    Args:
        session: Database session
        leads: Specific leads to process (optional)
        status_filter: Only process leads with this status
        limit: Maximum leads to process
        value_prop: Value proposition
        force: Re-generate even if exists

    Returns:
        Dict with generation statistics
    """
    # Get leads to process
    if leads is None:
        query = select(Lead).where(Lead.contact_id.isnot(None))
        if status_filter:
            query = query.where(Lead.status == status_filter)
        if not force:
            query = query.where(Lead.icebreakers.is_(None))
        query = query.limit(limit)
        result = await session.execute(query)
        leads = list(result.scalars().all())

    if not leads:
        logger.info("No leads for messaging generation")
        return {"total": 0, "generated": 0, "failed": 0}

    logger.info(f"Generating messaging for {len(leads)} leads")

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

    generated = 0
    failed = 0
    errors: list[str] = []

    for lead in leads:
        try:
            company = companies.get(lead.company_id)
            contact = contacts.get(lead.contact_id) if lead.contact_id else None

            if not contact:
                failed += 1
                errors.append(f"Lead {lead.id}: No contact")
                continue

            result = await generate_lead_messaging(
                session=session,
                lead=lead,
                company=company,
                contact=contact,
                value_prop=value_prop,
                force=force,
            )

            if "error" not in result:
                generated += 1
            else:
                failed += 1
                errors.append(f"Lead {lead.id}: {result['error']}")

        except BudgetExceeded as e:
            logger.error(f"Budget exceeded: {e}")
            errors.append(f"Budget exceeded after {generated} leads")
            break
        except Exception as e:
            logger.error(f"Error generating messaging for lead {lead.id}: {e}")
            failed += 1
            errors.append(f"Lead {lead.id}: {str(e)}")

    return {
        "total": len(leads),
        "generated": generated,
        "failed": failed,
        "errors": errors[:20],
    }


async def get_messaging_stats(session: AsyncSession) -> dict[str, Any]:
    """Get messaging generation statistics."""
    from sqlalchemy import func

    # Total with icebreakers
    icebreakers_result = await session.execute(
        select(func.count(Lead.id)).where(Lead.icebreakers.isnot(None))
    )
    total_with_icebreakers = icebreakers_result.scalar_one()

    # Total with email variants
    emails_result = await session.execute(
        select(func.count(Lead.id)).where(Lead.email_variants.isnot(None))
    )
    total_with_emails = emails_result.scalar_one()

    # Total with LinkedIn message
    linkedin_result = await session.execute(
        select(func.count(Lead.id)).where(Lead.linkedin_message.isnot(None))
    )
    total_with_linkedin = linkedin_result.scalar_one()

    # Total leads with contacts
    contacts_result = await session.execute(
        select(func.count(Lead.id)).where(Lead.contact_id.isnot(None))
    )
    total_with_contacts = contacts_result.scalar_one()

    return {
        "total_leads_with_contacts": total_with_contacts,
        "total_with_icebreakers": total_with_icebreakers,
        "total_with_email_variants": total_with_emails,
        "total_with_linkedin_message": total_with_linkedin,
        "icebreaker_rate": (
            total_with_icebreakers / total_with_contacts * 100
            if total_with_contacts > 0
            else 0
        ),
        "email_rate": (
            total_with_emails / total_with_contacts * 100
            if total_with_contacts > 0
            else 0
        ),
    }
