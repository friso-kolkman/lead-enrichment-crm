"""Stage 3: Contact enrichment - title, email, mobile, LinkedIn."""

import logging
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.models import Company, Contact, Lead, LeadStatus
from core.schemas import ContactEnrichment
from enrichment.cascade import cascade_manager
from utils.cost_tracker import BudgetExceeded, cost_tracker

logger = logging.getLogger(__name__)


async def enrich_contact(
    session: AsyncSession,
    contact: Contact,
    company: Company | None = None,
    force: bool = False,
) -> tuple[bool, ContactEnrichment | None]:
    """
    Enrich a single contact with title, email, mobile, LinkedIn.

    Args:
        session: Database session
        contact: Contact to enrich
        company: Company for domain (optional, will load if needed)
        force: Re-enrich even if already enriched

    Returns:
        Tuple of (success, enrichment_data)
    """
    # Check if already enriched
    if contact.enriched_at and not force:
        logger.debug(f"Contact {contact.email} already enriched, skipping")
        return True, None

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
            select(Company).where(Company.id == contact.company_id)
        )
        company = result.scalar_one_or_none()

    domain = company.domain if company else None

    logger.info(f"Enriching contact: {contact.email or contact.linkedin_url or contact.full_name}")

    # Run cascade enrichment
    enrichment, providers = await cascade_manager.enrich_contact(
        email=contact.email,
        linkedin_url=contact.linkedin_url,
        first_name=contact.first_name,
        last_name=contact.last_name,
        domain=domain,
        session=session,
        contact_id=contact.id,
    )

    if not enrichment:
        logger.warning(f"No enrichment data for contact {contact.id}")
        return False, None

    # Update contact with enrichment data
    _apply_contact_enrichment(contact, enrichment, providers)

    return True, enrichment


def _apply_contact_enrichment(
    contact: Contact,
    enrichment: ContactEnrichment,
    providers: list[str],
) -> None:
    """Apply enrichment data to contact model."""
    # Name
    if enrichment.first_name and not contact.first_name:
        contact.first_name = enrichment.first_name
    if enrichment.last_name and not contact.last_name:
        contact.last_name = enrichment.last_name
    if enrichment.full_name and not contact.full_name:
        contact.full_name = enrichment.full_name

    # Professional
    if enrichment.title:
        contact.title = enrichment.title
    if enrichment.normalized_title:
        contact.normalized_title = enrichment.normalized_title
    if enrichment.seniority_level:
        contact.seniority_level = enrichment.seniority_level
    if enrichment.department:
        contact.department = enrichment.department

    # Contact info
    if enrichment.email and not contact.email:
        contact.email = enrichment.email
    if enrichment.mobile_phone:
        contact.mobile_phone = enrichment.mobile_phone
    if enrichment.work_phone:
        contact.work_phone = enrichment.work_phone

    # Social
    if enrichment.linkedin_url and not contact.linkedin_url:
        contact.linkedin_url = enrichment.linkedin_url
    if enrichment.twitter_url and not contact.twitter_url:
        contact.twitter_url = enrichment.twitter_url

    # Metadata
    contact.enriched_at = datetime.utcnow()
    contact.enrichment_sources = providers


async def enrich_contacts_batch(
    session: AsyncSession,
    limit: int = 100,
    force: bool = False,
) -> dict[str, Any]:
    """
    Enrich a batch of contacts.

    Args:
        session: Database session
        limit: Maximum contacts to process
        force: Re-enrich even if already enriched

    Returns:
        Dict with batch statistics
    """
    # Get contacts needing enrichment
    query = select(Contact)
    if not force:
        query = query.where(Contact.enriched_at.is_(None))
    query = query.limit(limit)

    result = await session.execute(query)
    contacts = list(result.scalars().all())

    if not contacts:
        logger.info("No contacts to enrich")
        return {"total": 0, "enriched": 0, "failed": 0}

    logger.info(f"Enriching {len(contacts)} contacts")

    # Load companies for all contacts
    company_ids = list({c.company_id for c in contacts})
    company_result = await session.execute(
        select(Company).where(Company.id.in_(company_ids))
    )
    companies = {c.id: c for c in company_result.scalars().all()}

    enriched = 0
    failed = 0
    errors: list[str] = []

    for contact in contacts:
        try:
            company = companies.get(contact.company_id)
            success, _ = await enrich_contact(session, contact, company, force=force)
            if success:
                enriched += 1
            else:
                failed += 1
        except BudgetExceeded as e:
            logger.error(f"Budget exceeded: {e}")
            errors.append(f"Budget exceeded after {enriched} contacts")
            break
        except Exception as e:
            logger.error(f"Error enriching contact {contact.id}: {e}")
            failed += 1
            errors.append(f"Contact {contact.id}: {str(e)}")

    return {
        "total": len(contacts),
        "enriched": enriched,
        "failed": failed,
        "errors": errors[:20],
    }


async def enrich_leads_contacts(
    session: AsyncSession,
    leads: list[Lead] | None = None,
    status_filter: LeadStatus | None = LeadStatus.ENRICHING,
    limit: int = 100,
    force: bool = False,
) -> dict[str, Any]:
    """
    Enrich contacts for a set of leads.

    Args:
        session: Database session
        leads: Specific leads to process (optional)
        status_filter: Only process leads with this status
        limit: Maximum leads to process
        force: Re-enrich even if already enriched

    Returns:
        Dict with enrichment statistics
    """
    # Get leads to process
    if leads is None:
        query = select(Lead)
        if status_filter:
            query = query.where(Lead.status == status_filter)
        query = query.where(Lead.contact_id.isnot(None))
        query = query.limit(limit)
        result = await session.execute(query)
        leads = list(result.scalars().all())

    if not leads:
        logger.info("No leads to process")
        return {"total": 0, "enriched": 0, "failed": 0, "no_contact": 0}

    logger.info(f"Enriching contacts for {len(leads)} leads")

    # Get unique contact IDs
    contact_ids = [lead.contact_id for lead in leads if lead.contact_id]
    if not contact_ids:
        return {"total": len(leads), "enriched": 0, "failed": 0, "no_contact": len(leads)}

    # Load contacts
    result = await session.execute(
        select(Contact).where(Contact.id.in_(contact_ids))
    )
    contacts = {c.id: c for c in result.scalars().all()}

    # Load companies
    company_ids = list({c.company_id for c in contacts.values()})
    company_result = await session.execute(
        select(Company).where(Company.id.in_(company_ids))
    )
    companies = {c.id: c for c in company_result.scalars().all()}

    enriched = 0
    failed = 0
    no_contact = 0
    errors: list[str] = []

    for lead in leads:
        if not lead.contact_id:
            no_contact += 1
            continue

        contact = contacts.get(lead.contact_id)
        if not contact:
            failed += 1
            continue

        try:
            company = companies.get(contact.company_id)
            success, _ = await enrich_contact(session, contact, company, force=force)
            if success:
                enriched += 1
        except BudgetExceeded as e:
            logger.error(f"Budget exceeded: {e}")
            errors.append(f"Budget exceeded after {enriched} contacts")
            break
        except Exception as e:
            logger.error(f"Error enriching contact {contact.id}: {e}")
            failed += 1
            errors.append(f"Contact {contact.id}: {str(e)}")

    return {
        "total": len(leads),
        "enriched": enriched,
        "failed": failed,
        "no_contact": no_contact,
        "errors": errors[:20],
    }


async def find_contacts_for_leads(
    session: AsyncSession,
    leads: list[Lead] | None = None,
    limit: int = 100,
    titles: list[str] | None = None,
    seniority_levels: list[str] | None = None,
) -> dict[str, Any]:
    """
    Find contacts for leads that don't have one using Apollo search.

    Args:
        session: Database session
        leads: Specific leads to process (optional)
        limit: Maximum leads to process
        titles: Target job titles to search for
        seniority_levels: Target seniority levels

    Returns:
        Dict with search statistics
    """
    from enrichment.apollo import ApolloProvider
    from config import settings

    # Get leads without contacts
    if leads is None:
        query = (
            select(Lead)
            .where(Lead.contact_id.is_(None))
            .limit(limit)
        )
        result = await session.execute(query)
        leads = list(result.scalars().all())

    if not leads:
        logger.info("No leads without contacts")
        return {"total": 0, "found": 0, "failed": 0}

    # Get unique company IDs
    company_ids = list({lead.company_id for lead in leads})

    # Load companies
    result = await session.execute(
        select(Company).where(Company.id.in_(company_ids))
    )
    companies = {c.id: c for c in result.scalars().all()}

    # Initialize Apollo if available
    if not settings.apollo.api_key:
        return {"total": len(leads), "found": 0, "failed": 0, "error": "Apollo not configured"}

    apollo = ApolloProvider(settings.apollo.api_key)
    found = 0
    failed = 0
    errors: list[str] = []

    try:
        for lead in leads:
            company = companies.get(lead.company_id)
            if not company:
                failed += 1
                continue

            try:
                # Check budget
                if not await cost_tracker.check_budget(session):
                    raise BudgetExceeded(
                        "Budget exceeded",
                        spent=await cost_tracker.get_monthly_spend(session),
                        budget=(await cost_tracker.get_budget_status(session))["monthly_budget"],
                    )

                # Search for people at this company
                people = await apollo.search_people(
                    domain=company.domain,
                    titles=titles,
                    seniority_levels=seniority_levels,
                    limit=1,
                    session=session,
                )

                if people:
                    # Create contact from first result
                    person = people[0]
                    contact = Contact(
                        company_id=company.id,
                        first_name=person.first_name,
                        last_name=person.last_name,
                        full_name=person.full_name,
                        email=person.email,
                        title=person.title,
                        normalized_title=person.normalized_title,
                        seniority_level=person.seniority_level,
                        department=person.department,
                        mobile_phone=person.mobile_phone,
                        linkedin_url=person.linkedin_url,
                        enriched_at=datetime.utcnow(),
                        enrichment_sources=["apollo"],
                    )
                    session.add(contact)
                    await session.flush()

                    lead.contact_id = contact.id
                    found += 1
                else:
                    failed += 1

            except BudgetExceeded as e:
                logger.error(f"Budget exceeded: {e}")
                errors.append(f"Budget exceeded after finding {found} contacts")
                break
            except Exception as e:
                logger.error(f"Error finding contacts for {company.domain}: {e}")
                failed += 1
                errors.append(f"{company.domain}: {str(e)}")

    finally:
        await apollo.close()

    return {
        "total": len(leads),
        "found": found,
        "failed": failed,
        "errors": errors[:20],
    }
