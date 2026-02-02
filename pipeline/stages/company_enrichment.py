"""Stage 2: Company enrichment - firmographics, technographics, signals."""

import logging
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.models import Company, Lead, LeadStatus
from core.schemas import CompanyEnrichment
from enrichment.cascade import cascade_manager
from utils.cost_tracker import BudgetExceeded, cost_tracker

logger = logging.getLogger(__name__)


async def enrich_company(
    session: AsyncSession,
    company: Company,
    force: bool = False,
) -> tuple[bool, CompanyEnrichment | None]:
    """
    Enrich a single company with firmographics, technographics, and signals.

    Args:
        session: Database session
        company: Company to enrich
        force: Re-enrich even if already enriched

    Returns:
        Tuple of (success, enrichment_data)
    """
    # Check if already enriched
    if company.enriched_at and not force:
        logger.debug(f"Company {company.domain} already enriched, skipping")
        return True, None

    # Check budget
    if not await cost_tracker.check_budget(session):
        raise BudgetExceeded(
            "Monthly budget exceeded",
            spent=await cost_tracker.get_monthly_spend(session),
            budget=(await cost_tracker.get_budget_status(session))["monthly_budget"],
        )

    logger.info(f"Enriching company: {company.domain}")

    # Run cascade enrichment
    enrichment, providers = await cascade_manager.enrich_company(
        domain=company.domain,
        session=session,
        company_id=company.id,
    )

    if not enrichment:
        logger.warning(f"No enrichment data for company {company.domain}")
        return False, None

    # Update company with enrichment data
    _apply_company_enrichment(company, enrichment, providers)

    return True, enrichment


def _apply_company_enrichment(
    company: Company,
    enrichment: CompanyEnrichment,
    providers: list[str],
) -> None:
    """Apply enrichment data to company model."""
    # Firmographics
    if enrichment.industry:
        company.industry = enrichment.industry
    if enrichment.sub_industry:
        company.sub_industry = enrichment.sub_industry
    if enrichment.employee_count:
        company.employee_count = enrichment.employee_count
    if enrichment.employee_range:
        company.employee_range = enrichment.employee_range
    if enrichment.revenue:
        company.revenue = enrichment.revenue
    if enrichment.revenue_range:
        company.revenue_range = enrichment.revenue_range
    if enrichment.founded_year:
        company.founded_year = enrichment.founded_year

    # Location
    if enrichment.hq_city:
        company.hq_city = enrichment.hq_city
    if enrichment.hq_state:
        company.hq_state = enrichment.hq_state
    if enrichment.hq_country:
        company.hq_country = enrichment.hq_country
    if enrichment.hq_region:
        company.hq_region = enrichment.hq_region

    # Funding
    if enrichment.total_funding:
        company.total_funding = enrichment.total_funding
    if enrichment.last_funding_date:
        company.last_funding_date = enrichment.last_funding_date
    if enrichment.last_funding_amount:
        company.last_funding_amount = enrichment.last_funding_amount
    if enrichment.last_funding_type:
        company.last_funding_type = enrichment.last_funding_type
    if enrichment.funding_stage:
        company.funding_stage = enrichment.funding_stage

    # Technographics
    if enrichment.tech_stack:
        company.tech_stack = enrichment.tech_stack
    if enrichment.crm_platform:
        company.crm_platform = enrichment.crm_platform
    if enrichment.marketing_automation:
        company.marketing_automation = enrichment.marketing_automation

    # Signals
    if enrichment.is_hiring is not None:
        company.is_hiring = enrichment.is_hiring
    if enrichment.open_positions:
        company.open_positions = enrichment.open_positions
    if enrichment.hiring_departments:
        company.hiring_departments = enrichment.hiring_departments

    # Social
    if enrichment.linkedin_url:
        company.linkedin_url = enrichment.linkedin_url
    if enrichment.twitter_url:
        company.twitter_url = enrichment.twitter_url
    if enrichment.website_description:
        company.website_description = enrichment.website_description

    # Metadata
    company.enriched_at = datetime.utcnow()
    company.enrichment_sources = providers


async def enrich_companies_batch(
    session: AsyncSession,
    limit: int = 100,
    force: bool = False,
) -> dict[str, Any]:
    """
    Enrich a batch of companies.

    Args:
        session: Database session
        limit: Maximum companies to process
        force: Re-enrich even if already enriched

    Returns:
        Dict with batch statistics
    """
    # Get companies needing enrichment
    query = select(Company)
    if not force:
        query = query.where(Company.enriched_at.is_(None))
    query = query.limit(limit)

    result = await session.execute(query)
    companies = list(result.scalars().all())

    if not companies:
        logger.info("No companies to enrich")
        return {"total": 0, "enriched": 0, "failed": 0}

    logger.info(f"Enriching {len(companies)} companies")

    enriched = 0
    failed = 0
    errors: list[str] = []

    for company in companies:
        try:
            success, _ = await enrich_company(session, company, force=force)
            if success:
                enriched += 1
            else:
                failed += 1
        except BudgetExceeded as e:
            logger.error(f"Budget exceeded: {e}")
            errors.append(f"Budget exceeded after {enriched} companies")
            break
        except Exception as e:
            logger.error(f"Error enriching {company.domain}: {e}")
            failed += 1
            errors.append(f"{company.domain}: {str(e)}")

    return {
        "total": len(companies),
        "enriched": enriched,
        "failed": failed,
        "errors": errors[:20],
    }


async def enrich_leads_companies(
    session: AsyncSession,
    leads: list[Lead] | None = None,
    status_filter: LeadStatus | None = LeadStatus.NEW,
    limit: int = 100,
    force: bool = False,
) -> dict[str, Any]:
    """
    Enrich companies for a set of leads.

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
        query = select(Lead).options()
        if status_filter:
            query = query.where(Lead.status == status_filter)
        query = query.limit(limit)
        result = await session.execute(query)
        leads = list(result.scalars().all())

    if not leads:
        logger.info("No leads to process")
        return {"total": 0, "enriched": 0, "failed": 0}

    logger.info(f"Enriching companies for {len(leads)} leads")

    # Get unique company IDs
    company_ids = list({lead.company_id for lead in leads})

    # Load companies
    result = await session.execute(
        select(Company).where(Company.id.in_(company_ids))
    )
    companies = {c.id: c for c in result.scalars().all()}

    enriched = 0
    failed = 0
    errors: list[str] = []

    for lead in leads:
        company = companies.get(lead.company_id)
        if not company:
            failed += 1
            continue

        try:
            # Update lead status
            lead.status = LeadStatus.ENRICHING

            success, _ = await enrich_company(session, company, force=force)
            if success:
                enriched += 1
            else:
                failed += 1
        except BudgetExceeded as e:
            logger.error(f"Budget exceeded: {e}")
            errors.append(f"Budget exceeded after {enriched} companies")
            break
        except Exception as e:
            logger.error(f"Error enriching company {company.domain}: {e}")
            failed += 1
            errors.append(f"{company.domain}: {str(e)}")

    return {
        "total": len(leads),
        "enriched": enriched,
        "failed": failed,
        "errors": errors[:20],
    }
