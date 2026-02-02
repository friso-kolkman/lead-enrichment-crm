"""Stage 8: CRM sync to Attio."""

import logging
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.models import Company, Contact, Lead, LeadStatus, LeadTier
from core.schemas import SyncResult
from integrations.attio import AttioClient, attio_client

logger = logging.getLogger(__name__)


async def sync_lead_to_crm(
    session: AsyncSession,
    lead: Lead,
    company: Company | None = None,
    contact: Contact | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Sync a single lead to Attio CRM.

    Args:
        session: Database session
        lead: Lead to sync
        company: Company (optional, will load if needed)
        contact: Contact (optional, will load if needed)
        dry_run: If True, don't actually sync

    Returns:
        Dict with sync result
    """
    # Load company if needed
    if not company:
        result = await session.execute(
            select(Company).where(Company.id == lead.company_id)
        )
        company = result.scalar_one_or_none()

    if not company:
        logger.error(f"Company not found for lead {lead.id}")
        return {"success": False, "error": "Company not found"}

    # Load contact if needed
    if not contact and lead.contact_id:
        result = await session.execute(
            select(Contact).where(Contact.id == lead.contact_id)
        )
        contact = result.scalar_one_or_none()

    logger.info(f"Syncing lead {lead.id} ({company.domain}) to Attio")

    if dry_run:
        return {
            "success": True,
            "dry_run": True,
            "company_domain": company.domain,
            "contact_email": contact.email if contact else None,
        }

    try:
        # Sync company first
        company_record_id = await attio_client.upsert_company(
            domain=company.domain,
            name=company.name,
            industry=company.industry,
            employee_count=company.employee_count,
            revenue=company.revenue,
            hq_city=company.hq_city,
            hq_country=company.hq_country,
            tech_stack=company.tech_stack,
            is_hiring=company.is_hiring,
            linkedin_url=company.linkedin_url,
        )

        # Sync contact if available
        contact_record_id = None
        if contact and contact.email:
            contact_record_id = await attio_client.upsert_contact(
                email=contact.email,
                first_name=contact.first_name,
                last_name=contact.last_name,
                title=contact.title,
                phone=contact.mobile_phone,
                linkedin_url=contact.linkedin_url,
                company_record_id=company_record_id,
            )

        # Create or update lead/opportunity
        lead_data = {
            "company_record_id": company_record_id,
            "contact_record_id": contact_record_id,
            "score": lead.total_score,
            "tier": lead.tier.value if lead.tier else None,
            "status": lead.status.value if lead.status else None,
            "research_summary": lead.research_summary,
        }

        lead_record_id = await attio_client.upsert_lead(lead_data)

        # Update lead with Attio record ID
        lead.attio_record_id = lead_record_id
        lead.synced_at = datetime.utcnow()
        lead.status = LeadStatus.SYNCED

        logger.info(f"Lead {lead.id} synced to Attio: {lead_record_id}")

        return {
            "success": True,
            "lead_record_id": lead_record_id,
            "company_record_id": company_record_id,
            "contact_record_id": contact_record_id,
        }

    except Exception as e:
        logger.error(f"Error syncing lead {lead.id} to Attio: {e}")
        return {"success": False, "error": str(e)}


async def sync_leads_batch(
    session: AsyncSession,
    leads: list[Lead] | None = None,
    status_filter: LeadStatus | None = LeadStatus.READY,
    min_score: int | None = None,
    tier_filter: LeadTier | None = None,
    limit: int = 100,
    dry_run: bool = False,
) -> SyncResult:
    """
    Sync a batch of leads to Attio CRM.

    Args:
        session: Database session
        leads: Specific leads to sync (optional)
        status_filter: Only sync leads with this status
        min_score: Only sync leads with score >= this value
        tier_filter: Only sync leads with this tier
        limit: Maximum leads to sync
        dry_run: If True, don't actually sync

    Returns:
        SyncResult with statistics
    """
    # Get leads to sync
    if leads is None:
        query = select(Lead)
        if status_filter:
            query = query.where(Lead.status == status_filter)
        if min_score is not None:
            query = query.where(Lead.total_score >= min_score)
        if tier_filter:
            query = query.where(Lead.tier == tier_filter)
        query = query.where(Lead.attio_record_id.is_(None))
        query = query.limit(limit)
        result = await session.execute(query)
        leads = list(result.scalars().all())

    if not leads:
        logger.info("No leads to sync")
        return SyncResult(total=0, created=0, updated=0, failed=0, errors=[])

    logger.info(f"Syncing {len(leads)} leads to Attio")

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

    created = 0
    updated = 0
    failed = 0
    errors: list[str] = []

    for lead in leads:
        try:
            company = companies.get(lead.company_id)
            contact = contacts.get(lead.contact_id) if lead.contact_id else None

            result = await sync_lead_to_crm(
                session=session,
                lead=lead,
                company=company,
                contact=contact,
                dry_run=dry_run,
            )

            if result.get("success"):
                if lead.attio_record_id:
                    updated += 1
                else:
                    created += 1
            else:
                failed += 1
                errors.append(f"Lead {lead.id}: {result.get('error', 'Unknown error')}")

        except Exception as e:
            logger.error(f"Error syncing lead {lead.id}: {e}")
            failed += 1
            errors.append(f"Lead {lead.id}: {str(e)}")

    return SyncResult(
        total=len(leads),
        created=created,
        updated=updated,
        failed=failed,
        errors=errors[:20],
    )


async def sync_company_to_crm(
    company: Company,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Sync a single company to Attio."""
    if dry_run:
        return {"success": True, "dry_run": True, "domain": company.domain}

    try:
        record_id = await attio_client.upsert_company(
            domain=company.domain,
            name=company.name,
            industry=company.industry,
            employee_count=company.employee_count,
            revenue=company.revenue,
            hq_city=company.hq_city,
            hq_country=company.hq_country,
            tech_stack=company.tech_stack,
            is_hiring=company.is_hiring,
            linkedin_url=company.linkedin_url,
        )
        return {"success": True, "record_id": record_id}
    except Exception as e:
        return {"success": False, "error": str(e)}


async def sync_contact_to_crm(
    contact: Contact,
    company_record_id: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Sync a single contact to Attio."""
    if dry_run:
        return {"success": True, "dry_run": True, "email": contact.email}

    try:
        record_id = await attio_client.upsert_contact(
            email=contact.email,
            first_name=contact.first_name,
            last_name=contact.last_name,
            title=contact.title,
            phone=contact.mobile_phone,
            linkedin_url=contact.linkedin_url,
            company_record_id=company_record_id,
        )
        return {"success": True, "record_id": record_id}
    except Exception as e:
        return {"success": False, "error": str(e)}


async def get_sync_stats(session: AsyncSession) -> dict[str, Any]:
    """Get CRM sync statistics."""
    from sqlalchemy import func

    # Total synced
    synced_result = await session.execute(
        select(func.count(Lead.id)).where(Lead.attio_record_id.isnot(None))
    )
    total_synced = synced_result.scalar_one()

    # Total leads
    total_result = await session.execute(select(func.count(Lead.id)))
    total_leads = total_result.scalar_one()

    # Synced by tier
    tier_result = await session.execute(
        select(Lead.tier, func.count(Lead.id))
        .where(Lead.attio_record_id.isnot(None))
        .group_by(Lead.tier)
    )
    by_tier = {row[0].value if row[0] else "untiered": row[1] for row in tier_result.fetchall()}

    return {
        "total_leads": total_leads,
        "total_synced": total_synced,
        "sync_rate": (total_synced / total_leads * 100) if total_leads > 0 else 0,
        "synced_by_tier": by_tier,
    }
