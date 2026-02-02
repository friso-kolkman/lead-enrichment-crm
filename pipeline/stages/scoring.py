"""Stage 5: ICP scoring and lead tiering."""

import logging
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from core.models import Company, Contact, Lead, LeadStatus, LeadTier

logger = logging.getLogger(__name__)


def calculate_industry_score(company: Company) -> int:
    """Calculate industry match score (0-25 points)."""
    if not company.industry:
        return 0

    industry_lower = company.industry.lower()
    max_score = settings.scoring.industry_match

    # Check for exact match
    for target in settings.icp.target_industries:
        if target.lower() in industry_lower or industry_lower in target.lower():
            return max_score

    # Check exclusions
    for excluded in settings.icp.excluded_industries:
        if excluded.lower() in industry_lower:
            return 0

    # Partial match
    return max_score // 2


def calculate_revenue_score(company: Company) -> int:
    """Calculate revenue fit score (0-20 points)."""
    if not company.revenue:
        return 0

    max_score = settings.scoring.revenue_fit
    min_rev = settings.icp.min_revenue
    max_rev = settings.icp.max_revenue

    if min_rev <= company.revenue <= max_rev:
        return max_score

    # Within 2x range
    if min_rev / 2 <= company.revenue <= max_rev * 2:
        return max_score // 2

    return 0


def calculate_tech_stack_score(company: Company) -> int:
    """Calculate tech stack match score (0-20 points)."""
    if not company.tech_stack:
        return 0

    max_score = settings.scoring.tech_stack_match
    target_tech = [t.lower() for t in settings.icp.target_technologies]

    # Flatten tech stack
    all_tech: list[str] = []
    if isinstance(company.tech_stack, dict):
        for category_tech in company.tech_stack.values():
            if isinstance(category_tech, list):
                all_tech.extend([t.lower() for t in category_tech])
    elif isinstance(company.tech_stack, list):
        all_tech = [t.lower() for t in company.tech_stack]

    # Count matches
    matches = sum(1 for tech in target_tech if any(tech in t for t in all_tech))

    if matches >= 3:
        return max_score
    elif matches >= 2:
        return int(max_score * 0.75)
    elif matches >= 1:
        return max_score // 2

    return 0


def calculate_employee_score(company: Company) -> int:
    """Calculate employee fit score (0-15 points)."""
    if not company.employee_count:
        return 0

    max_score = settings.scoring.employee_fit
    min_emp = settings.icp.min_employees
    max_emp = settings.icp.max_employees

    if min_emp <= company.employee_count <= max_emp:
        return max_score

    # Within 2x range
    if min_emp / 2 <= company.employee_count <= max_emp * 2:
        return max_score // 2

    return 0


def calculate_geography_score(company: Company) -> int:
    """Calculate geography match score (0-10 points)."""
    max_score = settings.scoring.geography_match

    if company.hq_country:
        country_upper = company.hq_country.upper()
        if country_upper in [c.upper() for c in settings.icp.target_countries]:
            return max_score

    if company.hq_region:
        region_lower = company.hq_region.lower()
        for target_region in settings.icp.target_regions:
            if target_region.lower() in region_lower or region_lower in target_region.lower():
                return int(max_score * 0.75)

    return 0


def calculate_title_score(contact: Contact | None) -> int:
    """Calculate title/seniority match score (0-10 points)."""
    if not contact or not contact.title:
        return 0

    max_score = settings.scoring.title_match
    title_lower = contact.title.lower()

    # Check for target titles
    for target_title in settings.icp.target_titles:
        if target_title.lower() in title_lower:
            return max_score

    # Check for target departments
    if contact.department:
        dept_lower = contact.department.lower()
        for target_dept in settings.icp.target_departments:
            if target_dept.lower() in dept_lower:
                return int(max_score * 0.75)

    # Check seniority
    senior_levels = ["c_level", "vp", "director", "head"]
    if contact.seniority_level and contact.seniority_level.lower() in senior_levels:
        return max_score // 2

    return 0


def calculate_intent_signals(company: Company) -> tuple[int, dict[str, int]]:
    """
    Calculate intent signal bonuses.

    Returns:
        Tuple of (total_bonus, breakdown_dict)
    """
    bonuses: dict[str, int] = {}
    total_bonus = 0

    # Recent funding (< 6 months)
    if company.last_funding_date:
        six_months_ago = datetime.utcnow() - timedelta(days=180)
        if company.last_funding_date > six_months_ago:
            bonus = settings.scoring.recent_funding_bonus
            bonuses["recent_funding"] = bonus
            total_bonus += bonus

    # Is hiring
    if company.is_hiring:
        bonus = settings.scoring.hiring_bonus
        bonuses["is_hiring"] = bonus
        total_bonus += bonus

    # 5+ open positions
    if company.open_positions and company.open_positions >= 5:
        bonus = settings.scoring.open_positions_bonus
        bonuses["open_positions"] = bonus
        total_bonus += bonus

    return total_bonus, bonuses


def calculate_lead_score(company: Company, contact: Contact | None) -> dict[str, Any]:
    """
    Calculate complete lead score.

    Returns:
        Dict with total_score, icp_fit_score, intent_score, tier, and breakdown
    """
    # ICP fit scoring
    breakdown: dict[str, int] = {
        "industry": calculate_industry_score(company),
        "revenue": calculate_revenue_score(company),
        "tech_stack": calculate_tech_stack_score(company),
        "employee_count": calculate_employee_score(company),
        "geography": calculate_geography_score(company),
        "title": calculate_title_score(contact),
    }

    icp_fit_score = sum(breakdown.values())

    # Intent signals
    intent_score, intent_breakdown = calculate_intent_signals(company)
    breakdown.update(intent_breakdown)

    # Total score (capped at 100)
    total_score = min(icp_fit_score + intent_score, settings.scoring.max_score)

    # Determine tier
    if total_score >= settings.tiers.high_touch_min:
        tier = LeadTier.HIGH_TOUCH
    elif total_score >= settings.tiers.standard_min:
        tier = LeadTier.STANDARD
    else:
        tier = LeadTier.NURTURE

    return {
        "total_score": total_score,
        "icp_fit_score": icp_fit_score,
        "intent_score": intent_score,
        "tier": tier,
        "breakdown": breakdown,
    }


async def score_lead(
    session: AsyncSession,
    lead: Lead,
    company: Company | None = None,
    contact: Contact | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """
    Score a single lead.

    Args:
        session: Database session
        lead: Lead to score
        company: Company (optional, will load if needed)
        contact: Contact (optional, will load if needed)
        force: Re-score even if already scored

    Returns:
        Scoring result dict
    """
    # Check if already scored
    if lead.total_score is not None and not force:
        logger.debug(f"Lead {lead.id} already scored, skipping")
        return {
            "total_score": lead.total_score,
            "tier": lead.tier,
            "breakdown": lead.score_breakdown,
        }

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

    # Calculate score
    scoring_result = calculate_lead_score(company, contact)

    # Update lead
    lead.total_score = scoring_result["total_score"]
    lead.icp_fit_score = scoring_result["icp_fit_score"]
    lead.intent_score = scoring_result["intent_score"]
    lead.tier = scoring_result["tier"]
    lead.score_breakdown = scoring_result["breakdown"]
    lead.status = LeadStatus.SCORED

    logger.info(f"Scored lead {lead.id}: {scoring_result['total_score']} ({scoring_result['tier'].value})")

    return scoring_result


async def score_leads_batch(
    session: AsyncSession,
    leads: list[Lead] | None = None,
    status_filter: LeadStatus | None = LeadStatus.ENRICHED,
    limit: int = 500,
    force: bool = False,
) -> dict[str, Any]:
    """
    Score a batch of leads.

    Args:
        session: Database session
        leads: Specific leads to process (optional)
        status_filter: Only process leads with this status
        limit: Maximum leads to process
        force: Re-score even if already scored

    Returns:
        Dict with scoring statistics
    """
    # Get leads to process
    if leads is None:
        query = select(Lead)
        if status_filter:
            query = query.where(Lead.status == status_filter)
        if not force:
            query = query.where(Lead.total_score.is_(None))
        query = query.limit(limit)
        result = await session.execute(query)
        leads = list(result.scalars().all())

    if not leads:
        logger.info("No leads to score")
        return {"total": 0, "scored": 0, "by_tier": {}}

    logger.info(f"Scoring {len(leads)} leads")

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

    scored = 0
    by_tier: dict[str, int] = {
        "high_touch": 0,
        "standard": 0,
        "nurture": 0,
    }
    errors: list[str] = []
    score_distribution: dict[str, int] = {
        "90-100": 0,
        "80-89": 0,
        "70-79": 0,
        "60-69": 0,
        "50-59": 0,
        "40-49": 0,
        "30-39": 0,
        "0-29": 0,
    }

    for lead in leads:
        try:
            company = companies.get(lead.company_id)
            contact = contacts.get(lead.contact_id) if lead.contact_id else None

            result = await score_lead(session, lead, company, contact, force=force)

            if "error" not in result:
                scored += 1
                tier = result["tier"].value
                by_tier[tier] = by_tier.get(tier, 0) + 1

                # Track score distribution
                score = result["total_score"]
                if score >= 90:
                    score_distribution["90-100"] += 1
                elif score >= 80:
                    score_distribution["80-89"] += 1
                elif score >= 70:
                    score_distribution["70-79"] += 1
                elif score >= 60:
                    score_distribution["60-69"] += 1
                elif score >= 50:
                    score_distribution["50-59"] += 1
                elif score >= 40:
                    score_distribution["40-49"] += 1
                elif score >= 30:
                    score_distribution["30-39"] += 1
                else:
                    score_distribution["0-29"] += 1

        except Exception as e:
            logger.error(f"Error scoring lead {lead.id}: {e}")
            errors.append(f"Lead {lead.id}: {str(e)}")

    return {
        "total": len(leads),
        "scored": scored,
        "by_tier": by_tier,
        "score_distribution": score_distribution,
        "errors": errors[:20],
    }


async def get_scoring_stats(session: AsyncSession) -> dict[str, Any]:
    """Get overall scoring statistics."""
    from sqlalchemy import func

    # Count by tier
    tier_result = await session.execute(
        select(Lead.tier, func.count(Lead.id))
        .where(Lead.total_score.isnot(None))
        .group_by(Lead.tier)
    )
    by_tier = {row[0].value if row[0] else "unscored": row[1] for row in tier_result.fetchall()}

    # Average score
    avg_result = await session.execute(
        select(func.avg(Lead.total_score)).where(Lead.total_score.isnot(None))
    )
    avg_score = avg_result.scalar_one() or 0

    # Total scored
    total_result = await session.execute(
        select(func.count(Lead.id)).where(Lead.total_score.isnot(None))
    )
    total_scored = total_result.scalar_one()

    # Total leads
    all_leads_result = await session.execute(select(func.count(Lead.id)))
    total_leads = all_leads_result.scalar_one()

    return {
        "total_leads": total_leads,
        "total_scored": total_scored,
        "scoring_rate": (total_scored / total_leads * 100) if total_leads > 0 else 0,
        "average_score": round(avg_score, 1),
        "by_tier": by_tier,
    }
