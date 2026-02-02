"""Stage 4: Email verification using ZeroBounce."""

import logging
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.models import Contact, EmailStatus, Lead, LeadStatus
from core.schemas import EmailVerificationResult
from enrichment.cascade import cascade_manager
from utils.cost_tracker import BudgetExceeded, cost_tracker

logger = logging.getLogger(__name__)


def map_verification_status(status: str) -> EmailStatus:
    """Map verification result status to EmailStatus enum."""
    status_mapping = {
        "valid": EmailStatus.VALID,
        "invalid": EmailStatus.INVALID,
        "catch_all": EmailStatus.CATCH_ALL,
        "catch-all": EmailStatus.CATCH_ALL,
        "unknown": EmailStatus.UNKNOWN,
    }
    return status_mapping.get(status.lower(), EmailStatus.UNKNOWN)


async def verify_contact_email(
    session: AsyncSession,
    contact: Contact,
    force: bool = False,
) -> tuple[bool, EmailVerificationResult | None]:
    """
    Verify a contact's email address.

    Args:
        session: Database session
        contact: Contact with email to verify
        force: Re-verify even if already verified

    Returns:
        Tuple of (success, verification_result)
    """
    # Check if we have an email
    if not contact.email:
        logger.debug(f"Contact {contact.id} has no email, skipping")
        return False, None

    # Check if already verified
    if contact.email_status != EmailStatus.PENDING and not force:
        logger.debug(f"Email {contact.email} already verified, skipping")
        return True, None

    # Check budget
    if not await cost_tracker.check_budget(session):
        raise BudgetExceeded(
            "Monthly budget exceeded",
            spent=await cost_tracker.get_monthly_spend(session),
            budget=(await cost_tracker.get_budget_status(session))["monthly_budget"],
        )

    logger.info(f"Verifying email: {contact.email}")

    # Run verification
    result = await cascade_manager.verify_email(
        email=contact.email,
        session=session,
        contact_id=contact.id,
    )

    if not result:
        logger.warning(f"Verification failed for {contact.email}")
        return False, None

    # Update contact
    contact.email_status = map_verification_status(result.status)
    contact.email_verified_at = datetime.utcnow()

    logger.info(f"Email {contact.email} status: {result.status}")
    return True, result


async def verify_emails_batch(
    session: AsyncSession,
    limit: int = 100,
    force: bool = False,
    only_valid: bool = False,
) -> dict[str, Any]:
    """
    Verify a batch of emails.

    Args:
        session: Database session
        limit: Maximum emails to verify
        force: Re-verify even if already verified
        only_valid: Only return contacts with valid emails

    Returns:
        Dict with verification statistics
    """
    # Get contacts needing verification
    query = select(Contact).where(Contact.email.isnot(None))
    if not force:
        query = query.where(Contact.email_status == EmailStatus.PENDING)
    query = query.limit(limit)

    result = await session.execute(query)
    contacts = list(result.scalars().all())

    if not contacts:
        logger.info("No emails to verify")
        return {
            "total": 0,
            "verified": 0,
            "valid": 0,
            "invalid": 0,
            "catch_all": 0,
            "unknown": 0,
            "failed": 0,
        }

    logger.info(f"Verifying {len(contacts)} emails")

    stats = {
        "total": len(contacts),
        "verified": 0,
        "valid": 0,
        "invalid": 0,
        "catch_all": 0,
        "unknown": 0,
        "failed": 0,
    }
    errors: list[str] = []

    for contact in contacts:
        try:
            success, result = await verify_contact_email(session, contact, force=force)
            if success and result:
                stats["verified"] += 1
                status = result.status.lower()
                if status == "valid":
                    stats["valid"] += 1
                elif status == "invalid":
                    stats["invalid"] += 1
                elif status in ("catch_all", "catch-all"):
                    stats["catch_all"] += 1
                else:
                    stats["unknown"] += 1
            elif not success:
                stats["failed"] += 1
        except BudgetExceeded as e:
            logger.error(f"Budget exceeded: {e}")
            errors.append(f"Budget exceeded after {stats['verified']} verifications")
            break
        except Exception as e:
            logger.error(f"Error verifying {contact.email}: {e}")
            stats["failed"] += 1
            errors.append(f"{contact.email}: {str(e)}")

    stats["errors"] = errors[:20]
    return stats


async def verify_leads_emails(
    session: AsyncSession,
    leads: list[Lead] | None = None,
    status_filter: LeadStatus | None = LeadStatus.ENRICHING,
    limit: int = 100,
    force: bool = False,
) -> dict[str, Any]:
    """
    Verify emails for a set of leads.

    Args:
        session: Database session
        leads: Specific leads to process (optional)
        status_filter: Only process leads with this status
        limit: Maximum leads to process
        force: Re-verify even if already verified

    Returns:
        Dict with verification statistics
    """
    # Get leads to process
    if leads is None:
        query = select(Lead).where(Lead.contact_id.isnot(None))
        if status_filter:
            query = query.where(Lead.status == status_filter)
        query = query.limit(limit)
        result = await session.execute(query)
        leads = list(result.scalars().all())

    if not leads:
        logger.info("No leads to process")
        return {
            "total": 0,
            "verified": 0,
            "valid": 0,
            "invalid": 0,
            "catch_all": 0,
            "unknown": 0,
            "no_contact": 0,
            "no_email": 0,
            "failed": 0,
        }

    logger.info(f"Verifying emails for {len(leads)} leads")

    # Get unique contact IDs
    contact_ids = [lead.contact_id for lead in leads if lead.contact_id]

    # Load contacts
    result = await session.execute(
        select(Contact).where(Contact.id.in_(contact_ids))
    )
    contacts = {c.id: c for c in result.scalars().all()}

    stats = {
        "total": len(leads),
        "verified": 0,
        "valid": 0,
        "invalid": 0,
        "catch_all": 0,
        "unknown": 0,
        "no_contact": 0,
        "no_email": 0,
        "failed": 0,
    }
    errors: list[str] = []

    for lead in leads:
        if not lead.contact_id:
            stats["no_contact"] += 1
            continue

        contact = contacts.get(lead.contact_id)
        if not contact:
            stats["failed"] += 1
            continue

        if not contact.email:
            stats["no_email"] += 1
            continue

        try:
            success, result = await verify_contact_email(session, contact, force=force)
            if success and result:
                stats["verified"] += 1
                status = result.status.lower()
                if status == "valid":
                    stats["valid"] += 1
                elif status == "invalid":
                    stats["invalid"] += 1
                elif status in ("catch_all", "catch-all"):
                    stats["catch_all"] += 1
                else:
                    stats["unknown"] += 1
            elif not success:
                stats["failed"] += 1
        except BudgetExceeded as e:
            logger.error(f"Budget exceeded: {e}")
            errors.append(f"Budget exceeded after {stats['verified']} verifications")
            break
        except Exception as e:
            logger.error(f"Error verifying {contact.email}: {e}")
            stats["failed"] += 1
            errors.append(f"{contact.email}: {str(e)}")

    stats["errors"] = errors[:20]
    return stats


async def get_verification_stats(session: AsyncSession) -> dict[str, Any]:
    """Get overall email verification statistics."""
    from sqlalchemy import func

    # Count by status
    result = await session.execute(
        select(Contact.email_status, func.count(Contact.id))
        .where(Contact.email.isnot(None))
        .group_by(Contact.email_status)
    )

    stats_by_status = {row[0].value: row[1] for row in result.fetchall()}

    # Total with email
    total_result = await session.execute(
        select(func.count(Contact.id)).where(Contact.email.isnot(None))
    )
    total = total_result.scalar_one()

    return {
        "total_with_email": total,
        "pending": stats_by_status.get("pending", 0),
        "valid": stats_by_status.get("valid", 0),
        "invalid": stats_by_status.get("invalid", 0),
        "catch_all": stats_by_status.get("catch_all", 0),
        "unknown": stats_by_status.get("unknown", 0),
        "verified": total - stats_by_status.get("pending", 0),
        "verification_rate": (
            (total - stats_by_status.get("pending", 0)) / total * 100
            if total > 0
            else 0
        ),
        "valid_rate": (
            stats_by_status.get("valid", 0) / total * 100 if total > 0 else 0
        ),
    }
