"""Sequence processor — sends follow-up emails on schedule."""

import logging
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from config import settings
from core.models import (
    Company,
    Contact,
    EmailStatus,
    Lead,
    Sequence,
    SequenceEnrollment,
    SequenceStatus,
    SequenceStep,
)
from pipeline.stages.campaign import get_today_send_count, send_lead_email

logger = logging.getLogger(__name__)


async def process_pending_sequences(session: AsyncSession) -> dict[str, int]:
    """
    Main processor: find enrollments due for their next step and send emails.

    Returns dict with counts of sent, skipped, completed, errors.
    """
    # Check global daily limit first
    global_daily_limit = settings.resend.daily_limit
    sent_today = await get_today_send_count(session)
    if sent_today >= global_daily_limit:
        logger.info(f"Global daily limit reached ({sent_today}/{global_daily_limit}), skipping sequence processing")
        return {"sent": 0, "skipped": 0, "completed": 0, "errors": 0, "reason": "daily_limit"}

    remaining = global_daily_limit - sent_today

    # Query active enrollments that are due
    now = datetime.utcnow()
    result = await session.execute(
        select(SequenceEnrollment)
        .where(
            SequenceEnrollment.status == SequenceStatus.ACTIVE,
            SequenceEnrollment.next_send_at <= now,
        )
        .order_by(SequenceEnrollment.next_send_at)
        .limit(remaining)
    )
    enrollments = list(result.scalars().all())

    if not enrollments:
        return {"sent": 0, "skipped": 0, "completed": 0, "errors": 0}

    logger.info(f"Processing {len(enrollments)} pending sequence enrollments")

    sent = 0
    skipped = 0
    completed = 0
    errors = 0

    for enrollment in enrollments:
        # Load the sequence and its steps
        seq_result = await session.execute(
            select(Sequence)
            .options(selectinload(Sequence.steps))
            .where(Sequence.id == enrollment.sequence_id)
        )
        sequence = seq_result.scalar_one_or_none()

        if not sequence or not sequence.is_active or sequence.is_paused:
            skipped += 1
            continue

        # Find the next step to send (first step with step_number > current_step)
        # Steps are ordered by step_number via relationship, so this handles gaps
        step = next(
            (s for s in sequence.steps if s.step_number > enrollment.current_step),
            None,
        )

        if not step:
            # No more steps — mark completed
            enrollment.status = SequenceStatus.COMPLETED
            completed += 1
            continue

        # Load lead + contact
        lead_result = await session.execute(
            select(Lead).where(Lead.id == enrollment.lead_id)
        )
        lead = lead_result.scalar_one_or_none()
        if not lead or not lead.contact_id:
            enrollment.status = SequenceStatus.COMPLETED
            errors += 1
            continue

        contact_result = await session.execute(
            select(Contact).where(Contact.id == lead.contact_id)
        )
        contact = contact_result.scalar_one_or_none()
        if not contact or not contact.email:
            enrollment.status = SequenceStatus.COMPLETED
            errors += 1
            continue

        # Skip if contact bounced/unsubscribed since enrollment
        if contact.unsubscribed or contact.email_status == EmailStatus.INVALID:
            reason = "unsubscribed" if contact.unsubscribed else "bounced"
            enrollment.status = SequenceStatus(reason)
            skipped += 1
            continue

        # Load company for personalization
        company_result = await session.execute(
            select(Company).where(Company.id == lead.company_id)
        )
        company = company_result.scalar_one_or_none()

        # Send using existing send_lead_email with custom subject/body
        send_result = await send_lead_email(
            session=session,
            lead=lead,
            contact=contact,
            company=company,
            custom_subject=step.subject_template,
            custom_body=step.body_template,
        )

        if send_result.success:
            sent += 1
            enrollment.current_step = step.step_number
            enrollment.last_step_sent_at = datetime.utcnow()

            # Check if there's a following step
            following_step = next(
                (s for s in sequence.steps if s.step_number > step.step_number),
                None,
            )
            if following_step:
                enrollment.next_send_at = datetime.utcnow() + timedelta(days=following_step.delay_days)
            else:
                enrollment.status = SequenceStatus.COMPLETED
                enrollment.next_send_at = None
                completed += 1
        else:
            errors += 1
            # Push next_send_at forward to avoid infinite retry loop
            enrollment.next_send_at = datetime.utcnow() + timedelta(hours=1)
            logger.warning(f"Sequence send failed for enrollment {enrollment.id}: {send_result.error} — retrying in 1h")

    logger.info(f"Sequence processing done: sent={sent}, skipped={skipped}, completed={completed}, errors={errors}")
    return {"sent": sent, "skipped": skipped, "completed": completed, "errors": errors}


async def enroll_leads(
    session: AsyncSession,
    sequence_id: int,
    lead_ids: list[int],
) -> dict[str, Any]:
    """
    Enroll leads into a sequence.

    Sets next_send_at based on step 1's delay_days.
    Skips unsubscribed/bounced contacts and already-enrolled leads.
    """
    # Load sequence with steps
    seq_result = await session.execute(
        select(Sequence)
        .options(selectinload(Sequence.steps))
        .where(Sequence.id == sequence_id)
    )
    sequence = seq_result.scalar_one_or_none()
    if not sequence:
        return {"error": "Sequence not found"}

    if not sequence.steps:
        return {"error": "Sequence has no steps"}

    # Get first step for initial delay (steps are ordered by step_number)
    first_step = sequence.steps[0]

    # Check for ANY existing enrollments (unique constraint on lead_id + sequence_id)
    existing_result = await session.execute(
        select(SequenceEnrollment.lead_id).where(
            SequenceEnrollment.sequence_id == sequence_id,
            SequenceEnrollment.lead_id.in_(lead_ids),
        )
    )
    already_enrolled = set(existing_result.scalars().all())

    # Load leads with contacts to check eligibility
    leads_result = await session.execute(
        select(Lead)
        .join(Contact, Lead.contact_id == Contact.id)
        .where(
            Lead.id.in_(lead_ids),
            Contact.email.isnot(None),
            Contact.unsubscribed == False,
            Contact.email_status != EmailStatus.INVALID,
        )
    )
    eligible_leads = leads_result.scalars().all()

    enrolled = 0
    skipped = 0
    now = datetime.utcnow()

    for lead in eligible_leads:
        if lead.id in already_enrolled:
            skipped += 1
            continue

        enrollment = SequenceEnrollment(
            lead_id=lead.id,
            sequence_id=sequence_id,
            current_step=0,
            status=SequenceStatus.ACTIVE,
            enrolled_at=now,
            next_send_at=now + timedelta(days=first_step.delay_days),
        )
        session.add(enrollment)
        enrolled += 1

    await session.flush()
    logger.info(f"Enrolled {enrolled} leads in sequence '{sequence.name}' (skipped {skipped})")

    return {
        "sequence_name": sequence.name,
        "enrolled": enrolled,
        "skipped": skipped,
        "already_enrolled": len(already_enrolled),
    }


async def stop_enrollment(
    session: AsyncSession,
    lead_id: int,
    reason: str,
) -> int:
    """
    Stop all active enrollments for a lead.

    Called by webhook handler on bounce/reply/unsubscribe.
    Returns number of enrollments stopped.
    """
    valid_reasons = {
        "replied": SequenceStatus.REPLIED,
        "bounced": SequenceStatus.BOUNCED,
        "unsubscribed": SequenceStatus.UNSUBSCRIBED,
    }
    status = valid_reasons.get(reason, SequenceStatus.COMPLETED)

    result = await session.execute(
        select(SequenceEnrollment).where(
            SequenceEnrollment.lead_id == lead_id,
            SequenceEnrollment.status == SequenceStatus.ACTIVE,
        )
    )
    enrollments = result.scalars().all()

    count = 0
    for enrollment in enrollments:
        enrollment.status = status
        enrollment.next_send_at = None
        count += 1

    if count:
        logger.info(f"Stopped {count} enrollments for lead {lead_id} (reason: {reason})")

    return count


async def get_sequence_stats(
    session: AsyncSession,
    sequence_id: int,
) -> dict[str, Any]:
    """Get stats for a sequence."""
    seq_result = await session.execute(
        select(Sequence).where(Sequence.id == sequence_id)
    )
    sequence = seq_result.scalar_one_or_none()
    if not sequence:
        return {"error": "Sequence not found"}

    # Count enrollments by status
    status_result = await session.execute(
        select(
            SequenceEnrollment.status,
            func.count(SequenceEnrollment.id),
        )
        .where(SequenceEnrollment.sequence_id == sequence_id)
        .group_by(SequenceEnrollment.status)
    )
    status_counts = dict(status_result.all())

    total = sum(status_counts.values())
    active = status_counts.get(SequenceStatus.ACTIVE, 0)
    completed_count = status_counts.get(SequenceStatus.COMPLETED, 0)
    replied = status_counts.get(SequenceStatus.REPLIED, 0)
    bounced = status_counts.get(SequenceStatus.BOUNCED, 0)
    unsubscribed = status_counts.get(SequenceStatus.UNSUBSCRIBED, 0)

    # Count steps
    steps_result = await session.execute(
        select(func.count(SequenceStep.id)).where(
            SequenceStep.sequence_id == sequence_id
        )
    )
    step_count = steps_result.scalar_one()

    return {
        "sequence_id": sequence.id,
        "name": sequence.name,
        "is_active": sequence.is_active,
        "is_paused": sequence.is_paused,
        "total_steps": step_count,
        "total_enrolled": total,
        "active": active,
        "completed": completed_count,
        "replied": replied,
        "bounced": bounced,
        "unsubscribed": unsubscribed,
        "reply_rate": (replied / total * 100) if total > 0 else 0,
    }
