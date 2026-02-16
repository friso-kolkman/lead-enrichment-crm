"""Stage 9: Campaign launch via Resend email."""

import logging
from datetime import date, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from core.models import Campaign, Company, Contact, Lead, LeadStatus, LeadTier
from core.schemas import EmailSendRequest, EmailSendResult
from integrations.resend import ResendClient, resend_client

logger = logging.getLogger(__name__)


async def get_today_send_count(session: AsyncSession) -> int:
    """Get total emails sent today across all campaigns."""
    today_start = datetime.combine(date.today(), datetime.min.time())
    result = await session.execute(
        select(func.count(Lead.id)).where(
            Lead.last_email_sent >= today_start,
        )
    )
    return result.scalar_one()


async def send_lead_email(
    session: AsyncSession,
    lead: Lead,
    contact: Contact | None = None,
    company: Company | None = None,
    variant: str = "standard",
    custom_subject: str | None = None,
    custom_body: str | None = None,
    dry_run: bool = False,
) -> EmailSendResult:
    """
    Send an email to a lead.

    Args:
        session: Database session
        lead: Lead to email
        contact: Contact (optional, will load if needed)
        company: Company (optional, will load if needed)
        variant: Email variant to use (high_touch, standard, nurture)
        custom_subject: Override subject
        custom_body: Override body
        dry_run: If True, don't actually send

    Returns:
        EmailSendResult
    """
    # Load contact if needed
    if not contact and lead.contact_id:
        result = await session.execute(
            select(Contact).where(Contact.id == lead.contact_id)
        )
        contact = result.scalar_one_or_none()

    if not contact or not contact.email:
        return EmailSendResult(
            success=False,
            error="No contact email found",
        )

    # Skip unsubscribed contacts
    if contact.unsubscribed:
        return EmailSendResult(
            success=False,
            error="Contact has unsubscribed",
        )

    # Load company if needed
    if not company:
        result = await session.execute(
            select(Company).where(Company.id == lead.company_id)
        )
        company = result.scalar_one_or_none()

    # Get email content
    if custom_subject and custom_body:
        subject = custom_subject
        body = custom_body
    elif lead.email_variants:
        email_content = lead.email_variants.get(variant, {})
        subject = email_content.get("subject", "")
        body = email_content.get("body", "")
    else:
        return EmailSendResult(
            success=False,
            error="No email content available",
        )

    if not subject or not body:
        return EmailSendResult(
            success=False,
            error=f"Missing subject or body for variant '{variant}'",
        )

    # Personalize
    first_name = contact.first_name or "there"
    company_name = company.name if company else "your company"

    subject = subject.replace("{first_name}", first_name)
    subject = subject.replace("{company_name}", company_name)
    body = body.replace("{first_name}", first_name)
    body = body.replace("{company_name}", company_name)

    logger.info(f"Sending email to {contact.email} (lead {lead.id})")

    if dry_run:
        return EmailSendResult(
            success=True,
            message_id=f"dry_run_{lead.id}",
        )

    try:
        # Build HTML body with unsubscribe footer
        html_body = body.replace("\n", "<br>")
        unsub_url = settings.resend.unsubscribe_url
        company_name = settings.resend.company_name
        company_addr = settings.resend.company_address
        if unsub_url:
            unsub_link = f"{unsub_url}?email={contact.email}"
            footer = (
                '<br><br><div style="color:#999;font-size:11px;border-top:1px solid #eee;padding-top:8px;margin-top:16px">'
                f'{company_name}<br>{company_addr}<br>'
                f'<a href="{unsub_link}" style="color:#999">Unsubscribe</a>'
                '</div>'
            )
            html_body += footer

        # Build headers — List-Unsubscribe improves deliverability
        email_headers = {}
        if unsub_url:
            email_headers["List-Unsubscribe"] = f"<{unsub_link}>"
            email_headers["List-Unsubscribe-Post"] = "List-Unsubscribe=One-Click"

        reply_to = settings.resend.reply_to_email or None
        result = await resend_client.send_email(
            to_email=contact.email,
            subject=subject,
            html_body=html_body,
            reply_to=reply_to,
            headers=email_headers if email_headers else None,
        )

        if result.get("id"):
            # Update lead
            lead.last_email_sent = datetime.utcnow()
            lead.emails_sent = (lead.emails_sent or 0) + 1
            if lead.status == LeadStatus.SYNCED:
                lead.status = LeadStatus.CONTACTED
            if not lead.contacted_at:
                lead.contacted_at = datetime.utcnow()

            return EmailSendResult(
                success=True,
                message_id=result["id"],
            )
        else:
            return EmailSendResult(
                success=False,
                error=result.get("error", "Unknown error"),
            )

    except Exception as e:
        logger.error(f"Error sending email to {contact.email}: {e}")
        return EmailSendResult(
            success=False,
            error=str(e),
        )


async def launch_campaign(
    session: AsyncSession,
    campaign_id: int | None = None,
    campaign: Campaign | None = None,
    limit: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Launch a campaign - send emails to matching leads.

    Args:
        session: Database session
        campaign_id: Campaign ID (optional if campaign provided)
        campaign: Campaign object (optional)
        limit: Override daily limit
        dry_run: If True, don't actually send

    Returns:
        Dict with campaign results
    """
    # Load campaign if needed
    if not campaign and campaign_id:
        result = await session.execute(
            select(Campaign).where(Campaign.id == campaign_id)
        )
        campaign = result.scalar_one_or_none()

    if not campaign:
        return {"error": "Campaign not found"}

    if not campaign.is_active:
        return {"error": "Campaign is not active"}

    if campaign.is_paused:
        return {"error": "Campaign is paused"}

    # Enforce global daily limit (Resend free tier = 100/day)
    global_daily_limit = settings.resend.daily_limit
    sent_today = await get_today_send_count(session)
    if sent_today >= global_daily_limit:
        return {"error": f"Global daily limit reached ({sent_today}/{global_daily_limit})"}

    # Build lead query — exclude already contacted, unsubscribed, and bounced
    from core.models import EmailStatus

    query = (
        select(Lead)
        .join(Contact, Lead.contact_id == Contact.id)
        .where(
            Lead.contact_id.isnot(None),
            Lead.email_variants.isnot(None),
            Lead.status.in_([LeadStatus.READY, LeadStatus.SYNCED]),
            Lead.emails_sent == 0,
            Contact.unsubscribed == False,
            Contact.email_status != EmailStatus.INVALID,
        )
    )

    # Apply campaign filters
    if campaign.target_tier:
        query = query.where(Lead.tier == campaign.target_tier)
    if campaign.min_score:
        query = query.where(Lead.total_score >= campaign.min_score)
    if campaign.max_score:
        query = query.where(Lead.total_score <= campaign.max_score)

    # Apply limit — respect both campaign and global daily cap
    send_limit = limit or campaign.daily_limit
    remaining_global = global_daily_limit - sent_today
    send_limit = min(send_limit, remaining_global)
    query = query.limit(send_limit)

    result = await session.execute(query)
    leads = list(result.scalars().all())

    if not leads:
        return {"total": 0, "sent": 0, "failed": 0, "message": "No matching leads"}

    logger.info(f"Launching campaign '{campaign.name}' to {len(leads)} leads")

    # Load contacts
    contact_ids = [lead.contact_id for lead in leads if lead.contact_id]
    result = await session.execute(
        select(Contact).where(Contact.id.in_(contact_ids))
    )
    contacts = {c.id: c for c in result.scalars().all()}

    # Load companies
    company_ids = list({lead.company_id for lead in leads})
    result = await session.execute(
        select(Company).where(Company.id.in_(company_ids))
    )
    companies = {c.id: c for c in result.scalars().all()}

    # Determine variant based on tier
    tier_variant_map = {
        LeadTier.HIGH_TOUCH: "high_touch",
        LeadTier.STANDARD: "standard",
        LeadTier.NURTURE: "nurture",
    }

    sent = 0
    failed = 0
    errors: list[str] = []

    for lead in leads:
        contact = contacts.get(lead.contact_id) if lead.contact_id else None
        company = companies.get(lead.company_id)

        # Choose variant based on tier
        variant = tier_variant_map.get(lead.tier, "standard")

        send_result = await send_lead_email(
            session=session,
            lead=lead,
            contact=contact,
            company=company,
            variant=variant,
            dry_run=dry_run,
        )

        if send_result.success:
            sent += 1
            lead.campaign_id = campaign.id
        else:
            failed += 1
            if send_result.error:
                errors.append(f"Lead {lead.id}: {send_result.error}")

    # Update campaign stats
    if not dry_run:
        campaign.total_sent = (campaign.total_sent or 0) + sent

    return {
        "campaign_name": campaign.name,
        "total": len(leads),
        "sent": sent,
        "failed": failed,
        "dry_run": dry_run,
        "errors": errors[:20],
    }


async def create_campaign(
    session: AsyncSession,
    name: str,
    target_tier: LeadTier | None = None,
    min_score: int | None = None,
    max_score: int | None = None,
    daily_limit: int = 50,
    email_subject_template: str | None = None,
    email_body_template: str | None = None,
    description: str | None = None,
) -> Campaign:
    """
    Create a new campaign.

    Args:
        session: Database session
        name: Campaign name
        target_tier: Target lead tier
        min_score: Minimum lead score
        max_score: Maximum lead score
        daily_limit: Daily send limit
        email_subject_template: Email subject template
        email_body_template: Email body template
        description: Campaign description

    Returns:
        Created Campaign object
    """
    campaign = Campaign(
        name=name,
        description=description,
        target_tier=target_tier,
        min_score=min_score,
        max_score=max_score,
        daily_limit=daily_limit,
        email_subject_template=email_subject_template,
        email_body_template=email_body_template,
        is_active=False,
        is_paused=False,
    )

    session.add(campaign)
    await session.flush()

    logger.info(f"Created campaign '{name}' (ID: {campaign.id})")
    return campaign


async def activate_campaign(
    session: AsyncSession,
    campaign_id: int,
) -> Campaign | None:
    """Activate a campaign."""
    result = await session.execute(
        select(Campaign).where(Campaign.id == campaign_id)
    )
    campaign = result.scalar_one_or_none()

    if campaign:
        campaign.is_active = True
        campaign.is_paused = False
        logger.info(f"Activated campaign '{campaign.name}'")

    return campaign


async def pause_campaign(
    session: AsyncSession,
    campaign_id: int,
) -> Campaign | None:
    """Pause a campaign."""
    result = await session.execute(
        select(Campaign).where(Campaign.id == campaign_id)
    )
    campaign = result.scalar_one_or_none()

    if campaign:
        campaign.is_paused = True
        logger.info(f"Paused campaign '{campaign.name}'")

    return campaign


async def get_campaign_stats(
    session: AsyncSession,
    campaign_id: int | None = None,
) -> dict[str, Any]:
    """Get campaign statistics."""
    if campaign_id:
        # Stats for specific campaign
        result = await session.execute(
            select(Campaign).where(Campaign.id == campaign_id)
        )
        campaign = result.scalar_one_or_none()

        if not campaign:
            return {"error": "Campaign not found"}

        # Get lead stats for this campaign
        leads_result = await session.execute(
            select(
                func.count(Lead.id),
                func.sum(Lead.opens),
                func.sum(Lead.clicks),
                func.sum(Lead.replies),
            ).where(Lead.campaign_id == campaign_id)
        )
        row = leads_result.fetchone()

        return {
            "campaign_id": campaign.id,
            "name": campaign.name,
            "is_active": campaign.is_active,
            "is_paused": campaign.is_paused,
            "total_sent": campaign.total_sent,
            "total_opens": row[1] or 0 if row else 0,
            "total_clicks": row[2] or 0 if row else 0,
            "total_replies": row[3] or 0 if row else 0,
            "open_rate": (
                (row[1] or 0) / campaign.total_sent * 100
                if campaign.total_sent > 0
                else 0
            ) if row else 0,
            "click_rate": (
                (row[2] or 0) / campaign.total_sent * 100
                if campaign.total_sent > 0
                else 0
            ) if row else 0,
            "reply_rate": (
                (row[3] or 0) / campaign.total_sent * 100
                if campaign.total_sent > 0
                else 0
            ) if row else 0,
        }

    # Overall campaign stats
    result = await session.execute(
        select(
            func.count(Campaign.id),
            func.sum(Campaign.total_sent),
        )
    )
    row = result.fetchone()

    active_result = await session.execute(
        select(func.count(Campaign.id)).where(Campaign.is_active == True)
    )
    active_count = active_result.scalar_one()

    return {
        "total_campaigns": row[0] if row else 0,
        "active_campaigns": active_count,
        "total_emails_sent": row[1] or 0 if row else 0,
    }
