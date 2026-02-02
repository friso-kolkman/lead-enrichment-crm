"""AI-powered content generation for research and messaging."""

import json
import logging
from typing import Any

from ai.client import ai_client
from ai.prompts import (
    COMPANY_RESEARCH_PROMPT,
    CONTACT_RESEARCH_PROMPT,
    EMAIL_GENERATION_SYSTEM_PROMPT,
    HIGH_TOUCH_EMAIL_PROMPT,
    ICEBREAKER_PROMPT,
    ICEBREAKER_SYSTEM_PROMPT,
    LINKEDIN_ANALYSIS_PROMPT,
    LINKEDIN_MESSAGE_PROMPT,
    NURTURE_EMAIL_PROMPT,
    RESEARCH_SYSTEM_PROMPT,
    STANDARD_EMAIL_PROMPT,
)
from core.models import Company, Contact, Lead, LeadTier

logger = logging.getLogger(__name__)


def _safe_json_parse(text: str) -> dict[str, Any]:
    """Safely parse JSON from AI response."""
    try:
        # Try direct parse first
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to extract JSON from markdown code block
        if "```json" in text:
            start = text.find("```json") + 7
            end = text.find("```", start)
            if end > start:
                return json.loads(text[start:end].strip())
        elif "```" in text:
            start = text.find("```") + 3
            end = text.find("```", start)
            if end > start:
                return json.loads(text[start:end].strip())
        raise


async def research_company(company: Company) -> dict[str, Any]:
    """
    Research a company and identify sales opportunities.

    Args:
        company: Company to research

    Returns:
        Dict with summary, KPIs, pain points, opportunities, talking points
    """
    # Format funding info
    funding_info = "None"
    if company.last_funding_amount:
        funding_info = f"${company.last_funding_amount:,.0f}"
        if company.last_funding_type:
            funding_info += f" ({company.last_funding_type})"
        if company.last_funding_date:
            funding_info += f" on {company.last_funding_date.strftime('%Y-%m-%d')}"

    # Format tech stack
    tech_stack_str = "Unknown"
    if company.tech_stack:
        if isinstance(company.tech_stack, dict):
            all_tech = []
            for category, techs in company.tech_stack.items():
                if isinstance(techs, list):
                    all_tech.extend(techs[:5])
            tech_stack_str = ", ".join(all_tech[:15])
        elif isinstance(company.tech_stack, list):
            tech_stack_str = ", ".join(company.tech_stack[:15])

    prompt = COMPANY_RESEARCH_PROMPT.format(
        company_name=company.name or company.domain,
        domain=company.domain,
        industry=company.industry or "Unknown",
        employee_count=company.employee_count or "Unknown",
        revenue=f"${company.revenue:,.0f}" if company.revenue else "Unknown",
        tech_stack=tech_stack_str,
        funding_info=funding_info,
        is_hiring="Yes" if company.is_hiring else "No",
        open_positions=company.open_positions or 0,
    )

    try:
        response = await ai_client.generate_structured(
            prompt=prompt,
            system_prompt=RESEARCH_SYSTEM_PROMPT,
        )
        return _safe_json_parse(response)
    except Exception as e:
        logger.error(f"Company research failed: {e}")
        return {
            "summary": f"{company.name or company.domain} is a {company.industry or 'company'} company.",
            "kpis": [],
            "pain_points": [],
            "opportunities": [],
            "talking_points": [],
        }


async def research_contact(
    contact: Contact,
    company: Company,
) -> dict[str, Any]:
    """
    Research a contact's role and priorities.

    Args:
        contact: Contact to research
        company: Their company

    Returns:
        Dict with role summary, responsibilities, KPIs, challenges, buying signals
    """
    prompt = CONTACT_RESEARCH_PROMPT.format(
        full_name=contact.full_name or f"{contact.first_name} {contact.last_name}",
        title=contact.title or "Unknown",
        department=contact.department or "Unknown",
        seniority=contact.seniority_level or "Unknown",
        company_name=company.name or company.domain,
        industry=company.industry or "Unknown",
    )

    try:
        response = await ai_client.generate_structured(
            prompt=prompt,
            system_prompt=RESEARCH_SYSTEM_PROMPT,
        )
        return _safe_json_parse(response)
    except Exception as e:
        logger.error(f"Contact research failed: {e}")
        return {
            "role_summary": f"{contact.title or 'Professional'} at {company.name or company.domain}",
            "likely_responsibilities": [],
            "kpis": [],
            "challenges": [],
            "buying_signals": [],
        }


async def analyze_linkedin_posts(
    contact: Contact,
    company: Company,
    posts: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Analyze LinkedIn posts to understand interests and priorities.

    Args:
        contact: Contact
        company: Their company
        posts: List of LinkedIn post dicts with 'text' and optionally 'date'

    Returns:
        Dict with themes, interests, tone, engagement style, personalization hooks
    """
    if not posts:
        return {
            "themes": [],
            "interests": [],
            "tone": "unknown",
            "engagement_style": "unknown",
            "personalization_hooks": [],
        }

    # Format posts for prompt
    posts_text = "\n\n".join(
        f"Post {i+1}: {p.get('text', '')[:500]}"
        for i, p in enumerate(posts[:5])
    )

    prompt = LINKEDIN_ANALYSIS_PROMPT.format(
        full_name=contact.full_name or f"{contact.first_name} {contact.last_name}",
        title=contact.title or "Unknown",
        company_name=company.name or company.domain,
        posts=posts_text,
    )

    try:
        response = await ai_client.generate_structured(
            prompt=prompt,
            system_prompt=RESEARCH_SYSTEM_PROMPT,
        )
        return _safe_json_parse(response)
    except Exception as e:
        logger.error(f"LinkedIn analysis failed: {e}")
        return {
            "themes": [],
            "interests": [],
            "tone": "unknown",
            "engagement_style": "unknown",
            "personalization_hooks": [],
        }


async def generate_icebreakers(
    lead: Lead,
    contact: Contact,
    company: Company,
    research_summary: str,
    trigger_events: list[dict[str, Any]] | None = None,
    linkedin_insights: dict[str, Any] | None = None,
) -> list[str]:
    """
    Generate personalized icebreakers.

    Args:
        lead: Lead
        contact: Contact
        company: Company
        research_summary: Summary from company/contact research
        trigger_events: Recent events (funding, hiring, etc.)
        linkedin_insights: Insights from LinkedIn analysis

    Returns:
        List of 3 icebreaker strings
    """
    # Format trigger events
    events_str = "None recent"
    if trigger_events:
        events_str = "; ".join(
            f"{e.get('type', 'Event')}: {e.get('description', '')[:100]}"
            for e in trigger_events[:3]
        )

    # Format LinkedIn insights
    linkedin_str = "No LinkedIn data"
    if linkedin_insights:
        hooks = linkedin_insights.get("personalization_hooks", [])
        if hooks:
            linkedin_str = "; ".join(hooks[:3])

    prompt = ICEBREAKER_PROMPT.format(
        full_name=contact.full_name or f"{contact.first_name} {contact.last_name}",
        title=contact.title or "Unknown",
        company_name=company.name or company.domain,
        industry=company.industry or "Unknown",
        research_summary=research_summary,
        trigger_events=events_str,
        linkedin_insights=linkedin_str,
    )

    try:
        response = await ai_client.generate_structured(
            prompt=prompt,
            system_prompt=ICEBREAKER_SYSTEM_PROMPT,
        )
        data = _safe_json_parse(response)
        return data.get("icebreakers", [])[:3]
    except Exception as e:
        logger.error(f"Icebreaker generation failed: {e}")
        return []


async def generate_email_variants(
    lead: Lead,
    contact: Contact,
    company: Company,
    research_data: dict[str, Any],
    icebreaker: str | None = None,
    value_prop: str = "We help companies like yours improve efficiency and drive growth.",
) -> dict[str, dict[str, str]]:
    """
    Generate email variants for different tiers.

    Args:
        lead: Lead
        contact: Contact
        company: Company
        research_data: Combined research data
        icebreaker: Optional icebreaker to use
        value_prop: Value proposition to include

    Returns:
        Dict mapping tier to email dict with 'subject' and 'body'
    """
    variants: dict[str, dict[str, str]] = {}

    # High-touch email
    try:
        high_touch_prompt = HIGH_TOUCH_EMAIL_PROMPT.format(
            full_name=contact.full_name or f"{contact.first_name} {contact.last_name}",
            title=contact.title or "Unknown",
            company_name=company.name or company.domain,
            research_summary=research_data.get("summary", ""),
            icebreaker=icebreaker or research_data.get("talking_points", [""])[0] if research_data.get("talking_points") else "",
            trigger_events="; ".join(str(e) for e in research_data.get("trigger_events", [])[:2]),
            pain_points="; ".join(research_data.get("pain_points", [])[:3]),
            value_prop=value_prop,
        )
        response = await ai_client.generate_structured(
            prompt=high_touch_prompt,
            system_prompt=EMAIL_GENERATION_SYSTEM_PROMPT,
        )
        variants["high_touch"] = _safe_json_parse(response)
    except Exception as e:
        logger.error(f"High-touch email generation failed: {e}")
        variants["high_touch"] = {"subject": "", "body": ""}

    # Standard email
    try:
        standard_prompt = STANDARD_EMAIL_PROMPT.format(
            full_name=contact.full_name or f"{contact.first_name} {contact.last_name}",
            title=contact.title or "Unknown",
            company_name=company.name or company.domain,
            industry=company.industry or "Unknown",
            role_summary=research_data.get("role_summary", ""),
            pain_points="; ".join(research_data.get("pain_points", [])[:3]),
            value_prop=value_prop,
        )
        response = await ai_client.generate_structured(
            prompt=standard_prompt,
            system_prompt=EMAIL_GENERATION_SYSTEM_PROMPT,
        )
        variants["standard"] = _safe_json_parse(response)
    except Exception as e:
        logger.error(f"Standard email generation failed: {e}")
        variants["standard"] = {"subject": "", "body": ""}

    # Nurture email
    try:
        nurture_prompt = NURTURE_EMAIL_PROMPT.format(
            full_name=contact.full_name or f"{contact.first_name} {contact.last_name}",
            title=contact.title or "Unknown",
            industry=company.industry or "Unknown",
            challenges="; ".join(research_data.get("challenges", research_data.get("pain_points", []))[:3]),
            content="a relevant industry report or guide",
        )
        response = await ai_client.generate_structured(
            prompt=nurture_prompt,
            system_prompt=EMAIL_GENERATION_SYSTEM_PROMPT,
        )
        variants["nurture"] = _safe_json_parse(response)
    except Exception as e:
        logger.error(f"Nurture email generation failed: {e}")
        variants["nurture"] = {"subject": "", "body": ""}

    return variants


async def generate_linkedin_message(
    contact: Contact,
    company: Company,
    research_summary: str,
    personalization_hook: str | None = None,
) -> str:
    """
    Generate a LinkedIn connection request message.

    Args:
        contact: Contact
        company: Company
        research_summary: Research summary
        personalization_hook: Specific thing to reference

    Returns:
        LinkedIn message string (under 300 chars)
    """
    prompt = LINKEDIN_MESSAGE_PROMPT.format(
        full_name=contact.full_name or f"{contact.first_name} {contact.last_name}",
        title=contact.title or "Unknown",
        company_name=company.name or company.domain,
        research_summary=research_summary[:300],
        personalization_hook=personalization_hook or "your work in the industry",
    )

    try:
        response = await ai_client.generate_structured(
            prompt=prompt,
            system_prompt=ICEBREAKER_SYSTEM_PROMPT,
        )
        data = _safe_json_parse(response)
        message = data.get("message", "")
        # Ensure under 300 chars
        return message[:295] + "..." if len(message) > 300 else message
    except Exception as e:
        logger.error(f"LinkedIn message generation failed: {e}")
        return ""


async def generate_all_content(
    lead: Lead,
    contact: Contact,
    company: Company,
    value_prop: str = "We help companies like yours improve efficiency and drive growth.",
) -> dict[str, Any]:
    """
    Generate all content for a lead (research, icebreakers, emails, LinkedIn).

    Args:
        lead: Lead
        contact: Contact
        company: Company
        value_prop: Value proposition

    Returns:
        Dict with all generated content
    """
    # Research company
    company_research = await research_company(company)

    # Research contact
    contact_research = await research_contact(contact, company)

    # Combine research
    combined_research = {
        "summary": company_research.get("summary", ""),
        "role_summary": contact_research.get("role_summary", ""),
        "kpis": list(set(company_research.get("kpis", []) + contact_research.get("kpis", []))),
        "pain_points": company_research.get("pain_points", []),
        "challenges": contact_research.get("challenges", []),
        "opportunities": company_research.get("opportunities", []),
        "talking_points": company_research.get("talking_points", []),
        "buying_signals": contact_research.get("buying_signals", []),
        "trigger_events": lead.trigger_events or [],
    }

    # Generate icebreakers
    icebreakers = await generate_icebreakers(
        lead=lead,
        contact=contact,
        company=company,
        research_summary=combined_research["summary"],
        trigger_events=lead.trigger_events,
    )

    # Generate email variants
    email_variants = await generate_email_variants(
        lead=lead,
        contact=contact,
        company=company,
        research_data=combined_research,
        icebreaker=icebreakers[0] if icebreakers else None,
        value_prop=value_prop,
    )

    # Generate LinkedIn message
    linkedin_message = await generate_linkedin_message(
        contact=contact,
        company=company,
        research_summary=combined_research["summary"],
    )

    return {
        "research_summary": f"{combined_research['summary']} {combined_research['role_summary']}".strip(),
        "kpis": combined_research["kpis"],
        "pain_points": combined_research["pain_points"],
        "trigger_events": combined_research["trigger_events"],
        "icebreakers": icebreakers,
        "email_variants": email_variants,
        "linkedin_message": linkedin_message,
    }
