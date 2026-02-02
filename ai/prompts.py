"""Prompt templates for AI research and messaging generation."""

# Research prompts

RESEARCH_SYSTEM_PROMPT = """You are an expert B2B sales researcher. Your task is to analyze company and contact information to identify:
1. Key business priorities and challenges
2. Likely KPIs they care about
3. Recent trigger events (funding, hiring, product launches, etc.)
4. Personalization angles for outreach

Always respond in valid JSON format as specified."""

COMPANY_RESEARCH_PROMPT = """Research this company and identify sales opportunities:

Company: {company_name}
Domain: {domain}
Industry: {industry}
Employee Count: {employee_count}
Revenue: {revenue}
Tech Stack: {tech_stack}
Recent Funding: {funding_info}
Is Hiring: {is_hiring}
Open Positions: {open_positions}

Based on this information, provide a JSON response with:
{{
    "summary": "2-3 sentence summary of the company and their likely priorities",
    "kpis": ["list", "of", "likely", "KPIs", "they", "track"],
    "pain_points": ["potential", "pain", "points"],
    "opportunities": ["sales", "opportunities"],
    "talking_points": ["relevant", "talking", "points"]
}}"""

CONTACT_RESEARCH_PROMPT = """Research this contact's role and priorities:

Name: {full_name}
Title: {title}
Department: {department}
Seniority: {seniority}
Company: {company_name}
Industry: {industry}

Based on their role, provide a JSON response with:
{{
    "role_summary": "1-2 sentences about what this person likely does day-to-day",
    "likely_responsibilities": ["list", "of", "responsibilities"],
    "kpis": ["KPIs", "they", "likely", "own"],
    "challenges": ["challenges", "they", "face"],
    "buying_signals": ["what", "would", "make", "them", "buy"]
}}"""

LINKEDIN_ANALYSIS_PROMPT = """Analyze these LinkedIn posts to understand the person's interests and priorities:

Name: {full_name}
Title: {title}
Company: {company_name}

Recent LinkedIn Posts:
{posts}

Provide a JSON response with:
{{
    "themes": ["recurring", "themes", "in", "posts"],
    "interests": ["professional", "interests"],
    "tone": "formal/casual/technical/inspirational",
    "engagement_style": "how they engage (shares, original content, comments)",
    "personalization_hooks": ["specific", "things", "to", "reference"]
}}"""

NEWS_RESEARCH_PROMPT = """Find recent news and trigger events for this company:

Company: {company_name}
Domain: {domain}
Industry: {industry}

Search for:
1. Recent funding announcements
2. Product launches or updates
3. Leadership changes
4. Partnerships or acquisitions
5. Awards or recognition
6. Hiring announcements

Return the most relevant recent events."""

# Messaging prompts

ICEBREAKER_SYSTEM_PROMPT = """You are an expert at writing personalized B2B sales icebreakers. Your icebreakers should:
1. Be specific and reference real information about the person/company
2. Show you've done your research
3. Be concise (1-2 sentences max)
4. Create curiosity without being pushy
5. Feel natural, not templated

Never use generic phrases like "I noticed" or "I came across your profile"."""

ICEBREAKER_PROMPT = """Generate 3 personalized icebreakers for this prospect:

Contact: {full_name}
Title: {title}
Company: {company_name}
Industry: {industry}

Research Summary: {research_summary}
Trigger Events: {trigger_events}
LinkedIn Insights: {linkedin_insights}

Provide a JSON response with:
{{
    "icebreakers": [
        "First icebreaker - reference specific company news or achievement",
        "Second icebreaker - reference their role/responsibilities",
        "Third icebreaker - reference industry trend or challenge"
    ]
}}"""

EMAIL_GENERATION_SYSTEM_PROMPT = """You are an expert B2B email copywriter. Your emails should:
1. Be personalized and specific
2. Lead with value, not features
3. Be concise (under 150 words for body)
4. Have a clear, low-friction CTA
5. Sound human, not robotic

Match the tone to the tier:
- high_touch: Very personalized, reference specific details
- standard: Personalized to industry/role, but more templated
- nurture: Educational, value-first, softer CTA"""

HIGH_TOUCH_EMAIL_PROMPT = """Write a highly personalized cold email for this high-priority prospect:

Contact: {full_name}
Title: {title}
Company: {company_name}

Research Summary: {research_summary}
Icebreaker: {icebreaker}
Trigger Events: {trigger_events}
Pain Points: {pain_points}

Our Value Prop: {value_prop}

Provide a JSON response with:
{{
    "subject": "Personalized subject line",
    "body": "Email body with icebreaker, value prop, and soft CTA",
    "ps": "Optional PS line for extra personalization"
}}"""

STANDARD_EMAIL_PROMPT = """Write a personalized cold email for this prospect:

Contact: {full_name}
Title: {title}
Company: {company_name}
Industry: {industry}

Role Summary: {role_summary}
Pain Points: {pain_points}

Our Value Prop: {value_prop}

Provide a JSON response with:
{{
    "subject": "Subject line relevant to their role",
    "body": "Email body with industry-relevant hook, value prop, and CTA"
}}"""

NURTURE_EMAIL_PROMPT = """Write an educational, value-first email for this prospect:

Contact: {full_name}
Title: {title}
Industry: {industry}

Common Challenges: {challenges}

Content/Resource to Share: {content}

Provide a JSON response with:
{{
    "subject": "Value-focused subject line",
    "body": "Email leading with insight/value, positioning resource, soft CTA"
}}"""

LINKEDIN_MESSAGE_PROMPT = """Write a LinkedIn connection request message:

Contact: {full_name}
Title: {title}
Company: {company_name}

Research Summary: {research_summary}
Personalization Hook: {personalization_hook}

Requirements:
- Must be under 300 characters
- No pitch, just genuine connection
- Reference something specific about them

Provide a JSON response with:
{{
    "message": "LinkedIn connection request message"
}}"""

FOLLOW_UP_EMAIL_PROMPT = """Write a follow-up email (email #{sequence_number}):

Previous Email Subject: {previous_subject}
Days Since Last Email: {days_since}

Contact: {full_name}
Title: {title}
Company: {company_name}

New Angle/Value: {new_angle}

Provide a JSON response with:
{{
    "subject": "Re: {previous_subject} or new angle",
    "body": "Follow-up email with new value or angle"
}}"""
