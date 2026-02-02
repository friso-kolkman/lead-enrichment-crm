"""AI module for research and content generation."""

from ai.client import AIClient, ai_client
from ai.generator import (
    generate_all_content,
    generate_email_variants,
    generate_icebreakers,
    generate_linkedin_message,
    research_company,
    research_contact,
    analyze_linkedin_posts,
)

__all__ = [
    # Client
    "AIClient",
    "ai_client",
    # Generator functions
    "generate_all_content",
    "generate_email_variants",
    "generate_icebreakers",
    "generate_linkedin_message",
    "research_company",
    "research_contact",
    "analyze_linkedin_posts",
]
