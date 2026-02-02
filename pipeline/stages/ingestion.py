"""Stage 1: CSV/JSON import for lead ingestion."""

import csv
import json
import logging
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.models import Company, Contact, Lead, LeadStatus
from core.schemas import LeadImportResult

logger = logging.getLogger(__name__)


def extract_domain(url_or_email: str | None) -> str | None:
    """Extract domain from URL or email."""
    if not url_or_email:
        return None

    url_or_email = url_or_email.strip().lower()

    # If it's an email, extract domain
    if "@" in url_or_email:
        return url_or_email.split("@")[1]

    # If it's a URL, parse it
    if url_or_email.startswith(("http://", "https://")):
        parsed = urlparse(url_or_email)
        domain = parsed.netloc
    else:
        # Assume it's already a domain
        domain = url_or_email

    # Remove www. prefix
    if domain.startswith("www."):
        domain = domain[4:]

    return domain


async def import_csv(
    session: AsyncSession,
    file_path: str,
    source: str = "csv_import",
    skip_duplicates: bool = True,
    column_mapping: dict[str, str] | None = None,
) -> LeadImportResult:
    """
    Import leads from CSV file.

    Expected columns (or mapped equivalents):
    - domain OR website OR company_website OR email (for domain extraction)
    - company_name OR company OR name
    - first_name
    - last_name
    - email
    - title OR job_title
    - linkedin_url OR linkedin

    Args:
        session: Database session
        file_path: Path to CSV file
        source: Source identifier
        skip_duplicates: Skip existing domains if True
        column_mapping: Optional mapping of CSV columns to standard names

    Returns:
        LeadImportResult with import statistics
    """
    path = Path(file_path)
    if not path.exists():
        return LeadImportResult(
            total_rows=0,
            imported=0,
            skipped=0,
            errors=[f"File not found: {file_path}"],
        )

    # Read CSV
    try:
        df = pd.read_csv(path)
    except Exception as e:
        return LeadImportResult(
            total_rows=0,
            imported=0,
            skipped=0,
            errors=[f"Failed to read CSV: {e}"],
        )

    # Apply column mapping
    if column_mapping:
        df = df.rename(columns=column_mapping)

    # Normalize column names (lowercase, replace spaces with underscores)
    df.columns = [c.lower().replace(" ", "_").replace("-", "_") for c in df.columns]

    return await _process_dataframe(
        session=session,
        df=df,
        source=source,
        source_file=file_path,
        skip_duplicates=skip_duplicates,
    )


async def import_json(
    session: AsyncSession,
    file_path: str,
    source: str = "json_import",
    skip_duplicates: bool = True,
    records_path: str | None = None,
) -> LeadImportResult:
    """
    Import leads from JSON file.

    Args:
        session: Database session
        file_path: Path to JSON file
        source: Source identifier
        skip_duplicates: Skip existing domains if True
        records_path: JSON path to records array (e.g., "data.leads")

    Returns:
        LeadImportResult with import statistics
    """
    path = Path(file_path)
    if not path.exists():
        return LeadImportResult(
            total_rows=0,
            imported=0,
            skipped=0,
            errors=[f"File not found: {file_path}"],
        )

    try:
        with open(path) as f:
            data = json.load(f)
    except Exception as e:
        return LeadImportResult(
            total_rows=0,
            imported=0,
            skipped=0,
            errors=[f"Failed to read JSON: {e}"],
        )

    # Navigate to records path if specified
    if records_path:
        for key in records_path.split("."):
            if isinstance(data, dict):
                data = data.get(key, [])
            else:
                break

    # Convert to DataFrame
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        return LeadImportResult(
            total_rows=0,
            imported=0,
            skipped=0,
            errors=["JSON data must be an array of records"],
        )

    # Normalize column names
    df.columns = [c.lower().replace(" ", "_").replace("-", "_") for c in df.columns]

    return await _process_dataframe(
        session=session,
        df=df,
        source=source,
        source_file=file_path,
        skip_duplicates=skip_duplicates,
    )


async def _process_dataframe(
    session: AsyncSession,
    df: pd.DataFrame,
    source: str,
    source_file: str,
    skip_duplicates: bool = True,
) -> LeadImportResult:
    """Process a DataFrame and import leads."""
    total_rows = len(df)
    imported = 0
    skipped = 0
    errors: list[str] = []

    # Get existing domains for deduplication
    existing_domains: set[str] = set()
    if skip_duplicates:
        result = await session.execute(select(Company.domain))
        existing_domains = {row[0] for row in result.fetchall()}

    for idx, row in df.iterrows():
        try:
            # Extract domain
            domain = _extract_domain_from_row(row)
            if not domain:
                errors.append(f"Row {idx}: Could not extract domain")
                skipped += 1
                continue

            # Skip duplicates
            if skip_duplicates and domain in existing_domains:
                logger.debug(f"Skipping duplicate domain: {domain}")
                skipped += 1
                continue

            # Create or get company
            company = await _get_or_create_company(session, domain, row)
            existing_domains.add(domain)

            # Create contact if we have contact info
            contact = await _create_contact_if_possible(session, company.id, row)

            # Create lead
            lead = Lead(
                company_id=company.id,
                contact_id=contact.id if contact else None,
                status=LeadStatus.NEW,
                source=source,
                source_file=source_file,
            )
            session.add(lead)
            imported += 1

        except Exception as e:
            errors.append(f"Row {idx}: {str(e)}")
            skipped += 1
            continue

    await session.flush()

    logger.info(
        f"Import complete: {imported} imported, {skipped} skipped, {len(errors)} errors"
    )

    return LeadImportResult(
        total_rows=total_rows,
        imported=imported,
        skipped=skipped,
        errors=errors[:100],  # Limit error messages
    )


def _extract_domain_from_row(row: pd.Series) -> str | None:
    """Extract domain from a row using various possible columns."""
    # Check columns in priority order
    domain_columns = [
        "domain",
        "company_domain",
        "website",
        "company_website",
        "url",
        "company_url",
    ]

    for col in domain_columns:
        if col in row and pd.notna(row[col]):
            domain = extract_domain(str(row[col]))
            if domain:
                return domain

    # Try extracting from email
    email_columns = ["email", "work_email", "contact_email"]
    for col in email_columns:
        if col in row and pd.notna(row[col]):
            domain = extract_domain(str(row[col]))
            if domain:
                return domain

    return None


async def _get_or_create_company(
    session: AsyncSession,
    domain: str,
    row: pd.Series,
) -> Company:
    """Get existing company or create new one."""
    # Check if company exists
    result = await session.execute(select(Company).where(Company.domain == domain))
    company = result.scalar_one_or_none()

    if company:
        return company

    # Get company name from row
    name = None
    name_columns = ["company_name", "company", "name", "organization"]
    for col in name_columns:
        if col in row and pd.notna(row[col]):
            name = str(row[col]).strip()
            break

    # Create new company
    company = Company(
        domain=domain,
        name=name,
    )

    # Add any other available company data from row
    field_mappings = {
        "industry": ["industry", "company_industry"],
        "employee_count": ["employees", "employee_count", "company_size"],
        "hq_city": ["city", "hq_city", "location_city"],
        "hq_state": ["state", "hq_state", "location_state"],
        "hq_country": ["country", "hq_country", "location_country"],
        "linkedin_url": ["company_linkedin", "company_linkedin_url"],
    }

    for field, columns in field_mappings.items():
        for col in columns:
            if col in row and pd.notna(row[col]):
                value = row[col]
                if field == "employee_count":
                    try:
                        value = int(value)
                    except (ValueError, TypeError):
                        continue
                setattr(company, field, value)
                break

    session.add(company)
    await session.flush()

    return company


async def _create_contact_if_possible(
    session: AsyncSession,
    company_id: int,
    row: pd.Series,
) -> Contact | None:
    """Create a contact if we have sufficient contact data."""
    # Check for email first - required for contact
    email = None
    email_columns = ["email", "work_email", "contact_email"]
    for col in email_columns:
        if col in row and pd.notna(row[col]):
            email = str(row[col]).strip().lower()
            break

    # If no email, check for LinkedIn URL
    linkedin_url = None
    linkedin_columns = ["linkedin", "linkedin_url", "contact_linkedin"]
    for col in linkedin_columns:
        if col in row and pd.notna(row[col]):
            linkedin_url = str(row[col]).strip()
            break

    # Need at least email or LinkedIn to create contact
    if not email and not linkedin_url:
        return None

    # Check for existing contact by email
    if email:
        result = await session.execute(select(Contact).where(Contact.email == email))
        existing = result.scalar_one_or_none()
        if existing:
            return existing

    # Get name
    first_name = None
    last_name = None
    full_name = None

    name_mappings = {
        "first_name": ["first_name", "firstname", "given_name"],
        "last_name": ["last_name", "lastname", "surname", "family_name"],
        "full_name": ["full_name", "name", "contact_name"],
    }

    for field, columns in name_mappings.items():
        for col in columns:
            if col in row and pd.notna(row[col]):
                value = str(row[col]).strip()
                if field == "first_name":
                    first_name = value
                elif field == "last_name":
                    last_name = value
                elif field == "full_name":
                    full_name = value
                break

    # Parse full name if needed
    if full_name and not (first_name and last_name):
        parts = full_name.split(maxsplit=1)
        if len(parts) >= 1:
            first_name = parts[0]
        if len(parts) >= 2:
            last_name = parts[1]

    # Get title
    title = None
    title_columns = ["title", "job_title", "position", "role"]
    for col in title_columns:
        if col in row and pd.notna(row[col]):
            title = str(row[col]).strip()
            break

    # Create contact
    contact = Contact(
        company_id=company_id,
        first_name=first_name,
        last_name=last_name,
        full_name=full_name or (f"{first_name or ''} {last_name or ''}".strip() or None),
        email=email,
        title=title,
        linkedin_url=linkedin_url,
    )

    # Add phone if available
    phone_columns = ["phone", "mobile", "mobile_phone", "work_phone"]
    for col in phone_columns:
        if col in row and pd.notna(row[col]):
            contact.mobile_phone = str(row[col]).strip()
            break

    session.add(contact)
    await session.flush()

    return contact


async def import_leads(
    session: AsyncSession,
    file_path: str,
    source: str = "import",
    skip_duplicates: bool = True,
    column_mapping: dict[str, str] | None = None,
) -> LeadImportResult:
    """
    Import leads from a file (auto-detects format).

    Args:
        session: Database session
        file_path: Path to file (CSV or JSON)
        source: Source identifier
        skip_duplicates: Skip existing domains if True
        column_mapping: Optional column mapping for CSV

    Returns:
        LeadImportResult with import statistics
    """
    path = Path(file_path)
    suffix = path.suffix.lower()

    if suffix == ".csv":
        return await import_csv(
            session=session,
            file_path=file_path,
            source=source,
            skip_duplicates=skip_duplicates,
            column_mapping=column_mapping,
        )
    elif suffix == ".json":
        return await import_json(
            session=session,
            file_path=file_path,
            source=source,
            skip_duplicates=skip_duplicates,
        )
    else:
        return LeadImportResult(
            total_rows=0,
            imported=0,
            skipped=0,
            errors=[f"Unsupported file format: {suffix}. Use .csv or .json"],
        )
