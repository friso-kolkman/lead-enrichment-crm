"""Typer CLI for lead enrichment pipeline."""

import asyncio
import logging
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from config import settings
from core.database import db, init_db
from utils.rate_limiter import configure_provider_limits

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

app = typer.Typer(
    name="lead-enrichment",
    help="Lead enrichment and outbound campaign pipeline",
)
console = Console()


def run_async(coro):
    """Run an async function."""
    return asyncio.get_event_loop().run_until_complete(coro)


@app.command()
def enrich(
    input_file: Optional[str] = typer.Option(None, "--input", "-i", help="Input CSV/JSON file"),
    start: int = typer.Option(1, "--start", "-s", help="Starting stage (1-9)"),
    end: int = typer.Option(9, "--end", "-e", help="Ending stage (1-9)"),
    limit: int = typer.Option(100, "--limit", "-l", help="Maximum leads to process"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Don't send emails or sync to CRM"),
    force: bool = typer.Option(False, "--force", "-f", help="Re-process already processed leads"),
):
    """Run the enrichment pipeline."""
    from pipeline.orchestrator import PipelineOrchestrator

    async def _run():
        await init_db()
        configure_provider_limits()

        async with db.session() as session:
            orch = PipelineOrchestrator()

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task("Running pipeline...", total=None)

                if start == 1 and end == 9:
                    result = await orch.run_full_pipeline(
                        session=session,
                        input_file=input_file,
                        limit=limit,
                        dry_run=dry_run,
                    )
                else:
                    result = await orch.run_stages(
                        session=session,
                        start=start,
                        end=end,
                        input_file=input_file,
                        limit=limit,
                        force=force,
                    )

                progress.update(task, completed=True)

            # Display results
            console.print("\n[bold green]Pipeline Complete[/bold green]\n")

            for stage, stage_result in result.get("results", {}).items():
                console.print(f"[bold]Stage {stage}:[/bold] {stage_result}")

            if "budget_status" in result:
                budget = result["budget_status"]
                console.print(f"\n[bold]Budget:[/bold] ${budget['spent_this_month']:.2f} / ${budget['monthly_budget']:.2f}")

    run_async(_run())


@app.command()
def score(
    min_score: int = typer.Option(0, "--min", "-m", help="Minimum score filter"),
    tier: Optional[str] = typer.Option(None, "--tier", "-t", help="Filter by tier"),
    limit: int = typer.Option(50, "--limit", "-l", help="Maximum leads to show"),
):
    """View scored leads."""
    from sqlalchemy import select
    from core.models import Lead, Company, Contact, LeadTier

    async def _run():
        await init_db()

        async with db.session() as session:
            query = select(Lead).where(Lead.total_score.isnot(None))

            if min_score > 0:
                query = query.where(Lead.total_score >= min_score)
            if tier:
                try:
                    tier_enum = LeadTier(tier)
                    query = query.where(Lead.tier == tier_enum)
                except ValueError:
                    console.print(f"[red]Invalid tier: {tier}[/red]")
                    return

            query = query.order_by(Lead.total_score.desc()).limit(limit)
            result = await session.execute(query)
            leads = result.scalars().all()

            if not leads:
                console.print("[yellow]No leads found matching criteria[/yellow]")
                return

            # Load companies
            company_ids = list({l.company_id for l in leads})
            company_result = await session.execute(
                select(Company).where(Company.id.in_(company_ids))
            )
            companies = {c.id: c for c in company_result.scalars().all()}

            # Create table
            table = Table(title=f"Scored Leads (min score: {min_score})")
            table.add_column("ID", style="dim")
            table.add_column("Company")
            table.add_column("Score", justify="right")
            table.add_column("Tier")
            table.add_column("Status")
            table.add_column("Industry")

            for lead in leads:
                company = companies.get(lead.company_id)
                tier_color = {
                    LeadTier.HIGH_TOUCH: "green",
                    LeadTier.STANDARD: "yellow",
                    LeadTier.NURTURE: "dim",
                }.get(lead.tier, "white")

                table.add_row(
                    str(lead.id),
                    company.name or company.domain if company else "Unknown",
                    str(lead.total_score),
                    f"[{tier_color}]{lead.tier.value if lead.tier else 'N/A'}[/{tier_color}]",
                    lead.status.value if lead.status else "N/A",
                    company.industry if company else "N/A",
                )

            console.print(table)

    run_async(_run())


@app.command()
def sync(
    to_crm: bool = typer.Option(False, "--to-crm", help="Sync to Attio CRM"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Don't actually sync"),
    limit: int = typer.Option(100, "--limit", "-l", help="Maximum leads to sync"),
    min_score: Optional[int] = typer.Option(None, "--min-score", help="Minimum score filter"),
):
    """Sync leads to CRM."""
    from pipeline.stages import crm_sync
    from core.models import LeadTier

    if not to_crm:
        console.print("[yellow]Use --to-crm flag to sync to Attio CRM[/yellow]")
        return

    async def _run():
        await init_db()
        configure_provider_limits()

        async with db.session() as session:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task("Syncing to CRM...", total=None)

                result = await crm_sync.sync_leads_batch(
                    session=session,
                    limit=limit,
                    min_score=min_score,
                    dry_run=dry_run,
                )

                progress.update(task, completed=True)

            console.print(f"\n[bold green]Sync Complete[/bold green]")
            console.print(f"Total: {result.total}")
            console.print(f"Created: {result.created}")
            console.print(f"Updated: {result.updated}")
            console.print(f"Failed: {result.failed}")

            if result.errors:
                console.print(f"\n[red]Errors:[/red]")
                for error in result.errors[:5]:
                    console.print(f"  - {error}")

    run_async(_run())


@app.command()
def budget():
    """Check budget status."""
    from utils.cost_tracker import cost_tracker

    async def _run():
        await init_db()

        async with db.session() as session:
            status = await cost_tracker.get_budget_status(session)

            console.print("\n[bold]Budget Status[/bold]\n")

            # Progress bar
            pct = status["percentage_used"]
            bar_width = 40
            filled = int(bar_width * pct / 100)
            bar = "█" * filled + "░" * (bar_width - filled)

            color = "green" if pct < 50 else "yellow" if pct < 80 else "red"
            console.print(f"[{color}]{bar}[/{color}] {pct:.1f}%")

            console.print(f"\nSpent: ${status['spent_this_month']:.2f}")
            console.print(f"Budget: ${status['monthly_budget']:.2f}")
            console.print(f"Remaining: ${status['remaining']:.2f}")

            if status["breakdown_by_provider"]:
                console.print("\n[bold]By Provider:[/bold]")
                for provider, cost in sorted(status["breakdown_by_provider"].items(), key=lambda x: -x[1]):
                    console.print(f"  {provider}: ${cost:.2f}")

            if status["is_over_budget"]:
                console.print("\n[red bold]⚠ Budget exceeded![/red bold]")
            elif status["alert_threshold_reached"]:
                console.print("\n[yellow]⚠ Budget alert threshold reached[/yellow]")

    run_async(_run())


@app.command()
def status():
    """Show pipeline status."""
    from pipeline.orchestrator import PipelineOrchestrator

    async def _run():
        await init_db()

        async with db.session() as session:
            orch = PipelineOrchestrator()
            status = await orch.get_pipeline_status(session)

            console.print("\n[bold]Pipeline Status[/bold]\n")
            console.print(f"Total Leads: {status['total_leads']}")

            # Status breakdown
            console.print("\n[bold]By Status:[/bold]")
            for s, count in sorted(status["by_status"].items()):
                console.print(f"  {s}: {count}")

            # Tier breakdown
            if status["by_tier"]:
                console.print("\n[bold]By Tier:[/bold]")
                for tier, count in status["by_tier"].items():
                    color = {"high_touch": "green", "standard": "yellow", "nurture": "dim"}.get(tier, "white")
                    console.print(f"  [{color}]{tier}[/{color}]: {count}")

            # Progress
            console.print("\n[bold]Progress:[/bold]")
            console.print(f"  Enrichment: {status['enrichment_progress']:.1f}%")
            console.print(f"  Scoring: {status['scoring_progress']:.1f}%")
            console.print(f"  Research: {status['research_progress']:.1f}%")

    run_async(_run())


@app.command()
def init():
    """Initialize the database."""
    async def _run():
        console.print("Initializing database...")
        await init_db()
        console.print("[green]Database initialized successfully![/green]")

    run_async(_run())


@app.command()
def dashboard(
    host: str = typer.Option("0.0.0.0", "--host", "-h", help="Host to bind to"),
    port: int = typer.Option(8000, "--port", "-p", help="Port to bind to"),
):
    """Launch the web dashboard."""
    import uvicorn

    console.print(f"Starting dashboard at http://{host}:{port}")
    uvicorn.run(
        "app:app",
        host=host,
        port=port,
        reload=True,
    )


@app.command()
def campaign(
    create: Optional[str] = typer.Option(None, "--create", "-c", help="Create campaign with name"),
    launch: Optional[int] = typer.Option(None, "--launch", "-l", help="Launch campaign by ID"),
    list_campaigns: bool = typer.Option(False, "--list", help="List all campaigns"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Don't actually send emails"),
):
    """Manage email campaigns."""
    from pipeline.stages import campaign as campaign_module
    from sqlalchemy import select
    from core.models import Campaign

    async def _run():
        await init_db()
        configure_provider_limits()

        async with db.session() as session:
            if create:
                c = await campaign_module.create_campaign(
                    session=session,
                    name=create,
                )
                console.print(f"[green]Created campaign '{create}' (ID: {c.id})[/green]")

            elif launch is not None:
                result = await campaign_module.launch_campaign(
                    session=session,
                    campaign_id=launch,
                    dry_run=dry_run,
                )
                if "error" in result:
                    console.print(f"[red]Error: {result['error']}[/red]")
                else:
                    console.print(f"[green]Campaign launched![/green]")
                    console.print(f"Sent: {result['sent']}")
                    console.print(f"Failed: {result['failed']}")

            elif list_campaigns:
                result = await session.execute(select(Campaign))
                campaigns = result.scalars().all()

                if not campaigns:
                    console.print("[yellow]No campaigns found[/yellow]")
                    return

                table = Table(title="Campaigns")
                table.add_column("ID")
                table.add_column("Name")
                table.add_column("Active")
                table.add_column("Sent")
                table.add_column("Target Tier")

                for c in campaigns:
                    status = "[green]✓[/green]" if c.is_active else "[red]✗[/red]"
                    table.add_row(
                        str(c.id),
                        c.name,
                        status,
                        str(c.total_sent),
                        c.target_tier.value if c.target_tier else "All",
                    )

                console.print(table)

            else:
                console.print("Use --create, --launch, or --list")

    run_async(_run())


@app.command()
def providers():
    """Show configured enrichment providers."""
    from enrichment.cascade import cascade_manager

    status = cascade_manager.get_provider_status()

    table = Table(title="Enrichment Providers")
    table.add_column("Provider")
    table.add_column("Status")
    table.add_column("Cost/Request")
    table.add_column("URL")

    for name, info in status.items():
        if info["configured"]:
            status_str = "[green]Configured[/green]"
        else:
            status_str = "[red]Not configured[/red]"

        table.add_row(
            name,
            status_str,
            f"${info.get('cost_per_request', 0):.3f}" if info["configured"] else "-",
            info.get("base_url", "-") if info["configured"] else "-",
        )

    console.print(table)


if __name__ == "__main__":
    app()
