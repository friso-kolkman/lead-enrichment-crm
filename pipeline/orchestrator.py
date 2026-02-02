"""Pipeline orchestrator - coordinates all 9 stages."""

import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import db
from core.models import Lead, LeadStatus
from pipeline.stages import (
    ingestion,
    company_enrichment,
    contact_enrichment,
    email_verification,
    scoring,
    ai_research,
    messaging,
    crm_sync,
    campaign,
)
from utils.cost_tracker import BudgetExceeded, cost_tracker

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Orchestrates the 9-stage lead enrichment pipeline."""

    STAGES = {
        1: ("ingestion", "Import leads from CSV/JSON"),
        2: ("company_enrichment", "Enrich company firmographics and technographics"),
        3: ("contact_enrichment", "Enrich contact details"),
        4: ("email_verification", "Verify email addresses"),
        5: ("scoring", "Score leads against ICP"),
        6: ("ai_research", "AI-powered research"),
        7: ("messaging", "Generate personalized messaging"),
        8: ("crm_sync", "Sync to Attio CRM"),
        9: ("campaign", "Launch email campaigns"),
    }

    def __init__(self):
        self.results: dict[int, dict[str, Any]] = {}

    async def run_full_pipeline(
        self,
        session: AsyncSession,
        input_file: str | None = None,
        limit: int = 100,
        dry_run: bool = False,
        value_prop: str = "We help companies like yours improve efficiency and drive growth.",
    ) -> dict[str, Any]:
        """
        Run the complete pipeline from ingestion to campaign.

        Args:
            session: Database session
            input_file: Path to CSV/JSON file for ingestion
            limit: Maximum leads to process
            dry_run: If True, don't send emails or sync to CRM
            value_prop: Value proposition for email generation

        Returns:
            Dict with results from all stages
        """
        logger.info("Starting full pipeline run")

        # Stage 1: Ingestion
        if input_file:
            result = await self.run_stage(session, 1, input_file=input_file)
            if result.get("errors"):
                logger.warning(f"Ingestion errors: {result['errors'][:3]}")

        # Stage 2: Company Enrichment
        result = await self.run_stage(session, 2, limit=limit)

        # Stage 3: Contact Enrichment
        result = await self.run_stage(session, 3, limit=limit)

        # Stage 4: Email Verification
        result = await self.run_stage(session, 4, limit=limit)

        # Update leads to ENRICHED status
        await self._update_enriched_leads(session, limit)

        # Stage 5: Scoring
        result = await self.run_stage(session, 5, limit=limit)

        # Stage 6: AI Research
        result = await self.run_stage(session, 6, limit=limit)

        # Stage 7: Messaging
        result = await self.run_stage(session, 7, limit=limit, value_prop=value_prop)

        # Stage 8: CRM Sync
        if not dry_run:
            result = await self.run_stage(session, 8, limit=limit)

        # Stage 9: Campaign (only if requested)
        # Campaign launching is usually done separately

        # Commit all changes
        await session.commit()

        return {
            "stages_completed": len(self.results),
            "results": self.results,
            "budget_status": await cost_tracker.get_budget_status(session),
        }

    async def run_stage(
        self,
        session: AsyncSession,
        stage: int,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Run a specific pipeline stage.

        Args:
            session: Database session
            stage: Stage number (1-9)
            **kwargs: Stage-specific arguments

        Returns:
            Dict with stage results
        """
        if stage not in self.STAGES:
            return {"error": f"Invalid stage: {stage}"}

        stage_name, stage_desc = self.STAGES[stage]
        logger.info(f"Running Stage {stage}: {stage_name} - {stage_desc}")

        try:
            result = await self._execute_stage(session, stage, **kwargs)
            self.results[stage] = result
            logger.info(f"Stage {stage} complete: {result}")
            return result

        except BudgetExceeded as e:
            logger.error(f"Budget exceeded in stage {stage}: {e}")
            result = {"error": "Budget exceeded", "spent": e.spent, "budget": e.budget}
            self.results[stage] = result
            return result

        except Exception as e:
            logger.error(f"Stage {stage} failed: {e}")
            result = {"error": str(e)}
            self.results[stage] = result
            return result

    async def _execute_stage(
        self,
        session: AsyncSession,
        stage: int,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute the actual stage logic."""
        if stage == 1:
            # Ingestion
            input_file = kwargs.get("input_file")
            if not input_file:
                return {"error": "No input file provided"}
            return (await ingestion.import_leads(
                session=session,
                file_path=input_file,
                source=kwargs.get("source", "pipeline"),
                skip_duplicates=kwargs.get("skip_duplicates", True),
            )).model_dump()

        elif stage == 2:
            # Company Enrichment
            return await company_enrichment.enrich_leads_companies(
                session=session,
                limit=kwargs.get("limit", 100),
                force=kwargs.get("force", False),
            )

        elif stage == 3:
            # Contact Enrichment
            return await contact_enrichment.enrich_leads_contacts(
                session=session,
                limit=kwargs.get("limit", 100),
                force=kwargs.get("force", False),
            )

        elif stage == 4:
            # Email Verification
            return await email_verification.verify_leads_emails(
                session=session,
                limit=kwargs.get("limit", 100),
                force=kwargs.get("force", False),
            )

        elif stage == 5:
            # Scoring
            return await scoring.score_leads_batch(
                session=session,
                limit=kwargs.get("limit", 500),
                force=kwargs.get("force", False),
            )

        elif stage == 6:
            # AI Research
            return await ai_research.research_leads_batch(
                session=session,
                limit=kwargs.get("limit", 50),
                force=kwargs.get("force", False),
            )

        elif stage == 7:
            # Messaging
            return await messaging.generate_messaging_batch(
                session=session,
                limit=kwargs.get("limit", 50),
                value_prop=kwargs.get("value_prop", "We help companies like yours improve efficiency and drive growth."),
                force=kwargs.get("force", False),
            )

        elif stage == 8:
            # CRM Sync
            return (await crm_sync.sync_leads_batch(
                session=session,
                limit=kwargs.get("limit", 100),
                dry_run=kwargs.get("dry_run", False),
            )).model_dump()

        elif stage == 9:
            # Campaign
            campaign_id = kwargs.get("campaign_id")
            if not campaign_id:
                return {"error": "No campaign_id provided"}
            return await campaign.launch_campaign(
                session=session,
                campaign_id=campaign_id,
                limit=kwargs.get("limit"),
                dry_run=kwargs.get("dry_run", False),
            )

        return {"error": "Stage not implemented"}

    async def run_stages(
        self,
        session: AsyncSession,
        start: int = 1,
        end: int = 9,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Run a range of pipeline stages.

        Args:
            session: Database session
            start: Starting stage number (1-9)
            end: Ending stage number (1-9)
            **kwargs: Arguments passed to all stages

        Returns:
            Dict with results from all stages
        """
        logger.info(f"Running stages {start} to {end}")

        for stage in range(start, end + 1):
            result = await self.run_stage(session, stage, **kwargs)
            if result.get("error") and "Budget exceeded" in str(result.get("error", "")):
                break

        return {
            "stages_run": list(range(start, end + 1)),
            "results": {k: v for k, v in self.results.items() if start <= k <= end},
        }

    async def _update_enriched_leads(
        self,
        session: AsyncSession,
        limit: int = 100,
    ) -> None:
        """Update leads from ENRICHING to ENRICHED status."""
        result = await session.execute(
            select(Lead)
            .where(Lead.status == LeadStatus.ENRICHING)
            .limit(limit)
        )
        leads = result.scalars().all()

        for lead in leads:
            lead.status = LeadStatus.ENRICHED

        logger.info(f"Updated {len(leads)} leads to ENRICHED status")

    async def get_pipeline_status(
        self,
        session: AsyncSession,
    ) -> dict[str, Any]:
        """Get overall pipeline status."""
        from sqlalchemy import func

        # Count leads by status
        status_result = await session.execute(
            select(Lead.status, func.count(Lead.id)).group_by(Lead.status)
        )
        by_status = {row[0].value: row[1] for row in status_result.fetchall()}

        # Count leads by tier
        tier_result = await session.execute(
            select(Lead.tier, func.count(Lead.id))
            .where(Lead.tier.isnot(None))
            .group_by(Lead.tier)
        )
        by_tier = {row[0].value: row[1] for row in tier_result.fetchall()}

        # Total leads
        total_result = await session.execute(select(func.count(Lead.id)))
        total_leads = total_result.scalar_one()

        # Calculate progress
        enriched_statuses = [
            LeadStatus.ENRICHED.value,
            LeadStatus.SCORED.value,
            LeadStatus.RESEARCHED.value,
            LeadStatus.READY.value,
            LeadStatus.SYNCED.value,
            LeadStatus.CONTACTED.value,
        ]
        enriched_count = sum(by_status.get(s, 0) for s in enriched_statuses)

        scored_statuses = [
            LeadStatus.SCORED.value,
            LeadStatus.RESEARCHED.value,
            LeadStatus.READY.value,
            LeadStatus.SYNCED.value,
            LeadStatus.CONTACTED.value,
        ]
        scored_count = sum(by_status.get(s, 0) for s in scored_statuses)

        researched_statuses = [
            LeadStatus.RESEARCHED.value,
            LeadStatus.READY.value,
            LeadStatus.SYNCED.value,
            LeadStatus.CONTACTED.value,
        ]
        researched_count = sum(by_status.get(s, 0) for s in researched_statuses)

        return {
            "total_leads": total_leads,
            "by_status": by_status,
            "by_tier": by_tier,
            "enrichment_progress": (enriched_count / total_leads * 100) if total_leads > 0 else 0,
            "scoring_progress": (scored_count / total_leads * 100) if total_leads > 0 else 0,
            "research_progress": (researched_count / total_leads * 100) if total_leads > 0 else 0,
        }


# Global orchestrator instance
orchestrator = PipelineOrchestrator()


async def run_pipeline(
    input_file: str | None = None,
    start_stage: int = 1,
    end_stage: int = 9,
    limit: int = 100,
    dry_run: bool = False,
    value_prop: str = "We help companies like yours improve efficiency and drive growth.",
) -> dict[str, Any]:
    """
    Convenience function to run the pipeline.

    Args:
        input_file: Path to CSV/JSON file for ingestion
        start_stage: Starting stage number
        end_stage: Ending stage number
        limit: Maximum leads to process
        dry_run: If True, don't send emails or sync to CRM
        value_prop: Value proposition for email generation

    Returns:
        Dict with pipeline results
    """
    async with db.session() as session:
        orch = PipelineOrchestrator()

        if start_stage == 1 and end_stage == 9:
            return await orch.run_full_pipeline(
                session=session,
                input_file=input_file,
                limit=limit,
                dry_run=dry_run,
                value_prop=value_prop,
            )
        else:
            return await orch.run_stages(
                session=session,
                start=start_stage,
                end=end_stage,
                input_file=input_file,
                limit=limit,
                dry_run=dry_run,
                value_prop=value_prop,
            )
