"""Pipeline orchestrator — main entry point for the jurisdiction taxonomy pipeline.

Coordinates the five pipeline phases sequentially with per-state
transaction boundaries and resumability support.
"""

import logging
import sys
from datetime import datetime
from typing import Dict, List, Optional

import click
from sqlalchemy.orm import Session

from src.census.cache import DownloadManifest
from src.census.tiger_client import TigerClient
from src.common.exceptions import PipelineError
from src.common.logging import setup_logging
from src.config import Config
from src.db.repository import Repository
from src.db.session import get_session
from src.models.pipeline_run import PipelineRun
from src.pipeline import (
    phase1_state_classification,
    phase2_census_acquisition,
    phase3_data_processing,
    phase4_jurisdiction_assembly,
    phase4b_enrichment,
    phase5_validation_export,
)

logger = logging.getLogger("jurisdiction.orchestrator")


class PipelineOrchestrator:
    """Main pipeline runner with resumability and audit tracking."""

    def __init__(self, config: Config, session: Session) -> None:
        self.config = config
        self.session = session
        self.repo = Repository(session)

    def run(
        self,
        states: Optional[List[str]] = None,
        start_phase: int = 1,
    ) -> Dict:
        """Run the full pipeline.

        Args:
            states: Optional list of state FIPS to process. None = all.
            start_phase: Phase number to start from (1-5) for resumability.

        Returns:
            Dict with pipeline run results.
        """
        logger.info("=" * 60)
        logger.info("JURISDICTION TAXONOMY PIPELINE — STARTING")
        logger.info(f"Census year: {self.config.census_year}")
        logger.info(f"Start phase: {start_phase}")
        logger.info(f"States: {states or 'all'}")
        logger.info("=" * 60)

        # Create pipeline run record
        run = PipelineRun(
            status="running",
            census_year=self.config.census_year,
            states_processed=states,
            config_snapshot={
                "census_year": self.config.census_year,
                "tiger_resolution": self.config.tiger_resolution,
                "skip_territories": self.config.skip_territories,
                "start_phase": start_phase,
            },
        )
        self.session.add(run)
        self.session.commit()

        try:
            # Phase 1: State Classification
            if start_phase <= 1:
                seed_path = self.config.seed_dir / "state_classification_matrix.csv"
                phase1_state_classification.execute(self.session, seed_path)
                run.phase_reached = "phase1_classification"
                self.session.commit()

            # Phase 2: Census Acquisition
            if start_phase <= 2:
                self.config.ensure_directories()
                manifest = DownloadManifest(self.config.cache_dir)
                tiger_client = TigerClient(
                    manifest=manifest,
                    year=self.config.census_year,
                    resolution=self.config.tiger_resolution,
                    force_redownload=self.config.force_redownload,
                )
                phase2_census_acquisition.execute(
                    self.session, self.config, tiger_client, states
                )
                run.phase_reached = "phase2_acquisition"
                self.session.commit()

            # Phase 3: Data Processing
            if start_phase <= 3:
                phase3_data_processing.execute(self.session, self.config, states)
                run.phase_reached = "phase3_processing"
                self.session.commit()

            # Phase 4: Jurisdiction Assembly
            if start_phase <= 4:
                phase4_jurisdiction_assembly.execute(
                    self.session, self.config.census_year, states
                )
                run.phase_reached = "phase4_assembly"
                self.session.commit()

            # Phase 4b: Enrichment (dry/wet status + licensing authority names)
            if start_phase <= 5:
                phase4b_enrichment.execute(self.session, self.config, states)
                run.phase_reached = "phase4b_enrichment"
                self.session.commit()

            # Phase 5: Validation & Export
            if start_phase <= 5:
                results = phase5_validation_export.execute(self.session, self.config)
                run.phase_reached = "phase5_complete"
                run.total_jurisdictions = results["validation"]["total"]
                self.session.commit()

            run.status = "completed"
            run.completed_at = datetime.utcnow()
            self.session.commit()

            logger.info("=" * 60)
            logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 60)

            return {
                "status": "completed",
                "run_id": str(run.run_id),
                "total_jurisdictions": run.total_jurisdictions,
                "phase_reached": run.phase_reached,
            }

        except Exception as e:
            run.status = "failed"
            run.error_message = str(e)
            run.completed_at = datetime.utcnow()
            self.session.commit()

            logger.error(f"Pipeline failed: {e}", exc_info=True)
            raise PipelineError(run.phase_reached or "unknown", str(e))


@click.command()
@click.option(
    "--states",
    default=None,
    help="Comma-separated state FIPS codes to process (default: all)",
)
@click.option(
    "--phase",
    default=1,
    type=int,
    help="Start from phase number (1-5, default: 1)",
)
@click.option(
    "--year",
    default=None,
    type=int,
    help="Census TIGER year (default: from config)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Validate only, do not write to database",
)
def main(states: Optional[str], phase: int, year: Optional[int], dry_run: bool) -> None:
    """Run the US Alcohol Licensing Jurisdiction Taxonomy pipeline."""
    config = Config()

    if year:
        config.census_year = year

    setup_logging(config.log_level)

    state_list = [s.strip() for s in states.split(",")] if states else None

    if dry_run:
        logger.info("DRY RUN MODE — no database writes")
        return

    with get_session(config) as session:
        orchestrator = PipelineOrchestrator(config, session)
        result = orchestrator.run(states=state_list, start_phase=phase)

    logger.info(f"Pipeline result: {result}")


if __name__ == "__main__":
    main()
