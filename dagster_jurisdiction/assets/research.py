"""Research data asset — runs automated research agents to update seed CSVs."""

import logging
from pathlib import Path

import dagster as dg

from dagster_jurisdiction.resources.database import DatabaseResource
from dagster_jurisdiction.resources.pipeline_config import PipelineConfigResource

logger = logging.getLogger("jurisdiction.dagster.research")

# Seed files that the research step must produce/maintain
REQUIRED_SEED_FILES = [
    "state_classification_matrix.csv",
    "dry_wet_status.csv",
    "licensing_authority_names.csv",
]


@dg.asset(
    group_name="1_research",
    description=(
        "Run automated research agents to scrape ABC websites, parse NABCA data, "
        "collect dry/wet status and licensing authority names, and update seed CSVs."
    ),
    kinds={"python"},
)
def research_data(
    context: dg.AssetExecutionContext,
    pipeline_config: PipelineConfigResource,
    database: DatabaseResource,
) -> dg.MaterializeResult:
    """Execute the state research tool to refresh seed data.

    If seed CSVs already exist, validates them and skips re-research.
    Otherwise, runs the StateResearcher to generate initial data.
    """
    config = pipeline_config.to_config()
    seed_dir = config.seed_dir

    # Check if seed files already exist
    existing = []
    missing = []
    for fname in REQUIRED_SEED_FILES:
        fpath = seed_dir / fname
        if fpath.exists() and fpath.stat().st_size > 0:
            existing.append(fname)
        else:
            missing.append(fname)

    if not missing:
        # All seed files exist — just validate and succeed
        context.log.info(
            f"All {len(existing)} seed files present in {seed_dir}. "
            "Skipping re-research (seed data is current)."
        )
        return dg.MaterializeResult(
            metadata={
                "status": dg.MetadataValue.text("seed_files_validated"),
                "seed_dir": dg.MetadataValue.path(str(seed_dir)),
                "files_found": dg.MetadataValue.int(len(existing)),
            }
        )

    # Some seed files missing — run research agents
    context.log.info(
        f"Missing seed files: {missing}. Running research agents..."
    )

    try:
        from src.research.state_researcher import StateResearcher

        session = database.get_session()
        try:
            researcher = StateResearcher(session=session)
            results_df = researcher.research_all_states()

            # Export research worksheet for review
            worksheet_path = seed_dir / "research_worksheet.csv"
            researcher.export_research_worksheet(worksheet_path)

            session.commit()
            context.log.info(
                f"Research complete: {len(results_df)} states researched. "
                f"Worksheet exported to {worksheet_path}"
            )

            return dg.MaterializeResult(
                metadata={
                    "status": dg.MetadataValue.text("research_completed"),
                    "states_researched": dg.MetadataValue.int(len(results_df)),
                    "worksheet_path": dg.MetadataValue.path(str(worksheet_path)),
                }
            )
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    except Exception as e:
        context.log.error(f"Research failed: {e}")
        raise
