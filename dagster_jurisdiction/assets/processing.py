"""Processing tier asset — clean and normalize census data."""

import logging
import dagster as dg

from src.pipeline import phase3_data_processing
from src.models.census_geography import CensusGeography
from dagster_jurisdiction.resources.database import DatabaseResource
from dagster_jurisdiction.resources.pipeline_config import PipelineConfigResource

logger = logging.getLogger("jurisdiction.dagster.processing")


@dg.asset(
    group_name="3_processing",
    deps=["census_geographies"],
    description=(
        "Filter, clean, and normalize census data: remove CDPs, "
        "inactive entities, unorganized MCDs. Handle Virginia independent cities."
    ),
    kinds={"python", "postgres"},
)
def processed_geographies(
    context: dg.AssetExecutionContext,
    database: DatabaseResource,
    pipeline_config: PipelineConfigResource,
) -> dg.MaterializeResult:
    """Execute Phase 3: data processing and cleaning.

    Operates in-place on census_geographies table — removes irrelevant
    rows and normalizes names.
    """
    config = pipeline_config.to_config()
    session = database.get_session()
    try:
        removed_count = phase3_data_processing.execute(session, config, states=None)
        session.commit()

        # Validate: ensure table still has data after filtering
        db_count = session.query(CensusGeography).count()
        if db_count == 0:
            raise RuntimeError(
                f"Phase 3 FAILED: census_geographies table is empty after "
                f"processing (removed {removed_count} rows). "
                f"All rows were filtered out."
            )

        context.log.info(
            f"Phase 3 complete: {removed_count} records removed, "
            f"{db_count} remaining in DB"
        )
        return dg.MaterializeResult(
            metadata={
                "records_removed": dg.MetadataValue.int(removed_count),
                "db_row_count": dg.MetadataValue.int(db_count),
            }
        )
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
