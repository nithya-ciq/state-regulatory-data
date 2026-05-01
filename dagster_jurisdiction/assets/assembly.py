"""Assembly tier assets — combine data into final jurisdiction rows."""

import logging
import dagster as dg

from src.pipeline import phase4_jurisdiction_assembly, phase4b_enrichment
from src.models.jurisdiction import Jurisdiction
from dagster_jurisdiction.resources.database import DatabaseResource
from dagster_jurisdiction.resources.pipeline_config import PipelineConfigResource

logger = logging.getLogger("jurisdiction.dagster.assembly")


@dg.asset(
    group_name="4_assembly",
    deps=["processed_geographies"],
    description=(
        "Combine state classifications + processed census data into "
        "jurisdiction rows with FIPS breakdown, tier, and jurisdiction type."
    ),
    kinds={"python", "postgres"},
)
def jurisdictions(
    context: dg.AssetExecutionContext,
    database: DatabaseResource,
    pipeline_config: PipelineConfigResource,
) -> dg.MaterializeResult:
    """Execute Phase 4: jurisdiction assembly.

    Reads from state_classifications and census_geographies tables,
    writes to jurisdictions table.
    """
    config = pipeline_config.to_config()
    session = database.get_session()
    try:
        count = phase4_jurisdiction_assembly.execute(
            session, config.census_year, states=None
        )
        session.commit()

        # Validate: verify jurisdictions table has data
        db_count = session.query(Jurisdiction).count()
        if db_count == 0:
            raise RuntimeError(
                "Phase 4 FAILED: jurisdictions table is empty after assembly. "
                "Check state_classifications and census_geographies data."
            )

        context.log.info(
            f"Phase 4 complete: {count} jurisdiction rows created, "
            f"{db_count} total in DB"
        )
        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "db_row_count": dg.MetadataValue.int(db_count),
            }
        )
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@dg.asset(
    group_name="4_assembly",
    deps=["jurisdictions"],
    description=(
        "Enrich jurisdictions with dry/wet status, licensing authority names, "
        "and regulatory overrides from seed CSVs."
    ),
    kinds={"python", "postgres"},
)
def enriched_jurisdictions(
    context: dg.AssetExecutionContext,
    database: DatabaseResource,
    pipeline_config: PipelineConfigResource,
) -> dg.MaterializeResult:
    """Execute Phase 4b: enrichment overlay.

    Applies four enrichment layers to the jurisdictions table:
    1. Dry/wet status
    2. Licensing authority names (overrides + patterns)
    3. Regulatory details propagation
    4. Per-GEOID regulatory overrides
    """
    config = pipeline_config.to_config()
    session = database.get_session()
    try:
        count = phase4b_enrichment.execute(session, config, states=None)
        session.commit()

        if count == 0:
            context.log.warning(
                "Phase 4b: enrichment returned 0 updates — "
                "no dry/wet status or authority names were applied"
            )

        context.log.info(f"Phase 4b complete: {count} enrichment updates")
        return dg.MaterializeResult(
            metadata={"enrichment_updates": dg.MetadataValue.int(count)}
        )
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
