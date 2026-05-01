"""Raw tier assets — ingest external data into the database."""

import logging
import dagster as dg

from src.pipeline import phase1_state_classification, phase1b_license_types, phase2_census_acquisition
from src.models.state_classification import StateClassification
from src.models.census_geography import CensusGeography
from src.models.license_type import LicenseType
from dagster_jurisdiction.resources.database import DatabaseResource
from dagster_jurisdiction.resources.pipeline_config import PipelineConfigResource
from dagster_jurisdiction.resources.tiger import TigerClientResource

logger = logging.getLogger("jurisdiction.dagster.raw")

# Minimum expected state classifications (50 states + DC + 5 territories)
MIN_STATE_CLASSIFICATIONS = 56


@dg.asset(
    group_name="2_raw",
    deps=["research_data"],
    description="Load and validate the 56-row state classification matrix from seed CSV.",
    kinds={"python", "postgres"},
)
def state_classifications(
    context: dg.AssetExecutionContext,
    database: DatabaseResource,
    pipeline_config: PipelineConfigResource,
) -> dg.MaterializeResult:
    """Execute Phase 1: state classification loading.

    Reads state_classification_matrix.csv and upserts to state_classifications table.
    """
    config = pipeline_config.to_config()
    session = database.get_session()
    try:
        seed_path = config.seed_dir / "state_classification_matrix.csv"
        count = phase1_state_classification.execute(session, seed_path)
        session.commit()

        # Validate: verify DB actually has the expected rows
        db_count = session.query(StateClassification).count()
        if db_count < MIN_STATE_CLASSIFICATIONS:
            raise RuntimeError(
                f"Phase 1 FAILED: expected >= {MIN_STATE_CLASSIFICATIONS} "
                f"state classifications, found {db_count}"
            )

        context.log.info(
            f"Phase 1 complete: {count} loaded, {db_count} total in DB"
        )
        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "db_row_count": dg.MetadataValue.int(db_count),
                "seed_file": dg.MetadataValue.path(str(seed_path)),
            }
        )
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@dg.asset(
    group_name="2_raw",
    deps=["state_classifications"],
    description="Download Census TIGER/Line shapefiles for counties, places, and MCDs.",
    kinds={"python", "postgres"},
)
def census_geographies(
    context: dg.AssetExecutionContext,
    database: DatabaseResource,
    pipeline_config: PipelineConfigResource,
    tiger_client: TigerClientResource,
) -> dg.MaterializeResult:
    """Execute Phase 2: Census data acquisition.

    Downloads TIGER/Line data via pygris and stores in census_geographies table.
    """
    config = pipeline_config.to_config()
    session = database.get_session()
    try:
        client = tiger_client.create_client()
        newly_loaded = phase2_census_acquisition.execute(
            session, config, client, states=None
        )
        session.commit()

        # Validate: check actual DB row count (not just newly loaded)
        db_count = session.query(CensusGeography).count()

        if db_count == 0:
            raise RuntimeError(
                f"Phase 2 FAILED: census_geographies table is empty "
                f"(newly_loaded={newly_loaded}). "
                f"Check TIGER downloads and cache manifest."
            )

        if newly_loaded == 0:
            context.log.warning(
                f"Phase 2: 0 new records loaded (data already cached), "
                f"but {db_count} existing rows found in DB — OK"
            )
        else:
            context.log.info(
                f"Phase 2 complete: {newly_loaded} new records loaded, "
                f"{db_count} total in DB"
            )

        return dg.MaterializeResult(
            metadata={
                "newly_loaded": dg.MetadataValue.int(newly_loaded),
                "db_row_count": dg.MetadataValue.int(db_count),
            }
        )
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@dg.asset(
    group_name="2_raw",
    deps=["state_classifications"],
    description="Load per-state license type catalog from seed CSV.",
    kinds={"python", "postgres"},
)
def license_types(
    context: dg.AssetExecutionContext,
    database: DatabaseResource,
    pipeline_config: PipelineConfigResource,
) -> dg.MaterializeResult:
    """Execute Phase 1b: license types catalog loading.

    Reads license_types.csv and upserts to license_types table.
    Also updates license_type_count on state_classifications.
    """
    config = pipeline_config.to_config()
    session = database.get_session()
    try:
        seed_path = config.seed_dir / "license_types.csv"
        count = phase1b_license_types.execute(session, seed_path)
        session.commit()

        db_count = session.query(LicenseType).count()

        if count == 0:
            context.log.warning(
                "Phase 1b: license_types.csv is empty or missing — "
                "no license types loaded. Populate the seed CSV."
            )

        context.log.info(
            f"Phase 1b complete: {count} license types loaded, {db_count} total in DB"
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
