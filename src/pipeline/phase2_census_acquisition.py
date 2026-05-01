"""Phase 2: Download Census TIGER data for each state.

For each state, reads its classification to determine which TIGER layers
are needed, downloads them via pygris, and loads into census_geographies.
"""

import logging
from decimal import Decimal
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy.orm import Session

from src.census.tiger_client import TigerClient
from src.common.constants import FIPS_STATES, TERRITORY_FIPS
from src.common.enums import GeoLayer
from src.common.exceptions import DataAcquisitionError
from src.config import Config
from src.db.repository import Repository
from src.models.census_geography import CensusGeography
from src.models.state_classification import StateClassification

logger = logging.getLogger("jurisdiction.phase2")


def execute(
    session: Session,
    config: Config,
    tiger_client: TigerClient,
    states: Optional[List[str]] = None,
) -> int:
    """Execute Phase 2: download Census TIGER data.

    Args:
        session: SQLAlchemy database session.
        config: Application configuration.
        tiger_client: Configured TigerClient instance.
        states: Optional list of state FIPS codes to process. If None, processes all.

    Returns:
        Total number of census geography records loaded.
    """
    logger.info("Phase 2: Census data acquisition")

    # Get state classifications from database
    classifications = _get_classifications(session, states)

    if not classifications:
        logger.warning("No state classifications found. Run Phase 1 first.")
        return 0

    # Filter out territories if configured
    if config.skip_territories:
        classifications = {
            k: v for k, v in classifications.items() if k not in TERRITORY_FIPS
        }

    repo = Repository(session)
    total_loaded = 0

    for state_fips, classification in classifications.items():
        try:
            count = _process_state(
                session, repo, tiger_client, config, state_fips, classification
            )
            total_loaded += count
            session.commit()
        except Exception as e:
            logger.error(f"Failed to process state {state_fips}: {e}")
            session.rollback()
            raise DataAcquisitionError(
                f"Census acquisition failed for state {state_fips}: {e}"
            )

    logger.info(f"Phase 2 complete: {total_loaded} census geography records loaded")
    return total_loaded


def _get_classifications(
    session: Session, states: Optional[List[str]] = None
) -> Dict[str, StateClassification]:
    """Load state classifications, optionally filtered to specific states."""
    query = session.query(StateClassification)

    if states:
        query = query.filter(StateClassification.state_fips.in_(states))

    # Only process states with at least draft research status
    query = query.filter(
        StateClassification.research_status.in_(["draft", "verified"])
    )

    return {c.state_fips: c for c in query.all()}


def _process_state(
    session: Session,
    repo: Repository,
    tiger_client: TigerClient,
    config: Config,
    state_fips: str,
    classification: StateClassification,
) -> int:
    """Download and load TIGER data for a single state."""
    state_info = FIPS_STATES.get(state_fips, ("??", "Unknown"))
    logger.info(f"Processing {state_info[1]} ({state_fips})")

    # Download layers based on classification
    layer_data = tiger_client.get_all_for_state(
        state_fips=state_fips,
        delegates_to_county=True,  # Always download counties for reference
        delegates_to_municipality=classification.delegates_to_municipality,
        delegates_to_mcd=classification.delegates_to_mcd,
    )

    total = 0
    for layer_name, df in layer_data.items():
        if df.empty:
            continue

        records = _dataframe_to_records(df, state_fips, layer_name, config.census_year)
        if records:
            count = repo.bulk_upsert(
                model=CensusGeography,
                records=records,
                conflict_columns=["geoid", "geo_layer", "census_year"],
            )
            total += count
            logger.info(f"  {layer_name}: loaded {count} records for {state_fips}")

    return total


def _dataframe_to_records(
    df: pd.DataFrame, state_fips: str, geo_layer: str, census_year: int
) -> List[dict]:
    """Convert a TIGER DataFrame to dicts for database insertion.

    Handles the varying column names across county, place, and MCD layers.
    """
    records = []

    for _, row in df.iterrows():
        geoid = str(row.get("GEOID", "")).strip()
        if not geoid:
            continue

        record = {
            "geoid": geoid,
            "geo_layer": geo_layer,
            "state_fips": state_fips,
            "name": str(row.get("NAME", "")).strip(),
            "name_lsad": str(row.get("NAMELSAD", "")).strip() or None,
            "lsad_code": str(row.get("LSAD", "")).strip() or None,
            "functional_status": str(row.get("FUNCSTAT", "")).strip() or None,
            "class_fips": str(row.get("CLASSFP", "")).strip() or None,
            "census_year": census_year,
        }

        # Layer-specific FIPS components
        if geo_layer == GeoLayer.COUNTY.value:
            record["county_fips"] = str(row.get("COUNTYFP", "")).strip() or None
        elif geo_layer == GeoLayer.PLACE.value:
            record["place_fips"] = str(row.get("PLACEFP", "")).strip() or None
        elif geo_layer == GeoLayer.COUNTY_SUBDIVISION.value:
            record["county_fips"] = str(row.get("COUNTYFP", "")).strip() or None
            record["cousub_fips"] = str(row.get("COUSUBFP", "")).strip() or None

        # Geographic attributes
        land_area = row.get("ALAND")
        if pd.notna(land_area):
            try:
                record["land_area_sqm"] = int(land_area)
            except (ValueError, TypeError):
                pass

        water_area = row.get("AWATER")
        if pd.notna(water_area):
            try:
                record["water_area_sqm"] = int(water_area)
            except (ValueError, TypeError):
                pass

        lat = row.get("INTPTLAT")
        if pd.notna(lat):
            try:
                record["latitude"] = Decimal(str(lat).strip().lstrip("+"))
            except Exception:
                pass

        lon = row.get("INTPTLON")
        if pd.notna(lon):
            try:
                record["longitude"] = Decimal(str(lon).strip().lstrip("+"))
            except Exception:
                pass

        records.append(record)

    return records
