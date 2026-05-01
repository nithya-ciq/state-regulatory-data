"""Phase 3: Filter, clean, and normalize census data.

Applies per-state rules to the raw census_geographies data:
- Filter out CDPs (only incorporated places)
- Filter to active entities (FUNCSTAT='A')
- Tag Virginia independent cities
- Filter MCDs to governmental subdivisions
- Normalize names
"""

import logging
from typing import List, Optional

from sqlalchemy.orm import Session

from src.common.constants import (
    CDP_CLASS_CODES,
    INCORPORATED_PLACE_CLASS_CODES,
    VA_INDEPENDENT_CITY_FIPS,
)
from src.common.enums import GeoLayer
from src.config import Config
from src.models.census_geography import CensusGeography

logger = logging.getLogger("jurisdiction.phase3")


def execute(
    session: Session,
    config: Config,
    states: Optional[List[str]] = None,
) -> int:
    """Execute Phase 3: data processing and cleaning.

    This phase operates in-place on the census_geographies table,
    removing rows that don't qualify and tagging special cases.

    Args:
        session: SQLAlchemy database session.
        config: Application configuration.
        states: Optional list of state FIPS to process.

    Returns:
        Number of records removed during processing.
    """
    logger.info("Phase 3: Data processing and cleaning")

    total_removed = 0

    # 1. Remove inactive entities (FUNCSTAT != 'A')
    removed = _remove_inactive_entities(session, states)
    total_removed += removed

    # 2. Remove CDPs from the place layer
    removed = _remove_cdps(session, states)
    total_removed += removed

    # 3. Remove unorganized territory MCDs
    removed = _remove_unorganized_mcds(session, states)
    total_removed += removed

    session.commit()
    logger.info(f"Phase 3 complete: {total_removed} records removed during processing")
    return total_removed


def _remove_inactive_entities(session: Session, states: Optional[List[str]] = None) -> int:
    """Remove census geographies with FUNCSTAT != 'A' (not active).

    Exception: Virginia independent cities (CLASSFP='C7') have FUNCSTAT='F'
    in TIGER data but are legitimate governmental entities. These are preserved.
    """
    from sqlalchemy import and_, not_, or_

    query = session.query(CensusGeography).filter(
        CensusGeography.functional_status.isnot(None),
        CensusGeography.functional_status != "A",
        # Preserve VA independent cities (CLASSFP=C7, FUNCSTAT=F)
        not_(
            and_(
                CensusGeography.geo_layer == GeoLayer.COUNTY.value,
                CensusGeography.class_fips == "C7",
            )
        ),
    )

    if states:
        query = query.filter(CensusGeography.state_fips.in_(states))

    count = query.delete(synchronize_session="fetch")
    if count > 0:
        logger.info(f"  Removed {count} inactive entities (FUNCSTAT != 'A')")
    return count


def _remove_cdps(session: Session, states: Optional[List[str]] = None) -> int:
    """Remove Census Designated Places from the place layer.

    CDPs have no governmental authority and should not appear
    in the jurisdiction taxonomy.
    """
    query = session.query(CensusGeography).filter(
        CensusGeography.geo_layer == GeoLayer.PLACE.value,
        CensusGeography.class_fips.in_(CDP_CLASS_CODES),
    )

    if states:
        query = query.filter(CensusGeography.state_fips.in_(states))

    count = query.delete(synchronize_session="fetch")
    if count > 0:
        logger.info(f"  Removed {count} Census Designated Places (CDPs)")
    return count


def _remove_unorganized_mcds(session: Session, states: Optional[List[str]] = None) -> int:
    """Remove unorganized territory MCDs from the county subdivision layer.

    Class codes starting with 'Z' represent unorganized territory
    which has no governmental function.
    """
    # Get all county subdivision records with 'Z' class codes
    query = session.query(CensusGeography).filter(
        CensusGeography.geo_layer == GeoLayer.COUNTY_SUBDIVISION.value,
        CensusGeography.class_fips.like("Z%"),
    )

    if states:
        query = query.filter(CensusGeography.state_fips.in_(states))

    count = query.delete(synchronize_session="fetch")
    if count > 0:
        logger.info(f"  Removed {count} unorganized territory MCDs")
    return count


def get_virginia_independent_cities(session: Session) -> List[CensusGeography]:
    """Get Virginia independent city records from census data.

    These are county-equivalents in the county layer that are actually
    independent cities (not part of any county).
    """
    return (
        session.query(CensusGeography)
        .filter(
            CensusGeography.state_fips == "51",
            CensusGeography.geo_layer == GeoLayer.COUNTY.value,
            CensusGeography.geoid.in_(VA_INDEPENDENT_CITY_FIPS),
        )
        .all()
    )


def normalize_name(name_lsad: str, geo_layer: str) -> str:
    """Strip LSAD suffix from a geographic name for display.

    Examples:
        'Autauga County' -> 'Autauga'
        'Los Angeles city' -> 'Los Angeles'
        'Springfield township' -> 'Springfield'

    Args:
        name_lsad: Full name with LSAD suffix.
        geo_layer: Geographic layer (county, place, county_subdivision).

    Returns:
        Clean display name.
    """
    suffixes = [
        " County", " Parish", " Borough", " Census Area", " Municipality",
        " city", " town", " village", " borough", " township",
        " CDP", " plantation", " gore", " grant", " location",
        " purchase", " unorganized territory",
    ]

    name = name_lsad
    for suffix in suffixes:
        if name.endswith(suffix):
            name = name[: -len(suffix)]
            break

    return name.strip()
