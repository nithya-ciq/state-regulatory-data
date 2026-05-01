"""Phase 4: Jurisdiction assembly — the core pipeline logic.

Combines state classifications with processed census geographies
to produce the final jurisdiction taxonomy rows.
"""

import logging
from typing import Dict, List, Optional

from sqlalchemy.orm import Session

from src.common.constants import FIPS_STATES, VA_INDEPENDENT_CITY_FIPS
from src.common.enums import DelegationPattern, GeoLayer, JurisdictionType, Tier
from src.db.repository import Repository
from src.models.census_geography import CensusGeography
from src.models.jurisdiction import Jurisdiction
from src.models.state_classification import StateClassification
from src.pipeline.phase3_data_processing import normalize_name

logger = logging.getLogger("jurisdiction.phase4")


def execute(
    session: Session,
    census_year: int,
    states: Optional[List[str]] = None,
) -> int:
    """Execute Phase 4: assemble jurisdiction rows.

    Args:
        session: SQLAlchemy database session.
        census_year: Census TIGER vintage year.
        states: Optional list of state FIPS to process.

    Returns:
        Total number of jurisdiction rows created.
    """
    logger.info("Phase 4: Jurisdiction assembly")

    repo = Repository(session)
    total = 0

    # 1. Create the single federal row (TTB)
    federal_records = _build_federal_row(census_year)
    repo.bulk_upsert(
        model=Jurisdiction,
        records=federal_records,
        conflict_columns=["geoid", "jurisdiction_type", "census_year"],
    )
    total += len(federal_records)

    # 2. Get classifications
    classifications = _get_classifications(session, states)

    # 3. Process each state
    for state_fips, classification in classifications.items():
        try:
            count = _assemble_state(session, repo, classification, census_year)
            total += count
            session.commit()
            logger.info(f"  {classification.state_name}: {count} jurisdiction rows")
        except Exception as e:
            logger.error(f"Failed to assemble state {state_fips}: {e}")
            session.rollback()
            raise

    logger.info(f"Phase 4 complete: {total} total jurisdiction rows")
    return total


def _build_federal_row(census_year: int) -> List[dict]:
    """Build the single federal-tier row for TTB."""
    return [
        {
            "geoid": "US",
            "jurisdiction_type": JurisdictionType.FEDERAL.value,
            "tier": Tier.FEDERAL.value,
            "state_fips": "00",
            "county_fips": None,
            "place_fips": None,
            "cousub_fips": None,
            "jurisdiction_name": "Tobacco Tax and Trade Bureau",
            "jurisdiction_name_lsad": "TTB",
            "state_abbr": "US",
            "state_name": "United States",
            "county_name": None,
            "has_licensing_authority": True,
            "licensing_authority_name": "TTB",
            "licensing_authority_type": "state_agency",
            "licensing_authority_confidence": "verified",
            "is_dry": False,
            "dry_wet_status": "wet",
            "dry_wet_data_source": None,
            "control_status": "federal",
            "delegation_pattern": None,
            "land_area_sqm": None,
            "latitude": None,
            "longitude": None,
            "is_independent_city": False,
            "census_year": census_year,
            "data_source": "manual",
            "three_tier_enforcement": None,
            "sunday_sales_allowed": None,
            "grocery_beer_allowed": None,
            "grocery_wine_allowed": None,
            "beer_max_abv": None,
        }
    ]


def _get_classifications(
    session: Session, states: Optional[List[str]] = None
) -> Dict[str, StateClassification]:
    """Load state classifications."""
    query = session.query(StateClassification).filter(
        StateClassification.research_status.in_(["draft", "verified"])
    )
    if states:
        query = query.filter(StateClassification.state_fips.in_(states))
    return {c.state_fips: c for c in query.all()}


def _assemble_state(
    session: Session,
    repo: Repository,
    classification: StateClassification,
    census_year: int,
) -> int:
    """Assemble jurisdiction rows for a single state."""
    state_fips = classification.state_fips
    abbr = classification.state_abbr
    name = classification.state_name

    records: List[dict] = []

    # State-level row
    jtype = (
        JurisdictionType.TERRITORY.value
        if classification.is_territory
        else JurisdictionType.STATE.value
    )
    records.append(
        {
            "geoid": state_fips,
            "jurisdiction_type": jtype,
            "tier": Tier.STATE.value,
            "state_fips": state_fips,
            "county_fips": None,
            "place_fips": None,
            "cousub_fips": None,
            "jurisdiction_name": name,
            "jurisdiction_name_lsad": f"State of {name}" if not classification.is_territory else name,
            "state_abbr": abbr,
            "state_name": name,
            "county_name": None,
            "has_licensing_authority": True,
            "licensing_authority_name": classification.abc_agency_name,
            "licensing_authority_type": "state_agency",
            "licensing_authority_confidence": "verified",
            "is_dry": False,
            "dry_wet_status": "wet",
            "dry_wet_data_source": None,
            "control_status": classification.control_status,
            "delegation_pattern": _derive_delegation_pattern(classification),
            "land_area_sqm": None,
            "latitude": None,
            "longitude": None,
            "is_independent_city": False,
            "census_year": census_year,
            "data_source": "manual",
            "three_tier_enforcement": classification.three_tier_enforcement,
            "sunday_sales_allowed": classification.sunday_sales_allowed,
            "grocery_beer_allowed": classification.grocery_beer_allowed,
            "grocery_wine_allowed": classification.grocery_wine_allowed,
            "beer_max_abv": classification.beer_max_abv,
        }
    )

    # Local-level rows based on delegation pattern
    if not classification.has_local_licensing:
        # State-only licensing: no local rows needed
        pass
    else:
        # Counties
        if classification.delegates_to_county:
            county_records = _build_county_rows(
                session, classification, census_year
            )
            records.extend(county_records)

        # Municipalities (incorporated places)
        if classification.delegates_to_municipality:
            place_records = _build_place_rows(
                session, classification, census_year
            )
            records.extend(place_records)

        # MCDs (townships) — only for strong-MCD states
        if classification.delegates_to_mcd and classification.is_strong_mcd_state:
            mcd_records = _build_mcd_rows(
                session, classification, census_year
            )
            records.extend(mcd_records)

    # Upsert all records for this state
    if records:
        repo.bulk_upsert(
            model=Jurisdiction,
            records=records,
            conflict_columns=["geoid", "jurisdiction_type", "census_year"],
        )

    return len(records)


def _build_county_rows(
    session: Session,
    classification: StateClassification,
    census_year: int,
) -> List[dict]:
    """Build jurisdiction rows from county-level census data."""
    counties = (
        session.query(CensusGeography)
        .filter(
            CensusGeography.state_fips == classification.state_fips,
            CensusGeography.geo_layer == GeoLayer.COUNTY.value,
            CensusGeography.census_year == census_year,
        )
        .all()
    )

    records = []
    for county in counties:
        # Check for Virginia independent cities
        is_independent = county.geoid in VA_INDEPENDENT_CITY_FIPS
        jtype = (
            JurisdictionType.INDEPENDENT_CITY.value
            if is_independent
            else JurisdictionType.COUNTY.value
        )

        display_name = normalize_name(county.name_lsad or county.name, GeoLayer.COUNTY.value)

        records.append(
            {
                "geoid": county.geoid,
                "jurisdiction_type": jtype,
                "tier": Tier.LOCAL.value,
                "state_fips": classification.state_fips,
                "county_fips": county.county_fips,
                "place_fips": None,
                "cousub_fips": None,
                "jurisdiction_name": display_name,
                "jurisdiction_name_lsad": county.name_lsad or county.name,
                "state_abbr": classification.state_abbr,
                "state_name": classification.state_name,
                "county_name": None,
                "has_licensing_authority": True,
                "licensing_authority_name": None,
                "licensing_authority_type": None,
                "licensing_authority_confidence": None,
                "is_dry": False,
                "dry_wet_status": "wet",
                "dry_wet_data_source": None,
                "control_status": classification.control_status,
                "delegation_pattern": _derive_delegation_pattern(classification),
                "land_area_sqm": county.land_area_sqm,
                "latitude": county.latitude,
                "longitude": county.longitude,
                "is_independent_city": is_independent,
                "census_year": census_year,
                "data_source": "tiger_counties",
                "three_tier_enforcement": classification.three_tier_enforcement,
                "sunday_sales_allowed": classification.sunday_sales_allowed,
                "grocery_beer_allowed": classification.grocery_beer_allowed,
                "grocery_wine_allowed": classification.grocery_wine_allowed,
                "beer_max_abv": classification.beer_max_abv,
            }
        )

    return records


def _build_place_rows(
    session: Session,
    classification: StateClassification,
    census_year: int,
) -> List[dict]:
    """Build jurisdiction rows from incorporated place census data."""
    # Build a lookup of county names for parent reference
    county_lookup = _build_county_name_lookup(
        session, classification.state_fips, census_year
    )

    places = (
        session.query(CensusGeography)
        .filter(
            CensusGeography.state_fips == classification.state_fips,
            CensusGeography.geo_layer == GeoLayer.PLACE.value,
            CensusGeography.census_year == census_year,
        )
        .all()
    )

    records = []
    for place in places:
        display_name = normalize_name(place.name_lsad or place.name, GeoLayer.PLACE.value)

        records.append(
            {
                "geoid": place.geoid,
                "jurisdiction_type": JurisdictionType.MUNICIPALITY.value,
                "tier": Tier.LOCAL.value,
                "state_fips": classification.state_fips,
                "county_fips": None,
                "place_fips": place.place_fips,
                "cousub_fips": None,
                "jurisdiction_name": display_name,
                "jurisdiction_name_lsad": place.name_lsad or place.name,
                "state_abbr": classification.state_abbr,
                "state_name": classification.state_name,
                "county_name": None,
                "has_licensing_authority": True,
                "licensing_authority_name": None,
                "licensing_authority_type": None,
                "licensing_authority_confidence": None,
                "is_dry": False,
                "dry_wet_status": "wet",
                "dry_wet_data_source": None,
                "control_status": classification.control_status,
                "delegation_pattern": _derive_delegation_pattern(classification),
                "land_area_sqm": place.land_area_sqm,
                "latitude": place.latitude,
                "longitude": place.longitude,
                "is_independent_city": False,
                "census_year": census_year,
                "data_source": "tiger_places",
                "three_tier_enforcement": classification.three_tier_enforcement,
                "sunday_sales_allowed": classification.sunday_sales_allowed,
                "grocery_beer_allowed": classification.grocery_beer_allowed,
                "grocery_wine_allowed": classification.grocery_wine_allowed,
                "beer_max_abv": classification.beer_max_abv,
            }
        )

    return records


def _build_mcd_rows(
    session: Session,
    classification: StateClassification,
    census_year: int,
) -> List[dict]:
    """Build jurisdiction rows from MCD/township census data."""
    county_lookup = _build_county_name_lookup(
        session, classification.state_fips, census_year
    )

    mcds = (
        session.query(CensusGeography)
        .filter(
            CensusGeography.state_fips == classification.state_fips,
            CensusGeography.geo_layer == GeoLayer.COUNTY_SUBDIVISION.value,
            CensusGeography.census_year == census_year,
        )
        .all()
    )

    records = []
    for mcd in mcds:
        display_name = normalize_name(
            mcd.name_lsad or mcd.name, GeoLayer.COUNTY_SUBDIVISION.value
        )

        # Look up parent county name
        county_name = county_lookup.get(mcd.county_fips)

        records.append(
            {
                "geoid": mcd.geoid,
                "jurisdiction_type": JurisdictionType.MCD.value,
                "tier": Tier.LOCAL.value,
                "state_fips": classification.state_fips,
                "county_fips": mcd.county_fips,
                "place_fips": None,
                "cousub_fips": mcd.cousub_fips,
                "jurisdiction_name": display_name,
                "jurisdiction_name_lsad": mcd.name_lsad or mcd.name,
                "state_abbr": classification.state_abbr,
                "state_name": classification.state_name,
                "county_name": county_name,
                "has_licensing_authority": True,
                "licensing_authority_name": None,
                "licensing_authority_type": None,
                "licensing_authority_confidence": None,
                "is_dry": False,
                "dry_wet_status": "wet",
                "dry_wet_data_source": None,
                "control_status": classification.control_status,
                "delegation_pattern": _derive_delegation_pattern(classification),
                "land_area_sqm": mcd.land_area_sqm,
                "latitude": mcd.latitude,
                "longitude": mcd.longitude,
                "is_independent_city": False,
                "census_year": census_year,
                "data_source": "tiger_mcds",
                "three_tier_enforcement": classification.three_tier_enforcement,
                "sunday_sales_allowed": classification.sunday_sales_allowed,
                "grocery_beer_allowed": classification.grocery_beer_allowed,
                "grocery_wine_allowed": classification.grocery_wine_allowed,
                "beer_max_abv": classification.beer_max_abv,
            }
        )

    return records


def _build_county_name_lookup(
    session: Session, state_fips: str, census_year: int
) -> Dict[str, str]:
    """Build a dict mapping county_fips -> county name for a state."""
    counties = (
        session.query(CensusGeography)
        .filter(
            CensusGeography.state_fips == state_fips,
            CensusGeography.geo_layer == GeoLayer.COUNTY.value,
            CensusGeography.census_year == census_year,
        )
        .all()
    )
    return {c.county_fips: c.name_lsad or c.name for c in counties if c.county_fips}


def _derive_delegation_pattern(classification: StateClassification) -> str:
    """Derive the delegation pattern string from classification flags."""
    if not classification.has_local_licensing:
        return DelegationPattern.STATE_ONLY.value

    has_county = classification.delegates_to_county
    has_muni = classification.delegates_to_municipality
    has_mcd = classification.delegates_to_mcd

    if has_county and has_muni and has_mcd:
        return DelegationPattern.ALL_LEVELS.value
    if has_county and has_mcd:
        return DelegationPattern.COUNTY_AND_MCD.value
    if has_county and has_muni:
        return DelegationPattern.COUNTY_AND_MUNICIPALITY.value
    if has_muni and has_mcd:
        return DelegationPattern.ALL_LEVELS.value  # treat as all levels
    if has_county:
        return DelegationPattern.COUNTY.value
    if has_muni:
        return DelegationPattern.MUNICIPALITY.value
    if has_mcd:
        return DelegationPattern.MCD.value

    return DelegationPattern.STATE_ONLY.value
