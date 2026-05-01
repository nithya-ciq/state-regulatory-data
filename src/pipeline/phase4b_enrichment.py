"""Phase 4b: Enrichment — overlay dry/wet status and licensing authority names.

Reads curated seed CSVs and updates existing jurisdiction rows with:
- Dry/wet/moist status from data/seed/dry_wet_status.csv
- Licensing authority names from patterns + overrides
- Per-GEOID regulatory overrides from data/seed/regulatory_overrides.csv
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy.orm import Session

from src.config import Config
from src.models.jurisdiction import Jurisdiction
from src.models.state_classification import StateClassification
from src.pipeline.phase1_state_classification import (
    _parse_bool_or_none,
    _parse_decimal_or_none,
)

logger = logging.getLogger("jurisdiction.phase4b")


def execute(
    session: Session,
    config: Config,
    states: Optional[List[str]] = None,
) -> int:
    """Execute Phase 4b: enrichment overlay.

    Args:
        session: SQLAlchemy database session.
        config: Application configuration.
        states: Optional list of state FIPS to process.

    Returns:
        Total number of jurisdiction rows enriched.
    """
    logger.info("Phase 4b: Enrichment overlay")

    total_enriched = 0

    # 1. Dry/wet status enrichment
    dry_wet_count = _enrich_dry_wet_status(session, config.seed_dir, states)
    total_enriched += dry_wet_count

    # 2. Licensing authority name enrichment
    authority_count = _enrich_licensing_authority_names(session, config.seed_dir, states)
    total_enriched += authority_count

    # 3. Regulatory details propagation
    regulatory_count = _enrich_regulatory_details(session, config, states)
    total_enriched += regulatory_count

    # 4. Per-GEOID regulatory overrides (after state defaults)
    override_count = _enrich_regulatory_overrides(session, config.seed_dir, states)
    total_enriched += override_count

    session.commit()
    logger.info(f"Phase 4b complete: {total_enriched} enrichment updates applied")
    return total_enriched


def _enrich_dry_wet_status(
    session: Session,
    seed_dir: Path,
    states: Optional[List[str]] = None,
) -> int:
    """Overlay dry/wet status from seed CSV onto jurisdiction rows.

    The CSV is sparse — only non-wet jurisdictions need entries.
    Any jurisdiction not in the CSV remains wet (the default).
    """
    csv_path = seed_dir / "dry_wet_status.csv"
    if not csv_path.exists():
        logger.info("  No dry_wet_status.csv found, skipping dry/wet enrichment")
        return 0

    df = pd.read_csv(csv_path, dtype={"geoid": str, "state_fips": str})
    if df.empty:
        logger.info("  dry_wet_status.csv is empty, skipping")
        return 0

    # Filter to requested states if specified
    if states:
        df = df[df["state_fips"].isin(states)]

    count = 0
    for _, row in df.iterrows():
        geoid = str(row["geoid"]).strip()
        status = str(row["dry_wet_status"]).strip().lower()
        is_dry = status == "dry"
        data_source = str(row.get("data_source", "")).strip() or None
        restriction_notes = str(row.get("restriction_notes", "")).strip() or None

        updated = (
            session.query(Jurisdiction)
            .filter(Jurisdiction.geoid == geoid)
            .update(
                {
                    Jurisdiction.is_dry: is_dry,
                    Jurisdiction.dry_wet_status: status,
                    Jurisdiction.dry_wet_data_source: data_source,
                },
                synchronize_session="fetch",
            )
        )
        count += updated

    if count > 0:
        logger.info(f"  Updated {count} jurisdiction rows with dry/wet status")
    return count


def _enrich_licensing_authority_names(
    session: Session,
    seed_dir: Path,
    states: Optional[List[str]] = None,
) -> int:
    """Overlay licensing authority names from patterns and overrides.

    Strategy (in priority order):
    1. Per-GEOID overrides (verified names for specific jurisdictions)
    2. State naming patterns (auto-generated from templates)
    3. State-agency fallback (for states where the state handles all licensing)
    """
    count = 0

    # 1. Apply per-GEOID overrides
    override_count = _apply_authority_overrides(session, seed_dir, states)
    count += override_count

    # 2. Apply state naming patterns
    pattern_count = _apply_authority_patterns(session, seed_dir, states)
    count += pattern_count

    if count > 0:
        logger.info(f"  Updated {count} jurisdiction rows with authority names")
    return count


def _apply_authority_overrides(
    session: Session,
    seed_dir: Path,
    states: Optional[List[str]] = None,
) -> int:
    """Apply per-GEOID authority name overrides."""
    csv_path = seed_dir / "licensing_authority_overrides.csv"
    if not csv_path.exists():
        logger.debug("  No licensing_authority_overrides.csv found")
        return 0

    df = pd.read_csv(csv_path, dtype={"geoid": str})
    if df.empty:
        return 0

    count = 0
    for _, row in df.iterrows():
        geoid = str(row["geoid"]).strip()
        authority_name = str(row["licensing_authority_name"]).strip()
        authority_type = str(row.get("licensing_authority_type", "")).strip() or None

        updated = (
            session.query(Jurisdiction)
            .filter(Jurisdiction.geoid == geoid)
            .update(
                {
                    Jurisdiction.licensing_authority_name: authority_name,
                    Jurisdiction.licensing_authority_type: authority_type,
                    Jurisdiction.licensing_authority_confidence: "verified",
                },
                synchronize_session="fetch",
            )
        )
        count += updated

    if count > 0:
        logger.info(f"    Applied {count} per-GEOID authority name overrides")
    return count


def _apply_authority_patterns(
    session: Session,
    seed_dir: Path,
    states: Optional[List[str]] = None,
) -> int:
    """Apply state-level naming patterns to generate authority names.

    Reads licensing_authority_names.csv which has patterns like:
        state_fips=37, jurisdiction_type=county, naming_pattern="{name} County ABC Board"

    For each matching jurisdiction row that doesn't already have an authority name,
    generates the name by substituting {name} with the jurisdiction_name.
    """
    csv_path = seed_dir / "licensing_authority_names.csv"
    if not csv_path.exists():
        logger.debug("  No licensing_authority_names.csv found")
        return 0

    df = pd.read_csv(csv_path, dtype={"state_fips": str})
    if df.empty:
        return 0

    # Filter to requested states
    if states:
        df = df[df["state_fips"].isin(states)]

    count = 0
    for _, pattern_row in df.iterrows():
        state_fips = str(pattern_row["state_fips"]).strip()
        jtype = str(pattern_row["jurisdiction_type"]).strip()
        naming_pattern = str(pattern_row["naming_pattern"]).strip()
        authority_type = str(pattern_row.get("authority_type", "")).strip() or None

        # Check if this is a static pattern (no {name} placeholder) — e.g., state agency
        is_static = "{name}" not in naming_pattern

        # Get jurisdiction rows for this state+type that don't already have a name
        jurisdictions = (
            session.query(Jurisdiction)
            .filter(
                Jurisdiction.state_fips == state_fips,
                Jurisdiction.jurisdiction_type == jtype,
                Jurisdiction.licensing_authority_name.is_(None),
                Jurisdiction.tier == "local",
            )
            .all()
        )

        for j in jurisdictions:
            if is_static:
                generated_name = naming_pattern
            else:
                generated_name = naming_pattern.replace("{name}", j.jurisdiction_name)

            j.licensing_authority_name = generated_name
            j.licensing_authority_type = authority_type
            j.licensing_authority_confidence = "generated"
            count += 1

    if count > 0:
        logger.info(f"    Generated {count} authority names from state patterns")
    return count


def _enrich_regulatory_details(
    session: Session,
    config: Config,
    states: Optional[List[str]] = None,
) -> int:
    """Propagate regulatory detail fields from state_classifications to jurisdictions.

    For states that have non-null three_tier_enforcement in their classification,
    update all jurisdiction rows for that state with the 5 propagated fields.
    This handles re-enrichment when seed data is updated after Phase 4 has run.
    """
    if not config.include_regulatory_details:
        logger.info("  Regulatory details enrichment disabled, skipping")
        return 0

    # Find state classifications with regulatory data
    query = session.query(StateClassification).filter(
        StateClassification.three_tier_enforcement.isnot(None)
    )
    if states:
        query = query.filter(StateClassification.state_fips.in_(states))

    classifications = query.all()
    if not classifications:
        logger.info("  No states with regulatory detail data found, skipping")
        return 0

    count = 0
    for classification in classifications:
        updated = (
            session.query(Jurisdiction)
            .filter(Jurisdiction.state_fips == classification.state_fips)
            .update(
                {
                    Jurisdiction.three_tier_enforcement: classification.three_tier_enforcement,
                    Jurisdiction.sunday_sales_allowed: classification.sunday_sales_allowed,
                    Jurisdiction.grocery_beer_allowed: classification.grocery_beer_allowed,
                    Jurisdiction.grocery_wine_allowed: classification.grocery_wine_allowed,
                    Jurisdiction.beer_max_abv: classification.beer_max_abv,
                },
                synchronize_session="fetch",
            )
        )
        count += updated

    if count > 0:
        logger.info(f"  Propagated regulatory details to {count} jurisdiction rows")
    return count


# Valid override field names → Jurisdiction model columns
_OVERRIDE_FIELD_MAP: Dict[str, str] = {
    "control_status": "control_status",
    "three_tier_enforcement": "three_tier_enforcement",
    "sunday_sales_allowed": "sunday_sales_allowed",
    "grocery_beer_allowed": "grocery_beer_allowed",
    "grocery_wine_allowed": "grocery_wine_allowed",
    "beer_max_abv": "beer_max_abv",
}

# Fields that are boolean
_BOOL_FIELDS = {"sunday_sales_allowed", "grocery_beer_allowed", "grocery_wine_allowed"}

# Fields that are numeric
_NUMERIC_FIELDS = {"beer_max_abv"}


def _enrich_regulatory_overrides(
    session: Session,
    seed_dir: Path,
    states: Optional[List[str]] = None,
) -> int:
    """Overlay per-GEOID regulatory overrides from seed CSV.

    The CSV is sparse — only jurisdictions that differ from their state
    defaults need entries. One row per field per GEOID (long format).
    """
    csv_path = seed_dir / "regulatory_overrides.csv"
    if not csv_path.exists():
        logger.info("  No regulatory_overrides.csv found, skipping regulatory overrides")
        return 0

    df = pd.read_csv(csv_path, dtype={"geoid": str, "state_fips": str})
    if df.empty:
        logger.info("  regulatory_overrides.csv is empty, skipping")
        return 0

    # Filter to requested states if specified
    if states:
        df = df[df["state_fips"].isin(states)]

    if df.empty:
        return 0

    # Group by GEOID and build per-jurisdiction update dicts
    count = 0
    for geoid, group in df.groupby("geoid"):
        geoid = str(geoid).strip()
        update_dict: Dict = {}

        for _, row in group.iterrows():
            field = str(row["override_field"]).strip()
            value = str(row["override_value"]).strip()

            if field not in _OVERRIDE_FIELD_MAP:
                logger.warning(
                    f"  Unknown override_field '{field}' for GEOID {geoid}, skipping"
                )
                continue

            col_name = _OVERRIDE_FIELD_MAP[field]

            if field in _BOOL_FIELDS:
                parsed = _parse_bool_or_none(value)
                update_dict[getattr(Jurisdiction, col_name)] = parsed
            elif field in _NUMERIC_FIELDS:
                parsed = _parse_decimal_or_none(value)
                update_dict[getattr(Jurisdiction, col_name)] = parsed
            else:
                # String fields (control_status, three_tier_enforcement)
                update_dict[getattr(Jurisdiction, col_name)] = value

        if not update_dict:
            continue

        # Mark the override source
        update_dict[Jurisdiction.regulatory_override_source] = "regulatory_overrides_csv"

        updated = (
            session.query(Jurisdiction)
            .filter(Jurisdiction.geoid == geoid)
            .update(update_dict, synchronize_session="fetch")
        )
        count += updated

    if count > 0:
        logger.info(f"  Applied regulatory overrides to {count} jurisdiction rows")
    return count
