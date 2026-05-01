"""Phase 1b: Load and validate the license types catalog.

Reads the license_types.csv seed file, validates against known state FIPS,
and upserts into the license_types database table. Also computes
license_type_count and license_complexity_tier on state_classifications.
"""

import logging
from decimal import InvalidOperation
from pathlib import Path
from typing import List, Optional, Union

import pandas as pd
from sqlalchemy.orm import Session

from src.common.constants import FIPS_STATES
from src.common.enums import LicenseCategory, RetailChannel
from src.db.repository import Repository
from src.models.license_type import LicenseType
from src.models.state_classification import StateClassification

logger = logging.getLogger("jurisdiction.phase1b")

VALID_CATEGORIES = {e.value for e in LicenseCategory}
VALID_CHANNELS = {e.value for e in RetailChannel}


def execute(session: Session, seed_path: Path) -> int:
    """Execute Phase 1b: load and validate license types catalog.

    Args:
        session: SQLAlchemy database session.
        seed_path: Path to the license_types.csv seed file.

    Returns:
        Number of license types loaded.
    """
    logger.info("Phase 1b: Loading license types catalog")

    if not seed_path.exists():
        logger.warning(f"License types seed file not found: {seed_path}. Skipping Phase 1b.")
        return 0

    df = pd.read_csv(seed_path, dtype=str)
    df = df.fillna("")

    if len(df) == 0:
        logger.warning("License types CSV is empty. Skipping Phase 1b.")
        return 0

    # Validate
    errors = _validate_seed(df, session)
    if errors:
        for error in errors:
            logger.error(f"Validation error: {error}")
        raise ValueError(
            f"License types seed has {len(errors)} validation error(s). See log."
        )

    # Convert to records
    records = _dataframe_to_records(df)

    # Upsert into database
    repo = Repository(session)
    count = repo.bulk_upsert(
        model=LicenseType,
        records=records,
        conflict_columns=["state_fips", "license_type_code"],
    )

    # Update license_type_count and complexity_tier on state_classifications
    _update_state_counts(session, df)

    logger.info(f"Phase 1b complete: {count} license types loaded")
    return count


def _validate_seed(df: pd.DataFrame, session: Session) -> List[str]:
    """Validate the license types DataFrame."""
    errors: List[str] = []

    # Check required columns
    required = [
        "state_fips", "license_type_code", "license_type_name",
        "license_category", "permits_on_premise", "permits_off_premise",
        "permits_beer", "permits_wine", "permits_spirits",
    ]
    missing = set(required) - set(df.columns)
    if missing:
        errors.append(f"Missing required columns: {missing}")
        return errors

    # Check state FIPS validity
    known_fips = set(FIPS_STATES.keys())
    for _, row in df.iterrows():
        fips = row["state_fips"].strip()
        if fips not in known_fips:
            errors.append(f"Unknown state_fips '{fips}' for {row['license_type_code']}")

    # Check natural key uniqueness
    keys = df.groupby(["state_fips", "license_type_code"]).size()
    dupes = keys[keys > 1]
    if not dupes.empty:
        for (fips, code), cnt in dupes.items():
            errors.append(f"Duplicate key: ({fips}, {code}) appears {cnt} times")

    # Validate license_category values
    for _, row in df.iterrows():
        cat = row.get("license_category", "").strip().lower()
        if cat and cat not in VALID_CATEGORIES:
            errors.append(
                f"Invalid license_category '{cat}' for "
                f"{row['state_fips']}/{row['license_type_code']}"
            )

    # Validate retail_channel values
    for _, row in df.iterrows():
        channel = row.get("retail_channel", "").strip().lower()
        if channel and channel not in VALID_CHANNELS:
            errors.append(
                f"Invalid retail_channel '{channel}' for "
                f"{row['state_fips']}/{row['license_type_code']}"
            )

    return errors


def _parse_bool(value: Union[str, None]) -> bool:
    """Parse a string to bool, defaulting to False."""
    if value is None:
        return False
    val = str(value).strip().lower()
    return val in ("true", "1", "yes")


def _parse_bool_or_none(value: Union[str, None]) -> Optional[bool]:
    """Parse a string to bool or None for optional fields."""
    if value is None:
        return None
    val = str(value).strip().lower()
    if val in ("", "nan", "none"):
        return None
    if val in ("true", "1", "yes"):
        return True
    if val in ("false", "0", "no"):
        return False
    return None


def _parse_decimal_or_none(value: Union[str, None]) -> Optional[float]:
    """Parse a string to float or None."""
    if value is None:
        return None
    val = str(value).strip()
    if val in ("", "nan", "none"):
        return None
    try:
        return float(val)
    except (ValueError, InvalidOperation):
        return None


def _dataframe_to_records(df: pd.DataFrame) -> List[dict]:
    """Convert seed DataFrame rows to dicts for database upsert."""
    records = []
    for _, row in df.iterrows():
        records.append(
            {
                "state_fips": row["state_fips"].strip(),
                "license_type_code": row["license_type_code"].strip(),
                "license_type_name": row["license_type_name"].strip(),
                "license_category": row["license_category"].strip().lower(),
                "permits_on_premise": _parse_bool(row.get("permits_on_premise")),
                "permits_off_premise": _parse_bool(row.get("permits_off_premise")),
                "permits_beer": _parse_bool(row.get("permits_beer")),
                "permits_wine": _parse_bool(row.get("permits_wine")),
                "permits_spirits": _parse_bool(row.get("permits_spirits")),
                "retail_channel": row.get("retail_channel", "").strip().lower() or None,
                "abv_limit": _parse_decimal_or_none(row.get("abv_limit")),
                "quota_limited": _parse_bool_or_none(row.get("quota_limited")),
                "quota_notes": row.get("quota_notes", "").strip() or None,
                "transferable": _parse_bool_or_none(row.get("transferable")),
                "annual_fee_range": row.get("annual_fee_range", "").strip() or None,
                "issuing_authority": row.get("issuing_authority", "").strip() or None,
                "statutory_reference": row.get("statutory_reference", "").strip() or None,
                "notes": row.get("notes", "").strip() or None,
                "research_status": row.get("research_status", "pending").strip() or "pending",
                "research_source": row.get("research_source", "").strip() or None,
            }
        )
    return records


def _update_state_counts(session: Session, df: pd.DataFrame) -> None:
    """Update license_type_count and license_complexity_tier on state_classifications."""
    counts = df.groupby("state_fips").size().to_dict()

    for fips, count in counts.items():
        if count <= 5:
            tier = "simple"
        elif count <= 15:
            tier = "moderate"
        else:
            tier = "complex"

        session.query(StateClassification).filter(
            StateClassification.state_fips == fips
        ).update(
            {
                "license_type_count": count,
                "license_complexity_tier": tier,
            }
        )

    logger.info(
        f"Updated license_type_count for {len(counts)} states "
        f"(simple: {sum(1 for c in counts.values() if c <= 5)}, "
        f"moderate: {sum(1 for c in counts.values() if 5 < c <= 15)}, "
        f"complex: {sum(1 for c in counts.values() if c > 15)})"
    )
