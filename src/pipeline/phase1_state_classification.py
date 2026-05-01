"""Phase 1: Load and validate the state classification matrix.

Reads the seed CSV file, validates completeness and consistency,
and upserts into the state_classifications database table.
"""

import logging
from decimal import InvalidOperation
from pathlib import Path
from typing import List, Optional, Union

import pandas as pd
from sqlalchemy.orm import Session

from src.common.constants import EXPECTED_STATE_COUNT, FIPS_STATES, STRONG_MCD_STATES
from src.common.exceptions import ClassificationError
from src.db.repository import Repository
from src.models.state_classification import StateClassification

logger = logging.getLogger("jurisdiction.phase1")


def execute(session: Session, seed_path: Path) -> int:
    """Execute Phase 1: load and validate the state classification matrix.

    Args:
        session: SQLAlchemy database session.
        seed_path: Path to the state_classification_matrix.csv seed file.

    Returns:
        Number of state classifications loaded.

    Raises:
        ClassificationError: If the seed file is missing or invalid.
    """
    logger.info("Phase 1: Loading state classification matrix")

    if not seed_path.exists():
        raise ClassificationError(f"Seed file not found: {seed_path}")

    df = pd.read_csv(seed_path, dtype=str)
    df = df.fillna("")

    # Validate the seed data
    errors = _validate_seed(df)
    if errors:
        for error in errors:
            logger.error(f"Validation error: {error}")
        raise ClassificationError(
            f"Seed file has {len(errors)} validation error(s). See log for details."
        )

    # Convert to dicts for upsert
    records = _dataframe_to_records(df)

    # Upsert into database
    repo = Repository(session)
    count = repo.bulk_upsert(
        model=StateClassification,
        records=records,
        conflict_columns=["state_fips"],
    )

    # Count verified vs pending
    verified = df[df["research_status"].isin(["verified", "draft"])].shape[0]
    pending = df[df["research_status"] == "pending"].shape[0]

    logger.info(
        f"Phase 1 complete: {count} classifications loaded "
        f"({verified} verified/draft, {pending} pending)"
    )

    return count


def _validate_seed(df: pd.DataFrame) -> List[str]:
    """Validate the seed DataFrame for completeness and consistency.

    Returns:
        List of error messages. Empty list means valid.
    """
    errors: List[str] = []

    # Check required columns
    required_columns = [
        "state_fips", "state_abbr", "state_name", "control_status",
        "has_local_licensing", "delegates_to_county", "delegates_to_municipality",
        "delegates_to_mcd", "is_strong_mcd_state", "research_status",
    ]
    missing = set(required_columns) - set(df.columns)
    if missing:
        errors.append(f"Missing required columns: {missing}")
        return errors  # Can't proceed without required columns

    # Check row count
    if len(df) < EXPECTED_STATE_COUNT:
        errors.append(
            f"Expected {EXPECTED_STATE_COUNT} rows, got {len(df)}. "
            f"All 50 states + DC + 5 territories must be present."
        )

    # Check all known FIPS codes are present
    seed_fips = set(df["state_fips"])
    expected_fips = set(FIPS_STATES.keys())
    missing_fips = expected_fips - seed_fips
    if missing_fips:
        errors.append(f"Missing state FIPS codes: {sorted(missing_fips)}")

    extra_fips = seed_fips - expected_fips
    if extra_fips:
        errors.append(f"Unknown state FIPS codes: {sorted(extra_fips)}")

    # Check control_status values
    valid_status = {"control", "license", "hybrid"}
    for _, row in df.iterrows():
        status = row.get("control_status", "").strip().lower()
        if status and status not in valid_status:
            errors.append(
                f"Invalid control_status '{status}' for {row['state_fips']} ({row['state_name']})"
            )

    # Check consistency: delegates_to_mcd should only be true for strong-MCD states
    for _, row in df.iterrows():
        if row.get("delegates_to_mcd", "").lower() == "true":
            if row["state_fips"] not in STRONG_MCD_STATES:
                errors.append(
                    f"{row['state_name']} ({row['state_fips']}): delegates_to_mcd=true "
                    f"but not a strong-MCD state"
                )

    return errors


def _parse_bool_or_none(value: Union[str, None]) -> Optional[bool]:
    """Parse a string value to bool or None.

    Returns None for empty/missing values, True/False for boolean strings.
    """
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
    """Parse a string value to float or None.

    Returns None for empty/missing/invalid values, float for numeric strings.
    """
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
    """Convert seed DataFrame rows to dicts suitable for database upsert."""
    records = []
    for _, row in df.iterrows():
        records.append(
            {
                "state_fips": row["state_fips"],
                "state_abbr": row["state_abbr"],
                "state_name": row["state_name"],
                "is_territory": row.get("is_territory", "").lower() == "true",
                "control_status": row["control_status"].lower(),
                "has_local_licensing": row.get("has_local_licensing", "").lower() == "true",
                "delegates_to_county": row.get("delegates_to_county", "").lower() == "true",
                "delegates_to_municipality": row.get("delegates_to_municipality", "").lower() == "true",
                "delegates_to_mcd": row.get("delegates_to_mcd", "").lower() == "true",
                "is_strong_mcd_state": row.get("is_strong_mcd_state", "").lower() == "true",
                "has_local_option_law": row.get("has_local_option_law", "").lower() == "true",
                "local_option_level": row.get("local_option_level") or None,
                "abc_agency_name": row.get("abc_agency_name") or None,
                "abc_agency_url": row.get("abc_agency_url") or None,
                "research_status": row.get("research_status", "pending"),
                "research_source": row.get("research_source") or None,
                "research_notes": row.get("research_notes") or None,
                # Regulatory detail columns (all optional)
                "three_tier_enforcement": row.get("three_tier_enforcement") or None,
                "three_tier_notes": row.get("three_tier_notes") or None,
                "has_on_premise_license": _parse_bool_or_none(
                    row.get("has_on_premise_license")
                ),
                "has_off_premise_license": _parse_bool_or_none(
                    row.get("has_off_premise_license")
                ),
                "has_manufacturer_license": _parse_bool_or_none(
                    row.get("has_manufacturer_license")
                ),
                "has_distributor_license": _parse_bool_or_none(
                    row.get("has_distributor_license")
                ),
                "sunday_sales_allowed": _parse_bool_or_none(
                    row.get("sunday_sales_allowed")
                ),
                "sunday_sales_hours": row.get("sunday_sales_hours") or None,
                "sunday_sales_notes": row.get("sunday_sales_notes") or None,
                "grocery_beer_allowed": _parse_bool_or_none(
                    row.get("grocery_beer_allowed")
                ),
                "grocery_wine_allowed": _parse_bool_or_none(
                    row.get("grocery_wine_allowed")
                ),
                "convenience_beer_allowed": _parse_bool_or_none(
                    row.get("convenience_beer_allowed")
                ),
                "convenience_wine_allowed": _parse_bool_or_none(
                    row.get("convenience_wine_allowed")
                ),
                "grocery_store_notes": row.get("grocery_store_notes") or None,
                "beer_max_abv": _parse_decimal_or_none(row.get("beer_max_abv")),
                "beer_abv_notes": row.get("beer_abv_notes") or None,
                # License enrichment columns (Phase 2)
                "grocery_liquor_allowed": _parse_bool_or_none(
                    row.get("grocery_liquor_allowed")
                ),
                "convenience_liquor_allowed": _parse_bool_or_none(
                    row.get("convenience_liquor_allowed")
                ),
                "grocery_beer_confidence": row.get("grocery_beer_confidence") or None,
                "grocery_wine_confidence": row.get("grocery_wine_confidence") or None,
                "grocery_liquor_confidence": row.get("grocery_liquor_confidence") or None,
                "retail_channel_notes": row.get("retail_channel_notes") or None,
                "license_type_count": int(row["license_type_count"])
                if row.get("license_type_count", "").strip()
                else None,
                "license_complexity_tier": row.get("license_complexity_tier") or None,
                # Granular control flags
                "spirits_control": _parse_bool_or_none(row.get("spirits_control")),
                "wine_control": _parse_bool_or_none(row.get("wine_control")),
                "beer_control": _parse_bool_or_none(row.get("beer_control")),
                "wholesale_control": _parse_bool_or_none(row.get("wholesale_control")),
                "retail_control": _parse_bool_or_none(row.get("retail_control")),
                "data_effective_date": row.get("data_effective_date") or None,
            }
        )
    return records
