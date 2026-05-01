"""Phase 5: Validation and export.

Validates the assembled jurisdiction data for completeness and consistency,
then exports to CSV, JSON, and Parquet formats.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd
from sqlalchemy.orm import Session

from src.common.constants import EXPECTED_STATE_COUNT, FIPS_STATES
from src.common.enums import JurisdictionType, Tier
from src.config import Config
from src.models.jurisdiction import Jurisdiction

logger = logging.getLogger("jurisdiction.phase5")


def execute(session: Session, config: Config) -> Dict:
    """Execute Phase 5: validation and export.

    Args:
        session: SQLAlchemy database session.
        config: Application configuration.

    Returns:
        Dict with validation results and export file paths.
    """
    logger.info("Phase 5: Validation and export")

    # 1. Validate
    validation_results = _validate(session, config.census_year)

    # 2. Export
    config.ensure_directories()
    export_paths = _export(session, config)

    # 3. Generate summary report
    summary = _generate_summary(session, config.census_year, validation_results)

    results = {
        "validation": validation_results,
        "exports": export_paths,
        "summary": summary,
    }

    logger.info(f"Phase 5 complete: {len(export_paths)} files exported")
    return results


def _validate(session: Session, census_year: int) -> Dict:
    """Run validation checks on the assembled jurisdiction data."""
    issues: List[str] = []
    warnings: List[str] = []

    # Total count
    total = session.query(Jurisdiction).filter(
        Jurisdiction.census_year == census_year
    ).count()

    if total == 0:
        issues.append("No jurisdiction rows found. Pipeline may not have run.")
        return {"valid": False, "issues": issues, "warnings": warnings, "total": 0}

    # Check for federal row
    federal_count = session.query(Jurisdiction).filter(
        Jurisdiction.tier == Tier.FEDERAL.value,
        Jurisdiction.census_year == census_year,
    ).count()
    if federal_count == 0:
        issues.append("Missing federal (TTB) row")

    # Check state coverage
    state_count = session.query(Jurisdiction).filter(
        Jurisdiction.tier == Tier.STATE.value,
        Jurisdiction.census_year == census_year,
    ).count()
    if state_count < EXPECTED_STATE_COUNT:
        warnings.append(
            f"Only {state_count}/{EXPECTED_STATE_COUNT} states present. "
            f"Some states may have pending research status."
        )

    # Check for duplicate GEOIDs within same type
    from sqlalchemy import func

    dupes = (
        session.query(
            Jurisdiction.geoid,
            Jurisdiction.jurisdiction_type,
            func.count().label("cnt"),
        )
        .filter(Jurisdiction.census_year == census_year)
        .group_by(Jurisdiction.geoid, Jurisdiction.jurisdiction_type)
        .having(func.count() > 1)
        .all()
    )
    if dupes:
        issues.append(f"Found {len(dupes)} duplicate GEOID/type combinations")

    # Check for empty names
    empty_names = session.query(Jurisdiction).filter(
        Jurisdiction.census_year == census_year,
        (Jurisdiction.jurisdiction_name.is_(None)) | (Jurisdiction.jurisdiction_name == ""),
    ).count()
    if empty_names > 0:
        issues.append(f"{empty_names} jurisdiction(s) have empty names")

    valid = len(issues) == 0
    logger.info(
        f"Validation: {'PASSED' if valid else 'FAILED'} "
        f"({total} rows, {len(issues)} issues, {len(warnings)} warnings)"
    )

    return {
        "valid": valid,
        "total": total,
        "issues": issues,
        "warnings": warnings,
    }


def _export(session: Session, config: Config) -> Dict[str, str]:
    """Export jurisdiction data to CSV, JSON, and Parquet."""
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    year = config.census_year
    output_dir = config.output_dir

    # Query all jurisdiction rows
    jurisdictions = (
        session.query(Jurisdiction)
        .filter(Jurisdiction.census_year == year)
        .order_by(Jurisdiction.state_fips, Jurisdiction.tier, Jurisdiction.geoid)
        .all()
    )

    if not jurisdictions:
        logger.warning("No jurisdictions to export")
        return {}

    # Convert to DataFrame
    rows = []
    for j in jurisdictions:
        rows.append(
            {
                "geoid": j.geoid,
                "jurisdiction_type": j.jurisdiction_type,
                "tier": j.tier,
                "state_fips": j.state_fips,
                "county_fips": j.county_fips,
                "place_fips": j.place_fips,
                "cousub_fips": j.cousub_fips,
                "jurisdiction_name": j.jurisdiction_name,
                "jurisdiction_name_lsad": j.jurisdiction_name_lsad,
                "state_abbr": j.state_abbr,
                "state_name": j.state_name,
                "county_name": j.county_name,
                "has_licensing_authority": j.has_licensing_authority,
                "licensing_authority_name": j.licensing_authority_name,
                "licensing_authority_type": j.licensing_authority_type,
                "licensing_authority_confidence": j.licensing_authority_confidence,
                "is_dry": j.is_dry,
                "dry_wet_status": j.dry_wet_status,
                "dry_wet_data_source": j.dry_wet_data_source,
                "control_status": j.control_status,
                "delegation_pattern": j.delegation_pattern,
                "three_tier_enforcement": j.three_tier_enforcement,
                "sunday_sales_allowed": j.sunday_sales_allowed,
                "grocery_beer_allowed": j.grocery_beer_allowed,
                "grocery_wine_allowed": j.grocery_wine_allowed,
                "beer_max_abv": float(j.beer_max_abv) if j.beer_max_abv is not None else None,
                "regulatory_override_source": j.regulatory_override_source,
                "land_area_sqm": j.land_area_sqm,
                "latitude": float(j.latitude) if j.latitude else None,
                "longitude": float(j.longitude) if j.longitude else None,
                "is_independent_city": j.is_independent_city,
                "census_year": j.census_year,
                "data_source": j.data_source,
            }
        )

    df = pd.DataFrame(rows)
    paths = {}

    # CSV
    csv_path = output_dir / f"jurisdiction_taxonomy_{year}_{timestamp}.csv"
    df.to_csv(csv_path, index=False)
    paths["csv"] = str(csv_path)
    logger.info(f"Exported CSV: {csv_path} ({len(df)} rows)")

    # Parquet
    parquet_path = output_dir / f"jurisdiction_taxonomy_{year}_{timestamp}.parquet"
    df.to_parquet(parquet_path, index=False)
    paths["parquet"] = str(parquet_path)
    logger.info(f"Exported Parquet: {parquet_path}")

    # JSON (hierarchical by state)
    json_path = output_dir / f"jurisdiction_taxonomy_{year}_{timestamp}.json"
    json_data = _build_hierarchical_json(df, year)
    with open(json_path, "w") as f:
        json.dump(json_data, f, indent=2, default=str)
    paths["json"] = str(json_path)
    logger.info(f"Exported JSON: {json_path}")

    return paths


def _build_hierarchical_json(df: pd.DataFrame, census_year: int) -> Dict:
    """Build a hierarchical JSON structure grouped by state."""
    output = {
        "metadata": {
            "census_year": census_year,
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "total_jurisdictions": len(df),
        },
        "states": {},
    }

    # Federal row
    federal = df[df["tier"] == Tier.FEDERAL.value]
    if not federal.empty:
        output["federal"] = federal.iloc[0].to_dict()

    # Group by state
    state_groups = df[df["tier"] != Tier.FEDERAL.value].groupby("state_fips")
    for state_fips, group in state_groups:
        state_row = group[group["tier"] == Tier.STATE.value]
        local_rows = group[group["tier"] == Tier.LOCAL.value]

        state_data: Dict = {}
        if not state_row.empty:
            sr = state_row.iloc[0]
            state_data["state_name"] = sr["state_name"]
            state_data["state_abbr"] = sr["state_abbr"]
            state_data["control_status"] = sr["control_status"]
            state_data["delegation_pattern"] = sr["delegation_pattern"]

        state_data["jurisdictions"] = local_rows.to_dict(orient="records")
        output["states"][state_fips] = state_data

    return output


def _generate_summary(session: Session, census_year: int, validation: Dict) -> str:
    """Generate a text summary report of the pipeline output."""
    from sqlalchemy import func

    lines = [
        "=" * 60,
        "JURISDICTION TAXONOMY PIPELINE — SUMMARY REPORT",
        f"Census Year: {census_year}",
        f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "=" * 60,
        "",
    ]

    # Overall stats
    lines.append(f"Total jurisdictions: {validation['total']}")
    lines.append(f"Validation: {'PASSED' if validation['valid'] else 'FAILED'}")
    if validation["issues"]:
        lines.append(f"Issues: {len(validation['issues'])}")
        for issue in validation["issues"]:
            lines.append(f"  - {issue}")
    if validation["warnings"]:
        lines.append(f"Warnings: {len(validation['warnings'])}")
        for warning in validation["warnings"]:
            lines.append(f"  - {warning}")

    lines.append("")

    # Per-type counts
    type_counts = (
        session.query(
            Jurisdiction.jurisdiction_type,
            func.count().label("cnt"),
        )
        .filter(Jurisdiction.census_year == census_year)
        .group_by(Jurisdiction.jurisdiction_type)
        .all()
    )

    lines.append("Counts by jurisdiction type:")
    for jtype, count in sorted(type_counts, key=lambda x: x[1], reverse=True):
        lines.append(f"  {jtype:25s} {count:>8,}")

    lines.append("")

    # Per-state counts
    state_counts = (
        session.query(
            Jurisdiction.state_fips,
            Jurisdiction.state_name,
            func.count().label("cnt"),
        )
        .filter(
            Jurisdiction.census_year == census_year,
            Jurisdiction.tier == Tier.LOCAL.value,
        )
        .group_by(Jurisdiction.state_fips, Jurisdiction.state_name)
        .order_by(Jurisdiction.state_fips)
        .all()
    )

    lines.append("Local jurisdictions per state:")
    for fips, name, count in state_counts:
        lines.append(f"  {fips} {name:30s} {count:>6,}")

    summary = "\n".join(lines)
    logger.info("\n" + summary)
    return summary
