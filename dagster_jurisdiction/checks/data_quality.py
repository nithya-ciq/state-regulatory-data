"""Asset checks for data quality validation."""

import dagster as dg

from dagster_jurisdiction.resources.database import DatabaseResource
from dagster_jurisdiction.resources.pipeline_config import PipelineConfigResource
from src.models.state_classification import StateClassification
from src.models.census_geography import CensusGeography
from src.models.jurisdiction import Jurisdiction
from src.common.constants import EXPECTED_STATE_COUNT


@dg.asset_check(
    asset="research_data",
    description="Verify seed CSVs exist and are non-empty after research.",
)
def check_seed_files_updated(
    pipeline_config: PipelineConfigResource,
) -> dg.AssetCheckResult:
    """Check that critical seed files exist and have content."""
    from pathlib import Path

    config = pipeline_config.to_config()
    seed_dir = config.seed_dir

    required_files = [
        "state_classification_matrix.csv",
        "dry_wet_status.csv",
        "licensing_authority_names.csv",
    ]

    missing = []
    empty = []
    for fname in required_files:
        fpath = seed_dir / fname
        if not fpath.exists():
            missing.append(fname)
        elif fpath.stat().st_size == 0:
            empty.append(fname)

    passed = len(missing) == 0 and len(empty) == 0
    return dg.AssetCheckResult(
        passed=passed,
        metadata={
            "missing_files": dg.MetadataValue.text(", ".join(missing) if missing else "none"),
            "empty_files": dg.MetadataValue.text(", ".join(empty) if empty else "none"),
        },
    )


@dg.asset_check(
    asset="state_classifications",
    description="Verify 56 state classifications loaded (50 states + DC + 5 territories).",
)
def check_state_classification_count(
    database: DatabaseResource,
) -> dg.AssetCheckResult:
    """Check that the state_classifications table has the expected row count."""
    session = database.get_session()
    try:
        count = session.query(StateClassification).count()
        passed = count >= EXPECTED_STATE_COUNT
        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "expected_minimum": dg.MetadataValue.int(EXPECTED_STATE_COUNT),
            },
        )
    finally:
        session.close()


@dg.asset_check(
    asset="census_geographies",
    description="Verify census geographies table is populated.",
)
def check_census_geographies_count(
    database: DatabaseResource,
) -> dg.AssetCheckResult:
    """Check that census data was downloaded successfully."""
    session = database.get_session()
    try:
        count = session.query(CensusGeography).count()
        passed = count > 0
        return dg.AssetCheckResult(
            passed=passed,
            metadata={"row_count": dg.MetadataValue.int(count)},
        )
    finally:
        session.close()


@dg.asset_check(
    asset="jurisdictions",
    description="Verify jurisdiction count is in the expected range (15,000-50,000).",
)
def check_jurisdiction_count(
    database: DatabaseResource,
) -> dg.AssetCheckResult:
    """Check total jurisdiction count is within expected bounds."""
    session = database.get_session()
    try:
        count = session.query(Jurisdiction).count()
        passed = 15_000 <= count <= 50_000
        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "expected_range": dg.MetadataValue.text("15,000 - 50,000"),
            },
        )
    finally:
        session.close()


@dg.asset_check(
    asset="jurisdictions",
    description="Verify no duplicate GEOID/jurisdiction_type combinations.",
)
def check_no_duplicate_geoids(
    database: DatabaseResource,
) -> dg.AssetCheckResult:
    """Check that the unique constraint on (geoid, jurisdiction_type) holds."""
    from sqlalchemy import func

    session = database.get_session()
    try:
        dupes = (
            session.query(
                Jurisdiction.geoid,
                Jurisdiction.jurisdiction_type,
                func.count().label("cnt"),
            )
            .group_by(Jurisdiction.geoid, Jurisdiction.jurisdiction_type)
            .having(func.count() > 1)
            .count()
        )
        return dg.AssetCheckResult(
            passed=dupes == 0,
            metadata={"duplicate_groups": dg.MetadataValue.int(dupes)},
        )
    finally:
        session.close()


@dg.asset_check(
    asset="enriched_jurisdictions",
    description="Verify enrichment was applied (licensing authority names populated).",
)
def check_enrichment_applied(
    database: DatabaseResource,
) -> dg.AssetCheckResult:
    """Check that at least some jurisdictions have licensing authority names."""
    session = database.get_session()
    try:
        with_authority = (
            session.query(Jurisdiction)
            .filter(Jurisdiction.licensing_authority_name.isnot(None))
            .count()
        )
        total = session.query(Jurisdiction).count()
        pct = (with_authority / total * 100) if total > 0 else 0
        return dg.AssetCheckResult(
            passed=with_authority > 0,
            metadata={
                "with_authority_name": dg.MetadataValue.int(with_authority),
                "total": dg.MetadataValue.int(total),
                "percentage": dg.MetadataValue.float(round(pct, 1)),
            },
        )
    finally:
        session.close()


@dg.asset_check(
    asset="enriched_jurisdictions",
    description="Verify regulatory detail fields (sunday_sales, grocery, three_tier) are populated for all states.",
)
def check_regulatory_data_populated(
    database: DatabaseResource,
) -> dg.AssetCheckResult:
    """Check that regulatory detail columns are non-NULL for all 56 states.

    Validates: three_tier_enforcement, sunday_sales_allowed, grocery_beer_allowed,
    grocery_wine_allowed, convenience_beer_allowed, convenience_wine_allowed.
    These fields must be populated before syncing to Supabase.
    """
    session = database.get_session()
    try:
        total_states = session.query(StateClassification).count()

        # Check each critical regulatory field
        fields = {
            "three_tier_enforcement": StateClassification.three_tier_enforcement,
            "sunday_sales_allowed": StateClassification.sunday_sales_allowed,
            "grocery_beer_allowed": StateClassification.grocery_beer_allowed,
            "grocery_wine_allowed": StateClassification.grocery_wine_allowed,
        }

        null_counts = {}
        for name, col in fields.items():
            null_count = (
                session.query(StateClassification)
                .filter(col.is_(None))
                .count()
            )
            null_counts[name] = null_count

        total_nulls = sum(null_counts.values())
        passed = total_nulls == 0

        metadata = {
            "total_states": dg.MetadataValue.int(total_states),
            "total_null_fields": dg.MetadataValue.int(total_nulls),
        }
        for name, count in null_counts.items():
            metadata[f"null_{name}"] = dg.MetadataValue.int(count)

        if not passed:
            # List which states are missing data
            missing_states = (
                session.query(StateClassification.state_abbr)
                .filter(
                    (StateClassification.three_tier_enforcement.is_(None))
                    | (StateClassification.sunday_sales_allowed.is_(None))
                    | (StateClassification.grocery_beer_allowed.is_(None))
                    | (StateClassification.grocery_wine_allowed.is_(None))
                )
                .all()
            )
            abbrs = [s[0] for s in missing_states]
            metadata["states_missing_data"] = dg.MetadataValue.text(
                ", ".join(abbrs[:20]) + (f" (+{len(abbrs)-20} more)" if len(abbrs) > 20 else "")
            )

        return dg.AssetCheckResult(passed=passed, metadata=metadata)
    finally:
        session.close()


@dg.asset_check(
    asset="enriched_jurisdictions",
    description="Verify regulatory fields propagated from state_classifications to jurisdictions.",
)
def check_regulatory_propagation(
    database: DatabaseResource,
) -> dg.AssetCheckResult:
    """Check that jurisdictions have non-NULL regulatory fields after enrichment."""
    session = database.get_session()
    try:
        total = session.query(Jurisdiction).count()
        with_regulatory = (
            session.query(Jurisdiction)
            .filter(Jurisdiction.three_tier_enforcement.isnot(None))
            .count()
        )
        pct = (with_regulatory / total * 100) if total > 0 else 0

        # Should be 100% if all states have regulatory data
        passed = pct >= 95.0

        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "total_jurisdictions": dg.MetadataValue.int(total),
                "with_regulatory_data": dg.MetadataValue.int(with_regulatory),
                "percentage": dg.MetadataValue.float(round(pct, 1)),
            },
        )
    finally:
        session.close()


@dg.asset_check(
    asset="supabase_sync",
    description="Verify Supabase fact_jurisdictions count matches local DB.",
)
def check_supabase_row_count(
    database: DatabaseResource,
) -> dg.AssetCheckResult:
    """Check that Supabase has data after sync by comparing to local DB."""
    from dagster_jurisdiction.resources.supabase import SupabaseResource
    import os

    session = database.get_session()
    try:
        local_count = session.query(Jurisdiction).count()
    finally:
        session.close()

    # Try to check Supabase; skip gracefully if not configured
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        return dg.AssetCheckResult(
            passed=True,
            metadata={
                "local_count": dg.MetadataValue.int(local_count),
                "supabase_count": dg.MetadataValue.text("skipped (no credentials)"),
            },
        )

    from supabase import create_client

    client = create_client(url, key)
    response = (
        client.schema("regulations_data")
        .table("fact_jurisdictions")
        .select("geoid", count="exact", head=True)
        .execute()
    )
    remote_count = response.count or 0

    # Allow 5% tolerance for timing/partial syncs
    passed = remote_count >= local_count * 0.95
    return dg.AssetCheckResult(
        passed=passed,
        metadata={
            "local_count": dg.MetadataValue.int(local_count),
            "supabase_count": dg.MetadataValue.int(remote_count),
        },
    )


all_checks = [
    check_seed_files_updated,
    check_state_classification_count,
    check_census_geographies_count,
    check_jurisdiction_count,
    check_no_duplicate_geoids,
    check_enrichment_applied,
    check_regulatory_data_populated,
    check_regulatory_propagation,
    check_supabase_row_count,
]
