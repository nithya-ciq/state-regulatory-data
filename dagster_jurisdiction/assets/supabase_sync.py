"""Supabase sync asset — upsert jurisdiction data to star schema."""

import csv
import logging
import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Tuple

import dagster as dg

from src.models.jurisdiction import Jurisdiction
from src.models.license_type import LicenseType
from src.models.state_classification import StateClassification
from src.common.constants import FIPS_STATES
from dagster_jurisdiction.resources.database import DatabaseResource
from dagster_jurisdiction.resources.supabase import SupabaseResource

logger = logging.getLogger("jurisdiction.dagster.supabase_sync")

# Supabase REST API batch size (stay well under 1000 row limit)
BATCH_SIZE = 500

# Schema and table names in Supabase
SCHEMA = "regulations_data"
TABLES = {
    "dim_states": {
        "conflict": "state_fips",
    },
    "dim_geography": {
        "conflict": "geoid,jurisdiction_type",
    },
    "dim_licensing": {
        "conflict": "geoid,jurisdiction_type,census_year",
    },
    "fact_jurisdictions": {
        "conflict": "geoid,jurisdiction_type,census_year",
    },
    "dim_license_types": {
        "conflict": "state_fips,license_type_code",
    },
    "dim_dry_wet_counties": {
        "conflict": "geoid",
    },
}


def _safe_value(val: Any) -> Any:
    """Convert SQLAlchemy values to JSON-safe types."""
    if isinstance(val, Decimal):
        return float(val)
    return val


def _split_row(row: Jurisdiction) -> Tuple[Dict, Dict, Dict, Dict]:
    """Split one Jurisdiction ORM row into 4 star schema dicts.

    Returns (dim_state, dim_geography, dim_licensing, fact) dicts.
    """
    dim_state = {
        "state_fips": row.state_fips,
        "state_abbr": row.state_abbr,
        "state_name": row.state_name,
        "control_status": row.control_status,
        "delegation_pattern": row.delegation_pattern,
        "three_tier_enforcement": row.three_tier_enforcement,
        "sunday_sales_allowed": row.sunday_sales_allowed,
        "grocery_beer_allowed": row.grocery_beer_allowed,
        "grocery_wine_allowed": row.grocery_wine_allowed,
        "beer_max_abv": _safe_value(row.beer_max_abv),
    }

    dim_geography = {
        "geoid": row.geoid,
        "jurisdiction_type": row.jurisdiction_type,
        "county_fips": row.county_fips,
        "place_fips": row.place_fips,
        "cousub_fips": row.cousub_fips,
        "jurisdiction_name": row.jurisdiction_name,
        "jurisdiction_name_lsad": row.jurisdiction_name_lsad,
        "county_name": row.county_name,
        "land_area_sqm": row.land_area_sqm,
        "latitude": _safe_value(row.latitude),
        "longitude": _safe_value(row.longitude),
        "is_independent_city": row.is_independent_city,
    }

    dim_licensing = {
        "geoid": row.geoid,
        "jurisdiction_type": row.jurisdiction_type,
        "census_year": row.census_year,
        "has_licensing_authority": row.has_licensing_authority,
        "licensing_authority_name": row.licensing_authority_name,
        "licensing_authority_type": row.licensing_authority_type,
        "licensing_authority_confidence": row.licensing_authority_confidence,
        "is_dry": row.is_dry,
        "dry_wet_status": row.dry_wet_status,
        "dry_wet_data_source": row.dry_wet_data_source,
        "regulatory_override_source": row.regulatory_override_source,
    }

    fact = {
        "geoid": row.geoid,
        "jurisdiction_type": row.jurisdiction_type,
        "census_year": row.census_year,
        "state_fips": row.state_fips,
        "tier": row.tier,
        "pipeline_run_id": row.pipeline_run_id,
        "data_source": row.data_source,
    }

    return dim_state, dim_geography, dim_licensing, fact


def _batch_upsert(
    client: Any,
    table_name: str,
    records: List[Dict],
    conflict_key: str,
    context: dg.AssetExecutionContext,
) -> int:
    """Upsert records to a Supabase table in batches.

    Returns total rows upserted.
    """
    table = client.schema(SCHEMA).table(table_name)
    total = len(records)
    upserted = 0

    for i in range(0, total, BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        table.upsert(batch, on_conflict=conflict_key).execute()
        upserted += len(batch)
        context.log.info(
            f"  {table_name}: batch {i // BATCH_SIZE + 1} "
            f"({upserted}/{total} rows)"
        )

    return upserted


def _build_license_types(lt_rows: list) -> List[Dict]:
    """Convert LicenseType ORM rows to dicts for Supabase upsert."""
    records = []
    for lt in lt_rows:
        records.append({
            "state_fips": lt.state_fips,
            "license_type_code": lt.license_type_code,
            "license_type_name": lt.license_type_name,
            "license_category": lt.license_category,
            "permits_on_premise": lt.permits_on_premise,
            "permits_off_premise": lt.permits_off_premise,
            "permits_beer": lt.permits_beer,
            "permits_wine": lt.permits_wine,
            "permits_spirits": lt.permits_spirits,
            "retail_channel": lt.retail_channel,
            "abv_limit": _safe_value(lt.abv_limit),
            "quota_limited": lt.quota_limited,
            "quota_notes": lt.quota_notes,
            "transferable": lt.transferable,
            "annual_fee_range": lt.annual_fee_range,
            "issuing_authority": lt.issuing_authority,
            "statutory_reference": lt.statutory_reference,
            "notes": lt.notes,
            "research_status": lt.research_status,
            "research_source": lt.research_source,
            "last_verified_date": str(lt.last_verified_date) if lt.last_verified_date else None,
        })
    return records


def _deduplicate_license_types(records: List[Dict]) -> List[Dict]:
    """Deduplicate license types: keep latest research_status='verified' over 'draft'
    when same state_fips + license_type_code."""
    STATUS_PRIORITY = {"verified": 3, "reviewed": 2, "draft": 1, "pending": 0}
    best: Dict[str, Dict] = {}
    for rec in records:
        key = f"{rec['state_fips']}|{rec['license_type_code']}"
        if key not in best:
            best[key] = rec
        else:
            existing_priority = STATUS_PRIORITY.get(best[key].get("research_status", "pending"), 0)
            new_priority = STATUS_PRIORITY.get(rec.get("research_status", "pending"), 0)
            if new_priority > existing_priority:
                best[key] = rec
    return list(best.values())


def _build_dim_dry_wet() -> List[Dict]:
    """Read dry/wet county data from CSV seed files and build records for dim_dry_wet_counties."""
    seed_dir = Path(__file__).resolve().parents[2] / "data" / "seed"
    records: Dict[str, Dict] = {}

    # File 1: dry_wet_status.csv
    csv1 = seed_dir / "dry_wet_status.csv"
    if csv1.exists():
        with open(csv1, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                geoid = row.get("geoid", "").strip()
                if not geoid:
                    continue
                state_fips = row.get("state_fips", "").strip()
                state_abbr = FIPS_STATES.get(state_fips, ("", ""))[0] if state_fips else ""
                records[geoid] = {
                    "geoid": geoid,
                    "state_fips": state_fips,
                    "state_abbr": state_abbr,
                    "county_name": row.get("jurisdiction_name", "").strip(),
                    "dry_wet_status": row.get("dry_wet_status", "").strip(),
                    "restriction_details": row.get("restriction_notes", "").strip(),
                    "source": row.get("data_source", "").strip(),
                    "last_updated": row.get("last_verified", "").strip(),
                }

    # File 2: dry_wet_research_batch2.csv
    csv2 = seed_dir / "dry_wet_research_batch2.csv"
    if csv2.exists():
        with open(csv2, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                geoid = row.get("geoid", "").strip()
                if not geoid:
                    continue
                state_fips = row.get("state_fips", "").strip()
                state_abbr = FIPS_STATES.get(state_fips, ("", ""))[0] if state_fips else ""
                records[geoid] = {
                    "geoid": geoid,
                    "state_fips": state_fips,
                    "state_abbr": state_abbr,
                    "county_name": row.get("jurisdiction_name", "").strip(),
                    "dry_wet_status": row.get("dry_wet_status", "").strip(),
                    "restriction_details": row.get("dry_wet_notes", "").strip(),
                    "source": row.get("data_source", "").strip(),
                    "last_updated": "",
                }

    return list(records.values())


@dg.asset(
    group_name="6_supabase",
    deps=["jurisdiction_export"],
    description=(
        "Sync all jurisdiction data from local PostgreSQL to Supabase "
        "star schema (regulations_data): fact_jurisdictions, dim_states, "
        "dim_geography, dim_licensing."
    ),
    kinds={"python", "supabase"},
)
def supabase_sync(
    context: dg.AssetExecutionContext,
    database: DatabaseResource,
    supabase: SupabaseResource,
) -> dg.MaterializeResult:
    """Read jurisdictions from local DB and upsert to Supabase star schema."""
    start_time = time.time()

    # 1. Read all jurisdictions from local PostgreSQL
    session = database.get_session()
    try:
        rows = session.query(Jurisdiction).all()
        total_rows = len(rows)
        context.log.info(f"Read {total_rows} jurisdiction rows from local DB")

        if total_rows == 0:
            raise RuntimeError(
                "Supabase sync FAILED: no jurisdiction rows in local DB. "
                "Run the full pipeline first."
            )

        # 2. Split each row into 4 star schema tables
        states_map: Dict[str, Dict] = {}  # deduplicate by state_fips
        geo_list: List[Dict] = []
        lic_list: List[Dict] = []
        fact_list: List[Dict] = []

        for row in rows:
            dim_state, dim_geo, dim_lic, fact = _split_row(row)

            # Deduplicate dim_states (same state_fips appears many times)
            if dim_state["state_fips"] not in states_map:
                states_map[dim_state["state_fips"]] = dim_state

            geo_list.append(dim_geo)
            lic_list.append(dim_lic)
            fact_list.append(fact)

        # 2b. Enrich dim_states with Phase 2 columns from StateClassification
        state_class_rows = session.query(StateClassification).all()
        for sc in state_class_rows:
            if sc.state_fips in states_map:
                states_map[sc.state_fips].update({
                    "grocery_liquor_allowed": sc.grocery_liquor_allowed,
                    "convenience_liquor_allowed": sc.convenience_liquor_allowed,
                    "grocery_beer_confidence": sc.grocery_beer_confidence,
                    "grocery_wine_confidence": sc.grocery_wine_confidence,
                    "grocery_liquor_confidence": sc.grocery_liquor_confidence,
                    "retail_channel_notes": sc.retail_channel_notes,
                    "convenience_beer_allowed": sc.convenience_beer_allowed,
                    "convenience_wine_allowed": sc.convenience_wine_allowed,
                    "spirits_control": sc.spirits_control,
                    "wine_control": sc.wine_control,
                    "beer_control": sc.beer_control,
                    "wholesale_control": sc.wholesale_control,
                    "retail_control": sc.retail_control,
                    "data_effective_date": sc.data_effective_date,
                })

        states_list = list(states_map.values())

        # 2c. Build license types list from LicenseType table
        lt_rows = session.query(LicenseType).all()
        license_types_list = _build_license_types(lt_rows)
        license_types_list = _deduplicate_license_types(license_types_list)
        context.log.info(f"Read {len(license_types_list)} license types from local DB (after dedup)")
    finally:
        session.close()

    # 3. Get Supabase client
    client = supabase.get_client()

    # 4. Upsert in order: dimensions first, then fact table
    context.log.info(
        f"Syncing to Supabase ({SCHEMA}): "
        f"{len(states_list)} states, {len(geo_list)} geographies, "
        f"{len(lic_list)} licensing, {len(fact_list)} facts"
    )

    states_synced = _batch_upsert(
        client, "dim_states", states_list,
        TABLES["dim_states"]["conflict"], context,
    )

    geo_synced = _batch_upsert(
        client, "dim_geography", geo_list,
        TABLES["dim_geography"]["conflict"], context,
    )

    lic_synced = _batch_upsert(
        client, "dim_licensing", lic_list,
        TABLES["dim_licensing"]["conflict"], context,
    )

    fact_synced = _batch_upsert(
        client, "fact_jurisdictions", fact_list,
        TABLES["fact_jurisdictions"]["conflict"], context,
    )

    # Sync license types (may be 0 if seed not yet populated)
    lt_synced = 0
    if license_types_list:
        lt_synced = _batch_upsert(
            client, "dim_license_types", license_types_list,
            TABLES["dim_license_types"]["conflict"], context,
        )

    # Sync dry/wet counties from seed CSVs
    dry_wet_list = _build_dim_dry_wet()
    dry_wet_synced = 0
    if dry_wet_list:
        dry_wet_synced = _batch_upsert(
            client, "dim_dry_wet_counties", dry_wet_list,
            TABLES["dim_dry_wet_counties"]["conflict"], context,
        )
    context.log.info(f"dim_dry_wet_counties: {dry_wet_synced} rows synced from seed CSVs")

    elapsed = round(time.time() - start_time, 2)

    context.log.info(
        f"Supabase sync complete in {elapsed}s: "
        f"dim_states={states_synced}, dim_geography={geo_synced}, "
        f"dim_licensing={lic_synced}, fact_jurisdictions={fact_synced}, "
        f"dim_license_types={lt_synced}, dim_dry_wet_counties={dry_wet_synced}"
    )

    return dg.MaterializeResult(
        metadata={
            "dim_states_rows": dg.MetadataValue.int(states_synced),
            "dim_geography_rows": dg.MetadataValue.int(geo_synced),
            "dim_licensing_rows": dg.MetadataValue.int(lic_synced),
            "fact_jurisdictions_rows": dg.MetadataValue.int(fact_synced),
            "dim_license_types_rows": dg.MetadataValue.int(lt_synced),
            "dim_dry_wet_counties_rows": dg.MetadataValue.int(dry_wet_synced),
            "total_rows_synced": dg.MetadataValue.int(
                states_synced + geo_synced + lic_synced + fact_synced + lt_synced + dry_wet_synced
            ),
            "sync_duration_seconds": dg.MetadataValue.float(elapsed),
            "supabase_schema": dg.MetadataValue.text(SCHEMA),
        }
    )
