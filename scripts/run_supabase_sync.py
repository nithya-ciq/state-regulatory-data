#!/usr/bin/env python3
"""Standalone Supabase sync script.

Detects the current Supabase schema, applies DDL changes where possible,
then syncs all jurisdiction data from local PostgreSQL.
"""

import csv
import json
import sys
import time
import urllib.request
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from supabase import create_client

from src.common.constants import FIPS_STATES

# ── Config ──────────────────────────────────────────────────────────────────
DATABASE_URL = "postgresql://jurisdiction_user:ciq-eeaao~1@localhost:5432/jurisdiction_db"
SUPABASE_URL = "https://xhvsvhiysnacdinclncn.supabase.co"
SUPABASE_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InhodnN2aGl5c25hY2RpbmNsbmNuIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NzkxNzUyOCwiZXhwIjoyMDczNDkzNTI4fQ."
    "QocPO3khf3v3VJLviXnLAtN1Is8yA81a3WUIo9Mue5M"
)
SCHEMA = "regulations_data"
BATCH_SIZE = 500
SEED_DIR = PROJECT_ROOT / "data" / "seed"

DDL_STATEMENTS = [
    "ALTER TABLE regulations_data.dim_states ADD COLUMN IF NOT EXISTS convenience_beer_allowed BOOLEAN;",
    "ALTER TABLE regulations_data.dim_states ADD COLUMN IF NOT EXISTS convenience_wine_allowed BOOLEAN;",
    "ALTER TABLE regulations_data.dim_states DROP COLUMN IF EXISTS license_type_count;",
    "ALTER TABLE regulations_data.dim_states DROP COLUMN IF EXISTS license_complexity_tier;",
    "ALTER TABLE regulations_data.dim_licensing ADD COLUMN IF NOT EXISTS licensing_authority_type TEXT;",
    "ALTER TABLE regulations_data.dim_licensing ADD COLUMN IF NOT EXISTS licensing_authority_confidence TEXT;",
    "ALTER TABLE regulations_data.dim_licensing ADD COLUMN IF NOT EXISTS dry_wet_data_source TEXT;",
    """CREATE TABLE IF NOT EXISTS regulations_data.dim_dry_wet_counties (
    geoid TEXT PRIMARY KEY,
    state_fips TEXT,
    state_abbr TEXT,
    county_name TEXT,
    dry_wet_status TEXT,
    restriction_details TEXT,
    source TEXT,
    last_updated TEXT
);""",
]


def safe_val(val: Any) -> Any:
    if isinstance(val, Decimal):
        return float(val)
    return val


def get_table_columns(client: Any, table_name: str) -> Optional[Set[str]]:
    """Get column names for a table. Returns None if table doesn't exist."""
    try:
        r = client.schema(SCHEMA).table(table_name).select("*").limit(0).execute()
        # With limit(0), we get empty data but the columns are known from headers
        # Actually supabase-py doesn't expose column info with limit(0)
        # Fetch one row instead
        r = client.schema(SCHEMA).table(table_name).select("*").limit(1).execute()
        if r.data:
            return set(r.data[0].keys())
        # Table exists but empty - use information_schema via RPC
        rpc_result = client.rpc('execute_sql_with_schema', {
            'allowed_schemas': ['regulations_data'],
            'query': f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'regulations_data' AND table_name = '{table_name}'",
            'search_path': ['regulations_data'],
        }).execute()
        if isinstance(rpc_result.data, dict) and rpc_result.data.get('data'):
            return {row['column_name'] for row in rpc_result.data['data']}
        return set()
    except Exception:
        return None


def apply_ddl_via_supabase_sql_api() -> bool:
    """Try to apply DDL via Supabase SQL API endpoints."""
    combined_sql = "\n".join(DDL_STATEMENTS)

    # Method: Use supabase-js compatible endpoint (management API)
    project_ref = "xhvsvhiysnacdinclncn"
    api_url = f"https://api.supabase.com/v1/projects/{project_ref}/database/query"

    data = json.dumps({"query": combined_sql}).encode()
    req = urllib.request.Request(
        api_url,
        data=data,
        headers={
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        resp = urllib.request.urlopen(req, timeout=30)
        print(f"  Management API response: {resp.status}")
        print(f"  Body: {resp.read().decode()[:200]}")
        return True
    except Exception as e:
        body = ""
        if hasattr(e, 'read'):
            body = e.read().decode()[:200]
        print(f"  Management API failed: {e} {body}")
        return False


def strip_columns(records: List[Dict], allowed_columns: Set[str]) -> List[Dict]:
    """Remove any keys from records that aren't in the allowed column set."""
    return [{k: v for k, v in rec.items() if k in allowed_columns} for rec in records]


def batch_upsert(client: Any, table_name: str, records: List[Dict], conflict_key: str) -> int:
    if not records:
        return 0
    table = client.schema(SCHEMA).table(table_name)
    total = len(records)
    upserted = 0
    for i in range(0, total, BATCH_SIZE):
        batch = records[i: i + BATCH_SIZE]
        table.upsert(batch, on_conflict=conflict_key).execute()
        upserted += len(batch)
        if (i // BATCH_SIZE) % 10 == 0 or upserted == total:
            print(f"    {table_name}: {upserted}/{total} rows")
    return upserted


def deduplicate_license_types(records: List[Dict]) -> List[Dict]:
    STATUS_PRIORITY = {"verified": 3, "reviewed": 2, "draft": 1, "pending": 0}
    best: Dict[str, Dict] = {}
    for rec in records:
        key = f"{rec['state_fips']}|{rec['license_type_code']}"
        if key not in best:
            best[key] = rec
        else:
            existing_p = STATUS_PRIORITY.get(best[key].get("research_status", "pending"), 0)
            new_p = STATUS_PRIORITY.get(rec.get("research_status", "pending"), 0)
            if new_p > existing_p:
                best[key] = rec
    return list(best.values())


def read_local_data() -> Dict[str, List[Dict]]:
    print("\n=== Reading local PostgreSQL data ===")
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        rows = session.execute(text("SELECT * FROM jurisdiction.jurisdictions")).fetchall()
        columns = session.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='jurisdiction' AND table_name='jurisdictions' ORDER BY ordinal_position"
        )).fetchall()
        col_names = [c[0] for c in columns]
        print(f"  Jurisdictions: {len(rows)} rows")

        states_map: Dict[str, Dict] = {}
        geo_list, lic_list, fact_list = [], [], []

        for row_tuple in rows:
            row = dict(zip(col_names, row_tuple))
            state_fips = row.get("state_fips")

            if state_fips and state_fips not in states_map:
                states_map[state_fips] = {
                    "state_fips": state_fips,
                    "state_abbr": row.get("state_abbr"),
                    "state_name": row.get("state_name"),
                    "control_status": row.get("control_status"),
                    "delegation_pattern": row.get("delegation_pattern"),
                    "three_tier_enforcement": row.get("three_tier_enforcement"),
                    "sunday_sales_allowed": row.get("sunday_sales_allowed"),
                    "grocery_beer_allowed": row.get("grocery_beer_allowed"),
                    "grocery_wine_allowed": row.get("grocery_wine_allowed"),
                    "beer_max_abv": safe_val(row.get("beer_max_abv")),
                }

            geo_list.append({
                "geoid": row.get("geoid"),
                "jurisdiction_type": row.get("jurisdiction_type"),
                "county_fips": row.get("county_fips"),
                "place_fips": row.get("place_fips"),
                "cousub_fips": row.get("cousub_fips"),
                "jurisdiction_name": row.get("jurisdiction_name"),
                "jurisdiction_name_lsad": row.get("jurisdiction_name_lsad"),
                "county_name": row.get("county_name"),
                "land_area_sqm": row.get("land_area_sqm"),
                "latitude": safe_val(row.get("latitude")),
                "longitude": safe_val(row.get("longitude")),
                "is_independent_city": row.get("is_independent_city", False),
            })

            lic_list.append({
                "geoid": row.get("geoid"),
                "jurisdiction_type": row.get("jurisdiction_type"),
                "census_year": row.get("census_year"),
                "has_licensing_authority": row.get("has_licensing_authority", True),
                "licensing_authority_name": row.get("licensing_authority_name"),
                "licensing_authority_type": row.get("licensing_authority_type"),
                "licensing_authority_confidence": row.get("licensing_authority_confidence"),
                "is_dry": row.get("is_dry", False),
                "dry_wet_status": row.get("dry_wet_status"),
                "dry_wet_data_source": row.get("dry_wet_data_source"),
                "regulatory_override_source": row.get("regulatory_override_source"),
            })

            fact_list.append({
                "geoid": row.get("geoid"),
                "jurisdiction_type": row.get("jurisdiction_type"),
                "census_year": row.get("census_year"),
                "state_fips": state_fips,
                "tier": row.get("tier"),
                "pipeline_run_id": row.get("pipeline_run_id"),
                "data_source": row.get("data_source"),
            })

        # Enrich dim_states from StateClassification
        sc_rows = session.execute(text("SELECT * FROM jurisdiction.state_classifications")).fetchall()
        sc_columns = session.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='jurisdiction' AND table_name='state_classifications' ORDER BY ordinal_position"
        )).fetchall()
        sc_col_names = [c[0] for c in sc_columns]
        print(f"  State classifications: {len(sc_rows)} rows")

        for sc_tuple in sc_rows:
            sc = dict(zip(sc_col_names, sc_tuple))
            sfips = sc.get("state_fips")
            if sfips and sfips in states_map:
                states_map[sfips].update({
                    "grocery_liquor_allowed": sc.get("grocery_liquor_allowed"),
                    "convenience_liquor_allowed": sc.get("convenience_liquor_allowed"),
                    "grocery_beer_confidence": sc.get("grocery_beer_confidence"),
                    "grocery_wine_confidence": sc.get("grocery_wine_confidence"),
                    "grocery_liquor_confidence": sc.get("grocery_liquor_confidence"),
                    "retail_channel_notes": sc.get("retail_channel_notes"),
                    "convenience_beer_allowed": sc.get("convenience_beer_allowed"),
                    "convenience_wine_allowed": sc.get("convenience_wine_allowed"),
                })

        # License types
        lt_rows = session.execute(text("SELECT * FROM jurisdiction.license_types")).fetchall()
        lt_columns = session.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='jurisdiction' AND table_name='license_types' ORDER BY ordinal_position"
        )).fetchall()
        lt_col_names = [c[0] for c in lt_columns]
        print(f"  License types: {len(lt_rows)} rows")

        license_types_list = []
        for lt_tuple in lt_rows:
            lt = dict(zip(lt_col_names, lt_tuple))
            license_types_list.append({
                "state_fips": lt.get("state_fips"),
                "license_type_code": lt.get("license_type_code"),
                "license_type_name": lt.get("license_type_name"),
                "license_category": lt.get("license_category"),
                "permits_on_premise": lt.get("permits_on_premise", False),
                "permits_off_premise": lt.get("permits_off_premise", False),
                "permits_beer": lt.get("permits_beer", False),
                "permits_wine": lt.get("permits_wine", False),
                "permits_spirits": lt.get("permits_spirits", False),
                "retail_channel": lt.get("retail_channel"),
                "abv_limit": safe_val(lt.get("abv_limit")),
                "quota_limited": lt.get("quota_limited"),
                "quota_notes": lt.get("quota_notes"),
                "transferable": lt.get("transferable"),
                "annual_fee_range": lt.get("annual_fee_range"),
                "issuing_authority": lt.get("issuing_authority"),
                "statutory_reference": lt.get("statutory_reference"),
                "notes": lt.get("notes"),
                "research_status": lt.get("research_status"),
                "research_source": lt.get("research_source"),
                "last_verified_date": str(lt["last_verified_date"]) if lt.get("last_verified_date") else None,
            })

        license_types_list = deduplicate_license_types(license_types_list)
        print(f"  License types after dedup: {len(license_types_list)} rows")

    finally:
        session.close()

    return {
        "dim_states": list(states_map.values()),
        "dim_geography": geo_list,
        "dim_licensing": lic_list,
        "fact_jurisdictions": fact_list,
        "dim_license_types": license_types_list,
    }


def build_dry_wet_counties() -> List[Dict]:
    records: Dict[str, Dict] = {}

    csv1 = SEED_DIR / "dry_wet_status.csv"
    if csv1.exists():
        with open(csv1, "r") as f:
            for row in csv.DictReader(f):
                geoid = row.get("geoid", "").strip()
                if not geoid:
                    continue
                sf = row.get("state_fips", "").strip()
                records[geoid] = {
                    "geoid": geoid,
                    "state_fips": sf,
                    "state_abbr": FIPS_STATES.get(sf, ("", ""))[0] if sf else "",
                    "county_name": row.get("jurisdiction_name", "").strip(),
                    "dry_wet_status": row.get("dry_wet_status", "").strip(),
                    "restriction_details": row.get("restriction_notes", "").strip(),
                    "source": row.get("data_source", "").strip(),
                    "last_updated": row.get("last_verified", "").strip(),
                }

    csv2 = SEED_DIR / "dry_wet_research_batch2.csv"
    if csv2.exists():
        with open(csv2, "r") as f:
            for row in csv.DictReader(f):
                geoid = row.get("geoid", "").strip()
                if not geoid:
                    continue
                sf = row.get("state_fips", "").strip()
                records[geoid] = {
                    "geoid": geoid,
                    "state_fips": sf,
                    "state_abbr": FIPS_STATES.get(sf, ("", ""))[0] if sf else "",
                    "county_name": row.get("jurisdiction_name", "").strip(),
                    "dry_wet_status": row.get("dry_wet_status", "").strip(),
                    "restriction_details": row.get("dry_wet_notes", "").strip(),
                    "source": row.get("data_source", "").strip(),
                    "last_updated": "",
                }

    return list(records.values())


def main() -> None:
    start = time.time()
    print("=" * 60)
    print("SUPABASE SYNC — Regulatory Research Pipeline")
    print("=" * 60)

    client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # ── Step 1: Try to apply DDL ────────────────────────────────────────────
    print("\n=== Step 1: Schema migration ===")
    ddl_applied = apply_ddl_via_supabase_sql_api()
    if not ddl_applied:
        print("\n  DDL could not be applied automatically.")
        print("  Will adapt data to match current Supabase schema.")
        print("  To apply schema changes, run this SQL in Supabase SQL Editor:")
        print("  ---")
        for stmt in DDL_STATEMENTS:
            print(f"  {stmt}")
        print("  ---\n")

    # ── Step 2: Detect current schema ───────────────────────────────────────
    print("\n=== Step 2: Detecting current Supabase schema ===")
    table_schemas: Dict[str, Optional[Set[str]]] = {}
    for tbl in ["dim_states", "dim_geography", "dim_licensing", "fact_jurisdictions", "dim_license_types", "dim_dry_wet_counties"]:
        cols = get_table_columns(client, tbl)
        table_schemas[tbl] = cols
        if cols is None:
            print(f"  {tbl}: TABLE DOES NOT EXIST")
        else:
            print(f"  {tbl}: {len(cols)} columns")

    # ── Step 3: Read local data ─────────────────────────────────────────────
    data = read_local_data()
    dry_wet = build_dry_wet_counties()
    print(f"  Dry/wet counties from seed: {len(dry_wet)} rows")

    # ── Step 4: Sync to Supabase (adapt to actual schema) ──────────────────
    print("\n=== Step 3: Syncing to Supabase ===")

    conflicts = {
        "dim_states": "state_fips",
        "dim_geography": "geoid,jurisdiction_type",
        "dim_licensing": "geoid,jurisdiction_type,census_year",
        "fact_jurisdictions": "geoid,jurisdiction_type,census_year",
        "dim_license_types": "state_fips,license_type_code",
        "dim_dry_wet_counties": "geoid",
    }

    results = {}

    for table_name in ["dim_states", "dim_geography", "dim_licensing", "fact_jurisdictions", "dim_license_types"]:
        records = data[table_name]
        existing_cols = table_schemas.get(table_name)

        if existing_cols is None:
            print(f"  SKIP {table_name}: table does not exist in Supabase")
            results[table_name] = 0
            continue

        # Strip columns that don't exist in Supabase yet
        adapted = strip_columns(records, existing_cols)
        count = batch_upsert(client, table_name, adapted, conflicts[table_name])
        results[table_name] = count

    # Dry/wet counties
    if table_schemas.get("dim_dry_wet_counties") is not None:
        existing_cols = table_schemas["dim_dry_wet_counties"]
        adapted = strip_columns(dry_wet, existing_cols)
        dw_count = batch_upsert(client, "dim_dry_wet_counties", adapted, conflicts["dim_dry_wet_counties"])
        results["dim_dry_wet_counties"] = dw_count
    else:
        print("  SKIP dim_dry_wet_counties: table does not exist in Supabase")
        results["dim_dry_wet_counties"] = 0

    elapsed = round(time.time() - start, 2)
    print(f"\n{'=' * 60}")
    print(f"SYNC COMPLETE in {elapsed}s")
    print(f"{'=' * 60}")
    for table, count in results.items():
        print(f"  {table:30s} {count:>8,} rows")
    print(f"  {'TOTAL':30s} {sum(results.values()):>8,} rows")

    # Print post-sync instructions if DDL wasn't applied
    if not ddl_applied:
        missing_tables = [t for t, c in table_schemas.items() if c is None]
        if missing_tables or not ddl_applied:
            print(f"\n{'=' * 60}")
            print("ACTION REQUIRED: Run the following SQL in Supabase SQL Editor")
            print("then re-run this script to sync the remaining data.")
            print(f"{'=' * 60}")
            for stmt in DDL_STATEMENTS:
                print(stmt)


if __name__ == "__main__":
    main()
