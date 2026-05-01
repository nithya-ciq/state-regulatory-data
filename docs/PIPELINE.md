# Pipeline Architecture

## Dagster Assets (10 total)

```
research_data
  → state_classifications (loads state_classification_matrix.csv)
       → license_types (loads license_types.csv)
       → census_geographies (downloads TIGER shapefiles via pygris)
            → processed_geographies (filters CDPs, normalizes names)
                 → jurisdictions (assembles FIPS codes, delegation patterns)
                      → enriched_jurisdictions (dry/wet, authority names)
                           → jurisdiction_export (CSV/JSON/Parquet to data/output/)
                                → supabase_sync (pushes 6 tables to Supabase)
                                     → layer2_licenses (runs NJ+PA+KY scripts)
```

## Layer 2 Scripts

| Script | State | Command |
|--------|-------|---------|
| `scripts/build_pa_layer2.py` | PA | `python scripts/build_pa_layer2.py --state PA --sync-supabase` |
| `scripts/build_ky_layer2.py` | KY | `python scripts/build_ky_layer2.py --state KY --sync-supabase` |

NJ is handled differently (xlsx parsing in the Dagster asset directly).

## Supabase Tables

| Table | Rows | Primary Key |
|-------|------|-------------|
| `dim_states` | 56 | state_fips |
| `dim_geography` | 27,419 | geoid |
| `dim_licensing` | 27,419 | geoid, jurisdiction_type |
| `fact_jurisdictions` | 27,419 | geoid, jurisdiction_type, census_year |
| `dim_license_types` | 541 | state_fips, license_type_code |
| `dim_dry_wet_counties` | 329 | geoid |
| `layer2_municipality_licenses` | 3,644 | state_fips, municipality_name |
| `layer2_individual_licenses` | 134,502 | license_number |
| `v_establishment_full` | (view) | Joins all above |

## Running Locally

```bash
# Setup
make setup
source jurisdiction-env/bin/activate

# Docker PostgreSQL
make docker-up

# Migrations
make migrate

# Full pipeline (Phase 1-5)
make pipeline

# Dagster UI
dagster dev -p 3000

# Layer 2 only (after pipeline)
python scripts/build_pa_layer2.py --state PA --sync-supabase
python scripts/build_ky_layer2.py --state KY --sync-supabase
```

## Environment Variables (.env.local)

```
DATABASE_URL=postgresql://jurisdiction:jurisdiction@localhost:5432/jurisdiction
CENSUS_YEAR=2023
TIGER_RESOLUTION=500k
SUPABASE_URL=https://xhvsvhiysnacdinclncn.supabase.co
SUPABASE_KEY=<service_role_key>
```
