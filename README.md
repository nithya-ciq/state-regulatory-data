# State Regulatory Data Pipeline

Data pipeline that builds a FIPS-coded dataset of US alcohol licensing jurisdictions (~27,000 rows) plus Layer 2 municipality-level license data for 11 complex states (134,502 individual licenses).

## Quick Start

```bash
# 1. Setup
make setup
source jurisdiction-env/bin/activate

# 2. Docker (PostgreSQL 14)
make docker-up

# 3. Database migrations
make migrate

# 4. Run pipeline
make pipeline

# 5. Run Dagster UI
dagster dev -p 3000
```

## Environment

Copy `.env.example` to `.env.local` and fill in:
- `DATABASE_URL` — PostgreSQL connection string
- `SUPABASE_URL` — Supabase project URL
- `SUPABASE_KEY` — Supabase service role key

## Architecture

### Layer 1: State-Level Rules (56 rows)
- Source: `data/seed/state_classification_matrix.csv`
- Contains: Control status, Sunday sales, grocery/convenience permissions, three-tier enforcement, wet/dry flags
- Supabase table: `regulations_data.dim_states`

### Layer 2: Municipality-Level Data (11 states)
Individual license rosters from official state ABC portals:

| State | Licenses | Source | Script |
|-------|----------|--------|--------|
| NJ | 8,815 | NJ ABC Roster | `scripts/build_nj_layer2.py` |
| PA | 18,577 | PLCB CSV Export | `scripts/build_pa_layer2.py` |
| KY | 17,260 | ABC BELLE Portal | `scripts/build_ky_layer2.py` |
| MA | 12,471 | ABCC Active Licenses | (direct CSV) |
| TX | 77,379 | TX Open Data Portal | (direct CSV) |
| AK | 109 | AMCO Local Option PDF | (seed CSV) |
| AR | 260 | GIS REST API | (seed CSV) |
| MD | 25 | ATCC Directory | (seed CSV) |
| AL | 66 | ABC Wet Cities | (seed CSV) |
| NC | 100 | ABC Commission | (seed CSV) |
| MS | 6 | DOR ABC | (seed CSV) |

### Supabase View
```sql
-- One query answers everything
SELECT * FROM regulations_data.v_establishment_full
WHERE establishment_name ILIKE '%costco%'
-- Returns: business name, city, state, license type, sunday_sales, grocery permissions, wet/dry status
```

## Dagster Assets

```
state_classifications → license_types → census_geographies → processed_geographies
  → jurisdictions → enriched_jurisdictions → jurisdiction_export → supabase_sync → layer2_licenses
```

## Layer 2 Scripts

```bash
# Run individual state Layer 2
python scripts/build_pa_layer2.py --state PA --sync-supabase
python scripts/build_ky_layer2.py --state KY --sync-supabase
```

## Key Seed Files

| File | Rows | What |
|------|------|------|
| `data/seed/state_classification_matrix.csv` | 56 | Layer 1 state rules (47 columns) |
| `data/seed/license_types.csv` | 541 | License type catalog per state |
| `data/seed/nj_municipality_license_summary.csv` | 737 | NJ municipality aggregates |
| `data/seed/pa_license_list.csv` | 58,868 | Raw PA PLCB export |
| `data/seed/ky_license_list.csv` | 21,757 | Raw KY BELLE export |
| `data/seed/ma_license_list.csv` | 12,471 | Raw MA ABCC export |
| `data/seed/tx_license_list.csv` | 77,379 | Raw TX TABC export |

## Documentation

- `data/output/regulatory_data_technical_documentation.docx` — Full technical doc (data lineage, collection methodology, query guide)
- `data/output/regulatory_review_MASTER.docx` — Business overview for Gauri
- `CLAUDE.md` — AI assistant context file
