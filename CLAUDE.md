# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Data pipeline that builds a FIPS-coded dataset of US alcohol licensing jurisdictions (~20,000-35,000 rows). Output links regulatory authorities to geographic units across 50 states, DC, and 5 territories for the BevGenie platform.

## Common Commands

```bash
# Setup
make setup                    # Create venv (jurisdiction-env) + install deps
source jurisdiction-env/bin/activate

# Docker (PostgreSQL 14)
make docker-up                # Start PostgreSQL
make docker-down              # Stop
make docker-reset             # Stop + remove volumes

# Database
make migrate                  # Run Alembic migrations

# Testing
make test                     # All tests with coverage
make test-unit                # Unit tests only (no Docker/network)
make test-integration         # Integration tests (requires Docker)
# Single test file:
python -m pytest tests/unit/test_fips.py -v
# Single test:
python -m pytest tests/unit/test_fips.py::test_function_name -v

# Code quality
make lint                     # ruff + mypy
make format                   # black + ruff --fix

# Pipeline
make pipeline                 # Full run
python -m src.pipeline.orchestrator --states 01,06 --phase 2 --year 2023
python -m src.pipeline.orchestrator --dry-run   # Validate only

# Research tool
make research
```

## Architecture

**5-phase sequential pipeline** orchestrated by `src/pipeline/orchestrator.py` (Click CLI):

1. **Phase 1 — State Classification** (`phase1_state_classification.py`): Loads `data/seed/state_classification_matrix.csv` (56 rows: 50 states + DC + 5 territories) defining each state's delegation pattern and control status. Upserts to `state_classifications` table.

2. **Phase 2 — Census Acquisition** (`phase2_census_acquisition.py`): Downloads TIGER/Line shapefiles via pygris (counties, places, county subdivisions). Caches in `data/cache/` with manifest tracking. Stores in `census_geographies` table.

3. **Phase 3 — Data Processing** (`phase3_data_processing.py`): Filters by state delegation patterns, normalizes names (strips LSAD suffixes), excludes CDPs, handles Virginia independent cities. Per-state transaction boundaries.

4. **Phase 4 — Jurisdiction Assembly** (`phase4_jurisdiction_assembly.py`): Combines classification + geography into final `jurisdictions` table with FIPS breakdown, tier, and jurisdiction type. **Phase 4b — Enrichment** (`phase4b_enrichment.py`): Three enrichment layers — dry/wet status, licensing authority names, and regulatory overrides (from `regulatory_overrides.csv`) — applied per-GEOID.

5. **Phase 5 — Validation & Export** (`phase5_validation_export.py`): Validates completeness, exports CSV/JSON/Parquet to `data/output/`, records audit trail in `pipeline_runs`.

**Key layers:**
- `src/config.py` — Pydantic v2 BaseSettings, loads from `.env.local`
- `src/db/session.py` — `get_session(config)` context manager with auto-commit/rollback
- `src/db/repository.py` — Generic repository with `bulk_upsert()` using PostgreSQL `ON CONFLICT`
- `src/common/enums.py` — Domain enums: `JurisdictionType`, `Tier`, `ControlStatus`, `DelegationPattern`, `DryWetStatus`, `GeoLayer`, `ThreeTierEnforcement`, `ResearchStatus`, `PipelineStatus`
- `src/common/constants.py` — FIPS codes, state data, `VA_INDEPENDENT_CITY_FIPS` set
- `src/models/` — SQLAlchemy 2.0 ORM with `TimestampMixin` (created_at, updated_at). All tables use `schema="jurisdiction"`. Models: `StateClassification`, `CensusGeography`, `Jurisdiction`, `PipelineRun`, `ResearchNote`

## Key Domain Concepts

- **Delegation patterns** define which geographic layers a state delegates licensing to (state-only, county, municipality, MCD, or combinations)
- **Strong MCD states** (CT, ME, MA, MI, MN, NH, NJ, NY, PA, RI, VT, WI) have an additional county_subdivision layer where MCDs have governmental authority
- **Virginia independent cities** are county-equivalents (CLASSFP="C7"), not part of any county — identified via `VA_INDEPENDENT_CITY_FIPS`
- **FIPS coding**: state=2 digits, county=5, place=7, MCD=10. GEOID is the unique constraint key alongside jurisdiction_type and census_year
- **Control vs License states**: Control states run their own stores; license states authorize private retailers

## Seed Data (`data/seed/`)

Committed reference CSVs that drive pipeline behavior:
- `state_classification_matrix.csv` — 56-row state delegation/control matrix
- `dry_wet_status.csv` — GEOIDs of dry jurisdictions
- `licensing_authority_names.csv` — State-level naming templates
- `licensing_authority_overrides.csv` — Per-GEOID verified authority names
- `virginia_independent_cities.csv` — VA city FIPS identifiers
- `regulatory_overrides.csv` — Per-GEOID regulatory field overrides

## Research Tool (`src/research/`)

Semi-automated state research system invoked via `make research`. Key modules: `state_researcher.py` (orchestrator), `nabca_parser.py` (NABCA data), `abc_scraper.py` (ABC website scraping), `authority_name_collector.py`, `dry_wet_collector.py`, `geoid_matcher.py`. Logs evidence to the `ResearchNote` model.

## Database Migrations

Alembic migrations live in `database/migrations/`. Schema uses `jurisdiction` namespace. Run with `make migrate`. Four migrations track the schema evolution from initial tables through enrichment columns, regulatory details, and override source tracking.

## Code Style

- Python 3.9 target
- black formatter, 100-char line length
- ruff linter (E, W, F, I, B, N rules)
- mypy with `disallow_untyped_defs = true`
- Structured logging via `logging.getLogger("jurisdiction.<module>")`

## Environment

Copy `.env.example` to `.env.local`. Key variables: `DATABASE_URL`, `CENSUS_YEAR`, `TIGER_RESOLUTION` (500k or 20m), `SKIP_TERRITORIES`, `INCLUDE_DRY_STATUS`, `INCLUDE_REGULATORY_DETAILS`, `FORCE_REDOWNLOAD`.

## Test Markers

- `@pytest.mark.integration` — requires Docker PostgreSQL
- `@pytest.mark.network` — requires network access
- Deselect with: `pytest -m "not integration"`
