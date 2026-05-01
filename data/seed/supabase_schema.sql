-- Supabase Star Schema for Jurisdiction Data
-- Project: amber tree
-- Schema: regulations_data
--
-- Run this SQL in Supabase SQL Editor BEFORE the first sync.
-- This creates 4 tables: fact_jurisdictions, dim_states, dim_geography, dim_licensing

-- 1. Create schema
CREATE SCHEMA IF NOT EXISTS regulations_data;

-- 2. Dimension: States (56 rows — state-level rules & regulatory info)
CREATE TABLE IF NOT EXISTS regulations_data.dim_states (
    state_fips              TEXT PRIMARY KEY,
    state_abbr              TEXT NOT NULL,
    state_name              TEXT NOT NULL,
    control_status          TEXT NOT NULL,
    delegation_pattern      TEXT,
    three_tier_enforcement  TEXT,
    sunday_sales_allowed    BOOLEAN,
    grocery_beer_allowed    BOOLEAN,
    grocery_wine_allowed    BOOLEAN,
    beer_max_abv            NUMERIC(4,2),
    grocery_liquor_allowed      BOOLEAN,
    convenience_liquor_allowed  BOOLEAN,
    grocery_beer_confidence     TEXT,
    grocery_wine_confidence     TEXT,
    grocery_liquor_confidence   TEXT,
    retail_channel_notes        TEXT,
    convenience_beer_allowed    BOOLEAN,
    convenience_wine_allowed    BOOLEAN
);

-- 3. Dimension: Geography (location details per jurisdiction)
CREATE TABLE IF NOT EXISTS regulations_data.dim_geography (
    geoid                   TEXT NOT NULL,
    jurisdiction_type       TEXT NOT NULL,
    county_fips             TEXT,
    place_fips              TEXT,
    cousub_fips             TEXT,
    jurisdiction_name       TEXT NOT NULL,
    jurisdiction_name_lsad  TEXT,
    county_name             TEXT,
    land_area_sqm           BIGINT,
    latitude                NUMERIC(10,7),
    longitude               NUMERIC(10,7),
    is_independent_city     BOOLEAN NOT NULL DEFAULT false,
    CONSTRAINT pk_dim_geography PRIMARY KEY (geoid, jurisdiction_type)
);

CREATE INDEX IF NOT EXISTS idx_dim_geo_geoid
    ON regulations_data.dim_geography (geoid);

-- 4. Dimension: Licensing (licensing authority & dry/wet per jurisdiction)
CREATE TABLE IF NOT EXISTS regulations_data.dim_licensing (
    geoid                           TEXT NOT NULL,
    jurisdiction_type               TEXT NOT NULL,
    census_year                     INTEGER NOT NULL,
    has_licensing_authority          BOOLEAN NOT NULL DEFAULT true,
    licensing_authority_name         TEXT,
    licensing_authority_type         TEXT,
    licensing_authority_confidence   TEXT,
    is_dry                          BOOLEAN NOT NULL DEFAULT false,
    dry_wet_status                  TEXT DEFAULT 'wet',
    dry_wet_data_source             TEXT,
    regulatory_override_source      TEXT,
    CONSTRAINT pk_dim_licensing PRIMARY KEY (geoid, jurisdiction_type, census_year)
);

-- 5. Fact: Jurisdictions (core fact table linking dimensions)
CREATE TABLE IF NOT EXISTS regulations_data.fact_jurisdictions (
    geoid               TEXT NOT NULL,
    jurisdiction_type   TEXT NOT NULL,
    census_year         INTEGER NOT NULL,
    state_fips          TEXT NOT NULL REFERENCES regulations_data.dim_states(state_fips),
    tier                TEXT NOT NULL,
    pipeline_run_id     INTEGER,
    data_source         TEXT,
    synced_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_fact_jurisdictions PRIMARY KEY (geoid, jurisdiction_type, census_year)
);

CREATE INDEX IF NOT EXISTS idx_fact_state_fips
    ON regulations_data.fact_jurisdictions (state_fips);
CREATE INDEX IF NOT EXISTS idx_fact_geoid
    ON regulations_data.fact_jurisdictions (geoid);
CREATE INDEX IF NOT EXISTS idx_fact_tier
    ON regulations_data.fact_jurisdictions (tier);

-- 6. Dimension: License Types (per-state license category catalog)
CREATE TABLE IF NOT EXISTS regulations_data.dim_license_types (
    state_fips              TEXT NOT NULL REFERENCES regulations_data.dim_states(state_fips),
    license_type_code       TEXT NOT NULL,
    license_type_name       TEXT NOT NULL,
    license_category        TEXT NOT NULL,
    permits_on_premise      BOOLEAN NOT NULL DEFAULT false,
    permits_off_premise     BOOLEAN NOT NULL DEFAULT false,
    permits_beer            BOOLEAN NOT NULL DEFAULT false,
    permits_wine            BOOLEAN NOT NULL DEFAULT false,
    permits_spirits         BOOLEAN NOT NULL DEFAULT false,
    retail_channel          TEXT,
    abv_limit               NUMERIC(4,2),
    quota_limited           BOOLEAN,
    quota_notes             TEXT,
    transferable            BOOLEAN,
    annual_fee_range        TEXT,
    issuing_authority       TEXT,
    statutory_reference     TEXT,
    notes                   TEXT,
    research_status         TEXT NOT NULL DEFAULT 'pending',
    research_source         TEXT,
    last_verified_date      DATE,
    CONSTRAINT pk_dim_license_types PRIMARY KEY (state_fips, license_type_code)
);

CREATE INDEX IF NOT EXISTS idx_dim_lt_state
    ON regulations_data.dim_license_types (state_fips);
CREATE INDEX IF NOT EXISTS idx_dim_lt_category
    ON regulations_data.dim_license_types (license_category);
CREATE INDEX IF NOT EXISTS idx_dim_lt_channel
    ON regulations_data.dim_license_types (retail_channel);

-- 7. Dimension: Dry/Wet Counties (county-level dry/wet status from seed data)
CREATE TABLE IF NOT EXISTS regulations_data.dim_dry_wet_counties (
    geoid                   TEXT PRIMARY KEY,
    state_fips              TEXT,
    state_abbr              TEXT,
    county_name             TEXT,
    dry_wet_status          TEXT,
    restriction_details     TEXT,
    source                  TEXT,
    last_updated            TEXT
);

-- 8. Layer 2: Municipality License Summary (actual licenses per municipality)
-- Currently NJ only; will expand to other complex states
CREATE TABLE IF NOT EXISTS regulations_data.layer2_municipality_licenses (
    state_fips              TEXT NOT NULL,
    state_abbr              TEXT NOT NULL,
    municipality_name       TEXT NOT NULL,
    county_name             TEXT,
    total_active_licenses   INTEGER DEFAULT 0,
    has_consumption_license BOOLEAN DEFAULT false,
    has_distribution_license BOOLEAN DEFAULT false,
    has_limited_distribution BOOLEAN DEFAULT false,
    has_club_license        BOOLEAN DEFAULT false,
    has_hotel_license       BOOLEAN DEFAULT false,
    consumption_count       INTEGER DEFAULT 0,
    distribution_count      INTEGER DEFAULT 0,
    limited_distribution_count INTEGER DEFAULT 0,
    club_count              INTEGER DEFAULT 0,
    hotel_count             INTEGER DEFAULT 0,
    grocery_can_sell_alcohol BOOLEAN DEFAULT false,
    top_establishments      TEXT,
    data_source             TEXT,
    source_url              TEXT,
    CONSTRAINT pk_layer2_muni PRIMARY KEY (state_fips, municipality_name)
);

CREATE INDEX IF NOT EXISTS idx_layer2_state
    ON regulations_data.layer2_municipality_licenses (state_fips);
CREATE INDEX IF NOT EXISTS idx_layer2_grocery
    ON regulations_data.layer2_municipality_licenses (grocery_can_sell_alcohol)
    WHERE grocery_can_sell_alcohol = true;

-- 9. Layer 2: License Detail (municipality × license type breakdown)
CREATE TABLE IF NOT EXISTS regulations_data.layer2_license_detail (
    state_fips              TEXT NOT NULL,
    state_abbr              TEXT NOT NULL,
    municipality_name       TEXT NOT NULL,
    county_name             TEXT,
    license_type            TEXT NOT NULL,
    active_license_count    INTEGER DEFAULT 0,
    has_grocery_exception   BOOLEAN DEFAULT false,
    has_full_retail         BOOLEAN DEFAULT false,
    has_on_premise          BOOLEAN DEFAULT false,
    sample_establishments   TEXT,
    data_source             TEXT,
    source_url              TEXT,
    CONSTRAINT pk_layer2_detail PRIMARY KEY (state_fips, municipality_name, license_type)
);

CREATE INDEX IF NOT EXISTS idx_layer2_detail_state
    ON regulations_data.layer2_license_detail (state_fips);
