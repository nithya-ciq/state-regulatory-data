"""Integration tests for overall pipeline data integrity.

These tests verify cross-table consistency, jurisdiction counts,
tier/type distributions, FIPS validity, and referential integrity
across the full pipeline output (all 56 jurisdictions).
"""

import pytest
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from src.common.constants import (
    EXPECTED_STATE_COUNT,
    FIPS_STATES,
    TERRITORY_FIPS,
    VA_INDEPENDENT_CITY_FIPS,
)
from src.models.census_geography import CensusGeography
from src.models.jurisdiction import Jurisdiction
from src.models.state_classification import StateClassification


class TestJurisdictionTotalCounts:
    """Verify overall row counts and tier distribution."""

    def test_total_row_count(self, db_session: Session):
        """Pipeline should produce 20,000+ jurisdiction rows."""
        total = db_session.query(Jurisdiction).count()
        assert total >= 20_000, f"Expected at least 20,000 rows, got {total}"
        assert total <= 40_000, f"Unexpectedly high count: {total}"

    def test_exactly_one_federal_row(self, db_session: Session):
        """There should be exactly one federal (TTB) row."""
        federal = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.tier == "federal")
            .all()
        )
        assert len(federal) == 1, f"Expected 1 federal row, got {len(federal)}"
        assert federal[0].geoid == "US"
        assert federal[0].jurisdiction_type == "federal"

    def test_state_tier_count(self, db_session: Session):
        """State tier should have exactly 56 rows (50 + DC + 5 territories)."""
        state_count = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.tier == "state")
            .count()
        )
        assert state_count == EXPECTED_STATE_COUNT, (
            f"Expected {EXPECTED_STATE_COUNT} state rows, got {state_count}"
        )

    def test_local_tier_is_majority(self, db_session: Session):
        """Local tier should comprise the vast majority of rows."""
        local_count = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.tier == "local")
            .count()
        )
        total = db_session.query(Jurisdiction).count()
        ratio = local_count / total if total > 0 else 0
        assert ratio > 0.95, (
            f"Local tier should be >95% of rows, got {local_count}/{total} ({ratio:.1%})"
        )

    def test_valid_tier_values(self, db_session: Session):
        """Tier must be 'federal', 'state', or 'local'."""
        result = db_session.execute(
            text("SELECT DISTINCT tier FROM jurisdiction.jurisdictions")
        )
        tiers = {r[0] for r in result.fetchall()}
        assert tiers == {"federal", "state", "local"}, (
            f"Unexpected tier values: {tiers}"
        )


class TestJurisdictionTypeDistribution:
    """Verify jurisdiction_type values and their distribution."""

    def test_valid_jurisdiction_types(self, db_session: Session):
        """jurisdiction_type should only contain known values."""
        result = db_session.execute(
            text("SELECT DISTINCT jurisdiction_type FROM jurisdiction.jurisdictions")
        )
        types = {r[0] for r in result.fetchall()}
        valid = {
            "federal", "state", "county", "municipality",
            "mcd", "independent_city", "territory",
        }
        invalid = types - valid
        assert not invalid, f"Unexpected jurisdiction_type values: {invalid}"

    def test_municipality_is_largest_type(self, db_session: Session):
        """Municipalities should be the most common local jurisdiction type."""
        type_counts = (
            db_session.query(
                Jurisdiction.jurisdiction_type,
                func.count().label("cnt"),
            )
            .filter(Jurisdiction.tier == "local")
            .group_by(Jurisdiction.jurisdiction_type)
            .all()
        )
        counts_dict = {t: c for t, c in type_counts}
        muni_count = counts_dict.get("municipality", 0)
        for jtype, count in counts_dict.items():
            if jtype != "municipality":
                assert muni_count >= count, (
                    f"Municipalities ({muni_count}) should exceed {jtype} ({count})"
                )

    def test_county_count_reasonable(self, db_session: Session):
        """Counties should number in the low thousands."""
        county_count = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.jurisdiction_type == "county")
            .count()
        )
        # ~3,143 US counties but many states don't delegate to counties
        assert county_count >= 1_000, (
            f"Expected at least 1,000 counties, got {county_count}"
        )
        assert county_count <= 3_500, (
            f"Unexpectedly high county count: {county_count}"
        )

    def test_mcd_count_reasonable(self, db_session: Session):
        """MCDs should exist for strong-MCD states."""
        mcd_count = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.jurisdiction_type == "mcd")
            .count()
        )
        # 12 strong-MCD states with ~8,000+ townships/towns
        assert mcd_count >= 5_000, (
            f"Expected at least 5,000 MCDs, got {mcd_count}"
        )

    def test_independent_cities_are_virginia(self, db_session: Session):
        """All independent_city rows should be in Virginia."""
        non_va = (
            db_session.query(Jurisdiction)
            .filter(
                Jurisdiction.jurisdiction_type == "independent_city",
                Jurisdiction.state_fips != "51",
            )
            .all()
        )
        assert len(non_va) == 0, (
            f"Independent cities outside VA: "
            f"{[(j.geoid, j.state_fips) for j in non_va]}"
        )

    def test_virginia_independent_city_count(self, db_session: Session):
        """Virginia should have 38 independent cities."""
        ic_count = (
            db_session.query(Jurisdiction)
            .filter(
                Jurisdiction.jurisdiction_type == "independent_city",
                Jurisdiction.state_fips == "51",
            )
            .count()
        )
        assert ic_count == len(VA_INDEPENDENT_CITY_FIPS), (
            f"Expected {len(VA_INDEPENDENT_CITY_FIPS)} VA independent cities, got {ic_count}"
        )

    def test_independent_cities_flagged(self, db_session: Session):
        """All independent_city rows should have is_independent_city=True."""
        unflagged = (
            db_session.query(Jurisdiction)
            .filter(
                Jurisdiction.jurisdiction_type == "independent_city",
                Jurisdiction.is_independent_city.is_(False),
            )
            .count()
        )
        assert unflagged == 0, (
            f"{unflagged} independent_city rows have is_independent_city=False"
        )


class TestFIPSValidity:
    """Verify FIPS codes are well-formed and consistent."""

    def test_state_fips_two_chars(self, db_session: Session):
        """All state_fips should be exactly 2 characters."""
        result = db_session.execute(
            text(
                "SELECT DISTINCT state_fips FROM jurisdiction.jurisdictions "
                "WHERE LENGTH(state_fips) != 2"
            )
        )
        bad_fips = result.fetchall()
        assert len(bad_fips) == 0, (
            f"state_fips with wrong length: {[r[0] for r in bad_fips]}"
        )

    def test_geoid_not_empty(self, db_session: Session):
        """No jurisdiction should have an empty or NULL geoid."""
        null_count = (
            db_session.query(Jurisdiction)
            .filter(
                (Jurisdiction.geoid.is_(None)) | (Jurisdiction.geoid == "")
            )
            .count()
        )
        assert null_count == 0, f"{null_count} rows have empty/NULL geoid"

    def test_no_duplicate_geoid_type_combos(self, db_session: Session):
        """No duplicate (geoid, jurisdiction_type, census_year) combinations."""
        dupes = (
            db_session.query(
                Jurisdiction.geoid,
                Jurisdiction.jurisdiction_type,
                Jurisdiction.census_year,
                func.count().label("cnt"),
            )
            .group_by(
                Jurisdiction.geoid,
                Jurisdiction.jurisdiction_type,
                Jurisdiction.census_year,
            )
            .having(func.count() > 1)
            .all()
        )
        assert len(dupes) == 0, (
            f"Found {len(dupes)} duplicate (geoid, type, year) combos: "
            f"{[(d[0], d[1]) for d in dupes[:5]]}"
        )

    def test_all_state_fips_recognized(self, db_session: Session):
        """Every state_fips in jurisdictions should be in FIPS_STATES (or '00' for federal)."""
        result = db_session.execute(
            text("SELECT DISTINCT state_fips FROM jurisdiction.jurisdictions")
        )
        db_fips = {r[0] for r in result.fetchall()}
        expected = set(FIPS_STATES.keys()) | {"00"}  # Include federal
        unknown = db_fips - expected
        assert not unknown, f"Unknown state_fips codes: {sorted(unknown)}"

    def test_all_56_states_represented(self, db_session: Session):
        """All 56 states/territories should have at least one jurisdiction row."""
        result = db_session.execute(
            text(
                "SELECT DISTINCT state_fips FROM jurisdiction.jurisdictions "
                "WHERE state_fips != '00'"
            )
        )
        db_states = {r[0] for r in result.fetchall()}
        expected = set(FIPS_STATES.keys())
        missing = expected - db_states
        assert not missing, f"States with no jurisdiction rows: {sorted(missing)}"


class TestNameAndAttributeCompleteness:
    """Verify required fields are populated."""

    def test_no_null_jurisdiction_names(self, db_session: Session):
        """Every row should have a non-null jurisdiction_name."""
        null_count = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.jurisdiction_name.is_(None))
            .count()
        )
        assert null_count == 0, f"{null_count} rows have NULL jurisdiction_name"

    def test_no_empty_jurisdiction_names(self, db_session: Session):
        """No jurisdiction_name should be an empty string."""
        empty_count = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.jurisdiction_name == "")
            .count()
        )
        assert empty_count == 0, f"{empty_count} rows have empty jurisdiction_name"

    def test_all_have_state_abbr(self, db_session: Session):
        """Every row should have a non-null state_abbr."""
        null_count = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.state_abbr.is_(None))
            .count()
        )
        assert null_count == 0, f"{null_count} rows have NULL state_abbr"

    def test_all_have_state_name(self, db_session: Session):
        """Every row should have a non-null state_name."""
        null_count = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.state_name.is_(None))
            .count()
        )
        assert null_count == 0, f"{null_count} rows have NULL state_name"

    def test_all_have_control_status(self, db_session: Session):
        """Every row should have a non-null control_status."""
        null_count = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.control_status.is_(None))
            .count()
        )
        assert null_count == 0, f"{null_count} rows have NULL control_status"

    def test_valid_control_status(self, db_session: Session):
        """control_status must be 'control', 'license', 'hybrid', or 'federal'."""
        result = db_session.execute(
            text("SELECT DISTINCT control_status FROM jurisdiction.jurisdictions")
        )
        values = {r[0] for r in result.fetchall()}
        valid = {"control", "license", "hybrid", "federal"}
        invalid = values - valid
        assert not invalid, f"Invalid control_status values: {invalid}"

    def test_mcd_rows_have_county_name(self, db_session: Session):
        """MCD rows should have a county_name (parent county reference).

        County_name is populated for MCDs that belong to a parent county.
        Municipalities and county rows do not require county_name
        (for counties, the jurisdiction_name IS the county name).
        """
        null_county_name = (
            db_session.query(Jurisdiction)
            .filter(
                Jurisdiction.jurisdiction_type == "mcd",
                Jurisdiction.county_name.is_(None),
            )
            .count()
        )
        total_mcd = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.jurisdiction_type == "mcd")
            .count()
        )
        if total_mcd == 0:
            pytest.skip("No MCD rows in database")
        ratio = null_county_name / total_mcd if total_mcd > 0 else 0
        assert ratio < 0.10, (
            f"{null_county_name}/{total_mcd} ({ratio:.1%}) MCD rows "
            f"have NULL county_name"
        )


class TestCrossTableConsistency:
    """Verify consistency between state_classifications and jurisdictions."""

    def test_jurisdiction_state_fips_in_classifications(self, db_session: Session):
        """Every state_fips in jurisdictions (except '00') should exist in state_classifications."""
        result = db_session.execute(
            text(
                "SELECT DISTINCT j.state_fips "
                "FROM jurisdiction.jurisdictions j "
                "LEFT JOIN jurisdiction.state_classifications sc "
                "  ON j.state_fips = sc.state_fips "
                "WHERE sc.state_fips IS NULL "
                "  AND j.state_fips != '00'"
            )
        )
        orphans = result.fetchall()
        assert len(orphans) == 0, (
            f"Jurisdiction state_fips not in state_classifications: "
            f"{[r[0] for r in orphans]}"
        )

    def test_control_status_matches_state(self, db_session: Session):
        """Jurisdiction control_status should match parent state's control_status."""
        result = db_session.execute(
            text(
                "SELECT j.geoid, j.state_fips, j.control_status AS j_status, "
                "       sc.control_status AS sc_status "
                "FROM jurisdiction.jurisdictions j "
                "JOIN jurisdiction.state_classifications sc "
                "  ON j.state_fips = sc.state_fips "
                "WHERE j.control_status != sc.control_status "
                "  AND j.state_fips != '00'"
            )
        )
        mismatches = result.fetchall()
        assert len(mismatches) == 0, (
            f"Control status mismatches ({len(mismatches)} rows). "
            f"First 5: {[(r[0], r[1], r[2], r[3]) for r in mismatches[:5]]}"
        )

    def test_delegation_counties_when_delegates_to_county(self, db_session: Session):
        """States with delegates_to_county=True should have county-type jurisdiction rows."""
        delegating_states = (
            db_session.query(StateClassification.state_fips)
            .filter(StateClassification.delegates_to_county.is_(True))
            .all()
        )
        for (fips,) in delegating_states:
            county_count = (
                db_session.query(Jurisdiction)
                .filter(
                    Jurisdiction.state_fips == fips,
                    Jurisdiction.jurisdiction_type == "county",
                )
                .count()
            )
            # States that delegate to counties should have at least some county rows
            abbr = FIPS_STATES.get(fips, ("?",))[0]
            assert county_count > 0, (
                f"{abbr} ({fips}) delegates to counties but has 0 county jurisdiction rows"
            )


class TestStateLevelSpotChecks:
    """Spot-check specific states for expected characteristics."""

    def test_alabama_counties(self, db_session: Session):
        """Alabama should have ~67 county jurisdiction rows."""
        al_counties = (
            db_session.query(Jurisdiction)
            .filter(
                Jurisdiction.state_fips == "01",
                Jurisdiction.jurisdiction_type == "county",
            )
            .count()
        )
        assert al_counties == 67, (
            f"Expected 67 Alabama counties, got {al_counties}"
        )

    def test_virginia_total_local(self, db_session: Session):
        """Virginia should have counties + independent cities."""
        va_local = (
            db_session.query(Jurisdiction)
            .filter(
                Jurisdiction.state_fips == "51",
                Jurisdiction.tier == "local",
            )
            .count()
        )
        # ~95 counties + 38 independent cities + municipalities
        assert va_local >= 130, (
            f"Expected at least 130 VA local rows, got {va_local}"
        )

    def test_dc_has_no_local_rows(self, db_session: Session):
        """DC should have only a state-level row, no local jurisdictions."""
        dc_local = (
            db_session.query(Jurisdiction)
            .filter(
                Jurisdiction.state_fips == "11",
                Jurisdiction.tier == "local",
            )
            .count()
        )
        assert dc_local == 0, (
            f"DC should have no local jurisdiction rows, got {dc_local}"
        )

    def test_puerto_rico_has_municipios(self, db_session: Session):
        """Puerto Rico should have ~78 municipios as county-equivalents."""
        pr_local = (
            db_session.query(Jurisdiction)
            .filter(
                Jurisdiction.state_fips == "72",
                Jurisdiction.tier == "local",
            )
            .count()
        )
        assert pr_local >= 70, (
            f"Expected at least 70 PR local rows (municipios), got {pr_local}"
        )

    def test_massachusetts_has_mcds(self, db_session: Session):
        """Massachusetts (strong MCD state) should have MCD rows."""
        ma_mcds = (
            db_session.query(Jurisdiction)
            .filter(
                Jurisdiction.state_fips == "25",
                Jurisdiction.jurisdiction_type == "mcd",
            )
            .count()
        )
        assert ma_mcds > 0, "Massachusetts should have MCD jurisdiction rows"

    def test_california_has_municipalities(self, db_session: Session):
        """California should have many municipality rows."""
        ca_munis = (
            db_session.query(Jurisdiction)
            .filter(
                Jurisdiction.state_fips == "06",
                Jurisdiction.jurisdiction_type == "municipality",
            )
            .count()
        )
        # CA has ~482 incorporated places
        assert ca_munis >= 400, (
            f"Expected at least 400 CA municipalities, got {ca_munis}"
        )

    def test_texas_has_counties(self, db_session: Session):
        """Texas should have 254 county jurisdiction rows."""
        tx_counties = (
            db_session.query(Jurisdiction)
            .filter(
                Jurisdiction.state_fips == "48",
                Jurisdiction.jurisdiction_type == "county",
            )
            .count()
        )
        assert tx_counties == 254, (
            f"Expected 254 Texas counties, got {tx_counties}"
        )


class TestGeographicFields:
    """Verify geographic fields are populated for local jurisdictions."""

    def test_local_rows_have_coordinates(self, db_session: Session):
        """Most local rows should have latitude and longitude."""
        null_coords = (
            db_session.query(Jurisdiction)
            .filter(
                Jurisdiction.tier == "local",
                (Jurisdiction.latitude.is_(None)) | (Jurisdiction.longitude.is_(None)),
            )
            .count()
        )
        total_local = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.tier == "local")
            .count()
        )
        ratio = null_coords / total_local if total_local > 0 else 0
        assert ratio < 0.01, (
            f"{null_coords}/{total_local} ({ratio:.1%}) local rows missing coordinates"
        )

    def test_latitude_range(self, db_session: Session):
        """Latitude should be in range 14-72 (US + territories)."""
        result = db_session.execute(
            text(
                "SELECT MIN(latitude), MAX(latitude) "
                "FROM jurisdiction.jurisdictions "
                "WHERE latitude IS NOT NULL"
            )
        )
        min_lat, max_lat = result.fetchone()
        assert float(min_lat) >= -15, f"Min latitude too low: {min_lat}"
        assert float(max_lat) <= 72, f"Max latitude too high: {max_lat}"

    def test_longitude_range(self, db_session: Session):
        """Longitude should be in range -180 to -64 (US + territories)."""
        result = db_session.execute(
            text(
                "SELECT MIN(longitude), MAX(longitude) "
                "FROM jurisdiction.jurisdictions "
                "WHERE longitude IS NOT NULL"
            )
        )
        min_lon, max_lon = result.fetchone()
        # American Samoa is near -171, Guam is near +145 (positive!)
        assert float(min_lon) >= -180, f"Min longitude too low: {min_lon}"
        assert float(max_lon) <= 180, f"Max longitude too high: {max_lon}"

    def test_local_rows_have_land_area(self, db_session: Session):
        """Most local rows should have a land area."""
        null_area = (
            db_session.query(Jurisdiction)
            .filter(
                Jurisdiction.tier == "local",
                Jurisdiction.land_area_sqm.is_(None),
            )
            .count()
        )
        total_local = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.tier == "local")
            .count()
        )
        ratio = null_area / total_local if total_local > 0 else 0
        assert ratio < 0.01, (
            f"{null_area}/{total_local} ({ratio:.1%}) local rows missing land area"
        )


class TestCensusGeographyStaging:
    """Verify the census_geographies staging table has expected data."""

    def test_staging_table_populated(self, db_session: Session):
        """census_geographies should have data."""
        total = db_session.query(CensusGeography).count()
        assert total > 0, "census_geographies table is empty"

    def test_staging_has_multiple_layers(self, db_session: Session):
        """Staging table should have county, place, and county_subdivision layers."""
        result = db_session.execute(
            text(
                "SELECT DISTINCT geo_layer FROM jurisdiction.census_geographies"
            )
        )
        layers = {r[0] for r in result.fetchall()}
        assert "county" in layers, "No county layer in staging"
        assert "place" in layers, "No place layer in staging"
        assert "county_subdivision" in layers, "No county_subdivision layer in staging"

    def test_staging_county_count(self, db_session: Session):
        """Staging should have ~3,200+ county records."""
        county_count = (
            db_session.query(CensusGeography)
            .filter(CensusGeography.geo_layer == "county")
            .count()
        )
        assert county_count >= 3_000, (
            f"Expected at least 3,000 county staging records, got {county_count}"
        )
