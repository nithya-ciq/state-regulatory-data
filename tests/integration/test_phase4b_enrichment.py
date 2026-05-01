"""Integration tests for Phase 4b: Enrichment (dry/wet status + licensing authority names).

These tests verify that the enrichment overlay has been correctly applied
to the jurisdictions table — dry/wet status from seed CSV and licensing
authority names from patterns + overrides.
"""

import pytest
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from src.models.jurisdiction import Jurisdiction


class TestDryWetStatusEnrichment:
    """Verify dry/wet status is correctly applied from seed CSV."""

    def test_dry_count_matches_seed(self, db_session: Session):
        """Number of is_dry=True rows should match the seed CSV dry entries."""
        dry_count = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.is_dry.is_(True))
            .count()
        )
        # Seed CSV has 105 dry entries; some GEOIDs may not exist in jurisdictions
        # (e.g., SD tribal counties). We expect at least 90 dry rows.
        assert dry_count >= 90, f"Expected at least 90 dry jurisdictions, got {dry_count}"
        assert dry_count <= 110, f"Unexpected high dry count: {dry_count}"

    def test_moist_count_reasonable(self, db_session: Session):
        """Number of moist rows should match seed CSV moist entries."""
        moist_count = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.dry_wet_status == "moist")
            .count()
        )
        # Seed CSV has 193 moist entries
        assert moist_count >= 180, f"Expected at least 180 moist jurisdictions, got {moist_count}"
        assert moist_count <= 200, f"Unexpected high moist count: {moist_count}"

    def test_is_dry_consistent_with_dry_wet_status(self, db_session: Session):
        """is_dry=True should only exist when dry_wet_status='dry'."""
        violations = (
            db_session.query(Jurisdiction)
            .filter(
                Jurisdiction.is_dry.is_(True),
                Jurisdiction.dry_wet_status != "dry",
            )
            .all()
        )
        assert len(violations) == 0, (
            f"Rows with is_dry=True but dry_wet_status!='dry': "
            f"{[(v.geoid, v.dry_wet_status) for v in violations[:5]]}"
        )

    def test_dry_status_false_for_wet(self, db_session: Session):
        """is_dry=False for wet and moist rows."""
        violations = (
            db_session.query(Jurisdiction)
            .filter(
                Jurisdiction.is_dry.is_(False),
                Jurisdiction.dry_wet_status == "dry",
            )
            .all()
        )
        assert len(violations) == 0, (
            f"Rows with is_dry=False but dry_wet_status='dry': "
            f"{[(v.geoid, v.dry_wet_status) for v in violations[:5]]}"
        )

    def test_valid_dry_wet_status_values(self, db_session: Session):
        """dry_wet_status should only be 'dry', 'wet', or 'moist'."""
        result = db_session.execute(
            text(
                "SELECT DISTINCT dry_wet_status FROM jurisdiction.jurisdictions "
                "WHERE dry_wet_status IS NOT NULL"
            )
        )
        values = {r[0] for r in result.fetchall()}
        valid = {"dry", "wet", "moist"}
        invalid = values - valid
        assert not invalid, f"Invalid dry_wet_status values: {invalid}"

    def test_non_wet_rows_have_data_source(self, db_session: Session):
        """Every dry or moist row should have a dry_wet_data_source."""
        violations = (
            db_session.query(Jurisdiction)
            .filter(
                Jurisdiction.dry_wet_status.in_(["dry", "moist"]),
                Jurisdiction.dry_wet_data_source.is_(None),
            )
            .all()
        )
        assert len(violations) == 0, (
            f"Dry/moist rows without data_source: "
            f"{[(v.geoid, v.dry_wet_status) for v in violations[:5]]}"
        )


class TestDryWetSpotChecks:
    """Spot-check known dry/wet jurisdictions against seed data."""

    def test_texas_borden_county_is_dry(self, db_session: Session):
        """Borden County TX (48033) should be dry."""
        row = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.geoid == "48033")
            .first()
        )
        assert row is not None, "Borden County TX (48033) not found"
        assert row.is_dry is True, f"Borden should be dry, got is_dry={row.is_dry}"
        assert row.dry_wet_status == "dry"

    def test_texas_kent_county_is_dry(self, db_session: Session):
        """Kent County TX (48263) should be dry."""
        row = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.geoid == "48263")
            .first()
        )
        assert row is not None, "Kent County TX (48263) not found"
        assert row.is_dry is True

    def test_texas_roberts_county_is_dry(self, db_session: Session):
        """Roberts County TX (48393) should be dry."""
        row = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.geoid == "48393")
            .first()
        )
        assert row is not None, "Roberts County TX (48393) not found"
        assert row.is_dry is True
        assert row.dry_wet_data_source == "tabc_2020"

    def test_tennessee_hancock_is_dry(self, db_session: Session):
        """Hancock County TN (47033 is Crockett — use 47067 for Hancock).

        Hancock County TN (47067) should be dry per seed CSV.
        """
        row = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.geoid == "47067")
            .first()
        )
        if row is not None:
            # Hancock is in a state that may or may not delegate to counties
            assert row.dry_wet_status == "dry", (
                f"Hancock TN should be dry, got '{row.dry_wet_status}'"
            )

    def test_alabama_bibb_county_is_moist(self, db_session: Session):
        """Bibb County AL (01007) should be moist."""
        row = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.geoid == "01007")
            .first()
        )
        if row is not None:
            assert row.dry_wet_status == "moist", (
                f"Bibb AL should be moist, got '{row.dry_wet_status}'"
            )
            assert row.is_dry is False

    def test_dry_wet_states_coverage(self, db_session: Session):
        """Dry/wet enrichment should cover at least 10 states."""
        result = db_session.execute(
            text(
                "SELECT DISTINCT state_fips FROM jurisdiction.jurisdictions "
                "WHERE dry_wet_status IN ('dry', 'moist')"
            )
        )
        states = {r[0] for r in result.fetchall()}
        assert len(states) >= 10, (
            f"Expected dry/wet data in at least 10 states, got {len(states)}: {states}"
        )


class TestLicensingAuthorityNameCoverage:
    """Verify licensing authority names are populated."""

    def test_100_percent_authority_name_coverage(self, db_session: Session):
        """Every jurisdiction row should have a licensing_authority_name."""
        null_count = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.licensing_authority_name.is_(None))
            .count()
        )
        assert null_count == 0, (
            f"{null_count} jurisdiction rows have NULL licensing_authority_name"
        )

    def test_authority_names_non_empty(self, db_session: Session):
        """Authority names should not be empty strings."""
        empty_count = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.licensing_authority_name == "")
            .count()
        )
        assert empty_count == 0, (
            f"{empty_count} jurisdiction rows have empty licensing_authority_name"
        )

    def test_authority_name_uniqueness_reasonable(self, db_session: Session):
        """There should be many unique authority names (not all the same)."""
        result = db_session.execute(
            text(
                "SELECT COUNT(DISTINCT licensing_authority_name) "
                "FROM jurisdiction.jurisdictions"
            )
        )
        unique_count = result.scalar()
        total_count = db_session.query(Jurisdiction).count()
        # At least 50% should be unique (state-level rows share names)
        ratio = unique_count / total_count if total_count > 0 else 0
        assert ratio > 0.5, (
            f"Only {unique_count} unique authority names out of {total_count} "
            f"({ratio:.1%}) — too few"
        )


class TestLicensingAuthorityConfidence:
    """Verify authority confidence classifications are consistent."""

    def test_valid_confidence_values(self, db_session: Session):
        """licensing_authority_confidence should be 'verified', 'generated', or NULL."""
        result = db_session.execute(
            text(
                "SELECT DISTINCT licensing_authority_confidence "
                "FROM jurisdiction.jurisdictions "
                "WHERE licensing_authority_confidence IS NOT NULL"
            )
        )
        values = {r[0] for r in result.fetchall()}
        valid = {"verified", "generated", "unknown"}
        invalid = values - valid
        assert not invalid, f"Invalid confidence values: {invalid}"

    def test_verified_count_matches_overrides(self, db_session: Session):
        """Verified rows should correspond to per-GEOID overrides."""
        verified_count = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.licensing_authority_confidence == "verified")
            .count()
        )
        # We have 29 per-GEOID overrides: 4 Hawaii + 25 Maryland (24 counties + Baltimore City)
        # Plus state/federal rows might not have confidence set
        assert verified_count >= 25, (
            f"Expected at least 25 verified authority names (29 overrides), "
            f"got {verified_count}"
        )

    def test_generated_is_majority(self, db_session: Session):
        """Most authority names should be generated (from state patterns)."""
        generated = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.licensing_authority_confidence == "generated")
            .count()
        )
        total = db_session.query(Jurisdiction).count()
        ratio = generated / total if total > 0 else 0
        assert ratio > 0.8, (
            f"Expected >80% generated confidence, got {generated}/{total} ({ratio:.1%})"
        )


class TestLicensingAuthorityType:
    """Verify authority type classifications."""

    def test_valid_authority_type_values(self, db_session: Session):
        """licensing_authority_type should be a valid classification."""
        result = db_session.execute(
            text(
                "SELECT DISTINCT licensing_authority_type "
                "FROM jurisdiction.jurisdictions "
                "WHERE licensing_authority_type IS NOT NULL"
            )
        )
        values = {r[0] for r in result.fetchall()}
        valid = {"dedicated_board", "general_government", "state_agency"}
        invalid = values - valid
        assert not invalid, f"Invalid authority type values: {invalid}"

    def test_state_agency_for_control_state_rows(self, db_session: Session):
        """State-tier rows in control states should use state_agency type."""
        # Control states where the state IS the authority (PA, UT, VA, etc.)
        state_agency_count = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.licensing_authority_type == "state_agency")
            .count()
        )
        # Several states use state_agency type
        assert state_agency_count > 0, "No rows with state_agency authority type"

    def test_dedicated_board_exists(self, db_session: Session):
        """Some states (NC, MD) should have dedicated_board entries."""
        board_count = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.licensing_authority_type == "dedicated_board")
            .count()
        )
        assert board_count > 0, "No rows with dedicated_board authority type"

    def test_general_government_is_majority(self, db_session: Session):
        """Most local jurisdictions should have general_government authority type."""
        gg_count = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.licensing_authority_type == "general_government")
            .count()
        )
        local_count = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.tier == "local")
            .count()
        )
        ratio = gg_count / local_count if local_count > 0 else 0
        assert ratio > 0.7, (
            f"Expected >70% general_government in local tier, "
            f"got {gg_count}/{local_count} ({ratio:.1%})"
        )


class TestAuthorityNameOverrides:
    """Spot-check specific per-GEOID authority name overrides."""

    def test_hawaii_honolulu_override(self, db_session: Session):
        """Honolulu (15003) should have the specific override name."""
        row = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.geoid == "15003")
            .first()
        )
        if row is not None:
            assert row.licensing_authority_confidence == "verified", (
                f"Honolulu should be verified, got '{row.licensing_authority_confidence}'"
            )
            assert "Liquor" in row.licensing_authority_name or "liquor" in row.licensing_authority_name, (
                f"Honolulu authority name unexpected: '{row.licensing_authority_name}'"
            )

    def test_maryland_overrides_are_verified(self, db_session: Session):
        """Maryland per-GEOID overrides should all be verified confidence."""
        md_verified = (
            db_session.query(Jurisdiction)
            .filter(
                Jurisdiction.state_fips == "24",
                Jurisdiction.licensing_authority_confidence == "verified",
            )
            .count()
        )
        # 24 Maryland counties + Baltimore City = 25 overrides
        # (not all may be in overrides CSV — at least most)
        assert md_verified >= 20, (
            f"Expected at least 20 verified MD authority names, got {md_verified}"
        )

    def test_hawaii_verified_count(self, db_session: Session):
        """Hawaii should have 5 verified authority names: state-level + 4 counties."""
        hi_verified = (
            db_session.query(Jurisdiction)
            .filter(
                Jurisdiction.state_fips == "15",
                Jurisdiction.licensing_authority_confidence == "verified",
            )
            .count()
        )
        # 4 county overrides + 1 state-level row = 5
        assert hi_verified == 5, (
            f"Expected 5 verified Hawaii authority names (state + 4 counties), "
            f"got {hi_verified}"
        )
