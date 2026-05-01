"""Integration tests for Phase 1: State Classification.

These tests verify that the state_classifications table is correctly
populated from the seed CSV and that all 56 jurisdictions are present
with the expected attributes.
"""

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.common.constants import FIPS_STATES, STRONG_MCD_STATES, TERRITORY_FIPS
from src.models.state_classification import StateClassification


class TestStateClassificationCompleteness:
    """Verify all 56 states/territories are present with correct metadata."""

    def test_all_56_jurisdictions_present(self, db_session: Session):
        """The table should have exactly 56 rows (50 states + DC + 5 territories)."""
        count = db_session.query(StateClassification).count()
        assert count == 56, f"Expected 56 state classifications, got {count}"

    def test_all_fips_codes_present(self, db_session: Session):
        """Every FIPS code in constants.FIPS_STATES should have a row."""
        rows = db_session.query(StateClassification.state_fips).all()
        db_fips = {r[0] for r in rows}
        expected_fips = set(FIPS_STATES.keys())
        missing = expected_fips - db_fips
        extra = db_fips - expected_fips
        assert not missing, f"Missing FIPS codes: {sorted(missing)}"
        assert not extra, f"Unexpected FIPS codes: {sorted(extra)}"

    def test_no_null_state_abbr(self, db_session: Session):
        """Every row must have a non-null state_abbr."""
        result = db_session.execute(
            text(
                "SELECT state_fips FROM jurisdiction.state_classifications "
                "WHERE state_abbr IS NULL"
            )
        )
        nulls = result.fetchall()
        assert len(nulls) == 0, f"Rows with NULL state_abbr: {nulls}"

    def test_territories_flagged(self, db_session: Session):
        """The 5 territories should have is_territory=True."""
        territories = (
            db_session.query(StateClassification)
            .filter(StateClassification.is_territory.is_(True))
            .all()
        )
        territory_fips = {t.state_fips for t in territories}
        assert territory_fips == set(TERRITORY_FIPS), (
            f"Expected territory FIPS {TERRITORY_FIPS}, got {territory_fips}"
        )

    def test_dc_is_not_territory(self, db_session: Session):
        """DC (11) should NOT be flagged as a territory."""
        dc = db_session.get(StateClassification, "11")
        assert dc is not None, "DC (11) not found"
        assert dc.is_territory is False, "DC should not be a territory"
        assert dc.state_abbr == "DC"


class TestControlStatusValues:
    """Verify control_status is consistently populated."""

    def test_valid_control_status_values(self, db_session: Session):
        """All control_status values must be 'control', 'license', or 'hybrid'."""
        rows = db_session.query(StateClassification.control_status).distinct().all()
        values = {r[0] for r in rows}
        valid = {"control", "license", "hybrid"}
        invalid = values - valid
        assert not invalid, f"Invalid control_status values: {invalid}"

    def test_known_control_states(self, db_session: Session):
        """Spot-check known control states."""
        known_control = ["42", "49", "51"]  # PA, UT, VA
        for fips in known_control:
            row = db_session.get(StateClassification, fips)
            assert row is not None, f"Missing state {fips}"
            assert row.control_status == "control", (
                f"{row.state_abbr} ({fips}) should be 'control', got '{row.control_status}'"
            )

    def test_known_license_states(self, db_session: Session):
        """Spot-check known license states."""
        known_license = ["06", "48", "36"]  # CA, TX, NY
        for fips in known_license:
            row = db_session.get(StateClassification, fips)
            assert row is not None, f"Missing state {fips}"
            assert row.control_status == "license", (
                f"{row.state_abbr} ({fips}) should be 'license', got '{row.control_status}'"
            )


class TestDelegationPatterns:
    """Verify delegation flags are consistent."""

    def test_strong_mcd_states_match_constants(self, db_session: Session):
        """States with is_strong_mcd_state=True should match STRONG_MCD_STATES."""
        rows = (
            db_session.query(StateClassification)
            .filter(StateClassification.is_strong_mcd_state.is_(True))
            .all()
        )
        db_mcd = {r.state_fips for r in rows}
        assert db_mcd == set(STRONG_MCD_STATES), (
            f"DB strong-MCD states {db_mcd} != constants {STRONG_MCD_STATES}"
        )

    def test_delegates_to_mcd_implies_strong_mcd(self, db_session: Session):
        """If delegates_to_mcd is True, is_strong_mcd_state must also be True."""
        violations = (
            db_session.query(StateClassification)
            .filter(
                StateClassification.delegates_to_mcd.is_(True),
                StateClassification.is_strong_mcd_state.is_(False),
            )
            .all()
        )
        assert len(violations) == 0, (
            f"States with delegates_to_mcd=True but is_strong_mcd_state=False: "
            f"{[v.state_abbr for v in violations]}"
        )

    def test_no_licensing_means_no_delegation(self, db_session: Session):
        """States with has_local_licensing=False should have all delegation flags False."""
        rows = (
            db_session.query(StateClassification)
            .filter(StateClassification.has_local_licensing.is_(False))
            .all()
        )
        for row in rows:
            assert not row.delegates_to_county, (
                f"{row.state_abbr}: has_local_licensing=False but delegates_to_county=True"
            )
            assert not row.delegates_to_municipality, (
                f"{row.state_abbr}: has_local_licensing=False but delegates_to_municipality=True"
            )
            assert not row.delegates_to_mcd, (
                f"{row.state_abbr}: has_local_licensing=False but delegates_to_mcd=True"
            )

    def test_at_least_one_delegation_when_local_licensing(self, db_session: Session):
        """States with has_local_licensing=True should delegate to at least one level.

        Known exceptions where has_local_licensing=True but no delegation flags:
        - Montana (30): state-managed with local input
        - Delaware (10): state-managed (small state, no delegation)
        - DC (11): single jurisdiction, acts as both state and local
        """
        known_exceptions = {"30", "10", "11"}  # MT, DE, DC
        rows = (
            db_session.query(StateClassification)
            .filter(StateClassification.has_local_licensing.is_(True))
            .all()
        )
        for row in rows:
            if row.state_fips in known_exceptions:
                continue
            has_any = (
                row.delegates_to_county
                or row.delegates_to_municipality
                or row.delegates_to_mcd
            )
            assert has_any, (
                f"{row.state_abbr} ({row.state_fips}): has_local_licensing=True "
                f"but no delegation flags set"
            )


class TestLocalOptionLaw:
    """Verify local option law fields."""

    def test_local_option_level_values(self, db_session: Session):
        """local_option_level should be county, municipality, parish, or NULL."""
        valid_levels = {"county", "municipality", "parish", None}
        rows = (
            db_session.query(StateClassification.state_abbr, StateClassification.local_option_level)
            .filter(StateClassification.has_local_option_law.is_(True))
            .all()
        )
        for abbr, level in rows:
            assert level in valid_levels, (
                f"{abbr}: invalid local_option_level '{level}'"
            )

    def test_local_option_level_null_when_no_law(self, db_session: Session):
        """If has_local_option_law is False, local_option_level should be NULL."""
        violations = (
            db_session.query(StateClassification)
            .filter(
                StateClassification.has_local_option_law.is_(False),
                StateClassification.local_option_level.isnot(None),
            )
            .all()
        )
        assert len(violations) == 0, (
            f"States with no local option law but non-null level: "
            f"{[v.state_abbr for v in violations]}"
        )


class TestResearchStatus:
    """Verify research provenance fields."""

    def test_all_have_research_status(self, db_session: Session):
        """Every row should have a non-null research_status."""
        result = db_session.execute(
            text(
                "SELECT COUNT(*) FROM jurisdiction.state_classifications "
                "WHERE research_status IS NULL"
            )
        )
        assert result.scalar() == 0, "Some rows have NULL research_status"

    def test_valid_research_status_values(self, db_session: Session):
        """research_status must be 'draft', 'verified', or 'pending'."""
        result = db_session.execute(
            text(
                "SELECT DISTINCT research_status FROM jurisdiction.state_classifications"
            )
        )
        values = {r[0] for r in result.fetchall()}
        valid = {"draft", "verified", "pending"}
        invalid = values - valid
        assert not invalid, f"Invalid research_status values: {invalid}"
