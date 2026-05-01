"""Integration tests for regulatory detail propagation.

These tests verify that regulatory data from the state_classifications
table is correctly propagated to the jurisdictions table for pilot states.

Requires a running PostgreSQL instance with the pipeline fully executed
for at least the pilot states (04, 12, 34, 42, 48).
"""

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.models.jurisdiction import Jurisdiction
from src.models.state_classification import StateClassification


class TestStateClassificationRegulatoryData:
    """Verify regulatory data is stored in state_classifications for pilot states."""

    @pytest.mark.integration
    def test_pilot_states_have_three_tier(self, db_session: Session) -> None:
        """Pilot states should have non-null three_tier_enforcement."""
        pilot_fips = ["04", "12", "34", "42", "48"]
        for fips in pilot_fips:
            row = (
                db_session.query(StateClassification)
                .filter(StateClassification.state_fips == fips)
                .first()
            )
            if row is None:
                pytest.skip(f"State {fips} not in state_classifications — run Phase 1 first")
            assert row.three_tier_enforcement is not None, (
                f"State {fips} missing three_tier_enforcement"
            )

    @pytest.mark.integration
    def test_non_pilot_states_have_null_three_tier(self, db_session: Session) -> None:
        """Non-pilot states should have null three_tier_enforcement."""
        pilot_fips = {"04", "12", "34", "42", "48"}
        non_pilot = (
            db_session.query(StateClassification)
            .filter(~StateClassification.state_fips.in_(pilot_fips))
            .all()
        )
        for row in non_pilot:
            assert row.three_tier_enforcement is None, (
                f"Non-pilot state {row.state_fips} ({row.state_abbr}) "
                f"has unexpected three_tier_enforcement: {row.three_tier_enforcement}"
            )

    @pytest.mark.integration
    def test_tx_classification_values(self, db_session: Session) -> None:
        """Texas should have 'modified' three-tier enforcement."""
        tx = (
            db_session.query(StateClassification)
            .filter(StateClassification.state_fips == "48")
            .first()
        )
        if tx is None:
            pytest.skip("TX not in state_classifications")
        assert tx.three_tier_enforcement == "modified"
        assert tx.sunday_sales_allowed is True
        assert tx.grocery_beer_allowed is True
        assert tx.grocery_wine_allowed is True

    @pytest.mark.integration
    def test_pa_classification_values(self, db_session: Session) -> None:
        """Pennsylvania should have 'strict' enforcement and no grocery wine."""
        pa = (
            db_session.query(StateClassification)
            .filter(StateClassification.state_fips == "42")
            .first()
        )
        if pa is None:
            pytest.skip("PA not in state_classifications")
        assert pa.three_tier_enforcement == "strict"
        assert pa.sunday_sales_allowed is True
        assert pa.grocery_wine_allowed is False

    @pytest.mark.integration
    def test_nj_classification_values(self, db_session: Session) -> None:
        """New Jersey should have 'franchise' enforcement and no grocery beer."""
        nj = (
            db_session.query(StateClassification)
            .filter(StateClassification.state_fips == "34")
            .first()
        )
        if nj is None:
            pytest.skip("NJ not in state_classifications")
        assert nj.three_tier_enforcement == "franchise"
        assert nj.grocery_beer_allowed is False
        assert nj.grocery_wine_allowed is False

    @pytest.mark.integration
    def test_valid_three_tier_values(self, db_session: Session) -> None:
        """three_tier_enforcement should only contain valid enum values."""
        result = db_session.execute(
            text(
                "SELECT DISTINCT three_tier_enforcement "
                "FROM jurisdiction.state_classifications "
                "WHERE three_tier_enforcement IS NOT NULL"
            )
        )
        values = {r[0] for r in result.fetchall()}
        valid = {"strict", "modified", "franchise"}
        invalid = values - valid
        assert not invalid, f"Invalid three_tier_enforcement values: {invalid}"


class TestJurisdictionRegulatoryPropagation:
    """Verify regulatory fields are correctly propagated to jurisdiction rows."""

    @pytest.mark.integration
    def test_tx_jurisdictions_have_modified(self, db_session: Session) -> None:
        """All TX jurisdiction rows should have three_tier_enforcement='modified'."""
        tx_rows = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.state_fips == "48")
            .all()
        )
        if not tx_rows:
            pytest.skip("No TX jurisdiction rows — run pipeline for state 48 first")

        for row in tx_rows:
            assert row.three_tier_enforcement == "modified", (
                f"TX jurisdiction {row.geoid} has three_tier_enforcement="
                f"{row.three_tier_enforcement!r}, expected 'modified'"
            )

    @pytest.mark.integration
    def test_pa_jurisdictions_sunday_sales(self, db_session: Session) -> None:
        """PA jurisdiction rows should have sunday_sales_allowed=True."""
        pa_rows = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.state_fips == "42")
            .all()
        )
        if not pa_rows:
            pytest.skip("No PA jurisdiction rows — run pipeline for state 42 first")

        for row in pa_rows:
            assert row.sunday_sales_allowed is True, (
                f"PA jurisdiction {row.geoid} has sunday_sales_allowed="
                f"{row.sunday_sales_allowed!r}, expected True"
            )

    @pytest.mark.integration
    def test_pa_jurisdictions_no_grocery_wine(self, db_session: Session) -> None:
        """PA jurisdiction rows should have grocery_wine_allowed=False."""
        pa_rows = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.state_fips == "42")
            .all()
        )
        if not pa_rows:
            pytest.skip("No PA jurisdiction rows — run pipeline for state 42 first")

        for row in pa_rows:
            assert row.grocery_wine_allowed is False, (
                f"PA jurisdiction {row.geoid} has grocery_wine_allowed="
                f"{row.grocery_wine_allowed!r}, expected False"
            )

    @pytest.mark.integration
    def test_az_jurisdictions_grocery_beer(self, db_session: Session) -> None:
        """AZ jurisdiction rows should have grocery_beer_allowed=True."""
        az_rows = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.state_fips == "04")
            .all()
        )
        if not az_rows:
            pytest.skip("No AZ jurisdiction rows — run pipeline for state 04 first")

        for row in az_rows:
            assert row.grocery_beer_allowed is True, (
                f"AZ jurisdiction {row.geoid} has grocery_beer_allowed="
                f"{row.grocery_beer_allowed!r}, expected True"
            )

    @pytest.mark.integration
    def test_nj_jurisdictions_no_grocery(self, db_session: Session) -> None:
        """NJ jurisdiction rows should have grocery_beer_allowed=False."""
        nj_rows = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.state_fips == "34")
            .all()
        )
        if not nj_rows:
            pytest.skip("No NJ jurisdiction rows — run pipeline for state 34 first")

        for row in nj_rows:
            assert row.grocery_beer_allowed is False, (
                f"NJ jurisdiction {row.geoid} has grocery_beer_allowed="
                f"{row.grocery_beer_allowed!r}, expected False"
            )

    @pytest.mark.integration
    def test_non_pilot_jurisdictions_null_regulatory(self, db_session: Session) -> None:
        """Non-pilot state jurisdiction rows should have null regulatory fields."""
        # Pick a non-pilot state that should have data (Alabama = 01)
        al_row = (
            db_session.query(Jurisdiction)
            .filter(
                Jurisdiction.state_fips == "01",
                Jurisdiction.jurisdiction_type != "federal",
            )
            .first()
        )
        if al_row is None:
            pytest.skip("No AL jurisdiction rows found")

        assert al_row.three_tier_enforcement is None, (
            f"Non-pilot AL has three_tier_enforcement={al_row.three_tier_enforcement!r}"
        )
        assert al_row.sunday_sales_allowed is None
        assert al_row.grocery_beer_allowed is None

    @pytest.mark.integration
    def test_federal_row_null_regulatory(self, db_session: Session) -> None:
        """Federal (TTB) row should have null regulatory fields."""
        federal = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.jurisdiction_type == "federal")
            .first()
        )
        if federal is None:
            pytest.skip("No federal jurisdiction row found")

        assert federal.three_tier_enforcement is None
        assert federal.sunday_sales_allowed is None
        assert federal.grocery_beer_allowed is None

    @pytest.mark.integration
    def test_pilot_state_count_with_regulatory(self, db_session: Session) -> None:
        """Count of jurisdiction rows with non-null three_tier_enforcement
        should match the total rows for pilot states."""
        pilot_fips = ["04", "12", "34", "42", "48"]
        pilot_total = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.state_fips.in_(pilot_fips))
            .count()
        )
        regulatory_total = (
            db_session.query(Jurisdiction)
            .filter(
                Jurisdiction.state_fips.in_(pilot_fips),
                Jurisdiction.three_tier_enforcement.isnot(None),
            )
            .count()
        )
        if pilot_total == 0:
            pytest.skip("No pilot state jurisdiction rows found")

        assert regulatory_total == pilot_total, (
            f"Expected {pilot_total} rows with regulatory data, got {regulatory_total}. "
            f"Missing: {pilot_total - regulatory_total} rows"
        )
