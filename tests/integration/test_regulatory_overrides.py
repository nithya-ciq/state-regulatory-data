"""Integration tests for regulatory overrides propagation.

These tests verify that per-GEOID regulatory overrides from
data/seed/regulatory_overrides.csv are correctly applied to jurisdiction
rows, overriding state-level defaults where appropriate.

Requires a running PostgreSQL instance with the pipeline having been
executed for Maryland (state_fips='24').
"""

import pytest
from sqlalchemy.orm import Session

from src.models.jurisdiction import Jurisdiction


@pytest.mark.integration
class TestMontgomeryCountyOverrides:
    """Verify Montgomery County MD (24031) has county-specific overrides."""

    def test_montgomery_control_status(self, db_session: Session):
        """Montgomery County should be 'control' (overrides MD state default of 'license')."""
        row = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.geoid == "24031")
            .first()
        )
        if row is None:
            pytest.skip("Montgomery County MD (24031) not in database")
        assert row.control_status == "control", (
            f"Montgomery County should have control_status='control', "
            f"got '{row.control_status}'"
        )

    def test_montgomery_grocery_wine_disallowed(self, db_session: Session):
        """Montgomery County should have grocery_wine_allowed=False."""
        row = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.geoid == "24031")
            .first()
        )
        if row is None:
            pytest.skip("Montgomery County MD (24031) not in database")
        assert row.grocery_wine_allowed is False, (
            f"Montgomery County should have grocery_wine_allowed=False, "
            f"got {row.grocery_wine_allowed}"
        )

    def test_montgomery_grocery_beer_allowed(self, db_session: Session):
        """Montgomery County should have grocery_beer_allowed=True."""
        row = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.geoid == "24031")
            .first()
        )
        if row is None:
            pytest.skip("Montgomery County MD (24031) not in database")
        assert row.grocery_beer_allowed is True, (
            f"Montgomery County should have grocery_beer_allowed=True, "
            f"got {row.grocery_beer_allowed}"
        )

    def test_montgomery_has_override_source(self, db_session: Session):
        """Montgomery County should have regulatory_override_source set."""
        row = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.geoid == "24031")
            .first()
        )
        if row is None:
            pytest.skip("Montgomery County MD (24031) not in database")
        assert row.regulatory_override_source == "regulatory_overrides_csv", (
            f"Montgomery County should have regulatory_override_source="
            f"'regulatory_overrides_csv', got '{row.regulatory_override_source}'"
        )


@pytest.mark.integration
class TestEasternShoreCountyOverrides:
    """Verify Somerset, Wicomico, Worcester MD counties have wine override."""

    @pytest.mark.parametrize(
        "geoid,county_name",
        [
            ("24039", "Somerset"),
            ("24045", "Wicomico"),
            ("24047", "Worcester"),
        ],
    )
    def test_grocery_wine_disallowed(
        self, db_session: Session, geoid: str, county_name: str
    ):
        """Eastern Shore counties should have grocery_wine_allowed=False."""
        row = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.geoid == geoid)
            .first()
        )
        if row is None:
            pytest.skip(f"{county_name} County MD ({geoid}) not in database")
        assert row.grocery_wine_allowed is False, (
            f"{county_name} County should have grocery_wine_allowed=False, "
            f"got {row.grocery_wine_allowed}"
        )

    @pytest.mark.parametrize(
        "geoid,county_name",
        [
            ("24039", "Somerset"),
            ("24045", "Wicomico"),
            ("24047", "Worcester"),
        ],
    )
    def test_has_override_source(
        self, db_session: Session, geoid: str, county_name: str
    ):
        """Overridden counties should have regulatory_override_source set."""
        row = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.geoid == geoid)
            .first()
        )
        if row is None:
            pytest.skip(f"{county_name} County MD ({geoid}) not in database")
        assert row.regulatory_override_source == "regulatory_overrides_csv", (
            f"{county_name} County should have regulatory_override_source="
            f"'regulatory_overrides_csv', got '{row.regulatory_override_source}'"
        )


@pytest.mark.integration
class TestNonOverriddenCountiesRetainDefaults:
    """Verify non-overridden MD counties keep state defaults."""

    def test_anne_arundel_retains_license(self, db_session: Session):
        """Anne Arundel County (24003) should retain state default control_status."""
        row = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.geoid == "24003")
            .first()
        )
        if row is None:
            pytest.skip("Anne Arundel County MD (24003) not in database")
        assert row.control_status == "license", (
            f"Anne Arundel should retain state default 'license', "
            f"got '{row.control_status}'"
        )

    def test_anne_arundel_no_override_source(self, db_session: Session):
        """Non-overridden county should have NULL regulatory_override_source."""
        row = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.geoid == "24003")
            .first()
        )
        if row is None:
            pytest.skip("Anne Arundel County MD (24003) not in database")
        assert row.regulatory_override_source is None, (
            f"Anne Arundel should have NULL override source, "
            f"got '{row.regulatory_override_source}'"
        )

    def test_baltimore_county_retains_defaults(self, db_session: Session):
        """Baltimore County (24005) should retain state defaults."""
        row = (
            db_session.query(Jurisdiction)
            .filter(Jurisdiction.geoid == "24005")
            .first()
        )
        if row is None:
            pytest.skip("Baltimore County MD (24005) not in database")
        assert row.regulatory_override_source is None, (
            f"Baltimore County should have NULL override source, "
            f"got '{row.regulatory_override_source}'"
        )
