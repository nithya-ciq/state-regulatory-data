"""Unit tests for regulatory overrides CSV parsing and validation."""

from pathlib import Path

import pandas as pd
import pytest

VALID_OVERRIDE_FIELDS = {
    "control_status",
    "three_tier_enforcement",
    "sunday_sales_allowed",
    "grocery_beer_allowed",
    "grocery_wine_allowed",
    "beer_max_abv",
}


class TestRegulatoryOverridesCsvFormat:
    """Test regulatory_overrides.csv parsing and structure."""

    def test_csv_loads_without_errors(self):
        """Verify the actual seed CSV loads correctly."""
        csv_path = Path("data/seed/regulatory_overrides.csv")
        if not csv_path.exists():
            pytest.skip("regulatory_overrides.csv not found")
        df = pd.read_csv(csv_path, dtype={"geoid": str, "state_fips": str})
        assert not df.empty, "regulatory_overrides.csv should not be empty"

    def test_csv_has_expected_columns(self):
        """Verify the CSV has all required columns."""
        csv_path = Path("data/seed/regulatory_overrides.csv")
        if not csv_path.exists():
            pytest.skip("regulatory_overrides.csv not found")
        df = pd.read_csv(csv_path, dtype={"geoid": str, "state_fips": str})
        expected = {
            "geoid", "state_fips", "jurisdiction_name", "override_field",
            "override_value", "override_notes", "data_source", "last_verified",
        }
        assert expected == set(df.columns), (
            f"Column mismatch: expected {expected}, got {set(df.columns)}"
        )

    def test_valid_override_field_values(self):
        """All override_field values should be in the allowed set."""
        csv_path = Path("data/seed/regulatory_overrides.csv")
        if not csv_path.exists():
            pytest.skip("regulatory_overrides.csv not found")
        df = pd.read_csv(csv_path, dtype={"geoid": str, "state_fips": str})
        actual_fields = set(df["override_field"].unique())
        invalid = actual_fields - VALID_OVERRIDE_FIELDS
        assert not invalid, f"Invalid override_field values: {invalid}"

    def test_geoids_are_5_digit_county_codes(self):
        """All GEOIDs in the CSV should be 5-digit county FIPS codes."""
        csv_path = Path("data/seed/regulatory_overrides.csv")
        if not csv_path.exists():
            pytest.skip("regulatory_overrides.csv not found")
        df = pd.read_csv(csv_path, dtype={"geoid": str, "state_fips": str})
        for geoid in df["geoid"].unique():
            assert len(str(geoid)) == 5, f"GEOID {geoid} is not 5 digits"
            assert str(geoid).isdigit(), f"GEOID {geoid} is not numeric"

    def test_state_fips_matches_geoid_prefix(self):
        """state_fips should match the first 2 digits of the GEOID."""
        csv_path = Path("data/seed/regulatory_overrides.csv")
        if not csv_path.exists():
            pytest.skip("regulatory_overrides.csv not found")
        df = pd.read_csv(csv_path, dtype={"geoid": str, "state_fips": str})
        for _, row in df.iterrows():
            geoid_prefix = str(row["geoid"])[:2]
            assert geoid_prefix == str(row["state_fips"]), (
                f"state_fips {row['state_fips']} does not match "
                f"GEOID prefix {geoid_prefix} for GEOID {row['geoid']}"
            )


class TestRegulatoryOverridesCsvParsing:
    """Test CSV parsing with synthetic data."""

    def test_csv_format_valid(self, tmp_path: Path):
        """Verify a synthetic CSV can be parsed with correct columns."""
        csv_path = tmp_path / "regulatory_overrides.csv"
        csv_path.write_text(
            "geoid,state_fips,jurisdiction_name,override_field,override_value,"
            "override_notes,data_source,last_verified\n"
            "24031,24,Montgomery,control_status,control,"
            "County operates DLC,md_dlc,2026-02-27\n"
            "24031,24,Montgomery,grocery_wine_allowed,false,"
            "Wine at county stores only,md_dlc,2026-02-27\n"
        )

        df = pd.read_csv(csv_path, dtype={"geoid": str, "state_fips": str})
        assert len(df) == 2
        assert df.iloc[0]["geoid"] == "24031"
        assert df.iloc[0]["override_field"] == "control_status"
        assert df.iloc[0]["override_value"] == "control"
        assert df.iloc[1]["override_field"] == "grocery_wine_allowed"
        assert df.iloc[1]["override_value"] == "false"

    def test_empty_csv_handled(self, tmp_path: Path):
        """Verify empty CSV is handled gracefully."""
        csv_path = tmp_path / "regulatory_overrides.csv"
        csv_path.write_text(
            "geoid,state_fips,jurisdiction_name,override_field,override_value,"
            "override_notes,data_source,last_verified\n"
        )

        df = pd.read_csv(csv_path, dtype={"geoid": str, "state_fips": str})
        assert df.empty

    def test_state_filter(self, tmp_path: Path):
        """Verify state filtering works correctly."""
        csv_path = tmp_path / "regulatory_overrides.csv"
        csv_path.write_text(
            "geoid,state_fips,jurisdiction_name,override_field,override_value,"
            "override_notes,data_source,last_verified\n"
            "24031,24,Montgomery,control_status,control,,md_dlc,2026-02-27\n"
            "48033,48,Borden,grocery_beer_allowed,false,,tx_tabc,2026-02-27\n"
        )

        df = pd.read_csv(csv_path, dtype={"geoid": str, "state_fips": str})
        md_only = df[df["state_fips"].isin(["24"])]
        assert len(md_only) == 1
        assert md_only.iloc[0]["geoid"] == "24031"


class TestBooleanOverrideParsing:
    """Test that boolean override values parse correctly."""

    def test_true_values(self):
        """'true', '1', 'yes' should all parse as True."""
        from src.pipeline.phase1_state_classification import _parse_bool_or_none

        assert _parse_bool_or_none("true") is True
        assert _parse_bool_or_none("True") is True
        assert _parse_bool_or_none("1") is True
        assert _parse_bool_or_none("yes") is True

    def test_false_values(self):
        """'false', '0', 'no' should all parse as False."""
        from src.pipeline.phase1_state_classification import _parse_bool_or_none

        assert _parse_bool_or_none("false") is False
        assert _parse_bool_or_none("False") is False
        assert _parse_bool_or_none("0") is False
        assert _parse_bool_or_none("no") is False

    def test_none_values(self):
        """Empty, nan, None should all parse as None."""
        from src.pipeline.phase1_state_classification import _parse_bool_or_none

        assert _parse_bool_or_none(None) is None
        assert _parse_bool_or_none("") is None
        assert _parse_bool_or_none("nan") is None


class TestNumericOverrideParsing:
    """Test that numeric override values parse correctly."""

    def test_valid_abv(self):
        """Valid ABV strings should parse to floats."""
        from src.pipeline.phase1_state_classification import _parse_decimal_or_none

        assert _parse_decimal_or_none("6.0") == 6.0
        assert _parse_decimal_or_none("3.2") == 3.2

    def test_none_values(self):
        """Empty/None should parse as None."""
        from src.pipeline.phase1_state_classification import _parse_decimal_or_none

        assert _parse_decimal_or_none(None) is None
        assert _parse_decimal_or_none("") is None
