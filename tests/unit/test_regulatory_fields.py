"""Unit tests for regulatory detail field parsing and seed CSV compatibility."""

from pathlib import Path

import pandas as pd
import pytest

from src.pipeline.phase1_state_classification import (
    _dataframe_to_records,
    _parse_bool_or_none,
    _parse_decimal_or_none,
)


class TestParseBoolOrNone:
    """Test the _parse_bool_or_none helper function."""

    def test_empty_string_returns_none(self) -> None:
        assert _parse_bool_or_none("") is None

    def test_none_returns_none(self) -> None:
        assert _parse_bool_or_none(None) is None

    def test_nan_returns_none(self) -> None:
        assert _parse_bool_or_none("nan") is None

    def test_true_lowercase(self) -> None:
        assert _parse_bool_or_none("true") is True

    def test_true_titlecase(self) -> None:
        assert _parse_bool_or_none("True") is True

    def test_false_lowercase(self) -> None:
        assert _parse_bool_or_none("false") is False

    def test_false_titlecase(self) -> None:
        assert _parse_bool_or_none("False") is False

    def test_yes_returns_true(self) -> None:
        assert _parse_bool_or_none("yes") is True

    def test_no_returns_false(self) -> None:
        assert _parse_bool_or_none("no") is False

    def test_one_returns_true(self) -> None:
        assert _parse_bool_or_none("1") is True

    def test_zero_returns_false(self) -> None:
        assert _parse_bool_or_none("0") is False

    def test_whitespace_returns_none(self) -> None:
        assert _parse_bool_or_none("  ") is None

    def test_garbage_returns_none(self) -> None:
        assert _parse_bool_or_none("maybe") is None


class TestParseDecimalOrNone:
    """Test the _parse_decimal_or_none helper function."""

    def test_empty_string_returns_none(self) -> None:
        assert _parse_decimal_or_none("") is None

    def test_none_returns_none(self) -> None:
        assert _parse_decimal_or_none(None) is None

    def test_nan_returns_none(self) -> None:
        assert _parse_decimal_or_none("nan") is None

    def test_valid_float(self) -> None:
        assert _parse_decimal_or_none("6.0") == 6.0

    def test_valid_integer_string(self) -> None:
        assert _parse_decimal_or_none("12") == 12.0

    def test_valid_decimal(self) -> None:
        result = _parse_decimal_or_none("3.14")
        assert result is not None
        assert abs(result - 3.14) < 0.001

    def test_invalid_string_returns_none(self) -> None:
        assert _parse_decimal_or_none("abc") is None

    def test_whitespace_returns_none(self) -> None:
        assert _parse_decimal_or_none("  ") is None


class TestSeedCSVBackwardCompatibility:
    """Test that seed CSV loading works with and without new regulatory columns."""

    def _make_minimal_csv(self) -> pd.DataFrame:
        """Create a minimal seed DataFrame WITHOUT regulatory columns."""
        from src.common.constants import CONTROL_STATES, FIPS_STATES, STRONG_MCD_STATES

        rows = []
        for fips, (abbr, name) in FIPS_STATES.items():
            rows.append(
                {
                    "state_fips": fips,
                    "state_abbr": abbr,
                    "state_name": name,
                    "is_territory": "false",
                    "control_status": "control" if fips in CONTROL_STATES else "license",
                    "has_local_licensing": "true",
                    "delegates_to_county": "true",
                    "delegates_to_municipality": "false",
                    "delegates_to_mcd": "true" if fips in STRONG_MCD_STATES else "false",
                    "is_strong_mcd_state": "true" if fips in STRONG_MCD_STATES else "false",
                    "has_local_option_law": "false",
                    "research_status": "draft",
                }
            )
        return pd.DataFrame(rows).fillna("")

    def test_records_without_new_columns(self) -> None:
        """Loading a CSV without regulatory columns should produce records with None values."""
        df = self._make_minimal_csv()
        records = _dataframe_to_records(df)
        assert len(records) > 0

        # All regulatory fields should be None
        for record in records:
            assert record["three_tier_enforcement"] is None
            assert record["sunday_sales_allowed"] is None
            assert record["grocery_beer_allowed"] is None
            assert record["grocery_wine_allowed"] is None
            assert record["beer_max_abv"] is None
            assert record["has_on_premise_license"] is None
            assert record["beer_abv_notes"] is None

    def test_records_with_pilot_data(self) -> None:
        """Loading a CSV with regulatory columns should parse pilot state data."""
        df = self._make_minimal_csv()

        # Add regulatory columns with pilot data for TX (index where state_fips == '48')
        regulatory_cols = [
            "three_tier_enforcement", "three_tier_notes",
            "has_on_premise_license", "has_off_premise_license",
            "has_manufacturer_license", "has_distributor_license",
            "sunday_sales_allowed", "sunday_sales_hours", "sunday_sales_notes",
            "grocery_beer_allowed", "grocery_wine_allowed",
            "convenience_beer_allowed", "convenience_wine_allowed",
            "grocery_store_notes", "beer_max_abv", "beer_abv_notes",
        ]
        for col in regulatory_cols:
            df[col] = ""

        # Set TX data
        tx_mask = df["state_fips"] == "48"
        df.loc[tx_mask, "three_tier_enforcement"] = "modified"
        df.loc[tx_mask, "sunday_sales_allowed"] = "True"
        df.loc[tx_mask, "grocery_beer_allowed"] = "True"
        df.loc[tx_mask, "grocery_wine_allowed"] = "True"
        df.loc[tx_mask, "has_on_premise_license"] = "True"

        records = _dataframe_to_records(df)
        tx_record = next(r for r in records if r["state_fips"] == "48")

        assert tx_record["three_tier_enforcement"] == "modified"
        assert tx_record["sunday_sales_allowed"] is True
        assert tx_record["grocery_beer_allowed"] is True
        assert tx_record["grocery_wine_allowed"] is True
        assert tx_record["has_on_premise_license"] is True

        # Non-pilot state should have None
        al_record = next(r for r in records if r["state_fips"] == "01")
        assert al_record["three_tier_enforcement"] is None
        assert al_record["sunday_sales_allowed"] is None


class TestSeedCSVIntegrity:
    """Test the actual seed CSV file for structural integrity."""

    def test_actual_seed_csv_loads(self) -> None:
        """The actual seed CSV should load without errors."""
        csv_path = Path("data/seed/state_classification_matrix.csv")
        if not csv_path.exists():
            pytest.skip("Seed CSV not found")

        df = pd.read_csv(csv_path, dtype=str).fillna("")
        assert len(df) == 56

    def test_actual_seed_csv_has_regulatory_columns(self) -> None:
        """The actual seed CSV should have the new regulatory columns."""
        csv_path = Path("data/seed/state_classification_matrix.csv")
        if not csv_path.exists():
            pytest.skip("Seed CSV not found")

        df = pd.read_csv(csv_path, dtype=str).fillna("")
        expected_cols = [
            "three_tier_enforcement", "sunday_sales_allowed",
            "grocery_beer_allowed", "grocery_wine_allowed", "beer_max_abv",
        ]
        for col in expected_cols:
            assert col in df.columns, f"Missing column: {col}"

    def test_pilot_states_have_data(self) -> None:
        """Pilot states should have non-empty three_tier_enforcement."""
        csv_path = Path("data/seed/state_classification_matrix.csv")
        if not csv_path.exists():
            pytest.skip("Seed CSV not found")

        df = pd.read_csv(csv_path, dtype=str).fillna("")
        pilot_fips = {"04", "12", "34", "42", "48"}

        for fips in pilot_fips:
            row = df[df["state_fips"] == fips].iloc[0]
            assert row["three_tier_enforcement"] != "", (
                f"State {fips} missing three_tier_enforcement"
            )

    def test_non_pilot_states_are_empty(self) -> None:
        """Non-pilot states should have empty regulatory columns."""
        csv_path = Path("data/seed/state_classification_matrix.csv")
        if not csv_path.exists():
            pytest.skip("Seed CSV not found")

        df = pd.read_csv(csv_path, dtype=str).fillna("")
        pilot_fips = {"04", "12", "34", "42", "48"}
        non_pilot = df[~df["state_fips"].isin(pilot_fips)]

        for _, row in non_pilot.iterrows():
            assert row.get("three_tier_enforcement", "") == "", (
                f"Non-pilot state {row['state_fips']} has unexpected three_tier_enforcement"
            )

    def test_dataframe_to_records_with_actual_csv(self) -> None:
        """_dataframe_to_records should work with the actual seed CSV."""
        csv_path = Path("data/seed/state_classification_matrix.csv")
        if not csv_path.exists():
            pytest.skip("Seed CSV not found")

        df = pd.read_csv(csv_path, dtype=str).fillna("")
        records = _dataframe_to_records(df)
        assert len(records) == 56

        # Check TX record
        tx = next(r for r in records if r["state_fips"] == "48")
        assert tx["three_tier_enforcement"] == "modified"
        assert tx["sunday_sales_allowed"] is True
        assert tx["grocery_beer_allowed"] is True

        # Check PA record
        pa = next(r for r in records if r["state_fips"] == "42")
        assert pa["three_tier_enforcement"] == "strict"
        assert pa["grocery_wine_allowed"] is False

        # Check NJ record
        nj = next(r for r in records if r["state_fips"] == "34")
        assert nj["three_tier_enforcement"] == "franchise"
        assert nj["grocery_beer_allowed"] is False
