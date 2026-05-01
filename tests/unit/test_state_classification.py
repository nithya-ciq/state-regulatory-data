"""Unit tests for state classification validation logic."""

import io

import pandas as pd
import pytest

from src.pipeline.phase1_state_classification import _validate_seed


def _make_seed_df(overrides: dict = None) -> pd.DataFrame:
    """Create a minimal valid seed DataFrame for testing."""
    from src.common.constants import FIPS_STATES, CONTROL_STATES, STRONG_MCD_STATES

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

    df = pd.DataFrame(rows)

    if overrides:
        for key, value in overrides.items():
            if key in df.columns:
                df[key] = value

    return df


class TestValidateSeed:
    def test_valid_seed_passes(self) -> None:
        df = _make_seed_df()
        errors = _validate_seed(df)
        assert len(errors) == 0

    def test_missing_column(self) -> None:
        df = _make_seed_df()
        df = df.drop(columns=["control_status"])
        errors = _validate_seed(df)
        assert len(errors) > 0
        assert any("Missing required columns" in e for e in errors)

    def test_missing_state(self) -> None:
        df = _make_seed_df()
        df = df[df["state_fips"] != "06"]  # Remove California
        errors = _validate_seed(df)
        assert len(errors) > 0
        assert any("Missing state FIPS" in e for e in errors)

    def test_invalid_control_status(self) -> None:
        df = _make_seed_df()
        df.loc[df["state_fips"] == "01", "control_status"] = "invalid_value"
        errors = _validate_seed(df)
        assert any("Invalid control_status" in e for e in errors)

    def test_mcd_consistency_error(self) -> None:
        df = _make_seed_df()
        # Set delegates_to_mcd=true for a non-MCD state
        df.loc[df["state_fips"] == "01", "delegates_to_mcd"] = "true"
        errors = _validate_seed(df)
        assert any("delegates_to_mcd=true but not a strong-MCD state" in e for e in errors)

    def test_extra_fips_detected(self) -> None:
        df = _make_seed_df()
        extra = pd.DataFrame(
            [
                {
                    "state_fips": "99",
                    "state_abbr": "XX",
                    "state_name": "Unknown",
                    "control_status": "license",
                    "has_local_licensing": "false",
                    "delegates_to_county": "false",
                    "delegates_to_municipality": "false",
                    "delegates_to_mcd": "false",
                    "is_strong_mcd_state": "false",
                    "has_local_option_law": "false",
                    "research_status": "pending",
                }
            ]
        )
        df = pd.concat([df, extra], ignore_index=True)
        errors = _validate_seed(df)
        assert any("Unknown state FIPS" in e for e in errors)
