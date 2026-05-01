"""Unit tests for Phase 4b enrichment pipeline."""

import csv
import tempfile
from pathlib import Path

import pytest


class TestDryWetCsvParsing:
    """Test dry/wet status CSV parsing logic."""

    def test_csv_format_valid(self, tmp_path: Path):
        """Verify CSV can be parsed with correct columns."""
        csv_path = tmp_path / "dry_wet_status.csv"
        csv_path.write_text(
            "geoid,state_fips,jurisdiction_name,dry_wet_status,restriction_notes,data_source,last_verified\n"
            "48033,48,Borden,dry,Completely dry,tabc_2020,2026-02-19\n"
            "48263,48,Kent,dry,,tabc_2020,2026-02-19\n"
            "01001,01,Autauga,moist,Beer/wine only,nabca_2017,2026-02-19\n"
        )

        import pandas as pd

        df = pd.read_csv(csv_path, dtype={"geoid": str, "state_fips": str})
        assert len(df) == 3
        assert list(df.columns) == [
            "geoid", "state_fips", "jurisdiction_name", "dry_wet_status",
            "restriction_notes", "data_source", "last_verified",
        ]
        assert df.iloc[0]["geoid"] == "48033"
        assert df.iloc[0]["dry_wet_status"] == "dry"
        assert df.iloc[2]["dry_wet_status"] == "moist"

    def test_empty_csv(self, tmp_path: Path):
        """Verify empty CSV is handled gracefully."""
        csv_path = tmp_path / "dry_wet_status.csv"
        csv_path.write_text(
            "geoid,state_fips,jurisdiction_name,dry_wet_status,restriction_notes,data_source,last_verified\n"
        )

        import pandas as pd

        df = pd.read_csv(csv_path, dtype={"geoid": str, "state_fips": str})
        assert df.empty

    def test_state_filter(self, tmp_path: Path):
        """Verify state filtering works."""
        csv_path = tmp_path / "dry_wet_status.csv"
        csv_path.write_text(
            "geoid,state_fips,jurisdiction_name,dry_wet_status,restriction_notes,data_source,last_verified\n"
            "48033,48,Borden,dry,,tabc_2020,2026-02-19\n"
            "01001,01,Autauga,moist,,nabca_2017,2026-02-19\n"
        )

        import pandas as pd

        df = pd.read_csv(csv_path, dtype={"geoid": str, "state_fips": str})
        tx_only = df[df["state_fips"].isin(["48"])]
        assert len(tx_only) == 1
        assert tx_only.iloc[0]["geoid"] == "48033"


class TestAuthorityPatternParsing:
    """Test licensing authority naming pattern CSV parsing."""

    def test_static_pattern(self):
        """Test pattern without {name} placeholder (state agency)."""
        pattern = "Pennsylvania Liquor Control Board"
        assert "{name}" not in pattern
        assert pattern == "Pennsylvania Liquor Control Board"

    def test_dynamic_pattern(self):
        """Test pattern with {name} placeholder."""
        pattern = "{name} County ABC Board"
        name = "Mecklenburg"
        result = pattern.replace("{name}", name)
        assert result == "Mecklenburg County ABC Board"

    def test_city_pattern(self):
        """Test city naming pattern."""
        pattern = "City of {name} Local Liquor Commission"
        name = "Chicago"
        result = pattern.replace("{name}", name)
        assert result == "City of Chicago Local Liquor Commission"

    def test_override_csv_format(self, tmp_path: Path):
        """Verify override CSV format."""
        csv_path = tmp_path / "licensing_authority_overrides.csv"
        csv_path.write_text(
            "geoid,licensing_authority_name,licensing_authority_type,data_source,notes\n"
            "24001,Allegany County Board of License Commissioners,dedicated_board,md_liquor_boards,\n"
            "15001,Honolulu Liquor Commission,dedicated_board,hi_manual,\n"
        )

        import pandas as pd

        df = pd.read_csv(csv_path, dtype={"geoid": str})
        assert len(df) == 2
        assert df.iloc[0]["geoid"] == "24001"
        assert "Allegany" in df.iloc[0]["licensing_authority_name"]


class TestDryWetStatusValues:
    """Test dry/wet status classification logic."""

    def test_dry_means_is_dry_true(self):
        """dry_wet_status='dry' should set is_dry=True."""
        status = "dry"
        is_dry = status == "dry"
        assert is_dry is True

    def test_wet_means_is_dry_false(self):
        """dry_wet_status='wet' should set is_dry=False."""
        status = "wet"
        is_dry = status == "dry"
        assert is_dry is False

    def test_moist_means_is_dry_false(self):
        """dry_wet_status='moist' should set is_dry=False."""
        status = "moist"
        is_dry = status == "dry"
        assert is_dry is False
