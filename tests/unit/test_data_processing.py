"""Unit tests for Phase 3 data processing logic."""

import pytest

from src.pipeline.phase3_data_processing import normalize_name


class TestNormalizeName:
    def test_strip_county_suffix(self) -> None:
        assert normalize_name("Autauga County", "county") == "Autauga"

    def test_strip_parish_suffix(self) -> None:
        assert normalize_name("Orleans Parish", "county") == "Orleans"

    def test_strip_borough_suffix(self) -> None:
        assert normalize_name("Anchorage Borough", "county") == "Anchorage"

    def test_strip_census_area_suffix(self) -> None:
        assert normalize_name("Bethel Census Area", "county") == "Bethel"

    def test_strip_city_suffix(self) -> None:
        assert normalize_name("Alexandria city", "place") == "Alexandria"

    def test_strip_town_suffix(self) -> None:
        assert normalize_name("Springfield town", "place") == "Springfield"

    def test_strip_village_suffix(self) -> None:
        assert normalize_name("Round Lake village", "place") == "Round Lake"

    def test_strip_township_suffix(self) -> None:
        assert normalize_name("Lower Merion township", "county_subdivision") == "Lower Merion"

    def test_no_suffix_unchanged(self) -> None:
        assert normalize_name("New York", "place") == "New York"

    def test_strip_cdp_suffix(self) -> None:
        assert normalize_name("Levittown CDP", "place") == "Levittown"

    def test_preserves_internal_words(self) -> None:
        # "County" as part of a name, not as suffix
        assert normalize_name("County Club Hills city", "place") == "County Club Hills"

    def test_bare_suffix_unchanged(self) -> None:
        # Edge case: name is just the suffix word without leading space — left unchanged
        result = normalize_name("County", "county")
        assert result == "County"  # No leading space to match " County" suffix
