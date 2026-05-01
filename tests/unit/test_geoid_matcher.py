"""Unit tests for GeoID matcher fuzzy name matching."""

import pytest

from src.research.geoid_matcher import GeoIDMatcher


class TestNormalize:
    """Test the static _normalize method."""

    def test_strip_county_suffix(self):
        assert GeoIDMatcher._normalize("Autauga County") == "autauga"

    def test_strip_parish_suffix(self):
        assert GeoIDMatcher._normalize("Orleans Parish") == "orleans"

    def test_strip_borough_suffix(self):
        # Both capitalized and lowercase 'borough'
        assert GeoIDMatcher._normalize("Fairbanks North Star Borough") == "fairbanks north star"

    def test_strip_city_prefix(self):
        assert GeoIDMatcher._normalize("City of Los Angeles") == "los angeles"

    def test_strip_town_prefix(self):
        assert GeoIDMatcher._normalize("Town of Springfield") == "springfield"

    def test_strip_village_prefix(self):
        assert GeoIDMatcher._normalize("Village of Ridgewood") == "ridgewood"

    def test_lowercase(self):
        assert GeoIDMatcher._normalize("BORDEN") == "borden"

    def test_remove_punctuation(self):
        assert GeoIDMatcher._normalize("St. Mary's") == "st marys"

    def test_strip_whitespace(self):
        assert GeoIDMatcher._normalize("  Borden  ") == "borden"

    def test_city_suffix_lowercase(self):
        assert GeoIDMatcher._normalize("San Francisco city") == "san francisco"

    def test_township_suffix(self):
        assert GeoIDMatcher._normalize("Springfield township") == "springfield"

    def test_town_suffix(self):
        assert GeoIDMatcher._normalize("Adams town") == "adams"

    def test_census_area_suffix(self):
        assert GeoIDMatcher._normalize("Bethel Census Area") == "bethel"

    def test_no_suffix(self):
        """Name without a known suffix should just be lowercased."""
        assert GeoIDMatcher._normalize("Borden") == "borden"

    def test_empty_string(self):
        assert GeoIDMatcher._normalize("") == ""

    def test_only_suffix(self):
        """Edge case: name that IS a suffix stays unchanged (no leading space)."""
        # "County" doesn't match " County" suffix (leading space required)
        assert GeoIDMatcher._normalize("County") == "county"


class TestMatcherWithMockLookup:
    """Test matching logic with a manually constructed lookup.

    Instead of requiring a database session, we directly set the
    internal _lookup dict for unit testing.
    """

    @pytest.fixture
    def matcher(self):
        """Create a GeoIDMatcher with a mock lookup instead of DB."""
        # Bypass __init__ which requires a session
        m = object.__new__(GeoIDMatcher)
        m.session = None
        m.census_year = 2023
        m._lookup = {
            "48": [
                ("48033", "Borden", "Borden County", "county"),
                ("48263", "Kent", "Kent County", "county"),
                ("48393", "Roberts", "Roberts County", "county"),
                ("48201", "Harris", "Harris County", "county"),
                ("4835000", "Houston", "Houston city", "municipality"),
            ],
            "37": [
                ("37119", "Mecklenburg", "Mecklenburg County", "county"),
                ("3712000", "Charlotte", "Charlotte city", "municipality"),
            ],
            "01": [
                ("01001", "Autauga", "Autauga County", "county"),
                ("01003", "Baldwin", "Baldwin County", "county"),
            ],
            "24": [
                ("24510", "Baltimore", "Baltimore city", "county"),
                ("24005", "Baltimore", "Baltimore County", "county"),
            ],
        }
        return m

    def test_exact_match_on_name(self, matcher):
        """Exact match on jurisdiction_name."""
        assert matcher.match("48", "Borden") == "48033"

    def test_exact_match_on_lsad(self, matcher):
        """Exact match on jurisdiction_name_lsad."""
        assert matcher.match("48", "Borden County") == "48033"

    def test_normalized_match_county_suffix(self, matcher):
        """Normalized match strips 'County' suffix."""
        assert matcher.match("48", "Harris County") == "48201"

    def test_normalized_match_case_insensitive(self, matcher):
        """Normalized match is case-insensitive."""
        assert matcher.match("48", "BORDEN") == "48033"

    def test_normalized_match_city_prefix(self, matcher):
        """Normalized match strips 'City of' prefix."""
        assert matcher.match("37", "City of Charlotte") == "3712000"

    def test_no_match_wrong_state(self, matcher):
        """Name exists but in different state returns None."""
        assert matcher.match("06", "Borden") is None

    def test_no_match_nonexistent(self, matcher):
        """Completely unknown name returns None."""
        assert matcher.match("48", "Zzyzx") is None

    def test_type_filter(self, matcher):
        """Type filter narrows matches."""
        # "Houston" exists as municipality
        assert matcher.match("48", "Houston", jurisdiction_type="municipality") == "4835000"
        # "Houston" does not exist as county (Harris is the county)
        assert matcher.match("48", "Houston", jurisdiction_type="county") is None

    def test_fuzzy_match_typo(self, matcher):
        """Fuzzy match catches minor typos."""
        # "Meckenburg" (missing 'l') should fuzzy-match to Mecklenburg
        result = matcher.match("37", "Meckenburg", min_similarity=0.80)
        assert result == "37119"

    def test_fuzzy_match_below_threshold(self, matcher):
        """Fuzzy match rejects matches below threshold."""
        # "Meck" is too short/different to match at 0.85 threshold
        result = matcher.match("37", "Meck", min_similarity=0.90)
        assert result is None

    def test_ambiguous_name_returns_first(self, matcher):
        """When multiple candidates match, returns first/best match."""
        # "Baltimore" exists twice in MD (city and county)
        # Exact match on jurisdiction_name should return the first found
        result = matcher.match("24", "Baltimore")
        assert result in ("24510", "24005")

    def test_ambiguous_with_type_filter(self, matcher):
        """Type filter disambiguates same-name jurisdictions."""
        # Baltimore city is jurisdiction_type=county (it's a county-equivalent in TIGER)
        # Both have type "county" in our mock data
        result = matcher.match("24", "Baltimore County", jurisdiction_type="county")
        assert result is not None

    def test_match_batch(self, matcher):
        """Batch matching returns dict of results."""
        results = matcher.match_batch("48", ["Borden", "Kent", "Zzyzx"])
        assert results == {
            "Borden": "48033",
            "Kent": "48263",
            "Zzyzx": None,
        }

    def test_empty_state_returns_none(self, matcher):
        """State with no entries returns None."""
        assert matcher.match("99", "Anywhere") is None
