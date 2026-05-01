"""Unit tests for constants and reference data."""

from src.common.constants import (
    CONTROL_STATES,
    DC_FIPS,
    EXPECTED_STATE_COUNT,
    FIPS_STATES,
    STRONG_MCD_STATES,
    TERRITORY_FIPS,
    VA_INDEPENDENT_CITY_FIPS,
)


class TestFipsStates:
    def test_total_count(self) -> None:
        """All 50 states + DC + 5 territories = 56."""
        assert len(FIPS_STATES) == EXPECTED_STATE_COUNT

    def test_dc_present(self) -> None:
        assert DC_FIPS in FIPS_STATES
        assert FIPS_STATES[DC_FIPS] == ("DC", "District of Columbia")

    def test_all_territories_present(self) -> None:
        for fips in TERRITORY_FIPS:
            assert fips in FIPS_STATES, f"Territory {fips} not in FIPS_STATES"

    def test_fips_codes_are_two_digits(self) -> None:
        for fips in FIPS_STATES:
            assert len(fips) == 2, f"FIPS {fips} is not 2 digits"
            assert fips.isdigit(), f"FIPS {fips} is not numeric"

    def test_abbreviations_are_two_chars(self) -> None:
        for fips, (abbr, name) in FIPS_STATES.items():
            assert len(abbr) == 2, f"{name} abbreviation {abbr} is not 2 chars"


class TestTerritoryFips:
    def test_count(self) -> None:
        assert len(TERRITORY_FIPS) == 5

    def test_known_territories(self) -> None:
        assert "60" in TERRITORY_FIPS  # American Samoa
        assert "66" in TERRITORY_FIPS  # Guam
        assert "69" in TERRITORY_FIPS  # CNMI
        assert "72" in TERRITORY_FIPS  # Puerto Rico
        assert "78" in TERRITORY_FIPS  # USVI

    def test_dc_is_not_territory(self) -> None:
        assert DC_FIPS not in TERRITORY_FIPS


class TestStrongMcdStates:
    def test_count(self) -> None:
        assert len(STRONG_MCD_STATES) == 12

    def test_known_mcd_states(self) -> None:
        # CT, ME, MA, MI, MN, NH, NJ, NY, PA, RI, VT, WI
        expected = {"09", "23", "25", "26", "27", "33", "34", "36", "42", "44", "50", "55"}
        assert STRONG_MCD_STATES == expected

    def test_all_are_valid_fips(self) -> None:
        for fips in STRONG_MCD_STATES:
            assert fips in FIPS_STATES, f"MCD state {fips} not in FIPS_STATES"


class TestControlStates:
    def test_count(self) -> None:
        assert len(CONTROL_STATES) == 17

    def test_all_are_valid_fips(self) -> None:
        for fips in CONTROL_STATES:
            assert fips in FIPS_STATES, f"Control state {fips} not in FIPS_STATES"

    def test_no_territories_in_control(self) -> None:
        for fips in CONTROL_STATES:
            assert fips not in TERRITORY_FIPS, f"Territory {fips} in CONTROL_STATES"


class TestVaIndependentCities:
    def test_count(self) -> None:
        assert len(VA_INDEPENDENT_CITY_FIPS) == 38

    def test_all_start_with_virginia_fips(self) -> None:
        for geoid in VA_INDEPENDENT_CITY_FIPS:
            assert geoid.startswith("51"), f"{geoid} doesn't start with VA FIPS 51"

    def test_all_are_five_digits(self) -> None:
        for geoid in VA_INDEPENDENT_CITY_FIPS:
            assert len(geoid) == 5, f"{geoid} is not 5 digits"
