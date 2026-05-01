"""Unit tests for FIPS code utilities."""

import pytest

from src.census.fips import (
    format_county_geoid,
    format_mcd_geoid,
    format_place_geoid,
    get_state_info,
    parse_geoid,
    validate_geoid,
)


class TestFormatCountyGeoid:
    def test_basic_formatting(self) -> None:
        assert format_county_geoid("01", "001") == "01001"

    def test_zero_padding(self) -> None:
        assert format_county_geoid("1", "1") == "01001"

    def test_california_la_county(self) -> None:
        assert format_county_geoid("06", "037") == "06037"


class TestFormatPlaceGeoid:
    def test_basic_formatting(self) -> None:
        assert format_place_geoid("06", "44000") == "0644000"

    def test_zero_padding(self) -> None:
        assert format_place_geoid("6", "44000") == "0644000"

    def test_short_place_fips(self) -> None:
        assert format_place_geoid("01", "100") == "0100100"


class TestFormatMcdGeoid:
    def test_basic_formatting(self) -> None:
        assert format_mcd_geoid("25", "017", "00005") == "2501700005"

    def test_ten_digit_result(self) -> None:
        result = format_mcd_geoid("42", "003", "12345")
        assert len(result) == 10
        assert result == "4200312345"


class TestParseGeoid:
    def test_parse_state(self) -> None:
        result = parse_geoid("06")
        assert result["state_fips"] == "06"
        assert result["county_fips"] is None
        assert result["place_fips"] is None
        assert result["cousub_fips"] is None

    def test_parse_county(self) -> None:
        result = parse_geoid("06037")
        assert result["state_fips"] == "06"
        assert result["county_fips"] == "037"
        assert result["place_fips"] is None

    def test_parse_place(self) -> None:
        result = parse_geoid("0644000")
        assert result["state_fips"] == "06"
        assert result["place_fips"] == "44000"
        assert result["county_fips"] is None

    def test_parse_mcd(self) -> None:
        result = parse_geoid("2501700005")
        assert result["state_fips"] == "25"
        assert result["county_fips"] == "017"
        assert result["cousub_fips"] == "00005"

    def test_round_trip_county(self) -> None:
        geoid = format_county_geoid("01", "001")
        parsed = parse_geoid(geoid)
        assert parsed["state_fips"] == "01"
        assert parsed["county_fips"] == "001"

    def test_round_trip_place(self) -> None:
        geoid = format_place_geoid("06", "44000")
        parsed = parse_geoid(geoid)
        assert parsed["state_fips"] == "06"
        assert parsed["place_fips"] == "44000"

    def test_round_trip_mcd(self) -> None:
        geoid = format_mcd_geoid("25", "017", "00005")
        parsed = parse_geoid(geoid)
        assert parsed["state_fips"] == "25"
        assert parsed["county_fips"] == "017"
        assert parsed["cousub_fips"] == "00005"


class TestValidateGeoid:
    def test_valid_county(self) -> None:
        assert validate_geoid("01001") is True
        assert validate_geoid("06037") is True

    def test_valid_place(self) -> None:
        assert validate_geoid("0644000") is True

    def test_valid_mcd(self) -> None:
        assert validate_geoid("2501700005") is True

    def test_valid_state(self) -> None:
        assert validate_geoid("06") is True
        assert validate_geoid("72") is True  # Puerto Rico

    def test_invalid_empty(self) -> None:
        assert validate_geoid("") is False

    def test_invalid_non_numeric(self) -> None:
        assert validate_geoid("ABC") is False
        assert validate_geoid("01A01") is False

    def test_invalid_length(self) -> None:
        assert validate_geoid("0100") is False  # 4 digits
        assert validate_geoid("010011") is False  # 6 digits

    def test_invalid_unknown_state(self) -> None:
        assert validate_geoid("99001") is False  # State 99 doesn't exist

    def test_validate_with_expected_layer(self) -> None:
        assert validate_geoid("01001", expected_layer="county") is True
        assert validate_geoid("01001", expected_layer="place") is False  # Wrong length
        assert validate_geoid("0100100", expected_layer="place") is True

    def test_territory_geoids_valid(self) -> None:
        assert validate_geoid("72") is True  # Puerto Rico
        assert validate_geoid("60") is True  # American Samoa
        assert validate_geoid("66") is True  # Guam
        assert validate_geoid("69") is True  # CNMI
        assert validate_geoid("78") is True  # USVI


class TestGetStateInfo:
    def test_known_state(self) -> None:
        result = get_state_info("06")
        assert result is not None
        assert result["abbr"] == "CA"
        assert result["name"] == "California"

    def test_territory(self) -> None:
        result = get_state_info("72")
        assert result is not None
        assert result["abbr"] == "PR"
        assert result["name"] == "Puerto Rico"

    def test_dc(self) -> None:
        result = get_state_info("11")
        assert result is not None
        assert result["abbr"] == "DC"

    def test_unknown(self) -> None:
        assert get_state_info("99") is None
