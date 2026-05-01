"""FIPS code formatting, validation, and parsing utilities."""

from typing import Dict, Optional

from src.common.constants import FIPS_STATES


def format_county_geoid(state_fips: str, county_fips: str) -> str:
    """Format a 5-digit county GEOID (SSCCC).

    Args:
        state_fips: 2-digit state FIPS code.
        county_fips: 3-digit county FIPS code.

    Returns:
        5-digit GEOID string.
    """
    return f"{state_fips.zfill(2)}{county_fips.zfill(3)}"


def format_place_geoid(state_fips: str, place_fips: str) -> str:
    """Format a 7-digit place GEOID (SSPPPPP).

    Args:
        state_fips: 2-digit state FIPS code.
        place_fips: 5-digit place FIPS code.

    Returns:
        7-digit GEOID string.
    """
    return f"{state_fips.zfill(2)}{place_fips.zfill(5)}"


def format_mcd_geoid(state_fips: str, county_fips: str, cousub_fips: str) -> str:
    """Format a 10-digit MCD/county subdivision GEOID (SSCCCMMMMM).

    Args:
        state_fips: 2-digit state FIPS code.
        county_fips: 3-digit county FIPS code.
        cousub_fips: 5-digit county subdivision FIPS code.

    Returns:
        10-digit GEOID string.
    """
    return f"{state_fips.zfill(2)}{county_fips.zfill(3)}{cousub_fips.zfill(5)}"


def parse_geoid(geoid: str) -> Dict[str, Optional[str]]:
    """Parse a GEOID into its component FIPS parts.

    Args:
        geoid: A FIPS GEOID of length 2 (state), 5 (county), 7 (place), or 10 (MCD).

    Returns:
        Dict with keys: state_fips, county_fips, place_fips, cousub_fips.
        Values are None if not applicable for the given GEOID length.
    """
    result: Dict[str, Optional[str]] = {
        "state_fips": None,
        "county_fips": None,
        "place_fips": None,
        "cousub_fips": None,
    }

    if len(geoid) >= 2:
        result["state_fips"] = geoid[:2]

    if len(geoid) == 5:
        # County: SSCCC
        result["county_fips"] = geoid[2:5]
    elif len(geoid) == 7:
        # Place: SSPPPPP
        result["place_fips"] = geoid[2:7]
    elif len(geoid) == 10:
        # MCD: SSCCCMMMMM
        result["county_fips"] = geoid[2:5]
        result["cousub_fips"] = geoid[5:10]

    return result


def validate_geoid(geoid: str, expected_layer: Optional[str] = None) -> bool:
    """Validate a GEOID for correctness.

    Args:
        geoid: The GEOID string to validate.
        expected_layer: Optional expected layer ("county", "place", "county_subdivision").

    Returns:
        True if the GEOID is valid.
    """
    if not geoid or not geoid.isdigit():
        return False

    valid_lengths = {2, 5, 7, 10}
    if len(geoid) not in valid_lengths:
        return False

    # Check that the state FIPS portion is a known state
    state_fips = geoid[:2]
    if state_fips not in FIPS_STATES:
        return False

    # Validate length against expected layer
    if expected_layer:
        expected_length = {
            "state": 2,
            "county": 5,
            "place": 7,
            "county_subdivision": 10,
        }
        if expected_layer in expected_length and len(geoid) != expected_length[expected_layer]:
            return False

    return True


def get_state_info(state_fips: str) -> Optional[Dict[str, str]]:
    """Look up state abbreviation and name from FIPS code.

    Args:
        state_fips: 2-digit state FIPS code.

    Returns:
        Dict with 'abbr' and 'name', or None if not found.
    """
    entry = FIPS_STATES.get(state_fips)
    if entry is None:
        return None
    return {"abbr": entry[0], "name": entry[1]}
