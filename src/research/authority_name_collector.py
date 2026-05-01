"""Collector for licensing authority names from state directories.

Collects verified licensing authority names from individual state ABC
agency directories and exports them as CSV fragments that can be merged
into the seed files:
    - data/seed/licensing_authority_names.csv (state-level patterns)
    - data/seed/licensing_authority_overrides.csv (per-GEOID names)

Each collector method returns a pandas DataFrame with either the pattern
schema or the override schema.

These scripts are research helpers — they run independently of the pipeline.
"""

import logging
import re
import time
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.common.constants import FIPS_STATES

logger = logging.getLogger("jurisdiction.authority_name_collector")

# Standard columns for per-GEOID overrides
OVERRIDE_COLUMNS = [
    "geoid",
    "licensing_authority_name",
    "licensing_authority_type",
    "data_source",
    "notes",
]

# Standard columns for state naming patterns
PATTERN_COLUMNS = [
    "state_fips",
    "jurisdiction_type",
    "naming_pattern",
    "authority_type",
    "data_source",
    "notes",
]


class AuthorityNameCollector:
    """Collects licensing authority names from state directories.

    Usage:
        collector = AuthorityNameCollector(session)
        df = collector.collect_nc_abc_boards()
        collector.export_overrides("data/seed/licensing_authority_overrides.csv")
    """

    def __init__(
        self,
        geoid_matcher=None,
        request_delay: float = 2.0,
    ) -> None:
        """Initialize collector.

        Args:
            geoid_matcher: Optional GeoIDMatcher instance for name-to-GEOID resolution.
            request_delay: Seconds to wait between HTTP requests.
        """
        self.geoid_matcher = geoid_matcher
        self.request_delay = request_delay
        self._http = requests.Session()
        self._http.headers.update({
            "User-Agent": "JurisdictionTaxonomy/1.0 (regulatory research tool)",
        })
        self._override_frames: List[pd.DataFrame] = []
        self._pattern_frames: List[pd.DataFrame] = []

    def collect_nc_abc_boards(self) -> pd.DataFrame:
        """Scrape North Carolina ABC Board directory for ~170 named boards.

        The NC ABC Commission publishes a directory of local ABC boards
        at https://abc.nc.gov/boards/.

        Returns:
            DataFrame with override columns (per-GEOID verified names).
        """
        url = "https://abc.nc.gov/boards/"
        logger.info(f"Collecting NC ABC Board names from: {url}")

        try:
            time.sleep(self.request_delay)
            response = self._http.get(url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch NC ABC directory: {e}")
            return self._empty_override_frame()

        soup = BeautifulSoup(response.text, "html.parser")
        records = []

        # NC ABC directory typically lists boards in a table or list format
        # Look for links or list items containing board names
        for link in soup.find_all("a"):
            text = link.get_text(strip=True)
            # NC boards typically named like "Carteret County ABC Board"
            if "ABC Board" in text or "ABC Store" in text:
                board_name = text.strip()

                # Extract county/city name from the board name
                name = board_name
                for suffix in [
                    " County ABC Board", " ABC Board",
                    " County ABC Store", " ABC Store",
                ]:
                    if name.endswith(suffix):
                        name = name[: -len(suffix)]
                        break

                geoid = None
                if self.geoid_matcher:
                    # Try county first, then municipality
                    geoid = self.geoid_matcher.match(
                        "37", name, jurisdiction_type="county"
                    )
                    if not geoid:
                        geoid = self.geoid_matcher.match(
                            "37", name, jurisdiction_type="municipality"
                        )

                records.append({
                    "geoid": geoid or "",
                    "licensing_authority_name": board_name,
                    "licensing_authority_type": "dedicated_board",
                    "data_source": "nc_abc_directory",
                    "notes": "",
                })

        # Also try table-based parsing
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            for tr in rows[1:]:
                cells = tr.find_all(["td", "th"])
                if cells:
                    text = cells[0].get_text(strip=True)
                    if text and len(text) > 3:
                        name = text
                        board_name = text
                        if "ABC" not in board_name:
                            board_name = f"{name} ABC Board"

                        geoid = None
                        if self.geoid_matcher:
                            geoid = self.geoid_matcher.match(
                                "37", name, jurisdiction_type="county"
                            )

                        records.append({
                            "geoid": geoid or "",
                            "licensing_authority_name": board_name,
                            "licensing_authority_type": "dedicated_board",
                            "data_source": "nc_abc_directory",
                            "notes": "",
                        })

        df = self._records_to_override_frame(records)
        # Deduplicate by geoid
        df = df.drop_duplicates(subset=["geoid"], keep="first")
        logger.info(f"  NC ABC: collected {len(df)} board names")
        self._override_frames.append(df)
        return df

    def collect_md_liquor_boards(self) -> pd.DataFrame:
        """Scrape Maryland local liquor boards page for 24 boards.

        Maryland has 23 county-level liquor boards plus Baltimore City,
        each with unique (inconsistent) naming conventions.

        Returns:
            DataFrame with override columns.
        """
        url = "https://www.marylandtaxes.gov/business/alcohol-tobacco/liquor-boards.php"
        logger.info(f"Collecting MD liquor board names from: {url}")

        try:
            time.sleep(self.request_delay)
            response = self._http.get(url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch MD liquor boards page: {e}")
            return self._empty_override_frame()

        soup = BeautifulSoup(response.text, "html.parser")
        records = []

        # Maryland page typically lists boards with their full names
        # Look for strong/bold text or heading-like elements
        for element in soup.find_all(["li", "p", "strong", "h3", "h4"]):
            text = element.get_text(strip=True)

            # Look for patterns like "County Board of License Commissioners"
            # or "County Liquor Board" or similar
            md_patterns = [
                r"(\w[\w\s\.\']+?)\s+(Board of License Commissioners?)",
                r"(\w[\w\s\.\']+?)\s+(Liquor Board)",
                r"(\w[\w\s\.\']+?)\s+(Liquor Control (?:Board|Commission))",
                r"(\w[\w\s\.\']+?)\s+(Alcoholic Beverage (?:Board|Service))",
                r"(\w[\w\s\.\']+?)\s+(Liquor License Board)",
            ]

            for pattern in md_patterns:
                match = re.match(pattern, text)
                if match:
                    full_name = text.strip()
                    county_part = match.group(1).strip()

                    # Extract county name
                    county_name = county_part
                    for suffix in [" County", " City"]:
                        if county_name.endswith(suffix):
                            county_name = county_name[: -len(suffix)]
                            break

                    geoid = None
                    if self.geoid_matcher:
                        geoid = self.geoid_matcher.match(
                            "24", county_name, jurisdiction_type="county"
                        )

                    records.append({
                        "geoid": geoid or "",
                        "licensing_authority_name": full_name,
                        "licensing_authority_type": "dedicated_board",
                        "data_source": "md_liquor_boards",
                        "notes": "",
                    })
                    break  # Only match first pattern per element

        df = self._records_to_override_frame(records)
        df = df.drop_duplicates(subset=["geoid"], keep="first")
        logger.info(f"  MD Liquor Boards: collected {len(df)} board names")
        self._override_frames.append(df)
        return df

    def collect_in_county_boards(self) -> pd.DataFrame:
        """Scrape Indiana ATC for 92 county alcoholic beverage board names.

        Indiana has an Alcoholic Beverage Board in each of its 92 counties.

        Returns:
            DataFrame with override columns.
        """
        url = "https://www.in.gov/atc/"
        logger.info(f"Collecting IN county board names from: {url}")

        try:
            time.sleep(self.request_delay)
            response = self._http.get(url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch Indiana ATC page: {e}")
            return self._empty_override_frame()

        soup = BeautifulSoup(response.text, "html.parser")
        records = []

        # Indiana ATC may list local boards
        # Look for county names in links or list items
        for link in soup.find_all("a"):
            text = link.get_text(strip=True)
            if "Alcoholic Beverage Board" in text or "County Board" in text:
                board_name = text.strip()
                name = board_name
                for suffix in [
                    " County Alcoholic Beverage Board",
                    " Alcoholic Beverage Board",
                ]:
                    if name.endswith(suffix):
                        name = name[: -len(suffix)]
                        break

                geoid = None
                if self.geoid_matcher:
                    geoid = self.geoid_matcher.match(
                        "18", name, jurisdiction_type="county"
                    )

                records.append({
                    "geoid": geoid or "",
                    "licensing_authority_name": board_name,
                    "licensing_authority_type": "dedicated_board",
                    "data_source": "in_atc",
                    "notes": "",
                })

        df = self._records_to_override_frame(records)
        df = df.drop_duplicates(subset=["geoid"], keep="first")
        logger.info(f"  Indiana ATC: collected {len(df)} board names")
        self._override_frames.append(df)
        return df

    def generate_naming_patterns(self) -> pd.DataFrame:
        """Generate the state-level naming pattern CSV from research.

        Encodes the research findings about how each state names its
        local licensing authorities. States fall into these categories:

        1. State agency handles all licensing (PA, UT, VA, NH, etc.)
           → static pattern: "State Agency Name"
        2. Dedicated local boards with standard naming (NC, IN, IL)
           → pattern: "{name} County ABC Board" etc.
        3. General government authority (most license states)
           → pattern: "{name} County Government" or similar

        Returns:
            DataFrame with pattern columns.
        """
        patterns = []

        # --- State agency handles all licensing ---
        state_agency_patterns = {
            # (state_fips, abc_agency_name, jurisdiction_types)
            "42": ("Pennsylvania Liquor Control Board", ["county", "municipality"]),
            "49": ("Utah Department of Alcoholic Beverage Services", ["county", "municipality"]),
            "51": ("Virginia Alcoholic Beverage Control Authority", ["county", "municipality", "independent_city"]),
            "33": ("New Hampshire State Liquor Commission", ["county", "municipality", "mcd"]),
            "30": ("Montana Department of Revenue - Liquor Control Division", ["county"]),
        }

        for state_fips, (agency_name, jtypes) in state_agency_patterns.items():
            for jtype in jtypes:
                patterns.append({
                    "state_fips": state_fips,
                    "jurisdiction_type": jtype,
                    "naming_pattern": agency_name,
                    "authority_type": "state_agency",
                    "data_source": "manual_research",
                    "notes": "State handles all licensing centrally",
                })

        # --- Dedicated local boards with standard naming ---
        dedicated_board_patterns = {
            # state_fips: [(jtype, pattern)]
            "37": [  # North Carolina
                ("county", "{name} County ABC Board"),
                ("municipality", "{name} ABC Board"),
            ],
            "18": [  # Indiana
                ("county", "{name} County Alcoholic Beverage Board"),
            ],
        }

        for state_fips, type_patterns in dedicated_board_patterns.items():
            for jtype, pattern in type_patterns:
                patterns.append({
                    "state_fips": state_fips,
                    "jurisdiction_type": jtype,
                    "naming_pattern": pattern,
                    "authority_type": "dedicated_board",
                    "data_source": "manual_research",
                    "notes": "",
                })

        # --- General government authority ---
        general_gov_patterns = {
            # state_fips: [(jtype, pattern)]
            "17": [  # Illinois
                ("county", "{name} County Liquor Commission"),
                ("municipality", "City of {name} Local Liquor Commission"),
            ],
            "36": [  # New York
                ("county", "{name} County"),
                ("municipality", "{name}"),
            ],
            "06": [  # California
                ("county", "{name} County"),
                ("municipality", "City of {name}"),
            ],
            "12": [  # Florida
                ("county", "{name} County"),
                ("municipality", "City of {name}"),
            ],
            "13": [  # Georgia
                ("county", "{name} County"),
                ("municipality", "City of {name}"),
            ],
            "48": [  # Texas
                ("county", "{name} County"),
                ("municipality", "City of {name}"),
            ],
            "39": [  # Ohio
                ("county", "{name} County"),
                ("municipality", "{name}"),
            ],
        }

        for state_fips, type_patterns in general_gov_patterns.items():
            for jtype, pattern in type_patterns:
                patterns.append({
                    "state_fips": state_fips,
                    "jurisdiction_type": jtype,
                    "naming_pattern": pattern,
                    "authority_type": "general_government",
                    "data_source": "manual_research",
                    "notes": "",
                })

        df = pd.DataFrame(patterns, columns=PATTERN_COLUMNS)
        logger.info(f"  Generated {len(df)} naming patterns for {df['state_fips'].nunique()} states")
        self._pattern_frames.append(df)
        return df

    def add_manual_overrides(self, entries: List[Dict]) -> pd.DataFrame:
        """Add manually curated authority name overrides.

        Args:
            entries: List of dicts with override columns.

        Returns:
            DataFrame with override columns.
        """
        df = self._records_to_override_frame(entries)
        logger.info(f"  Manual: added {len(df)} authority name overrides")
        self._override_frames.append(df)
        return df

    def export_overrides(self, output_path: Path) -> pd.DataFrame:
        """Merge all collected overrides, deduplicate by GEOID, and export CSV.

        Args:
            output_path: Path to write the overrides CSV.

        Returns:
            Combined DataFrame.
        """
        if not self._override_frames:
            logger.warning("No override data collected — nothing to export")
            return self._empty_override_frame()

        combined = pd.concat(self._override_frames, ignore_index=True)

        # Remove entries without GEOIDs
        unresolved = combined[combined["geoid"] == ""]
        if len(unresolved) > 0:
            logger.warning(
                f"  {len(unresolved)} overrides could not be resolved to GEOIDs:"
            )
            for _, row in unresolved.iterrows():
                logger.warning(
                    f"    {row['licensing_authority_name']} | source={row['data_source']}"
                )

        combined = combined[combined["geoid"] != ""]
        combined = combined.drop_duplicates(subset=["geoid"], keep="first")
        combined = combined.sort_values("geoid").reset_index(drop=True)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        combined.to_csv(output_path, index=False)
        logger.info(f"  Exported {len(combined)} authority name overrides to {output_path}")

        return combined

    def export_patterns(self, output_path: Path) -> pd.DataFrame:
        """Merge all collected patterns and export CSV.

        Args:
            output_path: Path to write the patterns CSV.

        Returns:
            Combined DataFrame.
        """
        if not self._pattern_frames:
            logger.warning("No pattern data collected — nothing to export")
            return self._empty_pattern_frame()

        combined = pd.concat(self._pattern_frames, ignore_index=True)
        combined = combined.drop_duplicates(
            subset=["state_fips", "jurisdiction_type"], keep="first"
        )
        combined = combined.sort_values(
            ["state_fips", "jurisdiction_type"]
        ).reset_index(drop=True)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        combined.to_csv(output_path, index=False)
        logger.info(f"  Exported {len(combined)} naming patterns to {output_path}")

        return combined

    # ---- Private helpers ----

    @staticmethod
    def _empty_override_frame() -> pd.DataFrame:
        """Return an empty DataFrame with override columns."""
        return pd.DataFrame(columns=OVERRIDE_COLUMNS)

    @staticmethod
    def _empty_pattern_frame() -> pd.DataFrame:
        """Return an empty DataFrame with pattern columns."""
        return pd.DataFrame(columns=PATTERN_COLUMNS)

    @staticmethod
    def _records_to_override_frame(records: List[Dict]) -> pd.DataFrame:
        """Convert a list of record dicts to a DataFrame with override columns."""
        if not records:
            return pd.DataFrame(columns=OVERRIDE_COLUMNS)
        df = pd.DataFrame(records)
        for col in OVERRIDE_COLUMNS:
            if col not in df.columns:
                df[col] = None
        return df[OVERRIDE_COLUMNS]
