"""Collector for dry/wet jurisdiction status from external sources.

Collects dry/wet/moist status data from various state and national sources
and outputs CSV fragments that can be merged into the seed file
(data/seed/dry_wet_status.csv).

Each collector method returns a pandas DataFrame with the standard schema:
    geoid, state_fips, jurisdiction_name, dry_wet_status,
    restriction_notes, data_source, last_verified

These scripts are research helpers — they run independently of the pipeline.
Their output populates the seed CSVs which the pipeline then reads.
"""

import io
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

logger = logging.getLogger("jurisdiction.dry_wet_collector")

# Standard columns for dry/wet status output
DRY_WET_COLUMNS = [
    "geoid",
    "state_fips",
    "jurisdiction_name",
    "dry_wet_status",
    "restriction_notes",
    "data_source",
    "last_verified",
]


class DryWetCollector:
    """Collects dry/wet status data from various sources into CSV format.

    Usage:
        collector = DryWetCollector(session)
        df = collector.collect_from_wikipedia()
        collector.export_combined("data/seed/dry_wet_status.csv")
    """

    def __init__(
        self,
        geoid_matcher=None,
        request_delay: float = 2.0,
    ) -> None:
        """Initialize collector.

        Args:
            geoid_matcher: Optional GeoIDMatcher instance for name-to-GEOID resolution.
                           Required for sources that provide names but not GEOIDs.
            request_delay: Seconds to wait between HTTP requests.
        """
        self.geoid_matcher = geoid_matcher
        self.request_delay = request_delay
        self._http = requests.Session()
        self._http.headers.update({
            "User-Agent": "JurisdictionTaxonomy/1.0 (regulatory research tool)",
        })
        self._collected_frames: List[pd.DataFrame] = []

    def collect_from_wikipedia(self) -> pd.DataFrame:
        """Scrape Wikipedia 'List of dry communities by U.S. state'.

        Parses the state-by-state tables on the Wikipedia page listing
        dry communities. Returns a DataFrame of county-level dry/moist
        jurisdictions with GEOIDs resolved via the GeoIDMatcher.

        Returns:
            DataFrame with standard dry/wet columns.
        """
        url = "https://en.wikipedia.org/wiki/List_of_dry_communities_by_U.S._state"
        logger.info(f"Collecting dry/wet data from Wikipedia: {url}")

        try:
            time.sleep(self.request_delay)
            response = self._http.get(url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch Wikipedia page: {e}")
            return self._empty_frame()

        soup = BeautifulSoup(response.text, "html.parser")
        records = []

        # Wikipedia organizes dry communities by state using H2/H3 headings
        # followed by lists or tables
        current_state = None
        current_state_fips = None

        # Build reverse lookup: state name -> state_fips
        state_name_to_fips = {}
        for fips, (abbr, name) in FIPS_STATES.items():
            state_name_to_fips[name.lower()] = fips
            state_name_to_fips[abbr.lower()] = fips

        for heading in soup.find_all(["h2", "h3"]):
            heading_text = heading.get_text(strip=True).lower()
            # Remove "[edit]" suffix
            heading_text = re.sub(r"\[edit\]$", "", heading_text).strip()

            # Check if this heading matches a state name
            matched_fips = state_name_to_fips.get(heading_text)
            if matched_fips:
                current_state = heading_text
                current_state_fips = matched_fips
                continue

            if not current_state_fips:
                continue

            # Look for tables or lists following this heading
            sibling = heading.find_next_sibling()
            while sibling and sibling.name not in ["h2", "h3"]:
                if sibling.name == "table":
                    records.extend(
                        self._parse_wikipedia_table(
                            sibling, current_state_fips, current_state
                        )
                    )
                elif sibling.name == "ul":
                    records.extend(
                        self._parse_wikipedia_list(
                            sibling, current_state_fips, current_state
                        )
                    )
                sibling = sibling.find_next_sibling()

        # Also try to parse using pandas read_html for well-structured tables
        try:
            tables = pd.read_html(io.StringIO(response.text))
            for table in tables:
                table_records = self._parse_generic_table(table)
                if table_records:
                    records.extend(table_records)
        except Exception:
            logger.debug("pd.read_html fallback did not yield additional records")

        df = self._records_to_frame(records)
        logger.info(f"  Wikipedia: collected {len(df)} dry/wet records")
        self._collected_frames.append(df)
        return df

    def collect_from_tabc_xls(self, xls_path: Path) -> pd.DataFrame:
        """Parse Texas TABC wet/dry status spreadsheet.

        The Texas Alcoholic Beverage Commission publishes an Excel file
        with the wet/dry/moist status of every county in Texas.

        Args:
            xls_path: Path to the downloaded TABC XLS file.

        Returns:
            DataFrame with standard dry/wet columns.
        """
        logger.info(f"Collecting dry/wet data from TABC XLS: {xls_path}")

        if not xls_path.exists():
            logger.error(f"TABC XLS file not found: {xls_path}")
            return self._empty_frame()

        try:
            df_raw = pd.read_excel(xls_path, dtype=str)
        except Exception as e:
            logger.error(f"Failed to read TABC XLS: {e}")
            return self._empty_frame()

        records = []

        # TABC spreadsheet typically has columns like:
        # County, Status (Wet/Dry/Moist), Notes
        # Column names vary by year; try common patterns
        status_col = None
        name_col = None
        for col in df_raw.columns:
            col_lower = col.lower()
            if "status" in col_lower or "wet" in col_lower or "dry" in col_lower:
                status_col = col
            if "county" in col_lower or "name" in col_lower:
                name_col = col

        if not status_col or not name_col:
            logger.warning("Could not identify status/name columns in TABC XLS")
            logger.warning(f"Available columns: {list(df_raw.columns)}")
            return self._empty_frame()

        for _, row in df_raw.iterrows():
            county_name = str(row[name_col]).strip()
            status_raw = str(row[status_col]).strip().lower()

            if not county_name or county_name == "nan":
                continue

            # Normalize status
            status = self._normalize_status(status_raw)

            # Resolve GEOID using matcher
            geoid = None
            if self.geoid_matcher:
                geoid = self.geoid_matcher.match(
                    "48", county_name, jurisdiction_type="county"
                )

            notes = str(row.get("Notes", row.get("notes", ""))).strip()
            if notes == "nan":
                notes = ""

            records.append({
                "geoid": geoid or "",
                "state_fips": "48",
                "jurisdiction_name": county_name,
                "dry_wet_status": status,
                "restriction_notes": notes or None,
                "data_source": "tabc_xls",
                "last_verified": str(date.today()),
            })

        df = self._records_to_frame(records)
        logger.info(f"  TABC XLS: collected {len(df)} dry/wet records")
        self._collected_frames.append(df)
        return df

    def collect_from_arkansas_gis(self) -> pd.DataFrame:
        """Query Arkansas GIS Feature Service REST API for dry/wet status.

        Arkansas Department of Finance and Administration publishes an ArcGIS
        Feature Service with county-level dry/wet data.

        Returns:
            DataFrame with standard dry/wet columns.
        """
        # Arkansas GIS REST endpoint for wet/dry status
        base_url = (
            "https://gis.arkansas.gov/arcgis/rest/services/"
            "FEATURESERVICES/Boundaries/FeatureServer/0/query"
        )
        params = {
            "where": "1=1",
            "outFields": "NAME,WET_DRY,FIPS",
            "returnGeometry": "false",
            "f": "json",
        }

        logger.info("Collecting dry/wet data from Arkansas GIS Feature Service")

        try:
            time.sleep(self.request_delay)
            response = self._http.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except (requests.RequestException, ValueError) as e:
            logger.error(f"Failed to query Arkansas GIS: {e}")
            return self._empty_frame()

        records = []
        features = data.get("features", [])

        for feature in features:
            attrs = feature.get("attributes", {})
            name = str(attrs.get("NAME", "")).strip()
            wet_dry = str(attrs.get("WET_DRY", "")).strip().lower()
            fips = str(attrs.get("FIPS", "")).strip()

            if not name:
                continue

            status = self._normalize_status(wet_dry)

            # Build GEOID: Arkansas state FIPS = 05, county FIPS from the feature
            geoid = fips if fips else None
            if geoid and len(geoid) < 5:
                geoid = geoid.zfill(5)

            # If no FIPS in data, try to resolve via matcher
            if not geoid and self.geoid_matcher:
                geoid = self.geoid_matcher.match(
                    "05", name, jurisdiction_type="county"
                )

            records.append({
                "geoid": geoid or "",
                "state_fips": "05",
                "jurisdiction_name": name,
                "dry_wet_status": status,
                "restriction_notes": None,
                "data_source": "arkansas_gis",
                "last_verified": str(date.today()),
            })

        df = self._records_to_frame(records)
        logger.info(f"  Arkansas GIS: collected {len(df)} dry/wet records")
        self._collected_frames.append(df)
        return df

    def collect_from_alabama_abc(self) -> pd.DataFrame:
        """Scrape Alabama ABC Board for wet/dry county and city status.

        The Alabama ABC Board publishes a list of wet cities within
        otherwise dry counties.

        Returns:
            DataFrame with standard dry/wet columns.
        """
        url = "https://abc.alabama.gov/wet-dry-status"
        logger.info(f"Collecting dry/wet data from Alabama ABC: {url}")

        try:
            time.sleep(self.request_delay)
            response = self._http.get(url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch Alabama ABC page: {e}")
            return self._empty_frame()

        soup = BeautifulSoup(response.text, "html.parser")
        records = []

        # Alabama ABC typically presents data in tables
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            for tr in rows[1:]:  # Skip header row
                cells = tr.find_all(["td", "th"])
                if len(cells) >= 2:
                    name = cells[0].get_text(strip=True)
                    status_text = cells[1].get_text(strip=True).lower()
                    notes = cells[2].get_text(strip=True) if len(cells) > 2 else ""

                    if not name:
                        continue

                    status = self._normalize_status(status_text)
                    geoid = None
                    if self.geoid_matcher:
                        geoid = self.geoid_matcher.match(
                            "01", name, jurisdiction_type="county"
                        )

                    records.append({
                        "geoid": geoid or "",
                        "state_fips": "01",
                        "jurisdiction_name": name,
                        "dry_wet_status": status,
                        "restriction_notes": notes or None,
                        "data_source": "alabama_abc",
                        "last_verified": str(date.today()),
                    })

        df = self._records_to_frame(records)
        logger.info(f"  Alabama ABC: collected {len(df)} dry/wet records")
        self._collected_frames.append(df)
        return df

    def add_manual_entries(self, entries: List[Dict]) -> pd.DataFrame:
        """Add manually curated dry/wet entries.

        Useful for data from PDFs or other non-machine-readable sources
        that have been manually transcribed.

        Args:
            entries: List of dicts with standard dry/wet columns.

        Returns:
            DataFrame with standard dry/wet columns.
        """
        df = self._records_to_frame(entries)
        logger.info(f"  Manual: added {len(df)} dry/wet entries")
        self._collected_frames.append(df)
        return df

    def export_combined(self, output_path: Path) -> pd.DataFrame:
        """Merge all collected data, deduplicate by GEOID, and export CSV.

        When multiple sources provide data for the same GEOID, the first
        source added takes priority (earliest in collection order).

        Args:
            output_path: Path to write the combined CSV.

        Returns:
            Combined DataFrame.
        """
        if not self._collected_frames:
            logger.warning("No data collected — nothing to export")
            return self._empty_frame()

        combined = pd.concat(self._collected_frames, ignore_index=True)

        # Remove entries without GEOIDs (could not be resolved)
        unresolved = combined[combined["geoid"] == ""]
        if len(unresolved) > 0:
            logger.warning(
                f"  {len(unresolved)} entries could not be resolved to GEOIDs:"
            )
            for _, row in unresolved.iterrows():
                logger.warning(
                    f"    {row['state_fips']} | {row['jurisdiction_name']} | "
                    f"source={row['data_source']}"
                )

        combined = combined[combined["geoid"] != ""]

        # Deduplicate by GEOID (keep first occurrence = higher priority source)
        before_dedup = len(combined)
        combined = combined.drop_duplicates(subset=["geoid"], keep="first")
        dupes_removed = before_dedup - len(combined)
        if dupes_removed > 0:
            logger.info(f"  Removed {dupes_removed} duplicate GEOID entries")

        # Filter to only non-wet entries (the seed CSV is sparse)
        combined = combined[combined["dry_wet_status"] != "wet"]

        # Sort by geoid for readability
        combined = combined.sort_values("geoid").reset_index(drop=True)

        # Write CSV
        output_path.parent.mkdir(parents=True, exist_ok=True)
        combined.to_csv(output_path, index=False)
        logger.info(f"  Exported {len(combined)} dry/moist records to {output_path}")

        return combined

    # ---- Private helpers ----

    def _parse_wikipedia_table(
        self, table, state_fips: str, state_name: str
    ) -> List[Dict]:
        """Parse a Wikipedia HTML table for dry/wet records."""
        records = []
        rows = table.find_all("tr")
        if not rows:
            return records

        # Try to identify column headers
        headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]

        for tr in rows[1:]:
            cells = tr.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            name = cells[0].get_text(strip=True)
            status_text = cells[1].get_text(strip=True).lower() if len(cells) > 1 else ""

            if not name or len(name) < 2:
                continue

            status = self._normalize_status(status_text)
            geoid = None
            if self.geoid_matcher:
                geoid = self.geoid_matcher.match(state_fips, name, jurisdiction_type="county")

            records.append({
                "geoid": geoid or "",
                "state_fips": state_fips,
                "jurisdiction_name": name,
                "dry_wet_status": status,
                "restriction_notes": None,
                "data_source": "wikipedia",
                "last_verified": str(date.today()),
            })

        return records

    def _parse_wikipedia_list(
        self, ul, state_fips: str, state_name: str
    ) -> List[Dict]:
        """Parse a Wikipedia unordered list for dry county names."""
        records = []
        for li in ul.find_all("li", recursive=False):
            text = li.get_text(strip=True)
            if not text or len(text) < 2:
                continue

            # Extract county name (usually first part before parenthetical notes)
            name_match = re.match(r"^([A-Za-z\s\.\'-]+?)(?:\s*[\(\[]|$)", text)
            if not name_match:
                continue

            name = name_match.group(1).strip()
            # Remove common suffixes
            for suffix in [" County", " Parish"]:
                if name.endswith(suffix):
                    name = name[: -len(suffix)]

            # Infer status from context (most Wikipedia lists are about dry jurisdictions)
            status = "dry"
            if "moist" in text.lower() or "partial" in text.lower():
                status = "moist"
            elif "wet" in text.lower():
                status = "wet"

            # Extract notes from parenthetical
            notes_match = re.search(r"\((.+?)\)", text)
            notes = notes_match.group(1) if notes_match else None

            geoid = None
            if self.geoid_matcher:
                geoid = self.geoid_matcher.match(state_fips, name, jurisdiction_type="county")

            records.append({
                "geoid": geoid or "",
                "state_fips": state_fips,
                "jurisdiction_name": name,
                "dry_wet_status": status,
                "restriction_notes": notes,
                "data_source": "wikipedia",
                "last_verified": str(date.today()),
            })

        return records

    def _parse_generic_table(self, table_df: pd.DataFrame) -> List[Dict]:
        """Parse a generic pandas table looking for dry/wet data."""
        records = []

        # Check if this table has county/status-like columns
        cols_lower = {c.lower(): c for c in table_df.columns}
        name_col = None
        status_col = None

        for key in ["county", "jurisdiction", "name", "area"]:
            if key in cols_lower:
                name_col = cols_lower[key]
                break

        for key in ["status", "wet/dry", "dry/wet", "classification"]:
            if key in cols_lower:
                status_col = cols_lower[key]
                break

        if not name_col or not status_col:
            return records

        for _, row in table_df.iterrows():
            name = str(row[name_col]).strip()
            status_raw = str(row[status_col]).strip().lower()

            if not name or name == "nan":
                continue

            status = self._normalize_status(status_raw)
            records.append({
                "geoid": "",
                "state_fips": "",
                "jurisdiction_name": name,
                "dry_wet_status": status,
                "restriction_notes": None,
                "data_source": "wikipedia_table",
                "last_verified": str(date.today()),
            })

        return records

    @staticmethod
    def _normalize_status(status_text: str) -> str:
        """Normalize various dry/wet status strings to standard values.

        Args:
            status_text: Raw status text from source.

        Returns:
            One of: 'dry', 'wet', 'moist'.
        """
        text = status_text.lower().strip()

        if text in ("dry", "completely dry", "totally dry", "all dry"):
            return "dry"
        elif text in ("wet", "completely wet", "all wet", "legal", "yes"):
            return "wet"
        elif any(kw in text for kw in ["moist", "partial", "some", "beer", "wine"]):
            return "moist"
        elif text in ("no", "prohibited", "banned"):
            return "dry"
        else:
            # Default to dry for ambiguous status in a dry/wet context
            return "dry"

    @staticmethod
    def _empty_frame() -> pd.DataFrame:
        """Return an empty DataFrame with standard columns."""
        return pd.DataFrame(columns=DRY_WET_COLUMNS)

    @staticmethod
    def _records_to_frame(records: List[Dict]) -> pd.DataFrame:
        """Convert a list of record dicts to a DataFrame with standard columns."""
        if not records:
            return pd.DataFrame(columns=DRY_WET_COLUMNS)
        df = pd.DataFrame(records)
        # Ensure all standard columns exist
        for col in DRY_WET_COLUMNS:
            if col not in df.columns:
                df[col] = None
        return df[DRY_WET_COLUMNS]
