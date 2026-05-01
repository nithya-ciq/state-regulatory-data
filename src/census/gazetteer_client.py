"""Client for downloading and parsing Census Gazetteer reference files.

Gazetteer files are pipe-delimited text files providing supplementary
geographic reference data (names, coordinates, land area) for counties,
places, and county subdivisions.
"""

import io
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

logger = logging.getLogger("jurisdiction.gazetteer")

# Census Gazetteer base URL pattern
GAZETTEER_BASE_URL = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer"


class GazetteerClient:
    """Downloads and parses Census Bureau Gazetteer files."""

    def __init__(self, cache_dir: Path, year: int = 2023) -> None:
        self.cache_dir = cache_dir
        self.year = year

    def get_counties(self) -> pd.DataFrame:
        """Download and parse the counties Gazetteer file."""
        return self._fetch_gazetteer("counties")

    def get_places(self) -> pd.DataFrame:
        """Download and parse the places Gazetteer file."""
        return self._fetch_gazetteer("place")

    def get_county_subdivisions(self) -> pd.DataFrame:
        """Download and parse the county subdivisions Gazetteer file."""
        return self._fetch_gazetteer("cousubs")

    def _fetch_gazetteer(self, geo_type: str) -> pd.DataFrame:
        """Fetch a Gazetteer file, using local cache if available.

        Args:
            geo_type: One of 'counties', 'place', 'cousubs'.

        Returns:
            DataFrame with Gazetteer data.
        """
        cache_path = self._get_cache_path(geo_type)

        if cache_path.exists():
            logger.info(f"Loading {geo_type} Gazetteer from cache: {cache_path}")
            return pd.read_csv(cache_path, sep="\t", dtype=str)

        url = self._build_url(geo_type)
        logger.info(f"Downloading {geo_type} Gazetteer from {url}")

        response = requests.get(url, timeout=60)
        response.raise_for_status()

        df = pd.read_csv(io.StringIO(response.text), sep="\t", dtype=str)

        # Strip whitespace from column names
        df.columns = df.columns.str.strip()

        # Cache locally
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(cache_path, sep="\t", index=False)
        logger.info(f"Cached {len(df)} {geo_type} Gazetteer records to {cache_path}")

        return df

    def _build_url(self, geo_type: str) -> str:
        """Build the Gazetteer download URL for the given year and type."""
        filename_map = {
            "counties": f"{self.year}_Gaz_counties_national.txt",
            "place": f"{self.year}_Gaz_place_national.txt",
            "cousubs": f"{self.year}_Gaz_cousubs_national.txt",
        }
        filename = filename_map.get(geo_type)
        if not filename:
            raise ValueError(f"Unknown Gazetteer type: {geo_type}")
        return f"{GAZETTEER_BASE_URL}/{self.year}/{filename}"

    def _get_cache_path(self, geo_type: str) -> Path:
        """Get the local cache file path for a Gazetteer type."""
        return self.cache_dir / "gazetteer" / f"{geo_type}_{self.year}.tsv"
