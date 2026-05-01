"""Wrapper around pygris for downloading Census TIGER/Line shapefiles.

Provides a clean interface to download county, place, and county subdivision
data on a per-state basis, with caching support.
"""

import logging
from typing import Dict, Optional

import geopandas as gpd
import pandas as pd

from src.census.cache import DownloadManifest
from src.common.constants import CDP_CLASS_CODES, STRONG_MCD_STATES
from src.common.enums import GeoLayer

logger = logging.getLogger("jurisdiction.tiger")


class TigerClient:
    """Downloads Census TIGER/Line data via pygris with caching."""

    def __init__(
        self,
        manifest: DownloadManifest,
        year: int = 2023,
        resolution: str = "500k",
        force_redownload: bool = False,
    ) -> None:
        self.manifest = manifest
        self.year = year
        self.resolution = resolution
        self.force_redownload = force_redownload

    def get_counties(self, state_fips: str) -> pd.DataFrame:
        """Download county-equivalent jurisdictions for a state.

        Returns a DataFrame (not GeoDataFrame) with tabular attributes only.
        Geometry is dropped since we only need reference data.
        """
        layer = GeoLayer.COUNTY.value

        if not self.force_redownload and self.manifest.is_downloaded(
            state_fips, layer, self.year
        ):
            logger.info(f"Skipping {state_fips} counties (already downloaded)")
            return pd.DataFrame()

        import pygris

        logger.info(f"Downloading counties for state {state_fips}, year {self.year}")
        gdf: gpd.GeoDataFrame = pygris.counties(
            state=state_fips, year=self.year, cache=True
        )

        df = self._geodf_to_df(gdf)
        self.manifest.mark_downloaded(state_fips, layer, self.year, len(df))
        return df

    def get_places(self, state_fips: str) -> pd.DataFrame:
        """Download incorporated places for a state.

        Filters out Census Designated Places (CDPs) which have no
        governmental authority.
        """
        layer = GeoLayer.PLACE.value

        if not self.force_redownload and self.manifest.is_downloaded(
            state_fips, layer, self.year
        ):
            logger.info(f"Skipping {state_fips} places (already downloaded)")
            return pd.DataFrame()

        import pygris

        logger.info(f"Downloading places for state {state_fips}, year {self.year}")
        gdf: gpd.GeoDataFrame = pygris.places(
            state=state_fips, year=self.year, cache=True
        )

        df = self._geodf_to_df(gdf)

        # Filter out CDPs
        if "CLASSFP" in df.columns:
            pre_filter = len(df)
            df = df[~df["CLASSFP"].isin(CDP_CLASS_CODES)]
            logger.info(
                f"Filtered CDPs: {pre_filter} -> {len(df)} places for state {state_fips}"
            )

        self.manifest.mark_downloaded(state_fips, layer, self.year, len(df))
        return df

    def get_county_subdivisions(self, state_fips: str) -> pd.DataFrame:
        """Download county subdivisions (MCDs/townships) for a state.

        Should only be called for strong-MCD states.
        """
        layer = GeoLayer.COUNTY_SUBDIVISION.value

        if state_fips not in STRONG_MCD_STATES:
            logger.warning(
                f"State {state_fips} is not a strong-MCD state; "
                f"county subdivision download may not be needed"
            )

        if not self.force_redownload and self.manifest.is_downloaded(
            state_fips, layer, self.year
        ):
            logger.info(f"Skipping {state_fips} county subdivisions (already downloaded)")
            return pd.DataFrame()

        import pygris

        logger.info(
            f"Downloading county subdivisions for state {state_fips}, year {self.year}"
        )
        gdf: gpd.GeoDataFrame = pygris.county_subdivisions(
            state=state_fips, year=self.year, cache=True
        )

        df = self._geodf_to_df(gdf)
        self.manifest.mark_downloaded(state_fips, layer, self.year, len(df))
        return df

    def get_all_for_state(
        self,
        state_fips: str,
        delegates_to_county: bool = True,
        delegates_to_municipality: bool = False,
        delegates_to_mcd: bool = False,
    ) -> Dict[str, pd.DataFrame]:
        """Download all needed TIGER layers for a state based on classification.

        Args:
            state_fips: 2-digit state FIPS code.
            delegates_to_county: Whether to download counties (usually True).
            delegates_to_municipality: Whether to download places.
            delegates_to_mcd: Whether to download county subdivisions.

        Returns:
            Dict mapping layer name to DataFrame.
        """
        result: Dict[str, pd.DataFrame] = {}

        if delegates_to_county:
            result[GeoLayer.COUNTY.value] = self.get_counties(state_fips)

        if delegates_to_municipality:
            result[GeoLayer.PLACE.value] = self.get_places(state_fips)

        if delegates_to_mcd:
            result[GeoLayer.COUNTY_SUBDIVISION.value] = self.get_county_subdivisions(
                state_fips
            )

        return result

    @staticmethod
    def _geodf_to_df(gdf: gpd.GeoDataFrame) -> pd.DataFrame:
        """Convert a GeoDataFrame to a regular DataFrame, dropping geometry."""
        if "geometry" in gdf.columns:
            return pd.DataFrame(gdf.drop(columns=["geometry"]))
        return pd.DataFrame(gdf)
