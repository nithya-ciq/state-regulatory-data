"""Dagster resource wrapping the Census TIGER data client."""

import dagster as dg
from pathlib import Path

from src.census.cache import DownloadManifest
from src.census.tiger_client import TigerClient


class TigerClientResource(dg.ConfigurableResource):
    """Provides a configured TigerClient instance to Dagster assets.

    Wraps the DownloadManifest + TigerClient construction that the
    orchestrator currently does manually in Phase 2.
    """

    census_year: int = 2023
    tiger_resolution: str = "500k"
    cache_dir: str = "data/cache"
    force_redownload: bool = False

    def create_client(self) -> TigerClient:
        """Create a configured TigerClient instance."""
        cache_path = Path(self.cache_dir)
        cache_path.mkdir(parents=True, exist_ok=True)
        manifest = DownloadManifest(cache_path)
        return TigerClient(
            manifest=manifest,
            year=self.census_year,
            resolution=self.tiger_resolution,
            force_redownload=self.force_redownload,
        )
