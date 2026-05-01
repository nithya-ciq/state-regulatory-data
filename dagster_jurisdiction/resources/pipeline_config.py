"""Dagster resource wrapping the existing Pydantic Config."""

import dagster as dg
from pathlib import Path

from src.config import Config


class PipelineConfigResource(dg.ConfigurableResource):
    """Exposes pipeline configuration as a Dagster resource.

    Mirrors all fields from src/config.py as flat types that Dagster
    can render in its UI. The to_config() method reconstructs the
    original Config object that all phase code expects.
    """

    database_url: str = (
        "postgresql://jurisdiction_user:ciq-eeaao~1@localhost:5432/jurisdiction_db"
    )
    census_year: int = 2023
    tiger_resolution: str = "500k"
    cache_dir: str = "data/cache"
    output_dir: str = "data/output"
    seed_dir: str = "data/seed"
    log_level: str = "INFO"
    skip_territories: bool = False
    include_dry_status: bool = True
    include_regulatory_details: bool = True
    force_redownload: bool = False

    def to_config(self) -> Config:
        """Convert to the existing Config object that phase code expects."""
        return Config(
            database_url=self.database_url,
            census_year=self.census_year,
            tiger_resolution=self.tiger_resolution,
            cache_dir=Path(self.cache_dir),
            output_dir=Path(self.output_dir),
            seed_dir=Path(self.seed_dir),
            log_level=self.log_level,
            skip_territories=self.skip_territories,
            include_dry_status=self.include_dry_status,
            include_regulatory_details=self.include_regulatory_details,
            force_redownload=self.force_redownload,
        )
