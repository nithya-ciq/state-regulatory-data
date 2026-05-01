from pathlib import Path

from pydantic_settings import BaseSettings


class Config(BaseSettings):
    """Application configuration loaded from environment variables / .env file."""

    # Database
    database_url: str = (
        "postgresql://jurisdiction_user:ciq-eeaao~1@localhost:5432/jurisdiction_db"
    )

    # Census settings
    census_year: int = 2023
    tiger_resolution: str = "500k"  # "500k" for faster downloads, "20m" for full resolution

    # Paths
    cache_dir: Path = Path("data/cache")
    output_dir: Path = Path("data/output")
    seed_dir: Path = Path("data/seed")

    # Logging
    log_level: str = "INFO"

    # Pipeline options
    skip_territories: bool = False
    include_dry_status: bool = True
    include_regulatory_details: bool = True
    force_redownload: bool = False

    model_config = {
        "env_file": ".env.local",
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
        "extra": "ignore",
    }

    def ensure_directories(self) -> None:
        """Create data directories if they don't exist."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
