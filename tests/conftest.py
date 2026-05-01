import pytest

from src.config import Config


@pytest.fixture
def config() -> Config:
    """Provide a test configuration with defaults."""
    return Config(
        database_url="postgresql://jurisdiction_user:ciq-eeaao~1@localhost:5432/jurisdiction_db",
        census_year=2023,
        log_level="DEBUG",
    )
