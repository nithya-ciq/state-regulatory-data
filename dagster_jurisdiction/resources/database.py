"""Dagster resource wrapping the existing database session management."""

import dagster as dg
from sqlalchemy.orm import Session

from src.config import Config
from src.db.session import create_db_engine, create_session_factory


class DatabaseResource(dg.ConfigurableResource):
    """Provides SQLAlchemy sessions to Dagster assets.

    Wraps the existing src/db/session.py functions so that
    each asset can manage its own transaction lifecycle
    (commits, rollbacks) exactly as the phase code expects.
    """

    database_url: str = (
        "postgresql://jurisdiction_user:ciq-eeaao~1@localhost:5432/jurisdiction_db"
    )

    def get_session(self) -> Session:
        """Create and return a new SQLAlchemy Session.

        The caller is responsible for committing/rolling back and closing.
        This preserves the per-phase transaction pattern used by existing code.
        """
        config = Config(database_url=self.database_url)
        engine = create_db_engine(config)
        factory = create_session_factory(engine)
        return factory()
