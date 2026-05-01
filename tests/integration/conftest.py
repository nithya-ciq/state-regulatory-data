"""Integration test fixtures — provide a live DB session with rollback isolation.

Each test runs inside a SAVEPOINT so the database is never modified by tests.
Requires a running PostgreSQL instance with the jurisdiction schema populated.
"""

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session, sessionmaker

from src.config import Config


@pytest.fixture(scope="session")
def db_engine():
    """Create a single engine for the entire test session."""
    config = Config()
    engine = create_engine(config.database_url, echo=False)

    # Verify the schema exists and has data
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM jurisdiction.jurisdictions")
        )
        count = result.scalar()
        if count == 0:
            pytest.skip("No jurisdiction data in database — run the pipeline first")

    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def integration_config():
    """Provide the standard Config for integration tests."""
    return Config()


@pytest.fixture()
def db_session(db_engine):
    """Provide an isolated DB session that rolls back after each test.

    Uses nested transactions (SAVEPOINTs) so tests can call session.commit()
    without permanently altering the database.
    """
    connection = db_engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)

    # Start a SAVEPOINT — all test commits go here, not to the real DB
    nested = connection.begin_nested()

    @event.listens_for(session, "after_transaction_end")
    def restart_savepoint(session, transaction_record):
        """Re-open a SAVEPOINT if the test code commits or rolls back."""
        nonlocal nested
        if not nested.is_active:
            nested = connection.begin_nested()

    yield session

    # Rollback everything — no changes persist
    session.close()
    transaction.rollback()
    connection.close()
