from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.config import Config


def create_db_engine(config: Config) -> Engine:
    """Create a SQLAlchemy engine from configuration."""
    return create_engine(
        config.database_url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
        echo=config.log_level == "DEBUG",
    )


def create_session_factory(engine: Engine) -> sessionmaker:
    """Create a session factory bound to the given engine."""
    return sessionmaker(bind=engine, expire_on_commit=False)


@contextmanager
def get_session(config: Config) -> Generator[Session, None, None]:
    """Provide a transactional session scope.

    Usage:
        with get_session(config) as session:
            session.query(...)
    """
    engine = create_db_engine(config)
    factory = create_session_factory(engine)
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
