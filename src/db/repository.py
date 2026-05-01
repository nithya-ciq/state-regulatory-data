import logging
from typing import Any, Dict, List, Optional, Type, TypeVar

from sqlalchemy import inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.models.base import Base

logger = logging.getLogger("jurisdiction.repository")

T = TypeVar("T", bound=Base)


class Repository:
    """Generic repository with bulk upsert support for jurisdiction models."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def bulk_upsert(
        self,
        model: Type[T],
        records: List[Dict[str, Any]],
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None,
    ) -> int:
        """Bulk upsert records using PostgreSQL ON CONFLICT.

        Args:
            model: The SQLAlchemy model class.
            records: List of dicts to upsert.
            conflict_columns: Columns that define the unique constraint for conflict detection.
            update_columns: Columns to update on conflict. If None, updates all non-PK columns.

        Returns:
            Number of records processed.
        """
        if not records:
            return 0

        table = model.__table__

        if update_columns is None:
            pk_cols = {col.name for col in inspect(model).primary_key}
            conflict_set = set(conflict_columns)
            update_columns = [
                col.name
                for col in table.columns
                if col.name not in pk_cols and col.name not in conflict_set
            ]

        stmt = pg_insert(table).values(records)

        if update_columns:
            update_dict = {col: stmt.excluded[col] for col in update_columns}
            stmt = stmt.on_conflict_do_update(
                index_elements=conflict_columns,
                set_=update_dict,
            )
        else:
            stmt = stmt.on_conflict_do_nothing(index_elements=conflict_columns)

        self.session.execute(stmt)
        logger.info(f"Upserted {len(records)} records into {model.__tablename__}")
        return len(records)

    def count(self, model: Type[T]) -> int:
        """Return the total row count for a model."""
        return self.session.query(model).count()

    def get_by_pk(self, model: Type[T], pk_value: Any) -> Optional[T]:
        """Get a single record by primary key."""
        return self.session.get(model, pk_value)

    def get_all(self, model: Type[T]) -> List[T]:
        """Get all records for a model."""
        return self.session.query(model).all()

    def delete_all(self, model: Type[T]) -> int:
        """Delete all records for a model. Returns count deleted."""
        count = self.session.query(model).delete()
        logger.info(f"Deleted {count} records from {model.__tablename__}")
        return count
