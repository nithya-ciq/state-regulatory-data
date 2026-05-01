from sqlalchemy import Column, DateTime, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID

from src.models.base import Base


class PipelineRun(Base):
    """Audit trail for each pipeline execution."""

    __tablename__ = "pipeline_runs"
    __table_args__ = {"schema": "jurisdiction"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(UUID(as_uuid=True), nullable=False, unique=True, server_default=func.gen_random_uuid())
    started_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    completed_at = Column(DateTime(timezone=True), nullable=True)
    status = Column(String(20), nullable=False, default="running")
    phase_reached = Column(String(50), nullable=True)
    census_year = Column(Integer, nullable=False)
    states_processed = Column(ARRAY(String), nullable=True)
    total_jurisdictions = Column(Integer, nullable=True)
    error_message = Column(Text, nullable=True)
    config_snapshot = Column(JSONB, nullable=True)

    def __repr__(self) -> str:
        return f"<PipelineRun(run_id={self.run_id!r}, status={self.status!r})>"
