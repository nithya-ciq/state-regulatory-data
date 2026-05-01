from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text, func

from src.models.base import Base


class ResearchNote(Base):
    """Evidence log for the semi-automated state research process."""

    __tablename__ = "research_notes"
    __table_args__ = {"schema": "jurisdiction"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    state_fips = Column(
        String(2),
        ForeignKey("jurisdiction.state_classifications.state_fips"),
        nullable=False,
    )
    source_url = Column(String(500), nullable=True)
    source_type = Column(String(30), nullable=False)  # nabca, state_abc_website, statute, manual
    finding = Column(Text, nullable=False)
    confidence = Column(String(10), nullable=False, default="low")  # high, medium, low
    researcher = Column(String(100), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    def __repr__(self) -> str:
        return (
            f"<ResearchNote(state_fips={self.state_fips!r}, "
            f"source_type={self.source_type!r}, confidence={self.confidence!r})>"
        )
