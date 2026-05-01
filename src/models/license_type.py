from sqlalchemy import Boolean, Column, Date, Integer, Numeric, String, Text, UniqueConstraint

from src.models.base import Base, TimestampMixin


class LicenseType(Base, TimestampMixin):
    """Per-state license type catalog.

    Each row represents one alcohol license category for one state.
    States typically have 5-15 distinct license types covering
    on-premise, off-premise, manufacturing, distribution, and
    specialty categories.
    """

    __tablename__ = "license_types"
    __table_args__ = (
        UniqueConstraint(
            "state_fips",
            "license_type_code",
            name="uq_license_type_natural_key",
        ),
        {"schema": "jurisdiction"},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    state_fips = Column(String(2), nullable=False, index=True)
    license_type_code = Column(String(50), nullable=False)
    license_type_name = Column(String(200), nullable=False)
    license_category = Column(String(30), nullable=False)

    # What this license permits
    permits_on_premise = Column(Boolean, nullable=False, default=False)
    permits_off_premise = Column(Boolean, nullable=False, default=False)
    permits_beer = Column(Boolean, nullable=False, default=False)
    permits_wine = Column(Boolean, nullable=False, default=False)
    permits_spirits = Column(Boolean, nullable=False, default=False)

    # Retail channel and limits
    retail_channel = Column(String(50), nullable=True)
    abv_limit = Column(Numeric(4, 2), nullable=True)

    # License availability
    quota_limited = Column(Boolean, nullable=True)
    quota_notes = Column(String(200), nullable=True)
    transferable = Column(Boolean, nullable=True)
    annual_fee_range = Column(String(50), nullable=True)

    # Issuing details
    issuing_authority = Column(String(100), nullable=True)
    statutory_reference = Column(String(200), nullable=True)
    notes = Column(Text, nullable=True)

    # Research provenance
    research_status = Column(String(20), nullable=False, default="pending")
    research_source = Column(String(500), nullable=True)
    last_verified_date = Column(Date, nullable=True)

    def __repr__(self) -> str:
        return (
            f"<LicenseType(state_fips={self.state_fips!r}, "
            f"code={self.license_type_code!r}, "
            f"name={self.license_type_name!r})>"
        )
