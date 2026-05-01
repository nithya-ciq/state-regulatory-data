from sqlalchemy import BigInteger, Boolean, Column, DateTime, Integer, Numeric, String
from sqlalchemy import UniqueConstraint, func

from src.models.base import Base, TimestampMixin


class Jurisdiction(Base, TimestampMixin):
    """Final output table: one row per licensing authority.

    This is the primary deliverable of the pipeline — a FIPS-coded
    jurisdiction dataset for market access intelligence.
    """

    __tablename__ = "jurisdictions"
    __table_args__ = (
        UniqueConstraint(
            "geoid", "jurisdiction_type", "census_year", name="uq_jurisdiction_natural_key"
        ),
        {"schema": "jurisdiction"},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    geoid = Column(String(15), nullable=False, index=True)
    jurisdiction_type = Column(String(25), nullable=False, index=True)
    tier = Column(String(10), nullable=False, index=True)

    # FIPS breakdown (no FK to state_classifications because the federal row uses "00")
    state_fips = Column(String(2), nullable=False, index=True)
    county_fips = Column(String(3), nullable=True)
    place_fips = Column(String(5), nullable=True)
    cousub_fips = Column(String(5), nullable=True)

    # Names
    jurisdiction_name = Column(String(200), nullable=False)
    jurisdiction_name_lsad = Column(String(200), nullable=True)
    state_abbr = Column(String(2), nullable=False)
    state_name = Column(String(50), nullable=False)
    county_name = Column(String(100), nullable=True)

    # Licensing attributes
    has_licensing_authority = Column(Boolean, nullable=False, default=True)
    licensing_authority_name = Column(String(200), nullable=True)
    licensing_authority_type = Column(String(30), nullable=True)  # dedicated_board, general_government, state_agency
    licensing_authority_confidence = Column(String(20), nullable=True)  # verified, generated, unknown
    is_dry = Column(Boolean, nullable=False, default=False)
    dry_wet_status = Column(String(10), nullable=True, default="wet")
    dry_wet_data_source = Column(String(100), nullable=True)  # provenance for dry/wet status

    # Classification context (inherited from state)
    control_status = Column(String(20), nullable=False)
    delegation_pattern = Column(String(50), nullable=True)

    # Regulatory details (propagated from state classification)
    three_tier_enforcement = Column(String(30), nullable=True)
    sunday_sales_allowed = Column(Boolean, nullable=True)
    grocery_beer_allowed = Column(Boolean, nullable=True)
    grocery_wine_allowed = Column(Boolean, nullable=True)
    beer_max_abv = Column(Numeric(4, 2), nullable=True)
    regulatory_override_source = Column(String(100), nullable=True)

    # Geographic
    land_area_sqm = Column(BigInteger, nullable=True)
    latitude = Column(Numeric(10, 7), nullable=True)
    longitude = Column(Numeric(10, 7), nullable=True)

    # Virginia independent city flag
    is_independent_city = Column(Boolean, nullable=False, default=False)

    # Provenance
    census_year = Column(Integer, nullable=False)
    pipeline_run_id = Column(Integer, nullable=True)
    data_source = Column(String(100), nullable=True)

    def __repr__(self) -> str:
        return (
            f"<Jurisdiction(geoid={self.geoid!r}, type={self.jurisdiction_type!r}, "
            f"name={self.jurisdiction_name!r})>"
        )
