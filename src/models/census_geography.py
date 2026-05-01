from sqlalchemy import BigInteger, Column, DateTime, ForeignKey, Integer, Numeric, String, func
from sqlalchemy import UniqueConstraint

from src.models.base import Base


class CensusGeography(Base):
    """Raw TIGER/Gazetteer data staging table.

    One row per geographic entity across all layers (county, place, county_subdivision).
    Preserved unmodified from Census source data.
    """

    __tablename__ = "census_geographies"
    __table_args__ = (
        UniqueConstraint("geoid", "geo_layer", "census_year", name="uq_census_geo_natural_key"),
        {"schema": "jurisdiction"},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    geoid = Column(String(15), nullable=False, index=True)
    geo_layer = Column(String(20), nullable=False, index=True)  # county, place, county_subdivision
    state_fips = Column(
        String(2),
        ForeignKey("jurisdiction.state_classifications.state_fips"),
        nullable=False,
        index=True,
    )
    county_fips = Column(String(3), nullable=True)
    place_fips = Column(String(5), nullable=True)
    cousub_fips = Column(String(5), nullable=True)

    name = Column(String(200), nullable=False)
    name_lsad = Column(String(200), nullable=True)
    lsad_code = Column(String(5), nullable=True)
    functional_status = Column(String(1), nullable=True)
    class_fips = Column(String(5), nullable=True)

    # Geographic attributes
    land_area_sqm = Column(BigInteger, nullable=True)
    water_area_sqm = Column(BigInteger, nullable=True)
    latitude = Column(Numeric(10, 7), nullable=True)
    longitude = Column(Numeric(10, 7), nullable=True)

    # Census vintage
    census_year = Column(Integer, nullable=False)

    # Audit
    pipeline_run_id = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    def __repr__(self) -> str:
        return (
            f"<CensusGeography(geoid={self.geoid!r}, geo_layer={self.geo_layer!r}, "
            f"name={self.name!r})>"
        )
