from sqlalchemy import Boolean, Column, Date, Integer, Numeric, String, Text

from src.models.base import Base, TimestampMixin


class StateClassification(Base, TimestampMixin):
    """56-row state classification matrix driving the pipeline.

    Each row represents a US state, DC, or territory and captures
    how that jurisdiction delegates alcohol licensing authority.
    """

    __tablename__ = "state_classifications"
    __table_args__ = {"schema": "jurisdiction"}

    state_fips = Column(String(2), primary_key=True)
    state_abbr = Column(String(2), nullable=False, unique=True)
    state_name = Column(String(50), nullable=False)
    is_territory = Column(Boolean, nullable=False, default=False)

    # Regulatory classification
    control_status = Column(String(20), nullable=False)  # control, license, hybrid
    has_local_licensing = Column(Boolean, nullable=False)

    # Delegation pattern flags
    delegates_to_county = Column(Boolean, nullable=False, default=False)
    delegates_to_municipality = Column(Boolean, nullable=False, default=False)
    delegates_to_mcd = Column(Boolean, nullable=False, default=False)
    is_strong_mcd_state = Column(Boolean, nullable=False, default=False)

    # Dry/wet local option
    has_local_option_law = Column(Boolean, nullable=False, default=False)
    local_option_level = Column(String(30), nullable=True)  # county, municipality, precinct

    # State ABC agency metadata
    abc_agency_name = Column(String(200), nullable=True)
    abc_agency_url = Column(String(500), nullable=True)

    # Three-tier enforcement
    three_tier_enforcement = Column(String(30), nullable=True)
    three_tier_notes = Column(Text, nullable=True)

    # License types
    has_on_premise_license = Column(Boolean, nullable=True)
    has_off_premise_license = Column(Boolean, nullable=True)
    has_manufacturer_license = Column(Boolean, nullable=True)
    has_distributor_license = Column(Boolean, nullable=True)

    # Sunday sales
    sunday_sales_allowed = Column(Boolean, nullable=True)
    sunday_sales_hours = Column(String(100), nullable=True)
    sunday_sales_notes = Column(String(500), nullable=True)

    # Grocery/convenience store rules
    grocery_beer_allowed = Column(Boolean, nullable=True)
    grocery_wine_allowed = Column(Boolean, nullable=True)
    convenience_beer_allowed = Column(Boolean, nullable=True)
    convenience_wine_allowed = Column(Boolean, nullable=True)
    grocery_store_notes = Column(String(500), nullable=True)

    # Beer ABV limits
    beer_max_abv = Column(Numeric(4, 2), nullable=True)
    beer_abv_notes = Column(String(500), nullable=True)

    # Liquor in grocery/convenience (separate from beer/wine)
    grocery_liquor_allowed = Column(Boolean, nullable=True)
    convenience_liquor_allowed = Column(Boolean, nullable=True)

    # Granular control flags
    spirits_control = Column(Boolean, nullable=True)
    wine_control = Column(Boolean, nullable=True)
    beer_control = Column(Boolean, nullable=True)
    wholesale_control = Column(Boolean, nullable=True)
    retail_control = Column(Boolean, nullable=True)

    # Data effective date
    data_effective_date = Column(String(50), nullable=True)

    # Confidence scoring for binary fields
    grocery_beer_confidence = Column(String(10), nullable=True)
    grocery_wine_confidence = Column(String(10), nullable=True)
    grocery_liquor_confidence = Column(String(10), nullable=True)

    # Detailed retail channel notes
    retail_channel_notes = Column(Text, nullable=True)

    # License type metadata (computed from license_types table)
    license_type_count = Column(Integer, nullable=True)
    license_complexity_tier = Column(String(10), nullable=True)

    # Research provenance
    research_status = Column(String(20), nullable=False, default="pending")
    research_source = Column(String(500), nullable=True)
    research_notes = Column(Text, nullable=True)
    last_verified_date = Column(Date, nullable=True)

    def __repr__(self) -> str:
        return (
            f"<StateClassification(state_fips={self.state_fips!r}, "
            f"state_abbr={self.state_abbr!r}, control_status={self.control_status!r})>"
        )
