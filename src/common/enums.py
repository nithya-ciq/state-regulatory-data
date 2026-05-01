from enum import Enum


class JurisdictionType(str, Enum):
    FEDERAL = "federal"
    STATE = "state"
    COUNTY = "county"
    MUNICIPALITY = "municipality"
    MCD = "mcd"
    TERRITORY = "territory"
    INDEPENDENT_CITY = "independent_city"


class Tier(str, Enum):
    FEDERAL = "federal"
    STATE = "state"
    LOCAL = "local"


class ControlStatus(str, Enum):
    CONTROL = "control"
    LICENSE = "license"
    HYBRID = "hybrid"


class DelegationPattern(str, Enum):
    STATE_ONLY = "state_only"
    COUNTY = "county"
    MUNICIPALITY = "municipality"
    COUNTY_AND_MUNICIPALITY = "county_and_municipality"
    MCD = "mcd"
    COUNTY_AND_MCD = "county_and_mcd"
    ALL_LEVELS = "all_levels"


class ThreeTierEnforcement(str, Enum):
    STRICT = "strict"
    MODIFIED = "modified"
    FRANCHISE = "franchise"


class DryWetStatus(str, Enum):
    DRY = "dry"
    WET = "wet"
    MOIST = "moist"


class ResearchStatus(str, Enum):
    PENDING = "pending"
    DRAFT = "draft"
    VERIFIED = "verified"
    NEEDS_REVIEW = "needs_review"


class GeoLayer(str, Enum):
    COUNTY = "county"
    PLACE = "place"
    COUNTY_SUBDIVISION = "county_subdivision"


class PipelineStatus(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"


class LicenseCategory(str, Enum):
    ON_PREMISE = "on_premise"
    OFF_PREMISE = "off_premise"
    DUAL = "dual"
    MANUFACTURER = "manufacturer"
    DISTRIBUTOR = "distributor"
    WHOLESALER = "wholesaler"
    SPECIAL_EVENT = "special_event"
    FARM_WINERY = "farm_winery"
    CRAFT_BREWERY = "craft_brewery"
    OTHER = "other"


class RetailChannel(str, Enum):
    LIQUOR_STORE = "liquor_store"
    BAR_RESTAURANT = "bar_restaurant"
    GROCERY = "grocery"
    CONVENIENCE = "convenience"
    PHARMACY = "pharmacy"
    HOTEL = "hotel"
    CLUB = "club"
    GENERAL_RETAIL = "general_retail"
    ANY = "any"


class ConfidenceLevel(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class LicenseComplexityTier(str, Enum):
    SIMPLE = "simple"
    MODERATE = "moderate"
    COMPLEX = "complex"
