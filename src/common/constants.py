"""Hard-coded reference data for US jurisdictions, FIPS codes, and regulatory classifications."""

from typing import Dict, FrozenSet, Tuple

# All 50 states + DC + 5 territories: FIPS code -> (abbreviation, name)
FIPS_STATES: Dict[str, Tuple[str, str]] = {
    "01": ("AL", "Alabama"),
    "02": ("AK", "Alaska"),
    "04": ("AZ", "Arizona"),
    "05": ("AR", "Arkansas"),
    "06": ("CA", "California"),
    "08": ("CO", "Colorado"),
    "09": ("CT", "Connecticut"),
    "10": ("DE", "Delaware"),
    "11": ("DC", "District of Columbia"),
    "12": ("FL", "Florida"),
    "13": ("GA", "Georgia"),
    "15": ("HI", "Hawaii"),
    "16": ("ID", "Idaho"),
    "17": ("IL", "Illinois"),
    "18": ("IN", "Indiana"),
    "19": ("IA", "Iowa"),
    "20": ("KS", "Kansas"),
    "21": ("KY", "Kentucky"),
    "22": ("LA", "Louisiana"),
    "23": ("ME", "Maine"),
    "24": ("MD", "Maryland"),
    "25": ("MA", "Massachusetts"),
    "26": ("MI", "Michigan"),
    "27": ("MN", "Minnesota"),
    "28": ("MS", "Mississippi"),
    "29": ("MO", "Missouri"),
    "30": ("MT", "Montana"),
    "31": ("NE", "Nebraska"),
    "32": ("NV", "Nevada"),
    "33": ("NH", "New Hampshire"),
    "34": ("NJ", "New Jersey"),
    "35": ("NM", "New Mexico"),
    "36": ("NY", "New York"),
    "37": ("NC", "North Carolina"),
    "38": ("ND", "North Dakota"),
    "39": ("OH", "Ohio"),
    "40": ("OK", "Oklahoma"),
    "41": ("OR", "Oregon"),
    "42": ("PA", "Pennsylvania"),
    "44": ("RI", "Rhode Island"),
    "45": ("SC", "South Carolina"),
    "46": ("SD", "South Dakota"),
    "47": ("TN", "Tennessee"),
    "48": ("TX", "Texas"),
    "49": ("UT", "Utah"),
    "50": ("VT", "Vermont"),
    "51": ("VA", "Virginia"),
    "53": ("WA", "Washington"),
    "54": ("WV", "West Virginia"),
    "55": ("WI", "Wisconsin"),
    "56": ("WY", "Wyoming"),
    # Territories
    "60": ("AS", "American Samoa"),
    "66": ("GU", "Guam"),
    "69": ("MP", "Northern Mariana Islands"),
    "72": ("PR", "Puerto Rico"),
    "78": ("VI", "U.S. Virgin Islands"),
}

# Territory FIPS codes
TERRITORY_FIPS: FrozenSet[str] = frozenset({"60", "66", "69", "72", "78"})

# DC FIPS code
DC_FIPS: str = "11"

# Strong MCD states: townships/towns have real governmental authority
# CT, ME, MA, MI, MN, NH, NJ, NY, PA, RI, VT, WI
STRONG_MCD_STATES: FrozenSet[str] = frozenset(
    {"09", "23", "25", "26", "27", "33", "34", "36", "42", "44", "50", "55"}
)

# Control states: state controls distribution and/or retail of distilled spirits
# AL, ID, IA, ME, MI, MS, MT, NH, NC, OH, OR, PA, UT, VT, VA, WV, WY
CONTROL_STATES: FrozenSet[str] = frozenset(
    {"01", "16", "19", "23", "26", "28", "30", "33", "37", "39", "41", "42", "49", "50", "51", "54", "56"}
)

# Virginia independent cities: county-equivalents that are legally cities
# These appear in the county layer with CLASSFP="C7"
VA_INDEPENDENT_CITY_FIPS: FrozenSet[str] = frozenset(
    {
        "51510",  # Alexandria
        "51520",  # Bristol
        "51530",  # Buena Vista
        "51540",  # Charlottesville
        "51550",  # Chesapeake
        "51570",  # Colonial Heights
        "51580",  # Covington
        "51590",  # Danville
        "51595",  # Emporia
        "51600",  # Fairfax city
        "51610",  # Falls Church
        "51620",  # Franklin city
        "51630",  # Fredericksburg
        "51640",  # Galax
        "51650",  # Hampton
        "51660",  # Harrisonburg
        "51670",  # Hopewell
        "51678",  # Lexington
        "51680",  # Lynchburg
        "51683",  # Manassas
        "51685",  # Manassas Park
        "51690",  # Martinsville
        "51700",  # Newport News
        "51710",  # Norfolk
        "51720",  # Norton
        "51730",  # Petersburg
        "51735",  # Poquoson
        "51740",  # Portsmouth
        "51750",  # Radford
        "51760",  # Richmond city
        "51770",  # Roanoke city
        "51775",  # Salem
        "51790",  # Staunton
        "51800",  # Suffolk
        "51810",  # Virginia Beach
        "51820",  # Waynesboro
        "51830",  # Williamsburg
        "51840",  # Winchester
    }
)

# Census FIPS class codes for incorporated places (exclude CDPs)
INCORPORATED_PLACE_CLASS_CODES: FrozenSet[str] = frozenset(
    {"C1", "C2", "C5", "C6", "C7", "C8"}
)

# Census FIPS class codes for CDPs (excluded from the taxonomy)
CDP_CLASS_CODES: FrozenSet[str] = frozenset({"U1", "U2", "U9"})

# Total expected counts for validation
EXPECTED_STATE_COUNT: int = 56  # 50 states + DC + 5 territories
