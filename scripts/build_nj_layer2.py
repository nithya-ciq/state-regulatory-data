#!/usr/bin/env python3
"""
Build NJ Layer 2 dataset from official NJ ABC retail licensee roster.

Reads the NJ ABC Retail_Licensee.xlsx file and produces:
1. data/seed/local_exceptions.csv - Aggregated municipality-level license summary by type
2. data/seed/nj_municipality_license_summary.csv - One row per municipality
"""

import shutil
import sys
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SOURCE_FILE = Path(
    "/Users/nithyashree/.claude/projects/"
    "-Users-nithyashree-Desktop-Twenty20-RegulatoryResearchTool-main/"
    "3d568b28-1c89-4fd4-ad3e-6ad33caab0dc/tool-results/"
    "webfetch-1774484425339-pds4tn.xlsx"
)
SEED_DIR = PROJECT_ROOT / "data" / "seed"
DEST_XLSX = SEED_DIR / "nj_retail_licensees.xlsx"
OUTPUT_LOCAL_EXCEPTIONS = SEED_DIR / "local_exceptions.csv"
OUTPUT_SUMMARY = SEED_DIR / "nj_municipality_license_summary.csv"

# ---------------------------------------------------------------------------
# NJ county code mapping (first 2 digits of license number)
# ---------------------------------------------------------------------------
NJ_COUNTY_CODE_MAP = {
    1: "Atlantic",
    2: "Bergen",
    3: "Burlington",
    4: "Camden",
    5: "Cape May",
    6: "Cumberland",
    7: "Essex",
    8: "Gloucester",
    9: "Hudson",
    10: "Hunterdon",
    11: "Mercer",
    12: "Middlesex",
    13: "Monmouth",
    14: "Morris",
    15: "Ocean",
    16: "Passaic",
    17: "Salem",
    18: "Somerset",
    19: "Sussex",
    20: "Union",
    21: "Warren",
}

NJ_COUNTY_FIPS_MAP = {
    "Atlantic": "34001",
    "Bergen": "34003",
    "Burlington": "34005",
    "Camden": "34007",
    "Cape May": "34009",
    "Cumberland": "34011",
    "Essex": "34013",
    "Gloucester": "34015",
    "Hudson": "34017",
    "Hunterdon": "34019",
    "Mercer": "34021",
    "Middlesex": "34023",
    "Monmouth": "34025",
    "Morris": "34027",
    "Ocean": "34029",
    "Passaic": "34031",
    "Salem": "34033",
    "Somerset": "34035",
    "Sussex": "34037",
    "Union": "34039",
    "Warren": "34041",
}


def extract_county_code(license_number: str) -> int:
    """Extract county code from NJ license number.

    License numbers are formatted like '0101-33-001-007' or similar.
    The first two meaningful digits (positions 0-1 of the numeric portion)
    represent the county code.
    """
    # Remove any non-numeric characters and get the raw number string
    digits = "".join(c for c in str(license_number) if c.isdigit())
    if len(digits) < 4:
        return -1
    # First two digits are county code
    county_code = int(digits[:2])
    return county_code


def classify_license_type(license_type: str) -> dict:
    """Classify a license type into boolean categories."""
    lt = str(license_type).lower().strip()
    return {
        "is_consumption": "consumption" in lt,
        "is_distribution": (
            "plenary retail distribution" in lt
            and "limited" not in lt
        ),
        "is_limited_distribution": "limited retail distribution" in lt,
        "is_club": "club" in lt,
        "is_hotel": "hotel" in lt,
    }


def main():
    # ----- Step 0: Copy source file -----
    print(f"Copying source file to {DEST_XLSX} ...")
    shutil.copy2(SOURCE_FILE, DEST_XLSX)
    print(f"  Copied successfully.")

    # ----- Step 1: Read Excel -----
    print(f"\nReading {SOURCE_FILE} ...")
    df = pd.read_excel(SOURCE_FILE, engine="openpyxl")
    print(f"  Total rows: {len(df)}")
    print(f"  Columns: {list(df.columns)}")

    # Normalize column names
    df.columns = df.columns.str.strip()
    print(f"  Normalized columns: {list(df.columns)}")

    # ----- Step 2: Filter to Active only -----
    # Find the state/status column
    state_col = None
    for col in df.columns:
        if col.lower() in ("state", "status"):
            state_col = col
            break
    if state_col is None:
        # Try to find column with Active/Inactive values
        for col in df.columns:
            vals = df[col].dropna().unique()
            if any("Active" in str(v) for v in vals[:20]):
                state_col = col
                break

    if state_col is None:
        print("ERROR: Could not find State/Status column")
        sys.exit(1)

    print(f"\n  Status column: '{state_col}'")
    print(f"  Value counts:\n{df[state_col].value_counts()}")

    active = df[df[state_col].astype(str).str.strip() == "Active"].copy()
    print(f"\n  Active licenses: {len(active)}")

    # ----- Step 3: Derive county from license number -----
    lic_col = [c for c in active.columns if "license" in c.lower() and "number" in c.lower()]
    if not lic_col:
        lic_col = [c for c in active.columns if "license" in c.lower()]
    lic_col = lic_col[0]
    print(f"  License number column: '{lic_col}'")

    active["_county_code"] = active[lic_col].apply(extract_county_code)
    active["county_name"] = active["_county_code"].map(NJ_COUNTY_CODE_MAP)

    # Find city column
    city_col = [c for c in active.columns if c.lower().strip() == "city"]
    if not city_col:
        city_col = [c for c in active.columns if "city" in c.lower()]
    city_col = city_col[0]
    print(f"  City column: '{city_col}'")

    # Title-case municipality
    active["municipality_name"] = active[city_col].astype(str).str.strip().str.title()

    # Find license type column
    type_col = [c for c in active.columns if "type" in c.lower()]
    type_col = type_col[0]
    print(f"  License type column: '{type_col}'")

    # Find establishment column
    est_col = [c for c in active.columns if "establishment" in c.lower()]
    if not est_col:
        est_col = [c for c in active.columns if "licensee" in c.lower()]
    est_col = est_col[0]
    print(f"  Establishment column: '{est_col}'")

    active["license_type_clean"] = active[type_col].astype(str).str.strip()

    # ----- Step 4: Build local_exceptions.csv -----
    print("\nBuilding local_exceptions.csv ...")

    grouped = active.groupby(["municipality_name", "county_name", "license_type_clean"])

    rows_le = []
    for (muni, county, ltype), grp in grouped:
        classifications = classify_license_type(ltype)
        establishments = grp[est_col].dropna().astype(str).str.strip().tolist()
        sample = "; ".join(establishments[:3])

        rows_le.append({
            "state_fips": "34",
            "state_abbr": "NJ",
            "municipality_name": muni,
            "county_name": county,
            "license_type": ltype,
            "active_license_count": len(grp),
            "has_grocery_exception": ltype == "Limited Retail Distribution License",
            "has_full_retail": ltype == "Plenary Retail Distribution License",
            "has_on_premise": classifications["is_consumption"],
            "sample_establishments": sample,
            "data_source": "NJ ABC Retail Licensee Listing 2026",
            "source_url": "https://nj.gov/oag/abc/downloads/Retail_Licensee.xlsx",
        })

    df_le = pd.DataFrame(rows_le)
    df_le.sort_values(["county_name", "municipality_name", "license_type"], inplace=True)
    df_le.to_csv(OUTPUT_LOCAL_EXCEPTIONS, index=False)
    print(f"  Wrote {len(df_le)} rows to {OUTPUT_LOCAL_EXCEPTIONS}")

    # ----- Step 5: Build nj_municipality_license_summary.csv -----
    print("\nBuilding nj_municipality_license_summary.csv ...")

    muni_groups = active.groupby(["municipality_name", "county_name"])

    rows_summary = []
    for (muni, county), grp in muni_groups:
        ltypes = grp["license_type_clean"].tolist()
        classifications = [classify_license_type(lt) for lt in ltypes]

        consumption_count = sum(1 for c in classifications if c["is_consumption"])
        distribution_count = sum(1 for c in classifications if c["is_distribution"])
        limited_dist_count = sum(1 for c in classifications if c["is_limited_distribution"])
        club_count = sum(1 for c in classifications if c["is_club"])
        hotel_count = sum(1 for c in classifications if c["is_hotel"])

        has_consumption = consumption_count > 0
        has_distribution = distribution_count > 0
        has_limited_dist = limited_dist_count > 0
        has_club = club_count > 0
        has_hotel = hotel_count > 0

        establishments = grp[est_col].dropna().astype(str).str.strip().unique().tolist()
        top_5 = "; ".join(establishments[:5])

        rows_summary.append({
            "state_fips": "34",
            "state_abbr": "NJ",
            "municipality_name": muni,
            "county_name": county,
            "total_active_licenses": len(grp),
            "has_consumption_license": has_consumption,
            "has_distribution_license": has_distribution,
            "has_limited_distribution": has_limited_dist,
            "has_club_license": has_club,
            "has_hotel_license": has_hotel,
            "consumption_count": consumption_count,
            "distribution_count": distribution_count,
            "limited_distribution_count": limited_dist_count,
            "club_count": club_count,
            "hotel_count": hotel_count,
            "grocery_can_sell_alcohol": has_limited_dist or has_distribution,

        })

    df_summary = pd.DataFrame(rows_summary)
    df_summary.sort_values(["county_name", "municipality_name"], inplace=True)
    df_summary.to_csv(OUTPUT_SUMMARY, index=False)
    print(f"  Wrote {len(df_summary)} rows to {OUTPUT_SUMMARY}")

    # ----- Step 6: Summary stats -----
    print("\n" + "=" * 70)
    print("NJ LAYER 2 DATASET - SUMMARY STATISTICS")
    print("=" * 70)

    print(f"\nSource file: {SOURCE_FILE}")
    print(f"Total rows in source: {len(df)}")
    print(f"Active licenses: {len(active)}")
    print(f"Inactive licenses: {len(df) - len(active)}")

    print(f"\nUnique municipalities: {active['municipality_name'].nunique()}")
    print(f"Unique counties: {active['county_name'].nunique()}")

    print(f"\nLicense types (active):")
    for lt, cnt in active["license_type_clean"].value_counts().items():
        print(f"  {lt}: {cnt}")

    print(f"\nCounty distribution (active):")
    for county, cnt in active["county_name"].value_counts().head(10).items():
        print(f"  {county}: {cnt}")

    print(f"\nTop 10 municipalities by license count:")
    muni_counts = active["municipality_name"].value_counts().head(10)
    for muni, cnt in muni_counts.items():
        print(f"  {muni}: {cnt}")

    # Grocery/limited distribution stats
    ltd = active[active["license_type_clean"].str.contains("Limited Retail Distribution", case=False, na=False)]
    print(f"\nLimited Retail Distribution (grocery/convenience) licenses: {len(ltd)}")
    print(f"  In {ltd['municipality_name'].nunique()} municipalities")

    # Municipalities with NO consumption licenses
    consumption_munis = set(
        active[active["license_type_clean"].str.contains("Consumption", case=False, na=False)]["municipality_name"]
    )
    all_munis = set(active["municipality_name"])
    no_consumption = all_munis - consumption_munis
    print(f"\nMunicipalities with NO consumption (on-premise) licenses: {len(no_consumption)}")
    if no_consumption:
        for m in sorted(no_consumption)[:10]:
            print(f"  {m}")
        if len(no_consumption) > 10:
            print(f"  ... and {len(no_consumption) - 10} more")

    print(f"\nOutput files:")
    print(f"  1. {OUTPUT_LOCAL_EXCEPTIONS}")
    print(f"  2. {OUTPUT_SUMMARY}")
    print(f"  3. {DEST_XLSX} (copy of source)")
    print("\nDone!")


if __name__ == "__main__":
    main()
