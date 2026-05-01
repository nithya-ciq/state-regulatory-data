"""Build Layer 2 data for Kentucky from official KY ABC BELLE Portal export.

Source: https://abcportal.ky.gov/BELLEExternal (Reports > All Active Licenses > Excel export)
Downloaded: April 1, 2026

Usage:
    python scripts/build_ky_layer2.py --state KY
    python scripts/build_ky_layer2.py --state KY --sync-supabase
"""

import argparse
import csv
import json
import math
import os
import requests
import sys
from collections import Counter, defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

STATE_FIPS = "21"
STATE_ABBR = "KY"
SOURCE_FILE = "data/seed/ky_license_list.csv"
SOURCE_NAME = "KY ABC BELLE Portal Active Licenses Report April 2026"
SOURCE_URL = "https://abcportal.ky.gov/BELLEExternal"

# KY license categories
LICENSE_CATEGORIES = {
    "consumption": [
        "Quota Retail Drink License",
        "NQ1 Retail Drink License",
        "NQ2 Retail Drink License",
        "NQ3 Retail Drink License",
        "NQ4 Retail Malt Beverage Drink License",
        "Limited Restaurant License",
        "Special Sunday Retail Drink License",
        "Supplemental Bar License",
        "Limited Golf Course License",
        "Entertainment Destination Center License",
        "Authorized Public Consumption License",
        "Caterer's License",
        "Extended Hours Supplemental License",
        "Hotel In-Room License",
    ],
    "package": [
        "Quota Retail Package License",
        "NQ Retail Malt Beverage Package License",
        "Limited Non Quota Package License",
    ],
    "manufacturer": [
        "Microbrewery License",
        "Brewer's License",
        "Small Farm Winery License",
        "Distiller's License - Class A",
        "Distiller's License - Class B",
    ],
    "distributor": [
        "Distributor's License",
        "In-State Distilled Spirits Supplier's License",
        "Limited In-State Distilled Spirits Supplier's License",
        "Limited Out-of-State Distilled Spirits and Wine Supplier's License",
        "Limited Out-of-State Malt Beverage Supplier's License",
        "Sampling License",
    ],
    "transporter": [
        "Transporter's License",
        "Air Transporter License",
    ],
    "other": [
        "Direct Shipper License",
        "Cannabis-Infused Beverage Direct Shipper License Type B",
        "Auctioneer Temporary License",
        "Distilled Spirits and Wine Storage License",
        "Malt Beverage Storage License",
        "Bottling House or Bottling House Storage License",
    ],
}

# All 120 KY counties
ALL_KY_COUNTIES = [
    "Adair","Allen","Anderson","Ballard","Barren","Bath","Bell","Boone","Bourbon","Boyd",
    "Boyle","Bracken","Breathitt","Breckinridge","Bullitt","Butler","Caldwell","Calloway",
    "Campbell","Carlisle","Carroll","Carter","Casey","Christian","Clark","Clay","Clinton",
    "Crittenden","Cumberland","Daviess","Edmonson","Elliott","Estill","Fayette","Fleming",
    "Floyd","Franklin","Fulton","Gallatin","Garrard","Grant","Graves","Grayson","Green",
    "Greenup","Hancock","Hardin","Harlan","Harrison","Hart","Henderson","Henry","Hickman",
    "Hopkins","Jackson","Jefferson","Jessamine","Johnson","Kenton","Knott","Knox","Larue",
    "Laurel","Lawrence","Lee","Leslie","Letcher","Lewis","Lincoln","Livingston","Logan",
    "Lyon","Madison","Magoffin","Marion","Marshall","Martin","Mason","McCracken","McCreary",
    "McLean","Meade","Menifee","Mercer","Metcalfe","Monroe","Montgomery","Morgan",
    "Muhlenberg","Nelson","Nicholas","Ohio","Oldham","Owen","Owsley","Pendleton","Perry",
    "Pike","Powell","Pulaski","Robertson","Rockcastle","Rowan","Russell","Scott","Shelby",
    "Simpson","Spencer","Taylor","Todd","Trigg","Trimble","Union","Warren","Washington",
    "Wayne","Webster","Whitley","Wolfe","Woodford"
]


def categorize(license_type):
    """Map a KY license type to a category."""
    # Skip tobacco
    if "Tobacco" in license_type or "Nicotine" in license_type or "Vapor" in license_type:
        return "tobacco"
    for cat, types in LICENSE_CATEGORIES.items():
        if license_type in types:
            return cat
    return "other"


def get_population():
    """Fetch KY county populations from Census."""
    cache = PROJECT_ROOT / f"data/cache/census_pop_{STATE_FIPS}.json"
    if cache.exists():
        with open(cache) as f:
            return json.load(f)

    print("  Fetching Census population...")
    pop = {}

    # County populations (KY is county-based for wet/dry)
    url = f"https://api.census.gov/data/2020/dec/pl?get=NAME,P1_001N&for=county:*&in=state:{STATE_FIPS}"
    resp = requests.get(url)
    if resp.status_code == 200:
        for row in resp.json()[1:]:
            name = row[0].split(",")[0].replace(" County", "").strip()
            pop[f"COUNTY_{name.upper()}"] = int(row[1])

    # City/place populations
    url2 = f"https://api.census.gov/data/2020/dec/pl?get=NAME,P1_001N&for=place:*&in=state:{STATE_FIPS}"
    resp2 = requests.get(url2)
    if resp2.status_code == 200:
        for row in resp2.json()[1:]:
            name = row[0].split(",")[0].strip()
            for suffix in [" city", " CDP"]:
                if name.lower().endswith(suffix):
                    name = name[:-len(suffix)].strip()
            pop[name.upper()] = int(row[1])

    cache.parent.mkdir(parents=True, exist_ok=True)
    with open(cache, "w") as f:
        json.dump(pop, f)
    return pop


def build_county_summary(rows, pop):
    """Aggregate by county — KY's primary regulatory unit."""
    county_agg = defaultdict(lambda: {
        "total": 0, "cities": set(), "categories": Counter(),
        "quota_drink": 0, "quota_package": 0, "nq_drink": 0, "nq_package": 0,
        "sunday": 0, "manufacturer": 0, "establishments": []
    })

    for r in rows:
        cat = categorize(r["license_type"])
        if cat == "tobacco":
            continue

        county = r["county"].strip()
        if not county:
            continue

        cd = county_agg[county]
        cd["total"] += 1
        cd["cities"].add(r["city"].strip())
        cd["categories"][cat] += 1

        lt = r["license_type"]
        if "Quota Retail Drink" in lt:
            cd["quota_drink"] += 1
        elif "Quota Retail Package" in lt:
            cd["quota_package"] += 1
        elif "NQ" in lt and "Drink" in lt:
            cd["nq_drink"] += 1
        elif "NQ" in lt and "Package" in lt:
            cd["nq_package"] += 1
        elif "Sunday" in lt:
            cd["sunday"] += 1
        elif cat == "manufacturer":
            cd["manufacturer"] += 1

        dba = r.get("dba", "")
        if dba and dba not in ("None", ""):
            cd["establishments"].append(dba)

    # Build output including dry counties
    fieldnames = [
        "state_fips", "state_abbr", "county_name",
        "total_alcohol_licenses", "wet_dry_status", "wet_cities_count",
        "wet_cities", "has_quota_drink", "has_quota_package",
        "has_nq_drink", "has_nq_package", "has_sunday_license",
        "has_manufacturer",
        "quota_drink_count", "quota_package_count",
        "nq_drink_count", "nq_package_count", "sunday_count",
        "manufacturer_count",
        "consumption_total", "package_total",
        "population_2020", "quota_notes",
    ]

    summary = []
    for county in sorted(ALL_KY_COUNTIES):
        cd = county_agg.get(county, None)

        county_pop = pop.get(f"COUNTY_{county.upper()}", 0)

        if cd and cd["total"] > 0:
            cities = sorted(cd["cities"])
            consumption = cd["categories"].get("consumption", 0)
            package = cd["categories"].get("package", 0)

            # Determine wet/dry/moist
            if cd["quota_drink"] > 0 or cd["quota_package"] > 0:
                status = "wet"
            elif cd["nq_drink"] > 0 or cd["nq_package"] > 0:
                status = "moist"
            else:
                # Has some licenses but only non-quota (maybe just manufacturers or caterers)
                status = "moist"

            summary.append({
                "state_fips": STATE_FIPS, "state_abbr": STATE_ABBR,
                "county_name": county,
                "total_alcohol_licenses": cd["total"],
                "wet_dry_status": status,
                "wet_cities_count": len(cities),
                "wet_cities": "; ".join(cities),
                "has_quota_drink": str(cd["quota_drink"] > 0),
                "has_quota_package": str(cd["quota_package"] > 0),
                "has_nq_drink": str(cd["nq_drink"] > 0),
                "has_nq_package": str(cd["nq_package"] > 0),
                "has_sunday_license": str(cd["sunday"] > 0),
                "has_manufacturer": str(cd["manufacturer"] > 0),
                "quota_drink_count": cd["quota_drink"],
                "quota_package_count": cd["quota_package"],
                "nq_drink_count": cd["nq_drink"],
                "nq_package_count": cd["nq_package"],
                "sunday_count": cd["sunday"],
                "manufacturer_count": cd["manufacturer"],
                "consumption_total": consumption,
                "package_total": package,
                "population_2020": county_pop,
                "quota_notes": "",
            })
        else:
            summary.append({
                "state_fips": STATE_FIPS, "state_abbr": STATE_ABBR,
                "county_name": county,
                "total_alcohol_licenses": 0,
                "wet_dry_status": "dry",
                "wet_cities_count": 0,
                "wet_cities": "",
                "has_quota_drink": "False", "has_quota_package": "False",
                "has_nq_drink": "False", "has_nq_package": "False",
                "has_sunday_license": "False", "has_manufacturer": "False",
                "quota_drink_count": 0, "quota_package_count": 0,
                "nq_drink_count": 0, "nq_package_count": 0,
                "sunday_count": 0, "manufacturer_count": 0,
                "consumption_total": 0, "package_total": 0,
                "population_2020": county_pop,
                "quota_notes": "Completely dry — zero alcohol licenses",
            })

    return summary, fieldnames


def build_individual(rows):
    """Build individual license records (alcohol only)."""
    records = []
    for r in rows:
        cat = categorize(r["license_type"])
        if cat == "tobacco":
            continue
        records.append({
            "license_number": r["license_number"],
            "state_fips": STATE_FIPS,
            "municipality_name": r["city"].strip().title() if r["city"] else None,
            "county_name": r["county"].strip(),
            "license_type": r["license_type"],
            "establishment_name": r["dba"] if r.get("dba") not in ("", "None") else None,
            "licensee_name": r.get("licensee") if r.get("licensee") not in ("", "None") else None,
            "premise_address": r.get("address", "")[:200] if r.get("address") else None,
            "status": "Active",
            "effective_date": r.get("effective_date", ""),
        })
    return records


def sync_supabase(summary, individual):
    """Sync to Supabase."""
    from supabase import create_client

    env_path = PROJECT_ROOT / ".env.local"
    url = key = ""
    with open(env_path) as f:
        for line in f:
            if line.startswith("SUPABASE_URL="): url = line.split("=", 1)[1].strip()
            if line.startswith("SUPABASE_KEY="): key = line.split("=", 1)[1].strip()

    client = create_client(url, key)
    S = "regulations_data"

    # For KY, use layer2_municipality_licenses with county_name as the key
    # (KY is county-based, not municipality-based like NJ/PA)
    client.schema(S).table("layer2_municipality_licenses").delete().eq("state_fips", STATE_FIPS).execute()

    for i in range(0, len(summary), 400):
        batch = summary[i:i + 400]
        sb_rows = []
        for r in batch:
            sb_rows.append({
                "state_fips": STATE_FIPS, "state_abbr": STATE_ABBR,
                "municipality_name": r["county_name"],  # Using county as primary unit for KY
                "county_name": r["county_name"],
                "total_active_licenses": int(r.get("total_alcohol_licenses", 0)),
                "has_consumption_license": r.get("has_quota_drink") == "True" or r.get("has_nq_drink") == "True",
                "has_distribution_license": r.get("has_quota_package") == "True" or r.get("has_nq_package") == "True",
                "has_limited_distribution": False,
                "has_club_license": False,
                "has_hotel_license": False,
                "consumption_count": int(r.get("consumption_total", 0)),
                "distribution_count": int(r.get("package_total", 0)),
                "limited_distribution_count": 0,
                "club_count": 0,
                "hotel_count": 0,
                "grocery_can_sell_alcohol": r.get("has_nq_package") == "True" or r.get("has_quota_package") == "True",
                "data_source": SOURCE_NAME,
                "source_url": SOURCE_URL,
                "population_2020": int(r.get("population_2020", 0)) or None,
                "quota_notes": r.get("quota_notes") or None,
            })
        client.schema(S).table("layer2_municipality_licenses").upsert(
            sb_rows, on_conflict="state_fips,municipality_name"
        ).execute()
    print(f"  Synced {len(summary)} county rows")

    # Individual licenses
    client.schema(S).table("layer2_individual_licenses").delete().eq("state_fips", STATE_FIPS).execute()
    seen = set()
    deduped = []
    for r in individual:
        if r["license_number"] not in seen:
            seen.add(r["license_number"])
            deduped.append(r)

    for i in range(0, len(deduped), 400):
        batch = deduped[i:i + 400]
        client.schema(S).table("layer2_individual_licenses").upsert(
            batch, on_conflict="license_number"
        ).execute()
        if (i // 400) % 5 == 0:
            print(f"  Individual: {i + len(batch)}/{len(deduped)}...")
    print(f"  Synced {len(deduped)} individual licenses")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", required=True)
    parser.add_argument("--sync-supabase", action="store_true")
    args = parser.parse_args()

    print(f"\n{'=' * 60}")
    print(f"Building Layer 2 for {STATE_ABBR} (FIPS {STATE_FIPS})")
    print(f"{'=' * 60}")

    # Load data
    print("\n1. Loading source data...")
    source = PROJECT_ROOT / SOURCE_FILE
    with open(source) as f:
        rows = list(csv.DictReader(f))
    print(f"  Loaded {len(rows)} licenses from {source.name}")

    # Population
    print("\n2. Fetching Census population...")
    pop = get_population()

    # County summary
    print("\n3. Building county summary...")
    summary, fieldnames = build_county_summary(rows, pop)

    # Individual records
    print("\n4. Building individual records...")
    individual = build_individual(rows)

    # Write CSV
    print("\n5. Writing output...")
    out_path = PROJECT_ROOT / f"data/seed/ky_county_license_summary.csv"
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summary)
    print(f"  Wrote {len(summary)} rows to {out_path}")

    # Sync
    if args.sync_supabase:
        print("\n6. Syncing to Supabase...")
        sync_supabase(summary, individual)

    # Stats
    dry = sum(1 for r in summary if r["wet_dry_status"] == "dry")
    moist = sum(1 for r in summary if r["wet_dry_status"] == "moist")
    wet = sum(1 for r in summary if r["wet_dry_status"] == "wet")
    total_lic = sum(int(r["total_alcohol_licenses"]) for r in summary)

    print(f"\n{'=' * 60}")
    print(f"DONE — {STATE_ABBR} Layer 2")
    print(f"{'=' * 60}")
    print(f"  Counties: {len(summary)} (dry={dry}, moist={moist}, wet={wet})")
    print(f"  Alcohol licenses: {total_lic}")
    print(f"  Individual records: {len(individual)}")
    print(f"  Source: {SOURCE_NAME}")

    # Chain search
    chains = ["KROGER", "WALMART", "DOLLAR GENERAL", "SPEEDWAY", "CIRCLE K"]
    print(f"\n  Chain store licenses:")
    for chain in chains:
        matches = [r for r in rows if chain in str(r.get("dba", "")).upper()
                   or chain in str(r.get("licensee", "")).upper()]
        alcohol_matches = [r for r in matches if categorize(r["license_type"]) != "tobacco"]
        if alcohol_matches:
            print(f"    {chain}: {len(alcohol_matches)} alcohol licenses")


if __name__ == "__main__":
    main()
