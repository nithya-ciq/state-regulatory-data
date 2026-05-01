"""Validate regulatory data in state_classification_matrix.csv before pipeline sync.

Checks logical consistency, flags anomalies, and produces a review report.
Run: python scripts/validate_regulatory_data.py
"""

import csv
import sys
from pathlib import Path
from collections import Counter
from typing import Optional, List, Dict

CSV_PATH = Path("data/seed/state_classification_matrix.csv")

# Known control states (should have strict or modified three-tier)
CONTROL_STATES = {"AL", "ID", "IA", "ME", "MI", "MS", "MT", "NH", "NC", "OH",
                  "OR", "PA", "UT", "VA", "VT", "WV", "WY"}

# States known to prohibit grocery alcohol sales entirely
NO_GROCERY_ALCOHOL = {"DE", "MD", "RI"}

# States known to be very permissive (should be relaxed or modified)
PERMISSIVE_STATES = {"MO", "NV", "LA"}


def parse_bool(val: str) -> Optional[bool]:
    v = val.strip().lower()
    if v == "true":
        return True
    elif v == "false":
        return False
    return None


def validate() -> List[Dict]:
    """Run all validation checks. Returns list of issues."""
    issues: List[Dict] = []
    warnings: List[Dict] = []

    with open(CSV_PATH) as f:
        rows = list(csv.DictReader(f))

    # --- Check 1: Row count ---
    if len(rows) != 56:
        issues.append({"level": "ERROR", "check": "row_count",
                        "detail": f"Expected 56 rows, got {len(rows)}"})

    # --- Check 2: No empty critical fields ---
    critical_fields = ["three_tier_enforcement", "sunday_sales_allowed",
                       "grocery_beer_allowed", "grocery_wine_allowed",
                       "convenience_beer_allowed", "convenience_wine_allowed",
                       "has_on_premise_license", "has_off_premise_license"]
    for i, row in enumerate(rows):
        for field in critical_fields:
            if not row.get(field, "").strip():
                issues.append({"level": "ERROR", "check": "missing_field",
                                "detail": f"{row['state_abbr']}: '{field}' is empty"})

    # --- Check 3: three_tier_enforcement values ---
    valid_tiers = {"strict", "modified", "relaxed", "franchise"}
    for row in rows:
        val = row.get("three_tier_enforcement", "").strip().lower()
        if val and val not in valid_tiers:
            issues.append({"level": "ERROR", "check": "invalid_three_tier",
                            "detail": f"{row['state_abbr']}: three_tier='{val}' not in {valid_tiers}"})

    # --- Check 4: Control states should be strict or modified ---
    for row in rows:
        abbr = row["state_abbr"]
        ctrl = row.get("control_status", "").strip().lower()
        tier = row.get("three_tier_enforcement", "").strip().lower()
        if ctrl == "control" and tier == "relaxed":
            warnings.append({"level": "WARN", "check": "control_relaxed",
                              "detail": f"{abbr}: control state but three_tier='relaxed' — unusual"})

    # --- Check 5: Logical consistency — beer more permissive than wine ---
    for row in rows:
        abbr = row["state_abbr"]
        gb = parse_bool(row.get("grocery_beer_allowed", ""))
        gw = parse_bool(row.get("grocery_wine_allowed", ""))
        cb = parse_bool(row.get("convenience_beer_allowed", ""))
        cw = parse_bool(row.get("convenience_wine_allowed", ""))

        # Wine allowed but not beer? Very unusual.
        if gw is True and gb is False:
            issues.append({"level": "ERROR", "check": "wine_without_beer",
                            "detail": f"{abbr}: grocery_wine=True but grocery_beer=False — very unusual"})
        if cw is True and cb is False:
            issues.append({"level": "ERROR", "check": "wine_without_beer",
                            "detail": f"{abbr}: convenience_wine=True but convenience_beer=False — very unusual"})

        # Convenience more permissive than grocery? Unusual but possible.
        if cb is True and gb is False and abbr not in {"AK"}:
            warnings.append({"level": "WARN", "check": "convenience_more_permissive",
                              "detail": f"{abbr}: convenience_beer=True but grocery_beer=False"})
        if cw is True and gw is False:
            warnings.append({"level": "WARN", "check": "convenience_more_permissive",
                              "detail": f"{abbr}: convenience_wine=True but grocery_wine=False"})

    # --- Check 6: No-grocery states match known list ---
    for row in rows:
        abbr = row["state_abbr"]
        gb = parse_bool(row.get("grocery_beer_allowed", ""))
        gw = parse_bool(row.get("grocery_wine_allowed", ""))
        if abbr in NO_GROCERY_ALCOHOL and (gb is True or gw is True):
            warnings.append({"level": "WARN", "check": "grocery_override",
                              "detail": f"{abbr}: known no-grocery-alcohol state but grocery beer/wine = {gb}/{gw}"})
        is_territory = row.get("is_territory", "").strip().lower() == "true"
        if not is_territory and gb is False and gw is False and abbr not in NO_GROCERY_ALCOHOL and abbr not in {"NJ", "AK"}:
            warnings.append({"level": "WARN", "check": "unexpected_no_grocery",
                              "detail": f"{abbr}: grocery_beer=False, grocery_wine=False — verify this is correct"})

    # --- Check 7: Sunday sales consistency ---
    for row in rows:
        abbr = row["state_abbr"]
        sunday = parse_bool(row.get("sunday_sales_allowed", ""))
        hours = row.get("sunday_sales_hours", "").strip()
        if sunday is False and hours:
            warnings.append({"level": "WARN", "check": "sunday_hours_but_false",
                              "detail": f"{abbr}: sunday_sales=False but hours='{hours}'"})
        if sunday is True and not hours:
            # Not necessarily wrong (e.g., territories) but worth noting
            if not row.get("is_territory", "").strip().lower() == "true":
                warnings.append({"level": "INFO", "check": "sunday_no_hours",
                                  "detail": f"{abbr}: sunday_sales=True but no hours specified"})

    # --- Check 8: beer_max_abv is numeric where present ---
    for row in rows:
        abbr = row["state_abbr"]
        abv = row.get("beer_max_abv", "").strip()
        if abv:
            try:
                val = float(abv)
                if val < 3 or val > 25:
                    warnings.append({"level": "WARN", "check": "abv_range",
                                      "detail": f"{abbr}: beer_max_abv={val} — outside typical range 3-25"})
            except ValueError:
                issues.append({"level": "ERROR", "check": "abv_not_numeric",
                                "detail": f"{abbr}: beer_max_abv='{abv}' is not numeric"})

    # --- Check 9: Permissive states should not be strict ---
    for row in rows:
        abbr = row["state_abbr"]
        tier = row.get("three_tier_enforcement", "").strip().lower()
        if abbr in PERMISSIVE_STATES and tier == "strict":
            warnings.append({"level": "WARN", "check": "permissive_but_strict",
                              "detail": f"{abbr}: known permissive state but three_tier='strict'"})

    return issues + warnings


def print_summary(rows: list[dict]) -> None:
    """Print a state-by-state summary table for manual review."""
    with open(CSV_PATH) as f:
        data = list(csv.DictReader(f))

    print("\n" + "=" * 120)
    print("REGULATORY DATA SUMMARY — Review for accuracy")
    print("=" * 120)
    print(f"{'ST':<4} {'Control':<9} {'3-Tier':<10} {'Sunday':<8} "
          f"{'GrocBeer':<10} {'GrocWine':<10} {'ConvBeer':<10} {'ConvWine':<10} {'MaxABV':<8}")
    print("-" * 120)

    for row in data:
        abbr = row["state_abbr"]
        ctrl = row.get("control_status", "")[:8]
        tier = row.get("three_tier_enforcement", "")[:9]
        sun = row.get("sunday_sales_allowed", "")
        gb = row.get("grocery_beer_allowed", "")
        gw = row.get("grocery_wine_allowed", "")
        cb = row.get("convenience_beer_allowed", "")
        cw = row.get("convenience_wine_allowed", "")
        abv = row.get("beer_max_abv", "") or "-"
        print(f"{abbr:<4} {ctrl:<9} {tier:<10} {sun:<8} {gb:<10} {gw:<10} {cb:<10} {cw:<10} {abv:<8}")


def main() -> None:
    if not CSV_PATH.exists():
        print(f"ERROR: {CSV_PATH} not found")
        sys.exit(1)

    print("Validating regulatory data in state_classification_matrix.csv...\n")
    results = validate()

    errors = [r for r in results if r["level"] == "ERROR"]
    warns = [r for r in results if r["level"] == "WARN"]
    infos = [r for r in results if r["level"] == "INFO"]

    if errors:
        print(f"ERRORS ({len(errors)}):")
        for r in errors:
            print(f"  [ERROR] {r['detail']}")
        print()

    if warns:
        print(f"WARNINGS ({len(warns)}):")
        for r in warns:
            print(f"  [WARN]  {r['detail']}")
        print()

    if infos:
        print(f"INFO ({len(infos)}):")
        for r in infos:
            print(f"  [INFO]  {r['detail']}")
        print()

    # Distribution summary
    with open(CSV_PATH) as f:
        data = list(csv.DictReader(f))

    tier_dist = Counter(r.get("three_tier_enforcement", "").strip() for r in data)
    sunday_dist = Counter(r.get("sunday_sales_allowed", "").strip() for r in data)
    gb_dist = Counter(r.get("grocery_beer_allowed", "").strip() for r in data)

    print("DISTRIBUTIONS:")
    print(f"  three_tier_enforcement: {dict(tier_dist)}")
    print(f"  sunday_sales_allowed:   {dict(sunday_dist)}")
    print(f"  grocery_beer_allowed:   {dict(gb_dist)}")

    # Full summary table
    print_summary(results)

    print()
    if errors:
        print(f"RESULT: {len(errors)} ERRORS found — fix before syncing to Supabase")
        sys.exit(1)
    elif warns:
        print(f"RESULT: PASSED with {len(warns)} warnings — review warnings above")
    else:
        print("RESULT: ALL CHECKS PASSED")


if __name__ == "__main__":
    main()
