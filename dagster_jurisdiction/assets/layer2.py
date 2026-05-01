"""Layer 2 assets — municipality-level license data for complex states.

Each state has its own seed CSV (downloaded from official ABC roster)
and a processing script. This asset runs the processing and syncs to Supabase.
"""

import logging
import subprocess
import sys
import time
from pathlib import Path

import dagster as dg

logger = logging.getLogger("jurisdiction.dagster.layer2")

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# States with Layer 2 data available
LAYER2_STATES = {
    "NJ": {
        "script": "scripts/build_nj_layer2.py",
        "seed_file": "data/seed/nj_retail_licensees_april2026.xlsx",
        "summary_csv": "data/seed/nj_municipality_license_summary.csv",
        "source": "NJ ABC Retail License Report April 2026",
    },
    "PA": {
        "script": "scripts/build_pa_layer2.py",
        "seed_file": "data/seed/pa_license_list.csv",
        "summary_csv": "data/seed/pa_municipality_license_summary.csv",
        "source": "PA PLCB License Search Export April 2026",
    },
    "KY": {
        "script": "scripts/build_ky_layer2.py",
        "seed_file": "data/seed/ky_license_list.csv",
        "summary_csv": "data/seed/ky_county_license_summary.csv",
        "source": "KY ABC BELLE Portal Active Licenses Report April 2026",
    },
}


@dg.asset(
    group_name="7_layer2",
    deps=["supabase_sync"],
    description=(
        "Build Layer 2 municipality-level license data for complex states "
        "(NJ, PA, etc.) from official state ABC rosters. "
        "Processes seed CSVs, aggregates by municipality, calculates quotas, "
        "and syncs to Supabase layer2 tables."
    ),
    kinds={"python", "supabase"},
)
def layer2_licenses(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    """Process and sync Layer 2 data for all configured states."""
    start_time = time.time()
    results = {}

    for state_abbr, config in LAYER2_STATES.items():
        seed_path = PROJECT_ROOT / config["seed_file"]

        if not seed_path.exists():
            context.log.warning(
                f"Layer 2 seed file not found for {state_abbr}: {seed_path}. Skipping."
            )
            results[state_abbr] = {"status": "skipped", "reason": "seed file not found"}
            continue

        context.log.info(f"Processing Layer 2 for {state_abbr}...")

        script_path = PROJECT_ROOT / config["script"]
        if not script_path.exists():
            context.log.warning(f"Script not found: {script_path}. Skipping {state_abbr}.")
            results[state_abbr] = {"status": "skipped", "reason": "script not found"}
            continue

        # Run the state-specific script with --sync-supabase
        try:
            result = subprocess.run(
                [sys.executable, str(script_path), "--state", state_abbr, "--sync-supabase"],
                capture_output=True,
                text=True,
                cwd=str(PROJECT_ROOT),
                timeout=600,
            )

            if result.returncode != 0:
                context.log.error(
                    f"Layer 2 script failed for {state_abbr}:\n"
                    f"stdout: {result.stdout[-500:]}\n"
                    f"stderr: {result.stderr[-500:]}"
                )
                results[state_abbr] = {"status": "failed", "error": result.stderr[-200:]}
            else:
                # Parse output for stats
                output = result.stdout
                context.log.info(f"Layer 2 for {state_abbr}:\n{output[-300:]}")

                # Count rows in summary CSV
                summary_path = PROJECT_ROOT / config["summary_csv"]
                if summary_path.exists():
                    import csv
                    with open(summary_path) as f:
                        row_count = sum(1 for _ in csv.DictReader(f))
                    results[state_abbr] = {"status": "success", "municipalities": row_count}
                else:
                    results[state_abbr] = {"status": "success", "municipalities": 0}

        except subprocess.TimeoutExpired:
            context.log.error(f"Layer 2 script timed out for {state_abbr}")
            results[state_abbr] = {"status": "timeout"}
        except Exception as e:
            context.log.error(f"Layer 2 failed for {state_abbr}: {e}")
            results[state_abbr] = {"status": "error", "error": str(e)}

    elapsed = round(time.time() - start_time, 2)

    # Build metadata
    metadata = {"duration_seconds": dg.MetadataValue.float(elapsed)}
    total_munis = 0
    for state_abbr, res in results.items():
        metadata[f"{state_abbr}_status"] = dg.MetadataValue.text(res.get("status", "unknown"))
        if res.get("municipalities"):
            metadata[f"{state_abbr}_municipalities"] = dg.MetadataValue.int(res["municipalities"])
            total_munis += res["municipalities"]

    metadata["total_municipalities"] = dg.MetadataValue.int(total_munis)
    metadata["states_processed"] = dg.MetadataValue.int(
        sum(1 for r in results.values() if r.get("status") == "success")
    )

    # Fail if ALL states failed
    all_failed = all(r.get("status") != "success" for r in results.values())
    if all_failed and results:
        raise RuntimeError(
            f"Layer 2 FAILED for all states: {results}"
        )

    context.log.info(
        f"Layer 2 complete in {elapsed}s: {results}"
    )

    return dg.MaterializeResult(metadata=metadata)
