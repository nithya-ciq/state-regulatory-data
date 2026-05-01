"""Collects all asset definitions for the jurisdiction pipeline."""

from dagster_jurisdiction.assets.research import research_data
from dagster_jurisdiction.assets.raw import state_classifications, census_geographies, license_types
from dagster_jurisdiction.assets.processing import processed_geographies
from dagster_jurisdiction.assets.assembly import jurisdictions, enriched_jurisdictions
from dagster_jurisdiction.assets.export import jurisdiction_export
from dagster_jurisdiction.assets.supabase_sync import supabase_sync
from dagster_jurisdiction.assets.layer2 import layer2_licenses
from dagster_jurisdiction.checks.data_quality import all_checks

all_assets = [
    research_data,
    state_classifications,
    license_types,
    census_geographies,
    processed_geographies,
    jurisdictions,
    enriched_jurisdictions,
    jurisdiction_export,
    supabase_sync,
    layer2_licenses,
]

all_asset_checks = all_checks
