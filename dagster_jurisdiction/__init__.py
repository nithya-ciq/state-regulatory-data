"""Dagster code location for the Jurisdiction Taxonomy Pipeline.

This is the entry point that Dagster loads. It assembles the Definitions
object tying together all assets, resources, jobs, schedules, and sensors.
"""

import dagster as dg

from dagster_jurisdiction.assets import all_assets, all_asset_checks
from dagster_jurisdiction.jobs import (
    full_pipeline_job,
    pipeline_only_job,
    classification_only_job,
    export_only_job,
    sync_only_job,
)
from dagster_jurisdiction.resources.database import DatabaseResource
from dagster_jurisdiction.resources.pipeline_config import PipelineConfigResource
from dagster_jurisdiction.resources.tiger import TigerClientResource
from dagster_jurisdiction.resources.supabase import SupabaseResource
from dagster_jurisdiction.schedules import monthly_full_pipeline_schedule
from dagster_jurisdiction.sensors import data_quality_sensor, pipeline_health_sensor

defs = dg.Definitions(
    assets=all_assets,
    asset_checks=all_asset_checks,
    jobs=[
        full_pipeline_job,
        pipeline_only_job,
        classification_only_job,
        export_only_job,
        sync_only_job,
    ],
    schedules=[monthly_full_pipeline_schedule],
    sensors=[data_quality_sensor, pipeline_health_sensor],
    resources={
        "database": DatabaseResource(),
        "pipeline_config": PipelineConfigResource(),
        "tiger_client": TigerClientResource(),
        "supabase": SupabaseResource(
            supabase_url=dg.EnvVar("SUPABASE_URL"),
            supabase_key=dg.EnvVar("SUPABASE_KEY"),
        ),
    },
)
