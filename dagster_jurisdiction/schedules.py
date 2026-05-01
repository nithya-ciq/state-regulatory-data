"""Schedule definitions for the jurisdiction taxonomy pipeline."""

import dagster as dg

from dagster_jurisdiction.jobs import full_pipeline_job


# Monthly schedule: run full pipeline (research + phases 1-5) on the 1st of each month
monthly_full_pipeline_schedule = dg.ScheduleDefinition(
    name="monthly_full_pipeline",
    job=full_pipeline_job,
    cron_schedule="0 2 1 * *",  # 1st of month at 2:00 AM
    description=(
        "Monthly refresh: research agents scrape latest government data, "
        "then pipeline processes and exports updated jurisdiction taxonomy."
    ),
)
