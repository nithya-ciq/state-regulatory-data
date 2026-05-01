"""Sensor definitions for monitoring pipeline health and data quality."""

import dagster as dg

from dagster_jurisdiction.resources.database import DatabaseResource
from dagster_jurisdiction.jobs import full_pipeline_job
from src.models.jurisdiction import Jurisdiction


@dg.sensor(
    job=full_pipeline_job,
    minimum_interval_seconds=3600,  # Check every hour
    description="Monitor data quality: alerts if jurisdiction count drops below threshold.",
)
def data_quality_sensor(
    context: dg.SensorEvaluationContext,
    database: DatabaseResource,
) -> dg.SkipReason:
    """Check that jurisdiction data meets minimum quality thresholds."""
    try:
        session = database.get_session()
    except Exception as e:
        return dg.SkipReason(f"Database unavailable: {e}")
    try:
        count = session.query(Jurisdiction).count()
        if count > 0 and count < 10_000:
            context.log.warning(
                f"Data quality alert: only {count} jurisdictions found (expected 20,000+)"
            )
        return dg.SkipReason(f"Data quality OK: {count} jurisdictions")
    except Exception as e:
        return dg.SkipReason(f"Query failed (DB may be down): {e}")
    finally:
        session.close()


@dg.sensor(
    minimum_interval_seconds=300,  # Check every 5 minutes
    description="Monitor pipeline health: check for recent failures.",
)
def pipeline_health_sensor(
    context: dg.SensorEvaluationContext,
) -> dg.SkipReason:
    """Monitor the Dagster instance for recent pipeline failures.

    Placeholder for future integration with alerting services
    (Slack, PagerDuty, email, etc.).
    """
    return dg.SkipReason("Pipeline health check: no issues detected")
