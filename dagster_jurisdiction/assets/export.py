"""Export tier asset — validate and export final data."""

import logging
import dagster as dg

from src.pipeline import phase5_validation_export
from dagster_jurisdiction.resources.database import DatabaseResource
from dagster_jurisdiction.resources.pipeline_config import PipelineConfigResource

logger = logging.getLogger("jurisdiction.dagster.export")


@dg.asset(
    group_name="5_export",
    deps=["enriched_jurisdictions"],
    description="Validate completeness and export to CSV, JSON, and Parquet.",
    kinds={"python", "postgres"},
)
def jurisdiction_export(
    context: dg.AssetExecutionContext,
    database: DatabaseResource,
    pipeline_config: PipelineConfigResource,
) -> dg.MaterializeResult:
    """Execute Phase 5: validation and export.

    Validates data completeness, then exports to CSV, JSON, and Parquet
    files in the configured output directory.
    """
    config = pipeline_config.to_config()
    session = database.get_session()
    try:
        results = phase5_validation_export.execute(session, config)
        session.commit()

        validation = results["validation"]
        exports = results.get("exports", {})

        # Fail hard if validation didn't pass
        if not validation["valid"]:
            issues = validation.get("issues", [])
            raise RuntimeError(
                f"Phase 5 FAILED: data validation did not pass. "
                f"Issues ({len(issues)}): {issues}"
            )

        metadata = {
            "total_jurisdictions": dg.MetadataValue.int(validation["total"]),
            "validation_passed": dg.MetadataValue.bool(validation["valid"]),
            "issue_count": dg.MetadataValue.int(len(validation.get("issues", []))),
            "warning_count": dg.MetadataValue.int(len(validation.get("warnings", []))),
        }

        # Add export file paths as metadata
        for format_name, file_path in exports.items():
            metadata[f"export_{format_name}"] = dg.MetadataValue.path(str(file_path))

        context.log.info(
            f"Phase 5 complete: {validation['total']} jurisdictions, "
            f"validation PASSED"
        )
        return dg.MaterializeResult(metadata=metadata)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
