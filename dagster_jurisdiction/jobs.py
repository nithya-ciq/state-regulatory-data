"""Job definitions for the jurisdiction taxonomy pipeline."""

import dagster as dg


# Full pipeline: all 7 assets (research + phases 1-5)
full_pipeline_job = dg.define_asset_job(
    name="full_pipeline",
    selection=dg.AssetSelection.all(),
    description="Run the complete pipeline: research agents + all 5 phases.",
)

# Pipeline only: phases 1-5, skip research (uses existing seed CSVs as-is)
pipeline_only_job = dg.define_asset_job(
    name="pipeline_only",
    selection=dg.AssetSelection.all() - dg.AssetSelection.assets("research_data"),
    description="Run phases 1-5 only, skipping research (uses existing seed CSVs).",
)

# Classification only: just Phase 1
classification_only_job = dg.define_asset_job(
    name="classification_only",
    selection=dg.AssetSelection.assets("state_classifications"),
    description="Load/refresh state classification matrix only (Phase 1).",
)

# Export only: just Phase 5 (assumes prior phases have been materialized)
export_only_job = dg.define_asset_job(
    name="export_only",
    selection=dg.AssetSelection.assets("jurisdiction_export"),
    description="Re-export jurisdiction data without re-running prior phases (Phase 5 only).",
)

# Sync only: push existing jurisdiction data to Supabase
sync_only_job = dg.define_asset_job(
    name="sync_only",
    selection=dg.AssetSelection.assets("supabase_sync"),
    description="Sync jurisdiction data to Supabase without re-running the pipeline.",
)
