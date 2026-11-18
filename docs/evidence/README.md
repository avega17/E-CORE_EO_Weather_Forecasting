# Optional evidence exports

This directory holds small, explicitly requested summaries for sharing a measured
result or diagnosing an unresolved problem. Generated files are ignored by Git;
this explanation is tracked and does not depend on any particular run.

Routine progress belongs in the operational DuckDB index and current study state.
Diagnostics belong in results/dataset-report/report.duckdb. Source selections,
raw values and recovery checkpoints do not belong here.

Use the report command's `--evidence-dir docs/evidence` option to export one
replaceable summary.json. It records aggregate scope and outcomes, not individual
S3 assets, raw rasters, source code snapshots or credentials. An empty directory
means no optional export has been requested; it is not evidence of failed tests.

Keep useful methods and measured conclusions in the relevant guide. Once a
failure is resolved or its measurements have been imported into the report DB,
remove superseded generated exports. Never infer live job status from an old file.
