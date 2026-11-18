# Current development plan

The package is organized by responsibility: core, sources, ingest, storage, jobs,
processing, reporting and ui. See the [developer guide](developer_guide.md) for
navigation and [study jobs](study_jobs.md) for operational commands.

## Current priorities

- Package migration, native/recovery fixtures, multiprocessing, reporting,
  processing and mocked PBS execution are validated. Deploy separately after
  checking live allocation ownership; no PBS job was submitted during maintenance.
- Keep defaults and scientific archive identities unchanged during maintenance.
- Keep current run state and unresolved failures; keep compact history in DuckDB.
- Preserve existing archives and source-gap facts. Resume only after verification.
- Keep processing and model dependencies separate from the ingestion environment.

## Later research work

Validate cloud-height/parallax correction and regional model preparation. A 1 km
output grid does not add information to native 2 km infrared observations.
Model skill and target-machine throughput require separate measured experiments.

Current observations and limitations belong in [status](status.md), instructions
in the canonical guides, and optional aggregate exports in
[evidence](evidence/README.md). Do not grow this plan into a history of job logs.
