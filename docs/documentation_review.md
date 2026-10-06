# Documentation consolidation — October 6, 2026

The initial review is now incorporated into a smaller current guide set:

| Need | Canonical guide |
|---|---|
| New-machine setup, code map, checks and recovery decisions | [Developer guide](developer_guide.md) |
| Launch, monitor, verified pause and pinned configuration handoff | [Study jobs](study_jobs.md) |
| GOES/MRMS concurrency and measured elapsed runtime | [Pipeline](goes_parallel_pipeline.md) |
| Backend choices, native layout, access, backups, cleanup and measured tradeoffs | [Storage](storage_and_data_management.md) |
| Notebook actions, expected outputs, CLI equivalents and troubleshooting | [Notebooks](notebooks.md) |
| Quality/statistical meaning and exported reports | [Dataset report](dataset_report.md) |
| Actual current operation | [Status](status.md) |

Merged/removed: old GOES concurrency, restart, performance and estimate guides,
native rollout, dataset fetch run, validation, raw-format narrative and the
Earth2Studio review. Useful scientific/storage facts and matched measurements are
retained in the guides above. Research ROI, model notes and source references
remain separate because they address scientific questions, not launch procedures.
Links are redirected to canonical guides. Retired scripts and ephemeral JSON
paths are not operational instructions.

The new-machine walkthrough, recovery table, portable MRMS wrapper, archive/scratch
retention policy and notebook retry guidance are now documented. Argonne scheduler
and network directives remain deliberately unresolved until the target system is
known. The two-month operating choice is not claimed as a newly measured speedup.
