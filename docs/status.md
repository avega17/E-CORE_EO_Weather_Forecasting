# Project status — November 17, 2026

This is a repository-maintenance status, not a live Polaris monitoring result.
Use the [operational commands](study_jobs.md) to establish current job state.
The October job IDs and progress snapshots are historical observations.

## Refactor

Code is organized into eight responsibility-based subpackages. Native
archive paths and formats are unchanged. The misplaced notebook report database
was moved to results/dataset-report with 596 audits, 1,784 diagnostic rows and
80 inventory rows preserved. The database remains the resumable report record. The CPU suite passed 414 cases
(14 optional/environment skips), the processing environment passed all 27 cases,
and all five notebooks executed in local kernels without starting a fetch.
398 completed task receipts were consolidated into portable study journals;
results file count fell from roughly 1,200 to about 400. Unresolved recovery bundles
and scientific selections remain protected.

## Research and operational limits

Native GOES and MRMS archives, missing-source evidence and partial checkpoints
remain authoritative. Full scan counts are not final archive completion.
Polaris uses an allocation-specific runtime source copy to prevent worker/coordinator
protocol differences. Existing benchmark conclusions do not establish performance
for new hardware or model skill. Regional processing and model experiments remain
separate from native acquisition; see the processing and model guides.
