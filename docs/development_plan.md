# Current development plan

The data sprint prepares reproducible native NOAA Caribbean subsets. Scientific
preservation and interfaces are defined in [storage](storage_and_data_management.md),
[study jobs](study_jobs.md), [the pipeline guide](goes_parallel_pipeline.md) and
[AGENTS.md](../AGENTS.md). Research references remain in
[agent references](agent_dev_references.md).

## Current work

- Following the workstation crash, verify and reuse surviving GOES scan batches
  for October 2021–January 2022, then resume chronologically with two normal
  month owners and one C02-only tail. July–September have complete checkpoints.
- Keep current MRMS four-product full-study checkpoints and verified DAS archives.
- Retain only current study records, active scratch/snapshot and compact backup
  receipts. Completed-source provenance remains with canonical archives.
- Consolidate operational/storage documentation and prepare the current changes
  for review and commit. Current job progress is in [status](status.md).
- The installed-NVIDIA MRMS comparison is complete: exact matched pixels/times,
  2.979 s stock CARIB versus 3.247 s project medians on 32 observations. Keep the
  four-writer/eight-download/one-decoder profile; this small source-read test does
  not justify more concurrency. See [pipeline measurements](goes_parallel_pipeline.md).

## Acceptance checks

Source-to-archive values, coordinates, timestamps and calibration/DQF or
bitmap/sentinel metadata must match. Check interruption/resume, completed-month
reuse, one owner per store, global resource bounds, notebook/CLI parity and status
behavior during index locks. Completion markers follow destination verification.
Matched mentor comparisons and bounded GOES evidence are summarized in
[pipeline measurements](goes_parallel_pipeline.md);
older timings are not current-code claims. Live HF or Colab checks must identify
the actual tested revision; they are separate from local operation.

## Next planning phase

Prepare scheduler-specific Argonne submission after node/network/time policies
are confirmed. Configure destination, local scratch/index and wall-time limits
through the existing portable interface. Do not infer Argonne concurrency from
workstation core count alone.

Future training preparation: decode and inspect native observations, validate
cloud-height-dependent parallax correction, map onto an explicit 1 km training
grid, then build aligned windows. CMI alone lacks cloud height. No ingestion-time
interpolation or correction is implemented. See [StormScope](StormScope-paper-notes.md),
[CorrDiff](CorrDiff-paper-notes.md) and [ROI considerations](goes_roi_training_study.md).
Model training, inference and managed cloud-native architecture are outside this sprint.
