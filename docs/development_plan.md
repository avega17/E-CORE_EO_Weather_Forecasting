# Current development plan

The data sprint prepares reproducible native NOAA Caribbean subsets. Scientific
preservation and interfaces are defined in [storage](storage_and_data_management.md),
[study jobs](study_jobs.md), [the pipeline guide](goes_parallel_pipeline.md) and
[AGENTS.md](../AGENTS.md). Research references remain in
[agent references](agent_dev_references.md).

## Current work

- GOES has drained to a verified pause after December 2022. The next fetch
  stage should resume chronologically with two normal month owners and one
  C02-only tail, reusing completed native-band archives.
- Keep current MRMS four-product full-study checkpoints and verified DAS archives.
- The [2022 full-CARIB MRMS RQI/reflectivity figure](mrms_roi_and_radar_coverage.md)
  is complete. Its one-day source-to-archive check and monthly aggregate
  checkpoints are separate from the raw ROI archives.
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

## Polaris and figure rollout

The revised 2022 overview/regional maps render from the cached summary under
figures/. MRMS machine profiles, bounded ROI queues, independent source-month
scheduling and a Polaris PBS wrapper are being validated locally. The Polaris
profile (8 writers × 8 downloads × 1 decoder) is an initial candidate.

Commit/push tested code, then clone the exact revision on Polaris and prepare
the dedicated CPU environment. Authenticate, confirm allocation/queue/quota,
run the isolated PBS pilot and the bounded 10 GiB tuning suite, then pin any
measured profile changes before production. Completed months are durable on
Eagle; unfinished node-local months may be refetched after a job ends.

Generate the remaining annual workstation maps only after the pilot and first
production month verify for both composite and base reflectivity. Remote setup,
tuning and production are not yet claimed complete.

Future training preparation: decode and inspect native observations, validate
cloud-height-dependent parallax correction, map onto an explicit 1 km training
grid, then build aligned windows. CMI alone lacks cloud height. No ingestion-time
interpolation or correction is implemented. See [StormScope](StormScope-paper-notes.md),
[CorrDiff](CorrDiff-paper-notes.md) and [ROI considerations](goes_roi_training_study.md).
Model training, inference and managed cloud-native architecture are outside this sprint.
