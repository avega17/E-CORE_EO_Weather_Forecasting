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
scheduling and a Polaris PBS wrapper passed local checks and a live PBS pilot. The measured Polaris
profile is twelve writers × four downloads × one decoder; workstation defaults
remain four × eight × one.

Push tested code, then clone/update the repository on Polaris and prepare
the dedicated CPU environment. Authenticate, confirm allocation/queue/quota,
run the isolated PBS pilot and the bounded 10 GiB tuning suite, then save any
measured profile changes before production. Do not gate jobs on Git commits,
tracked-file changes or code hashes. Completed months are durable on
Eagle; unfinished node-local months may be refetched after a job ends.

The user authorized the remaining annual workstation maps while Polaris is
queued, after the successful pilots and tuning. The 2021–2025 and H1 2026
figures are now complete and validated; 2022 reused its cached summary. Polaris MRMS job 7743827 subsequently completed successfully in 1:14:14,
with both reflectivity products archived through June 2026. Remote CPU
setup and two pilots passed on `4db62f1`; tuning passed on `1f017e0`.
Production job `7743827` used `2038b86` and has no unfinished product-months.
The next manual rollout is the 2021–2022 GOES PBS job with a 400 GB retained-data
cap and a separate, higher-concurrency Polaris starting profile. It needs its
bounded four-context pilot; no Polaris GOES performance result is claimed.
The scripts use explicit ISO PBS date variables. MRMS now requests six hours.
See the operational guide for manual submission and the StormScope notes for
model/data requirements, including internally computed solar zenith at inference.

Future training preparation: decode and inspect native observations, validate
cloud-height-dependent parallax correction, map onto an explicit 1 km training
grid, then build aligned windows. CMI alone lacks cloud height. No ingestion-time
interpolation or correction is implemented. See [StormScope](StormScope-paper-notes.md),
[CorrDiff](CorrDiff-paper-notes.md) and [ROI considerations](goes_roi_training_study.md).
Model training, inference and managed cloud-native architecture are outside this sprint.

## GOES PBS failure investigation — October 20, 2026

Pilot 7762801 ended with exit 1, zero reported CPU time and empty output. Its
execution-to-exit timestamps span ten seconds; no successful GOES pilot is
established. A local reproduction confirms the old revision/clean-checkout
guards can produce this symptom, but the specific remote cause is unconfirmed.
At the developer's request those guards and the required revision variable are
removed from all PBS wrappers. Code-lineage inspection is off by default and
must never block jobs. Startup/phase/exit diagnostics now replace silent checks.
Follow the
[debugging sequence](study_jobs.md#debugging-a-goes-job-that-ends-without-output):
inspect failed-job records, run a short interactive wrapper dry run, then
validate bounded NOAA/storage probes and the
existing four-context pilot before another study submission. Keep competing
jobs held; preserve archives and recovery state.

Polaris configuration now lives in `jobs/polaris.env.sh`; sourcing refreshes a
conservative quota estimate from `myprojectquotas` without reading credentials.
The debug sequence separately checks local setup, a real one-hour NOAA listing
and the existing download/storage pilot. A full-day fetch is deferred until the
pilot succeeds. The configuration defaults to the requested first year of GOES
data; the two-year study interval remains an explicit later override.
