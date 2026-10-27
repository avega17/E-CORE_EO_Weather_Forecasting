# Current development plan

The data sprint prepares reproducible native NOAA Caribbean subsets. Scientific
preservation and interfaces are defined in [storage](storage_and_data_management.md),
[study jobs](study_jobs.md), [the pipeline guide](goes_parallel_pipeline.md) and
[AGENTS.md](../AGENTS.md). Research references remain in
[agent references](agent_dev_references.md).

## Current work

- Automatic GOES source recovery is implemented: bounded response/range checks,
  full-file fallback, exact checksum-backed source gaps and band-local deferral.
  Invalid NetCDF signatures no longer bypass handling. Existing Eagle parts
  migrate to a stable requested-selection mapping without rewriting verified
  pixels, including successive future gaps. Unresolved band-months stay pending
  and cannot authorize completion or backup. Source evidence is automatic, not
  a manually curated exception list. A final single-node Polaris run can use
  2024-09-01 to 2025-05-01, end excluded, to include April and its satellite
  transition. See [the command](study_jobs.md#single-node-recovery-for-september-2024-through-april-2025).
  Remote submission/recovery validation remain manual.

- The user confirmed the project Eagle quota is now **10 TB**, effective
  October 23, 2026. Extend Polaris GOES to the full study through June 2026.
  The PBS default dataset cap is removed; actual quota, filesystem and scratch
  checks remain. Native compressed GOES is roughly 0.86 TB, or 1.03 TB with the
  historical allowance. Preprocessing, validation datasets and model assets
  remain unestimated. The local workstation continuation remains unchanged.
- The [three-node GOES job](study_jobs.md#three-node-goes-fetching) is implemented
  with independent date partitions, node-local setup and private writable indices.
  It reuses existing selections/checkpoints and carries the known October 2023
  corrupt-source evidence. After workers stop, closed snapshots and previous
  GOES/MRMS records merge into one verified project index; failed merges retain
  inputs. Local tests cover ownership, bounds, independent exits and consolidation.
  A manual three-node pilot, target-machine placement/proxy checks and scaling
  measurements remain pending. No PBS job has been submitted in this update.

- The native GOES local continuation completed March–August 2023 and restarted
  at September after a C02 HDF5 range-read failure (see [status](status.md)).
  Saved September/October scan batches remain reusable. Source-specific errors
  identified a checksum-confirmed corrupt C02 file at 2023-10-13 14:20:20.9 UTC.
  It remains in STAC with its evidence and becomes an explicit observation gap,
  with no fabricated pixels or changes to saved measurements. Fresh-handle HDF5
  retries and incomplete-response checks handle transient failures; unconfirmed
  persistent failures still stop for inspection.
  It uses two normal months and one
  C02 tail, 8 GiB queued ROI and 64 GiB disk staging. Warn at 64 GiB summed RSS
  without an RSS stop; sustained available RAM below 8 GiB (60 seconds), or an
  immediate drop below 4 GiB, requests a checkpointed pause. Local `--max-hours 0`
  removes the twelve-hour cutoff. After all 66 months verify, sequential annual
  HF backups retain all eight native bands and satellite paths. Keep monthly DAS
  originals. Future GOES HF restoration to Polaris scratch is documentation-only
  and optional; expanded Eagle storage removes the original raw-study constraint.
  Eagle remains the long-term project data/model home.

- Supplied job 7776485 output shows May–July 2021 complete, then an application
  `MemoryError` just 1.05 MiB above the old 64 GiB RSS ceiling. Production now
  has a separate 384 GiB allowance and checkpointed handling of sustained memory
  pressure, with node availability and bounded process-level evidence. Preserve
  the three-month per-node profile and real shared-quota checks. See [memory recovery](study_jobs.md#goes-memory-protection-and-recovery)
  before resubmission; exact August–October recovery counts remain unverified.

- GOES job 7770028's supplied pause record confirms January–April 2021 complete
  (32 archives) in 8:24:40, not a sixteen-hour timeout. The old whole-month drain
  allowance stopped further admissions. The new Polaris profile has three normal
  month owners, the existing global pools, and incremental quarter-month Eagle
  checkpoints. Locally validate fresh-allocation restore, compact pause reasons
  and the revised deadline policy before manual submission; do not infer live
  Polaris success from these local checks.

- Historical Polaris GOES 7765036 failed after 5:56:43 on an internal DuckDB connection-mode
  race. The coordinator-monitor fix passed 132 local regressions (one mock case
  skipped). Its old node-local partial arrays were not durable across PBS
  allocations; the subsequent 7770028 record above supersedes that run's
  completion counts. Details and user-reported historical counts are in
  [the recovery guide](study_jobs.md#recovery-after-goes-job-7765036).
- Workstation GOES records include verified archives through August 2023,
  plus January–February 2026. Continue chronologically from September with two
  normal month owners and one C02-only tail, reusing completed native archives.
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
Single-node GOES has since run on Polaris; the user reports its latest job
reached the requested 24-hour walltime. The next proposed rollout is three nodes
covering unfinished 2022 through June 2026. Validate the multi-node launcher and
closed-index consolidation with a bounded pilot before submission; no three-node
execution or scaling result is claimed.
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
data. That was the earlier limited interval; current defaults cover the full
study. Explicitly reset old exported dates before extending a submission.
