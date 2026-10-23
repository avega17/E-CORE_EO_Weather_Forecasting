# Project development guidelines

Prepare reproducible native NOAA Caribbean observations for this NSF-funded
forecasting project. Read [current plan](docs/development_plan.md),
[developer guide](docs/developer_guide.md) and
[source references](docs/agent_dev_references.md) before extending the pipeline.
Keep explanations readable for researchers inheriting the work.

## Job startup and code lineage

- Do not inspect Git commits, enforce a clean checkout, or calculate/compare code
  hashes unless the developer explicitly requests that work. This is not a
  default reproducibility requirement for this small research team.
- Code-lineage metadata must never block, fail or delay a PBS job, fetch,
  resumption or environment setup. PBS wrappers do not require a Git checkout,
  a pinned commit, `ECORE_REVISION`, or a clean working tree. Optional recording
  through `ECORE_RECORD_CODE_IDENTITY=1` is explicit opt-in and best effort.
- Continue validating scientific selections, source/array equality, archive
  integrity and safe storage ownership. Those protect research data and are
  separate from code-lineage checks.
- Print a startup message before setup checks. Report a named phase and the
  actual exit status on failure; never dump credentials or the full environment.

## Scientific preservation

- Persist native ROI values, grids, timestamps, units, metadata and quality unchanged.
  Lossless format/compression changes are allowed; cleaning, interpolation,
  reprojection, normalization, imputation and aggregation are separate processing.
- GOES retains packed CMI, DQF, scale/offset/fill, scan calibration and projection.
  MRMS retains decoded GRIB numbers and bitmap missingness, not original GRIB bytes.
- Never mask all negative values. Reflectivity negatives can be valid. Distinguish
  zero rain, product-specific sentinels, bitmap gaps, unknown shear zeros and absent
  files. No missing observations become synthetic zeros; no future observations
  enter model windows. Preserve accumulation meaning and physical units.
- Never overwrite raw archives from diagnostics, map rendering or derived processing.
  Released StormScope compatibility is not established by the Earth2Studio API.
- The optional full-CARIB RQI figure is a derived report. NOAA documents -1/-3;
  valid zero remains a low score. Count observed out-of-range values such as
  -285.3 separately rather than declaring a new official sentinel or changing
  saved source values.

## Selection and defaults

- NOAA reads are anonymous; credentials stay out of URLs, logs, notebooks and Git.
- CARIB MRMS study products: precipitation rate, composite reflectivity and exact
  underscore/00.50 low-level shear keys on ten-minute slots, plus hourly multisensor
  Pass2 QPE. Match newest at/before each slot within five minutes without reuse.
  Record actual times, offsets and missing slots. S3 LastModified is not historical
  operational availability. Rolling QPE remains an accumulation.
- GOES: CMIPF, all available full-disk scans, C01/C02/C03/C07/C08/C09/C10/C13.
  Native bands stay separate (C02 nominal 0.5 km, C01/C03 1 km, infrared 2 km at nadir).
  Custom Earth2Studio sources remain one band per call; no implicit xarray alignment.
- MCMIPF/hybrid are optional distinct historical products. Never convert/relabel
  their pixels as native CMIPF. Hybrid reuse needs exact-time/grid checks and
  per-band origins. Preserve source-specific corrupt/missing-scan evidence.
- Training, inference, 1 km interpolation and cloud-top parallax correction are
  future processing. CMI alone does not provide the cloud height needed for correction.

## Storage and concurrency

- Read [storage](docs/storage_and_data_management.md) and [pipeline](docs/goes_parallel_pipeline.md).
  One compressed canonical archive per source/product/native ROI-grid/month.
  CMIPF has separate band ZIPs; optional MCMIPF groups bands in one ZIP.
- Local archive backend: Earth2Studio ZarrBackend, Zarr v3 lossless compression.
  Completion follows close/pack/copy/read-back verification. Deterministic verified
  scan batches and staged receipts survive interruption. Each store has one owner.
- Default long GOES destination is mounted P: DAS. Verify mount, write access and
  free space; use fast Linux scratch and a Linux-local DuckDB file.
- GOES selected limit: **two normal month owners plus one C02-only tail owner**, 32 global
  async downloads for seven bands, two local HDF5 readers, eight C02 range readers,
  2,048 MiB global ROI budget and 16,384 MiB staging. Limits do not multiply by band
  or month. HDF5 concurrency requires independent processes, not more threads.
- MRMS workstation defaults are four product-month writers, eight downloads each,
  one decode slot, eight admitted observations and 128 MiB ROI queue per writer.
  The measured Polaris MRMS profile uses twelve writers, four downloads, one
  decoder, sixteen admitted observations and 256 MiB ROI queues.
  Versioned MRMS JSON profiles are overridden by explicit CLI resource arguments.
  Source-month task partitioning prevents adjacent workers sharing a physical ZIP. Whole gzip downloads precede decoding; radar ROI does not reduce transfers.
- Update active fetching code/configuration only after supported pause drains every
  admitted month. Never edit a running job's files. Exit 75 means intentional resumable pause.
  Status must show actual PIDs, heartbeat and advancing counts before claiming active.
- Report verified archive runtime boundaries, transferred/listed/retained bytes and
  memory separately. Per-band average rates are not NIC speeds. Two-month settings
  are a user operating choice, not a new measured speedup/global optimum.

## Index, backups and minimal results

- One coordinator owns DuckDB writes. Workers send aggregate counters. Read-only
  viewers retry brief locks or use completed manifests; never require ephemeral paths.
  Keep index rebuildable from canonical archive metadata, not per-object logging.
- Inside the coordinator, progress queries use its read-write connection mode and
  the same lock as writes/closed snapshots. Never mix a read-only monitor with
  writable connections to that file in the same process. External viewers remain
  read-only and Polaris monitoring reads the durable progress JSON.
- Results keep only latest MRMS and ongoing native GOES configuration/checkpoints,
  active scratch/snapshot/run, compact backup receipts, derived report summaries
  and the local index. Generated images/animations belong under figures/, never docs/assets/. One bounded live error log is useful; remove old tests,
  guards, probes and obsolete launch folders. Completed selection copies may be
  removed when canonical archive provenance and verified checkpoints remain.
  Never delete live scratch or research data.
- HF is backup storage: one MRMS product/year ZIP of verified monthly members.
  Verify remote SHA-256/read-back before relying on it. Annual ZIPs need selected
  verified monthly restoration for analysis. Keep compact receipts; remove redundant
  annual working ZIPs only with retained matching originals or verified remote identity.
- HF_BUCKET_NAME=namespace/bucket; HF_DATASET_REPO is a compatibility alias.
  HF_TOKEN is not an S3 key. Use namespace endpoint, bare bucket, path addressing,
  us-east-1 and required checksum behavior. Optional direct HF monthly writes use
  AsyncZarrBackend with one publisher/read-back. Never silently redirect failures.

## Interfaces and validation

- Reusable code belongs in src/ecore_weather. Keep scripts/dataset_jobs.py and
  scripts/dataset_report.py thin. Add supported operations instead of bespoke watchers.
- Author percent-format notebook .py files; synchronize clean .ipynb partners.
  Widgets and CLI call the same functions, expose clear labels/tooltips and do not
  fetch during construction. Notebook 03 name remains 03_explore_datasets.
- Colab: detect, clone configurable revision, install dependencies, report actual
  commit. Push changed imported modules before clone-based validation.
- Conda environment.yml separates conda-forge system libraries from pip-only packages;
  GPU/model dependencies stay out of this CPU environment.
- Check exact scientific values/coordinates/quality/calibration or bitmap/sentinels,
  interruption/reuse, shared resource bounds, source transitions and status/lock
  behavior. Network benchmarks need verified pause and an explicit byte budget.
  `dataset_jobs.py benchmark mrms` compares exact CARIB reflectivity sources;
  stock CONUS and region-adapted comparisons must be labelled separately.
  Brief external read-only DuckDB locks are retried by the writer for up to
  30 seconds; do not leave exploratory connections open indefinitely.
- Diagnostic percentages are pixel-weighted; do not average image quartiles into
  pooled quartiles. Missing observations and missing pixels are distinct. Day-level
  performance requires day-level evidence, not manufactured month-log estimates.
- Update README and canonical docs when interfaces change. Preserve useful compact
  findings in guides rather than proliferating test result folders. Do not claim
  live HF, Colab or long-run checks without actual run evidence.
- Polaris PBS uses one node, the documented outbound proxy, local scratch/index
  and verified monthly publication to Eagle. Use debug for bounded environment setup, pilot and tuning, then
  capacity for the full study, after account/queue/quota checks. Completed months
  are durable; unfinished local stores may be lost between jobs. Push tested
  changes so they are available during remote setup; jobs do not verify commits.
  The measured Polaris MRMS profile uses twelve product-month writers, four
  downloads each and one decoder each; workstation settings remain four/eight/one.
  Use only documented PBS commands. Bound installer bytecode workers with
  MAMBA_EXTRACT_THREADS=4; do not run heavy installation on a login node.
  Re-measure settings on the target machine.
  Annual radar figures may run on the workstation while the Polaris job is
  queued, as authorized after the successful pilots and tuning. Keep GOES paused
  and separate figure reads from controlled network benchmarks.

- Polaris GOES has a separate **unmeasured three-month profile**: three normal month
  owners plus one C02 tail, 64 global async downloads, eight local HDF5 readers,
  sixteen C02 range readers, 4,096 MiB ROI and 32,768 MiB staging. Do not change
  workstation settings or call this profile optimal without target-machine data.
  The manual four-context pilot has a 32 GiB transfer ceiling; this sprint does
  not submit it. GOES study dates are 2021-01-01 to 2023-01-01, end excluded,
  with a 400 decimal GB cap for GOES Eagle data/state. MRMS PBS requests six hours.
- Polaris GOES production RAM protection is separate from benchmark ceilings:
  warn at 256 GiB summed RSS; pause after 60 seconds above 384 GiB RSS or below
  32 GiB node-available RAM; below 8 GiB available, pause without the grace delay.
  Sample every five seconds into one bounded memory.json and progress snapshot.
  Summed RSS includes shared pages more than once; do not call it unique RAM,
  VRAM, or the 4 GiB ROI queue. Memory pressure requests checkpointed exit 75,
  not a benchmark MemoryError. Pilot/scheduler benchmark limits remain strict.
- Status aggregate bandwidth is returned-data throughput over common elapsed
  time, not a sum of per-band rates or NIC speed. Recent average/median use at
  most sixty fresh snapshot intervals in the monitor's small local cache.
  Repeated snapshots add no zero samples; run/counter changes reset the window.
- GOES PBS study jobs publish incremental compressed partial checkpoints to
  Eagle approximately every 25% of a band-month (rounded to verified eight-scan
  batches). Restore them into local scratch on resubmission; never mark them as
  complete monthly archives. Count them against the GOES cap and delete them
  only after final ZIP verification. Supported shutdown attempts to save the
  latest verified batch; sudden node loss can lose work since the last durable
  part. With this enabled, use the configured admission drain interval rather
  than 130% of a whole-month runtime. Preserve a compact, explicit pause reason.
- PBS fetch dates come from PBS_GOES_START/END and PBS_MRMS_START/END, in ISO UTC.
  Setup/tuning are date-independent. Do not export the entire .env to PBS.
  Solar conditioning is cosine of **zenith**, not azimuth; the model computes it
  at inference. GPU inference and analysis-ready preprocessing remain separate.
- Source `jobs/polaris.env.sh` for non-sensitive Polaris defaults and a fresh
  conservative quota estimate; it does not select a job mode or read `.env`.
  Refresh quota before each submission. A wrapper dry run tests local setup;
  `python -m ecore_weather.pbs check-noaa --polaris-proxy` tests bounded real
  GOES listing without imagery. Downloads, range reads, native cropping and
  Zarr verification belong in the existing bounded pilot, not a dry run.
