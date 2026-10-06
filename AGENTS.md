# Project development guidelines

Prepare reproducible native NOAA Caribbean observations for this NSF-funded
forecasting project. Read [current plan](docs/development_plan.md),
[developer guide](docs/developer_guide.md) and
[source references](docs/agent_dev_references.md) before extending the pipeline.
Keep explanations readable for researchers inheriting the work.

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
- MRMS retains four product-month writers, eight downloads each, one decode slot
  each. Whole gzip downloads precede decoding; radar ROI does not reduce transfers.
- Pin changed code/configuration only after supported pause drains every admitted
  month. Never edit active snapshots. Exit 75 means intentional resumable pause.
  Status must show actual PIDs, heartbeat and advancing counts before claiming active.
- Report verified archive runtime boundaries, transferred/listed/retained bytes and
  memory separately. Per-band average rates are not NIC speeds. Two-month settings
  are a user operating choice, not a new measured speedup/global optimum.

## Index, backups and minimal results

- One coordinator owns DuckDB writes. Workers send aggregate counters. Read-only
  viewers retry brief locks or use completed manifests; never require ephemeral paths.
  Keep index rebuildable from canonical archive metadata, not per-object logging.
- Results keep only latest MRMS and ongoing native GOES configuration/checkpoints,
  active scratch/snapshot/run, compact backup receipts and the local index. One
  bounded live error log is useful; remove old tests, guards, probes and obsolete
  launch folders. Completed selection copies may be removed when canonical archive
  provenance and verified checkpoints remain. Never delete live scratch or research data.
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
  live HF, Colab or long-run checks without exact-revision evidence.
- Scheduler directives/submission remain deferred until the Argonne system/network
  policy is known. Portable MRMS configuration/wrapper are in jobs/ and documented
  in docs/study_jobs.md. Re-measure resource settings on the target machine.
