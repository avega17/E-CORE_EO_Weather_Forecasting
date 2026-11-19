# Processing observations for regional model experiments

Use notebook [05 Process datasets](../notebooks/05_process_datasets.ipynb) or
`python scripts/dataset_jobs.py process`. Both call the same functions. Native
monthly NOAA archives remain unchanged; derived arrays contain explicitly decoded,
quality-screened and regridded observations.

## Storage and environments

| Data | Workstation | Polaris |
|---|---|---|
| Native observations | Existing monthly ZIPs on P: | Verified monthly ZIPs on Eagle |
| Reusable preparation | Dataset root/preparation | Dataset root/preparation on Eagle |
| Derived plain Zarr | Dataset root/processed | Allocation-local scratch/processed |
| Live index and compact results | Linux results/dataset-processing | Local SSD; closed summaries on Eagle |

New transformed stores use **plain Zarr v3**, with lossless Blosc/Zstd level 3.
They open directly with xarray and the stock PhysicsNeMo training reader.
Icechunk is no longer a derived-storage option. Superseded stores and their
unusable state are deleted after a verified replacement, rather than kept behind
an unsupported-reader message. See the [retirement policy](developer_guide.md#retiring-generated-data).
Optional native virtualization remains a separate, explicit experiment; ordinary
processing does not build a virtual catalog or require Icechunk.

Create `environment-processing.yml` for CPU work. GPU packages belong in
`environment-model.yml`; never upgrade acquisition to enable model experiments.

On Polaris, `jobs/polaris_process.pbs` also provides `ECORE_PROCESS_TASK=setup`
and `benchmark`. Setup runs on a compute node, clones the project's existing
CPU Conda environment into a separate processing environment, then installs the
processing dependencies. It leaves the ingestion environment unchanged.

For a short 3 km timing comparison, source `jobs/polaris.env.sh` and submit:

```bash
qsub -A "$ECORE_PBS_ACCOUNT" -q debug -l walltime=01:00:00 \
  -v "$ECORE_PBS_ENV_VARS,ECORE_PROCESS_TASK=setup" jobs/polaris_process.pbs
# After setup exits successfully:
qsub -A "$ECORE_PBS_ACCOUNT" -q debug -l walltime=01:00:00 \
  -v "$ECORE_PBS_ENV_VARS,ECORE_PROCESS_TASK=benchmark,ECORE_PROCESS_START=2022-09-18T12:00:00Z,ECORE_PROCESS_END=2022-09-18T20:00:00Z,ECORE_PROCESS_RESOLUTION=3" \
  jobs/polaris_process.pbs
```

The benchmark compares 8/16/32/64 independent processes and a two-thread
reference on the same eight-hour selection, with three rotated trials. These
are CPU-affinity-aware processes, not extra threads per core. It includes
native reads, transforms, writing, closing and final read-back; successful
temporary stores are removed. The compact `worker-benchmark.json` and selection
description stay in `ecore-weather/state/processing/` on Eagle. Change
`ECORE_PROCESS_STATE` to isolate simultaneous reports, and pass that variable
through `qsub -v`. Fetches should be stopped for this controlled comparison.
Use the measured result before selecting workers for a larger build; the
workstation's eight-process result does not establish the Polaris setting.
The latter uses Earth2Studio 0.20.0, PhysicsNeMo 2.2.2 and a matched Torch/NATTEN
wheel pair. Verify target-machine driver compatibility before installation.

## First commands

```bash
python scripts/dataset_jobs.py process index \
  --start 2022-09-18T14:00Z --end 2022-09-18T18:00Z --resolution-km 3
python scripts/dataset_jobs.py process estimate --selection PREPARATION_PATH
python scripts/dataset_jobs.py process materialize --selection PREPARATION_PATH
python scripts/dataset_jobs.py process validate --unit DERIVED_SELECTION/full
python scripts/dataset_jobs.py process status --unit DERIVED_SELECTION/full
```

Copy the preparation path printed by Index. Dates are UTC; end is excluded.
Use `--bands --radar refc refc_base` for radar-only work, or `--radar` for GOES-only
work. Missing files remain absent observations, never synthetic zeros.
A saved selection fixes its scientific recipe. Re-index to change its grid,
quality, correction or layout. `--config` sets defaults; explicit CLI arguments
win. Notebook controls do not fetch during construction.

## Measurements, grids and time

Restore GOES unsigned encoding, fill/range, scale/offset, DQF and calibration per
scan. C01/C02/C03 are reflectance factors; selected infrared bands are brightness
temperatures in kelvin. Strict-good accepts DQF 0; conditional additionally accepts
DQF 1. Unknown quality remains distinct. Do not infer an outage from visible-band
nighttime behavior.

Radar retains valid negative dBZ. Product sentinels -99/-999, bitmap gaps and
nonfinite values remain missing. There is no clear-air clamping. Normalization
belongs at the model boundary, not in reusable physical-unit arrays.

The full acquisition bbox is (-70.24, 14.36, -62.56, 22.04). The WGS84 LCC grid is
centered on 66.4 W, 18.2 N, with standard parallels 15/21 N. Bounds round outward
to 12 km, giving patch-compatible dimensions:

| Spacing | Rows × columns |
|---|---:|
| 1 km | 864 × 840 |
| 2 km | 432 × 420 |
| 3 km | 288 × 280 |

GOES uses bilinear interpolation with valid contributors; flags are categorical.
Radar uses nearest-neighbor sampling. Coordinates outside the bbox or source
support are masked. A 1 km output does not add detail to native 2 km infrared.

GOES scan ends must be at/before a slot and no more than ten minutes old; radar
keeps the five-minute rule. Observations are not reused. Actual start/end times
remain separate from nominal slots. Six states span 50 minutes. Training windows
add the next ten-minute target and must stay entirely within their split.
These rules describe observation-time causality, not historical delivery latency.

## Store contents and reading

Each full/block unit contains `data.zarr`, an atomic `progress.json`, and a
`complete.json` only after final verification. Preparation holds scientific
Parquet indices and recipes, not duplicate raw rasters.

| Array | Dimensions | Meaning |
|---|---|---|
| values | time, variable, y, x | Physical float32 observations; invalid pixels are NaN |
| pixel_valid | time, variable, y, x | Declared pixel-quality policy |
| quality, classes | time, variable, y, x | Source flags and missingness categories |
| source_support | time, variable, y, x | Interpolation has geographic support |
| valid | time | All required variables have an observation and usable pixels |
| source_time_ns, source_end_ns | time, variable | Actual source timestamps |
| latitude, longitude, inside_bbox | y, x | Geographic coordinates and region mask |
| height_known, parallax_applied | time, variable, y, x | Corrected variants only; usable height-supported geometry |

Timestamp validity does not mean every pixel is valid. A GOES-only recipe does
not require radar. Unknown-height pixels in corrected bands remain unsupported;
zeros in correction flags on unshifted bands do not classify their cloud state.

```python
import xarray as xr
from ecore_weather.processing.process_store import ProcessedSource

with xr.open_zarr('/path/full/data.zarr', consolidated=False,
                  mask_and_scale=False, chunks=None) as ds:
    image = ds['values'].sel(variable='abi13c').isel(time=6).load()

with ProcessedSource('/path/full') as source:
    window = source(['2022-09-18T15:00Z', '2022-09-18T15:10Z'], ['abi08c', 'abi13c'])
```

Requests preserve order, including shuffled windows. Each reader caches only
metadata/static coordinates; close it after use. Worker processes open their own
handles. The experimental Flash adapter remains an engineering check, not a
validated Caribbean forecast.

## Workstation processing parallelism

Materialization uses independent processes for native reads, calibration and
resampling. `--workers 0` uses all CPUs allowed by the job's CPU affinity (32
hardware threads on this workstation). `--workers N` sets an explicit count;
`--executor thread` is retained for comparisons. This is unrelated to the number
of CPUs in a Colab runtime.

Four batches are prepared ahead by default (`--prefetch-batches 4`), distributing
work across variables and time so more than ten processes can be occupied. Each
worker keeps its own small archive-handle and geometry caches. A single writer
commits batches in time order, checks their values, then advances the checkpoint.
Transform processes never share a writable store. While the writer commits one
batch, processes can prepare later batches. A pause preserves the committed
prefix and discards only uncommitted work.

`--memory-mib` bounds the planned output buffers plus an allowance for worker
interpreters, geometry and decoded native arrays. The full-study workstation
configuration supplies 65,536 MiB. The operating system and scientific index
also need RAM; memory is not a target to fill. Numerical libraries use one
thread per process to avoid multiplying hidden thread pools. Actual throughput
can stop improving before all CPUs are busy when ZIP reads or DAS writes dominate.

To compare process counts on a saved selection of at most two days:

```bash
python scripts/dataset_jobs.py process benchmark --selection SHORT_PREPARATION \
  --worker-counts 8 16 32 --repeats 1 \
  --destination /mnt/p/ecore_eo_datasets/processed
```

This compares against two threads and includes output verification and final
publication. It checks identical batch hashes, records CPU time and summed
process RSS, and removes successful temporary stores. It reads native archives
without downloading NOAA objects. Retain the compact worker-benchmark.json.

The workstation's current 3 km profile uses **eight processes**, four prefetched
batches and a 64 GiB memory allowance. An eight-hour, ten-variable selection
produced identical verified arrays in all twelve rotated trials:

| Transform workers | Median total runtime, three trials |
|---|---:|
| Two threads | 38.67 s |
| Eight processes | 26.71 s |
| Sixteen processes | 28.02 s |
| Thirty-two processes | 28.28 s |

Eight processes reduced runtime by 31% relative to the updated two-thread path.
More processes did not help this DAS sample. Peak summed RSS was approximately
6 GiB for the eight-process case; the memory allowance is a limit, not a request
to allocate 64 GiB. Source reads remained the largest accumulated transform cost.
These short tests include process startup and do not establish a full-study rate.
The older writer also did unnecessary directory walks and repeated array-metadata
opens; it now flushes exact chunk paths and reuses array handles. No verification
or durability check was removed.

## Checkpoints, capacity and layout

One coordinator assembles each all-channel batch, writes complete chunks/shards,
flushes and verifies it, then atomically advances the committed prefix. A restart
checks that prefix and overwrites the unfinished batch. No committed shard is
rewritten. Final publication closes/reopens the store and verifies it before the
completion marker. Reader leases prevent deletion while data are in use.

`--chunk-time 1|6|12`, `--shard-time 0|24` and `--spatial-chunk 0|256` describe
layout. Zero spatial size means a full-ROI frame. Twenty-four-frame shards keep
independently readable chunks and use complete 24-frame checkpoints. Ordinary
one-frame storage checkpoints eight slots. Do not select a layout from peak Mbps.

```bash
python scripts/dataset_jobs.py process benchmark --unit DERIVED_SELECTION/full \
  --destination /mnt/p/ecore_eo_datasets/processed --disk-mib 64 --repeats 3
```

Benchmarks include values, flags and timestamps through the actual model-facing
reader. They measure 1/6/12-frame reads, writes, verification, physical files and
bytes. Fetching must be paused. Application caches are fresh; OS caches are not
claimed cold. Temporary candidates are deleted after success.

On the workstation DAS, three repeated six-frame reads took a median 0.114 s
with one-frame, full-ROI chunks and 0.148 s with six-frame chunks. The former
is the selected layout. Twenty-four-frame shards reduced the number of files,
but were slower in screening. These are bounded model-window measurements,
not a claim about the DAS's maximum bandwidth or full-study training speed.

`auto` prefers a complete variant when measured output plus temporary requirements
fit; `full` requires it; `blocks` creates bounded temporal units. Retain 5% of total
filesystem capacity. Workstation outputs go directly to DAS. Polaris variants stay
on local SSD and must be consumed in the allocation; preparation persists on Eagle.
A full 3 km study has 289,008 nominal slots before history padding, about 932 GB
of float32 values and another 932 GB for four byte-sized pixel arrays before
compression. Use empirical compression and current free space, not NOAA file size.

The current full-study request is saved in `jobs/process_3km_study.json`. Its
sample-based retained-size estimate is approximately 496 GB, including a 20%
allowance. The live run record is
`results/dataset-processing/materialization-run.json`; use its `unit` with
`process status` to read the durable checkpoint. Index coverage and an estimate
do not establish that the full materialization has completed.

## Cloud heights and parallax

The auxiliary native product is ABI-L2-ACHAF, not another forecast channel:

```bash
python scripts/dataset_jobs.py fetch goes --product ABI-L2-ACHAF \
  --start 2021-01-01 --end 2026-07-01 --destination /mnt/p/ecore_eo_datasets \
  --scratch /path/to/linux-scratch --output results/dataset-processing/heights --max-hours 0
```

This independently resumable job uses existing monthly storage/verification,
keeps native HT/DQF/status/calibration and includes a 50 km height-region margin.
Inspect actual historic grids; ACHAF is not assumed 2 km. Partial local batches
resume; completed monthly archives are durable. Unfinished node-local work may
be lost between Polaris allocations.

```bash
python scripts/dataset_jobs.py process index --start 2022-09-18T16:10Z \
  --end 2022-09-18T16:20Z --resolution-km 3 --correction cloud-height
python scripts/dataset_jobs.py process materialize --selection CORRECTED_PREPARATION
```

C01/C02/C03/C07/C13 use the newest same-platform height ending no later than CMI,
within twenty minutes. C08/C09/C10 stay unshifted. Unknown retrievals are not zero
height or inferred clear sky. Geometry and altitude units follow source metadata
and the documented Satpy approximation, not an exact ellipsoid claim. Corrected
CMI is resampled once onto the target grid. Mappings are reused only for identical
height evidence/native geometry/target grids; caches are bounded.

[Satpy's parallax API](https://satpy.readthedocs.io/en/stable/api/satpy.modifiers.parallax.html)
can replace undefined heights internally, so an independent uncertainty mask is
required. It does not guarantee that existing native ROI margins support every
corrected boundary pixel. Figures belong under figures/dataset-processing.

## Statistics, stock loading and inference

```bash
python scripts/dataset_jobs.py process stats --unit DERIVED_SELECTION/full \
  --train-start 2021-01-01 --train-end 2025-01-01
python scripts/dataset_jobs.py process loader-check --unit DERIVED_SELECTION/full \
  --physicsnemo-reference /path/to/regional_weather_diffusion/datasets
```

Training-only population moments go in stats.json; geographic fields go in
invariants.zarr. Empty/constant channels are errors. Validation is calendar 2025;
January–June 2026 is held out. Histories/targets may not cross split boundaries.
The [stock XarrayDataset contract](https://docs.nvidia.com/physicsnemo/latest/physicsnemo/examples/weather/regional_weather_diffusion/docs/XARRAY_DATASET.html)
uses these files directly. This check does not start training.

Pretrained inference uses checkpoint normalizers, not these regional statistics.
The separate model environment needs matching Torch/CUDA/NATTEN wheels, Flash
APIs, model weights and documented regional static fields. See
[model requirements](StormScope-paper-notes.md). Colab uses a bounded copied example;
Polaris uses a short allocation. Neither requires transferring the full study.
A supported-CONUS reference and a Caribbean engineering test are different checks.
Missing GPU/access/dependencies must produce an explicit blocked result.

The supported-CONUS reference completed two Flash steps on Colab L4 with the
CUDA 13 model environment. This closes the model execution smoke test; it does
not establish that the prepared Caribbean arrays satisfy every released model
requirement. No private dataset was uploaded for the CONUS check. A separately
approved Caribbean run used a bounded input and explicitly approximate elevation
scaling; its result is described below.

### A bounded Caribbean input while the full store is still being written

Use only the verified prefix, rather than waiting for the full study or reading
unwritten time slots. This command exports six ten-minute frames with their
coordinates, actual source times, quality masks and a scientific checksum:

```bash
python scripts/dataset_jobs.py process model-sample --unit FULL_STUDY_UNIT \
  --sample-time 2021-01-15T16:00Z --crop-size 256 --allow-partial \
  --output .cache/caribbean-flash-input
```

The January example has joint valid coverage of 99.98% across all eight bands
in its 256 × 256 crop. The export fixes the committed prefix at the moment it
opens the store; ongoing processing can continue. It rejects absent/future
observations and leaves every source array unchanged. Keep this small transfer
payload outside results/; retain its compact manifest with the model result.

Run the shared inference interface in the isolated GPU environment:

```bash
python scripts/dataset_jobs.py process model-check \
  --sample .cache/caribbean-flash-input/caribbean-sample.npz \
  --elevation .cache/caribbean-flash-input/caribbean-elevation.tif \
  --experimental-caribbean --download-model
```

The elevation sidecar must explain units, scaling, ocean treatment and evidence.
An empirical approximation is rejected by default. It requires the separate
`--allow-approximate-topography` option and produces explicitly experimental
results. This does not establish the released checkpoint's geographic validity.
Bounded forecast arrays are written beside model-check.json for inspection;
they are separate from the materialized observations.

After the approved approximate-topography run, both forecasts completed with
finite supported pixels. Compare the returned NPZ with later local observations:

```bash
python scripts/dataset_jobs.py process model-compare \
  --sample .cache/caribbean-flash-input/caribbean-sample.npz \
  --forecast .cache/caribbean-flash-input/caribbean-forecast.npz
```

This checks band order, grid coordinates and the last input, then reads only
committed observations for the forecast times. It writes per-band MAE/RMSE and
persistence errors to compact CSV/JSON and one figure under figures/. Later
observations never enter the inference payload. Persistence means repeating the
last input image. One January example beat that baseline across all eight bands
at both leads; it does not establish regional forecasting skill. See the model
notes for timings, static-field uncertainty and the example comparison.

## NWP audit and minimal outputs

`process nam-audit` probes one representative cycle in each study month and reads
bounded indexed GRIB messages. Exact byte ranges and decoded fields/levels must
match. The report distinguishes sample availability from complete cycle coverage,
retains initialization/lead/valid time and accumulation units, and measures crop
compression. It is not a bulk NWP ingest. The updated NOAA transition is checked
separately from historical study coverage.

Keep one processing database and current compact operation summaries. No per-pixel
or per-source operational logs. Preserve source provenance in canonical native
metadata. Successful disposable tests and retired formats are removed. Model
training, full-study corrected materialization and forecast-skill validation are
subsequent work.
