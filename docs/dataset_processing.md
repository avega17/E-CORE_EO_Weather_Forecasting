# Processing observations for regional model experiments

Notebook [05 Process datasets](../notebooks/05_process_datasets.ipynb) and
`python scripts/dataset_jobs.py process` use the same functions. Start with
completed NOAA subsets. Processing creates a separate dataset; the native
monthly archives retain their original measurements and metadata.

The first examples cover September 18, 2022 and January 31, 2026 at 18:00 UTC
through February 1 at 06:00 UTC. These provide day/night observations, both
satellites and a month boundary. Dates use an exclusive end. QPE is excluded:
the radar inputs here are composite and base reflectivity.

## Three kinds of stored data

| Contents | Workstation | Polaris |
|---|---|---|
| Native observations | Existing monthly ZIPs on P: | Existing monthly ZIPs on Eagle |
| Reusable preparation | `/mnt/p/ecore_eo_datasets/preparation/` | `<archive root>/preparation/` on Eagle |
| Transformed arrays | `/mnt/p/ecore_eo_datasets/processed/` | Job-specific `/local/scratch/.../processed/` |
| Live query index and compact reports | `results/dataset-processing/` on Linux | Scratch index; closed copy and summaries on Eagle |

Preparation contains portable Parquet tables, scientific recipes, interpolation
geometry and optional virtual Icechunk snapshots. It is much smaller than a
second copy of the images. The local DuckDB file indexes preparation selections;
deleting it does not delete any observations or prevent rebuilding it.

An Icechunk snapshot is a consistent version of stored arrays. It does not
automatically save and execute a Python processing graph. A virtual array points
to compressed chunks inside an existing ZIP, so that ZIP must remain available.
Rebuild references when moving from workstation paths to Eagle or renaming raw
directories. Our virtual reader also restores source metadata from the native
sidecar; the generic ZIP parser alone omits those scientific records.

Each native archive has an independent virtual group. Separate band grids are
never implicitly merged by xarray. See [VirtualiZarr's ZIP parser](https://virtualizarr.readthedocs.io/en/stable/api/parsers/zip.html)
and [Icechunk virtual ingestion](https://icechunk.io/en/latest/guides/zarr/#virtually-ingesting-existing-zarr-stores).

## Environment and first commands

Create the separate processing environment; do not upgrade an environment used
by a live fetch:

```bash
conda env create -f environment-processing.yml
conda activate ecore-processing
python -m ipykernel install --user --name ecore-processing --display-name 'E-CORE processing'
python scripts/dataset_jobs.py process --help
```

Local validation used an isolated clone under `.cache/process-env` with
Icechunk 2.2.0, VirtualiZarr 2.7.3, SciPy 1.18.1, pyresample 1.35.0 and
Satpy 0.59.0. Acquisition remains Earth2Studio 0.18.0. GPU/model work uses a
different environment; ordinary processing does not require a GPU.

Build a scientific index, then copy the printed preparation path into the
following commands:

```bash
python scripts/dataset_jobs.py process index --start 2022-09-18 --end 2022-09-19 --resolution-km 1
python scripts/dataset_jobs.py process catalog --selection /path/to/preparation/selection
python scripts/dataset_jobs.py process estimate --selection /path/to/preparation/selection
python scripts/dataset_jobs.py process materialize --selection /path/to/preparation/selection
python scripts/dataset_jobs.py process validate --unit /path/to/processed/selection/full
```

Choose `--radar refc` when only composite reflectivity exists, or `--radar`
with no values for a GOES-only experiment. Missing products appear in the index
and make affected windows ineligible; the processor never invents radar data.
Notebook 05 has the same paths, dates, quality and storage controls. Construction
does not read imagery. **Index** lists observations; **Estimate** reads a small
sample; **Materialize** writes the selected variant.
For a radar-only preparation, use `--bands --radar refc refc_base`.

`--config file.json` provides parser defaults. Explicit command arguments override
them. When `--selection` is supplied, its saved scientific recipe is authoritative;
build a new index to change resolution, bands, quality or compression.

## What happens to the measurements

Per-scan GOES metadata supplies the correct unsigned interpretation, scale,
offset, fill and valid range. This avoids calibrating a whole month using one
scan's attributes. `good` quality accepts DQF 0; `conditional` additionally accepts
DQF 1. Source categories, unknown quality and numeric missingness remain distinct.

The [NOAA reprocessed L1b guide](https://github.com/NOAA-Big-Data-Program/nodd-data-docs/blob/main/GOES/GOES-R_ABI_Reprocessed_L1b_User_Guide-v1.1.pdf)
explains calibration, navigation, eclipse artifacts and missing source data.
Our archives are operational **L2 CMIPF**, so this guide does not by itself define
their DQF policy. Use saved CMIPF flag metadata and the relevant product definitions;
reprocessed L1b fill codes and its extra pixel timestamps are not assumed present.

MRMS decoded numbers and bitmap missingness are evaluated separately. The
reflectivity sentinels -99/-999 remain missing/no-coverage evidence. A bitmap-valid
cell is not automatically a valid radar measurement, and valid negative dBZ is
retained. Nothing is automatically converted to clear air.

The common grid uses WGS84 Lambert Conformal Conic with central longitude
66.4°W, latitude 18.2°N and standard parallels 15°/21°N. It encloses the full
acquisition bbox, with common edges rounded outward to 6 km boundaries:

| Grid spacing | Rows × columns |
|---|---:|
| 1 km | 858 × 840 |
| 2 km | 429 × 420 |
| 3 km | 286 × 280 |

Rectangle corners outside the geographic bbox or native source support are masked.
GOES uses bilinear interpolation, including 2 km infrared to 1 km. Every
contributing pixel must be valid; invalid regions are not bridged. Quality labels
use a categorical nearest sample. Radar uses nearest-neighbor sampling for the
initial baseline rather than silently choosing a different reflectivity target
through dBZ averaging or maximum pooling. A 1 km output does not create finer
infrared observations.

The output arrays are `values`, `valid`, `quality`, `classes` and `source_support`,
with dimensions `(time, variable, y, x)`. `values` contains physical float32
measurements; invalid values are NaN. Actual source start/end timestamps are
stored separately from the common time coordinate. Latitude, longitude and the
bbox mask are two-dimensional arrays on the projected grid. Normalization for
model inference occurs at the model boundary, not in these reusable values.

## Time windows and random reads

GOES scan ends must be at/before the nominal slot and at most ten minutes old.
Radar uses its five-minute newest-at/before match. A source observation is not
reused in another slot. These rules establish observation-time causality, not
historical delivery latency. Source start/end times and offsets remain in
`aligned.parquet`; GOES does not rely on its often-empty raw requested-slot field.

`windows.parquet` identifies six-observation histories and reports missing
observations. Their timestamps span 50 minutes. Pixel gaps do not become missing
files, and complete timestamps do not imply complete pixel coverage.

```python
from ecore_weather.process_store import ProcessedSource

source = ProcessedSource('/path/to/processed/selection/full')
window = source(
    ['2022-09-18T12:00Z', '2022-09-18T12:10Z', '2022-09-18T12:20Z'],
    ['abi08c', 'abi13c'],
)
# Physical units; dimensions: time, variable, y, x.
# Request order is preserved, so shuffled eligible windows are supported.
```

## Complete selections, block fallback and recovery

`--mode auto` selects a complete dataset when the estimate fits the chosen
filesystem; `full` requires it to fit; `blocks` explicitly limits the working set.
All modes keep **5% of total filesystem capacity free**, account for existing
use and temporary writes, and check available space while processing.

Estimates use sampled transformed arrays and their actual compression, not NOAA
object lengths. A 20% output-size allowance handles sample uncertainty. A nominal
full-study float32 1 km array for eight GOES bands and two radar variables is
about 8.3 TB before masks. Whether compression makes it fit on a Polaris node
must be measured; the 3.2 TB disk rating alone is insufficient.

Full mode publishes verified incremental snapshots, then a final completion
record. Only the final complete selection enables unrestricted eligible-window
sampling. `--compression zstd` is the lossless default; `none` is an explicit
experimental representation. Raw chunks are decompressed only as needed in
bounded batches. Extracting the outer ZIP would not remove Zarr decompression.

Block mode starts at 24 hours with five history frames of overlap. The standalone
command builds at most two new blocks per invocation; `--max-blocks` changes that
limit. Resume skips completed blocks when applying the limit. A small consumer interface prepares the next block
while the current block is read:

```python
from ecore_weather.process_store import iter_materialized_blocks

for source in iter_materialized_blocks(preparation, scratch_destination, release=True):
    # Read eligible windows listed in source.path / 'windows.parquet'.
    # All reads for this block must finish before requesting the next block.
    pass
```

`release=True` explicitly removes disposable derived blocks after use; its
default is to retain them. Reader locks prevent deletion during active use.
The standalone materializer never deletes old blocks. Global random sampling
across evicted blocks is not implemented. Each node has its own scratch, not a
shared multi-node disk.

One coordinator commits a filesystem Icechunk repository. Threads compute
independent band batches; each ZIP handle belongs to its reader, and only the
coordinator writes/commits. Read-back checks values, masks, quality and source
times before advancing the processing checkpoint. Resume verifies the committed
prefix and reuses it. SIGTERM/SIGINT or `--max-hours` requests a checkpointed exit
75 at a batch boundary. Node-local work is disposable after allocation loss;
Eagle preparation remains reusable.

The [Icechunk consistency discussion](https://www.earthmover.io/blog/learning-about-icechunk-consistency)
explains transactions; [filesystem limitations](https://icechunk.io/en/stable/guides/storage/#filesystem-storage)
still rule out simultaneous commits. [Parallel writing](https://icechunk.io/en/stable/understanding/parallel/)
requires the coordinating session to know about all writes. Our processor uses
one writer rather than uncoordinated worker sessions. Snapshots version data and
are unrelated to Git/job code-lineage checks.

## Storage benchmarks and USB performance

Run during a supported fetch pause:

```bash
python scripts/dataset_jobs.py process benchmark --unit /path/to/processed/selection/full \
  --destination /mnt/p/ecore_eo_datasets/processed --repeats 3 \
  --linux-comparison /tmp/ecore-processing-comparison
```

Dedicated disposable files measure sequential reads, fsync/close writes and
simultaneous reading/writing. Derived-array comparisons use identical cached
tensors, compressed/uncompressed layouts, 1/6/12-frame chunks and 1/2/4 readers.
Final comparisons repeat three times with rotated layout order. Linux samples
are bounded; successful stores are removed. Cache conditions are recorded:
fresh application handles do not mean filesystem caches were flushed. USB
10 Gbit/s is an interface ceiling, not a measured disk speed. Inspect windows/s,
batch latency, CPU/decompression waiting and process I/O counters before calling
the DAS a model bottleneck. `--allow-contended` explicitly labels exploratory
comparisons alongside fetching; it is not a controlled result.

## Cloud-height and model engineering pilots

```bash
python scripts/dataset_jobs.py process parallax --selection /path/to/preparation/selection
python scripts/dataset_jobs.py process model-check --unit /path/to/3km/selection/full \
  --experimental-caribbean --model-package /path/to/flash/package --elevation /path/to/topography.tif
python scripts/dataset_jobs.py process nam-audit
python scripts/dataset_jobs.py process report
```

The parallax operation uses a 2 GiB imagery ceiling, matched same-satellite ACHA
heights and a 10 GiB disposable mapping cache. The attached *Parallax Shift in GOES
ABI Data* paper motivates spatially varying heights and inspection of artifacts.
[Satpy](https://satpy.readthedocs.io/en/stable/api/satpy.modifiers.parallax.html)
implements the correction. Unknown heights remain unknown even when a library
uses zero internally. C02/C13 are the initial experiments; water-vapor correction
is not automatically enabled. Corrected coordinates are mapped directly to the
target grid to avoid unnecessary intermediate resampling. Figures go to
`figures/dataset-processing/`.

The model check requires a separate CUDA/PhysicsNeMo/NATTEN environment, an actual
compatible Flash package and regional static fields. It starts with a 256×256
3 km crop, one ensemble member and two forecast steps, retrying 200×200 on GPU
memory exhaustion. An explicit project adapter replaces regional geometry and
static fields, rebuilds spatial caches and preserves weights/normalization. It
does not simply disable the stock `set_domain()` bounds check. Results are
engineering checks, not evidence of Caribbean forecast skill or successful training.

Elevation uses a companion `.tif.json` with `scale_factor`, `add_offset`, `ocean`
(`zero` or `retain`) and `checkpoint_convention_source`. These describe the
selected checkpoint's static-input convention. Do not infer that scaling from
the numerical range of a package array. A bounded [NOAA ETOPO subset](https://www.ncei.noaa.gov/products/etopo-global-relief-model)
can supply regional elevations once the convention is established.

The wrapper supplies cosine of solar zenith for the latest input and target
forecast times. Do not append duplicate solar channels or normalize physical
arrays twice. Coupled radar inference still requires GLM and additional radar
invariants. The [NVIDIA/Earthmover example](https://www.earthmover.io/blog/building-a-low-latency-earth-2-nowcasting-pipeline-with-earthmover)
informs input selection and overlapping work; its managed-cloud timings are not
Polaris or DAS performance predictions.

## Polaris materialization pilot

Create the processing environment in a compute allocation, then use the wrapper
with explicit exported paths and dates. No login instructions or code-hash gate
are required:

```bash
source jobs/polaris.env.sh
# Create this environment inside a compute allocation, before the pilot:
# conda env create --prefix "$ECORE_PROJECT_ROOT/ecore-weather/envs/processing" -f environment-processing.yml
export ECORE_PROCESS_PYTHON="$ECORE_PROJECT_ROOT/ecore-weather/envs/processing/bin/python"
export ECORE_PROCESS_START=2022-09-18 ECORE_PROCESS_END=2022-09-19
export ECORE_PROCESS_RESOLUTION=1 ECORE_PROCESS_MODE=auto
qsub -A "$ECORE_PBS_ACCOUNT" \
  -v ECORE_PROJECT_ROOT,ECORE_CODE_ROOT,ECORE_PROCESS_PYTHON,ECORE_PROCESS_START,ECORE_PROCESS_END,ECORE_PROCESS_RESOLUTION,ECORE_PROCESS_MODE \
  jobs/polaris_process.pbs
```

The wrapper is a materialization pilot, not a training job. It records a bounded
time limit from PBS and publishes compact summaries/closed index to Eagle.
Use the resulting node-local arrays inside that same allocation when integrating
a future model consumer. Subsequent allocations may rebuild them from durable
preparation. Multi-node training and random distributed samplers remain later work.

## Evidence and current limits

`results/dataset-processing/` holds one latest operation summary and compact
benchmark, parallax, model and NAM receipts. Parquet scientific indices and
transformed arrays stay outside results/. `report` combines receipts into
readable Markdown; it does not fabricate a completed model test from dependency
checks. See the current status for measured runs and exact outstanding blockers.
The CLI returns 75 for an intentional resumable pause and 2 for a reported
blocked experiment; a blocked model check is never a successful forward pass.

Local checks on October 27, 2026 used the complete acquisition bbox:

| Grid | Observation interval, UTC | Stored slots including history | Physical derived store | Array construction and verification |
|---|---|---:|---:|---:|
| 3 km | September 18, 2022 | 149 | 243.3 MB | 286.6 s |
| 2 km | September 18, 2022, 12:00–14:00 | 17 | 67.8 MB | 40.1 s |
| 1 km | January 31–February 1, 2026, 23:00–01:00 | 17 | 191.9 MB | 88.0 s |

These are bounded functional runs alongside the draining acquisition job, not
controlled comparisons between resolutions. Times exclude index construction
and the size-estimation sample. The 3 km example includes an explicitly absent
base-reflectivity variable; the others request composite reflectivity only.
The supplemental base-reflectivity fetch started for the missing 58 months,
September 2021–June 2026; see [current progress](status.md).
Absent inputs remain flagged and prevent affected model windows being eligible.
After new raw data arrive, run **Index** again to create a new preparation
selection. Existing snapshots retain their original inputs and missingness.
Virtual/native comparisons covered packed measurements, quality, coordinates
and metadata for all eight GOES bands and composite radar. Four day/night
C02/C13 parallax examples completed with 5.7 MB of cloud-height transfer. This
demonstrates execution, not a validated regional correction for every channel.

The storage suite ran with local acquisition paused, three repeats and warm
filesystem caches; it did not flush those caches. Median rates were:

| Operation | DAS | Linux comparison |
|---|---:|---:|
| Sequential reads | 0.30 GB/s | 5.50 GB/s |
| Writes including fsync/close | 0.17 GB/s | 1.62 GB/s |
| Concurrent read + write, combined bytes | 0.34 GB/s | 2.88 GB/s |

The Linux reads include cache effects and are not an SSD hardware specification.
The measured DAS path falls below the USB interface ceiling; enclosure, media
and WSL filesystem costs are not separated by this test.

The layout matrix used seventeen 1 km frames around January–February 2026 and
nine variables. These are mostly nighttime observations. It tested disposable
plain Zarr directories, separately from the actual Icechunk model reader. On
DAS the best tested combinations supplied 2.65 six-frame windows/s with Zstd,
six-frame chunks and four readers; twelve-frame chunks supplied 1.83 twelve-frame
windows/s. These combine a chunk change and more readers, so they are not an
isolated estimate of either improvement. Median directory storage was 183 MB
with Zstd versus 679 MB without compression, including padded chunk edges.

Actual `ProcessedSource` reads include validity masks and geographic coordinates.
Keeping its immutable snapshot metadata open gave 0.575 six-frame windows/s and
0.277 twelve-frame windows/s with one reader, about 7% and 6% above fresh opens.
Two readers supplied 0.629 and 0.312 windows/s; four gave little further benefit.
The source therefore caches metadata and static coordinates, while measurement
tensors remain bounded reads. Use it as a context manager or call `close()` when
finished. This modest optimization does not change pixels or snapshot identity.

Keep lossless compression and the current one-frame default. The promising
temporal chunks remain explicit alternatives until tested through the actual
model consumer and across more daylight scenes. GPU consumption and input
waiting were not measured: the isolated model check is blocked by missing
NATTEN, and Earth2Studio 0.19.0 does not contain the reviewed Flash helper from
NVIDIA's main branch. A compatible Flash runtime/package and documented regional
topography are still required. No successful checkpoint inference is claimed.
