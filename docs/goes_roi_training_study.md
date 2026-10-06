# GOES region size for regional StormScope research

This investigation compares the current acquisition region with larger native
GOES crops. It uses the attached StormScope paper, existing archives, and paired
source measurements. It does not change the live fetch region, train a model,
or implement interpolation or cloud-top parallax correction.

## What we have already acquired

The acquisition box is **70.24–62.56° W, 14.36–22.04° N**. Its geographic area
is about **690,000 km²**, roughly **810 × 850 km**. January 2026 CMIPF archives
confirm these native shapes:

| Channels | Nominal spacing at nadir | Native array, height × width |
| --- | --- | --- |
| C02 | 0.5 km | 1,572 × 1,664 |
| C01, C03 | 1 km | 786 × 832 |
| C07, C08, C09, C10, C13 | 2 km | 393 × 416 |

Those are source grids, not interchangeable training tensors. Off-nadir
footprints vary, and a native projected crop encloses the geographic box rather
than exactly following all four edges. The viewer renders **768 × 768** single
images and **384 × 384** animations; display size is not archived array size.
The infrared crop is numerically smaller than a CONUS model array, but this
alone does not show that our acquisition region is inadequate.

## What the paper actually requires

The attached [StormScope paper](../Stormscope_MRMS_GOES_Learning_Accurate_Storm-Scale_Evolution_from_Observations.pdf)
is arXiv:2601.17268v1. Sections 4.1–4.2 and Appendix 6.2 establish:

- Eight GOES channels matching our selection; GOES training years 2018–2023,
  and MRMS years 2020–2023. The paper's held-out evaluation year is 2024.
- Observations remapped to the HRRR Lambert grid, then downsampled to 6 km
  for the main experiment. The main model domain is **512 × 896**, covering
  roughly 3,072 × 5,376 km. That is a CONUS experiment, not a regional minimum.
- Nowcasting conditions a denoising diffusion model on **six past states**
  ten minutes apart to predict the next observed state. Nearcasting uses one
  hourly previous state and additional large-scale guidance. Satellite and
  radar models are trained independently; radar forecasting uses satellite
  conditioning, enabling a satellite model without radar everywhere.
- **4 × 4** input patch embedding produces **128 × 224 tokens**. Attention
  uses **31 × 31-token neighborhoods** through 16 transformer blocks. A patch
  here is a tokenization unit, not a prescribed image-training crop.
- The 3 km scaling experiment spans **1,024 × 1,792** cells. At that size,
  activations exceed an approximately 80 GB GPU even at local batch size one;
  spatial parallelism distributes the model across GPUs. The appendix warns
  that independently training/stitching small image patches can reduce
  coherence and available context.

The paper describes conditional forecasting trained on observation targets.
It does **not** specify a separate masked-image or contrastive self-supervised
pretraining stage, a regional minimum crop size, or a validated 1 km Caribbean
configuration. We can propose observation-based regional pretraining without
manual labels, but should identify that as our adaptation of its forecasting
objective rather than claim a demonstrated pretraining recipe.

The [released model card](https://huggingface.co/nvidia/stormscope-goes-mrms)
and [current Earth2Studio example](https://nvidia.github.io/earth2studio/main/examples/04_nowcasting/03_stormscope_goes_example/)
also differ in some model variables. The example includes base reflectivity
and GLM for its radar model. An exact checkpoint/configuration must be reviewed
before transfer experiments. A larger crop does not make our dataset compatible
with a released CONUS checkpoint.

## Paired acquisition experiment

We compare five boxes on the same **32 CMIPF source objects**: all eight bands
at midnight/noon on September 18, 2022 (GOES-16), and September 18, 2025
(GOES-19). The source selection is recorded in
`results/goes-roi-study/selection.json`.

| Option | West, south, east, north | Purpose |
| --- | --- | --- |
| Current | -70.24, 14.36, -62.56, 22.04 | Existing acquisition |
| 25% wider/taller | -71.20, 13.40, -61.60, 23.00 | Moderate extra context |
| 50% wider/taller | -72.16, 12.44, -60.64, 23.96 | Larger regional context |
| Twice as wide/tall | -74.08, 10.52, -58.72, 25.88 | Broad satellite-only comparison |
| Extend east to 60° W | -70.24, 14.36, -60.00, 22.04 | Extra Atlantic/eastern context |

Full NOAA objects are downloaded into a temporary Linux cache. The
production range-reader/cache path is replayed against those files for every
box; three local read repetitions use fresh application caches and retained
filesystem caches. Option order rotates between source files. We measure
native pixels, CMI/DQF bytes, uncompressed NetCDF crop bytes, compressed Zarr
chunks, range request/byte counts, and local read/write/read-back times.

Every larger crop must contain the current crop with **exactly equal CMI and
DQF pixels**. Compressed Earth2Studio Zarr crops must reopen with exact pixel
and coordinate equality. Successful sources and temporary stores are removed;
the compact measured findings are retained here; disposable JSON/CSV experiment artifacts were removed in the current-study cleanup. Replayed range bytes describe the
production access pattern, **not measured network throughput**. Background
fetching and warm local caches prevent these times from being isolated
network-performance benchmarks.

The experiment also fixed an estimator error: xarray preserved source HDF5
compression when writing files labelled “uncompressed NetCDF.” The helper now
clears serialization encodings while retaining packed values and calibration
attributes. An HDF5 inspection test verifies compression is absent. Older
NetCDF-size estimates need this qualification; Zarr compression measurements
are a separate accounting category.

### Measurement results

All **160 source/ROI comparisons passed**, including nested packed CMI/DQF
equality and compressed Earth2Studio Zarr pixel/coordinate read-back. The completed experiment was recorded before consolidation; that obsolete result directory has now been removed. No source objects
or successful temporary Zarr stores remain.

These ratios compare sums over exactly the same 32 source objects. They do
not represent a full-month network benchmark or a forecast-skill result.

| Region | Native pixels | Compressed chunk bytes | Replayed range bytes | Local read time |
| --- | ---: | ---: | ---: | ---: |
| Current | 1.00× | 1.00× | 1.00× | 1.00× |
| 25% wider/taller | 1.56× | 1.52× | 1.02× | 1.03× |
| 50% wider/taller | 2.24× | 2.15× | 1.08× | 1.05× |
| Twice as wide/tall | 3.94× | 3.70× | 1.13× | 1.07× |
| Extend east to 60° W | 1.32× | 1.30× | 1.00× | 1.02× |

Local read time sums each object's median of three repetitions. This includes
local range replay and decoding, with warm filesystem caches. Write timings
include import/warm-up effects and are not used to claim a size-related speedup.
The compressed chunks include coordinates; monthly archives pay fixed metadata
and coordinate costs differently from these single-frame test stores.

The measured cost ratios are listed in the table above; the retired plot artifact is not required to reproduce these values.

Increasing the crop retains substantially more pixels than it adds replayed
range bytes in this sample. Existing HDF5/cache reads already retrieve data
beyond our present crop. The eastward extension adds **32% more pixels and 30%
more compressed payload** without extra returned bytes here. This is promising
for additional eastern context, but does not establish the same behavior for
every season, scan or cache-block setting.

A separate live GOES-19 daytime C13 check exactly matched replayed counts:
current and eastward crops each returned **9,145,081 bytes in nine requests**;
the twice-wide/tall crop returned **10,193,657 bytes in ten requests**. Their
nested packed pixels also matched. This validates replay on one infrared
object, not every sample. Its wall times are uncontrolled because the live
MCMIPF drain was active.

The 32 distinct full objects total **1.87 GiB**. Completed sample downloads
returned 2,351,891,175 bytes because correcting the NetCDF measurement required
some repeat reads. An interrupted local-cache download added 20,971,520 bytes;
the live range check added 28,483,819 bytes. Accounted imagery transfer is
**2,401,346,514 bytes**, excluding listing/control responses. The partial file
was a local interruption, not evidence of a corrupt NOAA source. Atomic cache
publication and length checks now prevent its reuse.

### Study-period size sensitivity

The completed inventory lists **2,303,762** selected eight-band CMIPF files
for January 2021 through June 2026. Applying the new satellite/band-specific
sample means to those counts gives the following size sensitivity, in decimal
TB. This does not repeat the full inventory or claim completed downloads.

| Region | Corrected uncompressed cropped NetCDF | Compressed Zarr central sensitivity |
| --- | ---: | ---: |
| Current | 4.18 TB | 0.75 TB |
| 25% wider/taller | 6.47 TB | 1.14 TB |
| 50% wider/taller | 9.24 TB | 1.60 TB |
| Twice as wide/tall | 16.22 TB | 2.76 TB |
| Extend east to 60° W | 5.49 TB | 0.97 TB |

The corrected current raw estimate replaces the interpretation of the older
approximately 1.00 TB figure, which inadvertently preserved NetCDF compression.
The compressed column scales the prior study's 0.746 TB central Zarr estimate
by paired chunk ratios; it is not a newly calibrated seasonal storage forecast.
The prior low/high range was 0.422–1.262 TB even before enlargement. Both
columns depend on sampling: these four scans cover two satellites and day/night
but only September dates. Do not use them as precise capacity guarantees.

Future estimator results label true uncompressed samples
`packed-uncompressed-v1`. Old cached samples without this marker cannot supply
an uncompressed estimate; use a fresh output directory to resample. Existing
study reports and historical caches remain unchanged for audit.

### Reproduce and inspect

Use the repository Conda interpreter. Create a fresh output directory and select
the earliest available scan in each ten-minute window below, for all eight
bands, using `goes.discover(..., product="ABI-L2-CMIPF")`. Save the resulting
`Asset` records as `selection.json` with `dataclasses.asdict`:

| Satellite | Windows, UTC |
| --- | --- |
| GOES-16 | 2022-09-18 00:00–00:10 and 12:00–12:10 |
| GOES-19 | 2025-09-18 00:00–00:10 and 12:00–12:10 |

Then run:

```bash
python scripts/benchmark_goes_roi.py --output results/goes-roi-study --repeats 3 --budget-gib 2
python scripts/summarize_goes_roi.py --output results/goes-roi-study
python -m pytest tests/test_goes_roi.py -q
```

The benchmark resumes recorded source/ROI rows rather than rerunning them;
use a separate output folder for a fresh comparison. The summary currently
expects this exact 32-object/five-region experiment and the existing study
inventory, not arbitrary selections. Evidence includes `rows.json`, `rows.csv`,
`bands.csv`, `downloads.json`, `live-range-verification.json`, and `run_config.json`.
The latter records both code hashes and the baseline Git revision; a dirty
checkout is not represented by the revision alone.

## Consequences for a future 1 km model

Model sizing must use the proposed training grid, not the smallest native band.
A 1 km grid spanning our existing region contains approximately 690,000–750,000
cells depending on projection and margins. That is already **more cells than
the paper's 458,752-cell main model**, despite covering a much smaller physical
domain. Upsampling 2 km infrared measurements adds grid locations, not new
observed detail.

The sizing report uses geodesic centre-line width/height, rounds a hypothetical
1 km grid to multiples of four, and estimates 4 × 4 token counts and FP32
six-frame/eight-band input bytes. These are planning calculations, not an
implemented reprojection, checkpoint input, or GPU-memory prediction. GPU
activations, gradients, optimizer states, noise states and model parameters
are additional costs. The paper's approximately 70 GB/model training footprint
cannot be scaled to our 16 GB GPU solely from input-tensor bytes.

| Region | Illustrative 1 km shape | 4×4 tokens | Six-frame, eight-band FP32 input |
| --- | ---: | ---: | ---: |
| Current | 852 × 816 | 43,452 | 127 MiB |
| 25% wider/taller | 1,064 × 1,016 | 67,564 | 198 MiB |
| 50% wider/taller | 1,276 × 1,220 | 97,295 | 285 MiB |
| Twice as wide/tall | 1,704 × 1,628 | 173,382 | 508 MiB |
| Extend east to 60° W | 852 × 1,084 | 57,723 | 169 MiB |

At the current 512 MiB reader-queue budget, the production sizing proxy reduces
C02 batch sizes from eight scans to seven for the 50% enlargement, and four
for the doubled box. The 25% and eastward alternatives still allow eight, but
fewer batches fit concurrently. These are memory reservations, not measured
process RSS or promised download scaling. Native C02 pixels grow much faster
in memory than the infrared preview suggests.

With the same tokenization, one attention neighborhood spans about **124 km**
at 1 km spacing, versus **744 km** at the paper's 6 km spacing. Layers can
propagate information farther, but changing resolution also changes physical
context per layer. More acquisition pixels alone do not resolve that design
choice. Test context, multiscale conditioning, and model size alongside grid
spacing in the later training sprint.

Larger geographic support creates more spatial tiles and more incoming-weather
context, but not more independent timestamps or storm events. Split data by
time/storm before creating overlapping windows and tiles; avoid treating
neighboring crops of the same storm as independent validation examples. Include
day/night and GOES-16/19 calibration regimes, quality masks, solar illumination
and normalization fitted only on training observations. Missing scans and
unsupported pixels should reduce window eligibility rather than become zeros.

## Context and quality should determine the region

Define a forecast target smaller than the acquisition box, with margins for
incoming weather and future cloud-top correction. Simple displacement examples
for a two-hour forecast are **72, 144 and 216 km** at 10, 20 and 30 m/s. They
are design scenarios, not measured Caribbean steering winds. A 256 km target
with a 216 km margin on each side occupies 688 km before correction margins;
it can fit inside our present acquisition. A 512 km target with 144 km margins
occupies 800 km before correction, leaving little spare width. The latter
would give a concrete reason to test a modest enlargement.

Cloud-top parallax remains required future work: use heights, satellite
geometry, quality and source support to determine the margin; do not assume
one displacement. Decode/inspect native observations, correct validated cloud
geometry, map onto a defined 1 km grid, then construct causal windows.
[Cloud-height algorithm](https://www.star.nesdis.noaa.gov/goesr/documents/ATBDs/Enterprise/ATBD_Enterprise_Cloud_Height_v3.4_2020-09.pdf).

More GOES coverage does not extend TJUA's useful radar supervision. The
[bounded MRMS quality study](mrms_roi_and_radar_coverage.md) illustrates that
high-quality radar support is much smaller than our current geographic box.
GOES-only observation prediction can use oceanic context; radar losses require
their own coverage/quality masks. The twice-as-wide box extends outside the
CARIB file domain, so MRMS cannot cover that entire selection.

Current archives use one full native ROI frame per spatial chunk. Expanding
them increases decompression for small training tiles, even when only a tile
is requested. The [Zarr performance guidance](https://zarr.readthedocs.io/en/v3.1.4/user-guide/performance/)
supports matching chunk size to reads. Compare spatial tiles and six-frame
temporal windows before a broad enlargement; native C02 is the largest array.
The existing bounded reader queue may reduce batch concurrency as frame bytes
grow, even with unchanged process/thread settings.

## Recommendation and next experiment

Keep the current acquisition for the first Puerto Rico regional training pilot;
do not enlarge the full study merely to match the number of pixels in a CONUS
model. First define target extent, temporal context and forecast lead time.
Use the paired measurements above to cost a justified context enlargement.
An eastward extension is especially worth evaluating if incoming systems or
additional eastern islands are part of the target. A 25% wider/taller box is
a reasonable controlled alternative when larger forecast targets need margins.
The twice-as-wide box is a cost/context experiment, not the recommended default.

A later model pilot should hold storms, time splits, channels, cadence,
normalization, losses, model and compute budget constant. Compare current and
enlarged support on the **same interior forecast target**, report skill by
lead time/event and radar eligibility, and measure loader throughput and GPU
memory. That can test the benefit of extra context. This I/O experiment alone
does not establish improved pretraining or forecast skill.
