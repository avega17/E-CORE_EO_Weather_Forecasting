# StormScope and the Caribbean study

## First inference checks: what the fetched data enables

Our archives preserve observations; they are not yet analysis-ready model inputs.
The practical first check is a small inference example on the released model's
supported CONUS domain, followed by a separate assessment of Caribbean adaptation.
Successfully opening our Zarr stores does not establish checkpoint compatibility
or forecast skill in Puerto Rico.

The [current NVIDIA example](https://nvidia.github.io/earth2studio/main/examples/04_nowcasting/03_stormscope_goes_example/)
couples two models. The GOES model predicts eight ABI channels from their recent
history. The radar model is conditioned on GOES and predicts composite
reflectivity, base reflectivity and gridded GLM lightning density. Our completed
Polaris radar job supplies the two reflectivity products; **GLM is still a separate
data requirement** for that coupled configuration. GOES-only inference can be
tested independently of the radar/GLM model.

The broad sequence for a future smoke test is:

1. Prepare a separate GPU/model environment and pin Earth2Studio, PhysicsNeMo
   and the checkpoint package. Keep GPU dependencies out of the CPU ingestion
   environment. Inspect available model variants and their actual input/output
   coordinates before choosing a model or estimating GPU memory.
2. Load the package and model, move it to the selected device, and call `eval()`.
   Start with one forecast and one ensemble member; distinguish package download,
   optional compilation, initial inference and repeated-step timings.
3. Inspect `model.input_coords()` for variable order and required history, and
   use the checkpoint's physical units and normalization. The paper's nowcast
   setup uses six preceding ten-minute states, but the selected API/checkpoint
   remains the authority. Missing or poorly aligned observations need an explicit
   eligibility decision rather than silent filling in our raw stores.
4. On the supported domain, build the example's input interpolator from source
   latitude/longitude to the model grid, call `fetch_data`, and verify the tensor
   shape, time/lead-time coordinates and interpolation support. For the coupled
   path, assemble the radar and GLM initial state in the model's variable order.
5. Run a short forecast using the model call/iterator, or the example's coupled
   `call_with_conditioning` path. Inspect valid times, invalid-grid masks and
   output fields before interpreting the maps as forecasts.

For stock GOES access covering the Caribbean, the relevant source setting is:

```python
goes = GOES(satellite="goes16", scan_mode="F")
goes_lat, goes_lon = GOES.grid(satellite="goes16", scan_mode="F")
```

`F` selects full-disk observations. It does **not** relocate the trained CONUS
grid to Puerto Rico. The example's regional Flash option selects a region within
its supported domain; it is not a general geographic extension of the checkpoint.
Stock GOES reads the shared-grid MCMIPF product, while our CMIPF archives retain
separate native band grids. They cannot be stacked through implicit xarray
alignment. Our [derived processing](dataset_processing.md) must provide the chosen model's
grid, channels, coordinates, quality treatment and calibration explicitly.

The installed ingestion environment uses **Earth2Studio 0.18.0**. NVIDIA's changing
`main` example now demonstrates `3km_10min_flash`, `set_domain`, mixed precision
and optional compilation. Do not assume those newer interfaces exist in 0.18.0.
The baseline 3 km/ten-minute and legacy 6 km/hourly variants also have different
requirements. Pin and inspect the future inference installation rather than
copying a `main` example into the ingestion environment unchanged.

Polaris has four 40 GB A100 GPUs per node; their memory is not automatically
combined into one 160 GB device. A full-CONUS model may need a supported regional
mode or explicit parallel execution. Measure GPU memory on the selected model
and history length; neither the training-memory numbers below nor our CPU fetch
job establish inference-memory requirements.

This remains the model/data overview. Notebook 05 implements regional grids,
causal window preparation and a bounded parallax experiment separately from raw
fetching. The experimental Caribbean adapter has completed a bounded two-step
run with an explicitly approximate static field; see [the processing guide](dataset_processing.md).
Fine-tuning and validation of regional forecasting skill remain subsequent work.

## Solar geometry is also needed at inference

The relevant term is **solar zenith angle**, not solar azimuth. Zenith is the
angle between the Sun and the local upward vertical; azimuth is its compass
direction around the horizon. The attached paper names zenith in its model
conditioning. In both installed Earth2Studio 0.18.0 and the
[NVIDIA implementation](https://github.com/NVIDIA/earth2studio/blob/main/earth2studio/models/px/stormscope.py),
`build_condition` calls PhysicsNeMo's `cos_zenith_angle` for the current input
and target forecast times, then supplies those fields to the diffusion model.
The inference forward path calls this function: **it is not training-only**.

No extra NOAA imagery is needed to compute this feature: UTC time, latitude
and longitude determine the geometry. Using the known Sun position at a future
forecast time does not introduce future weather observations. Earth2Studio's
wrapper generates the feature internally; callers should not append duplicate
solar channels. Future training code must reproduce the checkpoint's convention
and treatment of nighttime values rather than silently substitute another
solar-position algorithm or clamp negative cosine values.

### What NREL SPA means

NREL's **Solar Position Algorithm (SPA)** is a published astronomical procedure
for calculating where the Sun appears from a given location and time. It accounts
for the Earth's orbital position, rotation and observer location; apparent-angle
outputs additionally account for atmospheric refraction. It calculates geometry,
not clouds, surface irradiance or radar quality.

[pvlib's solar-position interface](https://pvlib-python.readthedocs.io/en/stable/reference/solarposition.html)
offers multiple methods. Its
[Python SPA implementation](https://pvlib-python.readthedocs.io/en/stable/reference/generated/pvlib.solarposition.spa_python.html)
returns geometric and apparent zenith/elevation, azimuth and equation of time.
It accepts UTC/localized timestamps, coordinates and optional altitude,
pressure and temperature. Zenith and azimuth are different quantities; geometric
and apparent zenith are also not interchangeable near the horizon.

An illustrative **point-location diagnostic**, requiring optional pvlib in a
separate analysis environment, is:

```python
import numpy as np
import pandas as pd
from pvlib.solarposition import get_solarposition

times = pd.DatetimeIndex(["2022-09-18T12:00:00Z"])
sun = get_solarposition(times, latitude=18.2, longitude=-66.4,
                        altitude=0, method="nrel_numpy")
cos_zenith = np.cos(np.deg2rad(sun["zenith"]))
```

pvlib returns angles in degrees, whereas NumPy's cosine takes radians. A point
example is not a model-grid feature generator. SPA can independently check
day/night and coordinate conventions, but matching the released checkpoint
requires validating against PhysicsNeMo's implementation first. We add no pvlib
dependency or solar-field persistence to ingestion in this sprint.

The attached *Stormscope: Learning Accurate Storm-Scale Evolution from
Observations* paper (arXiv:2601.17268, January 2026 version) evaluates CONUS
forecasting at 6 km with ten-minute observation updates. Its observation-only
nowcasting and optionally large-scale-conditioned nearcasting should not be
confused with StormCast's high-resolution NWP-conditioned approach. HRRR is a
reference grid and comparison forecast; using the grid does not mean that every
StormScope model requires HRRR forecast fields as inputs.

The [released model card](https://huggingface.co/nvidia/stormscope-goes-mrms)
separately lists 3 km/ten-minute and 6 km/hourly checkpoints. Its GOES variables
are the eight bands selected here; composite reflectivity is its listed radar
variable, with optional large-scale conditioning. CONUS training/evaluation
does not establish Caribbean forecast skill. The current
[Earth2Studio example](https://nvidia.github.io/earth2studio/main/examples/04_nowcasting/03_stormscope_goes_example/)
also uses base reflectivity and GLM in parts of its workflow. Select an exact
checkpoint and software revision before defining model input requirements;
these additional fields are not in our default MRMS study collection.

The existing checkpoints are not plug-in 1 km Caribbean models. However,
changing resolution is not mathematically forbidden by fine-tuning: it requires
checking architecture, spatial coordinates, normalization, grid support and
transfer behavior. Training a regional model from scratch and adapting a
checkpoint are research alternatives, not a demonstrated either/or limitation.
Use **km grid spacing**, not km², when describing resolution.

Native CMIPF preserves C02 at nominal 0.5 km, C01/C03 at 1 km and infrared
channels at 2 km. Regridding infrared measurements to 1 km does not increase
their measured information. Keep separate native bands during ingestion.

See the [measured ROI study](goes_roi_training_study.md) before enlarging the
acquisition. The present region already spans approximately 810 × 850 km;
its smaller infrared array is not evidence of inadequate training support.
The paper uses six past ten-minute observations and a 4×4 token embedding,
but specifies no regional minimum crop or separate masked/contrastive
self-supervised pretraining stage. Regional predictive pretraining would be
our adaptation and needs a controlled model pilot.

Future preparation, outside this sprint:

1. Decode and inspect native observations, with product-specific masks and
   calibration restored for each scan.
2. Correct cloud-top parallax using validated geometry, heights and quality.
3. Map onto an explicit 1 km training grid and mark unsupported boundaries.
4. Construct aligned, causal temporal windows; report eligibility and coverage.

The [NOAA cloud-height algorithm](https://www.star.nesdis.noaa.gov/goesr/documents/ATBDs/Enterprise/ATBD_Enterprise_Cloud_Height_v3.4_2020-09.pdf)
describes height-dependent geolocation corrections. CMI does not supply the
height field. Next sprint must identify cloud-top height products or validated
estimates, their quality flags, view geometry and temporal matching. Do not use
one uniform displacement or treat brightness temperature as height without an
appropriate retrieval. Preserve the broad acquisition ROI for corrections and
context. A narrower training footprint needs a documented margin and masks at
boundaries that lack source support. The paper itself identifies parallax and
viewing geometry as limitations worth addressing.

# Compute comparison

The often cited 120 hours on 64 H100 GPUs is a [StormCast training example](https://docs.nvidia.com/physicsnemo/25.08/physicsnemo/examples/weather/stormcast/README.html),
not an estimate for StormScope or our regional experiment. It equals 7,680
H100 GPU-hours before considering utilization. [Polaris](https://docs.alcf.anl.gov/polaris/)
has four A100 GPUs per node: 64 GPUs would be 16 nodes, not eight, and H100 wall
times cannot be assumed on A100 hardware. Node-hours, GPU-hours and elapsed
hours differ. Our authenticated sbank report confirms that the shared allocation
is measured in **Polaris node-hours**, not summed CPU-hours or GPU-hours.
Budget a measured regional pilot before extrapolating training cost.

The attached StormScope paper separately reports approximately 48 elapsed
hours on 32 H100 GPUs per main model: 1,536 H100 GPU-hours, and approximately
70 GB GPU training memory. Its 3 km CONUS scaling experiment requires spatial
parallelism when activations exceed an approximately 80 GB GPU at batch one.
These experiment-specific numbers do not estimate a Caribbean 1 km run.


## CorrDiff and regional downscaling


The attached *Residual Corrective Diffusion Modeling for Km-scale Atmospheric
Downscaling* paper describes two stages: a regression predicts the conditional
mean and a diffusion model predicts residual detail. Its Taiwan experiment
pairs approximately 25 km ERA5 inputs with 2 km radar-assimilating WRF targets.
The targets include 2 m temperature, 10 m winds and derived reflectivity on a
448 × 448 regional grid. Training uses 2018–2020 and testing uses 2021.
These are paired modeled atmospheric fields; MRMS alone does not provide that
complete target state. Paper preprocessing is a description of that experiment,
not authorization to delete or clean our raw archives.

The [NVIDIA UAE example](https://developer.nvidia.com/blog/nvidia-earth-2-powers-regional-ai-weather-forecasting-in-the-united-arab-emirates/)
uses regional WRF target simulations at 2 km and nested 200 m spacing, with
coarser forcing. It supports considering a regional downscaler but does not
demonstrate a Caribbean checkpoint or guarantee small-island skill.

Two candidate research routes remain:

- Adapt or train an observation-driven regional forecast model using native
  GOES and suitable radar targets on a defined finer grid.
- Adapt a forecast model, then train a separate downscaler on paired coarse
  conditioning and valid fine targets.

The second route needs aligned targets, masks, domain support and storm/time
splits. MRMS can supervise precipitation or reflectivity where valid radar
coverage exists; it cannot supply absent offshore observations or all weather
variables. Coarsened observations used as training inputs may have a different
error distribution from real forecasts; validate with hindcasts. Correct cloud
parallax and specify geolocation before constructing paired samples.

The [PhysicsNeMo regional diffusion examples](https://github.com/NVIDIA/physicsnemo/tree/main/examples/weather/regional_weather_diffusion)
provide training patterns, not a ready data contract for this project. Select
variables, units, accumulation intervals, geography and forecast horizon before
estimating compute. Training and inference are outside the current fetching
and diagnostic sprint. See [StormScope notes](StormScope-paper-notes.md) for
native-band resolution, future 1 km preparation and compute accounting.

## Plain-Zarr handoff and Flash test environment

Preprocessing now writes data.zarr, with `pixel_valid(time, variable, y, x)` and
`valid(time)` for the stock training loader. The reference loader has been checked
on real regional data, but that does not validate a forecasting checkpoint.
Use physical values for Earth2Studio; its checkpoint normalizers remain authoritative.
Regional stats.json is for future training, not a replacement inference normalizer.

`environment-model.yml` separates GPU packages from active ingestion. Earth2Studio
0.20.0 includes the reviewed Flash helper; the previous local 0.19 installation
did not. The Colab L4 environment uses Torch 2.13.0/CUDA 13.0 and
NATTEN 0.21.7+torch2130cu130, following the
[NATTEN wheel matrix](https://natten.org/install/). Target-driver compatibility
and an actual compiled operation must be checked. An import is not a kernel test.

Earth2Studio's interpolator also needs `earth2grid`, which is not available on
PyPI in this environment. Its [official source installation](https://github.com/NVlabs/earth2grid#install)
compiles against the installed Torch and CUDA toolchain. The first Colab attempt
used CUDA 12.6 wheels with a CUDA 13.0 system compiler and did not build. The
matching CUDA 13 environment built Earth2Grid 2025.11.1+torch213 successfully.
An earlier finite NATTEN result and successful weight loading alone did not
establish that a forecast ran; consult model-reference.json for that result.

The matching-CUDA retry completed two Flash steps (10 and 20 minutes) for a
204 × 204 supported-CONUS region. Their combined forward time was 0.99 s;
model loading was 23.45 s and public GOES preparation 53.54 s. Peak Torch-allocated
GPU memory was 3.19 GiB on L4. Compilation was disabled and one realization was
used. These are one engineering check's timings, not a forecasting-skill score
or a repeated throughput benchmark. The stock source uses its own scan-matching
rules; this check does not validate the project's causal regional dataloader.
The Colab runtime was stopped after the result was retrieved.

For a Caribbean engineering test, the available six-frame January 2021 input
has sufficient joint pixel support. The remaining scientific uncertainty is the
static topography channel: the registry enables it and the wrapper reads
topo.npy without additional scaling, but the original normalization is not
documented. It is not valid to replace that field with raw metres or infer a
scale solely from its numerical range.

A paired check against NOAA elevation data at matching CONUS locations gives
an approximate conversion. A mountainous calibration patch and an independent
coastal/ocean patch show an error of about 0.004 in checkpoint units. This is
empirical evidence, not NVIDIA's documented training recipe. Applying it to the
Caribbean also places about 11.6% of this crop's elevation pixels outside the
checkpoint field's 0–1 range. Neither clipping those values nor substituting
CONUS terrain would establish compatibility. Any run using this approximation
must be explicitly enabled and labelled as an execution experiment. The
[NOAA ETOPO metadata](https://oceanwatch.pifsc.noaa.gov/erddap/info/ETOPO_2022_v1_60s/index.html)
defines elevation in metres relative to EGM2008 and includes bathymetry; ocean
treatment must therefore be recorded as part of the model input convention.

### Caribbean execution and a single-case comparison

With explicit approval for that approximation and bounded transfer, the GOES-only
Flash adapter ran on six committed frames from 15:10–16:00 UTC on January 15,
2021. It used a 256 × 256 three-kilometre crop, actual Caribbean coordinates,
unchanged released weights, seed 0 and no parallax correction. Both +10/+20-minute
outputs were finite on the supported 99.98% of pixels. Forward passes plus output
read-back took 1.35 s; model loading took 24.65 s separately. Peak allocated L4
GPU memory was 2.61 GiB. The runtime was released after retrieval.

Evaluation used later observations on the workstation, never as model input.
The same 65,522 valid pixels were compared against persistence (repeat the last
input). All eight bands had lower MAE than persistence at both leads in this case:

| Band and units | +10 min MAE | Persistence | +20 min MAE | Persistence |
|---|---:|---:|---:|---:|
| C02, reflectance factor | 0.0257 | 0.0313 | 0.0343 | 0.0400 |
| C13, kelvin | 0.942 | 2.009 | 1.450 | 2.692 |

This demonstrates execution with the prepared regional arrays and a useful
single-case diagnostic. It does not validate Caribbean forecast skill, rainfall
prediction, the empirical topography convention, or performance on a held-out
regional sample set. Proper static-field provenance, parallax-aware inputs and
broader regional evaluation remain necessary research work. The compact report
is model-check.json; caribbean-comparison.csv/json and the corresponding figure
record the observational comparison.

After creating the model environment, check the compiler and build the additional
dependency before attempting inference:

```bash
conda activate ecore-model
python -c "import torch; print(torch.__version__, torch.version.cuda)"
nvcc --version
export MAX_JOBS=2
export TORCH_CUDA_ARCH_LIST="$(python -c 'import torch; print(".".join(map(str,torch.cuda.get_device_capability())))')"
python -m pip install --no-deps --no-build-isolation \
  https://github.com/NVlabs/earth2grid/archive/main.tar.gz
python -c "from earth2studio.utils.interp import NearestNeighborInterpolator; print('Interpolator available')"
```

The selected architecture is 8.9 for L4; other GPUs require their own capability.
The compiler's CUDA major version must match the Torch build. This recipe is
tested on Colab, not a validated Polaris GPU environment. Perform heavy setup
inside an allocation rather than on a Polaris login node. Keep verbose build
output for failures; an ordinary pip summary can hide the compiler error.

While the workstation GPU is absent, transfer only a completed bounded 3 km
example to Colab or a short Polaris allocation. Install the separate environment,
verify CUDA and NATTEN, then load the compatible GOES Flash package. Test a supported
CONUS region before the explicit Caribbean adapter. The latter starts at 256×256,
falls back to 200×200 only for GPU memory, and runs two forecast steps with one
ensemble member. Document elevation units/scaling/ocean treatment; do not infer
checkpoint topography conventions from its numerical range. Missing GPU, weights,
NATTEN or static fields is a blocked result, not successful inference.

On WSL, the Colab CLI's OAuth keyring may lack a Linux Secret Service backend.
Use its explicit ADC mode after local interactive authorization:

```bash
gcloud auth application-default login --no-launch-browser \
  --scopes="openid,https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/userinfo.email,https://www.googleapis.com/auth/colaboratory"
colab --auth=adc sessions
```

ADC persists credentials outside this repository. Do not copy them into notebooks
or share the token file. A separate `uv tool install keyring` does not add a backend
to the Colab tool's isolated environment. Do not silently use plaintext alternate
keyrings to resolve the warning. Release any allocated Colab session after the test.

The supplied HTML implementation report is advisory. Its 4.2× comparison mixes
layouts/readers, C10 is water vapor, ACHAF grids require historical inspection,
and expert fine-tuning is not automatically equivalent to Flash distillation.
This sprint prepares input compatibility; it does not train those experts.
