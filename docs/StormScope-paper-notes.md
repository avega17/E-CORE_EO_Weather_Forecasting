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
alignment. Our future analysis-ready processing must provide the chosen model's
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

This is an overview, not a regional dataloader or preprocessing implementation.
Parallax correction, the 1 km training grid, causal window construction and
fine-tuning design remain subsequent research work.

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
