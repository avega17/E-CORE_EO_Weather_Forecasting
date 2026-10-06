# CorrDiff and regional Caribbean downscaling

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
