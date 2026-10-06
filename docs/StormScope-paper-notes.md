# StormScope and the Caribbean study

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
hours differ. The unit of the shared 5,000-hour allocation is not confirmed, so
no percentage of that allocation is claimed. Budget a measured regional pilot
before extrapolating training cost.

The attached StormScope paper separately reports approximately 48 elapsed
hours on 32 H100 GPUs per main model: 1,536 H100 GPU-hours, and approximately
70 GB GPU training memory. Its 3 km CONUS scaling experiment requires spatial
parallelism when activations exceed an approximately 80 GB GPU at batch one.
These experiment-specific numbers do not estimate a Caribbean 1 km run.
