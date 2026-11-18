# Project status — November 18, 2026

This is a repository-maintenance status, not a live Polaris monitoring result.
Use the [operational commands](study_jobs.md) to establish current job state.
The October job IDs and progress snapshots are historical observations.

## Refactor

Code is organized into eight responsibility-based subpackages. Native
archive paths and formats are unchanged. The misplaced notebook report database
was moved to results/dataset-report with 596 audits, 1,784 diagnostic rows and
80 inventory rows preserved. The database remains the resumable report record. The CPU suite passed 414 cases
(14 optional/environment skips), the processing environment passed all 27 cases,
and all five notebooks executed in local kernels without starting a fetch.
398 completed task receipts were consolidated into portable study journals;
results file count fell from roughly 1,200 to about 400. Unresolved recovery bundles
and scientific selections remain protected.

## Research and operational limits

Native GOES and MRMS archives, missing-source evidence and partial checkpoints
remain authoritative. Full scan counts are not final archive completion.
Polaris uses an allocation-specific runtime source copy to prevent worker/coordinator
protocol differences. Existing benchmark conclusions do not establish performance
for new hardware or model skill. Regional processing and model experiments remain
separate from native acquisition; see the processing and model guides.

## Plain-Zarr processing rollout

The bounded 3 km plain-Zarr replacement verified on real data with ten variables.
Four retired Icechunk selections, their preparation state and an unused virtual
catalog were deleted (653,638,508 logical bytes). Corrected 1/2/3 km examples cover both satellites;
C08/C09/C10 remain unshifted. The official PhysicsNeMo loader produced an
eight-channel target, 48-channel history and spatial mask from the real example.
Notebook 05 executed without fetching. The full CPU suite passed 434 tests, with
four optional checks skipped. A separate Colab L4 ran the compiled NATTEN CUDA
operation; the model forecast result is recorded separately.
The supported-CONUS Flash check completed on Colab L4 after matching the
Torch/NATTEN CUDA 13 wheels to the system compiler and building Earth2Grid.
Two forecast steps on a 204 × 204 grid took 0.99 s, with 3.19 GiB peak allocated
GPU memory. Public-source preparation took 53.54 s and model loading 23.45 s,
reported separately. This verifies execution, not Caribbean forecast skill.
The GPU runtime was released.

The approved Caribbean engineering test also completed using six committed
GOES frames from January 15, 2021, a 256 × 256 crop and empirical ETOPO scaling.
Both ten-minute forecast steps were finite on all supported pixels (99.98% of
the crop). Combined forward/read-back time was 1.35 s; peak allocated GPU memory
was 2.61 GiB on L4. Weights were unchanged; coordinates and static fields were
regional. In this one example, all eight bands had lower MAE than persistence
at both leads. At twenty minutes C13 MAE was 1.45 K versus 2.69 K for persistence.
Later observations stayed local and were used only for evaluation.
Topography remains approximate, 11.6% of this crop's encoded elevations lie
outside the checkpoint field's range, and imagery is uncorrected for parallax.
This is successful execution and a single-case diagnostic, not regional skill
validation. The GPU was released; compact results and the comparison figure
are under results/dataset-processing and figures/dataset-processing.

The full-study index contains 2,882,510 observations in 670 native archives and
286,352 eligible six-state histories out of 289,008 nominal study slots. This is
input coverage, not completed full-study transformed data. Consult the live
processing checkpoint before claiming the baseline complete. The uncorrected
3 km baseline is running from an isolated runtime copy; its run record is
`results/dataset-processing/materialization-run.json`.
Its revised profile uses eight independent transform processes, four batches
ahead and a 64 GiB working-memory allowance. Three rotated trials on the same
eight-hour selection took median 26.71 s versus 38.67 s for two threads;
16/32 processes took 28.02/28.28 s. Arrays matched exactly. This is bounded
workstation evidence, not a completed full-study runtime.
