# Extended Outline for Update Meeting with Prof. Chengyuan

## Agenda shared by email

1. **Review November’s work.**
   - Review nowcasting and AI weather forecasting literature.
   - Review the initial preprocessing implementation and its first full-study milestone.
   - Summarize development since work resumed on November 16 after contract sign-off. This includes the NOAA NAM archive audit, selective forecast acquisition for tropical storm encounters, and preliminary Caribbean StormScope Flash tests.
2. **Discuss the proposed Year 1 model roadmap.**
   - Validate preprocessing, including satellite parallax correction over Puerto Rico, and implement meteorological verification metrics.
   - Test regional fine-tuning of the published 3 km StormScope Flash checkpoint. Plan 2 km pre-training in parallel with the 3 km evaluation.
   - Consider the 16 storms identified through IBTrACS as a held-out evaluation set and plan spring-semester training.
3. **Identify precipitation scenarios** the model should be evaluated on.
4. **Agree on numerical weather prediction baselines.** Use the archived NAM Puerto Rico nest for retrospective comparisons and discuss RRFS and its ensemble, REFS, for future comparisons.
5. **Review GOES Level-2 parallax diagnostics, candidate models and exploratory Year 2 options** (CRTM, AI data assimilation, PG-FMM, CorrDiff and StormCast).
6. **Align the Overleaf manuscript with the project’s data and methods.** Decide how to present the Year 1 observation-based experiments and where to place the exploratory neural-operator work in the Year 2 publication plan.

## 1. November development progress

- **Code organization.** The E-CORE repository now groups related code for source access, ingestion, storage, job controls, processing and reporting. Notebooks and command-line jobs share those functions, so a workflow explored in a notebook can be run on the workstation or Polaris without maintaining separate implementations. Job status and recovery use archive verification records and compact progress summaries. [E-CORE developer guide](docs/developer_guide.md)
- **GOES preprocessing.** The pipeline decodes the eight native GOES bands, retains quality masks, selects scans that ended by each ten-minute slot and maps the bands onto a common Puerto Rico grid. It supports 1, 2 and 3 km spacing. Independent workers transform batches, and one writer validates and saves them in time order so a build can resume from its last verified batch. The full-study 3 km build checkpointed all **289,013 time slots**, including history padding. **246,920 slots** met the build’s eligibility criteria. Final archive verification was still pending at the November 20 check. [Dataset processing guide](docs/dataset_processing.md)
- **Processed data storage.** We save the transformed pixels and their quality masks in compressed, chunked Zarr arrays so a training job can read a short time window directly. Notebook 05 manages one primary full processed store per output location. Changing the selection requires confirmation of which processed store to replace. The native NOAA archives remain available for other analyses and for rebuilding the processed store. [Dataset processing guide](docs/dataset_processing.md)
- **NAM audit and selective acquisition.** The archived Puerto Rico nest covers **91.0% of our acquisition box** and **95.9% of the centered 3 km demo crop**. The principal gap is south of 15° N. The audit found forecasts eligible by its historical cutoff for **41,786 of 48,168 hourly slots (86.8%)**, with continuous hourly coverage from late September 2021 onward. We identified **16 storms and 17 encounters** with the study area. NAM is available for **843 of 1,107 requested event hours**. The gaps affect Elsa, Fred, Grace and Peter in 2021. Selective acquisition is complete for the available event selection. It identified 8,404 source forecasts and retained about **12 GB** of compressed crops for ten fields, including pressure, temperature, dew point, winds, water vapor, rainfall rate and accumulation, and composite reflectivity. [NAM audit](docs/nam_audit.md)
- **Caribbean Flash demo.** Notebook 08 ran the GOES-only StormScope Flash checkpoint on a Colab L4 GPU with both uncorrected and parallax-corrected Hurricane Fiona inputs. The public dataset has both variants for seven storms. The demo reuses the published CONUS weights with adapted Caribbean coordinates and approximate elevation scaling. It uses six observed ten-minute frames ending at the initialization hour, then feeds each prediction back into the model to forecast through +60 minutes. It displays the corresponding GOES observations and available hourly NAM comparison fields. This test demonstrated that the workflow runs on Caribbean data. Forecast skill remains to be evaluated. [Colab notebook](https://colab.research.google.com/drive/1VwPXEDuBPEskF6e1WKfuLQ6zBRicD3Rf?usp=sharing), [public storm dataset](https://huggingface.co/datasets/asvnpr/ecore-stormscope-demo)

**Which Flash variant are we using?** NVIDIA’s 3 km, ten-minute Flash release accelerates the original diffusion model with three expert checkpoints per branch. The GOES branch forecasts eight Advanced Baseline Imager (ABI) channels and uses five network evaluations per ten-minute step. The coupled radar branch uses seven and forecasts MRMS composite and base reflectivity plus a gridded Geostationary Lightning Mapper (GLM) lightning-density field, which it also requires as input history. **Our demo uses the GOES-only branch. The current project pipeline does not ingest GLM.** NVIDIA’s 18-step, 16-member configuration describes its published radar evaluation. The demo uses six steps and one realization. Earth2Studio 0.20.0 is the version tested for the demo. [NVIDIA checkpoint and evaluation documentation](https://huggingface.co/nvidia/stormscope-goes-mrms), [Earth2Studio Flash example](https://nvidia.github.io/earth2studio/main/examples/04_nowcasting/03_stormscope_goes_example/)

## 2. Proposed roadmap for NSF Year 1 objectives

The seed project aims to develop a spatiotemporal forecasting model for the Caribbean using regional radar and satellite imagery.

- **Compile and align the observations and terrain.** The acquisition box spans 70.24–62.56° W and 14.36–22.04° N. The proposal’s illustrative **512 × 512 km training tile** fits within that broader region and would be 512 × 512 cells on the planned 1 km grid. The current Flash demo uses a separate **256 × 256-cell crop at 3 km**, spanning 768 × 768 km.
- **Run self-supervised pre-training on GPUs.** Use historical sequences to predict future satellite fields. The Argonne Polaris account, allocation and storage are in place. Discuss an initial 3 km fine-tuning test in December and broader training in the spring semester. [ALCF Polaris](https://docs.alcf.anl.gov/polaris/)
- **Track the Year 1 milestones.** Native eight-band GOES imagery and two MRMS reflectivity products were archived on the workstation and Eagle for the 66-month study period, January 2021 through June 2026, with source gaps recorded. Final 3 km store verification, auxiliary cloud-height acquisition and preprocessing validation are pending. Preliminary trained model weights are a spring-semester target.

### Preliminary Caribbean Flash outputs and white pixels

*Figure 1. Earlier three-row preview for Hurricane Fiona on September 18, 2022 (13:50–14:10 UTC). Columns show Flash C02 visible reflectance, the corresponding GOES observations and NAM Puerto Rico nest composite reflectivity from the 12:00 UTC cycle, valid at 14:00 UTC.*

*Figure 2. Six-row, one-hour Fiona comparison on September 18, 2022 (16:10–17:00 UTC), using approximate regional elevation conditioning. The first two columns show successive Flash predictions and observed GOES infrared frames. The NAM column shows the forecast valid at initialization in the first five rows for context and advances to the next hourly valid time in the last row.*

The examples show white areas in the observed and predicted imagery, including some visible-band cloud cores and infrared areas. To identify where those pixels enter the workflow:

1. **Check the data against the display.** Compare the same pixels in the packed GOES source, the quality mask, the decoded image and the plot. A color scale can make finite values look alike. A masked or nonfinite pixel can also render white. Diagnose those cases separately before attributing the artifact to saturation or changing the valid range.
2. **Check the model sequence.** Record finite-pixel fractions at input, after conditioning and after each forecast step. Earth2Studio issue #1118 documents intermittent nonfinite outputs in an earlier, non-Flash StormScope configuration. It motivates this check. The cause of the white areas in our Flash demo remains unconfirmed. [Earth2Studio issue #1118](https://github.com/NVIDIA/earth2studio/issues/1118)
3. **Verify the elevation input and test regional transfer.** The StormScope paper lists elevation above sea level as a static input. The model combines it with observation history, coordinates and solar illumination to condition each forecast. In Notebook 08, our adapter samples a regional NOAA ETOPO elevation raster on the forecast grid and applies an empirical scaling to supply the checkpoint’s topography input. Check the grid, ocean treatment and scaling conventions before attributing any drift to terrain or the shift from the continental United States (CONUS) to the Caribbean. [StormScope methods](https://arxiv.org/html/2601.17268), [regional elevation setup](docs/dataset_processing.md)

### Proposed sequence for regional training and storm evaluation

1. **Validate the input fields.** Trace the white pixels to their source or processing step, check the elevation conventions, and quantify where parallax correction improves alignment or reduces usable coverage. Check calibration and sampling around the April 7, 2025 GOES-16 to GOES-19 handoff. If radar enters the evaluation, compare the MRMS products with available TJUA scans and gauges, accounting for their different sampling and physical quantities. [NOAA GOES-East transition](https://ospo.noaa.gov/operations/goes/status.html)
2. **Agree on verification metrics and thresholds.** For fields such as radar reflectivity and precipitation, report the **critical success index (CSI)**, the fraction of hits among all hits, misses and false alarms. Use **probability of detection (POD)** for the fraction of observed events detected, **success ratio (SR)** for the fraction of forecast events observed, and **frequency bias** for the forecast-to-observed event count ratio. Use the **fractions skill score (FSS)** and **probabilistic fractions skill score (pFSS)** to assess spatial agreement over several neighborhood sizes. Choose the event thresholds, scales and lead times before evaluation. The StormScope paper’s 0.5 pFSS reference applies to events occupying a small fraction of a neighborhood. [StormScope verification methods](https://arxiv.org/html/2601.17268)
3. **Add probabilistic scores when ensembles are available.** The **continuous ranked probability score (CRPS)** evaluates a forecast distribution against an observed value. Reliability diagrams check whether forecast probabilities agree with observed frequencies. Optional diagnostics include **structure, amplitude and location (SAL)** for precipitation objects and **two-dimensional power spectra** for spatial detail.
4. **Test 3 km fine-tuning and plan 2 km training in parallel.** Compare the regional fine-tuned model with the released model adaptation and simple baselines on the same held-out observations. While the 3 km experiment is being evaluated, scope 2 km training from scratch and whether data from the wider Caribbean, Gulf and Atlantic would improve the diversity of training cases. Sun et al. found that a global AI model trained without strong tropical cyclones struggled to predict Category 5 storms, with some evidence of transfer from similar storms in other basins. This motivates testing broader regional sampling. [Sun et al., 2025](https://doi.org/10.1073/pnas.2420914122)

**Candidate held-out set.** The NOAA International Best Track Archive for Climate Stewardship (IBTrACS v04r01) provides storm center positions and intensity estimates. The selection uses center tracks intersecting the acquisition box. These intersections identify candidate cases for review, since rain bands can reach the region when the storm center is elsewhere. [NOAA IBTrACS](https://www.ncei.noaa.gov/products/international-best-track-archive)

- **2021:** Elsa, Fred, Grace and Peter.
- **2022:** Earl, Fiona, Ian and Nicole.
- **2023:** Franklin, Philippe and Tammy.
- **2024:** Beryl, Ernesto and Oscar.
- **2025:** Erin and Jerry.

If approved, exclude all selected event windows and their agreed buffers from project training, fine-tuning and model selection. Keep any observation or prediction window that crosses a split boundary out of training. This defines a clean split for our experiments. Prior exposure of the published checkpoint to individual storms would need a separate check. Erin and Jerry follow the GOES-East handoff and can help test performance on GOES-19.

*Figure 3. Sixteen IBTrACS storm tracks intersect the Puerto Rico acquisition box across 2021–2025. The cross marks the Puerto Rico reference point. Review where the rainfall and observations overlap before locking the evaluation set.* [Storm-track map](figures/dataset-processing/nam-events/nam-storm-tracks.png)

## 3. Proposed precipitation scenarios for evaluation

- **Tropical cyclone rain bands and intense convective cores.** Measure where and when heavy precipitation develops, including rain arriving ahead of the storm center.
- **Shallow warm rain and terrain-enhanced showers.** Include events over the island that may have weak lightning or limited cold-cloud signatures. Use gauge and radar coverage to determine where scores are interpretable.
- **Developing or dissipating storms and low-rain periods.** Include non-cyclone cases and periods with little or no rain to test false alarms and identify where the model improves on persistence and optical-flow extrapolation.

Agree on event definitions, precipitation thresholds and observation coverage before drawing cases. Keep the seven-storm demo separate from the full evaluation design.

## 4. NOAA NWP baselines over Puerto Rico (5–10 min)

Use the NAM Puerto Rico nest where the historical archive supports the comparison. NOAA retired NAM and introduced the operational Rapid Refresh Forecast System (RRFS) and RRFS Ensemble Forecast System (REFS) on **November 3, 2026**. Their Puerto Rico products are candidates for future evaluation outside our existing study period. The 2.5 km figure refers to the published Puerto Rico output grids. [NWS NAM retirement notice](https://www.weather.gov/media/notification/pdf_2026/SCN26-47_Retirement_of_NAM_SREF_HREF_HiresW_NAM_MOS_aad.pdf), [NWS RRFS and REFS implementation notice](https://marine.weather.gov/product.php?format=TXT&glossary=1&issuedby=WSH&product=PNS&site=PPG&version=4)

| Baseline | Role in the study | Coverage and limitations |
| --- | --- | --- |
| **NAM Puerto Rico nest** | Retrospective hourly NWP comparison for eligible storm events and other selected hours. | Archive coverage starts in late September 2021 and misses part of the acquisition box. Match the valid time and precipitation accumulation interval, and select a cycle using the archive availability cutoff. |
| **HRRR** | Provides the CONUS grid used by the published StormScope checkpoint. | Operational HRRR coverage is outside Puerto Rico, so it is unsuitable as a local forecast baseline. |
| **RRFS and REFS** | Prospective deterministic and ensemble NWP comparisons for Puerto Rico. | Confirm the available variables, archive, issuance times and local grid coverage before adding cases. |
| **pySTEPS optical flow** | Historical motion-based comparison using recent observations. Also test persistence. | Short-term extrapolation primarily follows the motion inferred from observed fields. New convection and rapid decay are useful stress tests. |

**Verification observations.** Compare ABI forecasts with observed GOES in the same bands. Compare reflectivity forecasts with quality-screened radar on their common support. For surface precipitation, agree on gauge data and a suitable radar precipitation estimate, matching time intervals and flagging areas with weak radar coverage. NAM and RRFS supply forecast comparisons. The current GOES-only Flash model predicts imagery, so the demo’s NAM reflectivity panels give weather context. A quantitative comparison requires a common predicted quantity.

## 5. GOES Level-2 products, parallax diagnostics and radar constraints (10 min)

### Why the shift matters over Puerto Rico

GOES-East sits about 35,786 km above the equator near 75.2° W. At our reference location (18.2° N, 66.4° W), a spherical approximation gives a viewing zenith angle of about **23.6°**. A cloud at height h can appear displaced by approximately **h × tan(23.6°)**. For tropical cloud tops at 10–17 km, this is about **4.4–7.4 km**, or **1.5–2.5 cells at 3 km** and **2.2–3.7 cells at 2 km**. That is enough to shift storm features relative to the radar or coastline. The published StormScope experiments omit parallax correction. Our paired tests will measure what correcting it changes. [StormScope data methods](https://arxiv.org/html/2601.17268), [GOES-R parallax reference](https://www.goes-r.gov/resources/Scipubs/2023.html)

The original NVIDIA paper identified this as follow-up work. “We also hope to investigate parallax error effects at steep viewing angles and possible mitigation techniques in future work.” [Original StormScope paper](https://research.nvidia.com/publication/2026-01_learning-accurate-storm-scale-evolution-observations)

### What the pipeline does

- **Height retrieval.** The full-disk ABI Cloud Top Height product (ACHAF) estimates the height of cloud tops and includes quality flags. The archived samples we inspected have **nominal 10 km spacing at nadir**. We preserve that source grid and fetch heights with a 50 km margin around the acquisition box. As of the November 20 check, completed height ZIPs, including selected storm windows, occupied **3.38 GB**. Recent full-month crops typically take about **130 MB**, suggesting **roughly 9 GB** over the study period if coverage and compression are similar. This is retained height storage, with network transfers and temporary work space accounted for separately. [NOAA cloud-top height guide](https://vlab.noaa.gov/web/towr-s/goes-cloud-top-height), [project processing guide](docs/dataset_processing.md#cloud-heights-and-parallax)
- **Correction.** For C01, C02, C03, C07 and C13, match the newest height scan from the same satellite ending at or before the imagery scan end, within 20 minutes. Use valid heights to estimate where clouds sit above the surface, then resample onto the 1, 2 or 3 km target grid. The three water-vapor bands (C08–C10) remain unshifted in the current recipe. Keep a mask for pixels without usable height support. The 10 km height measurements set the detail available for the correction even on a finer output grid.
- **Validation.** Compare each corrected image with the same scan on the same target grid, scoring their common valid area and reporting coverage lost during correction. Check radial shifts, coastal and cloud-edge alignment, and radar agreement where reliable radar is available. White pixels already present in the uncorrected image and holes introduced by the remapping need separate diagnoses. Test whether the northwest holes in the Fiona example come from absent height retrievals, quality screening or source-window boundaries. Avoid treating an unknown height as clear sky.

*Figure 4. Geostationary parallax geometry (Bernal Ayala, Gerth and Nelson, 2023, adapted from Chad Gravelle). H is satellite altitude, θ is the viewing zenith angle and h is cloud-top height. The diagram shows the cloud’s true surface location R, apparent location D and their horizontal separation.*

*Figure 5. Fiona C01 reflectance around 16:00 UTC on September 18, 2022. The panels show uncorrected reflectance, height-corrected reflectance and corrected minus uncorrected reflectance. Adjacent positive and negative differences trace displaced cloud edges. White areas need to be checked against source and correction masks.* [Paired reflectance maps](figures/dataset-processing/parallax/goes16-C01-2022-09-18T1600Z-3km-comparison.png)

*Figure 6. Estimated C01 parallax displacement for the same Fiona scan. Shifts reach roughly 7–7.5 km over the central cloud shield. The white northwest area shows pixels without a usable displacement estimate.* [Displacement map](figures/dataset-processing/parallax/goes16-C01-2022-09-18T1600Z-3km-displacement.png)

### Implications for Level-2 rain and lightning products

- **GOES Rain Rate (RRQPE).** Infrared-based estimates can struggle with shallow warm rain. Establish local radar and gauge agreement before using RRQPE to verify surface rainfall.
- **GOES GLM lightning.** Lightning can add context on deep, electrified convection. Shallow rain can occur without lightning. GLM acquisition would be needed to test the coupled Flash radar variant.
- **Radar products.** Agree on which MRMS reflectivity and precipitation fields to use for training or evaluation and where to require coverage screening or independent gauge checks.

## 6. Model literature and exploratory physics-based paths (10 min)

### Candidate model landscape

| Inputs or approach | Models to discuss | Relevance to Puerto Rico |
| --- | --- | --- |
| **Satellite and radar** | StormScope and StormScope Flash | In the published CONUS study, StormScope’s pFSS for 55 dBZ echoes stayed above the study’s useful-skill threshold for about an hour. The 3 km GOES-only branch is the starting point for our regional test. The coupled experiment adds radar and GLM. Some other StormScope variants support large-scale conditioning such as 500 hPa geopotential. [StormScope paper](https://arxiv.org/html/2601.17268), [NVIDIA model card](https://huggingface.co/nvidia/stormscope-goes-mrms) |
| **Radar extrapolation and learned nowcasting** | pySTEPS, NowcastNet, DiffCast, PG-FMM | pySTEPS is a practical baseline. The learned models offer alternative ways to represent storm motion and growth. For cross-paper scores, check which quantity each model predicts, how thresholds were matched and how missing radar coverage was handled. |
| **Satellite only** | StormScopeGOES, RainCast | Useful for investigating offshore and weak-radar areas. Differences in sensors, training regions and forecast targets matter when considering transfer. |
| **3D regional state and downscaling** | StormCast, CorrDiff, and global guidance from FCN3, GraphCast or AIFS | Possible Year 2 directions. A regional downscaling experiment would require paired coarse inputs and high-resolution targets over the Caribbean. Global fields could supply large-scale context. [CorrDiff and regional downscaling notes](docs/StormScope-paper-notes.md#corrdiff-and-regional-downscaling) |
| **Further literature review** | FengLei, MetNet-2/3, and Swin-based models | Review the data and evaluation designs before deciding whether to include them. |

### Year 2 options and Overleaf alignment

**Path A. Radiative transfer for verification or training.** The Community Radiative Transfer Model (CRTM) simulates what a satellite would observe for a specified atmospheric state and viewing geometry. The direction of the calculation is:

**Atmospheric profiles (temperature, moisture and hydrometeors) + surface properties + viewing geometry → CRTM → simulated ABI radiances or brightness temperatures**

- **Observation-space verification.** Where NAM, RRFS or a future regional model supplies the required 3D and surface fields, simulate the relevant ABI channels and compare them with GOES at matched times, locations and viewing conditions. This would allow an observation-space comparison with StormScope for compatible fields.
- **Exploratory radiative consistency loss.** A differentiable CRTM surrogate could help train a future model that predicts a physical state by checking whether that state produces satellite signals consistent with observations. A published probabilistic CRTM emulator offers a starting point for reviewing the required inputs. Assess its accuracy for our channels and clouds before using it in training. [JCSDA Community Radiative Transfer Model](https://www.jcsda.org/jcsda-project-community-radiative-transfer-model), [Probabilistic Emulation of the CRTM](https://doi.org/10.1175/AIES-D-24-0109.1)

**Path B. Data assimilation.** Explore whether methods such as HealDA, score-based data assimilation or JCSDA’s JEDI framework could combine observations and a numerical forecast with a regional three-dimensional state. This would depend on the background forecast, observation operators and validation data. It is a separate research question from training a model to predict GOES imagery. [NVIDIA HealDA documentation](https://docs.nvidia.com/physicsnemo/26.08/physicsnemo/examples/weather/healda/README.html), [JCSDA JEDI](https://www.jcsda.org/jedi)

**Manuscript alignment.** Center the Year 1 manuscript on the regional GOES and radar dataset, its preprocessing, and a StormScope benchmark with observation-based verification. Keep NWP and motion-based forecasts as baselines. For the existing neural-operator section, agree on the physical state, equations or constraints, and paired data required for a credible Year 2 experiment. Candidate directions include NWP conditioning with pressure, moisture or 500 hPa geopotential height, PG-FMM motion conditioning, StormCast or CorrDiff, and physics-state verification with CRTM.

### Decisions for the next semester

1. **Year 1 sequence and splits.** Agree on the preprocessing checks, the held-out storms and buffers, and the criteria for starting 3 km fine-tuning. Confirm which 2 km planning steps can proceed in parallel.
2. **Baseline and observation suite.** Agree on persistence and pySTEPS, historical NAM, and prospective RRFS/REFS. Specify common variables and valid times, the gauge and radar references, and the minimum coverage for each metric.
3. **Manuscript scope.** Agree on which current methods and results belong in the Year 1 draft and identify one or two Year 2 directions worth scoping.

### Remaining semester action items

- **Resolve visual artifacts and verify preprocessing.** Locate where invalid pixels first appear, assess elevation scaling and quantify usable coverage before and after parallax correction. Finish verification of the full 3 km store.
- **Complete the data checks and finalize splits.** Review GOES-16 and GOES-19 continuity, determine which MRMS products will support training and evaluation, and record the agreed storm windows and buffers.
- **Prepare verification and training.** Implement the first CSI, pFSS and CRPS comparisons as their respective targets and ensembles become available. Prepare the 3 km fine-tuning test in the verified environment and audit prospective RRFS coverage.

## Works cited

1. [Frequently Asked Questions - Icechunk](https://icechunk.io/en/v0.2.1/faq/)
2. [Having it all: virtual and materialized data products - dynamical.org](https://dynamical.org/research/virtual-data-products/)
3. [Virtually Gribberish - Bringing Icechunk clarity to GRIB archives](https://tom-nicholas.com/blog/2026/virtual-grib-nbm/)
4. [2017-2018 Roadmap for the Production Suite at NCEP - the NOAA](https://repository.library.noaa.gov/view/noaa/66200/noaa_66200_DS1.pdf)
5. [Program and Abstracts of 2012 Congress / Programme et résumés](https://cmosarchives.ca/Congress_P_A/program_abstracts2012.pdf)
6. [Infoplaza Dataplatform](https://platform.infoplaza.com/)
7. [nvidia/stormscope-goes-mrms - Hugging Face](https://huggingface.co/nvidia/stormscope-goes-mrms)
8. [Download registry.json from nvidia/stormscope-goes-mrms](https://huggingface.co/nvidia/stormscope-goes-mrms/blob/969969bdeef83aaa5a4e1c323f9a02ed9ecfe65b/registry.json)
9. [Refine MRMS skill note and add printable comparison - Hugging Face](https://huggingface.co/nvidia/stormscope-goes-mrms/commit/b40b2a87a2dfa64cb6392b4ecf38f91a9a1cc3cd)
10. [Releases · NVIDIA/earth2studio - GitHub](https://github.com/NVIDIA/earth2studio/releases)
11. [CHANGELOG.md - NVIDIA/earth2studio - GitHub](https://github.com/NVIDIA/earth2studio/blob/main/CHANGELOG.md)
12. [Skillful Thunderstorm Forecasts Learned Directly from Observations](https://arxiv.org/html/2601.17268)
13. [GitHub - NVIDIA/earth2studio: Open-source deep-learning](https://github.com/NVIDIA/earth2studio)
14. [Earth2Studio - NVIDIA GitHub Pages](https://nvidia.github.io/earth2studio/main/)
15. [Meteorology Study Guide (2027) | Scio.ly](https://www.scio.ly/guides/meteorology)
16. [StormScope 3km_10min intermittently returns all-NaN forecasts from](https://github.com/NVIDIA/earth2studio/issues/1118)
17. [nvidia/stormscope-goes-mrms · NATTEN Error running Python3.12](https://huggingface.co/nvidia/stormscope-goes-mrms/discussions/2)
18. [earth2studio-fcn3-stormscope | Model Catalog - Microsoft Foundry](https://ai.azure.com/catalog/models/earth2studio-fcn3-stormscope)
19. [C3DIR: A Deep Learning 3-Dimensional Cloud Property Retrieval](https://arxiv.org/html/2607.16929v1)
20. [Design and Evaluation of the Fenglei System - Semantic Scholar](https://www.semanticscholar.org/paper/An-Intelligent-Deep-Learning-Framework-for-Heavy-of-Zhang-Zhang/7c5968f5b4b556fc76fa50de4ac1038516878e04)
21. [Weather Forecast Data: Icechunk vs. Iceberg Head-to-Head](https://earthmover.io/blog/weather-forecast-data-icechunk-vs-iceberg/)
22. [Can AI weather models predict out-of-distribution gray swan tropical](https://www.pnas.org/doi/10.1073/pnas.2420914122)
23. [AI is good at weather forecasting. Can it predict freak weather events?](https://www.preventionweb.net/news/ai-good-weather-forecasting-can-it-predict-freak-weather-events)
24. [(PDF) Can AI weather models predict out-of-distribution gray swan](https://www.researchgate.net/publication/385107536_Can_AI_weather_models_predict_out-of-distribution_gray_swan_tropical_cyclones)
25. [AI Weather Models Can Predict Unprecedented Extreme Events by](https://datascience.uchicago.edu/news/ai-weather-models-can-predict-unprecedented-extreme-events-by-learning-from-other-regions/)
26. [Predicting regional gray swans via translocation: AI weather models](https://www.ovid.com/journals/sciad/pdf/10.1126/sciadv.ady9406~predicting-regional-gray-swans-via-translocation-ai-weather)
27. [International Best Track Archive for Climate Stewardship (IBTrACS](https://www.ncei.noaa.gov/sites/default/files/2025-04/IBTrACS_version4r01_Technical_Details.pdf)
28. [International Best Track Archive for Climate Stewardship (IBTrACS)](https://www.ncei.noaa.gov/products/international-best-track-archive)
29. [About — Forecast Eagle](https://forecasteagle.com/about)
30. [WeatherDreamer: Learning a World Model from Multimodal Earth](https://arxiv.org/html/2611.18393v1)
31. [\[PDF\] Parallax Shift in GOES ABI Data - Semantic Scholar](https://www.semanticscholar.org/paper/Parallax-Shift-in-GOES-ABI-Data-Ayala-Gerth/594d30a660428dba04d805e8db3d2f9ca64681ff)
32. [Using Optical Flow Temporal Interpolation of Satellite Imagery to](https://journals.ametsoc.org/view/journals/atot/43/1/JTECH-D-24-0131.1.xml)
33. [Opiniones - Radares Mirando al Sur - LADETEC](https://meteoradares.ladetec.com/category/opiniones/)
34. [GOES-16 GLM and ABI characteristics of severe and convective](https://shareok.org/server/api/core/bitstreams/a72f99c3-c80b-4923-80fb-410e5e601b0d/content)
35. [Seminars - Cooperative Institute for Research in the Atmosphere](https://www.cira.colostate.edu/news-events/seminars/)
36. [Quick Guide Geostationary Lightning Mapper: - noaa/nesdis/star](https://www.star.nesdis.noaa.gov/goes/documents/GLM_Quick_Guides_May_2019.pdf)
37. [TJUA_loop - NWS Radar - National Weather Service](https://radar.weather.gov/station/tjua/standard)
38. [NWS San Juan, PR](https://www.weather.gov/sju/)
39. [GPM Ground Validation at NASA Wallops Precipitation Research](https://www.researchgate.net/publication/360648563_GPM_Ground_Validation_at_NASA_Wallops_Precipitation_Research_Facility)
40. [Learning Accurate Storm-Scale Evolution from Observations - arXiv](https://arxiv.org/html/2601.17268v1)
41. [Community Radiative Transfer Model (CRTM) - JCSDA](https://www.jcsda.org/jcsda-project-community-radiative-transfer-model)
42. [The Community Radiative Transfer Model (CRTM)](https://repository.library.noaa.gov/view/noaa/53568/noaa_53568_DS1.pdf)
43. [(PDF) The Community Radiative Transfer Model (CRTM)](https://www.researchgate.net/publication/370793235_The_Community_Radiative_Transfer_Model_CRTM_Community-Focused_Collaborative_Model_Development_Accelerating_Research_to_Operations)
44. [Community Radiative Transfer Model (CRTM) Support to Satellite](https://www.star.nesdis.noaa.gov/jpss/documents/meetings/2015/AMS/AMS2015_poster_Mark_Liu.pdf)
45. [arXiv:2504.16192v1 \[physics.ao-ph\] 22 Apr 2025](https://arxiv.org/pdf/2504.16192?)
46. [Probabilistic Emulation of the Community Radiative Transfer Model](https://journals.ametsoc.org/downloadpdf/view/journals/aies/5/2/AIES-D-24-0109.1.pdf)
47. [Thirty Minutes To Better Weather Insights With Earth2Studio](https://quantumzeitgeist.com/sda-better-weather-insights-thirty-minutes/)
48. [JEDI ™ 2026.1 Release - JCSDA](https://www.jcsda.org/jedi-2026-1)
49. [NOAA North American Mesoscale Forecast System (NAM) - AWS](https://aws.amazon.com/marketplace/pp/prodview-6i3fv77kpffsi)
50. [NCEP Data Products RRFS - NWS Central Operations - NOAA](https://www.nco.ncep.noaa.gov/pmb/products/rrfs/)
51. [Regional High-Resolution Weather Models using Generative Diffusion](https://docs.nvidia.com/physicsnemo/26.08/physicsnemo/examples/weather/stormcast/README.html)
52. [NOAA Rapid Refresh Forecast System (RRFS) and RRFS Ensemble](https://registry.opendata.aws/noaa-rrfs-ops/)
