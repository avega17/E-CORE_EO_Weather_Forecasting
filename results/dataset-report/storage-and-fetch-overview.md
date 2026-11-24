# GOES and MRMS storage and fetch overview

**Snapshot date: 2026-11-20 (AST).** The study covers 2021 through June 2026. GB and TB are decimal. The inventory uses published monthly ZIPs with completion markers. Source holdings, transfer counters and logical uncompressed arrays describe different stages.

## Published storage

| Site | Collection | ZIPs | Archived observations | Listed NOAA GB | Logical crop TB | ZIP GB |
| --- | --- | --- | --- | --- | --- | --- |
| workstation | GOES native eight-band CMIPF | 536 | 2,304,845 | 146,571.53 | 4.10 | 860.89 |
| workstation | MRMS, all indexed products | 358 | 1,278,931 | 29.83 | 11.94 | 27.85 |
| Polaris / Eagle | GOES native eight-band CMIPF | 536 | 2,304,845 | 146,571.53 | 4.10 | 860.86 |
| Polaris / Eagle | MRMS, two reflectivity products | 134 | 577,665 | 19.06 | 3.07 | 12.70 |

The logical crop estimate multiplies archived observation counts by the stored spatial shape and dtype in representative ZIP headers. GOES includes packed CMI and DQF. MRMS includes the decoded float64 measurement and byte validity bitmap. All 670 indexed Eagle ZIPs had matching file lengths and completion records. All workstation ZIP lengths also agreed with the archive index. Coordinates and sidecars are outside the logical crop estimate. ZIP lengths include compressed arrays, coordinates and provenance. Each physical ZIP is counted once.

Workstation also holds 3.38 GB ACHAF, 7.73 GB CMI-2KM-HYBRID, 16.55 GB MCMIPF. Including those GOES archives and all indexed MRMS products, the published instrument ZIPs total **916.40 GB**. This scope excludes processed model stores, NAM, scratch and off-site backups.

For the two reflectivity products held at both sites, the workstation also retains **12.70 GB**. Native GOES plus these two variables occupies about **873.59 GB** on the workstation and **873.56 GB** on Eagle.

## GOES storage by year and native resolution

The 0.5 km, 1 km and 2 km labels describe the nominal native band grids. The April 2025 satellite transition created separate GOES-16 and GOES-19 ZIPs within the same calendar month.

**Workstation (compressed GB)**

| Year | C02 (0.5 km) | C01 + C03 (1 km) | Five IR bands (2 km) | All bands |
| --- | --- | --- | --- | --- |
| 2021 | 90.68 | 28.65 | 36.96 | 156.29 |
| 2022 | 90.19 | 28.44 | 36.51 | 155.14 |
| 2023 | 90.14 | 28.19 | 36.58 | 154.90 |
| 2024 | 91.60 | 29.44 | 37.72 | 158.75 |
| 2025 | 91.67 | 28.38 | 37.14 | 157.18 |
| 2026 H1 | 46.03 | 14.38 | 18.22 | 78.62 |
| Total | 500.29 | 157.47 | 203.13 | 860.89 |

Across 66 eight-band calendar months, the mean is **13.04 GB**. P10, median and P90 are 11.86, 13.25 and 14.14 GB, with a 11.11–14.52 GB range. ZIPs retain 21.0% of the estimated logical crop size and 0.59% of listed full-disk NOAA object bytes. C02 accounts for 58.1% of the ZIP footprint.

**Polaris / Eagle (compressed GB)**. The 66 calendar months have the same **13.04 GB** mean and the same 11.86, 13.25 and 14.14 GB P10, median and P90 at the displayed precision.

| Year | C02 (0.5 km) | C01 + C03 (1 km) | Five IR bands (2 km) | All bands |
| --- | --- | --- | --- | --- |
| 2021 | 90.68 | 28.65 | 36.96 | 156.29 |
| 2022 | 90.19 | 28.44 | 36.51 | 155.14 |
| 2023 | 90.14 | 28.19 | 36.58 | 154.90 |
| 2024 | 91.60 | 29.44 | 37.72 | 158.75 |
| 2025 | 91.67 | 28.38 | 37.14 | 157.18 |
| 2026 H1 | 46.16 | 14.35 | 18.09 | 78.60 |
| Total | 500.42 | 157.44 | 203.00 | 860.86 |

Workstation byte accounting across the full study:

| Native resolution | Logical crop TB | Listed NOAA TB | Recorded build transfer TB | Final ZIP GB |
| --- | --- | --- | --- | --- |
| 0.5 km (C02) | 2.26 | 86.66 | 16.35 | 500.29 |
| 1 km (C01, C03) | 1.13 | 28.88 | 28.28 | 157.47 |
| 2 km (five infrared bands) | 0.71 | 31.03 | 30.48 | 203.13 |

## MRMS storage by GRIB variable

**Workstation (GB except logical crop in TB).** December 2020 boundary archives for January 1 slots are grouped into 2021.

| Variable | 2021 | 2022 | 2023 | 2024 | 2025 | 2026 H1 | ZIP total | Listed gzip | Logical TB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Composite reflectivity | 1.15 | 1.32 | 1.19 | 1.42 | 1.12 | 0.48 | 6.69 | 10.11 | 1.53 |
| Base reflectivity | 1.04 | 1.18 | 1.07 | 1.26 | 1.01 | 0.44 | 6.00 | 8.94 | 1.53 |
| Precipitation rate | 1.15 | 1.35 | 1.22 | 1.50 | 1.19 | 0.49 | 6.90 | 7.24 | 1.53 |
| Low-level azimuth shear | 1.03 | 1.03 | 0.95 | 0.85 | 0.74 | 0.35 | 4.94 | 1.27 | 6.13 |
| Pass2 hourly QPE | 0.26 | 0.29 | 0.26 | 0.33 | 0.27 | 0.11 | 1.52 | 1.28 | 0.26 |
| Midlevel azimuth shear | 0.67 | — | — | — | — | — | 0.67 | 0.01 | 0.74 |
| Pass1 hourly QPE | 0.16 | — | — | — | — | — | 0.16 | 0.13 | 0.03 |
| Radar-only hourly QPE | 0.95 | — | — | — | — | — | 0.95 | 0.85 | 0.19 |

The five workstation study variables account for **26.06 GB**. The other three variables have partial 2021 coverage. NOAA's original MRMS files already use gzip. The regional Zarr ZIP also retains explicit validity and provenance. This helps explain why the shear ZIPs use more space than their listed gzip inputs even with a much smaller decoded array footprint.

**Polaris / Eagle (GB except logical crop in TB)**. Each product has 67 ZIPs because of the source observations used for the opening January 1, 2021 slots. They were observed on December 31, 2020 and are grouped into 2021 below.

| Variable | 2021 | 2022 | 2023 | 2024 | 2025 | 2026 H1 | ZIP total | Listed gzip | Logical TB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Composite reflectivity | 1.15 | 1.32 | 1.19 | 1.42 | 1.12 | 0.48 | 6.69 | 10.11 | 1.53 |
| Base reflectivity | 1.04 | 1.18 | 1.07 | 1.26 | 1.01 | 0.44 | 6.00 | 8.94 | 1.53 |

## Coverage, missing sources and corruption

**GOES.** Both sites have 536 published ZIPs and 2,304,845 band observations. A simple six-scans-per-hour reference yields 2,312,064 slots, a net difference of 7,219 (0.31%). That difference also reflects nonnominal scan cadence. The study journal selected 2,304,847 source objects. The two confirmed C02 failures appear in both sites' completion records.

| Site | Source observation UTC | Band | Recorded evidence |
| --- | --- | --- | --- |
| workstation and Eagle | 2023-10-13T14:20:20.900000Z | C02 | Confirmed corrupt source |
| workstation and Eagle | 2024-09-29T16:00:21.100000Z | C02 | Confirmed corrupt source |

**MRMS workstation.** Counts below use the scheduler's expected time slots and post-filter matches. The invalid source column counts zero-byte NOAA files, already included in the missing-slot accounting.

| Variable | Expected slots | Matched | Missing | Coverage | Invalid sources |
| --- | --- | --- | --- | --- | --- |
| Composite reflectivity | 289,008 | 288,838 | 170 | 99.941% | 2 |
| Base reflectivity | 289,008 | 288,827 | 181 | 99.937% | 1 |
| Precipitation rate | 289,008 | 288,738 | 270 | 99.907% | 0 |
| Low-level azimuth shear | 289,008 | 288,828 | 180 | 99.938% | 1 |
| Pass2 hourly QPE | 48,168 | 48,109 | 59 | 99.878% | 0 |

Eagle's composite and base reflectivity matches have the same observation totals. Those totals imply 170 and 181 missing requested slots, respectively.

## Recorded transfer and runtime

Build transfer sums pick the latest successful saved report per archive, using its build byte counter where available. Failed work before the first preserved checkpoint and older unmetered transfers can be absent. The timing percentiles below require explicit, nonzero **current-attempt** byte counters and positive writer elapsed time. They measure concurrent archive tasks, including time spent waiting, cropping, writing and verifying.

| Site | Source | Saved records | ZIPs in scope | Recorded build TB | Summed writer h | Source retries recorded | Failed requests recorded |
| --- | --- | --- | --- | --- | --- | --- | --- |
| workstation | goes | 536 | 536 | 75.10 | 1,614.2 | 137 | 99 |
| workstation | mrms | 330 | 333 | 0.02 | 28.1 | 0 | 0 |
| Polaris | goes | 534 | 536 | 70.97 | 2,922.1 | 0 recorded | 0 recorded |
| Polaris | mrms | 134 | 134 | 0.019 | 11.3 | 0 recorded | 0 recorded |

For Eagle GOES, the merged index retained 534 saved builds and 175 archive reuse entries. The build totals exclude reuse and count each saved ZIP once. Recorded builds returned **70.97 TB**. The final attempts of those builds account for **64.80 TB**. The finished collection occupies **860.86 GB** on Eagle, about **1.21%** of the saved build transfer total. These counters do not capture every failure or retry, including work lost before publication. The two remaining ZIPs have publication evidence without a saved build record in the index.

On the workstation, GOES retains 1.15% of recorded build transfers. Its archived CMI and DQF arrays occupy about 4.10 TB logically before compression. The retained footprint reflects both the geographic crop and compression. Full downloads and range reads use temporary storage during ingestion. The largest saved single-writer sample at packing was 16.64 GB. That sample covers the closed store, packed ZIP and checkpoint metadata. Whole-job staging and filesystem allocation are separate.

Workstation MRMS build counters cover selected attempts, with many composite-reflectivity and shear months recording only sparse reads. Listed gzip bytes give a better estimate of the baseline whole-object input for those products. Summed writer hours are task time across workers.

| Site | Attempt scope | n | P10 Mb/s | Median Mb/s | P90 Mb/s | P95 Mb/s | Median minutes | P90 minutes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| workstation | GOES all bands | 480 | 49.9 | 75.8 | 172.5 | 321.1 | 190.0 | 253.9 |
| workstation | GOES C02 | 60 | 110.3 | 146.4 | 214.4 | 242.4 | 146.9 | 186.2 |
| workstation | GOES other seven | 420 | 47.9 | 71.4 | 130.4 | 850.7 | 195.3 | 260.4 |
| workstation | MRMS explicit-byte attempts | 59 | 1.4 | 2.2 | 3.9 | 4.1 | 7.3 | 7.6 |
| Polaris | GOES all bands | 534 | 29.0 | 43.0 | 79.7 | 103.0 | 358.5 | 441.5 |
| Polaris | GOES C02 | 66 | 53.3 | 79.2 | 129.9 | 141.0 | 278.1 | 408.7 |
| Polaris | MRMS base reflectivity | 67 | 1.9 | 3.1 | 5.4 | 5.8 | 5.2 | 5.3 |
| Polaris | MRMS composite reflectivity | 67 | 2.1 | 3.5 | 6.3 | 6.6 | 5.2 | 5.3 |

The workstation MRMS explicit-byte sample is base reflectivity. The timing sample covers preserved successful attempts with positive transfer, and includes resumed attempts when both counters are present. This excludes zero-byte archive reuse from the speed distribution.

Workstation closed coordinator records targeting the DAS account for **144.4 h** of GOES and **2.0 h** of MRMS. Older jobs without closed coordinator records and time between jobs are outside these totals.

The Eagle index has **18 GOES coordinator records across ten PBS jobs** with six one-node and four three-node groups. Summed coordinator time is **200.2 h** across nodes (135.9 h paused, 58.5 h failed and 5.8 h completed). Counting each group's elapsed interval once gives **117.0 h** of recorded application time, excluding queue time and intervals between jobs. Both totals are scoped to retained records. Complete PBS history and project accounting charges are unavailable from the live history query.

**Measured workstation GOES episode.** The 20-month completion report recorded 18.63 TB returned over 37.97 h, or **1,091 Mb/s** averaged across the coordinator. Its peaks were 32 active full-object downloads, eight range readers and two crop readers. Under an illustrative 2 Gb/s workstation link rate, the application averaged **54.5%** of nominal capacity. At a sustained 2 Gb/s those bytes alone would take 20.7 h, a network-only ceiling of **1.83×** more speed for the same bytes and no other constraints. Actual NIC utilization, instantaneous link saturation and achievable wall savings would need separate measurements.

**Measured Polaris MRMS production.** One 32-core/64-thread node completed both reflectivity products in **1 h 14 m 14 s** of PBS wall time. The application recorded 19.06 GB returned and 12.70 GB retained for 577,665 archived observations in 134 ZIPs, equivalent to roughly 34 Mb/s over PBS wall time. The configured production profile used 12 writers, four source tasks and one decoder per writer, and reported a 12.2 GiB peak of summed process RSS. The earlier PBS output recorded **30 h 02 m 30 s of CPU time**. The PBS wall time corresponds to approximately **1.24 allocated node-hours**.

**Final Polaris GOES recovery.** PBS job 7863950 finished with exit 0 after **5 h 16 m 49 s** on three nodes and 192 assigned hardware threads, approximately **15.84 allocated node-hours**. It published **32 new band-month ZIPs** totaling **47.87 GB** and reused **112 verified ZIPs** in the recovery selection. The newly published archives recorded **3.27 TB** returned during their final attempts, equivalent to about **1.37 Gb/s of application throughput** over PBS wall time. Partial checkpoints also contributed to the new ZIPs. PBS reported **29 h 05 m 32 s of CPU time** for this job. Per-node sampled peaks below sum process RSS and can count shared pages more than once. The PBS CPU and application memory metrics have different scopes.

| Partition | Published ZIPs | Reused ZIPs | Coordinator elapsed | Final-attempt returned TB | Peak summed RSS GiB |
| --- | --- | --- | --- | --- | --- |
| part-0 | 20 | 68 | 5 h 14 m 50 s | 2.20 | 43.8 |
| part-1 | 2 | 22 | 8 m 50 s | 0.03 | 3.6 |
| part-2 | 10 | 22 | 22 m 28 s | 1.04 | 4.1 |

The longest partition set the recovery runtime. Coordinators ran for **5.77 h combined** while PBS reserved three nodes for the full job. The imbalance and amount of reused work make this a poor test of three-node scaling or shared network saturation.

An earlier one-node Polaris GOES recovery (PBS 7770028) recorded 8 h 24 m 40 s in its application and verified or reused 32 existing January–April 2021 archives before an intentional pause. The two allocations handled different amounts of new source work. The final three-node wall time supports capacity accounting for that recovery.

## Parallelism and comparable speedups

| Experiment | Measured comparison | Interpretation |
| --- | --- | --- |
| Workstation MRMS weekly Pass2 | 59.24 → 9.55 s and 59.60 → 9.71 s | 6.20× and 6.14× for the tested sequential and four-worker paths |
| Polaris MRMS controlled two-variable sample | 215.5 s at 4 × 8 → 76.256 s median at 12 × 4 | 2.83× on the same 6,144 observations |
| Workstation corrected GOES concurrent scheduler | 33.50 → 30.24 s GOES-16 and 24.76 → 28.63 s GOES-19 | 1.11× on GOES-16 and 0.86× on GOES-19 for the matched small samples |
| Polaris GOES one-to-three-node change | Per-node ceilings permit 64 → 192 async downloads and 16 → 48 range readers | 3× resource ceiling under independent scaling; bandwidth and publication are shared |

Workstation GOES runs on 16 physical cores and 32 threads. The current profile uses two normal month owners and one C02-only tail. Polaris uses 32 cores and 64 hardware threads per node, so the final GOES allocation exposed 96 cores and 192 threads. Running a controlled single-node and three-node replay of the same selected files would isolate node scaling. Queue time, reopened checkpoints, proxy capacity and finalization change the production comparison.

Supporting records are the workstation archive index, completed ZIP headers and study journals, the latest Eagle publication inventory and PBS history, and the matched tests in [the GOES/MRMS pipeline guide](../../docs/goes_parallel_pipeline.md) and [the study jobs guide](../../docs/study_jobs.md).
