# MRMS study regions and radar quality

Two static maps show the Caribbean source domain and the Puerto Rico study
regions. The colored field is mean Radar Quality Index (RQI): a score describing
potential uncertainty in radar precipitation estimates from beam blockage and
beam height relative to the melting layer. Higher scores indicate more favorable
sampling. RQI is not a direct measure of reflectivity error or accuracy.
[NOAA RQI explanation](https://vlab.noaa.gov/web/wdtd/-/radar-quality-index-rqi-?selectedFolder=668045).

## The two maps

The Caribbean overview shows the CARIB file-grid boundary, current acquisition
box and approximate future region. The Puerto Rico map adds an illustrative
training patch and a black contour around pixels with valid reflectivity in at
least 1% of successfully read hourly samples. That contour depends on weather
and data availability; it does not establish a fixed station range.

PNG and SVG figures live in `figures/`; numerical working state stays out of
Git under `results/`. Open the 2022 examples:

- [Caribbean overview](../figures/mrms-2022/mrms-rqi-macro-2022.png)
- [Puerto Rico region](../figures/mrms-2022/mrms-rqi-regional-2022.png)

Each has an SVG partner. Numerical counts and the compressed source selection
stay under `results/dataset-report/mrms-radar-2022/`; no generated assets are
stored under `docs/`. Notebook 03 displays the two cached PNGs without fetching.

Red outlines show the acquisition box: 70.24–62.56° W, 14.36–22.04° N.
The orange dashed square is an illustrative 512 × 512 km training patch;
the purple dash-dot square is an approximate 1,024 × 1,024 km future region.
Both are centered at 66.4° W, 18.2° N. A planned 1 km training grid would have
approximately 512 cells along the smaller square, but rendering does not
interpolate the observations. The existing 512-native-pixel notebook crop is
another view, with a different physical interpretation.

## Render or calculate

The completed 2022 summary can be rendered again without NOAA access:

```bash
python scripts/dataset_report.py radar-figure --year 2022 --render-only
python scripts/dataset_report.py radar-figure --year 2022 --render-only --view regional
```

For a new period, run a source-to-archive pilot before aggregation. Keep long
source reads separate from active fetch performance measurements:

```bash
python scripts/dataset_report.py radar-figure --pilot-day 2022-09-18 \
  --location /mnt/p/ecore_eo_datasets
python scripts/dataset_report.py radar-figure --start 2026-01-01 --end 2026-07-01 \
  --location /mnt/p/ecore_eo_datasets --workers 4
```

After successful Polaris pilots and tuning, the remaining annual calculations
may run on the workstation while the full-study PBS job is queued. Process
2021, 2023–2025 and H1 2026 with bounded readers; 2022 reuses its saved summary.
Check the PBS job again after validating the figures.
`--figures-dir` changes image output, while `--output` changes numerical report
state. Other controls set the center and illustrative square sizes.

The aggregation reads one full-CARIB reflectivity and RQI observation at each
`:10` UTC hourly slot, selecting only a source at or before that slot within
five minutes. This sampling keeps source-month archive boundaries unambiguous.
It retains only cumulative counts/sums and one compressed source selection.
Completed monthly aggregates are reusable; an interrupted month is reread.
The requested interval determines completeness, including leap years and H1 2026.
Raw GRIB files are discarded after decoding, and saved research archives remain
unchanged.

The completed 2022 pass read **8,757 reflectivity** and **8,752 RQI** images out
of 8,760 hourly slots, returning **507.5 MB** in **976 seconds** of monthly
read/aggregation time. Three reflectivity and eight RQI source hours were absent.
No cell reached 50% valid-reflectivity frequency; the maximum was **26.4%**.
Missing files and missing pixels remain separate in the report.

Reflectivity -99/-999 and RQI -1/-3 are omitted from their respective aggregates.
Valid zero RQI scores remain included. Observed RQI values outside 0–1, including
-285.3, are counted separately rather than declared new official sentinels.
In 2022, these anomalies accounted for 3.56% of full-grid pixel observations;
77.0% encoded no coverage. The regional view helps interpret these source-grid
statistics around Puerto Rico. The mean is calculated from valid RQI observations
at each pixel, not by treating absent values as zero.

## Completed study-period figures

All six periods have separate Caribbean and Puerto Rico maps, each exported as
PNG and SVG. Source-to-archive checks passed for the five newly read periods;
2022 reused its previously validated numerical summary. Monthly counts, source
selection hashes, pixel bounds and image files passed read-back checks. The 66
original reflectivity completion markers still match their recorded hashes.

| Period | Reflectivity hours | RQI hours | Missing hours, reflectivity / RQI | Returned MB | Aggregation minutes | Maps |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 2021 | 8,739 | 8,738 | 21 / 22 | 473.4 | 17.5 | [Overview](../figures/mrms-2021/mrms-rqi-macro-2021.png), [regional](../figures/mrms-2021/mrms-rqi-regional-2021.png) |
| 2022 | 8,757 | 8,752 | 3 / 8 | 507.5 | 16.3 | [Overview](../figures/mrms-2022/mrms-rqi-macro-2022.png), [regional](../figures/mrms-2022/mrms-rqi-regional-2022.png) |
| 2023 | 8,756 | 8,746 | 4 / 14 | 469.7 | 16.5 | [Overview](../figures/mrms-2023/mrms-rqi-macro-2023.png), [regional](../figures/mrms-2023/mrms-rqi-regional-2023.png) |
| 2024 | 8,783 | 8,783 | 1 / 1 | 504.5 | 16.3 | [Overview](../figures/mrms-2024/mrms-rqi-macro-2024.png), [regional](../figures/mrms-2024/mrms-rqi-regional-2024.png) |
| 2025 | 8,760 | 8,760 | 0 / 0 | 457.4 | 16.3 | [Overview](../figures/mrms-2025/mrms-rqi-macro-2025.png), [regional](../figures/mrms-2025/mrms-rqi-regional-2025.png) |
| 2026-h1 | 4,344 | 4,344 | 0 / 0 | 220.7 | 7.6 | [Overview](../figures/mrms-2026-h1/mrms-rqi-macro-2026-h1.png), [regional](../figures/mrms-2026-h1/mrms-rqi-regional-2026-h1.png) |

MB means decimal megabytes. Timings sum monthly source reads and aggregation,
excluding listing, pilots and rendering; concurrent periods overlap in wall time.
The 2022 row reports the earlier calculation, not new downloads. Expected hours
are 8,760 per ordinary year, 8,784 for 2024 and 4,344 for January–June 2026.
All passes recorded zero retries. These hourly figure samples are separate from
the ten-minute research archives and their fetch performance.

The 2025 and H1 2026 maps show a broader area of positive mean RQI near Puerto
Rico than earlier years. These are observed source differences, not evidence
by themselves of improved radar accuracy or a particular algorithm change.
H1 2026 covers a different seasonal interval, so it is not a full-year comparison.
Investigate monthly RQI distributions and NOAA product history before using
these differences to choose a training mask.

Compact run and validation records are `results/dataset-report/figure-run.json`
and `figure-validation.json`. Each period retains its report, compressed source
selection and resumable numerical summary. No original GRIB copies are retained.

## Percentage retained

The current bounding box is **70.24–62.56° W, 14.36–22.04° N**.
An existing Pass2 archive confirms the **1,500 × 3,000** CARIB source grid,
with cell edges at **90–60° W, 10–25° N**. We select **768 × 768** native cells:

`589,824 / 4,500,000 × 100 = 13.1072%` of source pixels.

With latitude-dependent cell areas, the ROI is approximately **690,091 km²**
of the **5,274,284 km²** CARIB rectangle: **13.0841%** of geographic area.
WGS84 geodesic polygon areas use densely sampled edges to follow latitude
parallels. Approximate dimensions are 810 × 850 km. “768” describes pixels,
not 768 km². Finer MRMS grids have more cells in the same geographic ROI.

These percentages describe the *file grid*, including cells outside useful
radar coverage. MRMS is a multiple-source mosaic, not a single-station scan.

## Maximum instrument range

The [NOAA station API](https://api.weather.gov/radar/stations/TJUA) locates
TJUA near Cayey at **18.11566° N, 66.07817° W**, with listed elevation 867 m.
NWS describes short-range reflectivity out to approximately **230 km**, and
long-range reflectivity/composite products out to approximately **460 km**.
The distant beam samples high altitude; maximum display range does not imply
accurate surface rainfall. [NWS radar explanation](https://www.weather.gov/iwx/wsr_88d).

Geodesic range envelopes around TJUA:

| Range | West–east | South–north |
| --- | --- | --- |
| 230 km | 68.25–63.90° W | 16.04–20.19° N |
| 460 km | 70.42–61.73° W | 13.96–22.27° N |

Our ROI contains the 230 km circle, but not every edge of the 460 km circle.
These are geometric envelopes, not measured coverage polygons.

A densely sampled geodesic-circle intersection, measured in a TJUA-centred
Lambert azimuthal equal-area projection, places **100% of the nominal 230 km
disk** and **92.06% of the nominal 460 km disk** inside our ROI. The latter
intersection is approximately 611,728 km² out of 664,471 km². Evidence is in
the original geometric-range check. These percentages answer the
single-radar geometric comparison; the 13.08% figure above answers the CARIB
file-grid comparison. Neither measures reliable precipitation coverage.

An existing composite-reflectivity archive at **2022-09-18 12:08:56 UTC**
also supports this distinction. Its cropped footprint excluding bitmap gaps,
nonfinite values and the -999 no-coverage code reached **459.85 km** from TJUA.
That footprint still contains -99 missing measurements. Excluding that code
too, the farthest measured reflectivity pixel was **421.99 km** away. The
first number describes an encoded footprint; the second is weather-dependent,
not a quality guarantee or study-period maximum. Evidence is in
the archived source comparison. Neither establishes single-site
attribution in a mosaic.

## Quality-dependent footprint

There is no universal quality-adjusted radius. Radar Quality Index (RQI)
reflects beam blockage and vertical sampling relative to the melting layer;
quality varies with direction, terrain and atmospheric state.
[NOAA MRMS description](https://repository.library.noaa.gov/view/noaa/15285/noaa_15285_DS1.pdf).
RQI is a dimensionless score rather than a categorical DQF flag. Its missing
and no-coverage encodings are product-specific.
[NOAA table](https://www.nssl.noaa.gov/projects/mrms/operational/tables.php).

Three whole-grid CARIB `RadarQualityIndex_00.00` reads used **57,375 bytes**:
January 1, 2021 at 12:00 UTC and September 18, 2022 at 12:00/18:00 UTC.
Bitmap, nonfinite, -1 missing and -3 no-coverage values were checked separately.
These thresholds are illustrative, not NOAA-recommended acceptance rules:

| Condition | Farthest accepted pixel from TJUA, within 460 km | Accepted ROI area |
| --- | ---: | ---: |
| RQI > 0 | 99.3–100.3 km | 3.05–3.12% |
| RQI ≥ 0.5 | 78.1–79.1 km | 0.71–0.73% |
| RQI ≥ 0.8 | 56.3 km | 0.27% |

Percentages are cell-area weighted. A farthest pixel does not establish
quality at every azimuth or every nearer pixel. The mosaic does not identify
the contributing station. These snapshots describe quality near TJUA, not a
definitive single-radar boundary or study-period climatology. RQI zero is low
quality, not zero rain. Audit more times/seasons before choosing a training
mask or shrinking the acquisition ROI.

The four archived MRMS defaults do **not** include RQI. The 2022 figure aligns
hourly RQI and reflectivity source times, but it does not validate QPE accuracy
or define a training mask. GRIB bitmaps and sentinels establish missingness,
not rainfall accuracy. Quality masking remains separate from persisted
measurements.
