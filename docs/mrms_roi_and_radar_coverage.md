# MRMS ROI and radar coverage

Historical bounded coverage experiment. The scientific interpretation below remains
useful; obsolete probe files were removed during results consolidation. Live
coverage should be recomputed from the current archive with notebook 04, not from
old result paths.

Measured October 3, 2026. Reproduce with
`python scripts/estimate_mrms_roi.py`; results are in
`results/mrms-roi/coverage.json`. This diagnostic does not change raw archives.

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
`results/mrms-roi/geometric-range-overlap.json`. These percentages answer the
single-radar geometric comparison; the 13.08% figure above answers the CARIB
file-grid comparison. Neither measures reliable precipitation coverage.

An existing composite-reflectivity archive at **2022-09-18 12:08:56 UTC**
also supports this distinction. Its cropped footprint excluding bitmap gaps,
nonfinite values and the -999 no-coverage code reached **459.85 km** from TJUA.
That footprint still contains -99 missing measurements. Excluding that code
too, the farthest measured reflectivity pixel was **421.99 km** away. The
first number describes an encoded footprint; the second is weather-dependent,
not a quality guarantee or study-period maximum. Evidence is in
`results/mrms-roi/composite-snapshot.json`. Neither establishes single-site
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

The four archived MRMS defaults do **not** include RQI. GRIB bitmaps and
sentinels establish missingness, not rainfall accuracy. Future diagnostics
should align RQI and relevant accumulation-quality fields with QPE and source
availability. Quality masking remains separate from persisted measurements.
