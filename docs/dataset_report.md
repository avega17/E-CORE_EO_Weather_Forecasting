# Dataset report

The [paired GOES ROI study](goes_roi_training_study.md) records native-pixel,
compressed-storage and replayed-range costs for larger regions. It also
corrects the historical uncompressed NetCDF estimate; source compression was
previously retained in that serialization. These bounded measurements are
separate from completed monthly fetch throughput and future model skill.

See [MRMS ROI and radar coverage](mrms_roi_and_radar_coverage.md) for the
13.1% grid subset, an actual archived footprint, and the bounded RQI probe.
Bitmap/sentinel diagnostics alone do not establish quality-adjusted radar range.

Notebook 04 separates diagnostics, performance and model preparation. Raw
archives are read-only. The report database is separate from the fetch index,
so report progress does not require a write connection to the live index.

Pixel diagnostics distinguish absent observations from missing pixels. MRMS
uses each product's numeric codes and bitmap; negative reflectivity can be
valid, and shear zeros remain ambiguous. GOES reports packed encoding, fill and
range flags, plus DQF categories. Strict-good and good-plus-conditional summaries
are separate. Absent DQF means unknown quality, not automatically good data.
Visible-band nighttime behavior is not automatically a data outage.

Quartiles are calculated per observation and patch. Combining per-image
quartiles does not yield a pooled pixel quartile; those summaries must stay
separate. Full audits process each archive once in bounded batches, checkpoint
completed observations, and invalidate prior results after content/code changes.

Storage accounting keeps listed compressed source objects, returned network
bytes, logical uncompressed arrays, retained ROI arrays and compressed archive
sizes separate. A source object and a physical ZIP are counted once, even when
many band/observation records reference them. Ratios against compressed NOAA
objects are not lossless-compression ratios against raw arrays. GRIB2 can itself
contain compressed fields after gzip expansion.

Historical timings are useful evidence but must not be presented as current
code benchmarks. Summed concurrent task time is not elapsed wall time, and
month-level network counters cannot yield measured day-level throughput.
Network comparisons run only after other fetches pause at verified boundaries.
A fresh application cache does not prove that OS caches are cold.

The next model sprint needs native-band decoding, quality masks, validated
cloud-top parallax correction and an explicit 1 km grid before temporal-window
assembly. See [StormScope notes](StormScope-paper-notes.md) and
[CorrDiff notes](CorrDiff-paper-notes.md). This sprint does not train or infer.

## Commands and current evidence

```bash
python scripts/dataset_report.py inventory --source mrms
python scripts/dataset_report.py diagnose --source mrms --product MultiSensor_QPE_01H_Pass2_00.00 --start 2022-09-18 --end 2022-09-19
python scripts/dataset_report.py diagnose --source mrms --full
python scripts/dataset_report.py benchmark --benchmark-kind local --source goes --product ABI-L2-CMIPF --band 13 --start 2026-01-01 --end 2026-02-01
python scripts/dataset_report.py report --logs results/study-mrms/backups
```

Use the project's Conda interpreter. CLI and notebook diagnostics share the
same functions. The separate `report.duckdb` stores audit hashes, per-observation
patch rows and measurements. Exports include inventory.json, archives.csv,
diagnostic-months.csv, a maximum 1,000-row diagnostic-preview.csv, audits.csv,
diagnostic-days/weeks/years.csv, metadata-drift.csv, coverage-facts.json,
measurements.csv, coverage-timeline.png and report.md. The Diagnostics tab also
exports coverage-example.png with exclusive pixel classes on the native grid.
Full diagnostic rows
remain in DuckDB, avoiding a large duplicate CSV. Earlier results with another
code/archive hash remain historical and are excluded from current exports.
Inventory caches logical byte accounting by file identity for repeated requests.

The report reads the fetch index with `read_only=True`; if another process
holds its write connection, completed manifests still support discovery.
Concurrent wall time cannot be recovered by summing writer times. NVIDIA's
source returns decoded full-disk MCMIPF; its timing is a distinct workload from
native CMIPF regional archival work. The reference comparison records that
scope rather than claiming a direct speedup.

The October 2 metadata inventory covered **300 completed local MRMS archives**,
including experimental products/periods in addition to the 264 default-study
product-month checkpoints. It found 1,025,003 source records, 21.85 GB of listed
compressed NOAA objects and 22.49 GB of monthly ZIPs. Those ZIPs represent about
9.42 TB of logical measurement arrays plus 1.18 TB of quality/bitmap arrays.
Coordinate accounting is separate. This is not evidence of failed compression:
NOAA gzip/GRIB encoding is already compact, while raw decoded arrays are much
larger. Filter product and dates to report only a specific study collection.

A six-frame cached QPE backend probe (three repeats, rotated order, close and
read-back included) used 14.17 MB uncompressed and about 0.24 MB with either
compressed backend. It is a small, mostly sparse sample, not a study-wide rate.
Native C13 six-frame DAS reads took 25–37 ms. A 12-frame native C02 chunk/shard
probe favored full-ROI spatial chunks for full-frame windows: roughly 53–55 ms
for six frames versus 64–74 ms with 512-pixel chunks and 119–138 ms with
256-pixel chunks. Six-/twelve-frame sharding did not establish a clear benefit
in this warm, small sample. Keep full-frame chunks pending larger model-patch
and cross-month tests; do not rewrite existing archives from these measurements.

Successful temporary benchmark stores were removed. The source reads, reports
and hashes are under ignored results/. The full audit and controlled network
comparisons are resumable longer work; a sample report is not a completed full
study diagnosis. Controlled network commands wait for the active MCMIPF writers
to drain before mentor/NVIDIA/native/HF measurements. HF restore tests separate
returned bytes, GET requests, transfer, hashing, file writing, opening, first
window readiness and verified cache reuse. Annual containers use ZIP_STORED
around already compressed monthly ZIPs.

| Read experiment | Six frames | Twelve frames | Scope |
| --- | --- | --- | --- |
| Native C13 January on P: | 25–37 ms | 49–68 ms | One open monthly archive |
| Same C13 archive copied to Linux | 23–26 ms | 46–48 ms | Copy time excluded; temporary copy removed |
| C13 January–February boundary on P: | 25–38 ms | 45–67 ms | Two native archives, without alignment |

Each row includes three repeats and read/decompression time only. OS caches
were not flushed and old GOES fetches were active; these small differences do
not establish a general filesystem speedup. Network, calibration, geometry and
rendering are separate work.

The previous queued audit launcher is retired. Run an explicit resumable audit
with `python scripts/dataset_report.py diagnose --full`, then export with
`python scripts/dataset_report.py report`. A sample audit is not a completed
study diagnosis. Audits can run against immutable archives while fetching
continues; controlled network benchmarks still require a pause.
The audit owns its report database's write connection. While it runs, use a
different `--output` folder for interactive reports; the fetch index and raw
archives remain independent. Existing compact exports can be inspected without
opening the active report database.

Restore HF months in the notebook's collapsed backup panel: Find backups,
choose year/product/month, then Restore selected month. The resulting local
cache is used for diagnosis. This explicit restore is retained for reuse; test
restore caches are temporary and removed only after success.

## Cached summaries and shareable outputs

Notebook 04 now builds its controls in `ecore_weather.report_ui`. Buttons use
short labels, at least 145 px width and tooltips; actions disable controls while
running. Cached views show pixel-weighted quality timelines, date/product
heatmaps, patch comparisons, valid zeros and negatives, fill/bitmap gaps,
conditional-good percentages and unknown GOES quality. Available and audited
counts and read failures remain separate. The coverage map samples at most 24
frames and is labelled as a sample rather than full-period coverage.

Means and standard deviations combine pixel counts and second moments.
Observation quartiles stay observation quartiles, never averaged pooled
quartiles. The Performance tab separates byte representations, measured run
throughput and imported benchmark/task phase costs. It does not invent daily
network costs from monthly measurements.

`report.html` embeds Plotly once and can be shared offline. Aggregate CSVs and
readable Markdown accompany it. Export reads saved diagnostic results; full
array audits remain explicit, resumable and memory bounded. Use
`python scripts/dataset_report.py report` to export from the same functions.
Controlled network benchmarks use `scripts/dataset_jobs.py benchmark` while
fetching is paused; local read/backend tests remain available in the report.

October 5 live HF validation restored one month for each of the four MRMS
products (20.2–85.3 MB), verified SHA-256, opened six-frame windows in
17–77 ms, and verified cached reuse. The smallest complete annual Pass2 bundle
was 257.9 MB and transferred in 9.69 s before its hash check. Transfer rates are
measurements, not forecasts. All successful temporary restore files were removed.

## Concrete interpretations and commands

An absent hourly radar file is a missing observation. A present image whose bitmap
marks 20% of pixels missing is an available observation with missing pixels. A
valid zero is measured no rain, not automatically missing. A GOES DQF flag is
quality evidence, distinct from fill values; absent DQF means unknown quality.

A 10 MiB compressed source file can transfer 12 MiB after retries, retain 4 MiB
of decoded ROI arrays and produce a 1 MiB monthly contribution. Those figures
answer different questions; they are an illustrative example, not a measurement.
Pixel-weighted percentages use total eligible pixel counts, not a mean of image
percentages. Per-observation quartiles are not averaged into pooled quartiles.

```bash
python scripts/dataset_report.py inventory --location /path/to/archives
python scripts/dataset_report.py diagnose --location /path/to/archives \
  --source mrms --start 2022-09-18 --end 2022-09-19
python scripts/dataset_report.py report --output results/dataset-report
```

The default diagnosis samples observations. Add `--full` for an explicit resumable
audit; `--batch-frames` and `--memory-mib` bound work. Report outputs are disposable
and can be regenerated. Keep their separate database off the live fetch index.

The October 6 local radar backend comparison and the completed bounded NVIDIA
MRMS source comparison are described in [the pipeline guide](goes_parallel_pipeline.md#comparing-with-installed-nvidia-mrms).
Cached-tensor IO measurements are separate from source-download comparisons.
