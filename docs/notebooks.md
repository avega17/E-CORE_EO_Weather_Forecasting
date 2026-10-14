# Using the notebooks

Select the `ecore-weather` kernel and run cells in order. Notebook `.py` sources
and `.ipynb` partners use Jupytext; imported code lives in `src/ecore_weather`.
Controls are created without network fetching. Dates/times are UTC and the end is
excluded. Restart the kernel and rerun setup after editing imported modules.

| Notebook | Purpose | Expected output |
|---|---|---|
| [01 MRMS](../notebooks/01_mrms.ipynb) | Choose products/time/ROI, inspect selection, fetch and diagnose radar | Listed count/bytes, saved selection, verified monthly ZIPs, raw quality summaries and previews |
| [02 GOES](../notebooks/02_goes.ipynb) | Choose native bands and scans, inspect/fetch satellite subsets | Selection, scan/gap counts, separate native band ZIPs and calibrated session-only previews |
| [03 Explore datasets](../notebooks/03_explore_datasets.ipynb) | Search local/HF archives, choose a band/time and visualize | Single map, bounded one-day/multi-day animations, storage summary, and an optional cached 2022 radar-quality figure |
| [04 Dataset report](../notebooks/04_dataset_report.ipynb) | Inspect diagnostics and measured performance, export a report | Cached report tables, interactive summaries, CSV/Markdown/HTML exports |

## Fetch notebooks

Choose parameters, inspect the listed files, then explicitly fetch. Listing is
not downloading. GOES defaults to all available CMIPF scans and eight bands;
bands preserve separate native resolutions. The shared scheduler uses two total
months with global download/reader/byte limits. MRMS offers versioned workstation/Polaris profiles. Load a profile, then
adjust download, decoder and queue limits independently. Workstation defaults
are four product-month writers, eight downloads each and one decoder each. See
[the pipeline guide](goes_parallel_pipeline.md).

Hugging Face is the configured remote option; study acquisition uses the mounted
DAS. Choose an explicit local destination for an independent small test. Avoid
starting notebook fetches alongside the production study. Source files are
removed only after verified checkpoints/archive publication. Derived masks,
calibration, interpolation and plotting do not overwrite raw data.

```bash
python notebooks/01_mrms.py --operation inspect --start 2022-09-18 --end 2022-09-19
python notebooks/02_goes.py --operation inspect --start 2022-09-18 --end 2022-09-19 --satellite 16
```

CLI and widgets call the same selection/fetch helpers. See `--help` for fetch,
storage, worker and optional PNG controls; [study jobs](study_jobs.md) covers long jobs.

## Exploring existing data

Choose MRMS or GOES first, enter a local archive root or HF bucket path, and press
Find. Filter start/end dates and times, choose the dataset, then choose a GOES band
(the control remains visible but disabled for MRMS). Select an actual observation
for a single map, or a bounded day/date range for animation. Longer animations
load more frames; they are not streaming Zarr tiles. Save options export PNG/HTML.

HF annual ZIPs are backup containers. The explorer inspects their manifest and
restores a selected verified month to a local cache before opening it. Network
transfer and cache space are required. Local completed archives are directly readable.

```bash
python notebooks/03_explore_datasets.py /path/to/archives \
  --source goes --band 13 --start 2022-09-18 --end 2022-09-19 \
  --mode single --output figures/preview.png
```

If Find is stale, press Find again; it refreshes the read-only index. During an
index lock, completed manifests are used. If code changed, restart the kernel and
rerun setup/control cells. An empty selection is not a sensor outage: check product,
band, actual timestamp range and whether that month has verified. Stale prior
attempt failures can coexist with later verified/queued work; use the status command.
The portable HTML/map backend avoids the former VS Code `jupyter-leaflet` model
error. Do not confuse missing widget frontend support with missing imagery.

## Reports and cost

Default report views use cached results and representative samples. A full audit
is explicit, resumable and memory bounded; it reads every completed observation.
Use [report instructions](dataset_report.md) for interpretations and exports.
Deleting old report outputs does not alter raw archives; rerun a selected report
when needed. Colab requires cloning the actual intended revision and installing
dependencies; push changed imported modules before testing through that clone.

For the separate full-CARIB radar figure, use `dataset_report.py radar-figure`.
Start with `--pilot-day 2022-09-18`, then run `--year 2022` after the GOES fetch
has paused. It reads one reflectivity and one Radar Quality Index source image
at the :10 UTC slot of each hour, updates a cumulative summary after each
month, and exports separate overview/regional PNG/SVG maps. Use `--render-only`
to redraw a saved summary without NOAA access.
Notebook 03 opens the two finished local PNGs under
`figures/mrms-2022/`; opening the notebook never starts the annual NOAA read.
[The MRMS coverage guide](mrms_roi_and_radar_coverage.md)
explains the acquisition box, native pixels, and illustrative 512/1,024 km
regions.
