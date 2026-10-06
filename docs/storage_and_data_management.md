# How the weather datasets are stored and restored

We download NOAA radar and satellite observations, keep the pixels covering
Puerto Rico and the surrounding Caribbean, and save them in monthly files.
These files are the reusable observations for our forecasting research. They
retain NOAA's values, coordinates, observation times, units and quality information.
Cleaning, interpolation and other preparation for model training happen separately.
NOAA may already have calibrated or quality-controlled the selected product;
“raw” means we preserve that product without adding our own corrections.

This guide introduces the storage locations, folder layout and ways to read the
data. Use [the notebook guide](notebooks.md) for interactive exploration,
[study jobs](study_jobs.md) for fetching and backup commands, and
[the pipeline guide](goes_parallel_pipeline.md) for parallel processing and
performance measurements.

## Where the data live

| Location | Purpose |
|---|---|
| NOAA's public AWS buckets | Original radar and satellite files, usually covering much more area than we need |
| `/mnt/p/ecore_eo_datasets` on the workstation DAS | Completed monthly subsets for analysis, acquisition tests and performance tuning before Argonne jobs |
| Linux scratch directory | Temporary downloads and unfinished monthly stores, retained when needed to resume an interrupted job |
| Hugging Face bucket | Annual backups for restoring the acquired data on Argonne or another computer without fetching it from NOAA again |
| Linux-local `results/archive_index.duckdb` | A rebuildable index for finding archives and monitoring runs; it does not hold image pixels |

The **direct-attached storage (DAS)** enclosure is Windows drive P:, mounted in
WSL at `/mnt/p`. Its USB 3.1 Gen 2 connection is rated at 10 Gbit/s, allowing
roughly gigabyte-per-second transfers with suitable drives. That is an interface
capability, not a measured sustained speed for our workflow: the drives, enclosure
and Windows/WSL filesystem also affect performance.

We build unfinished stores on fast Linux storage, then copy completed archives
to the DAS. This avoids creating and updating thousands of temporary files
through the Windows-mounted filesystem. The live DuckDB index also stays on
Linux storage. Before fetching, the job checks the destination mount, write
access and free space. Destination, scratch and index paths are configurable
for another workstation or an Argonne node.

## Finding a monthly archive

Our current **region of interest (ROI)** is the bounding box
`west=-70.24, south=14.36, east=-62.56, north=22.04` degrees.
Each month is saved as `raw.zarr.zip`, accompanied by `complete.json`.
The latter describes its source, region, observations and successful verification.
An unfinished file without this completion record must not be treated as a
completed dataset.

### MRMS radar

```text
/mnt/p/ecore_eo_datasets/
└── mrms/
    └── <NOAA radar product>/
        └── roi-<identifier>/
            └── <year>/
                └── <month>/
                    ├── raw.zarr.zip
                    └── complete.json
```

Each radar product has its own monthly archive. For example, composite
reflectivity is under
`mrms/MergedReflectivityQCComposite_00.50/roi-…/2021/01/raw.zarr.zip`.
The study selects precipitation rate, composite reflectivity and low-level
azimuthal shear every ten minutes, and multisensor Pass2 one-hour rainfall
estimates every hour. Actual observation times are retained; missing times
are not filled with invented images.

### GOES satellite imagery

```text
/mnt/p/ecore_eo_datasets/
└── goes/
    └── ABI-L2-CMIPF/
        └── goes16/                 # or goes19
            └── C02/               # one folder per selected band
                └── roi-<identifier>/
                    └── <year>/
                        └── <month>/
                            ├── raw.zarr.zip
                            └── complete.json
```

The study saves all available full-disk scans for eight bands:
C01, C02, C03, C07, C08, C09, C10 and C13. Each band has a separate monthly
archive because the source grids have different resolutions. C02 is nominally
0.5 km, C01/C03 are 1 km and the selected infrared bands are 2 km directly
beneath the satellite. Pixel footprints vary with viewing geometry, so the
Caribbean subsets retain their actual native coordinates and dimensions.
GOES-16 and GOES-19 also remain separate.

`ABI-L2-CMIPF` identifies NOAA's product with separate files at each band's
native resolution. Older `ABI-L2-MCMIPF` archives are a different product with
all channels on a shared 2 km grid. They remain separately identified and cannot
replace the finer native-band data. [NOAA's CMI product description](https://goes-r.noaa.gov/products/RIMPs/RIMP_ABI-L2_CMI.pdf)

This follows the mentor's basic recommendation of **one monthly archive per
data type**. GOES adds a band folder to preserve different grids; year/month
folders keep each archive beside its completion record instead of collecting
all years in one long listing. Generic filenames are lowercase, while product
and band names keep NOAA's capitalization.

The `roi-…` folder identifies the chosen region and source configuration. Its
actual bounding box is recorded in `complete.json`. A future naming change
should make that region readable in the folder name; existing folders have
not been renamed. Repeating a matching request reuses verified observations
rather than creating another copy just because the requested dates overlap.

## Opening observations without loading a whole month

A monthly ZIP contains a **Zarr store**: arrays divided into separately readable
pieces, called chunks, plus their coordinates and descriptive metadata. Our
reader opens the ZIP directly. Choose an actual archive path in the example below:

```python
from ecore_weather.storage import open_raw

with open_raw('/path/to/month/raw.zarr.zip') as ds:
    print(ds.sizes)                          # observation count and grid dimensions
    sample = ds.isel(time=0).load()          # one image
    window = ds.isel(time=slice(0, 6)).load() # six observations from this month
# Loaded samples remain usable after the archive closes.
```

Notebook [03_explore_datasets](../notebooks/03_explore_datasets.ipynb) provides
source, product/band and date selectors, maps and animations. Its Hugging Face
option restores a monthly archive into a local cache before opening it.
An annual backup ZIP must therefore be restored as described below; it is not
itself an analysis-ready Zarr store.

### What the radar arrays mean

Radar images have dimensions **`time × latitude × longitude`**. Latitude and
longitude locate the pixels in degrees. Some source longitudes use 0–360:
293° east is equivalent to −67°. We preserve that source encoding.

| Array or metadata | Meaning |
|---|---|
| `measurement` | The selected radar product's decoded values |
| `bitmap_valid` | One flag per pixel: 1 if the original GRIB bitmap contains a measurement, 0 if it does not |
| `time` | Actual observation times in UTC |
| `request_slot_time`, `request_offset_seconds` | Requested times and each observation's offset from them |
| Units and source metadata | Product definitions, missing-value codes, accumulation periods and source identity |

**QPE** means quantitative precipitation estimate. Pass2 one-hour QPE is an
accumulated amount in millimetres; precipitation rate is in millimetres per
hour. Reflectivity is measured in dBZ, and negative reflectivity can be valid.
Azimuthal shear indicates rotation, but its zero values can have ambiguous
measurement/no-coverage meaning. Check the product definition and bitmap;
do not treat every zero or negative value as missing.

We save the decoded values and bitmap, not another copy of the original GRIB2
file. MRMS's gzip files must be downloaded and decompressed in full before
we select the desired pixels.

### What the satellite arrays mean

GOES's **Advanced Baseline Imager (ABI)** measures several spectral bands.
Each band's images have dimensions **`time × y × x`**.

| Array or metadata | Meaning |
|---|---|
| `CMI_Cxx` | Cloud and Moisture Imagery for band xx, preserved as NOAA's packed integer values |
| `DQF_Cxx` | Data Quality Flags for the same pixels; use the source's flag definitions to interpret them |
| `x`, `y` | Satellite viewing angles in radians, not longitude/latitude or kilometre distances |
| Projection information | Satellite position and Earth geometry needed to locate the pixels on a map |
| `time` | Actual scan times for that band |
| Scale, offset, fill and per-scan metadata | How to interpret packed values, identify missing pixels and apply that scan's calibration |

After checking missing-value and quality information, interpret CMI with
`physical_value = packed_value × scale_factor + add_offset`. Visible bands
represent reflectance; thermal infrared bands generally represent brightness
temperature in kelvin. The reader exposes `source_metadata_json` for information
that changes between scans. Do not assume one calibration applies to every
observation in a month.

Maps and statistics may decode or mask an in-memory copy. The saved packed
arrays remain unchanged. Future training preparation will address cloud-top
parallax and map observations onto a defined 1 km grid; those steps are not
part of acquisition. A 1 km output grid does not create finer infrared
information from a native 2 km measurement.

## How an unfinished month becomes available

During a fetch, the job writes cropped observations into a temporary Zarr store
in Linux scratch. It checks and records progress in batches of eight observations.
If the job stops, those records let it verify and reuse the completed batches
instead of starting the month again. Damaged or unfinished batches are retried;
failure evidence is retained for investigation. Do not delete scratch while a
month is unfinished.

After the selected observations have been written, the job closes the store and
packages it as `raw.zarr.zip`. It copies that file to the destination, reopens it
and checks the data. Only then does it write `complete.json` and add the completed
archive to the discovery index. A checksum—a value calculated from the file's
contents—allows later copies to be checked for corruption. Successful temporary
copies can then be removed.

Our arrays use Zarr v3 with lossless Blosc/Zstd compression: reopening them returns
the same numbers and data types. The monthly ZIP groups the already compressed
chunks into one file without compressing them again. GOES chunks contain one
cropped image; MRMS chunks contain one time step and up to 256 pixels along each
spatial axis. Each store has one writer, so concurrent fetching does not mean
several processes changing the same archive.

The temporary store and its ZIP can coexist while completion is being checked.
Jobs reserve space for this temporary duplication; only the completed archive
is kept as the permanent dataset. [The pipeline guide](goes_parallel_pipeline.md)
explains worker settings, restart behavior and the measurements behind them.

## Why we use Zarr, STAC and a local index

Weather data naturally form arrays indexed by time and location. The
[NetCDF multidimensional data model](https://www.earthmover.io/blog/tensors-vs-tables/#multidimensional-arrays-and-the-netcdf-data-model)
explains how measurements share coordinates without repeating them for every
pixel. Zarr keeps this model while allowing selected chunks to be read.
[Earth2Studio's array and coordinate interface](https://nvidia.github.io/earth2studio/main/userguide/components/io/)
provides our writing foundation; the project adds compression, source metadata
and monthly verification.

**STAC** describes which observations were selected: their region, dates,
variables and source locations. It complements the arrays themselves, as this
[discussion of STAC and Zarr](https://element84.com/software-engineering/zarr-stac/)
explains. Our saved selections and archive metadata preserve what was requested
and actually retained. The local DuckDB index makes discovery and progress queries
faster, but can be rebuilt from the archive records. Neither a selection nor an
index entry replaces the verified completion record.

Some datasets can use references to read original files without copying their
pixels. The [virtual GRIB example](https://www.earthmover.io/blog/virtual-grib-nbm)
describes this approach and its limitation with whole-file gzip compression.
It does not remove MRMS's full-download requirement. We physically store our
subsets so future research can read or restore them without repeating NOAA fetches.

## Restoring annual Hugging Face backups

Hugging Face holds backups of data we have already acquired. Their purpose is
to move the datasets to Argonne or another system without repeating a multi-year
NOAA download. For MRMS, each annual ZIP contains one product's verified monthly
ZIPs, their completion records and `year_manifest.json`, which lists the months
and their checksums. A partial study year includes only its available months.

```text
hf://buckets/<namespace>/<bucket>/noaa-subsets/
└── yearly-v1/mrms/<NOAA product>/<year>/
    ├── <yearly backup ZIP>
    └── complete.json
```

Annual grouping simplifies transfers without merging the monthly arrays or
changing their values. Notebook 03 can list backups and restore a selected month.
Because the annual ZIP does not compress its entries again, the reader can
retrieve one monthly ZIP without downloading the other months. It checks the
restored file's checksum and reuses a verified local copy on later visits.

```python
from ecore_weather.storage import destination_root
from ecore_weather.view_backup import list_bundles, inspect_bundle, restore_month

bundles = list_bundles(destination_root('hf'), source='mrms')
bundle = next(b for b in bundles
              if b['year'] == 2021
              and b['product'] == 'MultiSensor_QPE_01H_Pass2_00.00')
month = next(m for m in inspect_bundle(bundle) if m['month'] == '2021-09')
local_zip = restore_month(bundle, month, '/path/to/local-analysis-cache')
# Open local_zip with open_raw(), as above.
```

Configure `HF_BUCKET_NAME=namespace/bucket` and the S3 gateway credentials in
the environment. A Hub token is not an S3 access key. Keep credentials out of
archive URLs, notebooks and manifests; NOAA access remains anonymous. See
[the HF S3 guide](https://huggingface.co/docs/hub/storage-buckets-s3) for gateway
configuration and [study jobs](study_jobs.md) for backup commands.

Do not rely on a backup until remote verification succeeds. Annual working
copies can be removed only with a retained verification receipt and matching
monthly originals. Deleting research archives from the DAS is a separate,
explicit decision. Optional direct monthly HF writes are not the study's usual
local-acquisition and annual-backup workflow.

## Storage totals and housekeeping

Keep three quantities distinct when assessing storage:

- **Original NOAA files:** compressed source objects covering their original area.
- **Retained arrays:** our ROI measurements, quality information and coordinates
  before compression. This is the basis for measuring array compression.
- **Monthly ZIPs and annual backups:** actual files occupying local or remote
  storage. Backups are copies of observations, not additional observations.

Bytes downloaded are a separate cost: retries and repeated reads can increase
transfers without adding stored data. A small radar subset can also occupy more
space than NOAA's especially compact gzip/GRIB file while still greatly reducing
the size of its decoded arrays. Comparing the two file sizes alone does not
measure compression effectiveness. Detailed accounting and runtime comparisons
belong in [the dataset report](dataset_report.md) and
[pipeline measurements](goes_parallel_pipeline.md).

Keep current run configurations, compact progress/backup records, the local index,
pinned code used by active jobs and unfinished recovery data in `results/`.
Source identity and scientific metadata stay with the research archives; avoid
duplicating them in operational logs. Cleanup can remove old successful tests and
duplicate records without deleting research archives or remote bucket objects.

```bash
python scripts/dataset_jobs.py cleanup --current-study-only       # preview
python scripts/dataset_jobs.py cleanup --current-study-only --apply
python scripts/dataset_jobs.py index /mnt/p/ecore_eo_datasets
```

Rebuild the index only after pausing the coordinator. Do not replace a database
that a job is writing. The viewer and monitor can use completed archive records
when the live index is briefly unavailable.
