# Review of the mentor's radar downloader

The mentor's two scripts are a useful, small reference for building a fixed Puerto
Rico training image. They already separate the hourly operation from calendar-range
helpers, use anonymous NOAA access, and remove downloaded source files. Their
768-pixel grid leaves room for a random 512-pixel training crop. We retain these
scripts unchanged so new readers can be compared against concrete calculations.

## Where the research requirements extend beyond that prototype

| Finding in the original code | Consequence for longer research runs | Current response |
| --- | --- | --- |
| `fetch_mrms_radar_hour_768` fixes CARIB Pass2 QPE, geographic center, spacing and image size in module constants | Other products, domains, satellite bands and native grids cannot be selected through the function | Explicit source-specific requests, STAC selections and native-grid readers |
| Source names are built with `%H0000`; `exists` runs before every download | A real 16:58 observation is missed; long runs repeat metadata checks | Discover once, match one observation at/before each hour within five minutes, retain actual times |
| The range engine loops sequentially and uses an inclusive end | Idle network time is exposed; adjacent ranges can overlap at their shared boundary | Half-open UTC intervals, bounded concurrent reads and saved selections |
| A source file is downloaded, expanded on disk, opened with cfgrib and reprojected for every hour | Repeated disk I/O and grid construction add cost; full MRMS gzip transfer remains unavoidable | Temporary source decoding, cached source grid coordinates, crop before durable storage |
| Bilinear interpolation occurs before negative values are masked | Sentinel values can affect neighbouring interpolated pixels; the raw distinctions disappear | Persist native values first; compare the mentor recipe with product-specific masking before interpolation |
| Comments call hourly accumulation `mm/h` | A precipitation accumulation can be mistaken for instantaneous rain rate | Preserve `mm` and the actual one-hour accumulation interval |
| The first GRIB variable is chosen without validating the requested product | An unexpected source variable could be interpreted as the intended measurement | Validate product identity and retain GRIB metadata and bitmap missingness |
| Temporary names live in the working directory and cleanup uses `./temp*.idx` | Concurrent calls can collide or remove another call's index | Isolated owned temporary directories |
| The xarray dataset is not explicitly closed; failures return `False` and append to one text file | Resource lifetimes and missing-vs-failed outcomes are hard to audit | Context-managed readers, structured per-file status, nonzero script exits |
| The same output path is rewritten without source identity or read-back checks | Restarting repeats work and cannot prove a stored image matches its source | Content and metadata fingerprints, source ETag, completion markers and verified resume |
| GeoTIFF is the only durable output | Native values, calibration, quality distinctions and reusable metadata are lost | One lossless raw Zarr representation; derived images stay in session |
| Interactive prompts are the executable interface | Unattended scheduler jobs cannot reliably provide parameters or detect all failures | Shared argparse and notebook interfaces |

The tested mentor GeoTIFF lacked CRS and units tags in this environment, although
its transform locates the intended region. This is an observed output limitation,
not a claim that every library version behaves identically. See [validation](storage_and_data_management.md).
The 2022 and 2025 three-month comparisons matched every available processed image
and transform: 2,183 and 2,184 files respectively. Earlier repeated weekly tests
measured roughly sixfold improvement with four download workers. These observations
support the changes above; they do not predict throughput on Argonne or Windows DAS.

## Foundation-model and ALCF implications

Keep a reproducible native dataset separate from any model-specific representation.
Before fine-tuning, define input channels, physical units, accumulation periods,
projection, spatial resolution, temporal cadence, normalization and quality masks.
Split by storms/time with no future observations in input windows. Missing radar
coverage is not zero rain. Record which examples are eligible and why others are
excluded. Neither this QPE prototype nor an arbitrary eight-band GOES subset is
proof of compatibility with an Earth-2 checkpoint or of forecasting skill.

For example, the current [StormScope model card](https://huggingface.co/nvidia/stormscope-goes-mrms)
lists GOES C01/C02/C03/C07/C08/C09/C10/C13 and MRMS composite reflectivity (`refc`).
The current long sample's eight infrared bands and hourly QPE are therefore a
research dataset, not those exact checkpoint inputs. Its HRRR-based CONUS grid
also does not establish Puerto Rico compatibility. A model adapter or fine-tuning
study must explicitly address these differences.

ALCF uses PBS for resource requests and job launch. Project allocation, wall time
and filesystems must be specified; scheduler settings depend on the allocated
system. Stage research data to the appropriate project filesystem before training;
avoid having every GPU rank independently fetch NOAA files or publish HF chunks.
Use bounded local preprocessing and one publishing coordinator. Keep hardware and
CUDA assumptions outside the CPU ingestion environment. Actual job templates,
model adaptation and large-scale inference remain follow-up work.

Sources: [ALCF PBS guide](https://docs.alcf.anl.gov/running-jobs/),
[ALCF data management](https://www.alcf.anl.gov/onboarding-your-project/data-management),
and the project's [supplied model and data references](agent_dev_references.md).
