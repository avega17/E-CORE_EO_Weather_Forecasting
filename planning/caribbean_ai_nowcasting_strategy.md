# Technical Strategy & Implementation Framework: Adapting High-Resolution AI Weather and Precipitation Nowcasting to Puerto Rico and the Caribbean

**Project Reference:** NSF UPRRP AI Seed Award — Objective 1d: *Development of a spatial-temporal model for localized rain nowcasting and short-range severe weather forecasting using multi-source remote sensing data.*

**Author / Technical Lead:** Research Team (CZ, HJ, AVN, DVS)

**Date:** October 26, 2026

## Executive Summary & Strategic Narrative

Island ecosystems in the Greater and Lesser Antilles face disproportionate vulnerabilities to atmospheric hazards, ranging from localized flash flooding triggered by steep orographic convection to catastrophic tropical cyclones. Current operational forecasting relies on coarse global numerical weather prediction (NWP) models (such as NOAA GFS at $\approx 25\text{ km}$) and regional convection-allowing models (CAMs) that suffer from convective-scale data assimilation (DA) latency, spin-up shock, and high computational cost. Furthermore, continental AI weather benchmarks—such as the CONUS [StormScope](https://arxiv.org/abs/2601.17268) prototype ([Pathak et al., 2026](https://arxiv.org/abs/2601.17268))—rely on dense, overlapping mainland radar composites ([MRMS](https://huggingface.co/nvidia/stormscope-goes-mrms)) and 3–6 km grids that smooth out the complex island scales of Puerto Rico and the U.S. Virgin Islands (USVI).

This updated planning and roadmap document establishes the end-to-end technical architecture for **Objective 1d** and defines our immediate **Week 1 Data Engineering & Preprocessing Execution Plan**. Critically, this update grounds our strategy in the **existing data acquisition and storage codebase** already deployed in [`avega17/E-CORE_EO_Weather_Forecasting`](https://github.com/avega17/E-CORE_EO_Weather_Forecasting/blob/main/docs/storage_and_data_management.md) and our **Argonne ALCF Polaris study workflows** ([`docs/study_jobs.md#first-setup`](https://github.com/avega17/E-CORE_EO_Weather_Forecasting/blob/main/docs/study_jobs.md#first-setup)), where regional GOES-16/19 ABI (`ABI-L2-CMIPF`) and MRMS radar subsets over our Puerto Rico / Caribbean Region of Interest (`roi`: `west=-70.24°`, `south=14.36°`, `east=-62.56°`, `north=22.04°`) across the **5.5-year study period (`2021-01-01` to `2026-07-01` UTC)** are downloaded, compressed with Blosc/Zstd into **Zarr v3 stores** using [NVIDIA Earth2Studio's I/O backends](https://nvidia.github.io/earth2studio/main/examples/07_misc/03_io_performance/?h=io#versioned-icechunk-io), and archived as monthly `raw.zarr.zip` bundles on local Direct-Attached Storage (`/mnt/p/ecore_eo_datasets`), Argonne ALCF's `/eagle/lighthouse-upr` project filesystem (recently upgraded to a **10 TB quota** on October 24, 2026), and Hugging Face backup buckets.

Specifically, this roadmap covers:

1. **Zero-Duplication Storage Strategy Building on Our Existing Zarr v3 Codebase & 10 TB ALCF Quota:** Bridging our existing monthly `raw.zarr.zip` (and directory-based Zarr v3) archives into a unified, multi-year, model-ready datacube without duplicating raw pixel data—evaluating **VirtualiZarr** (`ZarrParser` and `ZippedZarrParser` per the [VirtualiZarr FAQ](https://virtualizarr.readthedocs.io/en/stable/explanation/faq.html) and [Icechunk Zarr Guide](https://icechunk.io/en/latest/guides/zarr/#virtually-ingesting-existing-zarr-stores)), **Icechunk** ([When to Use Icechunk](https://icechunk.io/en/latest/understanding/faq/#when-should-i-use-icechunk) and [Filesystem Storage](https://icechunk.io/en/latest/guides/storage/#filesystem-storage)), and Earth2Studio's native **[`IceChunkBackend`](https://nvidia.github.io/earth2studio/main/modules/generated/io/IceChunkBackend/)** and **[`AsyncZarrBackend` + Icechunk integration](https://nvidia.github.io/earth2studio/main/examples/07_misc/03_io_performance/?h=io#versioned-icechunk-io)** across our workstation DAS (`/mnt/p`) and Argonne Leadership Computing Facility (ALCF Polaris `/eagle/lighthouse-upr` **10 TB project quota** and `/local/scratch` 3.2 TB NVMe).
2. **Multi-Resolution Dataset Variants ($3\text{ km}$, $2\text{ km}$, and $1\text{ km}$) & Historical NWP Alignment (NAM $3\text{ km}$ PR Nest vs. RRFS):** Structuring three spatial resolution tiers from our single raw observational archive—a **$3\text{ km}$ downsampled variant** for immediate zero-shot inference and fine-tuning against NVIDIA's pretrained [`stormscope-goes-mrms`](https://huggingface.co/nvidia/stormscope-goes-mrms) checkpoints (and direct alignment with the operational **[NOAA NAM $3\text{ km}$ Puerto Rico Nest](https://emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/nam.php)** covering our `2021–2026` study period), a **$2\text{ km}$ native IR-aligned variant** for training a regional Caribbean StormScope model from scratch, and a **$1\text{ km}$ unified variant** utilizing $2\times$ bilinear interpolation on 2 km ABI thermal bands alongside $1\text{ km}$ native visible bands and $1\text{ km}$ MRMS/TJUA radar ground truth to resolve localized convective precipitation over Puerto Rico.
3. **This Week's Implementation Steps for Model-Ready Datasets:** Concrete engineering tasks connecting our `ecore_weather.storage` Zarr v3 / Icechunk pipeline to [NVIDIA Earth2Studio](https://nvidia.github.io/earth2studio/main/examples/04_nowcasting/03_stormscope_goes_example/) ([`StormScopeGOES`](https://nvidia.github.io/earth2studio/main/modules/generated/models/px/StormScopeGOES/) and `StormScopeMRMS`) and [NVIDIA PhysicsNeMo](https://github.com/NVIDIA/physicsnemo/tree/main/examples/weather/regional_weather_diffusion) (`StormCastDataset` interface in [`regional_weather_diffusion`](https://docs.nvidia.com/physicsnemo/latest/physicsnemo/examples/weather/regional_weather_diffusion/README.html)), strictly matching StormScope's native exogenous conditioning inputs (**latitude, longitude, and cosine of solar zenith angle**, without speculative DEM/elevation inputs), alongside auditing the [AWS Open Data `noaa-nam` bucket](https://registry.opendata.aws/noaa-nam/) for historical Puerto Rico `priconest` coverage.
4. **Resolving Methodological Incongruences (Physics PDE Solvers vs. Observation-Space Diffusion & Historical NWP Baselines):** A rigorous audit of the discrepancies between the PI's physics-based PDE operator framing ([GeoTransolver](https://arxiv.org/abs/2512.20399) in `Nowcast_PR512.pdf`) and the high-frequency observational datasets (GOES radiances and MRMS radar reflectivity) mandated by our NSF Year 1–2 milestones (`Objective 1d-CZ-HJ.pdf`)—including correcting the NWP baseline from experimental/inaccessible HRRR-Caribbean and future-only RRFS (late 2026+) to the operational **NOAA NAM $3\text{ km}$ Puerto Rico Nest** for `2021–2026`, removing unverified DEM conditioning claims from our StormScope plan, and concluding with a synthesis of our Deep Research Technical Assessment and recent [NVIDIA PhysicsNeMo Diffusion Module](https://nvidia.github.io/physicsnemo/blog/2026/06/11/physicsnemo-diffusion/) blogs.

---

## 1. Zero-Duplication Storage Strategy: Evolving `E-CORE_EO_Weather_Forecasting` Zarr v3 Archives with VirtualiZarr and Icechunk

### 1.1 Baseline Audit: Our Current Raw Dataset Storage Architecture (`E-CORE_EO_Weather_Forecasting`)

As documented in [`docs/storage_and_data_management.md`](https://github.com/avega17/E-CORE_EO_Weather_Forecasting/blob/main/docs/storage_and_data_management.md) and [`docs/study_jobs.md#first-setup`](https://github.com/avega17/E-CORE_EO_Weather_Forecasting/blob/main/docs/study_jobs.md#first-setup), our existing data acquisition pipeline downloads NOAA radar and satellite observations across our **5.5-year study period (`2021-01-01T00:00:00Z` to `2026-07-01T00:00:00Z`)**, crops them to our Caribbean Region of Interest (**ROI bounding box: `west = -70.24°`, `south = 14.36°`, `east = -62.56°`, `north = 22.04°`**, spanning $\approx 768 \times 768\text{ km}$ centered on Puerto Rico), and writes them into **Zarr v3 stores compressed with lossless Blosc/Zstd** using Earth2Studio's asynchronous I/O backend (`AsyncZarrBackend` / `ZarrBackend`).

Currently, our storage infrastructure spans five tiers:
1. **Workstation Direct-Attached Storage (`/mnt/p/ecore_eo_datasets`):** Windows drive `P:` mounted in WSL at 10 Gbit/s ($\approx 1\text{ GB/s}$ throughput), holding completed monthly archives (`raw.zarr.zip` + `complete.json`).
2. **Argonne ALCF Polaris Shared Project Filesystem (`/eagle/lighthouse-upr` / `/lus/eagle/projects/lighthouse-upr`) & Node-Local NVMe (`/local/scratch`):** Our project allocation (`lighthouse-upr`, confirmed October 13, 2026) on ALCF's Eagle Lustre filesystem was **officially upgraded from 1 TB to a 10 TB storage quota on October 24, 2026** (effective October 23, 2026, per [`docs/study_jobs.md#first-setup`](https://github.com/avega17/E-CORE_EO_Weather_Forecasting/blob/main/docs/study_jobs.md#first-setup)). This 10 TB quota removes earlier raw-study storage restrictions, allowing the complete `2021–2026` GOES and MRMS raw archives to reside directly on `/eagle/lighthouse-upr` alongside fast 3.2 TB node-local NVMe scratch (`/local/scratch`) on Polaris compute nodes.
3. **Hugging Face Storage Bucket:** Annual cloud backups enabling rapid restoration onto Argonne Polaris or campus GPU servers without re-querying NOAA AWS S3 buckets or hitting single-file inode limits.
4. **Local Metadata Catalog (`results/archive_index.duckdb`):** A rebuildable DuckDB index tracking completed monthly archives, observation counts, and acquisition runs.
5. **Reader API (`ecore_weather.storage.open_raw`):** Opens individual monthly `raw.zarr.zip` stores cleanly as lazy Xarray datasets.

#### Current On-Disk Folder Layout & Schema Preservation
Our current pipeline preserves NOAA's exact calibrated/QC'd packed values and quality flags ("raw" in the sense that no destructive spatial interpolation or lossy normalization is applied during acquisition):

* **GOES-16/19 Satellite Imagery (`ABI-L2-CMIPF`):** Saved per band across the 8 StormScope channels (`C01`, `C02`, `C03`, `C07`, `C08`, `C09`, `C10`, `C13`):
  ```text
  /mnt/p/ecore_eo_datasets/goes/ABI-L2-CMIPF/{goes16|goes19}/{C01..C13}/roi-<id>/<year>/<month>/raw.zarr.zip
  ```
  * *Arrays & Native Grids:* Dimensions `(time, y, x)` in satellite viewing radians (`y`, `x`), preserving packed integer `CMI_Cxx` (where $\text{physical\_value} = \text{packed\_value} \times \text{scale\_factor} + \text{add\_offset}$) and pixel-level Data Quality Flags `DQF_Cxx` at native resolution (`0.5 km` for `C02`, `1.0 km` for `C01`/`C03`, and `2.0 km` for IR bands `C07`–`C13`).
* **MRMS Radar Products (`MergedReflectivityQCComposite_00.50`, `MultiSensor_QPE_01H_Pass2_00.00`, etc.):**
  ```text
  /mnt/p/ecore_eo_datasets/mrms/<NOAA_product>/roi-<id>/<year>/<month>/raw.zarr.zip
  ```
  * *Arrays & Native Grids:* Dimensions `(time, latitude, longitude)` at $1.0\text{ km}$ ($0.01^\circ$) native resolution, preserving decoded float `measurement` (dBZ or mm/hr), `bitmap_valid` (`1` = valid radar measurement, `0` = missing/out-of-range), `time` (actual UTC scan time), `request_slot_time`, and `request_offset_seconds`.

### 1.2 The Storage & Training Challenge: Bridging Monthly `raw.zarr.zip` Files to Multi-Year Model Training

While packaging each month and each GOES band into an uncompressed `.zip` archive of Zarr v3 Blosc/Zstd chunks (`raw.zarr.zip`) solves the HPC "millions of tiny files" (inode exhaustion) problem on `/eagle/lighthouse-upr` and speeds up Hugging Face transfers, it introduces three friction points when transitioning to **multi-year StormScope training (`2021–2026`) and multi-resolution ($3\text{ km}$, $2\text{ km}$, $1\text{ km}$) experimentation**:
1. **Cross-Month & Cross-Band Stitching Overhead:** Training on 5.5 years (66 months from `2021-01` to `2026-06`) across 8 separate GOES band directories and 2 MRMS product directories means a single training run must coordinate over $66 \times (8 + 2) = 660$ separate `raw.zarr.zip` archives. Calling `xr.open_mfdataset` across 660 zip stores at the start of every training script incurs heavy metadata verification overhead and breaks sliding 6-frame (60-minute) windows that straddle month boundaries (e.g., `Jan 31 23:50 UTC` $\to$ `Feb 01 00:00 UTC`).
2. **Preserving Our 10 TB ALCF `/eagle/lighthouse-upr` Quota Across Multiple Resolution Variants:** Following the October 24, 2026 quota increase to **10 TB** on `/eagle/lighthouse-upr` ([`docs/study_jobs.md#first-setup`](https://github.com/avega17/E-CORE_EO_Weather_Forecasting/blob/main/docs/study_jobs.md#first-setup)), we now have sufficient capacity to store the complete 5.5-year raw GOES and MRMS `raw.zarr.zip` study archive directly on Eagle without running out of space during PBS acquisition jobs. However, `/eagle/lighthouse-upr` is still shared across project workflows, Conda environments (`$ECORE_PROJECT_ROOT/ecore-weather/envs/cpu`), model checkpoints, and NWP baseline data (requiring `myprojectquotas` / `ecore_refresh_quota` checks before job submission). If we materialize three separate pre-processed copies of the 5.5-year dataset for the $3\text{ km}$, $2\text{ km}$, and $1\text{ km}$ variants—unpacked from `int16` to uncompressed `float32`—we would multiply our storage footprint by $3\times\text{–}5\times$ and rapidly consume the 10 TB allocation.
3. **Version Control & Safe Appends for Preprocessed & Output Stores:** As we experiment with parallax correction, DQF filtering, and multi-resolution grids—and as we write autoregressive forecast rollouts in Earth2Studio—we need transactional snapshots and branches without duplicating unmodified chunks.

### 1.3 Zero-Duplication Solution: Virtualizing Existing Zarr v3 Stores into Icechunk & Leveraging `IceChunkBackend`

Instead of rewriting our `raw.zarr.zip` archives into new duplicated Zarr stores, we combine **[VirtualiZarr](https://virtualizarr.readthedocs.io/en/stable/explanation/faq.html)** and **[Icechunk](https://icechunk.io/en/latest/guides/zarr/#virtually-ingesting-existing-zarr-stores)** with **[Earth2Studio's native `IceChunkBackend`](https://nvidia.github.io/earth2studio/main/modules/generated/io/IceChunkBackend/)** to build a **Zero-Duplication Layered Data Architecture**:

```
  ┌──────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │            LAYER 1: EXISTING IMMUTABLE RAW ZARR v3 ARCHIVES (E-CORE_EO_Weather_Forecasting)              │
  │  • Workstation DAS (/mnt/p/ecore_eo_datasets), ALCF (/eagle/lighthouse-upr 10TB), or Hugging Face Bucket │
  │  • Monthly Zarr v3 stores: goes/ABI-L2-CMIPF/.../{C01..C13}/.../raw.zarr.zip & mrms/.../raw.zarr.zip     │
  │  • Preserves packed int16 CMI_Cxx, DQF_Cxx, scale_factor/add_offset, MRMS measurement & bitmap_valid     │
  └────────────────────────────────────────────────────┬─────────────────────────────────────────────────────┘
                                                       │
                                                       ▼ (Zero-Copy Virtual Ingestion — virtualizarr >= 2.5.1)
  ┌──────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │            LAYER 2: VIRTUALIZARR + ICECHUNK UNIFIED LOGICAL DATACUBE (0% Pixel Duplication)              │
  │  • VirtualiZarr ZarrParser() / ZippedZarrParser() scans byte offsets of chunks inside existing Zarrs     │
  │  • Concatenates 66 months (2021–2026) along `time` & merges C01..C13 into a single seamless datacube     │
  │  • Committed to Icechunk via VirtualChunkContainer -> Only ~100 MB of FlatBuffer manifests on disk!      │
  └────────────────────────────────────────────────────┬─────────────────────────────────────────────────────┘
                                                       │
                                                       ▼ (Hybrid Native + Virtual Chunks in Icechunk)
  ┌──────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │            LAYER 3: MULTI-RESOLUTION PREPROCESSING & EARTH2STUDIO IceChunkBackend                        │
  │  • Small Pre-Computed Native Chunks (<1 GB): 3 km, 2 km, & 1 km target LCC lat/lon grids, TJUA/MRMS      │
  │    valid coverage masks, sparse bilinear weights, & ACHA parallax lookup maps                            │
  │  • Earth2Studio Direct Integration: `earth2studio.io.IceChunkBackend` & `AsyncZarrBackend(store=...)`    │
  │    for non-blocking, Blosc/Zstd-compressed, versioned writes of model-ready slices & forecast rollouts   │
  └──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

#### Pathway A: Virtually Ingesting Our Existing Zarr v3 Stores (Zero Byte Duplication)
As highlighted in the [Icechunk Zarr Guide (`#virtually-ingesting-existing-zarr-stores`)](https://icechunk.io/en/latest/guides/zarr/#virtually-ingesting-existing-zarr-stores) and the [VirtualiZarr FAQ](https://virtualizarr.readthedocs.io/en/stable/explanation/faq.html#i-already-have-some-data-in-zarr-do-i-have-to-resave-it), because our `E-CORE_EO_Weather_Forecasting` codebase *already saves raw observations as Zarr v3 stores*, we do **not** have to re-save or duplicate those chunks to unify all months and bands into a single multi-year dataset:
* **Unzipped Zarr v3 Directories (`ZarrParser`):** For any monthly Zarr v3 stores kept as standard directories on disk or object storage, `virtualizarr >= 2.5.1` provides `ZarrParser()`, which auto-detects Zarr v2 and Zarr v3 (including sharded v3 arrays) and extracts `ManifestArray` references pointing directly to the existing compressed Blosc/Zstd chunk files.
* **Zipped Zarr v3 Archives (`raw.zarr.zip`):** Because our `raw.zarr.zip` archives combine already-compressed Zstd Zarr v3 chunks into an uncompressed (`ZIP_STORED`) zip container, each chunk occupies a contiguous, deterministic byte range `(offset, length)` inside `raw.zarr.zip`! As shown in the [VirtualiZarr Cloud Suitability Table](https://virtualizarr.readthedocs.io/en/stable/explanation/faq.html#can-my-file-format-be-virtualized), *"Zipped Native Zarr (`.zip` no compression)"* is 100% virtualizable via VirtualiZarr's `ZippedZarrParser` (or by reading the zip central directory index once to map each internal chunk key `c/0/0/0` to its exact byte offset and length inside `raw.zarr.zip`).
* **Solving the CF `scale_factor` / `add_offset` Concatenation Footgun:** In our raw GOES stores, `CMI_Cxx` is stored as packed integers with CF attributes `scale_factor` and `add_offset`. The [VirtualiZarr FAQ](https://virtualizarr.readthedocs.io/en/stable/explanation/faq.html#can-my-specific-data-be-virtualized) warns that if `scale_factor` or `add_offset` ever varies across files, Xarray's default attribute merge overwrites them silently. When virtually concatenating our monthly `ABI-L2-CMIPF` stores across `2021–2026`, our ingestion script explicitly asserts that `(scale_factor, add_offset, _FillValue)` are identical across all months for each band `Cxx`, and stores them as group-level metadata (or registers a numcodecs/Zarr `FixedScaleOffset` codec) so lazy decoding to physical reflectance/brightness temperature is guaranteed bit-exact.
* **In-Memory Coordinates for Instant Alignment:** When calling `open_virtual_dataset` on each monthly store, we pass `loadable_variables=["time", "y", "x", "request_slot_time"]` (for GOES) and `loadable_variables=["time", "latitude", "longitude", "request_slot_time"]` (for MRMS). This loads the tiny 1D coordinate vectors into memory while leaving the multi-gigabyte `CMI_Cxx`, `DQF_Cxx`, and `measurement` arrays as zero-copy `ManifestArray` pointers, allowing `xr.concat(..., dim="time")` and `xr.merge([c01, c02, ..., c13])` to stitch 5.5 years of data in seconds.

```python
# Zero-Copy Consolidation of E-CORE Monthly Zarr v3 Stores into a Single Icechunk Repository
from pathlib import Path
import xarray as xr
import icechunk as ic
from virtualizarr import open_virtual_dataset
from virtualizarr.parsers import ZarrParser  # or ZippedZarrParser for raw.zarr.zip
from obstore.store import LocalStore
from obspec_utils.registry import ObjectStoreRegistry

# 1. Point to existing E-CORE raw dataset root on Workstation DAS (/mnt/p) or ALCF (/eagle/lighthouse-upr)
ecore_root = "/eagle/lighthouse-upr/ecore_eo_datasets"  # or "/mnt/p/ecore_eo_datasets"
file_url_prefix = f"file://{ecore_root}"
registry = ObjectStoreRegistry({file_url_prefix: LocalStore()})

# 2. Virtually open and concatenate monthly Zarr v3 stores per GOES band across 2021-2026
bands = ["C01", "C02", "C03", "C07", "C08", "C09", "C10", "C13"]
band_datasets = {}

for band in bands:
    monthly_stores = sorted(Path(f"{ecore_root}/goes/ABI-L2-CMIPF/goes16/{band}/roi-pr").glob("*/*/raw.zarr*"))
    vds_months = [
        open_virtual_dataset(
            url=f"file://{p}",
            parser=ZarrParser(),
            registry=registry,
            loadable_variables=["time", "y", "x"],
        )
        for p in monthly_stores
    ]
    # Concatenate across all months along time without reading or copying chunk bytes
    band_datasets[band] = xr.concat(vds_months, dim="time", coords="minimal", compat="override")

# 3. Commit virtual datacube to Icechunk Repository
storage = ic.local_filesystem_storage(path=f"{ecore_root}/icechunk_repos/ecore_goes_mrms_virtual")
config = ic.config.RepositoryConfig.default()
config.set_virtual_chunk_container(
    ic.virtual.VirtualChunkContainer(file_url_prefix, ic.storage.local_filesystem_store(ecore_root))
)
repo = ic.Repository.open_or_create(storage=storage, config=config)
session = repo.writable_session("main")

for band, vds in band_datasets.items():
    vds.vz.to_icechunk(session.store, group=f"goes/{band}")

snapshot_id = session.commit("Virtually ingest E-CORE 2021-2026 monthly GOES Zarr v3 stores (zero data duplication)")
```

#### Pathway B: Direct Versioned I/O via Earth2Studio's `IceChunkBackend` & `AsyncZarrBackend`
As documented in the [Earth2Studio `IceChunkBackend` API reference](https://nvidia.github.io/earth2studio/main/modules/generated/io/IceChunkBackend/) and the [Earth2Studio I/O Performance Guide (`#versioned-icechunk-io`)](https://nvidia.github.io/earth2studio/main/examples/07_misc/03_io_performance/?h=io#versioned-icechunk-io), Earth2Studio (v0.19+) now natively integrates **Icechunk** as a first-class I/O backend (`earth2studio.io.IceChunkBackend`, which inherits from `ZarrBackend`), and also allows passing an `icechunk` writable session store (`session.store`) directly into **`AsyncZarrBackend`**!

This unlocks two high-leverage capabilities for our `E-CORE_EO_Weather_Forecasting` codebase:
1. **Direct Transactional Acquisition & Preprocessing Writes (`AsyncZarrBackend` + `Icechunk`):**
   In our current acquisition pipeline (referenced in our Sept 21 email regarding [Earth2Studio's compressed local non-blocking async Zarr I/O](https://nvidia.github.io/earth2studio/main/examples/07_misc/03_io_performance/#compressed-local-non-blocking-async-zarr-io)), we use `AsyncZarrBackend` with `BloscCodec(cname="zstd", clevel=3, shuffle=BloscShuffle.shuffle)` to write monthly stores. By passing `store=session.store` from an `icechunk.Repository` into `AsyncZarrBackend` (or using `IceChunkBackend(..., blocking=False, pool_size=8, zarr_codecs=BloscCodec(...))`), our acquisition and preprocessing jobs can **append new months or write pre-regridded training crops directly into a single versioned Icechunk repository**:
   * If a PBS job on ALCF Polaris is preempted or a node fails mid-month, uncommitted background writes are cleanly discarded—eliminating the need for temporary staging folders and `complete.json` sentinel files, because **an Icechunk snapshot is only visible once `session.commit()` succeeds**.
   * Furthermore, recent Earth2Studio optimizations ([v0.19.0 release notes](https://github.com/NVIDIA/earth2studio/releases)) upgraded `ZarrBackend` and `IceChunkBackend` to use basic slice indexing on contiguous coordinate subsets, making step-by-step writes **$\approx 7\times$ faster** than before.
2. **Overlapping GPU Inference with Versioned Forecast Publication:**
   When running `StormScopeGOES` and `StormScopeMRMS` rollouts in Earth2Studio, `IceChunkBackend(blocking=False, pool_size=8)` dispatches each 10-minute forecast step to background writer threads while the GPU immediately begins denoising the next lead time, committing the complete forecast trajectory as an immutable snapshot at the end of the rollout (`io.commit(...)`).

```python
# Combining Earth2Studio's Non-Blocking AsyncZarrBackend + Zstd Compression with Icechunk Version Control
import icechunk
import zarr
from earth2studio.io import AsyncZarrBackend, IceChunkBackend

# Option 1: Native IceChunkBackend with non-blocking thread pool & Zstd compression
io_ic = IceChunkBackend(
    storage="/eagle/lighthouse-upr/ecore_eo_datasets/icechunk_repos/pr_model_ready",
    branch="main",
    chunks={"time": 1, "variable": 1, "y": 256, "x": 256},
    zarr_codecs=zarr.codecs.BloscCodec(
        cname="zstd", clevel=3, shuffle=zarr.codecs.BloscShuffle.shuffle
    ),
    blocking=False,
    pool_size=8,
)

# Option 2: High-Performance AsyncZarrBackend writing directly into an Icechunk Session Store
repo = icechunk.Repository.open_or_create(
    icechunk.local_filesystem_storage("/eagle/lighthouse-upr/ecore_eo_datasets/icechunk_repos/pr_model_ready")
)
session = repo.writable_session("main")

io_async = AsyncZarrBackend(
    None,
    parallel_coords=parallel_coords,
    blocking=False,
    store=session.store,
    zarr_codecs=zarr.codecs.BloscCodec(
        cname="zstd", clevel=3, shuffle=zarr.codecs.BloscShuffle.shuffle
    ),
)
# ... execute data ingestion / regridding / inference loop ...
io_async.close()  # Crucial: flush all in-flight async writes before committing
session.commit("Append preprocessed 2024-08 Puerto Rico 10-min batch")
```

### 1.4 Comparison of Storage & Virtualization Layers for Our Pipeline

| Storage / I/O Component | Role in Our Updated `E-CORE` Architecture | Disk Footprint Impact | Key Operational Guardrails ([Icechunk Storage Guide](https://icechunk.io/en/latest/guides/storage/#filesystem-storage)) |
| :--- | :--- | :--- | :--- |
| **Existing `raw.zarr.zip` Stores** (`ecore_weather.storage`) | Immutable Layer-1 archive of packed GOES `CMI_Cxx`/`DQF_Cxx` & MRMS `measurement`/`bitmap_valid` on `/mnt/p`, `/eagle/lighthouse-upr` (10 TB), and Hugging Face buckets. | **Baseline ($1.0\times$)** — Already compressed with Blosc/Zstd in Zarr v3. | Do not modify in place; preserve as our ground-truth regional sensor archive across `2021-01-01` to `2026-07-01`. |
| **[VirtualiZarr](https://virtualizarr.readthedocs.io/en/stable/explanation/faq.html)** (`ZarrParser` / `ZippedZarrParser`) | Scans existing monthly Zarr v3 stores and stitches 66 months $\times$ 8 bands into a unified Xarray dataset of `ManifestArray` pointers. | **$\approx 0\%$ Duplication** — Only generates byte-offset manifests (`<100 MB`). | Pass `loadable_variables=['time', 'y', 'x']`; verify identical CF `scale_factor`/`add_offset` across months before `xr.concat`. |
| **[Icechunk Engine](https://icechunk.io/en/latest/understanding/faq/#when-should-i-use-icechunk)** (`icechunk >= 2.0.5`) | Transactional storage layer holding virtual chunk pointers to `raw.zarr.zip` alongside native chunks for static grids, coverage masks, and regridding weights. | **Minimal (`<1–2 GB`)** for static lat/lon grids, valid radar masks, and reprojection weights. | **Local POSIX Warning:** `local_filesystem_storage` is **not safe for concurrent commits**; use a single writer process to commit and `repo.readonly_session("main")` for multi-GPU PyTorch readers. |
| **[Earth2Studio `IceChunkBackend`](https://nvidia.github.io/earth2studio/main/modules/generated/io/IceChunkBackend/)** & **`AsyncZarrBackend`** | High-throughput non-blocking writer for materialized resolution branches (`exp-3km`, `exp-2km`, `exp-1km`) and StormScope forecast outputs. | Controlled via Zstd `clevel=3` & selective `float16` / `int16` branch caching on ALCF `/local/scratch` or `/eagle/lighthouse-upr`. | Always call `io.close()` (for `AsyncZarrBackend`) before `session.commit()`, or call `io.commit(msg)` directly on `IceChunkBackend`. |

### 1.5 Managing Storage Quotas Across Workstation DAS (`/mnt/p`), Hugging Face, and ALCF Polaris (`/eagle/lighthouse-upr` 10 TB Quota vs. `/local/scratch`)

As documented in [`docs/study_jobs.md#first-setup`](https://github.com/avega17/E-CORE_EO_Weather_Forecasting/blob/main/docs/study_jobs.md#first-setup), our Argonne ALCF storage posture changed significantly on **October 24, 2026** (effective October 23, 2026), when the storage quota on `/eagle/lighthouse-upr` (`/lus/eagle/projects/lighthouse-upr`) was increased from 1 TB to **10 TB**:
1. **Full `2021–2026` Raw Archive Now Fits Directly on `/eagle/lighthouse-upr` (10 TB Quota):** With the 10 TB allocation on `/eagle/lighthouse-upr`, earlier raw-study storage restrictions are removed. We can run the full `2021-01-01` to `2026-07-01` UTC PBS study acquisition jobs (`jobs/polaris_mrms.pbs` and `jobs/polaris_goes.pbs`) and store all monthly `raw.zarr.zip` archives directly under `/eagle/lighthouse-upr`, synchronized with `/mnt/p/ecore_eo_datasets` and our Hugging Face backup bucket.
2. **Operational Guardrails on Polaris (`myprojectquotas` & Compute-Node `setup` Jobs):** Per [`docs/study_jobs.md#first-setup`](https://github.com/avega17/E-CORE_EO_Weather_Forecasting/blob/main/docs/study_jobs.md#first-setup), because Polaris login nodes enforce strict process-resource limits, all Conda/Mamba environment creation (`$ECORE_PROJECT_ROOT/ecore-weather/envs/cpu`, including installing `virtualizarr` and `icechunk`) must be executed on a compute node via the one-time PBS `setup` mode (`qsub -v ECORE_PBS_MODE=setup jobs/polaris_mrms.pbs`). Before launching large multi-year acquisition or preprocessing jobs, team members must verify free capacity using `myprojectquotas` (or `ecore_refresh_quota` inside PBS scripts).
3. **Why Zero-Duplication Virtualization Remains Essential Under a 10 TB Quota:** Even with 10 TB on `/eagle/lighthouse-upr`, storing the raw 5.5-year GOES (8 bands) + MRMS (reflectivity + QPE) archive alongside historical NAM $3\text{ km}$ NWP baselines, multi-epoch DiT model checkpoints, and ensemble rollout outputs will consume substantial space. If we materialized three separate uncompressed `float32` copies of the 5.5-year archive for our **$3\text{ km}$, $2\text{ km}$, and $1\text{ km}$ variants**, we would waste $4\text{–}6\text{ TB}$ on redundant pixels. By keeping the consolidated **Icechunk repository** on `/eagle/lighthouse-upr` as zero-copy virtual references to the `raw.zarr.zip` chunks (and staging active hot epochs onto each compute node's **3.2 TB `/local/scratch` NVMe** during multi-GPU training), we keep `/eagle/lighthouse-upr` usage lean while eliminating Lustre metadata contention across 4x A100 GPUs.

---

## 2. Multi-Resolution Dataset Variants ($3\text{ km}$, $2\text{ km}$, and $1\text{ km}$): Design & Zero-Duplication Implementation

Our raw `E-CORE` GOES stores live in native geostationary fixed-grid scan coordinates (`y`, `x` in radians at `0.5 km` for `C02`, `1.0 km` for `C01`/`C03`, and `2.0 km` for `C07`–`C13`), while our raw MRMS stores live in `0.01°` ($\approx 1.0\text{ km}$) `latitude`/`longitude` coordinates over `[-70.24°, -62.56°] × [14.36°, 22.04°]`.

To support all three modeling tiers without writing three duplicated multi-terabyte copies of the 5.5-year archive, we store **pre-computed sparse regridding operators (`scipy.sparse` CSR matrices / index-weight tensors), target `lat`/`lon` coordinate grids, and ACHA parallax offset maps** as native chunks inside our Icechunk repository (`groups: grid_3km, grid_2km, grid_1km`).

```
            EXISTING E-CORE RAW ZARR v3 STORES (GOES radian (y,x) @ 0.5/1/2 km + MRMS (lat,lon) @ 1 km)
                                                      │
                     ┌────────────────────────────────┴────────────────────────────────┐
                     │  Shared Pre-Computed Transformation Layer in Icechunk (<1 GB):  │
                     │  • Sparse Bilinear / Area-Average Regridding Weights (W_3, W_2, W_1)
                     │  • ACHA Parallax Displacement Vectors (Δy, Δx) for PR Viewing Geometry
                     │  • Target Lat/Lon Coordinate Grids & MRMS bitmap_valid Coverage Masks
                     └────────────────────────────────┬────────────────────────────────┘
         ┌────────────────────────────────────────────┼────────────────────────────────────────────┐
         ▼                                            ▼                                            ▼
  ┌──────────────────────────────────────┐ ┌──────────────────────────────────────┐ ┌──────────────────────────────────────┐
  │ VARIANT 1: 3.0 km Downsampled Grid   │ │ VARIANT 2: 2.0 km "Native" IR Grid   │ │ VARIANT 3: 1.0 km Unified PR Grid    │
  ├──────────────────────────────────────┤ ├──────────────────────────────────────┤ ├──────────────────────────────────────┤
  │ • Target: Zero-shot inference &      │ │ • Target: Train-from-scratch         │ │ • Target: High-res PR flash-flood    │
  │   fine-tuning with pretrained NVIDIA │ │   regional Caribbean StormScope      │ │   nowcaster resolving localized      │
  │   3km_10min StormScope checkpoints   │ │   at native 2.0 km ABI IR resolution │ │   convection & 1 km MRMS truth       │
  │ • Grid: 3.0 km LCC (256x256 master / │ │ • Grid: 2.0 km LCC (384x384 master / │ │ • Grid: 1.0 km LCC (768x768 master / │
  │   128x128 or 256x256 crop)           │ │   256x256 training crop)             │ │   512x512 training crop)             │
  │ • GOES C01–C13: Regridded & pooled   │ │ • GOES IR (C07–C13): 1:1 native 2km; │ │ • GOES IR (C07–C13): 2x bilinear     │
  │   to 3.0 km (matches NAM 3 km Nest)  │ │   VIS (C01–C03) pooled to 2.0 km     │ │   upsampled; VIS at 1.0 km native    │
  │ • Radar: MRMS measurement pooled     │ │ • Radar: MRMS 1 km pooled to 2.0 km  │ │ • Radar: Native 1.0 km MRMS          │
  │   to 3.0 km (`-10 dBZ` clear air)    │ │   using `bitmap_valid` mask          │ │   `measurement` & `bitmap_valid`     │
  │ • Tokens (4x4 patch): 4,096 tokens   │ │ • Tokens (4x4 patch): 4,096 tokens   │ │ • Tokens (4x4 patch): 16,384 tokens  │
  └──────────────────────────────────────┘ └──────────────────────────────────────┘ └──────────────────────────────────────┘
```

### 2.1 Variant 1: $3.0\text{ km}$ Downsampled Dataset (Pretrained Checkpoint Validation & NAM $3\text{ km}$ PR Nest Alignment)

* **Strategic Purpose:** As noted in our ALCF planning analysis, if we want to evaluate zero-shot performance or directly fine-tune NVIDIA's released [`nvidia/stormscope-goes-mrms`](https://huggingface.co/nvidia/stormscope-goes-mrms) checkpoints (**`3 km grid, 10 min DiT`, 260M parameters**, or **StormScope Flash `3km_10min`**), we must feed the model inputs at the **$3.0\text{ km}$ spatial resolution** on which its patch embedder and NATTEN kernels were trained. Furthermore, $3.0\text{ km}$ matches the exact spatial resolution of the operational **[NOAA NAM $3\text{ km}$ Puerto Rico Nest (`priconest`, Grid 194)](https://emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/nam.php)** that covered Puerto Rico across our `2021–2026` study period. Variant 1 thus provides an immediate, low-compute testbed to validate our end-to-end Earth2Studio inference, PhysicsNeMo fine-tuning, and operational NAM $3\text{ km}$ NWP benchmarking pipelines this week.
* **Spatial & Spectral Specification:**
  * **Grid Geometry:** $3.0\text{ km}$ Lambert Conformal Conic (LCC) grid centered on Puerto Rico ($18.2^\circ\text{N}, 66.1^\circ\text{W}$), with a **$256 \times 256\text{ pixel}$ master grid** ($768 \times 768\text{ km}$, matching our `[-70.24°, -62.56°] × [14.36°, 22.04°]` ROI). With StormScope's $4 \times 4$ patch size, $256 \times 256\text{ pixels}$ yields $64 \times 64 = 4{,}096\text{ tokens}$ (comfortably exceeding NATTEN's minimum $31 \times 31$ window requirement while requiring $7\times$ less GPU memory than CONUS).
  * **Transformation from `E-CORE` Raw Stores:** Unpacks `CMI_C01`, `C02`, `C03`, `C07`, `C08`, `C09`, `C10`, `C13` via `scale_factor` and `add_offset`, masks corrupt pixels where `DQF_Cxx != 0`, and applies conservative area-weighted regridding to the $3.0\text{ km}$ grid (`abi01c`–`abi13c`). MRMS `measurement` is regridded from $1.0\text{ km}$ to $3.0\text{ km}$, using `bitmap_valid` to distinguish valid clear-air echoes (set to `-10.0 dBZ`) from out-of-coverage pixels (`NaN` / `mask = 0`).

### 2.2 Variant 2: $2.0\text{ km}$ "Native" Satellite Resolution Dataset (Regional From-Scratch Training)

* **Strategic Purpose:** Six of our eight downloaded GOES ABI channels (`C07`, `C08`, `C09`, `C10`, `C13`, plus any additional thermal bands) have a **native nadir spatial resolution of $2.0\text{ km}$**. Downsampling to $3.0\text{ km}$ discards more than half of the native thermal pixels, while upsampling to $1.0\text{ km}$ interpolates synthetic sub-pixel values across the infrared channels. Variant 2 trains a regional Caribbean StormScope DiT model **from scratch** at the true $2.0\text{ km}$ native resolution of the ABI infrared sensor in pure observation-driven mode (without requiring external NWP forecast inputs during 0–3h nowcasting).
* **Spatial & Spectral Specification:**
  * **Grid Geometry:** $2.0\text{ km}$ uniform grid spanning $384 \times 384\text{ pixels}$ ($768 \times 768\text{ km}$ master ROI) with random/importance-sampled **$256 \times 256\text{ pixel}$ training crops** ($512 \times 512\text{ km}$, matching the PI's `PR_rain_512_crop` design).
  * **Transformation from `E-CORE` Raw Stores:** Maps the $2.0\text{ km}$ infrared channels (`C07`–`C13`) at a 1:1 spatial scale (applying inverse parallax shift correction), while area-averaging the $0.5\text{ km}$ (`C02`) and $1.0\text{ km}$ (`C01`, `C03`) visible channels and $1.0\text{ km}$ MRMS `measurement` onto the $2.0\text{ km}$ grid.
  * **Compute Footprint on ALCF Polaris:** Because a $256 \times 256\text{ pixel}$ crop at $2.0\text{ km}$ also produces only **$4{,}096\text{ tokens}$**, a from-scratch regional training run requires a fraction of the compute of CONUS StormCast/StormScope (easily fitting inside our ALCF Polaris 4x A100 40GB nodes with `fp32` precision and large batch sizes).

### 2.3 Variant 3: $1.0\text{ km}$ High-Resolution Unified Dataset (Puerto Rico Convective Target & Hybrid Downscaling)

* **Strategic Purpose:** Over Puerto Rico ($180 \times 65\text{ km}$), flash-flood-producing convective cells have updraft cores of $1.0\text{–}2.5\text{ km}$, and our `E-CORE` MRMS radar archive (`MergedReflectivityQCComposite_00.50` and `MultiSensor_QPE_01H_Pass2_00.00`) is natively **$1.0\text{ km}$ ($0.01^\circ$)**, as are GOES visible bands `C01` and `C03` (`0.5 km` for `C02`). Variant 3 preserves the full $1.0\text{ km}$ radar ground truth and visible cloud-edge sharpness.
* **Two Complementary Training Pathways for Variant 3:**
  1. **Unified $1.0\text{ km}$ StormScope DiT:** Regrids all 8 GOES channels to the $1.0\text{ km}$ grid ($768 \times 768\text{ px}$ master, **$512 \times 512\text{ px}$ training crops** $= 16{,}384\text{ tokens}$) by applying **$2\times$ bilinear interpolation** to the parallax-corrected $2.0\text{ km}$ IR channels (`C07`–`C13`), $1:1$ mapping for $1.0\text{ km}$ `C01`/`C03`, and $2 \times 2$ pooling for $0.5\text{ km}$ `C02`, paired directly with native $1.0\text{ km}$ MRMS `measurement`.
  2. **Cascaded $3\text{ km} / 2\text{ km}$ StormScope (or $3\text{ km}$ NAM PR Nest) + $1\text{ km}$ CorrDiff / Conditional Radar Diffusion (`regional_weather_diffusion`):** As noted in the [PhysicsNeMo `regional_weather_diffusion` docs](https://docs.nvidia.com/physicsnemo/latest/physicsnemo/examples/weather/regional_weather_diffusion/README.html) (*"Both models can be formulated in terms of a high-resolution state update guided by a low-resolution conditioning"*), we can also use Variant 3's $1.0\text{ km}$ MRMS radar fields as the high-resolution `state` target conditioned on $2.0\text{ km}$ or $3.0\text{ km}$ `StormScopeGOES` predictions—or $3.0\text{ km}$ [NOAA NAM Puerto Rico Nest](https://emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/nam.php) forecasts (`background`)—combining fast satellite/NWP rollouts with $1.0\text{ km}$ radar super-resolution!

### 2.4 Summary Matrix of Dataset Variants & Storage Strategy

| Specification | Variant 1: $3\text{ km}$ Downsampled | Variant 2: $2\text{ km}$ Native Satellite | Variant 3: $1\text{ km}$ Unified PR |
| :--- | :--- | :--- | :--- |
| **Primary Use Case** | Zero-shot inference, fine-tuning NVIDIA `3km_10min` weights, & $3\text{ km}$ NAM PR Nest comparison | From-scratch training of regional Caribbean StormScope DiT | High-res PR flash-flood nowcasting & 1 km MRMS radar target |
| **Master ROI Grid (`768x768 km`)** | $256 \times 256\text{ px}$ | $384 \times 384\text{ px}$ | $768 \times 768\text{ px}$ |
| **Training Crop Grid (`512x512 km`)** | $172 \times 172\text{ px}$ (or $256\times 256\text{ px}$ full) | $256 \times 256\text{ px}$ (`PR_rain_512_crop`) | $512 \times 512\text{ px}$ (`PR_rain_512_crop`) |
| **DiT Token Count ($4\times 4$ Patch)** | $4{,}096\text{ tokens}$ (at $256\times 256\text{ px}$) | $4{,}096\text{ tokens}$ | $16{,}384\text{ tokens}$ ($36{,}864$ full master) |
| **GOES IR Bands (`C07`–`C13`, $2\text{ km}$)** | Area-averaged to $3.0\text{ km}$ | **1:1 Native ($2.0\text{ km}$)** | **$2\times$ Bilinear Upsampled to $1.0\text{ km}$** |
| **GOES VIS Bands (`C01`–`C03`, $0.5\text{/}1\text{ km}$)** | Area-averaged to $3.0\text{ km}$ | Area-averaged to $2.0\text{ km}$ | **1:1 Native ($1.0\text{ km}$)** / $2\times 2$ pool (`C02`) |
| **MRMS Radar (`measurement`, $1\text{ km}$)** | Area-averaged / max-pooled to $3.0\text{ km}$ | Area-averaged / max-pooled to $2.0\text{ km}$ | **1:1 Native ($1.0\text{ km}$)** |
| **Exogenous Conditioning (`c`)** | **Normalized `lat`, `lon`, & `cos(solar_zenith_angle)`** ([Pathak et al., 2026](https://arxiv.org/abs/2601.17268)) | **Normalized `lat`, `lon`, & `cos(solar_zenith_angle)`** | **Normalized `lat`, `lon`, & `cos(solar_zenith_angle)`** |
| **Parallax Shift ($h=15\text{ km} \approx 6.7\text{ km}$)** | $\approx 2.2\text{ pixels}$ (moderate impact) | $\approx 3.3\text{ pixels}$ (significant; correct) | **$\approx 6.7\text{ pixels}$ (critical; mandatory correction)** |
| **Zero-Duplication Execution** | On-the-fly sparse matrix regrid from virtualized `raw.zarr.zip` | On-the-fly sparse matrix regrid from virtualized `raw.zarr.zip` | On-the-fly regrid or ephemeral `float16` cache on Polaris `/local/scratch` |

---

## 3. Next Implementation Steps This Week: Connecting `ecore_weather` to Earth2Studio & PhysicsNeMo

To transition our existing `/mnt/p/ecore_eo_datasets` and `/eagle/lighthouse-upr` `raw.zarr.zip` archives into running zero-shot inference in [Earth2Studio](https://nvidia.github.io/earth2studio/main/examples/04_nowcasting/03_stormscope_goes_example/) and training loops in [PhysicsNeMo `regional_weather_diffusion`](https://docs.nvidia.com/physicsnemo/latest/physicsnemo/examples/weather/regional_weather_diffusion/README.html) this week, we must bridge the schema of `ecore_weather.storage` with the exact data contracts of `StormScopeGOES`, `StormScopeMRMS`, and `StormCastDataset`.

### 3.1 Critical Schema & Preprocessing Translations (`ecore_weather` $\to$ `StormScope`)

Based on our inspection of [`docs/storage_and_data_management.md`](https://github.com/avega17/E-CORE_EO_Weather_Forecasting/blob/main/docs/storage_and_data_management.md), [`StormScopeGOES`](https://nvidia.github.io/earth2studio/main/modules/generated/models/px/StormScopeGOES/), the [StormScope paper (Table 1 & Section 4.2.2)](https://arxiv.org/abs/2601.17268), [Earth2Studio Issue #1118](https://github.com/NVIDIA/earth2studio/issues/1118), [Earth2Studio Discussion #820](https://github.com/NVIDIA/earth2studio/discussions/820), and the [PhysicsNeMo `regional_weather_diffusion` README](https://docs.nvidia.com/physicsnemo/latest/physicsnemo/examples/weather/regional_weather_diffusion/README.html), our dataset adapter must perform six exact translations:

1. **Temporal Alignment Using `request_slot_time` & `request_offset_seconds`:**
   * In our `E-CORE` MRMS stores, the `time` coordinate records the *actual UTC radar scan timestamp* (which can jitter by $\pm 1\text{–}3\text{ minutes}$ around the nominal 10-minute mark), while `request_slot_time` records the nominal 10-minute slot (`00:00`, `00:10`, `00:20`, ...) and `request_offset_seconds` records the time delta.
   * To align MRMS with GOES `ABI-L2-CMIPF` (6 scans/hour at 10-minute intervals), our VirtualiZarr/Xarray consolidation script must index both modalities by **`request_slot_time`** (filtering out any radar scan where `|request_offset_seconds| > 300s`) so that the 6-step historical sliding window ($t - 50\text{m}, \dots, t$) is strictly synchronous across all 8 GOES bands and MRMS.
2. **Unpacking GOES `CMI_Cxx` & Filtering `DQF_Cxx` (Preventing the Issue #1118 Eclipse NaN Bug):**
   * Our `E-CORE` GOES stores preserve packed integer `CMI_Cxx` and Data Quality Flags `DQF_Cxx`. Upon reading a batch, we convert to physical values ($\text{CMI} = \text{packed} \times \text{scale\_factor} + \text{add\_offset}$) and rename variables to the Earth2Studio lexicon: `abi01c`, `abi02c`, `abi03c`, `abi07c`, `abi08c`, `abi09c`, `abi10c`, `abi13c`.
   * **Crucial NaN Guard ([Earth2Studio Issue #1118](https://github.com/NVIDIA/earth2studio/issues/1118)):** During GOES-East eclipse / keep-out-zone windows around satellite local midnight ($\approx 04:40\text{–}05:31\text{ UTC}$ in Aug–Oct), raw ABI scans contain ~1% `NaN` or corrupt (`DQF_Cxx != 0`) pixels inside the domain. Because Earth2Studio's `StormScope.build_input_interpolator` derives `valid_mask` purely from geometric nearest-neighbor distance, any unmasked `NaN` or corrupt pixel survives normalization into `build_condition` and corrupts the entire forecast rollout into 100% `NaN`s. We must use our saved **`DQF_Cxx`** arrays: for isolated corrupt pixels (`DQF_Cxx > 1`), infill via local spatial neighborhood interpolation; for contiguous solar/eclipse keep-out zones, update the dynamic `valid_mask` (`0.0`) and replace the input tensor value with `_INPUT_INVALID_FILL_CONSTANT` prior to normalization.
3. **Mapping MRMS `measurement` & `bitmap_valid` to the `-10.0 dBZ` Clear-Air Contract ([Discussion #820](https://github.com/NVIDIA/earth2studio/discussions/820)):**
   * Our `E-CORE` MRMS stores preserve `measurement` and `bitmap_valid` (`1` = inside GRIB valid radar bitmap, `0` = outside radar coverage / missing).
   * As revealed in [Earth2Studio Discussion #820](https://github.com/NVIDIA/earth2studio/discussions/820), StormScope's MRMS normalization expects **valid clear-air (no-echo) pixels to be `-10.0 dBZ`** (which maps to `_INPUT_INVALID_FILL_CONSTANT = -0.25285158`), NOT `0.0 dBZ`. Setting clear air to `0 dBZ` causes pretrained StormScope checkpoints to hallucinate `22–41%` false rain coverage and saturate at `75 dBZ`.
   * Therefore, our preprocessing rule for MRMS reflectivity (`refc`) is:
     * Where `bitmap_valid == 1` and `measurement < -10.0` (or no-echo sentinel like `-99.0` / `-999.0` inside the valid radar umbrella): clamp/set `refc = -10.0 dBZ` and set training loss `mask = 1.0`.
     * Where `bitmap_valid == 0` (outside the TJUA/MRMS radar coverage umbrella or severe terrain beam blockage): set training loss **`mask = 0.0`** (and set tensor input to `_INPUT_INVALID_FILL_CONSTANT` for inference).
4. **Enabling NATTEN Token Masking (`use_nan_mask_tokens: true`) in PhysicsNeMo:**
   * Passing our `bitmap_valid` array as the `"mask"` key of shape `(1, H, W)` (`float32` in `{0.0, 1.0}`) in PhysicsNeMo's [`regional_weather_diffusion`](https://docs.nvidia.com/physicsnemo/latest/physicsnemo/examples/weather/regional_weather_diffusion/README.html) with `use_nan_mask_tokens: true` in `model.hyperparameters` automatically pools `bitmap_valid` to $4 \times 4$ token granularity, replacing out-of-range ocean tokens with learned mask tokens inside every NATTEN block and zeroing out loss backpropagation outside the radar umbrella.
5. **Supporting Additional MRMS Products (Hourly Rain QPE `MultiSensor_QPE_01H_Pass2_00.00`):**
   * Because we retained our custom MRMS download pipeline (which fetches both `MergedReflectivityQCComposite_00.50` and `MultiSensor_QPE_01H_Pass2_00.00`), we can seamlessly expose hourly QPE (`mm/hr`) as an additional target channel in our custom `StormCastDataset` for Year 2 quantitative precipitation estimation (QPE/QPF) fine-tuning.
6. **Exogenous Conditioning (`lat`, `lon`, & Cosine of Solar Zenith Angle) — Removing Speculative DEM Conditioning:**
   * **Audit of StormScope Conditioning ([Pathak et al., 2026, Table 1 & Section 4.2.2](https://arxiv.org/abs/2601.17268)):** A direct check of the StormScope paper and [`StormScopeGOES`](https://nvidia.github.io/earth2studio/main/modules/generated/models/px/StormScopeGOES/) implementation confirms that StormScope **does not use a Digital Elevation Model (DEM) or surface elevation channel**. Earlier references to DEM conditioning in our preliminary context documents were conflated from: (a) **StormCast / CorrDiff** ([Pathak et al., 2024](https://arxiv.org/abs/2408.10958); [Mardani et al., 2024](https://arxiv.org/abs/2309.15214)), which use HRRR `orography` and `land_mask` as static invariants when emulating or downscaling 3D NWP state variables, and (b) the Year 1 bullet in `Objective 1d-CZ-HJ.pdf` and `Nowcast_PR512.pdf`.
   * Instead, StormScope's exact exogenous conditioning scheme ([Pathak et al., 2026, p. 15 & p. 18](https://arxiv.org/abs/2601.17268)) concatenates **2D normalized `latitude` and `longitude` coordinate grids** ( passed via `get_invariants()`) and **the dynamically computed Cosine of Solar Zenith Angle ($\cos \theta_z$)** (computed from the UTC timestamp and pixel lat/lon) directly with the noisy latent state and prior observation frames. In addition, the high-resolution visible reflectance (`C01`–`C03`) and infrared window (`C07`, `C13`) channels inherently capture island coastlines, diurnal land-surface heating, and stationary orographic cloud development without requiring a separate static DEM channel (keeping the input channel count strictly compatible with NVIDIA's pretrained `3km_10min` StormScope weights!).
   * On ALCF Polaris (A100 GPUs), we also enforce `fp32` precision for DiT training (`training.perf.fp_optimizations: fp32`, as `bf16` can destabilize NATTEN/DiT layers per the [PhysicsNeMo guide](https://docs.nvidia.com/physicsnemo/latest/physicsnemo/examples/weather/regional_weather_diffusion/README.html#training-is-unstable-not-converging)).

### 3.2 Action Plan for This Week

| Sprint Milestone | Engineering Tasks & Deliverables | Key Tools & Target Modules |
| :--- | :--- | :--- |
| **Step 1 (Days 1–2): Virtualize `E-CORE` Monthly Zarr v3 Archives into Icechunk & Audit AWS NAM Bucket** | • Run Polaris PBS `setup` job (`qsub -v ECORE_PBS_MODE=setup jobs/polaris_mrms.pbs`) to install `virtualizarr` and `icechunk` into `$ECORE_PROJECT_ROOT/ecore-weather/envs/cpu` on `/eagle/lighthouse-upr` (10 TB quota).<br>• Query `results/archive_index.duckdb` to list all completed monthly `raw.zarr.zip` stores under `goes/ABI-L2-CMIPF/` (`C01`–`C13`) and `mrms/`.<br>• Write `ecore_weather/icechunk_catalog.py` using `VirtualiZarr` (`ZarrParser` / `ZippedZarrParser`) to align timestamps along `request_slot_time` ($\Delta t = 10\text{ min}$) and merge the 8 GOES bands into a single virtual dataset without duplicating chunks.<br>• Audit `s3://noaa-nam-pds` ([AWS Open Data `noaa-nam`](https://registry.opendata.aws/noaa-nam/)) across `2021–2026` to verify availability of `nam.tCCz.priconest.hiresfFF.tm00.grib2` (3 km Puerto Rico Nest) vs. NCEI archives. | `virtualizarr >= 2.5.1`<br>`icechunk >= 2.0.5`<br>`duckdb`, `xarray`<br>`s3fs` / `herbie` |
| **Step 2 (Day 2): Pre-Compute $3\text{ km}$, $2\text{ km}$, & $1\text{ km}$ Regridding & Parallax Operators** | • Define the 3 target LCC grids over our ROI (`[-70.24°, -62.56°] × [14.36°, 22.04°]`): Variant 1 ($3\text{ km}$, $256\times 256$), Variant 2 ($2\text{ km}$, $384\times 384$ master / $256\times 256$ crop), and Variant 3 ($1\text{ km}$, $768\times 768$ master / $512\times 512$ crop).<br>• Compute sparse bilinear/area-averaging weight matrices mapping native GOES `(y, x)` radians and MRMS `(latitude, longitude)` to each target grid, plus ACHA parallax displacement vectors ([Bernal Ayala et al., 2023](https://doi.org/10.15191/nwajom.2023.1102)) and static normalized `(lat, lon)` coordinate grids.<br>• Store these compact operators (`<1 GB` total) as native Zarr chunks (`ChunkPayload::Ref`) inside the Icechunk repository using Earth2Studio's [`IceChunkBackend`](https://nvidia.github.io/earth2studio/main/modules/generated/io/IceChunkBackend/). | `earth2studio.io.IceChunkBackend`<br>`pyproj`, `scipy.sparse` |
| **Step 3 (Day 3): Zero-Shot $3\text{ km}$ Inference in Earth2Studio** | • Adapt the [Earth2Studio StormScope example](https://nvidia.github.io/earth2studio/main/examples/04_nowcasting/03_stormscope_goes_example/) (`03_stormscope_goes_example.py`) to read a 6-step (60-minute) window from our $3\text{ km}$ virtualized dataset.<br>• Apply the `DQF_Cxx == 0` NaN guard ([Issue #1118](https://github.com/NVIDIA/earth2studio/issues/1118)) and the `bitmap_valid` / `-10.0 dBZ` clear-air convention ([Discussion #820](https://github.com/NVIDIA/earth2studio/discussions/820)).<br>• Run coupled autoregressive rollouts with pretrained `StormScopeGOES` and `StormScopeMRMS` (`3km_10min` checkpoint from [`nvidia/stormscope-goes-mrms`](https://huggingface.co/nvidia/stormscope-goes-mrms)), streaming the forecast outputs directly into a versioned Icechunk store via `IceChunkBackend(..., blocking=False)` and committing with `io.commit(...)`. | `earth2studio`<br>`StormScopeGOES`<br>`StormScopeMRMS`<br>`IceChunkBackend` |
| **Step 4 (Day 4): Implement `ECoreStormScopeDataset` in PhysicsNeMo** | • Subclass `StormCastDataset` in `physicsnemo/examples/weather/regional_weather_diffusion/datasets/dataset_ecore_icechunk.py`.<br>• Open the Icechunk repo via `repo.readonly_session(branch="main")` for thread-safe multi-GPU reading.<br>• Implement on-the-fly unpacking (`CMI_Cxx * scale_factor + add_offset`), `DQF_Cxx` masking, `-10.0 dBZ` clear-air clamping, `bitmap_valid` loss masking (`mask` key), dynamic **cosine of solar zenith angle (`cos_zenith`)** computation, normalized `(lat, lon)` `get_invariants()`, and random $512\times 512\text{ km}$ cropping (`256x256 px` at $2\text{ km}$, `512x512 px` at $1\text{ km}$).<br>• Create Hydra dataset configs `pr_3km.yaml`, `pr_2km.yaml`, and `pr_1km.yaml`. | `physicsnemo`<br>`StormCastDataset`<br>Hydra / Pydantic |
| **Step 5 (Day 5): Benchmark I/O & Smoke-Test Training on Workstation & ALCF Polaris** | • Verify quota headroom via `myprojectquotas` on Polaris (`10 TB` `/eagle/lighthouse-upr`), run `pytest test_training.py`, and launch `torchrun` training smoke tests in `fp32` with `use_nan_mask_tokens: true` and `track_sigma_bin_loss: true`.<br>• Benchmark training throughput (`samples/sec/GPU`) comparing on-the-fly sparse regridding from virtualized `raw.zarr.zip` on `/eagle/lighthouse-upr` vs. staging a pre-regridded `AsyncZarrBackend` + Icechunk branch on Polaris `/local/scratch` (3.2 TB NVMe). | `train.py` in `regional_weather_diffusion`<br>ALCF Polaris (4x A100) |

### 3.3 Reference Implementation Blueprint: `ECoreStormScopeDataset` Bridging `ecore_weather` and PhysicsNeMo

```python
# Blueprint: datasets/dataset_ecore_icechunk.py for NVIDIA PhysicsNeMo regional_weather_diffusion
import numpy as np
import xarray as xr
import icechunk as ic
from datasets.dataset import StormCastDataset

class ECoreStormScopeDataset(StormCastDataset):
    """
    Adapts E-CORE_EO_Weather_Forecasting Zarr v3 / Icechunk archives (CMI_Cxx, DQF_Cxx,
    MRMS measurement, bitmap_valid) to NVIDIA PhysicsNeMo's StormCastDataset interface
    across 3 km, 2 km, and 1 km spatial resolution variants.
    Follows StormScope (Pathak et al., 2026) exogenous conditioning: normalized (lat, lon)
    invariants + dynamically computed cosine of solar zenith angle (`cos_zenith`).
    """
    def __init__(self, params, split="train"):
        self.params = params
        self.res_km = params.resolution_km          # 3.0, 2.0, or 1.0
        self.crop_px = params.crop_size_px          # (256, 256) for 3km/2km; (512, 512) for 1km
        self.n_history = params.n_history           # 6 steps (60 min @ 10-min cadence)
        self.mode = params.mode                     # "goes_backbone" or "mrms_conditioned"

        # Open consolidated E-CORE Icechunk repository in lock-free read-only mode
        storage = ic.local_filesystem_storage(path=params.icechunk_repo_path)
        repo = ic.Repository.open(storage=storage)
        session = repo.readonly_session(branch=params.icechunk_branch)
        self.root_store = session.store

        # Open virtualized/hybrid dataset group and pre-computed grid operators
        self.ds = xr.open_zarr(self.root_store, group=params.data_group, consolidated=False)
        self.grid_ds = xr.open_zarr(self.root_store, group=f"operators/grid_{int(self.res_km)}km", consolidated=False)

        self.bands = ["C01", "C02", "C03", "C07", "C08", "C09", "C10", "C13"]
        self.e2s_goes_names = ["abi01c", "abi02c", "abi03c", "abi07c", "abi08c", "abi09c", "abi10c", "abi13c"]
        self._cache_invariants()

    def _cache_invariants(self):
        # StormScope static spatial conditioning: normalized 2D latitude and longitude grids
        lat = self.grid_ds["lat_norm"].values.astype(np.float32)
        lon = self.grid_ds["lon_norm"].values.astype(np.float32)
        self.invariants_full = np.stack([lat, lon], axis=0)  # (2, H_master, W_master)

    def __len__(self):
        return len(self.ds.request_slot_time) - self.n_history

    def state_channels(self):
        return self.e2s_goes_names if self.mode == "goes_backbone" else ["refc"]

    def background_channels(self):
        # Dynamic exogenous conditioning includes `cos_zenith` (plus GOES channels for MRMS model)
        return ["cos_zenith"] if self.mode == "goes_backbone" else self.e2s_goes_names + ["cos_zenith"]

    def image_shape(self):
        return tuple(self.crop_px)

    def get_invariants(self):
        return self.invariants_full[:, :self.crop_px[0], :self.crop_px[1]]

    def __getitem__(self, idx):
        # 1. Slice 6 past steps + 1 target step along aligned `request_slot_time`
        # 2. Decode packed GOES `CMI_Cxx` (packed * scale_factor + add_offset) & mask `DQF_Cxx != 0`
        # 3. For MRMS `measurement`: where `bitmap_valid == 1` and val < -10 dBZ -> clamp to -10.0 dBZ;
        #    where `bitmap_valid == 0` -> set loss `mask = 0.0` (used by NATTEN `use_nan_mask_tokens: true`)
        # 4. Apply pre-computed sparse regridding + parallax shift to 3km, 2km, or 1km grid
        # 5. Compute dynamic Cosine of Solar Zenith Angle (`cos_zenith`) from `request_slot_time` and (lat, lon)
        # 6. Apply random/importance-weighted 512x512 km crop and Z-score normalize
        # Returns {"state": [past_state, future_state], "mask": valid_mask, "background": cond}
        ...
```

---

## 6. Updated Multi-Scale Architecture & 4-Year Project Roadmap

### 6.1 End-to-End Multi-Scale Model Coupling (0–24 Hours)

| Forecast Horizon | Primary Engine & Mode | Spatial Resolution & Grid | Temporal Cadence | Driving Inputs & Conditioning (`E-CORE` Schema) |
| :--- | :--- | :--- | :--- | :--- |
| **0–3 Hours (Nowcasting)** | **StormScope** (`StormScopeGOES` + `StormScopeMRMS` DiT in PhysicsNeMo) | **Variant 1:** $3.0\text{ km}$ (Baseline)<br>**Variant 2:** $2.0\text{ km}$ (Native IR)<br>**Variant 3:** $1.0\text{ km}$ (PR Target) | $\Delta t = 10\text{ min}$ (Zero NWP DA latency; 6-step `request_slot_time` window) | 8 GOES ABI `CMI_Cxx` bands (`DQF_Cxx == 0` filtered & parallax-corrected), MRMS `measurement` (`-10 dBZ` clear-air) + `bitmap_valid` NATTEN mask, normalized `(lat, lon)` coordinates, & dynamic `cos(solar_zenith_angle)`. |
| **0–3 Hours (Physics & NWP Baselines)** | **NowcastNet** (Physics Evolution Operator), **pySTEPS** (Optical Flow), & **[NOAA NAM $3\text{ km}$ PR Nest](https://emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/nam.php)** | Identical $3\text{ km}$, $2\text{ km}$, and $1\text{ km}$ Icechunk grids | $\Delta t = 10\text{ min}$ ($1\text{ hr}$ for NAM `priconest`) | Identical MRMS `measurement` sequences + GOES IR conditioning; benchmarked historically (`2021–2026`) against **NOAA NAM $3\text{ km}$ PR Nest** and prospectively (late 2026+) against **RRFS**. |
| **3–6+ Hours (Synoptic Nearcasting)** | **StormScope Nearcaster** (NWP-Conditioned DiT) | $2.0\text{ km}$ / $3.0\text{ km}$ regional grid | $\Delta t = 10\text{ min}$ to $1\text{ hr}$ | Conditioned on **NOAA NAM** / **GFS** / **ERA5** $500\text{ hPa}$ geopotential height (`z500`), simulated composite reflectivity (`REFC`), `(lat, lon)`, and `cos(solar_zenith_angle)` ([Pathak et al., 2026, Table 1](https://arxiv.org/abs/2601.17268)). |
| **3–24 Hours (Hazard Downscaling)** | **CorrDiff** (2-Step UNet Regression + EDM Diffusion Corrector) | $3.0\text{ km} \to 1.0\text{ km}$ (3:1 downscaling ratio over $512\times 512\text{ km}$) | $\Delta t = 1\text{ hr}$ (and $15\text{ min}$ sub-hourly slices) | Driven by $3\text{ km}$ **StormScope** / **NAM $3\text{ km}$ PR Nest (`priconest`)** (`2021–2026`) / **RRFS PR Nest** (`late 2026+`); restores $1\text{ km}$ localized rain (`MultiSensor_QPE_01H_Pass2_00.00` & `refc`). *(Note: Static terrain/land-sea invariants apply only here in CorrDiff NWP downscaling, where supported by [Mardani et al., 2024](https://arxiv.org/abs/2309.15214)).* |

### 6.2 Immediate Execution Checklist (This Week) & Year 1–4 Milestones

* **This Week (Sprint 1 — `E-CORE` Zarr v3 Virtualization, 10 TB ALCF Setup, `IceChunkBackend` Integration, & NAM Audit):**
  * [ ] Leverage the newly upgraded **10 TB `/eagle/lighthouse-upr` storage quota** ([`docs/study_jobs.md#first-setup`](https://github.com/avega17/E-CORE_EO_Weather_Forecasting/blob/main/docs/study_jobs.md#first-setup)) to complete any remaining `2021-01-01` to `2026-07-01` GOES/MRMS study acquisition batches on ALCF Polaris and install `virtualizarr` + `icechunk` via a compute-node PBS `setup` job.
  * [ ] Virtually ingest our existing monthly `raw.zarr.zip` stores (`goes/ABI-L2-CMIPF/goes16/{C01..C13}`, `mrms/MergedReflectivityQCComposite_00.50`, and `mrms/MultiSensor_QPE_01H_Pass2_00.00`) into a unified **Icechunk** repository using `VirtualiZarr` (`ZarrParser` / `ZippedZarrParser`), aligning timestamps via `request_slot_time` without duplicating chunks.
  * [ ] Audit the [AWS Open Data `noaa-nam` bucket (`s3://noaa-nam-pds`)](https://registry.opendata.aws/noaa-nam/) across `2021–2026` to verify file availability for the **[NOAA NAM $3\text{ km}$ Puerto Rico Nest (`nam.tCCz.priconest.hiresfFF.tm00.grib2`)](https://emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/nam.php)** vs. $12\text{ km}$ parent NAM and NCEI archives.
  * [ ] Generate and commit the pre-computed sparse regridding matrices, ACHA parallax displacement vectors ([Bernal Ayala et al., 2023](https://doi.org/10.15191/nwajom.2023.1102)), and static normalized `(lat, lon)` coordinate grids for the **$3\text{ km}$, $2\text{ km}$, and $1\text{ km}$ dataset variants** using Earth2Studio's [`IceChunkBackend`](https://nvidia.github.io/earth2studio/main/modules/generated/io/IceChunkBackend/).
  * [ ] Execute zero-shot $3\text{ km}$ inference over a Puerto Rico convective event using [`StormScopeGOES`](https://nvidia.github.io/earth2studio/main/modules/generated/models/px/StormScopeGOES/) and `StormScopeMRMS` in Earth2Studio (verifying `DQF_Cxx` eclipse NaN filtering, `bitmap_valid` masking, and `-10 dBZ` clear-air handling), writing outputs via `IceChunkBackend(..., blocking=False)`.
  * [ ] Implement `ECoreStormScopeDataset(StormCastDataset)` in PhysicsNeMo [`regional_weather_diffusion`](https://docs.nvidia.com/physicsnemo/latest/physicsnemo/examples/weather/regional_weather_diffusion/README.html) with StormScope's native `(lat, lon)` and `cos(solar_zenith_angle)` conditioning, and benchmark training throughput on workstation and ALCF Polaris (4x A100 40GB nodes, comparing virtualized reads on `/eagle/lighthouse-upr` vs. `/local/scratch` caching).
  * [ ] Present the synthesized architectural roadmap (`StormScope` + `NowcastNet` + `NAM 3 km PR Nest` vs. `GeoTransolver` and experimental `HRRR-Caribbean`) to the PI/mentor team and update the Overleaf manuscript (`Nowcast_PR512.pdf`).
* **Year 1 (Core System Architecture & Satellite Pre-Training):**
  * Complete self-supervised pre-training of the regional Caribbean `StormScopeGOES` DiT backbone on ALCF Polaris (comparing Variant 1 $3\text{ km}$ fine-tuning vs. Variant 2 $2\text{ km}$ and Variant 3 $1\text{ km}$ from-scratch training) across our 5.5-year (`2021–2026`) `E-CORE` archive on `/eagle/lighthouse-upr`.
* **Year 2 (Localized Radar Fine-Tuning & 0–3h Validation Against NAM $3\text{ km}$ PR Nest):**
  * Fine-tune the radar DiT (`StormScopeMRMS`) on `MergedReflectivityQCComposite_00.50` and hourly QPE (`MultiSensor_QPE_01H_Pass2_00.00`) conditioned on the satellite backbone, `(lat, lon)`, and `cos(solar_zenith_angle)` using `bitmap_valid` masked loss (`use_nan_mask_tokens: true`); publish `2021–2026` benchmark comparison against **NowcastNet**, **pySTEPS**, and the operational **NOAA NAM $3\text{ km}$ Puerto Rico Nest**.
* **Year 3 (Archipelago Domain Scaling — $1024 \times 1024\text{ km}$ & Prospective RRFS Evaluation):**
  * Scale domain from Hispaniola to the Lesser Antilles using PhysicsNeMo `ShardTensor` domain parallelism and `MultiDiffusionModel2D`; integrate exploratory Météo-France C-band radars (Guadeloupe/Martinique), DPS score-based data assimilation, and prospective evaluation against the newly operational **NOAA RRFS Puerto Rico Nest** (late 2026+).
* **Year 4 (3–24h Nearcasting, CorrDiff Downscaling, & Operational Handoff):**
  * Train and couple `CorrDiff` ($3\text{ km} \to 1.0\text{ km}$) on historical `2021–2026` **NAM $3\text{ km}$ PR Nest** / **StormScope** pairs (and transition to live **RRFS** conditioning) for 3–24h hazard downscaling and marine radar infilling; deploy low-latency automated inference pipeline using `IceChunkBackend` for NWS San Juan and regional emergency management stakeholders.

---

## 7. References & Technical Documentation

1. **Vega Nogales, A. S., Vavoulis Salas, D. A., Zhuang, C., & Janwa, H. L. (2026).** *How the Weather Datasets Are Stored and Restored (`docs/storage_and_data_management.md`) & Running the Study Dataset Jobs on Polaris (`docs/study_jobs.md#first-setup`).* `E-CORE_EO_Weather_Forecasting` GitHub Repository, University of Puerto Rico, Río Piedras. [https://github.com/avega17/E-CORE_EO_Weather_Forecasting/blob/main/docs/storage_and_data_management.md](https://github.com/avega17/E-CORE_EO_Weather_Forecasting/blob/main/docs/storage_and_data_management.md) and [https://github.com/avega17/E-CORE_EO_Weather_Forecasting/blob/main/docs/study_jobs.md#first-setup](https://github.com/avega17/E-CORE_EO_Weather_Forecasting/blob/main/docs/study_jobs.md#first-setup).
2. **NVIDIA Earth2Studio (2026).** *`earth2studio.io.IceChunkBackend` API Reference & I/O Backend Performance Guide (`#versioned-icechunk-io` and `#compressed-local-non-blocking-async-zarr-io`).* NVIDIA Earth2Studio Documentation. [https://nvidia.github.io/earth2studio/main/modules/generated/io/IceChunkBackend/](https://nvidia.github.io/earth2studio/main/modules/generated/io/IceChunkBackend/) and [https://nvidia.github.io/earth2studio/main/examples/07_misc/03_io_performance/?h=io#versioned-icechunk-io](https://nvidia.github.io/earth2studio/main/examples/07_misc/03_io_performance/?h=io#versioned-icechunk-io).
3. **NOAA Environmental Modeling Center (EMC) & NCEP Central Operations (NCO).** *North American Mesoscale Forecast System (NAM) Documentation (12 km Parent & 3 km Puerto Rico Nest `priconest` Grid 194) & AWS Open Data Registry (`noaa-nam`).* [https://emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/nam.php](https://emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/nam.php), [https://www.nco.ncep.noaa.gov/pmb/products/nam/](https://www.nco.ncep.noaa.gov/pmb/products/nam/), and [https://registry.opendata.aws/noaa-nam/](https://registry.opendata.aws/noaa-nam/).
4. **Pathak, J., Abbas, M. S., Harrington, P., Hu, Z., Brenowitz, N., Ravuri, S., Carpentieri, A., Leinonen, J., Adams, C., Hennigh, O., Geneva, N., Durran, D., & Pritchard, M. (2026).** *Learning Accurate Storm-Scale Evolution from Observations (StormScope).* arXiv preprint [arXiv:2601.17268](https://arxiv.org/abs/2601.17268).
5. **Laurent, C., & Harrington, P. (2026).** *Introducing the PhysicsNeMo Diffusion Module: Composable, Extensible Generative Modeling for Physics-AI.* NVIDIA PhysicsNeMo Technical Blog (June 11, 2026). [https://nvidia.github.io/physicsnemo/blog/2026/06/11/physicsnemo-diffusion/](https://nvidia.github.io/physicsnemo/blog/2026/06/11/physicsnemo-diffusion/).
6. **NVIDIA PhysicsNeMo Framework (2026).** *Regional Weather Diffusion: High-Resolution Weather Models using Generative Diffusion (StormCast & StormScope Training Recipe).* NVIDIA Docs & GitHub. [https://docs.nvidia.com/physicsnemo/latest/physicsnemo/examples/weather/regional_weather_diffusion/README.html](https://docs.nvidia.com/physicsnemo/latest/physicsnemo/examples/weather/regional_weather_diffusion/README.html) and [https://github.com/NVIDIA/physicsnemo/tree/main/examples/weather/regional_weather_diffusion](https://github.com/NVIDIA/physicsnemo/tree/main/examples/weather/regional_weather_diffusion).
7. **NVIDIA Earth2Studio (2026).** *StormScope Satellite and Radar Nowcasting Example & `StormScopeGOES` Model API.* NVIDIA Earth2Studio Documentation. [https://nvidia.github.io/earth2studio/main/examples/04_nowcasting/03_stormscope_goes_example/](https://nvidia.github.io/earth2studio/main/examples/04_nowcasting/03_stormscope_goes_example/) and [https://nvidia.github.io/earth2studio/main/modules/generated/models/px/StormScopeGOES/](https://nvidia.github.io/earth2studio/main/modules/generated/models/px/StormScopeGOES/).
8. **NVIDIA Corporation (2026).** *Earth-2 Checkpoints: `nvidia/stormscope-goes-mrms` (6 km, 3 km, and StormScope Flash Weights & Model Card).* Hugging Face Repository. [https://huggingface.co/nvidia/stormscope-goes-mrms](https://huggingface.co/nvidia/stormscope-goes-mrms).
9. **Zarr Developers (2026).** *VirtualiZarr Documentation: Frequently Asked Questions & Format Comparison (VirtualiZarr vs. Kerchunk vs. Icechunk).* Read the Docs. [https://virtualizarr.readthedocs.io/en/stable/explanation/faq.html](https://virtualizarr.readthedocs.io/en/stable/explanation/faq.html).
10. **Earthmover PBC (2026).** *Icechunk Documentation: Frequently Asked Questions, Filesystem Storage Backends, & Virtually Ingesting Existing Zarr Stores.* [https://icechunk.io/en/latest/understanding/faq/#when-should-i-use-icechunk](https://icechunk.io/en/latest/understanding/faq/#when-should-i-use-icechunk), [https://icechunk.io/en/latest/guides/storage/#filesystem-storage](https://icechunk.io/en/latest/guides/storage/#filesystem-storage), and [https://icechunk.io/en/latest/guides/zarr/#virtually-ingesting-existing-zarr-stores](https://icechunk.io/en/latest/guides/zarr/#virtually-ingesting-existing-zarr-stores).
11. **Nicholas, T. (2026).** *Old format, no problem!: Cloud-optimizing the GOES-16 archive as Virtual Zarr.* Earthmover Technical Blog (June 2, 2026). [https://earthmover.io/blog/virtual-zarr/](https://earthmover.io/blog/virtual-zarr/).
12. **Zhang, Y., Long, M., Chen, K., Xing, L., Jin, R., Jordan, M. I., & Wang, J. (2023).** *Skilful nowcasting of extreme precipitation with NowcastNet.* Nature, 619(7970), 526–532. [https://doi.org/10.1038/s41586-023-06184-4](https://doi.org/10.1038/s41586-023-06184-4).
13. **Adams, C., Ranade, R., Nidhan, S., Nabian, M. A., Akhare, D., Cherukuri, R., & Choudhry, S. (2025).** *GeoTransolver: Learning physics on irregular domains using multi-scale geometry aware physics attention transformer.* arXiv preprint [arXiv:2512.20399](https://arxiv.org/abs/2512.20399).
14. **Bernal Ayala, A. C., Gerth, J. J., Schmit, T. J., Lindstrom, S. S., & Nelson, J. P. III (2023).** *Parallax Shift in GOES ABI Data.* Journal of Operational Meteorology, 11(2), 14–23. [https://doi.org/10.15191/nwajom.2023.1102](https://doi.org/10.15191/nwajom.2023.1102).
15. **Pathak, J., Cohen, Y., Garg, P., Harrington, P., Brenowitz, N., Durran, D., Mardani, M., Vahdat, A., Xu, S., Kashinath, K., & Pritchard, M. (2024).** *Kilometer-Scale Convection Allowing Model Emulation using Generative Diffusion Modeling (StormCast).* arXiv preprint [arXiv:2408.10958](https://arxiv.org/abs/2408.10958).
16. **Mardani, M., Brenowitz, N., Cohen, Y., Pathak, J., Chen, C.-Y., Liu, C.-C., Vahdat, A., Nabian, M. A., Ge, T., Subramaniam, A., Kashinath, K., Kautz, J., & Pritchard, M. (2024).** *Residual Corrective Diffusion Modeling for Km-scale Atmospheric Downscaling (CorrDiff).* arXiv preprint [arXiv:2309.15214](https://arxiv.org/abs/2309.15214).
17. **NVIDIA Earth2Studio GitHub Community (2026).** *Issue #1118: StormScope 3km_10min GOES Eclipse NaN Propagation & Discussion #820: StormScope MRMS Clear-Air (-10 dBZ) Preprocessing Contract.* [https://github.com/NVIDIA/earth2studio/issues/1118](https://github.com/NVIDIA/earth2studio/issues/1118) and [https://github.com/NVIDIA/earth2studio/discussions/820](https://github.com/NVIDIA/earth2studio/discussions/820).
