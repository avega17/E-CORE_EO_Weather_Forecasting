# NOAA radar and satellite subsets for Puerto Rico

> GOES uses two normal month writers plus one C02-only tail, C02 range readers and async
> downloads for the other seven bands after the admitted October 2021–January 2022
> months drain. See [current operating settings](docs/goes_parallel_pipeline.md).


This project prepares reproducible raw subsets from NOAA MRMS radar and GOES
satellite archives for weather-forecasting research. The notebooks use custom
Earth2Studio-compatible sources to retain the Puerto Rico region, requested
products and bands, and native source metadata. They save validated, compressed
monthly Zarr archives and keep analysis steps separate from the raw data.

| Notebook | Description |
| --- | --- |
| [MRMS radar](notebooks/01_mrms.ipynb) · [Python source](notebooks/01_mrms.py) | Select CARIB radar products, archive raw monthly subsets, and inspect coverage and missing-value codes. |
| [GOES imagery](notebooks/02_goes.ipynb) · [Python source](notebooks/02_goes.py) | Select full-disk scans and ABI bands, archive each band on its native grid, and inspect data quality. |
| [Dataset explorer](notebooks/03_explore_datasets.ipynb) · [Python source](notebooks/03_explore_datasets.py) | Browse monthly archives, prepare one month from an HF yearly backup, compare storage sizes, and preview samples or sequences on a portable map that works in VS Code. |
| [Dataset report](notebooks/04_dataset_report.ipynb) · [Python source](notebooks/04_dataset_report.py) | Audit raw quality, compare storage and I/O, and review future model preparation. |

## Set up

```bash
conda env create -f environment.yml
conda activate ecore-weather
python -m ipykernel install --user --name ecore-weather --display-name 'E-CORE weather'
```

Set `HF_BUCKET_NAME=namespace/bucket`, Hub token credentials, and the separate HF
S3 gateway keys in an untracked `.env` to use the supported remote destination.
`HF_DATASET_REPO` remains a compatibility alias for the bucket name. NOAA source
reads are anonymous. Use a local path in the **Save to** control or
`--destination PATH` for local archives. Read [storage and bucket access](docs/storage_and_data_management.md)
before starting a large upload.

MRMS defaults to four fields: precipitation rate, composite reflectivity, and
low-level azimuthal shear sampled at ten-minute slots, plus multisensor Pass2
QPE at hourly cadence. Other MRMS products remain selectable. GOES defaults to
every available scan and eight StormScope example channels: C01, C02, C03, C07,
C08, C09, C10, and C13. Both keep actual observation times and report gaps.
GOES defaults to native-band CMIPF: C02 at nominal 0.5 km, C01/C03 at 1 km,
and selected infrared bands at 2 km. Each band has a separate monthly ZIP.
Existing MCMIPF and hybrid months remain distinct historical products and are
not converted for this study. See [native rollout](docs/storage_and_data_management.md).

Each source, product, native ROI/grid, and UTC month has one compressed
Zarr archive. MCMIPF keeps the selected bands as groups inside one monthly ZIP;
CMIPF remains a per-band archive. A repeated fetch merges additional dates into
that month. GOES uses two normal month owners plus one C02-only tail, with
global download/read budgets; MRMS keeps four product-month writers. See the
[measured pipeline settings](docs/goes_parallel_pipeline.md). HF writing uses one
Earth2Studio async coordinator and verifies remote arrays before completion.
Raw values and product metadata are preserved. Cleaning,
interpolation, calibration decoding, and display reprojection remain session-only
operations. MRMS and CMIPF local archives use Earth2Studio's `ZarrBackend`;
the MCMIPF grouped ZIP uses Zarr v3 arrays that the project's
Earth2Studio-compatible monthly source adapter reopens by band. Direct HF
MCMIPF writing is not part of this local-DAS refactor.

## Use notebooks or scripts

Open the paired notebooks in Jupyter or VS Code and inspect the selection before fetching.
The Python sources also accept argparse options for batch use:

```bash
python notebooks/01_mrms.py --operation inspect --start 2023-01-01 --end 2023-02-01
python notebooks/01_mrms.py --operation fetch --start 2023-01-01 --end 2023-02-01 \
  --product precipitation-rate composite-reflectivity \
  --destination /mnt/p/ecore_eo_datasets --workers 8 --monthly-writers 2
python notebooks/02_goes.py --operation fetch --start 2025-09-01 --end 2025-10-01 \
  --bands 1 2 3 7 8 9 10 13 --destination /mnt/p/ecore_eo_datasets
python notebooks/02_goes.py --operation estimate --output results/study-goes-estimate
python scripts/dataset_jobs.py fetch mrms --destination /mnt/p/ecore_eo_datasets
```

MRMS notebook download pools default to half the detected CPU count; long
study runs use explicit limits. GOES uses the global profile in jobs/goes_workstation.json. Decoding can be bounded separately. See [notebook usage and outputs](docs/notebooks.md) and the
[developer guide](docs/developer_guide.md) for more examples.

## Colab

[Open MRMS in Colab](https://colab.research.google.com/github/avega17/E-CORE_EO_Radar_GFMs/blob/main/notebooks/01_mrms.ipynb),
[GOES in Colab](https://colab.research.google.com/github/avega17/E-CORE_EO_Radar_GFMs/blob/main/notebooks/02_goes.ipynb), or
[the dataset explorer in Colab](https://colab.research.google.com/github/avega17/E-CORE_EO_Radar_GFMs/blob/main/notebooks/03_explore_datasets.ipynb)
after those files are pushed. Setup detects Colab, clones the chosen revision,
and installs the notebook dependencies. Never store credentials in a notebook.

## Guides

- [Notebook usage and expected outputs](docs/notebooks.md)
- [Clean-machine setup and developer recovery guide](docs/developer_guide.md)
- [Launch, monitor, pause and resume commands](docs/study_jobs.md)
- [Current live status](docs/status.md)
- [Storage choices, measured tradeoffs, archive layout and HF access](docs/storage_and_data_management.md)
- [GOES/MRMS concurrency and verified archive runtime](docs/goes_parallel_pipeline.md)
- [Diagnostics and report exports](docs/dataset_report.md)
- [GOES acquisition ROI and future training context](docs/goes_roi_training_study.md)
- [MRMS ROI and quality-aware radar coverage](docs/mrms_roi_and_radar_coverage.md)
- [Mentor implementation review](docs/original_code_review.md)
- [StormScope](docs/StormScope-paper-notes.md) and [CorrDiff](docs/CorrDiff-paper-notes.md) research notes
- [Current plan](docs/development_plan.md), [documentation consolidation](docs/documentation_review.md) and [agent instructions](AGENTS.md)

Use `scripts/dataset_jobs.py` for operational jobs and `scripts/dataset_report.py`
for research reports. The portable MRMS wrapper is `jobs/mrms_study.sh`; configure
its environment from `jobs/mrms_study.env.example`. Scheduler-specific Argonne
submission is the next planning phase. Keep only current study records and live
scratch in `results/`; verified research data lives in the chosen archive root.

A brief MRMS comparison with installed NVIDIA readers is available through
`python scripts/dataset_jobs.py benchmark mrms` during a verified study pause.
It reports the default CONUS workload separately from matched CARIB source reads.
[Measured local backend results and comparison limits](docs/goes_parallel_pipeline.md#comparing-with-installed-nvidia-mrms)
explain what has passed and what remains queued.
