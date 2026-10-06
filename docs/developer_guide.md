# Developer and new-machine guide

Read [AGENTS.md](../AGENTS.md), [storage](storage_and_data_management.md) and [study jobs](study_jobs.md)
before changing the scientific archive contract. The current environment uses
Earth2Studio 0.18.0; its stock GOES source reads full-domain MCMIPF. Our compatible
sources preserve native-band CMIPF crops and CARIB MRMS products instead.
Compatibility with the API does not establish compatibility with a released model.

## Set up a clean machine

```bash
conda env create -f environment.yml
conda activate ecore-weather
python -m ipykernel install --user --name ecore-weather --display-name 'E-CORE weather'
python notebooks/01_mrms.py --help
python notebooks/02_goes.py --help
```

Use conda-forge for compiled geospatial dependencies and explicit pip entries
for pip-only packages. GPU/model training dependencies stay outside this CPU data
environment. Set destination, scratch and index paths for the machine. On WSL,
verify P: is mounted at `/mnt/p` before a long job; a writable directory alone
is not evidence that the intended disk is mounted. Keep scratch/index Linux-local.
The job performs mount, write, free-space and writer preflight checks.

Start with an inspect operation (listing only), then a short fetch:

```bash
python notebooks/02_goes.py --operation inspect --start 2022-09-18 --end 2022-09-19 --satellite 16
python notebooks/02_goes.py --operation fetch --start 2022-09-18T12:00:00Z \
  --end 2022-09-18T12:20:00Z --satellite 16 --bands 13 \
  --destination /path/to/archives --scratch /path/to/linux-scratch
```

Dates are UTC; end is excluded. Tiny fetches still write verified monthly stores.
Do not run another workstation fetch while the production study is active.
[Study jobs](study_jobs.md) gives resumable full-period commands and an independent
MRMS shell wrapper. Scheduler directives await the actual Argonne system/network
policy. Reuse one command per source across wall-time-limited resumptions.

## Where changes belong

| Area | Modules |
|---|---|
| NOAA discovery and native reads | `mrms`, `goes`, `goes_native`, `goes_staging` |
| Earth2Studio-compatible source and storage | `earth2_sources`, `earth2_io`, `monthly`, `monthly_stream` |
| Concurrent GOES scheduling | `goes_shared`, `goes_rollout`, `jobs_goes` |
| MRMS jobs | `jobs_mrms` |
| Operational records and index | `runlog`, `index`, `job_status` |
| Notebook controls and CLI | `ui`, `cli`, `viewer`, `report_ui`, `report_cli` |
| Quality/reporting | `diagnostics`, `dataset_report`, `report_views` |
| HF backup/restore | `jobs_backup`, `hf_storage`, `view_backup` |

Keep scripts thin: `scripts/dataset_jobs.py` for operations and
`scripts/dataset_report.py` for diagnostics/exports. Avoid new one-off watchers.
Use the supported verified-pause handoff to change worker settings; never edit a
running snapshot. GOES global limits do not multiply by band count. MRMS retains
four product-month writers, eight downloads each and one decode slot each.

## Notebooks and checks

Author `.py` percent-format sources and regenerate clean `.ipynb` partners:

```bash
python -m jupytext --to ipynb notebooks/02_goes.py
python -m pytest tests/test_goes_shared.py tests/test_job_status.py tests/test_jobs.py -q
```

Choose tests for the changed behavior. Validate source values, coordinates,
packed calibration/DQF or GRIB bitmap/sentinels, interruption/reuse, and exclusive
store ownership. No interpolation or cleaning may enter ingestion. Verify a small
local round trip before a long run; remote HF tests are separate evidence.

In Colab, detect the runtime, clone the configurable revision and install notebook
dependencies. Display the resolved revision. Push modified imported modules before
validating them through a clone; do not call a different revision local validation.
Restart a notebook kernel after package changes and rerun setup/control cells.

## Recovery decisions

| Situation | Next step |
|---|---|
| PID alive and committed counts advancing | Leave the job running; rates can vary across bands |
| Heartbeat advances but counts do not | Inspect the current bounded run log and task Updated timestamps; distinguish retry/finalization from a true stall |
| Index is briefly locked | Retry status/Find; use completed-manifest fallback; do not change scientific data |
| Process absent but status says running | Treat it as interrupted; verify checkpoints and use the recorded resume command |
| Partial monthly build | Keep its deterministic scratch and saved selection; resume verifies completed batches |
| `complete.json` plus matching archive hash | Reuse the archive; do not download it again |
| Intentional exit 75 | Verified resumable pause, not study completion |
| Other nonzero exit | Retain current failure details and inspect before retry; never relabel it successful |

Update current guides and a compact run summary rather than adding a new dated
log document. Keep credentials out of all artifacts and notebook outputs.

The index writer retries brief external DuckDB read-lock conflicts for up to
30 seconds; notebook/SQL readers should close their connections promptly.
A persistent read lock still fails clearly after that window. Network MRMS
reference comparisons must run in a verified fetch pause; use the guarded
[benchmark command](study_jobs.md#brief-mrms-source-comparison), not a separate
one-off script.

After a crash, use the recovery sequence in [study jobs](study_jobs.md#recovering-after-a-workstation-restart).
Keep partial native-band stores: scan-batch verification can replay a damaged
tail while retaining the verified prefix. A stale launcher PID is not live work.
