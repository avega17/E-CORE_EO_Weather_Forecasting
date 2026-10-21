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
MRMS shell wrapper. The Polaris PBS wrapper and authenticated pilot procedure are documented there. Reuse one command per source across wall-time-limited resumptions.

## Where changes belong

| Area | Modules |
|---|---|
| NOAA discovery and native reads | `mrms`, `goes`, `goes_native`, `goes_staging` |
| Earth2Studio-compatible source and storage | `earth2_sources`, `earth2_io`, `monthly`, `monthly_stream` |
| Concurrent GOES scheduling | `goes_shared`, `goes_rollout`, `jobs_goes` |
| MRMS jobs | `jobs_mrms`, `mrms_profile`, `mrms_pipeline`, `mrms_tuning` |
| Operational records and index | `runlog`, `index`, `job_status` |
| Notebook controls and CLI | `ui`, `cli`, `viewer`, `report_ui`, `report_cli` |
| Quality/reporting | `diagnostics`, `dataset_report`, `report_views`, `mrms_radar_figures` |
| HF backup/restore | `jobs_backup`, `hf_storage`, `view_backup` |

Keep scripts thin: `scripts/dataset_jobs.py` for operations and
`scripts/dataset_report.py` for diagnostics/exports. Avoid new one-off watchers.
The report script also exposes `radar-figure`; its annual full-CARIB source
read should follow the one-day pilot and run only while GOES is paused. It
stores derived counts and figure files, never another raw GRIB archive. See
[the MRMS coverage guide](mrms_roi_and_radar_coverage.md).
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

## Polaris implementation and publication

Use `jobs/environment-polaris.yml` for CPU ingestion and the versioned MRMS
profiles in `jobs/`. Profile values are defaults; explicit CLI resource options
override them. A profile is not part of scientific archive identity. Test exact
values, bitmap preservation, queue bounds and source-month boundary matching.

Push reviewed changes so they are available on Polaris, build the environment
once, and avoid editing files used by a running job. PBS jobs do not inspect Git
commits, require a clean checkout or compare code hashes. Code-lineage inspection
is performed only when explicitly requested by the developer and must never
block a job. Optional `ECORE_RECORD_CODE_IDENTITY=1` recording is best effort;
leave it unset for normal use. Scientific selection and archive-integrity
checks remain required. See the setup and submission commands in
[study jobs](study_jobs.md#polaris-pbs).


## PBS date handling and GOES portability

The GOES runner accepts explicit `--start`/`--end` UTC bounds; combining these
with a specific half-year phase is rejected. It clips both boundary months and
checks interval, bands and ROI before reusing a run checkpoint. Archive identity
still depends on native scientific data, not the machine profile.

`jobs/polaris_goes.pbs` uses the initial profile in `jobs/goes_polaris.json`;
`jobs/polaris_mrms.pbs` retains the measured radar profile and requests six hours.
Both wrappers take exported ISO fetch-date variables. Neither automatically
exports credentials from `.env`. Setup and tuning remain date-independent.
Source `jobs/polaris.env.sh` in each Polaris terminal for fixed non-sensitive
settings and an automatically refreshed project-quota estimate. Missing or
ambiguous quota rows clear stale values and fail with a useful explanation.
The parser runs with standard-library Python before the CPU environment exists.
The GOES-only `check-noaa` helper uses production anonymous discovery for at
most one hour, with no imagery reads. Follow local wrapper checks with that
listing check on a compute node, then use pilot mode for actual downloads and
archive validation. A full day is a later bounded test, not a dry run.
The startup correction also passed 117 local regression checks (one mock-only
case skipped). Both wrappers report entry, failing phase and final exit status;
one small source/mode `startup.log` is overwritten for each attempt.
[Manual testing instructions](study_jobs.md#polaris-goes-manual-pilot-and-20212022-study)
cover the four-context GOES pilot, progress snapshots, quotas and resubmission.

GOES storage reservations are a disposable node-local resource ledger. Scientific
provenance stays in native archives; the shared database copy is always closed.
The publication guard checks actual packed candidate bytes before copying, and
progress snapshots serialize coordinator/heartbeat writes to the same path.
A storage/admission pause or application deadline preserves verified archives;
never relabel an unverified partial store complete.

The original preparation passed 178 relevant local checks (source/metadata equality,
archive read-back, scheduler, interruptions, status, dates and PBS mocks), plus
shell syntax and Python compilation. This is fixture-based workstation evidence,
not a completed live Polaris GOES pilot. A short repeatable subset is:

```bash
python -m pytest -q tests/test_polaris_goes.py tests/test_goes_shared.py \
  tests/test_jobs.py tests/test_mrms_profiles.py
bash -n jobs/polaris_goes.pbs jobs/polaris_mrms.pbs
```
