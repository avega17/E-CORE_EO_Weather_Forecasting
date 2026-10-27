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

For local GOES, `--max-hours 0 --backup-after-complete` keeps the shared study
running until completion or an explicit/safety pause, then packages and verifies
one HF annual backup at a time. It does not start backups on exit 75 or failure.
Use the workstation profile for its 8 GiB ROI budget, 64 GiB disk staging and
available-RAM safeguards; the Polaris profile is separate. See [annual backup
commands](study_jobs.md#local-fetch-followed-by-annual-hf-backup). Future GOES
restoration into Polaris scratch is optional future transfer/cache functionality;
the expanded Eagle quota removes the original raw-study storage constraint.
Eagle is the intended long-term home for datasets and model weights. Storage for
preprocessed arrays, validation datasets and model assets remains unestimated.

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
| HF backup/restore | `goes_backup`, `jobs_backup`, `hf_storage`, `view_backup` (MRMS restoration) |

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

<a id="automatic-source-error-recovery-next-change"></a>
### Automatic source-error recovery

The shared native-CMIPF pipeline handles recognized source failures separately
from failures that could damage an archive. It does not need a list of known bad
object keys. Normal successful reads keep their existing format and access path.

- HTTP responses are checked against the selected ETag, exposed object size and
  requested byte range. The checks use SDK metadata already returned by the GET.
- C02 range workers allow three failed HTTP/HDF operations per source. Cache and
  file handles are reopened for HDF retries. If these attempts fail, one diagnostic
  whole-file download goes through the shared anonymous download service, using
  the configured ALCF proxy and existing concurrency, disk and ROI limits.
- A successful full-file read supplies the same packed native subset. If local
  decoding instead confirms invalid format or a recognized integrity defect,
  the complete size and supported single/multipart ETag MD5 must match before
  the observation can become an explicit source gap. An HTML page with an
  unsupported or mismatched checksum is not enough evidence.
- All seven staged bands use that same full-object verification on a decoding
  failure. Normal staged downloads retain their existing three-attempt limit.
  Unexplained source failures receive one further band attempt. If still
  unresolved, that band closes and publishes its verified prefix; other bands
  and months continue. The final status is deferred/paused (75), not complete.
- Three independently deferred band-months request a node pause because a wider
  service or environment problem is likely. Archive-write/ownership/hash errors,
  invalid requests, reader-process death and memory protection retain their
  existing fatal or checkpointed-pause behavior. They are not converted to gaps.

A C02 task reserves the disk space it might need for fallback before it takes
ROI credits. This prevents a failed first observation from holding all ROI
memory while waiting for other staged files to retire. The reservation is
returned even on failure. Before a band retries, old downloads are cancelled,
readers finish, and the broker acknowledges released reservations. Separate
band stores remain owned by the same month process.

Source evidence is saved atomically by the coordinator in the selected STAC
manifest, and copied into native archive attributes and completion records.
Raw arrays omit only the confirmed unavailable observation: no synthetic zeros,
interpolation or changed quality flags are introduced. Unresolved reads remain
pending and prevent that month from authorizing a completion marker or backup.
The status table retains `deferred`, with the exception and source identity in
its compact task record. Notebook results and CLI exit 75 distinguish pending
work from completed data.

#### Reusing existing Eagle checkpoints

Checkpoint schema 2 stores the original requested source identity separately
from available observations and source-gap evidence. Existing schema-1 parts
remain readable. If a new gap lies after the verified prefix, their immutable
ZIPs and pixel positions are reused; only unwritten array capacity and future
timestamps change. A gap that would move a verified pixel stops for inspection.
Successive new gaps and fresh-allocation restores are covered by local tests.
Receipt evidence is recovered before constructing the new observation mapping,
so a resubmission does not have to rediscover a confirmed defect. Failed reads
force publication of the latest verified eight-scan boundary, rather than
waiting for the next 25% interval. Monthly ZIPs still require final read-back.

Keep the same destination and shared checkpoint root when resubmitting. See
[the final single-node command](study_jobs.md#single-node-recovery-for-september-2024-through-april-2025).
Previously recorded scientific facts in `jobs/goes_source_issues.json` remain
historical evidence; adding new hardcoded exceptions is not part of this workflow.

A resumed band first reads its saved arrays and checks their recorded checksums.
It can therefore show an unchanged observation count and no new download bytes
while doing useful local recovery work. A large C02 checkpoint takes longer
because its native pixels are finer. Other bands may already be fetching while
C02 is still validating. Preserve the scratch directory; use the live heartbeat,
process activity and subsequent checkpoint updates to assess recovery.

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

Coordinator heartbeat/progress queries use `jobs.status(..., coordinator=True)`:
they share the writable DuckDB configuration and writer lock with task updates
and closed database copies. External CLI/viewer calls keep read-only access and
fallback behavior. Mixing RO/RW configurations inside one process caused the
Polaris GOES 7765036 failure; retrying only external lock errors cannot fix it.
Regression checks force overlapping connections, task writes and snapshots.

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

GOES study jobs now use three normal month owners plus one C02-only tail on
Polaris. Global download/read limits remain unchanged; workstation defaults are
separate. The PBS wrapper enables `--checkpoint-dir` with
`--checkpoint-fraction .25` for incremental recovery files on Eagle. Restoring
them verifies file hashes and source-array checksums before new NOAA reads.
They assume the current unsharded one-frame chunks and never claim monthly
completion. Do not reuse this mechanism for partial shards without revising
and testing the checkpoint format. See [resubmission](study_jobs.md#capacity-walltime-and-resubmission)
for the updated deadline policy and compact pause records.

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
[Manual testing instructions](study_jobs.md#polaris-goes-pilot-and-full-study-fetching)
cover the four-context GOES pilot, progress snapshots, quotas and resubmission.
The default GOES interval now extends through June 2026, with no artificial
dataset cap. Explicit exported dates still override defaults, so reset an older
`PBS_GOES_END` when extending a resumed study. An optional positive dataset cap
can be set through `ECORE_GOES_STORAGE_CAP_GB`; pass its name explicitly with
`qsub -v` only when it is set. Actual quota and scratch checks remain required.
The [three-node extension](study_jobs.md#three-node-goes-fetching)
keeps existing readers/writers, isolates state per date partition, and merges
closed node databases only after fetching stops. `jobs_multinode` and
`index_merge` provide the orchestration and consolidation without changing the
active workstation pipeline. The multi-node PBS wrapper starts three independent
one-node MPI applications; each invokes the existing wrapper with node-only setup,
so it never starts nested MPI. Local fixture tests do not establish Polaris
placement, external access or a production speedup.

Run the focused orchestration checks with:

```bash
python -m pytest -q tests/test_goes_multinode.py tests/test_polaris_goes.py
bash -n jobs/polaris_goes_multinode.pbs jobs/polaris_goes.pbs
```

Each node's writable DB stays on SSD. Closed Eagle snapshots are merger inputs,
not databases to open for writing. Consolidation uses current completion markers
and physical ZIP sizes; it does not repeat the original array/hash read-back of
every archive. The merge leaves inputs intact on conflict or missing snapshots.
Keep the small confirmed source-gap file in `jobs/` with the code: ignored
workstation STAC selections do not otherwise travel to Polaris.

The October 24 implementation passed 196 local checks (one skipped), including
native/source metadata, checkpoint recovery, independent process exits, index
consolidation and PBS mocks. Shell syntax and compilation also passed. This
does not substitute for the manual multi-node pilot or measure shared-proxy
scaling on Polaris.

GOES storage reservations are a disposable node-local resource ledger. Scientific
provenance stays in native archives; the shared database copy is always closed.
`goes_memory` separates production memory-pressure pauses from strict benchmark
ceilings. Summed RSS is not unique RAM: shared pages can be counted more than
once. The Polaris study allows 384 GiB, warns at 256 GiB, and samples node
availability alongside per-process RSS every five seconds. Pressure normally
has a 60-second grace period; critically low available RAM bypasses that delay.
The atomic `memory.json` is one bounded snapshot, not per-object logging.
Do not relabel RSS-based benchmark measurements as unique RAM or GPU VRAM.
Status aggregate bandwidth uses current-attempt byte counters over shared
elapsed time. Recent average/median rates use a bounded local monitor cache
and fresh snapshot timestamps, including for `--progress`; do not sum per-band
rates or invent rates from repeated cached snapshots.
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
