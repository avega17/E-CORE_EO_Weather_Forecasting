# Study jobs: launch, monitor, pause and resume

Use the `ecore-weather` environment from the repository root. Dates are UTC and
end-exclusive. The study covers 2021-01-01 through 2026-06-30; use end
`2026-07-01`. The ROI is `[-70.24, 14.36, -62.56, 22.04]`. “768” refers to the
mentor's pixel grid, not square kilometres.

## GOES

Default native CMIPF, all available scans, eight bands C01/C02/C03/C07/C08/C09/C10/C13.
Selected operating profile: **two normal month writers plus one C02-only tail writer**,
32 global async downloads, two local readers, eight C02 range readers, 8 GiB ROI
memory and 64 GiB source staging on Linux disk. [Pipeline guide](goes_parallel_pipeline.md)
explains which steps overlap. These settings preserve separate native band ZIPs.

```bash
python scripts/dataset_jobs.py resume goes --pipeline shared --phase all \
  --destination /mnt/p/ecore_eo_datasets --output results/study-goes-native \
  --scratch /path/to/linux-scratch --shared-profile jobs/goes_workstation.json \
  --max-hours 0 --backup-after-complete \
  --backup-work-dir /path/to/linux-yearly-work
watch -n 30 -t 'python scripts/dataset_jobs.py status goes --start 2021-10-01'
python scripts/dataset_jobs.py pause goes --output results/study-goes-native
```

Do not start a second coordinator. A supported pause stops new month admissions
and drains every already admitted month to verified completion. Exit 75 is an
intentional pause. Resume reuses matching completed months and verified scan batches.

`--max-hours 0` removes the time cutoff for a local shared-pipeline job. It is
rejected inside PBS, where the application must stop within its allocation.
The command above covers 2021-01-01 through 2026-07-01, end excluded. Completed
CMIPF months are checked when their chronological turn is reached; January and
February 2026 are reused even if earlier years still need fetching. Keep the
same scratch directory when resuming verified batches. Do not use RAM-backed
`/tmp` for this long-running job or for annual working ZIPs.

### Local fetch followed by annual HF backup

`--backup-after-complete` waits for the **entire study** before it contacts HF
for packaging/upload. A pause, failure, clipped interval or stop-after request
cannot authorize annual backups. It verifies all 66 monthly checkpoints and
their native-band archive records before admitting the first year. Missing
NOAA observations remain documented gaps; they never become synthetic pixels.

Each annual bundle contains all eight bands and any GOES-16/19 satellite paths
needed that year. January–June is the final 2026 bundle. One year is packaged,
uploaded and read back at a time; monthly ZIPs are already compressed and are
copied into the outer ZIP without recompression. HF transfers use one publisher
and bounded multipart requests. Monthly research archives are never deleted.

The normal status table also reports annual backup phase, year and transferred
bytes. Compact receipts and one current backup progress record live under
`results/study-goes-native/backups/`; large working files live at
`--backup-work-dir` (default `~/.cache/ecore-weather/goes-yearly`). Successful
annual work ZIPs are removed after remote hash verification. Failed work remains
available for retry:

```bash
python scripts/dataset_jobs.py backup goes --year 2023 \
  --source-root /mnt/p/ecore_eo_datasets \
  --study-output results/study-goes-native \
  --work-dir /path/to/linux-yearly-work
```

Omit `--year` to check/retry every year. Both forms require full-study completion.
A verified remote receipt is checked again without rebuilding its annual ZIP;
a failed upload reuses the retained, verified working bundle.

To have a live GOES coordinator stop after a particular month, arm the
month-checkpoint controller with that run's identifier:

```bash
python scripts/dataset_jobs.py stop-after goes --month 2022-12 \
  --run-id <active-run-id> --output results/study-goes-native
```

The controller waits until the checkpoint says the month is complete and all
eight native-band archives have their completion records and verified sizes.
It then creates the same `pause.request` used by a manual safe pause. The
coordinator finishes any months it had already admitted before exiting with
status 75. As a result, the final completed month can be later than the target
when other month writers were already working. The controller's
`stop-after-YYYY-MM.json` record shows whether it is waiting, has requested the
pause, or has observed the final paused receipt. This is useful when the
coordinator was launched without its startup-only `--stop-after-month` option.
Choose the same scratch path as the recorded run when recovering partial work.

To change resource settings after a verified drain:

```bash
python scripts/dataset_jobs.py handoff goes --wait-for-pause \
  --profile jobs/goes_workstation.json --output results/study-goes-native \
  --scratch results/study-scratch
```

The waiting handoff pins its replacement code/configuration immediately so later
workspace edits cannot change it. After the pause receipt, mount, capacity and
completed checkpoints verify, it launches the earliest unfinished chronological month.
It does not rerun benchmarks. Half-year stages advance automatically, switching
GOES-East satellites according to observed operational availability. The current
active months and progress are shown by the status command above; do not infer
the live run from an old launch description. The configured profile uses two
normal month owners and one additional C02-only tail.

## MRMS and a portable batch wrapper

Four products: precipitation rate, composite reflectivity, low-level azimuthal
shear at ten-minute slots, and multisensor Pass2 QPE at hourly cadence. The newest
observation at/before a slot may be selected within five minutes, without reuse.

```bash
python scripts/dataset_jobs.py fetch mrms --phase all \
  --start 2021-01-01 --end 2026-07-01 \
  --destination /path/to/archives --scratch /path/to/linux-scratch \
  --monthly-writers 4 --workers 8 --decode-workers 1 --max-hours 12
python scripts/dataset_jobs.py status mrms
```

For a fresh compute environment, copy `jobs/mrms_study.env.example`, set its
paths and source it; run `bash jobs/mrms_study.sh`. Set `ECORE_PYTHON` to the
Conda interpreter if needed. `ECORE_MAX_HOURS` controls the application time budget;
the [Polaris PBS procedure](#polaris-pbs) uses the same runner with a dedicated
reflectivity collection and machine profile. Keep the live index on node-local/Linux storage. A
`--durable-state` location can receive closed snapshots at supported checkpoints.
Do not copy a live DuckDB WAL/database as a backup.

## Reading status

Started/Updated are UTC timestamps. Started is the latest recorded band/task
attempt, not the NOAA observation date. Updated is the coordinator's latest
record for that task; it is not proof that the observation count increased.
Waiting means retained work has not yet been admitted in the current run.
Complete means the monthly archive verified. Missing observations remain separate.

Average Mb/s is returned bytes divided by recorded elapsed writer time, including
crop/write/verification waits. It is not a NIC meter or a sum of simultaneous
request speeds. If only a monitoring window is available, rates use counter
changes since monitoring began. Date filters select entire intersecting months.
Counts may lag by 30 seconds. Status uses the typed index and a compact cache;
completed manifests are a fallback during locks. No ephemeral JSON paths need
manual updates.

## Minimal records

`results/study-mrms` and `results/study-goes-native` hold compact configuration and
month checkpoints. Current snapshots/run records identify exact code/environment.
The archive index is rebuildable. All source provenance belongs with canonical
archives; `results/` is not a second copy of the dataset. Keep only the current
bounded error log and necessary in-progress scratch. [Storage](storage_and_data_management.md)
describes backup receipts and the explicit current-study cleanup command.

## Brief MRMS source comparison

After a verified study pause, with no automatic continuation launching:

```bash
python scripts/dataset_jobs.py benchmark mrms --frames-per-month 8 --repeats 3 \
  --budget-mib 128 --output results/study-mrms/source-benchmark.json
```

This guarded command compares identical CARIB composite-reflectivity objects
with NVIDIA's region-adapted reader, and reports a default CONUS call separately.
It does not write research archives. See [comparison scope and completed local
backend results](goes_parallel_pipeline.md#comparing-with-installed-nvidia-mrms).
The October 6 comparison is complete: 2.979 s versus 3.247 s median for
NVIDIA/project source reads, respectively. Values and times matched; the small
coordinate-representation difference is recorded. Keep the current MRMS profile.
The full measurement scope is in the linked guide.

The handoff can schedule this comparison in the gap between a verified drain
and the next GOES run:

```bash
python scripts/dataset_jobs.py handoff goes --wait-for-pause \
  --mrms-source-benchmark --profile jobs/goes_workstation.json \
  --output results/study-goes-native --scratch results/study-scratch
```

It pins the comparison code and replacement profile together. A failed benchmark
stops this handoff rather than claiming a successful measurement or starting
another fetch. Inspect the compact MRMS benchmark report before retrying.

Progress/index writes now retry a brief external DuckDB read lock for up to
30 seconds. Reader searches still return quickly and use manifest fallback when
blocked by the coordinator. Close manual read-only connections promptly; a
persistent reader can still prevent writes beyond the retry window.

Resumed tasks now start their new-attempt network counter at zero until the
source reader registers. Old cumulative bytes are retained separately; they
are never divided by a few seconds of new-attempt elapsed time. Existing pinned
runs keep their original code, so this correction applies to the next snapshot.

## Recovering after a workstation restart

A crash is an interrupted run, not a verified month-boundary pause. First confirm
no old coordinator or waiting launcher survives, verify P: is mounted and writable,
and inspect durable completed checkpoints and deterministic scratch batches.
The reader rechecks ordered source identities and raw-array batch checksums on
resume; any untrusted tail is preserved for diagnosis and replayed. Do not remove
scratch or use an old “running” launcher label as evidence of active work.

Use a newly pinned code snapshot and `resume goes --phase all --begin-month
YYYY-MM --pipeline shared --shared-profile /pinned/jobs/goes_workstation.json`,
with the normal destination, scratch and Linux-local index paths. The starting
month must be the earliest unfinished month established by the checkpoints.
Existing later partial months are retained and reused when admitted. The
October 6 recovery starts in October 2021 after July–September archive checks.
The launch record contains the exact command, snapshot hash and selected limits.
Confirm advancing committed counts before declaring the job active.

Completed archives are hash-checked as chronological admission reaches their
months. The coordinator no longer scans every future completed ZIP before
starting the earliest unfinished month; this removes unnecessary startup delay
without changing verified reuse.

## Polaris PBS

The Polaris collection is deliberately limited to **composite and base
reflectivity**, both sampled on ten-minute causal slots, for 2021-01-01 through
2026-07-01 (end excluded). StormScope's “Max. Comp. Refl.” is composite
reflectivity, not a third field. The workstation's four-product collection
remains available through its normal defaults.

### Profiles and actual concurrency

`--profile jobs/mrms_workstation.json` or `--profile jobs/mrms_polaris.json`
loads versioned resource settings. Built-in defaults are followed by profile
values and then explicit CLI overrides. Widgets have a Load profile button;
changing a control afterwards overrides that value. Runs record resolved
limits, the profile hash, actual CPU affinity, memory and package versions.

| Setting | Workstation | Polaris starting candidate | Polaris selected |
|---|---:|---:|---:|
| Product-month writer processes | 4 | 8 | 12 |
| Download threads per writer | 8 | 8 | 4 |
| Decode slots per writer | 1 | 1 | 1 |
| Active plus queued observations per writer | 8 | 16 | 16 |
| Reserved ROI payload per writer | 128 MiB | 256 MiB | 256 MiB |
| Total process RSS ceiling | 32 GiB | 64 GiB | 64 GiB |

The selected Polaris profile has 48 download threads in total and twelve
independent GRIB decoders. The initial candidate had 64 download threads and
eight decoders. In this experiment, more independent product-month processes
helped, while increasing downloads per writer did not. These are different
limits: download threads can wait on the network while each process permits
only one GRIB decode at a time. Numerical-library threads are capped to one.

Compressed gzip and expanded GRIB buffers, full-grid decoding and returned
ROI arrays have different memory costs. The ROI queue limit is not a total
memory estimate: it reserves room for ordered subset results, while the RSS
ceiling covers all job processes. Temporary storage reported at packing counts
the closed Zarr files, ZIP and checkpoint metadata. It is not a whole-job disk
peak and excludes filesystem allocation overhead.

The scheduler admits independent **source-month** stores across adjacent months.
For example, a February 00:00 slot may use January 23:58 data. That observation
belongs to January's store, so it is assigned to the January owner rather than
making a February worker contend for the same ZIP. Requested and actual times
remain recorded, and unmatched slots are counted once. A study starting at
midnight can therefore have a small preceding-month boundary archive.

For a new system or a new data workload, screen 4/8/12
writers, then 4/8/16 download threads near the best result. Test a second decode
slot only when decode waiting is material. Compare larger queues only when
phase waiting indicates a benefit. The supported tuning command uses identical
CARIB objects for both variables in six 2022 monthly contexts, a cumulative
10 GiB returned-byte budget and three rotated finalist repeats. It includes
worker startup through packing and destination verification; comparison hashing
is timed separately. Prefer fewer resources when medians differ by at most 5%.
Successful temporary stores are removed; failed stores retain a small failure
record. Fresh application stores do not imply flushed filesystem caches.
The suite reports archive-task completion and a separate content-comparison
phase in the same compact summary. These additional comparisons can take time
after all source downloads finish; they are not pending NOAA transfers. The
resource-preferred screening candidate is always included among the finalists.

### Measured Polaris comparison

PBS job `7743767` completed with exit 0 on revision `1f017e0`. Each case
fetched the same 6,144 observations: both reflectivity products, 512 scans each
in January, March, May, July, September and November 2022. Source files totaled
210.0 MB per case; verified regional ZIPs totaled 134.0 MB. The machine exposed
32 physical cores, 64 hardware threads and about 504 GiB usable memory.

| Writers × downloads per writer | Measurement | Archive time | Peak job RSS |
|---|---|---:|---:|
| 4 × 8 | One screening pass | 215.5 s | 3.40 GiB |
| 8 × 8 | One screening pass | 129.0 s | 6.17 GiB |
| 12 × 8 | One screening pass | 77.2 s | 9.14 GiB |
| **12 × 4** | Three finalist repeats, median | **76.256 s** | **8.76 GiB maximum** |
| 12 × 16 | Three finalist repeats, median | 77.043 s | 9.42 GiB maximum |

The finalists performed essentially the same work in the same time; four
downloads per writer used fewer resources. Every case preserved exact values,
coordinates, bitmap information and source metadata. Timings include process
startup, fetching, decoding, writing, packing and destination verification;
initial inventory and the additional comparison-hash pass are separate. Fresh
application stores do not mean cleared filesystem or proxy caches. The shared
byte-budget instrumentation is part of these measurements; production does not
use that benchmark ledger. These short selections establish a best tested
profile, not a full-study runtime guarantee.

The cumulative byte ledger retained the initial controlled-stop run and charged
3.52 GiB against the 10 GiB limit, including its unfinished comparison case.
The initial run was stopped to correct finalist selection, not because of a
source-data failure. Remaining queues and decoder settings were not expanded
because their measured waiting did not trigger those experiments. Review the
first production months before extrapolating to the entire study.

### First setup

For a fresh Polaris checkout/environment, first commit and push the reviewed
implementation. Check PBS allocation and queue eligibility, `myprojectquotas`,
and the project path before using the example values below. Do not assume the PBS account equals the
storage directory name. The authenticated check on October 13, 2026 confirmed
`lighthouse-upr` as the allocation and `/eagle/lighthouse-upr` (also
`/lus/eagle/projects/lighthouse-upr`) as the project directory. The user confirmed
the increase to **10 TB** on October 24, 2026, effective the previous day. Refresh
current usage before submission; the quota is shared with other project assets.

```bash
export ECORE_PROJECT_ROOT=/eagle/lighthouse-upr
mkdir -p "$ECORE_PROJECT_ROOT/ecore-weather"
git clone https://github.com/avega17/E-CORE_EO_Weather_Forecasting.git \
  "$ECORE_PROJECT_ROOT/ecore-weather/repo"
cd "$ECORE_PROJECT_ROOT/ecore-weather/repo"
module use /soft/modulefiles
module load conda
export CONDA_PKGS_DIRS="$ECORE_PROJECT_ROOT/ecore-weather/conda-cache"
export CONDA_ENVS_PATH="$ECORE_PROJECT_ROOT/ecore-weather/envs"
export ECORE_PYTHON="$ECORE_PROJECT_ROOT/ecore-weather/envs/cpu/bin/python"
source jobs/polaris.env.sh
export ECORE_PBS_MODE=setup
qsub -A "$ECORE_PBS_ACCOUNT" -q debug -l walltime=01:00:00 \
  -v "$ECORE_PBS_ENV_VARS,ECORE_PBS_MODE" \
  jobs/polaris_mrms.pbs
```

The `setup` job installs dependencies on a compute node, with bounded Conda
installer and numerical-library thread pools. It resumes a partially installed
environment, performs imports and `pip check`, and saves
`ecore-weather/environment-resolved.txt`. Check its PBS exit status before
submitting the pilot. A login-node installation hit the process-resource limit
during first setup, so the supported procedure uses PBS for installation.

The explicit Linux package list avoids repeating the Conda dependency solve.
It was generated from `jobs/environment-polaris.yml`; keep both files together
when updating dependencies. GCSFS 2026.8.1 and S3FS 2026.7.0 share the required
FSSPEC version, despite their different release numbers. The final pip check
and resolved package export cover the pip-only dependencies too.

Use the dedicated CPU environment rather than cloning the site's large GPU
base. ALCF documents the [available Python environment approaches](https://docs.alcf.anl.gov/polaris/data-science/python/).
Installation and heavy checks use compute allocations where required by site
policy. Login nodes are used for normal setup, submission and monitoring.

### Pilot, tuning, production

The script uses the [documented Polaris proxy](https://docs.alcf.anl.gov/polaris/getting-started/#proxy)
for anonymous NOAA listing and GET requests, with TLS verification enabled.
It launches one coordinator with access to the node's CPUs. Verify that its
recorded affinity is appropriate rather than assuming the MPI launch exposed
all cores. GPUs are unused.

Source `jobs/polaris.env.sh` to derive `ECORE_QUOTA_FREE_GIB` from the current
`myprojectquotas` result. The minimum
32 GiB gate is provisional: increase it if pilot output projections plus safety
margin require more. Global filesystem free space does not measure project
quota. Keep output/state on Eagle and live stores/index under `/local/scratch`.
[ALCF storage guidance](https://docs.alcf.anl.gov/data-management/filesystem-and-storage/)
explains that local SSD is erased between jobs and Eagle is not automatically
backed up.

```bash
source jobs/polaris.env.sh
export ECORE_PBS_MODE=pilot
export PBS_MRMS_START=2022-09-18T12:00:00Z PBS_MRMS_END=2022-09-18T12:40:00Z
qsub -A "$ECORE_PBS_ACCOUNT" -q debug -l walltime=01:00:00 \
  -v "$ECORE_PBS_ENV_VARS,ECORE_PBS_MODE,PBS_MRMS_START,PBS_MRMS_END" \
  jobs/polaris_mrms.pbs
```

The pilot fetches four slots per variable at midday on September 18, 2022 into
an isolated pilot destination. Confirm source reads, bitmap and exact-array
read-back, Eagle publication, PBS exit status and reuse in a fresh pilot job.
This proves correctness and proxy operation, not full-month throughput.

```bash
export ECORE_PBS_MODE=tune
qsub -A "$ECORE_PBS_ACCOUNT" -q debug -l walltime=01:00:00 \
  -v "$ECORE_PBS_ENV_VARS,ECORE_PBS_MODE" \
  jobs/polaris_mrms.pbs
```

Tuning writes one compact suite, one compressed scientific selection and a
selected-profile JSON. Resume an incomplete tuning suite with the same scientific selection
and output; its cumulative byte budget is not reset. Review the results, commit
and push any selected profile/code changes, and update the Polaris
checkout before starting production. Do not alter a running checkout.

```bash
export ECORE_PBS_MODE=study
export PBS_MRMS_START=2021-01-01 PBS_MRMS_END=2026-07-01
qsub -A "$ECORE_PBS_ACCOUNT" \
  -v "$ECORE_PBS_ENV_VARS,ECORE_PBS_MODE,PBS_MRMS_START,PBS_MRMS_END" \
  jobs/polaris_mrms.pbs
```

The MRMS production request is one node in `capacity` for **six hours**.
Its measured twelve-writer/four-download/one-decoder profile is unchanged. Confirm
current [queue limits](https://docs.alcf.anl.gov/polaris/running-jobs/) first.
Use normal `qstat -was1 JOBID`, `qstat -f JOBID` and `qstat -x -f JOBID` to inspect
queue comments, current state and completed exit status. If access is denied,
report that limitation instead of trying alternative access routes.

### Progress and recovery

```bash
export ECORE_PROJECT_ROOT=/eagle/lighthouse-upr
watch -n 30 -t "$ECORE_PROJECT_ROOT/ecore-weather/envs/cpu/bin/python \
  $ECORE_PROJECT_ROOT/ecore-weather/repo/scripts/dataset_jobs.py status mrms \
  --progress $ECORE_PROJECT_ROOT/ecore-weather/state/study/progress.json"
```

The compact shared progress file is updated approximately every 30 seconds.
A checkpoint count can describe work still on node-local SSD; it does not mean
that month is durable on Eagle. Only a verified completed product-month survives
node loss. After resubmission, unfinished counts may restart from zero while
completed months are reused.
A closed DuckDB snapshot is published after each product-month and on normal
shutdown. The live database stays node-local. Status distinguishes remote
heartbeat evidence from a locally visible PID; a stale remote heartbeat is
unknown, not proof that the process failed. Completed receipts remain usable
without scratch selections. Keep a stable progress path across resubmissions.

Before wall time, the application stops new admissions using at least fifteen
minutes or 130% of its longest measured task, whichever is greater. It drains
active tasks when time permits and has a hard deadline before the scheduler
limit. Exit 75 means an intentional resumable pause; real errors retain their
failure status. A final shell timeout prevents a blocked library call from
waiting indefinitely.

Only verified completed product-months are durable on Eagle. An unfinished
local month can be lost when PBS clears the SSD; that month is fetched again.
Resubmit the same study command to reuse completed archives. Do not run two
coordinators against the same state/destination. Retain compact receipts and
bounded error logs rather than per-object operational histories.

Following successful pilots and tuning, the user authorized annual workstation
radar maps while the full-study PBS job is queued. Their validation and the
first verified production month remain separate checks.


<a id="polaris-goes-manual-pilot-and-20212022-study"></a>

## Polaris GOES: pilot and full-study fetching

Single-node GOES jobs have run on Polaris; the user reports the latest reached
its requested 24-hour walltime. Exact completion and runtime measurements still
come from its saved progress and PBS output. Reuse the prepared CPU ingestion
environment. The three-node extension below is locally implemented and tested,
but has not been submitted to Polaris. Jobs do not require Git metadata, a clean checkout or a pinned
revision. Update the code only when no job is using it. Fetch jobs do not install GPU
model dependencies or run inference.

The study keeps eight native CMIPF bands, every available full-disk scan and the
current Puerto Rico box. The full study runs from 2021-01-01 through 2026-07-01,
end excluded. The expanded project quota removes the earlier raw-study restriction.
C02 uses range reads; the other seven bands share asynchronous whole-file staging.
Full files are cropped locally and retired after verified scan checkpoints.
See the [pipeline explanation](goes_parallel_pipeline.md) and
[storage layout](storage_and_data_management.md).

### Polaris GOES resource settings

`jobs/goes_polaris.json` uses three normal month owners plus one C02-only
owner, 64 global async downloads, eight local HDF5 readers, sixteen C02 range
readers and two finalizers. ROI queues share 4,096 MiB; staging shares 32,768 MiB.
The production summed-RSS allowance is now 384 GiB, with a warning at 256 GiB
and checkpointed handling of sustained pressure; the bounded pilot retains its
strict 64 GiB ceiling. See [memory protection](#goes-memory-protection-and-recovery).
These limits do not multiply by month or
band. Async requests are not CPU threads. Independent HDF5 reader processes,
writers and finalizers also use the node's CPU resources.

This is the revised target-machine profile, not a measured optimum.
The workstation's two-month profile remains unchanged. Record effective
concurrency and first production-month runtimes before drawing conclusions about
Polaris throughput. The shared proxy, source sizes and C02's different read method
can matter more than hardware thread count.

### Date variables and common submission settings

After checking that no job is using this checkout, update it:

```bash
git pull --ff-only
```

The existing CPU environment needs no new packages for these job changes.
Commit checks, clean-checkout requirements and code-hash gates are absent from
both PBS wrappers. Code-lineage recording is off unless explicitly requested by
the developer and cannot stop a job. The ordinary date, capacity and scientific
archive checks still apply.

Both fetch wrappers require explicit ISO dates or UTC timestamps. Examples are
in `.env.example`; assigning a value there does not automatically export it to
PBS. Export the listed settings and pass them with `qsub -v`. Do not source or
export the entire credential-bearing `.env`, and do not use `qsub -V`.

```bash
source jobs/polaris.env.sh
```

This non-sensitive file exports the project/code/Python paths, allocation and
default fetch dates. It preserves explicit overrides. It does not read `.env`,
submit jobs or select pilot/study mode. Both GOES and MRMS default to the full
2021–June 2026 interval. Explicitly exported values remain unchanged when sourcing
this file: reset an older `PBS_GOES_END` before extending a study. For the pending
GOES work, start at 2022-01-01 and reuse verified 2022 months, then continue
through 2026-07-01. The three-node date assignments below cover that same union.

Sourcing runs the documented [project-quota command](https://docs.alcf.anl.gov/data-management/filesystem-and-storage/disk-quota/)
and exports `ECORE_QUOTA_FREE_GIB` from the exact project/Eagle row. Call
`ecore_refresh_quota` again before a later submission. A missing/changed table
returns an error and clears the old quota value; resolve that error before
submitting. The helper can use system Python 3 before the CPU environment exists.

The displayed `G`/`T` suffixes do not state decimal versus binary units. The
parser honors explicit GB/GiB suffixes, otherwise takes the smaller free-space
estimate under both conventions, rounds down and leaves 1 GiB for display
rounding. This is a submission-time estimate, not a space reservation or an exact
byte quota; other jobs and queue delays can change availability. The GOES wrapper
no longer imposes an artificial dataset cap by default. Actual project quota,
filesystem headroom and node-local scratch checks still apply.

`ECORE_PBS_ENV_VARS` contains the common exported variable names, so submission
commands can use `-v "$ECORE_PBS_ENV_VARS,..."` instead of typing each name.

The scripts validate dates, use the documented proxy, and cap
incidental BLAS/OpenMP threads. The MRMS study uses `PBS_MRMS_START` and
`PBS_MRMS_END`; GOES uses `PBS_GOES_START` and `PBS_GOES_END`. End dates are
excluded. Setup/tuning do not require these fetch-date variables.

### Bounded four-context pilot

```bash
export ECORE_PBS_MODE=pilot
export PBS_GOES_START=2021-01-01T12:00:00Z
export PBS_GOES_END=2021-01-01T13:20:00Z
qsub -A "$ECORE_PBS_ACCOUNT" -q debug -l walltime=01:00:00 \
  -v "$ECORE_PBS_ENV_VARS,ECORE_PBS_MODE,PBS_GOES_START,PBS_GOES_END" \
  jobs/polaris_goes.pbs
```

Pilot mode uses the supplied UTC time-of-day window on January 1, April 1,
July 1 and October 1 of the start year. It requires eight available scans per
band in each context, admitted using the profile's month-owner limit (now three
normal owners plus one C02-only tail). It is
not a continuous January-to-October fetch. The cumulative transfer ceiling is
**32 GiB**, including retries and conservative failed-request reservations.
The compact transfer ledger remains on Eagle across failed submissions; retries
do not reset it. Its accounting I/O is included in the pilot runtime.

The pilot runs the normal source-to-Zarr checks for all arrays, native
coordinates and scan metadata, then starts a fresh pipeline against the
completed stores and requires zero additional source bytes. Its summary records
source-identity hashes, effective concurrency, runtime through publication,
returned bytes, scratch/ROI reservations and peak process RSS. It removes only
its isolated successful temporary archives after preserving compact evidence.
A failure retains its stores for inspection; choose a fresh isolated pilot
location or inspect/remove those test files before retrying.

Check `state/goes/pilot/pilot-summary.json` under `ecore-weather/`, the joined
`ecore_goes.oJOBID` output and `qstat -x -f JOBID`. Require exit 0, 32 verified
band archives, 256 observations and successful no-download reuse. These are
bounded pilot measurements, not a full-study performance forecast. Its CLI operation is `dataset_jobs.py benchmark goes --polaris-pilot`,
with the profile, dates, destination, scratch and output arguments used by the PBS wrapper.

### Study submission and monitoring

```bash
export ECORE_PBS_MODE=study
export PBS_GOES_START=2022-01-01 PBS_GOES_END=2026-07-01
qsub -A "$ECORE_PBS_ACCOUNT" \
  -v "$ECORE_PBS_ENV_VARS,ECORE_PBS_MODE,PBS_GOES_START,PBS_GOES_END" \
  jobs/polaris_goes.pbs
```

GOES requests one capacity node for 24 hours. The current Polaris profile has
three normal month owners plus one C02-only tail, with the existing global
64 full-object downloads, eight local readers and sixteen C02 range readers.
These limits do not multiply by month; this change is intended to reduce the
work competing for bandwidth, not a demonstrated throughput improvement.
`ECORE_PROFILE` and
`ECORE_DESTINATION` can be exported and explicitly added to `-v` to override
profile/destination. Do not increase resource limits by launching competing jobs
against the same stores.

```bash
qstat -was1 JOBID
qstat -x -f JOBID
watch -n 30 -t "$ECORE_PYTHON scripts/dataset_jobs.py status goes --progress $ECORE_PROJECT_ROOT/ecore-weather/state/goes/study/progress.json"
sbank-list-transactions -r polaris -p lighthouse-upr -j JOBID
```

Progress is an atomic aggregate snapshot published about every 30 seconds and
at shutdown. Completed band records survive scratch deletion. A closed index
snapshot is published after each verified band and at shutdown; no live DuckDB
WAL is copied to Eagle. Status can report a stale remote heartbeat without
pretending a compute-node PID is accessible on the login node.

The bottom of the status table now reports **aggregate returned-data bandwidth**:
the current run's average, plus a recent average and median. The run average
divides the displayed tasks' current-attempt bytes by elapsed time since that
run started. It excludes old attempts and does not add per-band average rates.
When current-attempt counters are unavailable, the run average stays blank.

The recent statistics need at least two different progress snapshots. Leave
`watch` running: they use at most 60 fresh snapshot intervals. The recent
average is total byte change divided by total interval time; the median is the
middle interval rate. Re-reading the same snapshot adds no artificial zero
sample. Counter resets, unknown newly appearing counters and changed runs
restart the recent window. Date/product/band filters limit the aggregate to
the displayed tasks. These rates include idle, cropping and writing time;
they are not a NIC measurement or a median over the whole study.

For `--progress`, one small monitor cache defaults to the login node's temporary
directory, separate from Eagle research data and the live DuckDB file. Use
`--cache /path/to/writable/monitor.json` for an explicit location. Deleting that
cache only resets the recent statistics; it cannot affect fetching or archives.

### Capacity, walltime and resubmission

The project now has a user-confirmed **10 TB Eagle quota**. The full native
eight-band study is roughly **0.86 TB of compressed monthly archives**, or
**1.03 TB with the historical 20% allowance**; see the assumptions in
[storage and data management](storage_and_data_management.md#full-study-goes-storage-on-polaris).
This does not estimate preprocessing, training/validation datasets or model assets.
The wrapper now scales its historical monthly estimate to the selected interval,
including partial months, rather than printing a fixed two-year estimate.

The default profile has `storage_cap_gb: null`. The wrapper omits `--storage-cap-gb`
when no cap is configured. To constrain a specific job, export a positive
`ECORE_GOES_STORAGE_CAP_GB` value and add that variable name to `qsub -v`; its
value overrides a profile cap. Optional cap reservations count physical archives,
recovery/state files and publication copies. They are not required for the normal
full-study run. Project quota and physical filesystem headroom are checked at
startup separately; the quota estimate does not reserve space against other jobs.
Node-local SSD must also fit open stores, staged inputs and packed copies.
Insufficient scratch does not redirect unfinished stores onto Eagle.

The wrapper now enables incremental partial checkpoints under
`state/goes/study/partial-archives/`. Roughly every quarter of a band-month,
the writer copies **only the newly committed compressed Zarr chunks**, their
source metadata and verification records to an immutable ZIP on Eagle. It does
not re-download source files or rewrite the growing final monthly ZIP. Copies
are hashed and flushed before the small checkpoint receipt is replaced. These
temporary recovery files consume real Eagle space and are removed only after
the completed monthly ZIP passes destination verification.

Resubmit with the same dates, archive destination and shared state directory.
The runner first reuses completed archives, then restores matching partial
checkpoints into the new node's local scratch and verifies their pixels before
fetching the remaining observations. Changes to the source selection cannot
silently reuse a different checkpoint. Supported shutdown attempts to save the
latest verified eight-scan batch. A sudden node loss can still lose progress
since the last quarter checkpoint; the original node-local-only checkpoints
cannot be recovered retrospectively after PBS clears that node.

With durable partial checkpoints enabled, new month admissions stop fifteen
minutes before the application deadline. Active months continue until they
finish or the deadline is reached. The deadline leaves four minutes before
the PBS limit for shutdown, including a best-effort final partial checkpoint;
the outer timeout remains a safeguard. Without durable checkpoints, the old
rule still reserves 130% of the longest completed whole-month runtime, with a
fifteen-minute minimum. This can stop an otherwise healthy job many hours early.

Exit **75** means intentional pause, not completion of the whole selection.
`fetch/paused.json` now records a compact reason, elapsed time and completion
count; detailed monthly receipts stay under `fetch/checkpoints/`. A genuine
I/O failure, timeout or killed process remains a different exit. Inspect without
printing a large selection or the PBS environment:

```bash
cat "$ECORE_PROJECT_ROOT/ecore-weather/state/goes/study/fetch/paused.json"
qstat -x -f JOBID | rg 'resources_used.(walltime|cput)|Resource_List.walltime|Exit_status'
```

The ordinary `qstat` **Time Use** field is CPU time accumulated across the
processes, and can exceed elapsed hours. `resources_used.walltime` is the elapsed
PBS runtime to compare with the requested `Resource_List.walltime`.
The [PBS qstat reference](https://github.com/openpbs/openpbs/blob/master/doc/man1/qstat.1B)
defines these two display formats.

Job **7770028** illustrates the old rule. Its supplied pause record shows all
32 January–April 2021 band archives verified/reused in **30,280.1 seconds
(8:24:40)**. At the first completed month, about 8.26 hours, the 130% allowance
already exceeded the remaining part of a sixteen-hour request. No next month
was admitted. The record contains no partial later months lost by this pause;
it is not a fresh inventory of Eagle. It also reports all four C02 archives as
reused, so this run is not a new benchmark of C02 range throughput.

After updating code, **submit a new PBS job** to use the revised wrapper and
checkpoint options; an already queued job holds its earlier script. Do not pull
changes into a checkout while another fetch is using it. Keep the same dates
to reuse January–April automatically. Quota checks still apply to both recovery
parts and final archives.

<a id="three-node-goes-extension-proposed"></a>
### Three-node GOES fetching

`jobs/polaris_goes_multinode.pbs` runs three independent copies of the existing
GOES pipeline. It follows ALCF's [multi-node ensemble pattern](https://docs.alcf.anl.gov/running-jobs/example-job-scripts/#multi-node-ensemble-calculations-example):
one one-node hostfile and one `mpiexec` per application. Node setup happens on
each assigned node; the worker then runs Python directly, without nested MPI.
The readers, native grids, compression and monthly archive paths are unchanged.
This is implemented and tested locally. **A real multi-node Polaris pilot and
scaling measurement have not yet run.** Submission is manual.

The [queue guide](https://docs.alcf.anl.gov/polaris/running-jobs/)
allows three-node requests in `capacity` and `debug-scaling`; ordinary `debug`
is limited to two nodes. Three nodes for 24 hours request 72 node-hours, and a
larger allocation can wait longer in the queue. This exercises node allocation
and independent ingestion, not distributed GPU training.

#### Dates, ownership and resources

The versioned `jobs/goes_polaris_partitions.json` controls **multi-node study
dates**, rather than `PBS_GOES_START/END`. Those environment variables still
control the single-node wrapper. The launcher supplies each worker's bounds:

| Stable partition | Start | End excluded | Work |
|---|---|---|---|
| `part-0` | 2022-01-01 | 2024-01-01 | Reuse verified 2022; finish leftovers and 2023 |
| `part-1` | 2024-01-01 | 2025-04-01 | 2024 and January–March 2025 |
| `part-2` | 2025-04-01 | 2026-07-01 | Rest of 2025 and January–June 2026 |

The launcher rejects duplicate labels, overlaps, gaps and boundaries that split
a physical month. Keep labels stable when resubmitting. Adjust the split dates
in the JSON before submission if the latest inventory suggests a better balance.
The default assignment assumes 2021 is complete; confirm that separately. A node
which finishes its static partition stays allocated until the job ends.

Every node uses `jobs/goes_polaris.json`: three normal month owners, one C02 tail,
64 async downloads, eight local HDF5 readers, sixteen C02 range readers, 4 GiB
ROI queues and 32 GiB disk staging. These limits are **per node**, not per band
or month. Three nodes permit 192 async downloads and 48 C02 readers. A shared
outbound proxy or Eagle publication can limit scaling; no threefold speedup is
claimed. Production memory protection remains 384 GiB summed RSS per node with
available-memory checks; strict pilot limits are separate.

Each worker owns Linux-local scratch and a writable DuckDB file. Its durable
configuration, bounded `worker.log`, progress and closed snapshot are under:

```text
ecore-weather/state/goes/study/
    ensemble.json                 # current allocation, placement and outcome
    worker-exits.json             # each worker's exit status
    merge.json                    # last verified database consolidation
    partitions/part-0/            # similarly part-1 and part-2
        progress.json
        worker.log                # 4 MiB plus one rotated copy
        fetch/                    # selections and monthly checkpoints
        archive_index.duckdb      # temporary closed snapshot, removed after merge
    partial-archives/             # existing durable scan batches, reused in place
```

All nodes publish to the same canonical archive destination, but their source
months are disjoint. Recovery selections are copied from the earlier single-node
`study/fetch` only when needed. Existing `partial-archives` are reused in place;
verified monthly ZIPs are validated and reused normally. **Do not run an older
single-node GOES job against these same months simultaneously.** The new launcher
excludes another multi-node launcher using the same state directory.

The known October 13, 2023 C02 source failure is carried in
`jobs/goes_source_issues.json`. The owning node prepares the October selection
and attaches that evidence only after matching the exact source identity, ETag,
size and ROI. A changed NOAA object is read normally. No synthetic pixels or
unproven skip list is introduced. Updating this small file with the Python code
avoids having to transfer the workstation's entire ignored STAC directory.

#### Submit and monitor

First make the tested files available in the Polaris checkout and use the
existing CPU environment. There are no Git revision or clean-checkout gates.
Ensure any previous GOES job targeting these dates has stopped, then:

```bash
source jobs/polaris.env.sh       # refreshes actual quota; no credentials exported
qsub -A "$ECORE_PBS_ACCOUNT" \
  -v "$ECORE_PBS_ENV_VARS,ECORE_PBS_MODE=study" \
  jobs/polaris_goes_multinode.pbs
```

This queues a three-node, 24-hour `capacity` job for the JSON partitions above.
A local/mock test is not a successful Polaris pilot. For the manual bounded pilot
instead, use the same script with:

```bash
qsub -A "$ECORE_PBS_ACCOUNT" -q debug-scaling -l walltime=01:00:00 \
  -v "$ECORE_PBS_ENV_VARS,ECORE_PBS_MODE=pilot" \
  jobs/polaris_goes_multinode.pbs
```

Pilot windows are explicit `pilot_start/end` fields in the partition JSON.
Each node tests eight scans per band in four seasons, using GOES-East for the
observation dates, including the GOES-19 transition where applicable. The limit
is 32 GiB returned per node, **96 GiB combined**, including retries and failures.
Pilot archives are isolated by partition and removed after successful equality
and reuse checks. Inspect failure evidence before retrying a failed pilot.
The pilot does not claim to validate the complete production dataset.

To inspect the allocation use `qstat -f JOBID` and later `qstat -x -f JOBID`.
To leave the readable per-partition tables running:

```bash
watch -n 30 -t -- "$ECORE_PYTHON" -m ecore_weather.jobs_multinode status \
  --state "$ECORE_PROJECT_ROOT/ecore-weather/state/goes/study"
```

For a smaller table that fits a terminal, add `--partition part-0` and an
intersecting window such as `--start 2023-01-01 --end 2023-04-01`. Each table
retains the existing aggregate bandwidth estimates; these are returned-byte
rates for that worker, not a sum of per-band rates or a measured NIC speed.

A worker pause (75) or failure does not kill the other independent applications.
The allocation returns 1 if any worker failed, otherwise 75 if any paused, or 0
when all completed. Read `ensemble.json`, `worker-exits.json`, partition progress
and the bounded worker logs. Resubmitting the same command restores the same
partitions, verified ZIPs and published partial checkpoints, even on new hosts.
The launcher reserves the final minutes for shutdown and database consolidation;
PBS can still kill abruptly, so only published recovery files survive node loss.

#### One operational database after the allocation

After every application stops, one process merges the closed node snapshots,
previous GOES/MRMS indices and any prior canonical database. It uses
[read-only attachments](https://duckdb.org/docs/1.5/sql/statements/attach)
and a fresh local output. Records are deduplicated by their schema keys;
canonical archive paths are normalized. Archive size and completion-marker hash
must agree. A verified archive wins over stale failed/partial task state;
conflicting archive or immutable measurement evidence leaves the merge pending.
Run history remains history: counters and concurrent task runtimes are not added
as if they were disjoint work. Array equality was checked when each ZIP was
published; consolidation does not download NOAA or rewrite raw arrays.

The merger closes, reopens and validates its output, copies it to Eagle, validates
that copy and atomically publishes **one project-wide operational database** at:

```text
ecore-weather/state/archive_index.duckdb
```

Only after success are redundant node snapshots removed. Previous single-node
GOES/MRMS snapshots remain recovery inputs until deliberately retired; new jobs
use the canonical index as their seed. Active coordinators never share a writable
DuckDB. This respects DuckDB's [single-writer-process rule](https://duckdb.org/docs/1.5/connect/concurrency).

If a snapshot is missing, evidence conflicts, or insufficient time remains,
`ensemble.json` says `merge: pending` and all available inputs remain. After
checking that **no fetch or snapshot publisher is running**, consolidate them
from a short compute allocation using local SSD:

```bash
"$ECORE_PYTHON" -m ecore_weather.jobs_multinode merge \
  --state "$ECORE_PROJECT_ROOT/ecore-weather/state/goes/study" \
  --scratch /local/scratch/ecore-index-merge \
  --destination "$ECORE_PROJECT_ROOT/ecore-weather/state/archive_index.duckdb"
```

The ordinary archive index rebuild remains available for discovery recovery;
this merge operation also preserves run, task, benchmark and backup history.
No research archives are deleted by consolidation. The workstation fetch and
its database remain independent.

### GOES memory protection and recovery

Job **7776485** ended with exit **1**, not an intentional pause: our application
raised `MemoryError` at 68,720,574,464 bytes of summed RSS, only **1.05 MiB**
above its old 64 GiB ceiling. This was an application limit carried into
production, not evidence that the node exhausted its RAM or that PBS killed it.
The supplied log shows May, June and July 2021 completed all eight band archives
before August–October were admitted. The Zarr metadata warnings were unrelated
to that exception. Exact unfinished counts require the saved checkpoints.

**RSS (resident set size)** is the physical RAM mapped by a process. Our total
adds the coordinator and its descendants; it includes Python, Torch, HDF5,
compression, metadata and queued arrays, not only image pixels. Shared library
pages can be counted in several processes. Therefore this total can exceed
the unique RAM used by the job. It is also separate from GPU VRAM and from the
4 GiB ROI queue. The [psutil explanation](https://psutil.io/glossary/#rss) covers
this distinction. [Polaris nodes have 512 GiB RAM](https://docs.alcf.anl.gov/polaris/#polaris-compute-nodes).

The revised `jobs/goes_polaris.json` keeps the existing concurrency and queues:

| Production condition | Response |
| --- | --- |
| Summed RSS reaches 256 GiB | Warn and continue; retain a memory sample |
| Summed RSS stays above 384 GiB for 60 seconds | Request checkpointed shutdown, exit 75 |
| Node-available RAM stays below 32 GiB for 60 seconds | Request checkpointed shutdown, exit 75 |
| Node-available RAM drops below 8 GiB | Request checkpointed shutdown immediately |
| A brief spike above 384 GiB that recovers | Continue; reset the pressure timer |

These are starting safety margins, not measured optimal settings. The 384 GiB
allowance leaves substantial room below the node's installed memory. Available
RAM includes memory the OS can reclaim; it is more useful than completely free
RAM when file caching is heavy. Readings are node-wide, not a promise about an
independently imposed PBS/OS memory limit. A single supplied RSS value cannot
establish whether there is a memory leak or which process consumes most memory.

Samples are taken every five seconds. One atomically replaced
`state/goes/study/memory.json` contains the latest/peak sampled summed RSS,
available/total node RAM, twelve recent aggregate samples and the twelve largest
processes. It contains no command arguments or credentials. The same compact
data appears in `progress.json`, and the normal `dataset_jobs.py status goes
--progress ...` table now prints RAM use and available node memory above the
archive rows. Direct CLI study runs also load the profile's memory settings;
explicit benchmark limits take precedence.

When a production memory threshold is reached, the coordinator cancels further
reads and lets writers attempt to publish the latest verified eight-scan prefix
to Eagle. Earlier durable parts and completed archives remain protected. A
sudden kill or slow shutdown can still lose work after the last published part.
This does not turn a real corrupt-array, disk or network error into success.
The bounded pilot still fails at its separate 64 GiB RSS limit, and scheduler
benchmarks retain their existing 48 GiB budget; they do not inherit the relaxed
production policy.

Temporary checkpoint/publication copies still need physical space and remaining
project quota; the old fixed dataset cap is no longer a default stop condition.
The 32 GiB staging limit concerns disk, and the 4 GiB ROI limit bounds
queued arrays rather than total process RAM. Neither is increased simply because
the node has more RAM. The actual PBS walltime, four-minute shutdown reserve,
fifteen-minute admission cutoff and scientific integrity checks remain in force.

For a quick list of durable partial counts, without reading image arrays:

```bash
"$ECORE_PYTHON" - <<'PY'
import json, os, pathlib, zipfile
root = pathlib.Path(os.environ['ECORE_PROJECT_ROOT']) / 'ecore-weather/state/goes/study/partial-archives'
print('Month    Band  Durable scans / selected')
for path in sorted(root.glob('*/checkpoint.json')):
    receipt = json.loads(path.read_text())
    with zipfile.ZipFile(path.parent / receipt['parts'][-1]['file']) as archive:
        progress = json.loads(archive.read('progress.json'))
    print(f"{progress['month']}  C{progress['band']:02}  {receipt['next_index']:,} / {receipt['selected']:,}")
PY
```

These are recorded durable counts, not a fresh array-integrity audit. Restoration
checks the ZIP hashes and source-pixel checksums before reusing them. Reuse the
same dates, destination and state on resubmission. Pull only when no active job
uses that checkout, and submit a **fresh PBS wrapper** for the new environment
settings; releasing an older queued script retains its old exported RSS limit.

### Debugging a GOES job that ends without output

PBS `job_state = F` means finished, not successful. Pilot **7762801** ended
with `Exit_status = 1`, no recorded CPU work and empty output. Its start/exit
timestamps span ten seconds even though sampled walltime is zero. The scheduler
allocated a node; this was not a continued queue wait. `Stageout_status = 1`
means stage-out succeeded, not that an output error occurred, according to the
[PBS reference guide](https://help.altair.com/2024.1.0/PBS%20Professional/PBSReferenceGuide2024.1.pdf).
With joined output (`Join_Path = oe`), the `.e` file may normally be empty.

The old wrapper could exit silently at its revision/clean-checkout checks. A
local reproduction returned exit 1 with zero bytes on both output streams.
This explains a possible failure, not a confirmed remote cause. Those Git
checks are now **removed**, including the required revision variable, from
both wrappers. Do not reintroduce commit or code-hash checks unless explicitly
requested by the developer; optional lineage metadata must never stop a job.

Both wrappers print a message immediately, name setup phases and report the
exit status. One small `startup.log` in the source/mode state directory retains
wrapper messages and is overwritten on the next attempt. Application output
continues through PBS. No environment dump or general shell tracing is used.
An exit before the first message still needs shell-startup/PBS investigation.

1. **Recover scheduler evidence.** Run this for the pilot and job 7762846. It
   deliberately excludes potentially sensitive environment values:

   ```bash
   qstat -x -f -F json JOBID | jq '.Jobs | map_values({
     Job_Name, job_state, Exit_status, comment, Output_Path, Error_Path,
     resources_used, exec_host, stime, obittime, Stageout_status
   })'
   ```

   Read the actual `Output_Path` (PBS prefixes it with a hostname), and confirm
   its parent directory is writable. See [PBS output issues](https://docs.alcf.anl.gov/running-jobs/known-issues/).

2. **Use a short interactive allocation.** Keep any competing queued fetch held
   and confirm no GOES job is using the same archives/state. From the repository:

   ```bash
   source jobs/polaris.env.sh
   qsub -I -A "$ECORE_PBS_ACCOUNT" -q debug \
     -l select=1:system=polaris:ncpus=64 \
     -l filesystems=home:eagle -l walltime=00:20:00 \
     -v "$ECORE_PBS_ENV_VARS"
   ```

   Once PBS gives a compute-node shell, run a short study **dry run**:

   ```bash
   cd /eagle/lighthouse-upr/ecore-weather/repo
   export ECORE_PBS_MODE=study ECORE_DRY_RUN=1
   export PBS_GOES_START=2021-01-01 PBS_GOES_END=2021-01-02
   bash jobs/polaris_goes.pbs
   ```

   This checks the wrapper, Python/date handling, quota, scratch and MPI launch,
   then prints the plan. The script's `#PBS` lines are comments when run inside
   an allocation. Inspect the terminal and `ecore-weather/state/goes/study/startup.log`.

   **In that same compute allocation, test real NOAA discovery next:**

   ```bash
   timeout 120s "$ECORE_PYTHON" -m ecore_weather.pbs check-noaa \
     --polaris-proxy --start 2021-01-01T12:00:00Z --minutes 60 --satellite 16
   ```

   This uses the production GOES discovery function and anonymous S3 listing,
   with the documented outbound proxy and all eight study bands. It reports
   matching files/scans and listed object bytes; **no imagery is downloaded**.
   Listing transfers a small metadata response. Zero matching observations is
   distinguished from a failed network request. The 120-second timeout prevents
   a connection failure from consuming the interactive allocation. It proves
   listing access, not GET/range-read, HDF5 or archive-writing performance.
   Exit the interactive shell after these checks. This follows the
   [documented Polaris approach](https://docs.alcf.anl.gov/polaris/running-jobs/).

3. **Validate imagery in pilot mode.** After local setup and discovery succeed,
   repeat the existing four-context pilot with its 32 GiB ceiling and short
   pilot date window. Require exit 0, `pilot-summary.json`, exact archive checks
   and no-download reuse before another study job. The pilot exercises staged
   downloads, C02 range reads, HDF5 cropping, local/Eagle Zarr publication and
   reuse. A full day is a separate larger validation run, not a dry run: it can
   consume substantial transfer and should wait until this pilot passes.
   A one-year study uses
   `PBS_GOES_START=2021-01-01` and `PBS_GOES_END=2022-01-01`.

Publish wrapper updates to Polaris before testing them. **Already submitted
jobs contain the old PBS script**: updating a repository file does not update
that stored script. Replace an old queued submission with a fresh one using the
corrected wrapper. Only one job may write these archives/state at a time.
If an immediate startup message is still absent, ask ALCF support to investigate
shell startup/output delivery; do not assume the NOAA reader was reached.

### Recovery after GOES job 7765036

The October 21 study failed in the coordinator's task-index update with:

```text
Connection Error: Can't open a connection to same database file
with a different configuration than existing connections
```

The heartbeat/progress monitor opened the live database read-only while another
thread in that same coordinator opened it writable. This was an application
connection-mode race, not evidence of a damaged Zarr archive, an S3 error or an
external user's long-held SQL lock. The fixed internal monitor uses the same
writable connection configuration and lock as updates and closed index copies.
External viewers remain read-only; Polaris status uses the shared progress JSON.
See [DuckDB concurrency guidance](https://duckdb.org/docs/stable/connect/concurrency).
Repetitive worker-cancellation tracebacks are suppressed; the root failure is
still reported. Scratch messages now state the PBS durability limitation.

The user-supplied final progress table records:

| Source month | Selected band observations | Verified observations | In completed Eagle archives | In unfinished node-local stores |
| --- | ---: | ---: | ---: | ---: |
| 2021-01 | 35,560 | 21,637 | 4,445 | 17,192 |
| 2021-02 | 32,256 | 21,120 | 0 | 21,120 |
| 2021-03 | 35,285 | 18,504 | 0 | 18,504 |
| 2021-04 | 34,396 | 18,128 | 0 | 18,128 |
| Total | 137,497 | 79,389 | 4,445 | 74,944 |

One band observation means one scan of one band, not a complete eight-band
model input. These counts describe the last saved snapshot, not a fresh archive
inventory. January C02 is the only reported completed band-month. Its physical
ZIP and `complete.json` on Eagle are reusable independently of the other bands
and a whole-month completion report. The next fetch validates that archive.

The other 74,944 observations had verified eight-scan batches on the node, but
their arrays were not published to Eagle. Shared progress JSON/SQL counts do
not contain those pixels. ALCF [wipes node SSD between jobs](https://docs.alcf.anl.gov/data-management/filesystem-and-storage/#local-node-ssd).
Under this wrapper, treat those partial observations as requiring refetching
after the allocation ends. A new job also uses a different job-specific scratch
directory. No automatic cross-job partial-store restoration is implemented.
Thus 5.6% of the reported verified observation count was in a completed archive;
this is not a percentage of retained storage or elapsed compute time.

The rounded rows total 2,343.31 GiB (about 2.52 decimal TB) returned in 5:56:43,
approximately 940 Mb/s across the application. Individual 20–40 Mb/s band rates
are not the aggregate network rate. January C02 accounts for 175.24 GiB of
returned source bytes, not retained Zarr size. These figures do not identify a
proxy, CPU or NIC bottleneck and do not justify changing concurrency alone.

Use the existing monitor and canonical markers to inspect recovery evidence:

```bash
"$ECORE_PYTHON" scripts/dataset_jobs.py status goes \
  --progress "$ECORE_PROJECT_ROOT/ecore-weather/state/goes/study/progress.json" \
  --start 2021-01-01 --end 2022-01-01
find "$ECORE_PROJECT_ROOT/ecore_eo_datasets/goes/ABI-L2-CMIPF" \
  -name complete.json -exec jq -r \
  '[input_filename, .band, .observations, .stored_bytes] | @tsv' {} +
```

The first command is historical progress; the second lists durable completion
markers and retained bytes. It does not replace the fetcher's archive read-back.
Do not delete the index or partial research data to resolve this connection race.

For a queued continuation, hold it with `qhold JOBID`, update the repository on
Polaris while no fetch uses it, then release it with `qrls JOBID`. This fix is
in Python modules, so an already-queued job using the current wrapper can load
the correction at startup; no PBS-script replacement is needed for this fix.
Keep the same study dates and archive destination to reuse completed bands.
Confirm that January C02 is reused and fresh checkpoint counts advance.
Only completed band-months remain durable recovery points on Eagle. Preserving
partial months across allocations would require a separate storage/workflow
change; it is not part of this connection fix.

### Completed MRMS production evidence

MRMS job 7743827 on revision `2038b86` finished with exit 0: PBS walltime 1:14:14,
application time 4,372.7 seconds, 577,665 archived observations across the two
reflectivity products, 19.06 GB returned and 12.70 GB retained. Application peak
process RSS was 12.2 GiB; there were no unfinished product-months or retries.
Its one boundary observation per product comes from December 31, 2020 for the
January 1, 2021 00:00 requested slot. This is intentional causal matching.
The six-hour MRMS request leaves headroom without changing the measured profile.

The allocation is in **node-hours**. One node for 1:14:14 is approximately
1.237 node-hours, subject to the authoritative sbank charge. PBS `cput` sums CPU
execution time; it is not the allocation deduction. A transaction may take time
to become visible. Per-band average transfer rates include waiting and processing
and are not historical NIC-speed measurements.
