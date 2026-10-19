# Study jobs: launch, monitor, pause and resume

Use the `ecore-weather` environment from the repository root. Dates are UTC and
end-exclusive. The study covers 2021-01-01 through 2026-06-30; use end
`2026-07-01`. The ROI is `[-70.24, 14.36, -62.56, 22.04]`. “768” refers to the
mentor's pixel grid, not square kilometres.

## GOES

Default native CMIPF, all available scans, eight bands C01/C02/C03/C07/C08/C09/C10/C13.
Selected operating profile: **two normal month writers plus one C02-only tail writer**,
32 global async downloads, two local readers, eight C02 range readers, 2 GiB ROI
memory and 16 GiB source staging. [Pipeline guide](goes_parallel_pipeline.md)
explains which steps overlap. These settings preserve separate native band ZIPs.

```bash
python scripts/dataset_jobs.py resume goes --pipeline shared --phase all \
  --destination /mnt/p/ecore_eo_datasets --output results/study-goes-native \
  --scratch /path/to/linux-scratch --shared-profile jobs/goes_workstation.json \
  --max-hours 12
watch -n 30 -t 'python scripts/dataset_jobs.py status goes --start 2021-10-01'
python scripts/dataset_jobs.py pause goes --output results/study-goes-native
```

Do not start a second coordinator. A supported pause stops new month admissions
and drains every already admitted month to verified completion. Exit 75 is an
intentional pause. Resume reuses matching completed months and verified scan batches.

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
`/lus/eagle/projects/lighthouse-upr`) as the project directory. At that check,
149.4 GB of the 1 TB quota was used; refresh this value before submission.

```bash
export ECORE_PROJECT_ROOT=/eagle/lighthouse-upr
export ECORE_REVISION=<tested-pushed-commit>
mkdir -p "$ECORE_PROJECT_ROOT/ecore-weather"
git clone https://github.com/avega17/E-CORE_EO_Weather_Forecasting.git \
  "$ECORE_PROJECT_ROOT/ecore-weather/repo"
cd "$ECORE_PROJECT_ROOT/ecore-weather/repo"
git checkout --detach "$ECORE_REVISION"
module use /soft/modulefiles
module load conda
export CONDA_PKGS_DIRS="$ECORE_PROJECT_ROOT/ecore-weather/conda-cache"
export CONDA_ENVS_PATH="$ECORE_PROJECT_ROOT/ecore-weather/envs"
export ECORE_PYTHON="$ECORE_PROJECT_ROOT/ecore-weather/envs/cpu/bin/python"
export ECORE_PBS_ACCOUNT=lighthouse-upr
export ECORE_QUOTA_FREE_GIB=<current-free-project-GiB>
export ECORE_PBS_MODE=setup
qsub -A "$ECORE_PBS_ACCOUNT" -q debug -l walltime=01:00:00 \
  -v ECORE_PROJECT_ROOT,ECORE_PYTHON,ECORE_REVISION,ECORE_QUOTA_FREE_GIB,ECORE_PBS_MODE \
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

Set `ECORE_QUOTA_FREE_GIB` from the current `myprojectquotas` result. The minimum
32 GiB gate is provisional: increase it if pilot output projections plus safety
margin require more. Global filesystem free space does not measure project
quota. Keep output/state on Eagle and live stores/index under `/local/scratch`.
[ALCF storage guidance](https://docs.alcf.anl.gov/data-management/filesystem-and-storage/)
explains that local SSD is erased between jobs and Eagle is not automatically
backed up.

```bash
export ECORE_PBS_ACCOUNT=<confirmed-allocation>
export ECORE_QUOTA_FREE_GIB=<current-free-project-GiB>
export ECORE_PBS_MODE=pilot
export PBS_MRMS_START=2022-09-18T12:00:00Z PBS_MRMS_END=2022-09-18T12:40:00Z
qsub -A "$ECORE_PBS_ACCOUNT" -q debug -l walltime=01:00:00 \
  -v ECORE_PROJECT_ROOT,ECORE_PYTHON,ECORE_REVISION,ECORE_QUOTA_FREE_GIB,ECORE_PBS_MODE,PBS_MRMS_START,PBS_MRMS_END \
  jobs/polaris_mrms.pbs
```

The pilot fetches four slots per variable at midday on September 18, 2022 into
an isolated pilot destination. Confirm source reads, bitmap and exact-array
read-back, Eagle publication, PBS exit status and reuse in a fresh pilot job.
This proves correctness and proxy operation, not full-month throughput.

```bash
export ECORE_PBS_MODE=tune
qsub -A "$ECORE_PBS_ACCOUNT" -q debug -l walltime=01:00:00 \
  -v ECORE_PROJECT_ROOT,ECORE_PYTHON,ECORE_REVISION,ECORE_QUOTA_FREE_GIB,ECORE_PBS_MODE \
  jobs/polaris_mrms.pbs
```

Tuning writes one compact suite, one compressed scientific selection and a
selected-profile JSON. Resume an incomplete tuning suite with the same revision
and output; its cumulative byte budget is not reset. Review the results, commit
and push any selected profile/code changes, and update the detached Polaris
checkout before starting production. Do not alter a running checkout.

```bash
export ECORE_PBS_MODE=study
export PBS_MRMS_START=2021-01-01 PBS_MRMS_END=2026-07-01
qsub -A "$ECORE_PBS_ACCOUNT" \
  -v ECORE_PROJECT_ROOT,ECORE_PYTHON,ECORE_REVISION,ECORE_QUOTA_FREE_GIB,ECORE_PBS_MODE,PBS_MRMS_START,PBS_MRMS_END \
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


## Polaris GOES: manual pilot and 2021–2022 study

The GOES PBS job is ready for **manual validation**; no GOES Polaris execution
or speedup is claimed by the local tests. Reuse the prepared CPU ingestion
environment. The code must be a clean checkout of the tested pushed revision;
update it only when no pinned job is running. Fetch jobs do not install GPU
model dependencies or run inference.

The study keeps eight native CMIPF bands, every available full-disk scan and the
current Puerto Rico box. It defaults to 2021-01-01 through 2023-01-01, end excluded.
C02 uses range reads; the other seven bands share asynchronous whole-file staging.
Full files are cropped locally and retired after verified scan checkpoints.
See the [pipeline explanation](goes_parallel_pipeline.md) and
[storage layout](storage_and_data_management.md).

### Initial Polaris GOES resource settings

`jobs/goes_polaris.json` starts with four normal month owners plus one C02-only
owner, 64 global async downloads, eight local HDF5 readers, sixteen C02 range
readers and two finalizers. ROI queues share 4,096 MiB; staging shares 32,768 MiB.
The job-process RSS ceiling is 64 GiB. These limits do not multiply by month or
band. Async requests are not CPU threads. Independent HDF5 reader processes,
writers and finalizers also use the node's CPU resources.

This is a higher-concurrency **starting profile**, not a measured optimum.
The workstation's two-month profile remains unchanged. Record effective
concurrency and first production-month runtimes before drawing conclusions about
Polaris throughput. The shared proxy, source sizes and C02's different read method
can matter more than hardware thread count.

### Date variables and common submission settings

After checking that no pinned job is active, update the existing checkout:

```bash
git fetch origin
git checkout --detach <tested-pushed-commit>
```

Use the commit provided in the implementation handoff. The existing CPU
environment needs no new packages for these job changes.

Both fetch wrappers require explicit ISO dates or UTC timestamps. Examples are
in `.env.example`; assigning a value there does not automatically export it to
PBS. Export the listed settings and pass them with `qsub -v`. Do not source or
export the entire credential-bearing `.env`, and do not use `qsub -V`.

```bash
export ECORE_PROJECT_ROOT=/eagle/lighthouse-upr
export ECORE_PYTHON="$ECORE_PROJECT_ROOT/ecore-weather/envs/cpu/bin/python"
export ECORE_REVISION=$(git rev-parse HEAD)  # reviewed and pushed revision
export ECORE_PBS_ACCOUNT=lighthouse-upr
export ECORE_QUOTA_FREE_GIB=<current-free-project-GiB>
```

The scripts validate dates and revision, use the documented proxy, and cap
incidental BLAS/OpenMP threads. The MRMS study uses `PBS_MRMS_START` and
`PBS_MRMS_END`; GOES uses `PBS_GOES_START` and `PBS_GOES_END`. End dates are
excluded. Setup/tuning do not require these fetch-date variables.

### Bounded four-context pilot

```bash
export ECORE_PBS_MODE=pilot
export PBS_GOES_START=2021-01-01T12:00:00Z
export PBS_GOES_END=2021-01-01T13:20:00Z
qsub -A "$ECORE_PBS_ACCOUNT" -q debug -l walltime=01:00:00 \
  -v ECORE_PROJECT_ROOT,ECORE_PYTHON,ECORE_REVISION,ECORE_QUOTA_FREE_GIB,ECORE_PBS_MODE,PBS_GOES_START,PBS_GOES_END \
  jobs/polaris_goes.pbs
```

Pilot mode uses the supplied UTC time-of-day window on January 1, April 1,
July 1 and October 1 of the start year. It requires eight available scans per
band in each context, allowing all four monthly pipelines to overlap. It is
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
export PBS_GOES_START=2021-01-01 PBS_GOES_END=2023-01-01
qsub -A "$ECORE_PBS_ACCOUNT" \
  -v ECORE_PROJECT_ROOT,ECORE_PYTHON,ECORE_REVISION,ECORE_QUOTA_FREE_GIB,ECORE_PBS_MODE,PBS_GOES_START,PBS_GOES_END \
  jobs/polaris_goes.pbs
```

GOES requests one capacity node for 24 hours. `ECORE_PROFILE` and
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

### Capacity, walltime and resubmission

The **400 decimal GB cap** covers GOES arrays and GOES state on Eagle, including
pilot data and temporary publication copies; it excludes MRMS, environments and
node-local scratch. Saved workstation checkpoints for the 24 study months total
311.4 GB; a 20% allowance gives 373.7 GB. This is historical evidence, not a
new Polaris measurement or the much larger volume transferred from NOAA.

Admission reserves estimated remaining native subsets. Before copying a closed,
verified ZIP, the finalizer replaces that estimate with its actual size and
checks the cap again. Existing physical files are counted once, with a 512 MiB
allowance for changing progress/index metadata. Shared project quota and physical
filesystem free space are checked independently. The larger profile also needs
node-local space for open stores, staged inputs and simultaneous packed copies;
insufficient space stops the job rather than redirecting to Eagle scratch.

Before walltime the runner stops new admissions using at least fifteen minutes,
or 130% of the longest observed month runtime. The application deadline leaves
four minutes for cleanup; an outer timeout is a final safeguard. Exit 75 means
an intentional pause. A timeout, killed process or genuine I/O error is not
reported as successful study completion.

Resubmit the same pinned command to continue. Verified band-month ZIPs on Eagle
are reusable even if their other bands never finished. Unfinished node-local
stores may need refetching because Polaris erases local SSD between jobs. Keep
the same date range and shared state path. Do not copy node-local partial stores
onto Eagle to evade the storage cap.

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
