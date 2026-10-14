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

| Setting | Workstation | Polaris initial candidate |
|---|---:|---:|
| Product-month writer processes | 4 | 8 |
| Download threads per writer | 8 | 8 |
| Decode slots per writer | 1 | 1 |
| Active plus queued observations per writer | 8 | 16 |
| Reserved ROI payload per writer | 128 MiB | 256 MiB |
| Total process RSS ceiling | 32 GiB | 64 GiB |

The Polaris candidate has 64 download threads in total, twice the workstation
profile. Doubling both writers and per-writer downloads would produce 128.
Decode slots are separate from download threads. Compressed gzip and expanded
GRIB buffers, full-grid decoding and returned ROI arrays have different memory
costs; the ROI queue limit is not a total-memory estimate. Conservative ROI
reservations reduce effective admission when necessary, while the job RSS
ceiling covers the whole process tree. Incidental numerical-library threads
are capped to one so they do not multiply the intended process concurrency.

The scheduler admits independent **source-month** stores across adjacent months.
For example, a February 00:00 slot may use January 23:58 data. That observation
belongs to January's store, so it is assigned to the January owner rather than
making a February worker contend for the same ZIP. Requested and actual times
remain recorded, and unmatched slots are counted once. A study starting at
midnight can therefore have a small preceding-month boundary archive.

Polaris settings are starting candidates, not measured speedups. Screen 4/8/12
writers, then 4/8/16 download threads near the best result. Test a second decode
slot only when decode waiting is material. Compare larger queues only when
phase waiting indicates a benefit. The supported tuning command uses identical
CARIB objects for both variables in six 2022 monthly contexts, a cumulative
10 GiB returned-byte budget and three rotated finalist repeats. It includes
worker startup through packing and destination verification; comparison hashing
is timed separately. Prefer fewer resources when medians differ by at most 5%.
Successful temporary stores are removed; failed stores retain a small failure
record. Fresh application stores do not imply flushed filesystem caches.

### First setup

For a fresh Polaris checkout/environment, first commit
and push the reviewed implementation. Authenticate with `ssh
asvnpr@polaris.alcf.anl.gov` using a fresh application passcode. Check the PBS
allocation and queue eligibility, `myprojectquotas`, and the project path before
using the example values below. Do not assume the PBS account equals the
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
conda env create -p "$ECORE_PROJECT_ROOT/ecore-weather/envs/cpu" \
  -f jobs/environment-polaris.yml
export ECORE_PYTHON="$ECORE_PROJECT_ROOT/ecore-weather/envs/cpu/bin/python"
"$ECORE_PYTHON" -m pip install --no-deps -e .
"$ECORE_PYTHON" -m pip check
"$ECORE_PYTHON" -m pip freeze > "$ECORE_PROJECT_ROOT/ecore-weather/environment-resolved.txt"
```

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
qsub -A "$ECORE_PBS_ACCOUNT" -q debug -l walltime=01:00:00 \
  -v ECORE_PROJECT_ROOT,ECORE_PYTHON,ECORE_REVISION,ECORE_QUOTA_FREE_GIB,ECORE_PBS_MODE \
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
qsub -A "$ECORE_PBS_ACCOUNT" \
  -v ECORE_PROJECT_ROOT,ECORE_PYTHON,ECORE_REVISION,ECORE_QUOTA_FREE_GIB,ECORE_PBS_MODE \
  jobs/polaris_mrms.pbs
```

The default production request is one node in `capacity` for 24 hours. Confirm
current [queue limits](https://docs.alcf.anl.gov/polaris/running-jobs/) first.
Use normal `qstat -was1 JOBID`, `qstat -f JOBID` and `qstat -x -f JOBID` to inspect
queue comments, current state and completed exit status. If access is denied,
report that limitation instead of trying alternative access routes.

### Progress and recovery

```bash
watch -n 30 -t "$ECORE_PYTHON scripts/dataset_jobs.py status mrms \
  --progress $ECORE_PROJECT_ROOT/ecore-weather/state/study/progress.json"
```

The compact shared progress file is updated approximately every 30 seconds.
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

Annual workstation radar-map calculations remain gated on a successful pilot
and a verified production month for both reflectivity products.
