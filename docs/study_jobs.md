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
four-month run October 2021–January 2022 is draining; its successor uses two normal
month owners and one additional C02-only tail.

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
scheduler directives and submission are deferred until Argonne's node, network and
wall-time policy are known. Keep the live index on node-local/Linux storage. A
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
