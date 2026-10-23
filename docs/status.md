# Polaris GOES memory stop — October 23, 2026

The user-supplied tail for **7776485** shows complete eight-band archives for
May–July 2021 and admission of August–October. It ended with **exit 1** when
summed process RSS exceeded a self-imposed 64 GiB ceiling by about 1.05 MiB.
There is no node-OOM or PBS memory-limit evidence in that excerpt. It contains
no exact durable partial counts for the newly admitted months.

Production now permits 384 GiB summed RSS, warns at 256 GiB, and uses a
checkpointed pause for sustained pressure or critically low available node RAM.
One bounded atomic `memory.json` and the durable status snapshot report node
availability and the largest processes. Pilot and benchmark ceilings remain
strict. The concurrency, source arrays, archive layout, Eagle budget and
workstation settings are unchanged. No remote restart or long-run RAM result
is claimed from local implementation tests. See [policy and recovery commands](study_jobs.md#goes-memory-protection-and-recovery).

The status footer also reports run aggregate average and recent average/median
returned-data rates. Remote `--progress` monitoring now persists its bounded
local sampling cache across `watch` invocations; duplicated snapshots and prior
attempt counters cannot manufacture a speed. These are application byte-counter
measurements, not NIC bandwidth or a whole-study median.

Local validation: **169 passed, one skipped** across RAM-policy/PBS controls,
aggregate bandwidth/status, exact native archives, shared scheduling and durable
recovery. Tests reproduce the supplied 64 GiB crossing with synthetic counters,
exercise sustained and transient pressure, then cancel a real local writer and
resume verified data with fresh scratch and zero extra sample downloads.
Shell syntax and Python compilation also passed. No new network benchmark or
Polaris submission ran during this correction.

# GOES pause and cross-job recovery — October 22, 2026

The user-supplied `7770028` pause record reports all 32 January–April 2021
band archives complete/reused after 30,280.1 seconds (8:24:40). The old
130%-of-a-month drain allowance explains stopping before the sixteen-hour
request expired; this was not a second DuckDB failure. Later incomplete months
are not present in this successful drain. All four C02 archives were reused,
so the record does not measure new C02 range transfers.

The updated Polaris profile uses three normal month owners plus the existing
one C02 tail and unchanged global download/read pools. The revised study wrapper
enables incremental quarter-month checkpoints on Eagle, restoring verified
chunks to fresh local scratch. Controlled shutdown also attempts to publish the
last verified scan batch. Sudden interruption may still lose the work since
the last durable checkpoint. The final ZIP layout is unchanged. Future pause
records are compact and state the reason explicitly. This is a local
implementation, not yet a validated live Polaris restart.

Validation: **136 passed, one skipped** across GOES native/monthly storage,
shared scheduling, durable checkpoint recovery, PBS, status and resume tests.
The recovery checks erase simulated node scratch, restore verified partials,
fetch only the remaining scans and compare CMI, DQF and per-scan metadata.
Corrupt copies, interrupted publication, storage accounting and the revised
admission deadline are covered. Shell syntax, Python compilation and an explicit
2021 CLI dry run also passed. No new NOAA benchmark or remote submission ran.

# GOES coordinator recovery — October 21, 2026

User-supplied output for **7765036** identifies an in-process DuckDB RO/RW
connection-mode race during coordinator task updates. The internal progress
monitor now uses the writable mode under the coordinator lock; external viewers
stay read-only. Repeated cancellation tracebacks no longer obscure the root
error. This is not evidence of a source-file or scientific archive failure.
The failure was reproduced locally with DuckDB 1.5.6; the corrected monitor
passed deterministic connection-overlap checks and the broader 132-test local
regression run (one mock-only case skipped). Source-array round trips,
interruption/reuse and status behavior passed. No long Polaris rerun has yet
validated this correction.

The final table contains 79,389 verified band observations out of 137,497
selected for admitted January–April contexts. Only January C02 is reported
complete on Eagle (4,445 observations); 74,944 were still node-local and should
be treated as requiring refetching after PBS cleanup. Returned source bytes sum
to about 2.52 TB, approximately 940 Mb/s over the 5:56:43 run. Per-band rates
are not NIC rates. These are user-provided final-snapshot measurements, not a
fresh remote inventory. See [recovery details and commands](study_jobs.md#recovery-after-goes-job-7765036).

# GOES PBS startup correction — October 20, 2026

`jobs/polaris.env.sh` now exports non-sensitive job defaults and refreshes a
conservative quota estimate from `myprojectquotas`. GOES initially selects 2021;
the longer interval remains an explicit date override. The discovery-only
command passed a live workstation NOAA check: six GOES-16 scans / 48 study-band
files for one hour, with zero imagery bytes downloaded. This is not a Polaris
proxy test. Configuration, quota, discovery and wrapper regressions passed
78 focused tests (one mock-only case skipped), plus shell/compile checks.

User-reported GOES pilot **7762801** failed with exit 1 and empty output. It
received a compute node; timestamps span ten seconds and sampled CPU time is
zero. `Stageout_status = 1` reports successful stage-out. The old wrapper's
silent Git guards reproduce the empty-output symptom locally; the exact remote
cause is unconfirmed. GOES pilot validation remains incomplete. Job **7762846**
was the later study submission; its current state has not been verified here.

Both PBS wrappers now omit commit/clean-checkout/code-hash gates and the required
revision variable. Operational code-lineage inspection is off by default;
explicitly requested metadata is best effort and cannot stop jobs. Startup,
phase and exit messages plus one overwritten startup log make subsequent errors
visible. Scientific array/archive checks remain in place. The correction passed
117 local PBS/scheduler/profile/status checks (one application-mock case was
skipped), shell syntax and Python compilation. These are local checks, not a
successful Polaris execution. Use the
[short interactive dry run](study_jobs.md#debugging-a-goes-job-that-ends-without-output)
before the bounded NOAA/storage checks and pilot. Existing queued submissions
contain the old wrapper and need fresh submission to use the correction.

# Polaris validation and annual radar figures

The implementation passed **137 focused checks**. CPU setup `7743696`, fetch
pilot `7743706` and fresh-job reuse pilot `7743711` all exited 0. Polaris tuning
`7743767` selected twelve product-month writers, four downloads each and one
decoder each: median 76.256 seconds on the matched selection and maximum
8.76 GiB job RSS. The cumulative experiment used 3.52 GiB of its 10 GiB budget.
See [measured settings and commands](study_jobs.md#polaris-pbs).

MRMS full-study job **7743827**, pinned to `2038b86`, completed with exit 0 on
October 14, 2026: PBS walltime 1:14:14, 577,665 observations, 19.06 GB returned,
12.70 GB retained and 12.2 GiB application peak process RSS. Both reflectivity
products are published through June 2026, with no unfinished product-months.
The PBS request is now six hours; its measured profile remains twelve writers,
four downloads each and one decoder each. Accounting is in node-hours.

The GOES PBS implementation passed **178 relevant local regression checks**
and its explicit-date dry run selected exactly 24 study months. The wrapper is
for **manual pilot and study submission**, not an already launched job. The initial Polaris profile uses four normal months plus
one C02 tail, 64 async downloads, eight local readers and sixteen range readers.
It is unmeasured on Polaris. The intended dates are 2021-01-01 through 2023-01-01,
end excluded, with a 400 GB GOES-only cap. See the [manual procedure](study_jobs.md#polaris-goes-manual-pilot-and-20212022-study).

The annual workstation figures are complete and validated for 2021–2025 and
January–June 2026: twelve maps, each in PNG and SVG. All five new source pilots
passed; 2022 reused its cached summary. Counts, source-selection hashes, RQI
bounds, all image files and 66 unchanged archive completion markers passed.
See the [annual results and maps](mrms_roi_and_radar_coverage.md#completed-study-period-figures).

# Current operation — October 9, 2026

GOES has finished the verified December 2022 month and exited with the planned
resumable pause (status 75); no NOAA fetch coordinator is active. The next
chronological stage can resume from completed CMIPF archives and checkpoints.
The 2022 full-CARIB MRMS [radar-quality figure](mrms_roi_and_radar_coverage.md)
is complete: 8,757 reflectivity and 8,752 RQI hourly samples, with three and
eight absent source hours. Its one-day exact source-to-archive check passed;
the annual report and PNG/SVG are linked in the coverage guide. Raw archives
were not modified.

## Earlier workstation-recovery notes — October 6

The workstation restart interrupted the previous GOES attempt. No fetch process
or tmux session survived. July–September 2021 retain eight-band completed
checkpoints. October–December 2021 and January 2022 retain all 32 partial native
band stores, with committed counts between 808 and 3,256 observations at initial
inspection. Preserve these stores; recovery verifies batch hashes and source
ordering rather than discarding downloaded progress.

Before restarting NOAA fetching, the bounded MRMS comparison completed three
matched repeats on 32 CARIB composite-reflectivity observations: median **2.979 s**
for the inherited NVIDIA reader adapted to CARIB, **3.247 s** for the project.
Exact valid values and timestamps matched. Coordinate representations differed
by at most 0.000000915 degrees, explicitly reported. Keep MRMS's four writers,
eight downloads each and one decoder each; this short source-read benchmark does
not establish a benefit from higher limits. Details are in
[pipeline measurements](goes_parallel_pipeline.md).

GOES recovery uses **two normal month owners plus one C02-only tail**, eight
global C02 range readers, 32 global asynchronous downloads for the other seven
bands, two local HDF5 readers, and 2/16 GiB ROI/staging limits. The configuration
is pinned before launch. The earliest incomplete chronological month is October
2021; later partial months remain reusable when admitted. The launcher checks
P: mount/write access and scratch capacity. Snapshot `99ac6d56fdc8ab17` launched coordinator PID **20059**, run
`c95a366ac166…`, at 20:58 UTC. October and November were admitted with two
separate month owners; their readers first recheck the existing batches.
The compact recovery receipt retains **42,648 verified band observations** and
records **240 observations** for replay across 27 damaged tails. Failed bytes
are preserved under `results/evidence/checkpoint-recovery/`; completed archives
were not modified. December and January remain intact for later admission.

Completed future monthly ZIPs are now checked when chronological admission
reaches them, rather than rereading every future archive before October starts.
This changes startup scheduling, not the verification requirement.

Use `python scripts/dataset_jobs.py status goes` and tmux
`ecore_goes_chronological` to monitor. [Study jobs](study_jobs.md) defines commands
and timestamps. [Pipeline](goes_parallel_pipeline.md) explains runtime/rate limits.

The four-product MRMS study retains its completed month checkpoints and verified
DAS archives. A later small reuse test overwrote its top-level run_config, so
`study-definition.json` describes the intended full period separately. Canonical
completion metadata remains the authority for observed coverage and values.

The current-study cleanup removed obsolete GOES run trees, reports/probes/logs,
completed selection copies and redundant annual working ZIPs after remote receipt
verification. Backup receipts moved under `results/study-mrms/backups/`.
Active scratch, current snapshot/run, index and all DAS research archives remain.
Actual removal totals are in `results/cleanup.json`; the local result tree now
mainly consists of necessary live scratch. No remote research objects were deleted.

Commit preparation checks: 50 focused regression tests passed, including cleanup
retention, pinned waiting handoff, notebook/CLI limits and status behavior.
Documentation links resolve; all four notebook pairs are clean and synchronized.
The current cleanup receipt records 9.09 GiB removed. Research data is unchanged.

October 6 storage review: nine local backend read-backs passed on six cached
radar frames; compressed standard Zarr used 0.88% of the uncompressed backend
space. This is local float32 tensor IO, not a new NOAA acquisition benchmark.
The live source comparison is now complete, as described above. The final combined
report/job/cleanup/scheduler/status checks passed 61 tests, including the real
external-reader lock retry and pinned benchmark handoff. See [pipeline measurements](goes_parallel_pipeline.md).

Current-revision recovery checks: **60 tests passed** (11 existing warnings);
all four Jupytext pairs remain synchronized and documentation links resolve.

Live recovery confirmation: November 2021 C02 advanced from **808 to 832**
verified observations. The coordinator has two month owners and eight independent
range-reader processes; asynchronous bands start source requests as their saved
prefix checks finish. This is advancing work, not only a stale launcher label.

Subsequent live confirmation: October **C03 1,040 → 1,056** and November
**C02 808 → 920**. Both async-staged and range-read paths commit new batches.
The live process tree confirms two month owners, two local HDF5 readers and
eight C02 range readers; the coordinator owns the global async download service.
