# Polaris validation and annual radar figures

The implementation passed **137 focused checks**. CPU setup `7743696`, fetch
pilot `7743706` and fresh-job reuse pilot `7743711` all exited 0. Polaris tuning
`7743767` selected twelve product-month writers, four downloads each and one
decoder each: median 76.256 seconds on the matched selection and maximum
8.76 GiB job RSS. The cumulative experiment used 3.52 GiB of its 10 GiB budget.
See [measured settings and commands](study_jobs.md#polaris-pbs).

Full-study job **7743827** is pinned to `2038b86` under
`/eagle/lighthouse-upr/ecore-weather/repo`. At the last check it was queued
because the capacity queue had reached its node limit. Leave that checkout
unchanged while queued/running. First-production-month verification remains
outstanding.

The annual workstation figures are complete and validated for 2021–2025 and
January–June 2026: twelve maps, each in PNG and SVG. All five new source pilots
passed; 2022 reused its cached summary. Counts, source-selection hashes, RQI
bounds, all image files and 66 unchanged archive completion markers passed.
See the [annual results and maps](mrms_roi_and_radar_coverage.md#completed-study-period-figures).
The live post-render PBS check still reports job 7743827 queued because of the
capacity queue's node limit; production fetching has not started.

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
