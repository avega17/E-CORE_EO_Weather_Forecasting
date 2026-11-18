# Running dataset jobs

Prepare the CPU environment once and source `polaris.env.sh` for non-sensitive
configuration and a fresh quota estimate. Do not export the credential-bearing
.env file to PBS. See [setup](../docs/study_jobs.md#first-setup).

| Entry point | Purpose |
|---|---|
| polaris_mrms.pbs | One-node radar study; six-hour request |
| polaris_goes.pbs | Single-node satellite pilot or study |
| polaris_goes_multinode.pbs | Three independent date partitions |
| polaris_process.pbs | Separate derived-data processing environment |
| mrms_study.sh | Scheduler-independent MRMS wrapper |

Machine JSON profiles control resources; they do not define scientific identity.
`process_3km_study.json` selects the full-study uncorrected derived baseline.
See [processing commands and recovery](../docs/dataset_processing.md) for the
separate processing environment, cloud-height acquisition, statistics and model checks.
Single-node fetch dates use PBS_MRMS_START/END or PBS_GOES_START/END; end is
excluded. Multi-node dates come from goes_polaris_partitions.json.

Follow [MRMS submission](../docs/study_jobs.md#pilot-tuning-production),
[GOES submission](../docs/study_jobs.md#study-submission-and-monitoring), and
[three-node operation](../docs/study_jobs.md#three-node-goes-fetching).
Use qstat for queue/allocation state and dataset_jobs.py status for application
progress. Exit 75 is a resumable pause. Complete monthly archives and published
partial checkpoints survive allocation loss; uncommitted node scratch may not.
Never run overlapping owners of the same archive. Preserve the active runtime
source copy. See [recovery](../docs/study_jobs.md#progress-and-recovery).
