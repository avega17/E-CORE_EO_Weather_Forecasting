# Advanced read-only index and backup queries

The canonical live monitor is `python scripts/dataset_jobs.py status goes`.
It checks process identity and reads current aggregate counters. SQL alone cannot
prove that a job is alive. Use read-only connections, retry brief coordinator
locks, and never rebuild/replace a live index while the job writes.

```python
from pathlib import Path
import duckdb
with duckdb.connect('results/archive_index.duckdb', read_only=True) as db:
    display(db.execute(Path('docs/sql/fetch_progress.sql').read_text()).fetchdf())
```

The query uses typed band/product-month tasks, not per-source rows or ephemeral
scratch paths. Unknown attempt timestamps stay null. Completed counts and saved
checkpoints mean different things; changing table status does not verify an archive.
`goes_2026_progress.sql` is a fixed-window example for January–June 2026. For other
windows prefer CLI date filters. Inspect current table schemas before adapting SQL.

HF annual ZIPs are backup containers. They can expose small year manifests without
restoring all data, but DuckDB does not make Zarr pixels into rows automatically.
The earlier bounded remote probe found useful month-summary access; its obsolete
probe directories are removed. We do not claim that caches eliminate fresh
network transfers. Restore a selected verified month with notebook 03 before
reading bounded xarray/Zarr pixel batches, then register aggregate tables if needed.
Use known manifest/member paths rather than repeatedly scanning every annual ZIP.
See [storage](storage_and_data_management.md) for remote identity, receipts and byte representations.
