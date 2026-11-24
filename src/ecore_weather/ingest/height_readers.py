"""Independent HDF5 reads for an ordered ACHA archive writer."""
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
import multiprocessing
from time import perf_counter
from types import SimpleNamespace

from ecore_weather.core.common import Transport

_transport = None
_context = None


def initialize(selection):
    global _transport, _context
    _transport = Transport('obstore')
    _context = selection


def read(asset):
    from ecore_weather.ingest.monthly_stream import _read_new

    before = _transport.bytes
    start = perf_counter()
    ds, timings = _read_new(asset, _context, None, _transport, 1024**2)
    return ds, timings, _transport.bytes - before, perf_counter() - start


def ordered_height_reads(rows, selection, scratch, metrics, *, processes, admitted, roi_mib):
    if not rows:
        return
    context = SimpleNamespace(source=selection.source, bbox=selection.bbox,
                              product=selection.product, bands=selection.bands)
    budget = roi_mib * 1024**2
    pool = ProcessPoolExecutor(max_workers=processes, mp_context=multiprocessing.get_context('spawn'),
                               initializer=initialize, initargs=(context,))
    try:
        # Use the first read to size the queue, then share the budget across
        # its slots. Per-scan metadata can vary on the same science grid.
        first = pool.submit(read, rows[0]['asset']).result()
        first_cost = 2 * max(1, first[0].nbytes)  # Include IPC copies.
        if first_cost > budget:
            raise MemoryError('One ACHA payload exceeds the configured ROI budget')
        limit = min(admitted, len(rows), budget // first_cost)
        reserve = budget // limit
        metrics.update(reader_processes=processes, admitted_observations_limit=limit,
                       roi_queue_budget_bytes=budget, roi_reserved_per_observation=reserve)
        pending, ready = {}, {0: first}
        submitted, expected = 1, 0
        while expected < len(rows):
            while submitted < len(rows) and len(pending) + len(ready) < limit:
                pending[pool.submit(read, rows[submitted]['asset'])] = submitted
                submitted += 1
            metrics['active_readers'] = min(processes, len(pending))
            if expected not in ready:
                start = perf_counter()
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                metrics['writer_idle_seconds'] = metrics.get('writer_idle_seconds', 0) + perf_counter() - start
                for future in done:
                    index = pending.pop(future)
                    try:
                        result = future.result()
                    except Exception as exc:
                        raise OSError(f"ACHA read failed for {rows[index]['asset'].key}") from exc
                    if result[0].nbytes * 2 > reserve:
                        raise MemoryError(f"ACHA payload for {rows[index]['asset'].key} exceeds "
                                          f"its {reserve}-byte reservation; increase --height-roi-mib "
                                          "or reduce --height-prefetch-observations")
                    ready[index] = result
                continue
            ds, timing, size, elapsed = ready.pop(expected)
            metrics['read_bytes'] = metrics.get('read_bytes', 0) + size
            metrics['reader_seconds'] = metrics.get('reader_seconds', 0) + elapsed
            yield rows[expected], (ds, timing)
            expected += 1
    finally:
        pool.shutdown(wait=True, cancel_futures=True)
