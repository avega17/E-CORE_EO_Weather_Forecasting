"""Bounded native-band CMIPF reads; no resampling or multiband alignment."""
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, wait
import multiprocessing
import time
from types import SimpleNamespace
from .common import Transport

_transport = None
_pool = None
_context = None


def _read_context(selection, band, block_size):
    """Send only read parameters, never a full month inventory, to workers."""
    return SimpleNamespace(source=selection.source,bbox=selection.bbox,
        product=selection.product,bands=selection.bands,band=band,block_size=block_size)


def _initialize(threads, backend, context):
    global _transport, _pool, _context
    _transport = Transport(backend)
    _pool = ThreadPoolExecutor(max_workers=threads)
    _context = context


def _batch(assets):
    from .monthly_stream import _read_new
    before = (_transport.bytes, _transport.requests, _transport.retries,
              _transport.failed_requests)
    started = time.perf_counter()
    cpu_started = time.process_time()
    transfer_before = _transport.seconds
    datasets = list(_pool.map(lambda a: _read_new(a, _context, _context.band, _transport, _context.block_size), assets))
    return {'datasets': datasets, 'payload_bytes': sum(ds.nbytes for ds, _ in datasets),
            'read_bytes': _transport.bytes-before[0], 'range_requests': _transport.requests-before[1],
            'source_retries': _transport.retries-before[2],
            'failed_requests': _transport.failed_requests-before[3],
            'reader_seconds': time.perf_counter()-started,
            'reader_cpu_seconds':time.process_time()-cpu_started,
            'transfer_task_seconds':_transport.seconds-transfer_before,
            'read_decode_crop_seconds':sum(t.get('read_decode_crop_s',0) for _,t in datasets)}


def ordered_reads(rows, selection, band, threads, processes, budget_mib, block_size, metrics, backend="obstore",
                  read_mode="range", download_concurrency=8, staging_mib=4096, staging_directory=None):
    """Probe one native frame, then reserve its actual payload for queued batches.

    Completed out-of-order batches replenish reads immediately when capacity
    permits. The ordered writer consumes results without sharing its store.
    Budget excludes process runtimes, HDF5 caches and IPC copies.
    """
    if not rows:
        return
    if read_mode in ('async_full', 'async_pipeline'):
        from .goes_staging import staged_reads, pipeline_reads
        if staging_directory is None:
            raise ValueError('Async staging requires an explicit scratch directory')
        reader = pipeline_reads if read_mode == 'async_pipeline' else staged_reads
        yield from reader(rows,selection,band,processes,download_concurrency,
            budget_mib,staging_mib,staging_directory,metrics)
        return
    if read_mode != 'range':
        raise ValueError('GOES read mode must be range or async_full')
    budget = int(budget_mib)*1024*1024
    if min(threads, processes, budget_mib) < 1:
        raise ValueError('GOES reader counts and queue budget must be positive')
    with ProcessPoolExecutor(max_workers=processes, mp_context=multiprocessing.get_context('spawn'),
                             initializer=_initialize, initargs=(threads,backend,_read_context(selection,band,block_size))) as pool:
        first = pool.submit(_batch, [rows[0]['asset']]).result()
        reserve = max(1, first['payload_bytes'])
        if reserve > budget:
            raise MemoryError('One native frame exceeds the GOES queue budget')
        def account(batch):
            for key in ('read_bytes','range_requests','source_retries','failed_requests','reader_seconds','reader_cpu_seconds','transfer_task_seconds','read_decode_crop_seconds'):
                metrics[key] = metrics.get(key,0)+batch[key]
            metrics['max_batch_payload_bytes'] = max(metrics.get('max_batch_payload_bytes',0),batch['payload_bytes'])
        account(first)
        yield rows[0], first['datasets'][0]
        # x2 reserves tolerate IPC plus the returned dataset; report payload separately.
        slots = max(1, budget//(2*reserve))
        batch_size = min(threads, max(1, slots//processes))
        max_batches = max(1, slots//batch_size)
        portions = iter((i, rows[i:i+batch_size]) for i in range(1,len(rows),batch_size))
        pending, ready = {}, {}
        next_index = 1
        def submit():
            try:
                i, portion = next(portions)
            except StopIteration:
                return False
            pending[pool.submit(_batch,[r['asset'] for r in portion])] = (i,portion)
            return True
        for _ in range(max_batches):
            if not submit():
                break
        while pending or ready:
            if next_index not in ready:
                start = time.perf_counter()
                done,_ = wait(pending,return_when=FIRST_COMPLETED)
                metrics['writer_idle_seconds'] = metrics.get('writer_idle_seconds',0)+time.perf_counter()-start
                for future in done:
                    i, portion = pending.pop(future)
                    batch = future.result()
                    if batch['payload_bytes'] > 2*reserve*len(portion):
                        raise MemoryError('Native payload grew beyond its queue reservation; scratch retained')
                    account(batch)
                    ready[i] = (portion,batch)
            while next_index in ready:
                portion,batch = ready.pop(next_index)
                for row,result in zip(portion,batch['datasets']):
                    yield row,result
                next_index += len(portion)
            while len(pending)+len(ready)<max_batches and submit():
                pass
