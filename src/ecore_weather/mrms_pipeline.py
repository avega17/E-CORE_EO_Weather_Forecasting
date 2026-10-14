"""Ordered MRMS source reads with independent task and ROI byte limits."""
from __future__ import annotations

from collections import deque
from concurrent.futures import ThreadPoolExecutor
import math
import time


def roi_reservation(bbox):
    """Conservative CARIB float64 measurement + uint8 bitmap + coordinate size."""
    width = math.ceil((bbox[2]-bbox[0])/.01)+2
    height = math.ceil((bbox[3]-bbox[1])/.01)+2
    return width*height*9 + (width+height)*8 + 65536


def ordered_reads(rows, selection, transport, workers, max_inflight, roi_queue_mib, metrics, reader=None):
    """Yield original order; an early slow observation always retains its reservation.

    Source buffers are transient and separate from the ROI budget. Downloaded
    compressed and inflated GRIB payloads are counted by Transport; its decode
    semaphore bounds full-grid decode allocations. The parent enforces total RSS.
    """
    from . import mrms
    budget = int(roi_queue_mib*1024**2)
    reservation = roi_reservation(selection.bbox)
    if budget < reservation:
        raise ValueError(f'ROI queue needs at least {math.ceil(reservation/1024**2)} MiB for one observation')
    admitted = min(max_inflight, budget//reservation)
    metrics.update(max_inflight=max_inflight, effective_max_inflight=admitted,
                   roi_queue_limit_bytes=budget, roi_reservation_bytes=reservation,
                   peak_roi_reserved_bytes=0, peak_roi_payload_bytes=0,
                   source_wait_seconds=0., summed_download_seconds=0., summed_decode_crop_seconds=0.,
                   summed_decode_wait_seconds=0., peak_source_compressed_bytes=0,
                   peak_source_inflated_grib_bytes=0, peak_decoded_grid_bytes=0)
    pending = deque()
    iterator = iter(rows)
    def read(row):
        ds, timing = (reader(row['asset'], selection, None, transport, 1024*1024) if reader else
                      mrms.read(row['asset'], selection.bbox, selection.product, transport))
        if ds.nbytes > reservation:
            raise MemoryError(f'Actual ROI {ds.nbytes} bytes exceeds conservative reservation {reservation}')
        return ds, timing
    with ThreadPoolExecutor(max_workers=workers) as pool:
        def enqueue():
            try:
                row = next(iterator)
            except StopIteration:
                return False
            pending.append((row, pool.submit(read, row)))
            metrics['peak_roi_reserved_bytes'] = max(metrics['peak_roi_reserved_bytes'],len(pending)*reservation)
            return True
        for _ in range(admitted):
            if not enqueue():
                break
        try:
            while pending:
                row, future = pending.popleft()
                before = time.perf_counter()
                ds, timing = future.result()
                metrics['source_wait_seconds'] += time.perf_counter()-before
                metrics['summed_download_seconds'] += timing.get('download_s',0)
                metrics['summed_decode_crop_seconds'] += timing.get('decode_crop_s',0)
                metrics['summed_decode_wait_seconds'] += timing.get('decode_wait_s',0)
                for key,source in [('peak_source_compressed_bytes','compressed_bytes'),
                                   ('peak_source_inflated_grib_bytes','inflated_grib_bytes'),
                                   ('peak_decoded_grid_bytes','decoded_grid_bytes')]:
                    metrics[key]=max(metrics[key],timing.get(source,0))
                metrics['peak_roi_payload_bytes'] = max(metrics['peak_roi_payload_bytes'],ds.nbytes)
                yield row, (ds, timing)
                del ds
                enqueue()
        finally:
            for _, future in pending:
                future.cancel()
