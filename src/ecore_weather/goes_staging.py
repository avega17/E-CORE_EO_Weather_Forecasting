"""Bounded async NOAA downloads followed by independent local HDF5 readers."""
import asyncio
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
from pathlib import Path
import shutil
import time
import numpy as np
import xarray as xr
from . import goes


async def download_batch(assets,directory,concurrency):
    import obstore
    from obstore.store import S3Store
    stores={a.bucket:S3Store(a.bucket,region='us-east-1',skip_signature=True) for a in assets}
    semaphore=asyncio.Semaphore(concurrency)
    async def fetch(asset):
        path=Path(directory)/(asset.id+'.nc')
        counted=requests=retries=0
        started=time.perf_counter()
        async with semaphore:
            for attempt in range(3):
                from .transfer_budget import active, BudgetExceeded
                budget=active();reservation=budget.reserve(asset.size) if budget else None
                size=0
                try:
                    requests+=1
                    response=await obstore.get_async(stores[asset.bucket],asset.key,
                        options={'if_match':f'"{asset.etag}"'})
                    size=0
                    with path.open('wb') as stream:
                        async for block in response.stream(min_chunk_size=1024**2):
                            counted+=len(block);size+=len(block)
                            await asyncio.to_thread(stream.write,block)
                    if budget:budget.settle(reservation,size);reservation=None
                    if size!=asset.size:raise IOError('Incomplete NOAA source download')
                    return {'path':str(path),'read_bytes':counted,'range_requests':requests,
                        'source_retries':retries,'failed_requests':0,
                        'transfer_task_seconds':time.perf_counter()-started}
                except Exception:
                    if budget and reservation is not None:budget.settle(reservation,size)
                    path.unlink(missing_ok=True)
                    if attempt==2:raise
                    retries+=1
                    await asyncio.sleep(.25*2**attempt)
    return await asyncio.gather(*(fetch(asset) for asset in assets))


def local_read(asset,path,bbox,band):
    started=time.perf_counter();cpu=time.process_time()
    with xr.open_dataset(path,engine='h5netcdf',decode_cf=False,mask_and_scale=False) as ds:
        subset=goes.subset_dataset(ds,bbox,(band,))
    subset.attrs.update(source_url=asset.url,source_etag=asset.etag,
        observation_time=asset.time,scan_end=asset.end_time,requested_bbox=list(bbox))
    return subset, {'read_decode_crop_s':time.perf_counter()-started,
                    'reader_cpu_seconds':time.process_time()-cpu}


def staged_reads(rows,selection,band,processes,concurrency,budget_mib,staging_mib,directory,metrics):
    """Download finite batches outside HDF5; retain failed scratch for inspection.

    Full-object scratch and returned ROI arrays have distinct byte limits. The
    first native frame calibrates the latter; sources stream to disk rather than
    collecting each entire object in RAM. This initial implementation overlaps
    downloads within a batch, then decodes it; it does not overlap phases.
    """
    if not rows:return
    if min(processes,concurrency,budget_mib,staging_mib)<1:raise ValueError('Staging bounds must be positive')
    directory=Path(directory);directory.mkdir(parents=True,exist_ok=True)
    # Uncommitted source files are disposable cache entries. Verified scan
    # checkpoints live outside this directory and remain the resume authority.
    for row in rows:
        (directory/(row['asset'].id+'.nc')).unlink(missing_ok=True)
    scratch_budget=staging_mib*1024**2;roi_budget=budget_mib*1024**2
    reserve=None;position=0
    with ProcessPoolExecutor(max_workers=processes,mp_context=multiprocessing.get_context('spawn')) as pool:
        while position<len(rows):
            batch=[];source_size=0
            slots=1 if reserve is None else max(1,roi_budget//(2*reserve))
            for row in rows[position:position+min(concurrency,slots)]:
                size=row['asset'].size
                if size>scratch_budget:raise MemoryError('One GOES object exceeds full-file staging budget')
                if source_size+size>scratch_budget:break
                batch.append(row);source_size+=size
            if shutil.disk_usage(directory).free<source_size+128*1024**2:
                raise OSError('Insufficient free scratch space for GOES async staging batch')
            started=time.perf_counter()
            downloads=asyncio.run(download_batch([r['asset'] for r in batch],directory,concurrency))
            metrics['download_cache_seconds']=metrics.get('download_cache_seconds',0)+time.perf_counter()-started
            metrics['scratch_peak_bytes']=max(metrics.get('scratch_peak_bytes',0),source_size)
            for item in downloads:
                for key in ('read_bytes','range_requests','source_retries','failed_requests','transfer_task_seconds'):
                    metrics[key]=metrics.get(key,0)+item[key]
            decode_started=time.perf_counter()
            futures=[pool.submit(local_read,row['asset'],item['path'],selection.bbox,band)
                     for row,item in zip(batch,downloads)]
            payload=0
            for row,item,future in zip(batch,downloads,futures):
                result=future.result();ds,timing=result
                if reserve is None:reserve=max(1,ds.nbytes)
                if ds.nbytes>2*reserve:raise MemoryError('Native ROI grew beyond async staging reservation')
                payload+=ds.nbytes
                metrics['read_decode_crop_seconds']=metrics.get('read_decode_crop_seconds',0)+timing['read_decode_crop_s']
                metrics['reader_cpu_seconds']=metrics.get('reader_cpu_seconds',0)+timing['reader_cpu_seconds']
                yield row,result
                Path(item['path']).unlink()
            metrics['reader_seconds']=metrics.get('reader_seconds',0)+time.perf_counter()-decode_started
            metrics['max_batch_payload_bytes']=max(metrics.get('max_batch_payload_bytes',0),payload)
            position+=len(batch)


# One ledger per independent store; writers release files only after checkpoint.
_LEDGERS = {}


class _Credits:
    def __init__(self, maximum):
        import threading
        self.maximum=maximum;self.used=0;self.peak=0;self.held={};self.lock=threading.Lock()

    def take(self, key, size):
        with self.lock:
            if key in self.held:return True
            if self.used+size>self.maximum:return False
            self.held[key]=size;self.used+=size;self.peak=max(self.peak,self.used)
            return True

    def release(self,key):
        with self.lock:self.used-=self.held.pop(key,0)


def retire_committed(directory, asset_ids):
    """Retire full sources after the raw scan checkpoint passes read-back."""
    directory=Path(directory)
    ledger=_LEDGERS.get(str(directory.resolve()))
    for key in asset_ids:
        (directory/(key+'.nc')).unlink(missing_ok=True)
        (directory/(key+'.receipt.json')).unlink(missing_ok=True)
        if ledger:ledger.release(key)


async def _stream_one(asset,directory,store,metrics,semaphore,stop):
    import hashlib,json,os,obstore
    path=directory/(asset.id+'.nc');receipt=directory/(asset.id+'.receipt.json')
    def cached():
        if not path.is_file() or not receipt.is_file():return False
        try:
            data=json.loads(receipt.read_text())
            if data['etag']!=asset.etag or data['size']!=asset.size or path.stat().st_size!=asset.size:return False
            digest=hashlib.sha256()
            with path.open('rb') as stream:
                for block in iter(lambda:stream.read(8*1024**2),b''):digest.update(block)
            return digest.hexdigest()==data['sha256']
        except (OSError,ValueError,KeyError):return False
    if await asyncio.to_thread(cached):
        metrics['cached_full_source_files']+=1
        return str(path)
    async with semaphore:
        metrics['active_downloads']+=1
        metrics['peak_active_downloads']=max(metrics['peak_active_downloads'],metrics['active_downloads'])
        before=time.perf_counter()
        try:
            for attempt in range(3):
                temporary=path.with_suffix('.partial');size=0;digest=hashlib.sha256();budget=None;reservation=None
                try:
                    from .transfer_budget import active
                    budget=active();reservation=budget.reserve(asset.size) if budget else None
                    metrics['range_requests']+=1
                    response=await obstore.get_async(store,asset.key,options={'if_match':f'"{asset.etag}"'})
                    with temporary.open('wb') as stream:
                        async for block in response.stream(min_chunk_size=1024**2):
                            if stop.is_set():raise asyncio.CancelledError()
                            metrics['read_bytes']+=len(block);size+=len(block);digest.update(block)
                            await asyncio.to_thread(stream.write,block)
                    if budget:budget.settle(reservation,size);reservation=None
                    if size!=asset.size:raise IOError('Incomplete NOAA source download')
                    os.replace(temporary,path)
                    from .goes_monthly import _checkpoint
                    _checkpoint(receipt,{'etag':asset.etag,'size':size,'sha256':digest.hexdigest()})
                    return str(path)
                except BaseException:
                    if budget and reservation is not None:budget.settle(reservation,size)
                    temporary.unlink(missing_ok=True);metrics['failed_requests']+=1
                    from .transfer_budget import BudgetExceeded
                    if attempt==2 or stop.is_set() or isinstance(__import__('sys').exception(),BudgetExceeded):raise
                    metrics['source_retries']+=1
                    await asyncio.sleep(.25*2**attempt)
        finally:
            metrics['active_downloads']-=1
            metrics['transfer_task_seconds']+=time.perf_counter()-before


def pipeline_reads(rows,selection,band,processes,concurrency,budget_mib,staging_mib,directory,metrics):
    """Continuous bounded downloads -> independent HDF readers -> ordered writer.

    Disk credits include completed source files until their eight-scan checkpoint.
    ROI credits include queued results and IPC reservations. No whole-selection
    arrays or full files are accumulated in memory by the downloader.
    """
    import threading
    if not rows:return
    if min(processes,concurrency,budget_mib,staging_mib)<1:raise ValueError('Positive pipeline bounds required')
    directory=Path(directory);directory.mkdir(parents=True,exist_ok=True)
    disk_limit=staging_mib*1024**2;roi_limit=budget_mib*1024**2
    # Admission is ordered. An entire checkpoint must fit or disk credits could
    # deadlock before the writer has eight scans ready to commit.
    if any(sum(r['asset'].size for r in rows[i:i+8])>disk_limit for i in range(0,len(rows),8)):
        raise MemoryError('An eight-scan source checkpoint exceeds staging budget; increase --staging-mib')
    disk=_Credits(disk_limit);roi=_Credits(roi_limit)
    _LEDGERS[str(directory.resolve())]=disk
    stop=threading.Event();condition=threading.Condition();ready={};failure=[]
    keys=('read_bytes','range_requests','source_retries','failed_requests','transfer_task_seconds',
        'read_decode_crop_seconds','reader_cpu_seconds','writer_idle_seconds','download_cache_seconds',
        'cached_full_source_files','active_downloads','peak_active_downloads','scratch_peak_bytes',
        'max_batch_payload_bytes','roi_queue_peak_bytes','reader_seconds')
    for key in keys:metrics.setdefault(key,0)
    reserve=[None]
    async def produce():
        from obstore.store import S3Store
        stores={a.bucket:S3Store(a.bucket,region='us-east-1',skip_signature=True)
                for a in (r['asset'] for r in rows)}
        semaphore=asyncio.Semaphore(concurrency)
        children=[]
        with ProcessPoolExecutor(max_workers=processes,mp_context=multiprocessing.get_context('spawn')) as pool:
            loop=asyncio.get_running_loop()
            async def one(index,row):
                asset=row['asset'];started=time.perf_counter()
                path=await _stream_one(asset,directory,stores[asset.bucket],metrics,semaphore,stop)
                metrics['download_cache_seconds']+=time.perf_counter()-started
                # Probe the first ROI before dispatching other local reads.
                while reserve[0] is None and index!=0:
                    if stop.is_set():return
                    await asyncio.sleep(.01)
                reservation=min(roi_limit,2*reserve[0]) if reserve[0] else roi_limit
                while not roi.take(index,reservation):
                    if stop.is_set():return
                    await asyncio.sleep(.01)
                started=time.perf_counter()
                ds,timing=await loop.run_in_executor(pool,local_read,asset,path,selection.bbox,band)
                metrics['reader_seconds']+=time.perf_counter()-started
                if reserve[0] is None:
                    if 2*ds.nbytes>roi_limit:raise MemoryError('One native ROI plus IPC exceeds result queue budget')
                    reserve[0]=max(1,ds.nbytes)
                if ds.nbytes>reserve[0]*2:raise MemoryError('Native ROI grew beyond pipeline reservation')
                for field in ('read_decode_crop_s','reader_cpu_seconds'):
                    key='read_decode_crop_seconds' if field=='read_decode_crop_s' else field
                    metrics[key]+=timing[field]
                metrics['max_batch_payload_bytes']=max(metrics['max_batch_payload_bytes'],ds.nbytes)
                metrics['roi_queue_peak_bytes']=max(metrics['roi_queue_peak_bytes'],roi.peak)
                with condition:ready[index]=(ds,timing);condition.notify_all()
            try:
                for index,row in enumerate(rows):
                    asset=row['asset']
                    while not disk.take(asset.id,asset.size):
                        if stop.is_set():return
                        # Propagate a failed earlier task instead of waiting forever.
                        for task in children:
                            if task.done() and not task.cancelled() and task.exception():raise task.exception()
                        await asyncio.sleep(.01)
                    if shutil.disk_usage(directory).free<asset.size+128*1024**2:
                        raise OSError('Insufficient source staging space')
                    metrics['scratch_peak_bytes']=max(metrics['scratch_peak_bytes'],disk.peak)
                    children.append(asyncio.create_task(one(index,row)))
                while any(not task.done() for task in children):
                    if stop.is_set():return
                    for task in children:
                        if task.done() and not task.cancelled() and task.exception():raise task.exception()
                    await asyncio.sleep(.01)
                await asyncio.gather(*children)
            finally:
                for task in children:
                    if not task.done():task.cancel()
                await asyncio.gather(*children,return_exceptions=True)
    def background():
        try:asyncio.run(produce())
        except BaseException as exc:
            with condition:failure.append(exc);condition.notify_all()
    thread=threading.Thread(target=background,name='goes-download-pipeline',daemon=True);thread.start()
    try:
        for index,row in enumerate(rows):
            started=time.perf_counter()
            with condition:
                while index not in ready:
                    if failure:raise failure[0]
                    condition.wait(.1)
                result=ready.pop(index)
            metrics['writer_idle_seconds']+=time.perf_counter()-started
            yield row,result
            roi.release(index)
    finally:
        stop.set();thread.join(timeout=10)
        if not thread.is_alive():_LEDGERS.pop(str(directory.resolve()),None)
