"""Global GOES download/read budgets and exclusive monthly store owners.

Only the coordinator owns the pools and admission ledgers. Monthly processes
own their separate native-band stores; their threads never read remote HDF5.
"""
from __future__ import annotations

import asyncio
from collections import deque
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import asdict, dataclass
import multiprocessing as mp
from pathlib import Path
import queue
import threading
import time
from types import SimpleNamespace

MIB = 1024**2


@dataclass(frozen=True)
class SharedConfig:
    month_writers: int = 2
    tail_months: int = 2
    download_concurrency: int = 32
    local_readers: int = 4
    range_readers: int = 4
    roi_budget_mib: int = 2048
    staging_mib: int = 16384
    block_size: int = MIB
    finalizers: int = 2
    schema: int = 2

    def validate(self):
        if self.schema != 2:
            raise ValueError('Shared GOES configuration requires schema 2')
        if self.tail_months < 0 or min(self.month_writers, self.download_concurrency,
                self.local_readers, self.range_readers, self.roi_budget_mib,
                self.staging_mib, self.block_size, self.finalizers) < 1:
            raise ValueError('Positive global limits and nonnegative tail slots required')
        return self

    def manifest(self):
        return {**asdict(self), 'scope': 'global across all admitted months',
                'band_modes': {'C02': 'range', 'other': 'async_pipeline'}}


class MonthSlots:
    """Chronological admission with bounded additional C02-only store owners."""
    def __init__(self, normal=2, tails=2):
        if normal < 1 or tails < 0: raise ValueError('Invalid month slot counts')
        self.normal, self.tails, self.active = normal, tails, {}

    def admit(self, key, bands):
        if key in self.active: raise ValueError('Month already admitted')
        if self.normal_count >= self.normal: return False
        self.active[key] = {'remaining': set(bands), 'role': 'normal'}
        return True

    @property
    def normal_count(self): return sum(v['role'] == 'normal' for v in self.active.values())

    def verified(self, key, band):
        self.active[key]['remaining'].discard(band)
        self.rebalance()

    def rebalance(self):
        tails = sum(v['role'] == 'tail' for v in self.active.values())
        for entry in self.active.values():
            if entry['role'] == 'normal' and entry['remaining'] == {2} and tails < self.tails:
                entry['role'] = 'tail'; tails += 1

    def finish(self, key):
        self.active.pop(key); self.rebalance()


_RANGE_TRANSPORT = None


def _range_initialize():
    from .common import Transport
    global _RANGE_TRANSPORT
    _RANGE_TRANSPORT = Transport('obstore', 1)


def range_read(asset, bbox, band, block_size):
    from .monthly_stream import _read_new
    transport = _RANGE_TRANSPORT
    before = (transport.bytes, transport.requests, transport.retries,
              transport.failed_requests, transport.seconds)
    started = time.perf_counter(); cpu = time.process_time()
    context = SimpleNamespace(source='goes', bbox=bbox, product='ABI-L2-CMIPF', bands=(band,))
    dataset, timing = _read_new(asset, context, band, transport, block_size)
    return dataset, {**timing, 'read_bytes': transport.bytes-before[0],
        'range_requests': transport.requests-before[1], 'source_retries': transport.retries-before[2],
        'failed_requests': transport.failed_requests-before[3],
        'transfer_task_seconds': transport.seconds-before[4],
        'reader_seconds': time.perf_counter()-started, 'reader_cpu_seconds': time.process_time()-cpu}


class SharedReads:
    """One fair broker, one download event loop, and two independent read pools."""
    def __init__(self, config, context=None, pool_class=ProcessPoolExecutor):
        self.config = config.validate(); self.context = context or mp.get_context('spawn')
        self.requests = self.context.Queue()
        self.replies = {}; self.states = {}; self.stores = {}
        self.stop = threading.Event(); self.error = None; self.lock = threading.RLock()
        self.disk = self.roi = 0; self.disk_peak = self.roi_peak = 0
        self.downloads = self.locals = self.ranges = self.probes = 0
        self.max_downloads = self.max_locals = self.max_ranges = 0
        self.tasks = set(); self.order = deque()
        options = {'mp_context': self.context} if pool_class is ProcessPoolExecutor else {}
        self.local_pool = pool_class(max_workers=config.local_readers, **options)
        self.range_pool = pool_class(max_workers=config.range_readers, initializer=_range_initialize, **options)
        self.final_pool = pool_class(max_workers=config.finalizers, **options)
        self.thread = threading.Thread(target=self._background, name='goes-global-read-broker', daemon=True)

    def attach(self, month):
        reply = self.context.Queue(); self.replies[month] = reply
        return self.requests, reply

    def __enter__(self): self.thread.start(); return self

    def __exit__(self, *args):
        self.stop.set(); self.thread.join(30)
        if self.thread.is_alive(): raise RuntimeError('GOES broker did not stop; readers remain active')
        for pool in (self.local_pool, self.range_pool, self.final_pool):
            if self.error and pool is not self.final_pool and isinstance(pool,ProcessPoolExecutor):
                # Cancelled asyncio wrappers cannot stop already-running HDF5.
                # Stop only this broker's read-only children, preserving scratch.
                workers=list((getattr(pool,'_processes',None) or {}).values())
                pool.shutdown(wait=False,cancel_futures=True)
                for worker in workers:
                    if worker.is_alive():worker.terminate()
                    worker.join(5)
                    if worker.is_alive():worker.kill();worker.join(5)
            else:pool.shutdown(wait=True, cancel_futures=True)
        self.requests.close()
        for reply in self.replies.values(): reply.close()

    def _reply(self, state, message):
        self.replies[state['month']].put({'key': state['key'], **message})

    def snapshot(self):
        with self.lock:
            return {'active_downloads': self.downloads, 'active_local_readers': self.locals,
                'active_range_readers': self.ranges, 'peak_active_downloads': self.max_downloads,
                'peak_local_readers': self.max_locals, 'peak_range_readers': self.max_ranges,
                'scratch_reserved_bytes': self.disk, 'scratch_peak_bytes': self.disk_peak,
                'roi_reserved_bytes': self.roi, 'roi_queue_peak_bytes': self.roi_peak,
                'bands': {k: dict(v['metrics']) for k,v in self.states.items()}}

    def _register(self, message):
        key = message['key']
        if key in self.states: raise ValueError('Duplicate reader/store registration')
        rows = message['rows']; directory = Path(message['directory']); directory.mkdir(parents=True, exist_ok=True)
        if message['band'] != 2:
            batches = {}
            for i,row in enumerate(rows):batches.setdefault(row.get('archive_index',i)//8,[]).append(row)
            largest = max((sum(r['asset'].size for r in batch) for batch in batches.values()), default=0)
            if largest > self.config.staging_mib*MIB:
                raise MemoryError('One eight-scan checkpoint exceeds global staging capacity')
        metrics = {k:0 for k in ('read_bytes','range_requests','source_retries','failed_requests',
            'transfer_task_seconds','download_cache_seconds','cached_full_source_files',
            'read_decode_crop_seconds','reader_cpu_seconds','reader_seconds','active_downloads','peak_active_downloads')}
        self.states[key] = {**message, 'directory': directory, 'cursor':0, 'batch':None,
            'files':{}, 'dispatched':set(), 'held':{}, 'payload':None, 'metrics':metrics}
        self.order.append(key)

    async def _download(self, state, index):
        from .goes_staging import _stream_one
        from obstore.store import S3Store
        asset = state['rows'][index]['asset']
        if asset.bucket not in self.stores:
            self.stores[asset.bucket] = S3Store(asset.bucket, region='us-east-1', skip_signature=True)
        try:
            started = time.perf_counter()
            path = await asyncio.wait_for(_stream_one(asset,state['directory'],self.stores[asset.bucket],
                state['metrics'],self.semaphore,self.stop), timeout=900)
            state['metrics']['download_cache_seconds'] += time.perf_counter()-started
            state['files'][index] = path
        finally: self.downloads -= 1

    async def _read(self, state, index, reservation):
        from .goes_staging import local_read
        asset = state['rows'][index]['asset']; band = state['band']
        try:
            loop = asyncio.get_running_loop(); started = time.perf_counter()
            if band == 2:
                dataset,timing = await loop.run_in_executor(self.range_pool,range_read,asset,state['bbox'],band,self.config.block_size)
                for key in ('read_bytes','range_requests','source_retries','failed_requests','transfer_task_seconds'):
                    state['metrics'][key] += timing[key]
            else:
                dataset,timing = await loop.run_in_executor(self.local_pool,local_read,asset,state['files'][index],state['bbox'],band)
            # Account for reader IPC, broker payload and writer IPC simultaneously.
            actual = max(1,4*dataset.nbytes)
            if actual > reservation:
                raise MemoryError('Native ROI plus IPC exceeds its global reservation')
            state['payload'] = actual
            state['metrics']['reader_seconds'] += time.perf_counter()-started
            state['metrics']['read_decode_crop_seconds'] += timing.get('read_decode_crop_s',0)
            state['metrics']['reader_cpu_seconds'] += timing.get('reader_cpu_seconds',0)
            self._reply(state, {'op':'result','index':index,'result':(dataset,timing),
                                'metrics':dict(state['metrics'])})
        finally:
            if state['payload'] is None or reservation == self.config.roi_budget_mib*MIB:
                self.probes = max(0,self.probes-1)
            if band == 2: self.ranges -= 1
            else: self.locals -= 1

    async def _finalize(self, message):
        from .monthly_stream import _finalize_archive
        result = await asyncio.get_running_loop().run_in_executor(self.final_pool,_finalize_archive,*message['arguments'])
        self.replies[message['month']].put({'key':message['key'],'op':'finalized','result':result})

    def _spawn(self, coroutine):
        task = asyncio.create_task(coroutine); self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        def failed(done):
            if not done.cancelled() and done.exception() is not None:
                import traceback
                traceback.print_exception(done.exception())
                self.error = done.exception(); self.stop.set()
        task.add_done_callback(failed)

    def _messages(self):
        for _ in range(128):
            try: message = self.requests.get_nowait()
            except queue.Empty: break
            op = message['op']; key = message['key']
            if op == 'register': self._register(message); continue
            if op == 'finalize': self._spawn(self._finalize(message)); continue
            if op == 'close':
                state=self.states.pop(key,None)
                if state:
                    self.roi-=sum(state['held'].values())
                    if state['batch']:self.disk-=state['batch'][2]
                    self.order.remove(key)
                continue
            state = self.states[key]
            if op == 'release':
                self.roi -= state['held'].pop(message['index'],0)
            elif op == 'commit':
                from .goes_staging import retire_committed
                batch = state['batch']
                if batch:
                    expected = {r['asset_id'] for r in state['rows'][batch[0]:batch[1]]}
                    if not expected.issubset(message['ids']):
                        raise IOError('Checkpoint does not cover the reserved source batch')
                    retire_committed(state['directory'], expected)
                    self.disk -= batch[2]; state['batch'] = None
                    for i in range(batch[0],batch[1]): state['files'].pop(i,None)

    def _admit(self):
        # A first-frame probe reserves the full ROI budget until its payload is
        # known. Drain established streams when a ready probe waits; otherwise
        # their continuous replenishment can keep ROI credits nonzero forever.
        probe_waiting=any(state['payload'] is None and state['cursor']<len(state['rows'])
            and (self.ranges<self.config.range_readers if state['band']==2 else
                 state['cursor'] in state['files'] and self.locals<self.config.local_readers)
            for state in self.states.values())
        for _ in range(len(self.order)):
            key = self.order.popleft(); self.order.append(key); state = self.states[key]
            rows = state['rows']; band = state['band']
            if state['cursor'] >= len(rows): continue
            if band != 2 and state['batch'] is None:
                start = state['cursor']; archive_index = rows[start].get('archive_index', start)
                end = min(start + (8-archive_index%8),len(rows))
                reserved = sum(r['asset'].size for r in rows[start:end])
                if self.disk+reserved > self.config.staging_mib*MIB: continue
                import shutil
                if shutil.disk_usage(state['directory']).free < reserved+128*MIB:
                    raise OSError('Insufficient Linux scratch for the next GOES checkpoint')
                state['batch'] = (start,end,reserved); self.disk += reserved
                self.disk_peak = max(self.disk_peak,self.disk)
            end = len(rows) if band == 2 else state['batch'][1]
            # At most one request per band per round, avoiding C01 monopolization.
            if band != 2 and self.downloads < self.config.download_concurrency:
                candidates = [i for i in range(state['cursor'],end) if i not in state['dispatched']]
                if candidates:
                    i = candidates[0]; state['dispatched'].add(i); self.downloads += 1
                    self.max_downloads = max(self.max_downloads,self.downloads)
                    self._spawn(self._download(state,i))
            i = state['cursor']
            if band != 2 and i not in state['files']: continue
            if band == 2 and self.ranges >= self.config.range_readers: continue
            if band != 2 and self.locals >= self.config.local_readers: continue
            if probe_waiting and state['payload'] is not None:continue
            reserve = state['payload'] or self.config.roi_budget_mib*MIB
            # First probes are exclusive; afterwards admission is in source order.
            if self.roi+reserve > self.config.roi_budget_mib*MIB: continue
            self.roi += reserve; state['held'][i] = reserve; self.roi_peak = max(self.roi_peak,self.roi)
            state['cursor'] += 1
            if state['payload'] is None: self.probes += 1
            if band == 2: self.ranges += 1; self.max_ranges = max(self.max_ranges,self.ranges)
            else: self.locals += 1; self.max_locals = max(self.max_locals,self.locals)
            self._spawn(self._read(state,i,reserve))
        if self.order:self.order.rotate(-1)

    async def _run(self):
        self.semaphore = asyncio.Semaphore(self.config.download_concurrency)
        try:
            while not self.stop.is_set():
                with self.lock:
                    self._messages(); self._admit()
                await asyncio.sleep(.005)
        finally:
            for task in list(self.tasks): task.cancel()
            await asyncio.gather(*self.tasks,return_exceptions=True)
            if self.error:
                for month,reply in self.replies.items():
                    reply.put({'key':'*','op':'error','error':f'{type(self.error).__name__}: {self.error}'})

    def _background(self):
        try: asyncio.run(self._run())
        except BaseException as exc:
            self.error = exc; self.stop.set()
            for reply in self.replies.values():reply.put({'key':'*','op':'error','error':f'{type(exc).__name__}: {exc}'})


class ReaderClient:
    """Demultiplex one month's responses to its separate ordered band writers."""
    def __init__(self, month, requests, replies):
        self.month,self.requests,self.replies = month,requests,replies
        self.condition = threading.Condition(); self.messages = {}; self.error = None
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self._receive,daemon=True); self.thread.start()

    def _receive(self):
        while not self.stopped.is_set():
            try: message = self.replies.get(timeout=.2)
            except queue.Empty: continue
            with self.condition:
                if message['op'] == 'error': self.error = message['error']
                else:self.messages.setdefault(message['key'],[]).append(message)
                self.condition.notify_all()

    def send(self,key,op,**values): self.requests.put({'month':self.month,'key':key,'op':op,**values})

    def wait(self,key,op,index=None):
        with self.condition:
            while True:
                if self.error: raise OSError(self.error)
                rows = self.messages.setdefault(key,[])
                for position,row in enumerate(rows):
                    if row['op'] == op and (index is None or row.get('index') == index):return rows.pop(position)
                self.condition.wait(.2)

    def reads(self,key,rows,selection,band,directory,metrics):
        initial = dict(metrics)
        self.send(key,'register',rows=rows,bbox=selection.bbox,band=band,directory=str(directory))
        try:
            for i,row in enumerate(rows):
                started = time.perf_counter(); response = self.wait(key,'result',i)
                for name,value in response['metrics'].items():metrics[name] = initial.get(name,0)+value
                metrics['writer_idle_seconds'] = metrics.get('writer_idle_seconds',0)+time.perf_counter()-started
                try:yield row,response['result']
                finally:self.send(key,'release',index=i)
        finally:self.send(key,'close')

    def finalize(self,key,*arguments):
        self.send(key,'finalize',arguments=arguments)
        return self.wait(key,'finalized')['result']

    def close(self):self.stopped.set(); self.thread.join(2)


def write_month(month, selection, groups, config, requests, replies, events, scratch):
    """One process owns all band stores; threads write disjoint stores only."""
    from .monthly_stream import _write_one
    import os
    os.environ['OMP_NUM_THREADS'] = '1'
    if os.getenv('ECORE_DEBUG_DUMP_SECONDS'):
        import faulthandler
        faulthandler.dump_traceback_later(float(os.environ['ECORE_DEBUG_DUMP_SECONDS']),repeat=True)
    client = ReaderClient(month,requests,replies)
    results = []
    def write(item):
        (target,band),assets = item; key = f'{month}:C{band:02}'
        try:
            result = _write_one(selection,assets,target,band,workers=1,scratch=scratch,
                read_mode='async_pipeline' if band != 2 else 'range',
                reader_factory=lambda rows,sel,b,d,metrics:client.reads(key,rows,sel,b,d,metrics),
                checkpoint_callback=lambda ids:client.send(key,'commit',ids=set(ids)) if band != 2 else None,
                finalizer=lambda *a:client.finalize(key,*a))
            result.update(band=band,month=assets[0].time[:7],attempt_ended_at=time.time())
            result.pop('assets',None)
            events.put({'op':'band_verified','month':month,'band':band,'result':result})
            return result
        except BaseException as exc:
            import traceback
            traceback.print_exc()
            client.error = f'{type(exc).__name__}: {exc}'
            with client.condition:client.condition.notify_all()
            events.put({'op':'failed','month':month,'band':band,'error':client.error})
            return {'path':target,'band':band,'status':'failed','error':client.error}
    try:
        with ThreadPoolExecutor(max_workers=max(1,len(groups))) as pool:results = list(pool.map(write,groups))
        events.put({'op':'month_done','month':month,'results':results})
    finally:client.close()


def _groups(selection,destination):
    from .monthly import _relative_path, _band
    groups = {};targets = {};root = Path(destination).resolve()
    for asset in selection.assets:
        band = _band(asset)
        if band is None:raise ValueError('Native CMIPF object has no band identity')
        identity = (asset.bucket,asset.time[:7],band)
        if identity not in targets:
            targets[identity] = str((root/_relative_path(selection,asset)).resolve())
        target = targets[identity]
        groups.setdefault((target,band),[]).append(asset)
    return list(groups.items())


def _progress(active,broker,index_results):
    """Coordinator-only aggregate updates; no source-object operational records."""
    from .index import _writer_lock,connect
    from .runlog import compact
    import json,os
    updates=[];snap=broker.snapshot()
    for key,context in active.items():
        for (target,band),assets in context['groups']:
            if band in context['verified']:continue
            metrics=snap['bands'].get(f'{key}:C{band:02}',{})
            task=context['task_progress'].get(band,{})
            if task.get('progress_path') and Path(task['progress_path']).is_file():
                try:task.update(json.loads(Path(task['progress_path']).read_text()))
                except (OSError,ValueError):pass
            elapsed=time.time()-context['started_at']
            # Before a resumed band's source reader registers, its scratch
            # metrics belong to the prior attempt. Do not divide those bytes
            # by the new attempt's few seconds of elapsed time.
            attempt_bytes=metrics.get('read_bytes',0)
            data={**compact(task),**metrics,'target':target,'attempt_started_at':context['started_at'],
                'attempt_elapsed_seconds':elapsed,'attempt_read_bytes':attempt_bytes,
                'read_bytes':task.get('initial_read_bytes',0)+attempt_bytes,
                'writer_role':context['role'],'global_limits':broker.config.manifest(),
                'global_roi_reserved_bytes':snap['roi_reserved_bytes'],
                'global_scratch_reserved_bytes':snap['scratch_reserved_bytes']}
            updates.append([str(Path(target)/'raw.zarr.zip'),os.getenv('ECORE_RUN_ID','shared-goes'),
                'goes',context['selection'].product,assets[0].time[:7],band,len(assets),
                sum(i<task.get('next_index',0) for i in context['requested_indices'][band]),0,'checkpointed' if task.get('next_index') else 'fetching',
                time.time(),json.dumps(data,default=str)])
    if index_results:
        with _writer_lock(),connect() as con:
            if updates:con.executemany('INSERT OR REPLACE INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',updates)


def fetch_contexts(requests,destination,config=None,scratch=None,index_results=True,
                   pause=None,on_complete=None,progress=None,retain_records=False,progress_total=None):
    """Consume chronological (key, selection-loader, report-dir) requests lazily.

    No new month is admitted after pause(). Admitted normal/tail writers drain.
    Selection loaders run in the coordinator, never in archive-owning processes.
    """
    from .runlog import save,scratch_root
    from .common import iso
    from datetime import datetime,timezone
    import hashlib,json,os
    config=(config or default_config()).validate();scratch=Path(scratch or scratch_root())
    scratch.mkdir(parents=True,exist_ok=True)
    context=mp.get_context('spawn');events=context.Queue();slots=MonthSlots(config.month_writers,config.tail_months)
    iterator=iter(requests);exhausted=False;active={};reports=[];failures=[];started=time.perf_counter()
    completed_observations=0
    def notify_progress(status='checkpointed'):
        if not progress:return
        count=completed_observations
        for entry in active.values():
            requested={band:len(assets) for (_,band),assets in entry['groups']}
            count+=sum(requested[band] if band in entry['verified'] else
                sum(i<task.get('next_index',0) for i in entry['requested_indices'][band])
                for band,task in entry['task_progress'].items())
        total=progress_total if progress_total is not None else count
        progress(count,total,{'status':status,'progress_kind':'aggregate'})
    with SharedReads(config,context) as broker:
        heartbeat=0
        try:
            while active or not exhausted:
                stopping=bool(pause and pause()) or bool(failures) or broker.error is not None
                if stopping:exhausted=True
                while not exhausted and slots.normal_count<config.month_writers:
                    try:key,load,report_dir=next(iterator)
                    except StopIteration:exhausted=True;break
                    selection=load() if callable(load) else load
                    if selection.source!='goes' or selection.product!='ABI-L2-CMIPF':
                        raise ValueError('Shared GOES scheduler supports native CMIPF only')
                    groups=_groups(selection,destination)
                    if not groups:
                        empty={'source':'goes','selection_id':selection.id,'selection_summary':selection.summary(),
                            'monthly_archives':[],'records':[],'interrupted':False,'wall_s':0,
                            'read_bytes':0,'stored_bytes':0,'status':'unavailable'}
                        reports.append(empty)
                        if on_complete:on_complete(key,empty)
                        continue
                    if not slots.admit(key,[band for (_,band),_ in groups]):raise RuntimeError('Invalid admission')
                    req,reply=broker.attach(key)
                    tasks={};requested_indices={}
                    for (target,band),assets in groups:
                        from .monthly_stream import _archive_rows
                        marker=Path(target)/'complete.json';prior=json.loads(marker.read_text()) if marker.is_file() else {}
                        rows=_archive_rows(selection,assets,prior)
                        selected_ids={asset.id for asset in assets}
                        requested_indices[band]=[i for i,row in enumerate(rows) if row['asset_id'] in selected_ids]
                        identity=hashlib.sha256(json.dumps({'target':str(Path(target).resolve()),
                            'ids':[r['asset_id'] for r in rows]},sort_keys=True).encode()).hexdigest()[:24]
                        path=scratch/f'goes-cmipf-{identity}'/'progress.json'
                        data=json.loads(path.read_text()) if path.is_file() else {}
                        tasks[band]={**data,'progress_path':str(path),'initial_read_bytes':data.get('read_bytes',0)}
                    process=context.Process(target=write_month,args=(key,selection,groups,config,req,reply,events,str(scratch)))
                    active[key]={'selection':selection,'selection_id':selection.id,'groups':groups,'process':process,'verified':set(),
                        'results':[],'role':'normal','started_at':time.time(),'report_dir':report_dir,'task_progress':tasks,'requested_indices':requested_indices}
                    print(f'Admitted GOES {key}: {len(groups)} separate native-band stores',flush=True)
                    process.start()
                    if pause and pause():exhausted=True;break
                try:message=events.get(timeout=.1)
                except queue.Empty:message=None
                if message:
                    key=message['month'];entry=active[key]
                    if message['op']=='band_verified':
                        band=message['band'];entry['verified'].add(band);entry['results'].append(message['result'])
                        slots.verified(key,band)
                        print(f'Verified GOES {key} C{band:02}: {message["result"]["status"]}',flush=True)
                        if index_results:
                            from .index import record_fetch
                            record_fetch({'run_id':f'{os.getenv("ECORE_RUN_ID","shared")}:{key}:C{band:02}',
                                'source':'goes','selection_id':entry['selection_id'],
                                'selection_summary':entry['selection'].summary(),'root':destination,
                                'started_at':iso(datetime.fromtimestamp(entry['started_at'],timezone.utc)),
                                'wall_s':message['result'].get('write_seconds',0),
                                'read_bytes':message['result'].get('read_bytes',0),
                                'stored_bytes':message['result'].get('stored_bytes',0),
                                'monthly_archives':[message['result']]})
                        notify_progress('archived')
                    elif message['op']=='failed':
                        failures.append(message);broker.error=OSError(message['error']);broker.stop.set()
                    elif message['op']=='month_done':
                        entry['process'].join(10)
                        if entry['process'].is_alive():raise RuntimeError('Monthly writer did not exit after completion')
                        results=message['results'];sel=entry['selection']
                        by_band={r['band']:r for r in results}
                        from .monthly import _band,_identity
                        records=[{'asset_id':a.id,'time':a.time,'source_url':a.url,'source_bytes':a.size,
                            'etag':a.etag,'product':sel.product,'band':_band(a),
                            'url':by_band[_band(a)]['path'],'status':'reused' if by_band[_band(a)]['status']=='reused' else 'archived',
                            'subset_id':_identity(sel,_band(a))} for a in sel.assets] if retain_records else []
                        report={'source':'goes','selection_id':entry['selection_id'],'selection_summary':sel.summary(),
                            'root':destination,'destination':destination,'records':records,
                            'monthly_archives':results,'read_bytes':sum(r.get('read_bytes',0) for r in results),
                            'stored_bytes':sum(r.get('stored_bytes',0) for r in results),
                            'wall_s':time.time()-entry['started_at'],'interrupted':any(r['status']=='failed' for r in results),
                            'global_config':config.manifest(),'storage_layout':'monthly native-grid Earth2Studio Zarr v3 ZIP'}
                        if entry['report_dir']:save(Path(entry['report_dir'])/f'{sel.id}-monthly.json',report)
                        if not retain_records:
                            from .runlog import compact
                            report=compact(report)
                        reports.append(report)
                        if on_complete:on_complete(key,report)
                        slots.finish(key);del active[key]
                        completed_observations+=len(sel.assets)
                        notify_progress('archived')
                        with broker.lock:
                            broker.replies.pop(key).close()
                for key,entry in active.items():entry['role']=slots.active[key]['role']
                if time.time()-heartbeat>=30:
                    if index_results or progress:_progress(active,broker,index_results)
                    notify_progress();heartbeat=time.time()
                for key,entry in active.items():
                    if entry['process'].exitcode is not None and entry['process'].exitcode!=0:
                        raise RuntimeError(f'Month writer {key} exited {entry["process"].exitcode}; scratch retained')
                rss_limit=int(os.getenv('ECORE_RSS_LIMIT_BYTES','0'))
                if rss_limit:
                    import psutil
                    root=psutil.Process();rss=sum(p.memory_info().rss for p in [root,*root.children(recursive=True)] if p.is_running())
                    if rss>rss_limit:raise MemoryError(f'GOES job RSS {rss} exceeds benchmark ceiling {rss_limit}')
                if broker.error:raise OSError(f'Global GOES pipeline failed: {broker.error}')
        finally:
            # Notify waiters before joining so a failed reader cannot strand writers.
            if active:
                broker.error=broker.error or RuntimeError('Coordinator stopping after an incomplete month')
                broker.stop.set()
                for reply in broker.replies.values():reply.put({'key':'*','op':'error','error':'Coordinator stopping; scratch retained'})
                for entry in active.values():
                    entry['process'].join(30)
                    if entry['process'].is_alive():entry['process'].terminate();entry['process'].join(10)
            events.close()
        metrics=broker.snapshot()
    return {'reports':reports,'interrupted':bool(failures),'wall_s':time.perf_counter()-started,
            'global_metrics':{k:v for k,v in metrics.items() if k!='bands'},'global_config':config.manifest(),
            'paused':bool(pause and pause())}


def fetch(selection,destination,config=None,scratch=None,report_dir=None,index_results=True,progress=None):
    """Notebook and CLI API; each satellite/month remains an independent context."""
    from dataclasses import replace
    groups={}
    for asset in selection.assets:groups.setdefault((asset.bucket,asset.time[:7]),[]).append(asset)
    requests=[]
    for (bucket,month),assets in sorted(groups.items(),key=lambda pair:(pair[0][1],pair[0][0])):
        satellite=int(bucket.removeprefix('noaa-goes'))
        requests.append((f'{month}-goes{satellite}',replace(selection,assets=assets,satellite=satellite),None))
    result=fetch_contexts(requests,str(Path(destination).resolve()),config,scratch,index_results,progress=progress,retain_records=True,progress_total=len(selection.assets))
    reports=result.pop('reports')
    report={**result,'source':'goes','selection_id':selection.id,'selection_summary':selection.summary(),
        'destination':destination,'root':str(Path(destination).resolve()),
        'monthly_archives':[a for r in reports for a in r['monthly_archives']],
        'records':[a for r in reports for a in r['records']],
        'read_bytes':sum(r['read_bytes'] for r in reports),'stored_bytes':sum(r['stored_bytes'] for r in reports)}
    if report_dir:
        from .runlog import save
        save(Path(report_dir)/f'{selection.id}-monthly.json',report)
    return report


def default_config():
    """Measured workstation defaults; explicit profiles remain portable."""
    import json
    path=Path(__file__).resolve().parents[2]/'jobs/goes_workstation.json'
    if not path.exists():return SharedConfig()
    data=json.loads(path.read_text()).get('global_config',{})
    if data.get('schema')!=2:raise ValueError('Workstation GOES defaults require schema 2')
    return SharedConfig(**{k:v for k,v in data.items() if k in SharedConfig.__dataclass_fields__}).validate()


def add_arguments(parser):
    import argparse
    defaults=default_config()
    writers=parser.add_mutually_exclusive_group()
    writers.add_argument('--month-writers',type=int,default=None,help=f'Normal monthly store-owner processes; default {defaults.month_writers}')
    writers.add_argument('--monthly-writers',type=int,default=None,help='Deprecated GOES alias for --month-writers; legacy pipeline uses band-month writers')
    parser.add_argument('--shared-profile',help='Pinned schema-2 global configuration JSON; overrides global defaults')
    parser.add_argument('--tail-months',type=int,default=defaults.tail_months,help='Additional C02-only monthly owners; share global readers and budgets')
    parser.add_argument('--local-readers',type=int,default=defaults.local_readers,help='Global local HDF5 crop processes')
    parser.add_argument('--range-readers',type=int,default=defaults.range_readers,help='Global C02 range-read processes, one source call each')
    parser.add_argument('--roi-budget-mib',type=int,default=2048,help='Global queued ROI and IPC reservation budget')
    parser.add_argument('--pipeline',choices=['shared','legacy'],default='shared',help='Concurrent native-band monthly pipelines with global download and reader limits; legacy is retained for comparisons')


def config_from_args(args):
    import warnings
    if args.monthly_writers is not None:
        warnings.warn('GOES --monthly-writers is deprecated; use --month-writers (normal months, with separate C02 tail slots)',FutureWarning)
    if args.shared_profile:
        import json
        from dataclasses import fields
        data=json.loads(Path(args.shared_profile).read_text())
        data=data.get('global_config',data)
        if data.get('schema')!=2:raise ValueError('Shared profile must have schema 2; legacy per-band files are not global limits')
        allowed={field.name for field in fields(SharedConfig)}
        return SharedConfig(**{k:v for k,v in data.items() if k in allowed}).validate()
    owners=(args.month_writers if args.month_writers is not None else
            args.monthly_writers if args.monthly_writers is not None else default_config().month_writers)
    return SharedConfig(month_writers=owners,
        tail_months=args.tail_months,download_concurrency=args.download_concurrency,
        local_readers=args.local_readers,range_readers=args.range_readers,
        roi_budget_mib=args.roi_budget_mib,staging_mib=args.staging_mib,
        block_size=args.block_size_kib*1024).validate()
