"""Single-owner, resumable materialization and random-window reading."""
from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from contextlib import contextmanager
from dataclasses import asdict
import fcntl
import hashlib
import json
import math
from pathlib import Path
import shutil
import signal
import time
import threading

import numpy as np
import pandas as pd
import xarray as xr

from ecore_weather.processing.transforms import NativeReader, atomic_json, decode_observation, geometry, regrid, scientific_id, target_grid
from ecore_weather.processing.process_catalog import load_index, recipe_from_dict


def _slots(info):
    from ecore_weather.core.common import utc
    return pd.date_range(pd.Timestamp(utc(info['start'])).ceil('10min')-pd.Timedelta(minutes=50),
                         pd.Timestamp(utc(info['end_excluded'])),freq='10min',inclusive='left')


def capacity(path, required=0, reserve_percent=5, usage=None):
    """Reserve a percentage of total filesystem capacity, not just current free."""
    if not 0 <= reserve_percent < 100:
        raise ValueError('Reserve must be between 0 and 100 percent')
    path = Path(path)
    while not path.exists(): path = path.parent
    usage = usage or shutil.disk_usage(path)
    reserve = math.ceil(usage.total * reserve_percent / 100)
    usable = max(0, usage.free-reserve)
    return {'total_bytes': usage.total, 'free_bytes': usage.free, 'reserve_bytes': reserve,
            'usable_bytes': usable, 'required_bytes': int(required), 'fits': required <= usable}


def choose_mode(mode, required, disk):
    if mode not in ('auto', 'full', 'blocks'):
        raise ValueError('Materialization mode must be auto, full or blocks')
    if mode == 'full' and required > disk['usable_bytes']:
        raise OSError(f'Complete variant needs {required/1e9:.2f} GB including temporary writes; '
                      f'{disk["usable_bytes"]/1e9:.2f} GB available after the scratch reserve. Use blocks.')
    return ('full' if required <= disk['usable_bytes'] else 'blocks') if mode == 'auto' else mode


@contextmanager
def owner(path):
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    with (path/'owner.lock').open('a') as lock:
        try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc: raise RuntimeError('This derived store already has a writer') from exc
        try: yield
        finally: fcntl.flock(lock, fcntl.LOCK_UN)


def _arrays_hash(arrays):
    value = hashlib.sha256()
    for name in sorted(arrays):
        array = np.ascontiguousarray(arrays[name])
        value.update(name.encode()); value.update(str(array.shape).encode()); value.update(array.dtype.str.encode())
        value.update(array.tobytes())
    return value.hexdigest()


def transform_records(records, recipe, grid, geometry_cache, parallax_cache=None):
    """One band/batch at a time; source handles and decoded grids are bounded."""
    results = []
    started = time.perf_counter()
    timing={name:0. for name in ('native_read_seconds','calibration_seconds','geometry_seconds','resampling_seconds')}
    with NativeReader(maximum=2) as reader:
        for record in records:
            if record is None:
                shape = grid.inside_bbox.shape
                results.append({'values': np.full(shape, np.nan, 'float32'), 'valid': np.zeros(shape, bool),
                    'quality': np.full(shape, 255, 'uint8'), 'classes': np.full(shape, 255, 'uint8'),
                    'source_support': np.zeros(shape, bool)})
                continue
            before=time.perf_counter()
            dataset = reader.read(record).load()
            timing['native_read_seconds']+=time.perf_counter()-before
            before=time.perf_counter()
            decoded = decode_observation(dataset, quality=recipe.quality)
            timing['calibration_seconds']+=time.perf_counter()-before
            before=time.perf_counter()
            if recipe.parallax == 'cloud-height' and record['source'] == 'goes':
                from ecore_weather.processing.process_parallax import corrected_mapping
                mapping = corrected_mapping(dataset, record, grid, parallax_cache)
            else:
                mapping = geometry(dataset, grid, 'bilinear' if record['source']=='goes' else 'nearest', geometry_cache)
            timing['geometry_seconds']+=time.perf_counter()-before
            before=time.perf_counter()
            results.append(regrid(decoded, mapping))
            timing['resampling_seconds']+=time.perf_counter()-before
    timing['transform_task_seconds']=time.perf_counter()-started
    return {name: np.stack([r[name] for r in results]) for name in results[0]},timing


def estimate(preparation, destination, sample_frames=3, memory_mib=8192, reserve_percent=5):
    """Measure transformed/compressed ROI bytes; never use full NOAA object size."""
    import zarr
    from zarr.codecs import BloscCodec, BloscShuffle
    info, aligned, windows = load_index(preparation)
    recipe = recipe_from_dict(info['recipe']); grid = target_grid(recipe)
    memory = memory_mib * 1024**2
    frame_bytes = int(grid.inside_bbox.size) * 8  # values + masks/quality/classes
    if memory < frame_bytes*3:
        raise MemoryError('Working memory cannot hold one frame plus interpolation buffers')
    selected = []
    for variable in recipe.variables:
        group = aligned[aligned.variable == variable]
        if len(group): selected.extend(group.iloc[np.unique(np.linspace(0,len(group)-1,min(sample_frames,len(group))).astype(int))].to_dict('records'))
    logical = physical = 0
    for row in selected:
        arrays, _ = transform_records([row], recipe, grid, Path(preparation)/'geometry', Path(preparation)/'parallax')
        store = zarr.storage.MemoryStore()
        root = zarr.group(store=store)
        for name, values in arrays.items():
            codec = BloscCodec(cname='zstd',clevel=3,shuffle=BloscShuffle.shuffle) if recipe.compression=='zstd' else None
            if name in ('valid','source_support'):values=values.astype('uint8')
            arr = root.create_array(name,data=values,chunks=(1,256,256),compressors=codec,
                fill_value=255 if name in ('quality','classes') else 0 if name in ('valid','source_support') else None)
            logical += values.nbytes
            physical += arr.nbytes_stored()
    ratio = physical/logical if logical else 1.0
    times = _slots(info)
    pixel_bytes = len(times)*len(recipe.variables)*frame_bytes
    coordinate_bytes = grid.nbytes + len(times)*(8+len(recipe.variables)*16)
    uncompressed = pixel_bytes+coordinate_bytes
    time_padding=math.ceil(len(times)/recipe.chunk_time)*recipe.chunk_time/len(times)
    estimated = math.ceil(pixel_bytes*max(ratio,.01)*time_padding*1.20+coordinate_bytes*1.5+1024**2)
    temporary = min(memory, frame_bytes*8*2) + grid.nbytes*2
    result = {'sample_observations':len(selected),'sample_logical_bytes':logical,'sample_stored_bytes':physical,
        'sample_storage_ratio':ratio,'logical_variant_bytes':uncompressed,'estimated_stored_bytes':estimated,
        'estimated_peak_temporary_bytes':temporary,'required_bytes':estimated+temporary,
        'shape_yx':list(grid.inside_bbox.shape),'slots':len(times),'variables':list(recipe.variables),
        'compression':recipe.compression,'time_padding_factor':time_padding,
        'scope':'production spatial chunks/fills, temporal edge padding, coordinates and 20% size allowance; validate on every committed batch',
        'disk':capacity(destination,estimated+temporary,reserve_percent)}
    atomic_json(Path(preparation)/f'estimate-{recipe.resolution_km}km-{recipe.compression}.json', result)
    return result


def _read_batch(root, start, stop):
    return {name: np.asarray(root[name][start:stop]) for name in
            ('values','valid','quality','classes','source_support','source_time_ns','source_end_ns')}


def materialize(preparation, destination='/mnt/p/ecore_eo_datasets/processed', mode='auto',
                reserve_percent=5, workers=2, memory_mib=8192, block_hours=24,
                stop=None, progress=None, max_blocks=None):
    info, aligned, windows = load_index(preparation)
    recipe = recipe_from_dict(info['recipe']); grid = target_grid(recipe)
    root = Path(destination).expanduser().resolve()/info['selection_id']
    source_root = Path(info['archive_root']).resolve()
    if root == source_root or (root.is_relative_to(source_root) and
            root.relative_to(source_root).parts[0] in ('goes','mrms')):
        raise ValueError('Derived destination must be separate from native archives')
    size = estimate(preparation,destination,memory_mib=memory_mib,reserve_percent=reserve_percent)
    selected_mode = choose_mode(mode,size['required_bytes'],size['disk'])
    slots = _slots(info)
    root.mkdir(parents=True,exist_ok=True)
    outcomes = []
    if selected_mode == 'full':
        partitions = [(0,len(slots))]
    else:
        if block_hours < 1: raise ValueError('Block hours must be positive')
        width = block_hours*6
        partitions = [(max(0,i-5), min(i+width,len(slots))) for i in range(0,len(slots),width)]
        if max_blocks is None:max_blocks=2
    built=0
    for number,(first,last) in enumerate(partitions):
        unit = root/('full' if selected_mode=='full' else f'block-{number:05d}')
        if max_blocks is not None and built>=max_blocks and not (unit/'complete.json').exists():break
        result = _materialize_unit(preparation,unit,recipe,grid,slots[first:last],aligned,
            reserve_percent,workers,memory_mib,stop,progress)
        outcomes.append(result)
        if not result.get('reused'):built+=1
        if result['status'] != 'complete':break
    result = {'status':'complete' if len(outcomes)==len(partitions) and all(r['status']=='complete' for r in outcomes) else 'paused',
        'mode':selected_mode,'selection_id':info['selection_id'],'destination':str(root),
        'estimate':size,'units':outcomes,'total_units':len(partitions),
        'random_sampling': 'all eligible windows' if selected_mode=='full' else 'within available temporal blocks; global sampler is future work'}
    if result['status']=='paused':
        result['pause_reason']=outcomes[-1].get('pause_reason','bounded block working set is ready; use the block consumer or resume') if outcomes else 'no block admitted'
    if result['status']=='complete':
        windows.to_parquet(root/'windows.parquet',index=False)
        atomic_json(root/'complete.json',{k:v for k,v in result.items() if k!='estimate'})
    atomic_json(root/'summary.json', result)
    return result


def _materialize_unit(preparation,path,recipe,grid,slots,aligned,reserve_percent,workers,memory_mib,stop,progress):
    import icechunk as ic
    import zarr
    from zarr.codecs import BloscCodec,BloscShuffle
    from earth2studio.io import IceChunkBackend
    import torch
    started = time.perf_counter()
    shape = (len(slots),len(recipe.variables),*grid.inside_bbox.shape)
    batch = math.lcm(8,recipe.chunk_time)
    # All completed worker payloads plus source/geometry buffers must fit.
    budget = memory_mib*1024**2
    payload = batch*grid.inside_bbox.size*8
    if workers < 1 or workers*payload*3 > budget:
        raise MemoryError('Reduce workers/chunk time or increase memory-mib for this native ROI')
    with owner(path):
        (path/'readers.lock').touch(exist_ok=True)
        completion = path/'complete.json'
        if completion.exists():
            result = json.loads(completion.read_text())
            validate_store(path)
            return {**result,'reused':True}
        repo_path = path/'data.icechunk'
        saved = json.loads((path/'progress.json').read_text()) if (path/'progress.json').exists() else None
        if saved:
            repo = ic.Repository.open(ic.local_filesystem_storage(str(repo_path)))
            _verify_batches(repo,saved)
            repo.reset_branch('main',saved['snapshot_id'])
        codec = BloscCodec(cname='zstd',clevel=3,shuffle=BloscShuffle.shuffle) if recipe.compression=='zstd' else []
        io = IceChunkBackend(str(repo_path), chunks={'time':recipe.chunk_time,'variable':1,'y':256,'x':256},
                            zarr_codecs=codec,blocking=True)
        coordinates = OrderedDict(time=slots.tz_localize(None).values.astype('datetime64[ns]'),
            variable=np.array(recipe.variables),y=grid.y.values,x=grid.x.values)
        if not saved:
            io.add_array(coordinates,'values')
            io.root['values'].attrs['units_by_variable'] = {v: ('dBZ' if v.startswith('refc') else '1' if int(v[3:5])<=6 else 'K') for v in recipe.variables}
            for name in ('valid','quality','classes','source_support'):
                io.root.create_array(name,shape=shape,dtype='uint8',chunks=io.root['values'].chunks,
                    dimension_names=list(coordinates),compressors=codec,
                    fill_value=255 if name in ('quality','classes') else 0)
            for name in ('latitude','longitude','inside_bbox'):
                io.root.create_array(name,data=grid[name].values,dimension_names=['y','x'],compressors=codec)
            for name in ('source_time_ns','source_end_ns'):
                io.root.create_array(name,shape=shape[:2],dtype='int64',chunks=(batch,1),
                    dimension_names=['time','variable'],fill_value=np.iinfo('int64').min,compressors=codec)
            io.root.attrs.update({'recipe':asdict(recipe),'grid':grid.attrs,'scientific_selection':str(preparation),
                'source_provenance':'aligned.parquet plus canonical archive sidecars','quality_categories':
                '0 good;1 fill;2 no coverage;3 bitmap;4 nonfinite;5 bad quality;6 range;8 unknown;9 conditional;10 calibration;255 absent observation'})
        offset = saved['verified_slots'] if saved else 0
        snapshot = saved['snapshot_id'] if saved else None
        batches = saved['batches'] if saved else []
        lookup = {(r['slot_time'],r['variable']):r for r in aligned.to_dict('records')}
        metrics = {'transform_task_seconds':0.,'write_seconds':0.,'verification_seconds':0.}
        pause_reason=None
        try:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                for first in range(offset,len(slots),batch):
                    if stop and stop():
                        pause_reason='requested signal or processing deadline'
                        break
                    last=min(len(slots),first+batch)
                    needed=min(budget,payload*workers*3)
                    if not capacity(path,needed,reserve_percent)['fits']:
                        pause_reason=f'next batch would cross the {reserve_percent:g}% filesystem free-space reserve'
                        break  # Existing verified snapshot is a resumable pause.
                    pending={};variables=iter(enumerate(recipe.variables))
                    def submit():
                        try:j,var=next(variables)
                        except StopIteration:return
                        records=[lookup.get((t.isoformat().replace('+00:00','Z'),var)) for t in slots[first:last]]
                        future=pool.submit(transform_records,records,recipe,grid,Path(preparation)/'geometry',Path(preparation)/'parallax')
                        pending[future]=(j,records)
                    for _ in range(workers):submit()
                    # Hash in variable order below after read-back; independently compare each worker payload first.
                    while pending:
                        done,_=wait(pending,return_when=FIRST_COMPLETED)
                        for future in done:
                            j,records=pending.pop(future)
                            arrays,timings=future.result()
                            for name,seconds in timings.items():metrics[name]=metrics.get(name,0.)+seconds
                            write=time.perf_counter()
                            for name,values in arrays.items():
                                io.root[name][first:last,j]=values.astype('uint8') if name in ('valid','source_support') else values
                                np.testing.assert_array_equal(io.root[name][first:last,j],values)
                            for k,r in enumerate(records,first):
                                if r:
                                    io.root['source_time_ns'][k,j]=pd.Timestamp(r['time']).value
                                    io.root['source_end_ns'][k,j]=pd.Timestamp(r['end_time']).value
                            metrics['write_seconds']+=time.perf_counter()-write
                            submit()
                    snapshot=io.commit(f'Verified derived slots {first}:{last}')
                    verify=time.perf_counter()
                    read=zarr.open_group(io.repo.readonly_session(snapshot_id=snapshot).store,mode='r')
                    checksum=_arrays_hash(_read_batch(read,first,last))
                    metrics['verification_seconds']+=time.perf_counter()-verify
                    batches.append({'start':first,'stop':last,'sha256':checksum})
                    offset=last
                    state={'schema':1,'verified_slots':last,'snapshot_id':snapshot,'batches':batches,
                           'recipe_id':recipe.identity,'slots':len(slots),'metrics':metrics}
                    atomic_json(path/'progress.json',state)
                    if progress:progress(last,len(slots),str(path))
        finally:
            io.flush()
        if offset==len(slots):
            result={'status':'complete','path':str(path),'snapshot_id':snapshot if len(slots) else None,
                'slots':len(slots),'variables':list(recipe.variables),'recipe_id':recipe.identity,
                'wall_seconds':time.perf_counter()-started,'stored_bytes':sum(p.stat().st_size for p in path.rglob('*') if p.is_file()),
                'metrics':metrics}
            atomic_json(completion,result)
            return result
        return {'status':'paused','path':str(path),'verified_slots':offset,'slots':len(slots),'pause_reason':pause_reason}


def _verify_batches(repo,state):
    import zarr
    root=zarr.open_group(repo.readonly_session(snapshot_id=state['snapshot_id']).store,mode='r')
    recipe=recipe_from_dict(root.attrs['recipe'])
    if recipe.identity!=state['recipe_id']:
        raise IOError('Derived scientific recipe differs from its checkpoint')
    grid=target_grid(recipe)
    if root['values'].shape[0]!=state['slots']:
        raise IOError('Derived time axis differs from its checkpoint')
    for name in ('x','y','latitude','longitude','inside_bbox'):
        np.testing.assert_array_equal(root[name][:],grid[name].values,
            err_msg=f'Derived {name} differs from the declared grid')
    np.testing.assert_array_equal(root['variable'][:],np.asarray(recipe.variables))
    next_row=0
    for batch in state['batches']:
        if batch['start']!=next_row:raise IOError('Derived checkpoint prefix is not contiguous')
        if _arrays_hash(_read_batch(root,batch['start'],batch['stop'])) != batch['sha256']:
            raise IOError('Derived snapshot differs from its verified values')
        next_row=batch['stop']
    if next_row!=state['verified_slots']:raise IOError('Derived checkpoint length differs')


def validate_store(path):
    import icechunk as ic
    path=Path(path)
    state=json.loads((path/'progress.json').read_text())
    repo=ic.Repository.open(ic.local_filesystem_storage(str(path/'data.icechunk')))
    _verify_batches(repo,state)
    return {'status':'verified','slots':state['verified_slots'],'snapshot_id':state['snapshot_id']}


@contextmanager
def reader_lease(path):
    """Prevent the optional block consumer from retiring a store in active use."""
    path=Path(path)
    if not path.is_dir():raise FileNotFoundError(path)
    lease=path/'readers.lock'
    with lease.open('rb' if lease.exists() else 'a') as lock:
        fcntl.flock(lock,fcntl.LOCK_SH)
        try:yield
        finally:fcntl.flock(lock,fcntl.LOCK_UN)


def iter_materialized_blocks(preparation,destination,block_hours=24,workers=2,
                             memory_mib=8192,reserve_percent=5,release=False):
    """Yield validated blocks while preparing the next one in the background.

    ``release=True`` explicitly retires each disposable block after its consumer
    returns. Native archives and preparation are never removed. Random sampling
    is restricted to the currently yielded block. Closing the iterator settles
    its one outstanding prefetch before returning.
    """
    if block_hours<1:raise ValueError('Block hours must be positive')
    info,aligned,windows=load_index(preparation)
    recipe=recipe_from_dict(info['recipe']);grid=target_grid(recipe);slots=_slots(info)
    root=Path(destination).resolve()/info['selection_id']
    raw=Path(info['archive_root']).resolve()
    if root==raw or (root.is_relative_to(raw) and root.relative_to(raw).parts[0] in ('goes','mrms')):
        raise ValueError('Derived destination must be separate from native archives')
    partitions=[(max(0,i-5),min(i+block_hours*6,len(slots))) for i in range(0,len(slots),block_hours*6)]
    def prepare(number):
        first,last=partitions[number];path=root/f'block-{number:05d}'
        result=_materialize_unit(preparation,path,recipe,grid,slots[first:last],aligned,
            reserve_percent,workers,memory_mib,None,None)
        if result['status']!='complete':raise OSError('Block preparation paused for capacity; release unused blocks or shorten block_hours')
        selected=windows[(pd.to_datetime(windows.time,utc=True)>=slots[min(first+5,last-1)]) &
                         (pd.to_datetime(windows.time,utc=True)<=slots[last-1])]
        selected.to_parquet(path/'windows.parquet',index=False)
        return path
    with ThreadPoolExecutor(max_workers=1) as pool:
        future=pool.submit(prepare,0)
        for number in range(len(partitions)):
            path=future.result()
            future=pool.submit(prepare,number+1) if number+1<len(partitions) else None
            with reader_lease(path), ProcessedSource(path) as source:
                yield source
            if release:
                # Acquire both ownership and reader exclusion before removing
                # this derived-only directory. Another consumer keeps it intact.
                with owner(path), (path/'readers.lock').open('a') as lock:
                    try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
                    except BlockingIOError:continue
                    shutil.rmtree(path)


class ProcessedSource:
    """Physical-unit Earth2Studio DataSource; random exact-time windows."""
    def __init__(self,path,allow_partial=False,cache_metadata=True):
        self.path=Path(path)
        self.cache_metadata=cache_metadata
        self._dataset=None
        self._open_lock=threading.Lock()
        if not allow_partial and not (self.path/'complete.json').is_file():
            raise ValueError('A completed materialized snapshot is required')
        self.state=json.loads((self.path/'progress.json').read_text())
        self.snapshot_id=self.state['snapshot_id']
        completion=self.path/'complete.json'
        if completion.exists():
            saved=json.loads(completion.read_text())
            if saved['snapshot_id']!=self.snapshot_id or saved['slots']!=self.state['verified_slots']:
                raise IOError('Reader snapshot differs from the verified completion record')

    def __call__(self,time,variable):
        with reader_lease(self.path):
            return self._read(time,variable)

    def _read(self,time,variable):
        import icechunk as ic
        def open_snapshot():
            repo=ic.Repository.open(ic.local_filesystem_storage(str(self.path/'data.icechunk')))
            ds=xr.open_zarr(repo.readonly_session(snapshot_id=self.snapshot_id).store,
                consolidated=False,decode_times=True,mask_and_scale=False,chunks=None)
            ds=ds.isel(time=slice(0,self.state['verified_slots']))
            ds.latitude.load();ds.longitude.load()
            return ds
        if self.cache_metadata:
            with self._open_lock:
                if self._dataset is None:self._dataset=open_snapshot()
            ds=self._dataset
        else:ds=open_snapshot()
        try:
            stamps=pd.to_datetime(np.atleast_1d(time).tolist(),utc=True).tz_localize(None).values
            chosen=ds.sel(time=stamps,variable=list(np.atleast_1d(variable)))
            result=chosen['values'].where(chosen.valid.astype(bool)).load()
            result=result.assign_coords(latitude=ds.latitude.load(),longitude=ds.longitude.load())
            return result
        finally:
            if not self.cache_metadata:ds.close()

    def close(self):
        """Call after outstanding reads finish; no image cache is retained."""
        with self._open_lock:
            if self._dataset is not None:self._dataset.close();self._dataset=None

    def __enter__(self):return self
    def __exit__(self,*unused):self.close()
    def __del__(self):
        try:self.close()
        except Exception:pass  # Read-only handles only; interpreter shutdown is best effort.
