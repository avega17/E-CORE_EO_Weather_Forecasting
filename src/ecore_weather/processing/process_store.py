"""Single-owner, resumable materialization and random-window reading."""
from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait, FIRST_COMPLETED
from contextlib import contextmanager, nullcontext
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
import os

import numpy as np
import pandas as pd
import xarray as xr

from ecore_weather.processing.transforms import NativeReader, atomic_json, decode_observation, geometry, regrid, scientific_id, target_grid
from ecore_weather.processing.process_catalog import load_index, recipe_from_dict

_GEOMETRY_CACHE=OrderedDict()
_GEOMETRY_LOCK=threading.Lock()
_GEOMETRY_BYTES=256*1024**2
_WORKER_STATE=None


def available_workers():
    """Use the CPUs assigned to this job, including container/PBS affinity."""
    return len(os.sched_getaffinity(0)) if hasattr(os,'sched_getaffinity') else os.cpu_count() or 1


def _initialize_worker(recipe,grid,geometry_cache,parallax_cache):
    import atexit
    import zarr
    global _WORKER_STATE
    signal.signal(signal.SIGINT,signal.SIG_IGN)
    # Parallelism comes from these processes, not another large pool per worker.
    zarr.config.set({'async.concurrency':4,'threading.max_workers':2})
    reader=NativeReader(maximum=4)
    atexit.register(reader.close)
    _WORKER_STATE=(recipe,grid,geometry_cache,parallax_cache,reader)


def _worker_transform(records):
    return transform_records(records,*_WORKER_STATE)


def cached_geometry(dataset,grid,method,cache):
    """Bounded immutable LUTs; coordinates and projection drift invalidate reuse."""
    radar='measurement' in dataset
    axes=('longitude','latitude') if radar else ('x','y')
    identity={'grid':grid.attrs,'method':method,'projection':{} if radar else dataset.goes_imager_projection.attrs}
    for axis in axes:
        values=np.ascontiguousarray(dataset[axis].values)
        identity[axis]={'dtype':values.dtype.str,'shape':values.shape,
            'sha256':hashlib.sha256(values.tobytes()).hexdigest(),'attrs':dataset[axis].attrs}
    key=scientific_id(identity)
    with _GEOMETRY_LOCK:
        if key in _GEOMETRY_CACHE:
            _GEOMETRY_CACHE.move_to_end(key);return _GEOMETRY_CACHE[key]
    result=geometry(dataset,grid,method,cache)
    size=sum(a.nbytes for a in result.values())
    with _GEOMETRY_LOCK:
        used=sum(sum(a.nbytes for a in value.values()) for value in _GEOMETRY_CACHE.values())
        while _GEOMETRY_CACHE and used+size>_GEOMETRY_BYTES:
            _,old=_GEOMETRY_CACHE.popitem(last=False);used-=sum(a.nbytes for a in old.values())
        if size<=_GEOMETRY_BYTES:_GEOMETRY_CACHE[key]=result
    return result


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


def transform_records(records, recipe, grid, geometry_cache, parallax_cache=None, reader=None):
    """One band/batch at a time; source handles and decoded grids are bounded."""
    results = []
    started = time.perf_counter()
    timing={name:0. for name in ('native_read_seconds','calibration_seconds','geometry_seconds','resampling_seconds')}
    with (nullcontext(reader) if reader is not None else NativeReader(maximum=2)) as reader:
        for record in records:
            if record is None:
                shape = grid.inside_bbox.shape
                results.append({'values': np.full(shape, np.nan, 'float32'), 'valid': np.zeros(shape, bool),
                    'quality': np.full(shape, 255, 'uint8'), 'classes': np.full(shape, 255, 'uint8'),
                    'source_support': np.zeros(shape, bool)})
                if recipe.parallax=='cloud-height':
                    results[-1].update(height_known=np.zeros(shape,'uint8'),parallax_applied=np.zeros(shape,'uint8'))
                continue
            before=time.perf_counter()
            dataset = reader.read(record).load()
            timing['native_read_seconds']+=time.perf_counter()-before
            before=time.perf_counter()
            decoded = decode_observation(dataset, quality=recipe.quality)
            timing['calibration_seconds']+=time.perf_counter()-before
            before=time.perf_counter()
            if (recipe.parallax == 'cloud-height' and record['source'] == 'goes'
                    and int(record['variable'][3:5]) in recipe.correction_bands):
                from ecore_weather.processing.process_parallax import corrected_mapping,MissingHeightError
                try:
                    mapping = corrected_mapping(dataset, record, grid, parallax_cache, max_age_minutes=recipe.height_age_minutes)
                except MissingHeightError:
                    mapping=cached_geometry(dataset,grid,'bilinear',geometry_cache).copy()
                    mapping['support']=np.zeros_like(mapping['support'])
                    timing['missing_height_observations']=timing.get('missing_height_observations',0)+1
            else:
                mapping = cached_geometry(dataset, grid, 'bilinear' if record['source']=='goes' else 'nearest', geometry_cache)
            timing['geometry_seconds']+=time.perf_counter()-before
            before=time.perf_counter()
            results.append(regrid(decoded, mapping))
            if recipe.parallax=='cloud-height':
                corrected=record['source']=='goes' and int(record['variable'][3:5]) in recipe.correction_bands
                support=mapping['support'].reshape(tuple(mapping['shape']))
                results[-1]['height_known']=support.astype('uint8') if corrected else np.zeros(support.shape,'uint8')
                results[-1]['parallax_applied']=results[-1]['height_known'].copy()
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
    frame_bytes = int(grid.inside_bbox.size) * (10 if recipe.parallax=='cloud-height' else 8)
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
            ('values','pixel_valid','quality','classes','source_support','valid','source_time_ns','source_end_ns','height_known','parallax_applied') if name in root}


def materialize(preparation, destination='/mnt/p/ecore_eo_datasets/processed', mode='auto',
                reserve_percent=5, workers=0, memory_mib=32768, block_hours=24,
                stop=None, progress=None, max_blocks=None, executor='process', prefetch_batches=4):
    workers=available_workers() if workers==0 else workers
    if workers<1 or executor not in ('process','thread') or prefetch_batches<1:
        raise ValueError('Use positive workers/prefetch-batches and a process/thread executor')
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
            reserve_percent,workers,memory_mib,stop,progress,executor,prefetch_batches)
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


def array_layout(recipe, shape):
    spatial=tuple(min(recipe.spatial_chunk,n) if recipe.spatial_chunk else n for n in shape[2:])
    chunks=(recipe.chunk_time,shape[1],*spatial)
    options={'chunks':chunks}
    if recipe.shard_time:options['shards']=(recipe.shard_time,shape[1],*spatial)
    return options


def _sync_batch(path,root,first,last):
    """Flush just this batch's physical chunks, not a walk over the study."""
    from itertools import product
    for name in ('values','pixel_valid','quality','classes','source_support','valid','source_time_ns','source_end_ns','height_known','parallax_applied'):
        if name not in root:continue
        array=root[name];width=(array.shards or array.chunks)[0]
        dimensions=array.shards or array.chunks
        other=[range(math.ceil(n/c)) for n,c in zip(array.shape[1:],dimensions[1:])]
        for index in range(first//width,(last-1)//width+1):
            for rest in product(*other):
                item=Path(path)/'data.zarr'/name/array.metadata.encode_chunk_key((index,*rest))
                try:
                    with item.open('rb') as stream:os.fsync(stream.fileno())
                except FileNotFoundError:pass  # All-fill chunks can be omitted; read-back still verifies them.


def _materialize_unit(preparation,path,recipe,grid,slots,aligned,reserve_percent,workers,memory_mib,stop,progress,
                      executor='process',prefetch_batches=4):
    import zarr
    from zarr.codecs import BloscCodec,BloscShuffle
    started=time.perf_counter()
    shape=(len(slots),len(recipe.variables),*grid.inside_bbox.shape)
    batch=math.lcm(8,recipe.chunk_time,recipe.shard_time or 1)
    budget=memory_mib*1024**2
    payload=batch*grid.inside_bbox.size*(10 if recipe.parallax=='cloud-height' else 8)
    # One all-channel batch, bounded worker results, plus decoding/geometry space.
    required=payload*(len(recipe.variables)*(prefetch_batches+1)+2*workers)+grid.nbytes*3
    if executor=='process' and workers>1:
        required+=workers*768*1024**2  # interpreter, bounded geometry and decoded source buffers
    if workers<1 or required>budget:
        raise MemoryError(f'Batch needs approximately {required/1024**2:.0f} MiB; reduce workers or increase memory-mib')
    path=Path(path)
    with owner(path):
        (path/'readers.lock').touch(exist_ok=True)
        completion=path/'complete.json'
        if completion.exists():
            result=json.loads(completion.read_text());validate_store(path)
            return {**result,'reused':True}
        saved=json.loads((path/'progress.json').read_text()) if (path/'progress.json').exists() else None
        root=zarr.open_group(str(path/'data.zarr'),mode='a',zarr_format=3)
        codec=BloscCodec(cname='zstd',clevel=3,shuffle=BloscShuffle.shuffle) if recipe.compression=='zstd' else []
        coordinates=OrderedDict(time=slots.tz_localize(None).values.astype('datetime64[ns]'),
            variable=np.array(recipe.variables),y=grid.y.values,x=grid.x.values)
        if saved:
            _verify_batches(root,saved)
            if saved['recipe_id']!=recipe.identity:raise IOError('Derived recipe differs from requested recipe')
            np.testing.assert_array_equal(root['time'][:],coordinates['time'])
        else:
            # An interrupted first uncommitted batch is disposable.
            for name in list(root.keys()):del root[name]
            for name,values in coordinates.items():
                root.create_array(name,data=values,dimension_names=[name],compressors=codec)
            layout=array_layout(recipe,shape)
            root.create_array('values',shape=shape,dtype='float32',fill_value=np.nan,
                dimension_names=list(coordinates),compressors=codec,**layout)
            root['values'].attrs['units_by_variable']={v:('dBZ' if v.startswith('refc') else '1' if int(v[3:5])<=6 else 'K') for v in recipe.variables}
            auxiliary=('pixel_valid','quality','classes','source_support')+(('height_known','parallax_applied') if recipe.parallax=='cloud-height' else ())
            for name in auxiliary:
                root.create_array(name,shape=shape,dtype='uint8',dimension_names=list(coordinates),
                    fill_value=255 if name in ('quality','classes') else 0,compressors=codec,**layout)
            for name in ('latitude','longitude','inside_bbox'):
                root.create_array(name,data=grid[name].values,dimension_names=['y','x'],compressors=codec)
            for name in ('source_time_ns','source_end_ns'):
                root.create_array(name,shape=shape[:2],dtype='int64',chunks=(batch,shape[1]),
                    dimension_names=['time','variable'],fill_value=np.iinfo('int64').min,compressors=codec)
            root.create_array('valid',shape=(len(slots),),dtype='bool',chunks=(batch,),
                dimension_names=['time'],fill_value=False,compressors=codec)
            root.attrs.update({'schema':2,'recipe':asdict(recipe),'grid':grid.attrs,
                'scientific_selection':str(preparation),'required_variables':list(recipe.variables),
                'source_provenance':'aligned.parquet plus canonical archive sidecars',
                'valid_definition':'all required observations present and each has at least one valid pixel',
                'quality_categories':'0 good;1 fill;2 no coverage;3 bitmap;4 nonfinite;5 bad quality;6 range;8 unknown;9 conditional;10 calibration;255 absent observation'})
        offset=saved['verified_slots'] if saved else 0
        eligible_slots=int(np.asarray(root['valid'][:offset]).sum()) if offset else 0
        batches=list(saved['batches']) if saved else []
        # Keep a compact positional lookup, not millions of duplicated dictionaries.
        lookup={(r.slot_time,r.variable):i for i,r in enumerate(aligned.itertuples(index=False))}
        metrics=dict(saved.get('metrics',{})) if saved else {}
        for name in ('transform_task_seconds','write_seconds','verification_seconds'):metrics.setdefault(name,0.)
        pause_reason=None
        state=saved or {'schema':2,'verified_slots':0,'batches':[], 'recipe_id':recipe.identity,'slots':len(slots),'metrics':metrics}
        atomic_json(path/'progress.json',state)
        handles={name:root[name] for name in ('values','pixel_valid','quality','classes','source_support',
            'valid','source_time_ns','source_end_ns','height_known','parallax_applied') if name in root}
        import multiprocessing
        process=executor=='process' and workers>1
        if process:
            pool=ProcessPoolExecutor(max_workers=workers,mp_context=multiprocessing.get_context('spawn'),
                initializer=_initialize_worker,initargs=(recipe,grid,Path(preparation)/'geometry',Path(preparation)/'parallax'))
        else:pool=ThreadPoolExecutor(max_workers=workers)
        starts=iter(range(offset,len(slots),batch));prefetched={}
        def admit():
            try:first=next(starts)
            except StopIteration:return
            pending={}
            for j,var in enumerate(recipe.variables):
                records=[]
                for t in slots[first:min(len(slots),first+batch)]:
                    index=lookup.get((t.isoformat().replace('+00:00','Z'),var))
                    records.append(aligned.iloc[index].to_dict() if index is not None else None)
                future=(pool.submit(_worker_transform,records) if process else
                    pool.submit(transform_records,records,recipe,grid,Path(preparation)/'geometry',Path(preparation)/'parallax'))
                pending[future]=(j,records)
            prefetched[first]=pending
        try:
            for _ in range(prefetch_batches):admit()
            for first in range(offset,len(slots),batch):
                if stop and stop():pause_reason='requested signal or processing deadline';break
                if not capacity(path,required,reserve_percent)['fits']:
                    pause_reason=f'next batch would cross the {reserve_percent:g}% filesystem free-space reserve';break
                last=min(len(slots),first+batch);n=last-first
                expected={'values':np.full((n,*shape[1:]),np.nan,'float32')}
                auxiliary=('pixel_valid','quality','classes','source_support')+(('height_known','parallax_applied') if recipe.parallax=='cloud-height' else ())
                for name in auxiliary:
                    expected[name]=np.full((n,*shape[1:]),255 if name in ('quality','classes') else 0,'uint8')
                for name in ('source_time_ns','source_end_ns'):
                    expected[name]=np.full((n,shape[1]),np.iinfo('int64').min,'int64')
                pending=prefetched.pop(first)
                while pending:
                    done,_=wait(pending,return_when=FIRST_COMPLETED)
                    for future in done:
                        j,records=pending.pop(future);arrays,timings=future.result()
                        for name,seconds in timings.items():metrics[name]=metrics.get(name,0.)+seconds
                        for name,values in arrays.items():expected['pixel_valid' if name=='valid' else name][:,j]=values
                        for k,r in enumerate(records):
                            if r:
                                expected['source_time_ns'][k,j]=pd.Timestamp(r['time']).value
                                expected['source_end_ns'][k,j]=pd.Timestamp(r['end_time']).value
                expected['valid']=((expected['source_time_ns']!=np.iinfo('int64').min).all(axis=1)
                    & expected['pixel_valid'].any(axis=(-2,-1)).all(axis=1))
                before=time.perf_counter()
                for name,array in expected.items():handles[name][first:last]=array
                _sync_batch(path,handles,first,last)
                metrics['write_seconds']+=time.perf_counter()-before
                before=time.perf_counter();actual=_read_batch(handles,first,last)
                for name,values in expected.items():np.testing.assert_array_equal(actual[name],values,err_msg=name)
                checksum=_arrays_hash(actual);metrics['verification_seconds']+=time.perf_counter()-before
                batches.append({'start':first,'stop':last,'sha256':checksum})
                offset=last
                eligible_slots+=int(expected['valid'].sum())
                state={'schema':2,'verified_slots':last,'batches':batches,'recipe_id':recipe.identity,
                    'slots':len(slots),'eligible_slots':eligible_slots,'metrics':metrics,'updated_at':time.time(),
                    'execution':{'executor':'process' if process else 'thread','workers':workers,
                        'prefetch_batches':prefetch_batches,'memory_mib':memory_mib},
                    'elapsed_seconds':(saved or {}).get('elapsed_seconds',0)+time.perf_counter()-started}
                atomic_json(path/'progress.json',state)
                if progress:progress(last,len(slots),str(path))
                if not (stop and stop()):admit()
        finally:
            for pending in prefetched.values():
                for future in pending:future.cancel()
            pool.shutdown(wait=True,cancel_futures=True)
        if offset==len(slots):
            root.store.close()
            before=time.perf_counter()
            validate_store(path)
            metrics['final_verification_seconds']=time.perf_counter()-before
            result={'status':'complete','path':str(path),'schema':2,'slots':len(slots),
                'variables':list(recipe.variables),'recipe_id':recipe.identity,'wall_seconds':time.perf_counter()-started,
                'stored_bytes':sum(p.stat().st_size for p in path.rglob('*') if p.is_file()),'metrics':metrics}
            atomic_json(completion,result);return result
        root.store.close()
        return {'status':'paused','path':str(path),'verified_slots':offset,'slots':len(slots),'pause_reason':pause_reason}


def _verify_batches(root,state):
    recipe=recipe_from_dict(root.attrs['recipe'])
    if state.get('schema')!=2 or recipe.identity!=state['recipe_id']:
        raise IOError('Derived scientific recipe differs from its checkpoint')
    grid=target_grid(recipe)
    if root['values'].shape[0]!=state['slots']:raise IOError('Derived time axis differs from its checkpoint')
    for name in ('x','y','latitude','longitude','inside_bbox'):
        np.testing.assert_array_equal(root[name][:],grid[name].values,err_msg=f'Derived {name} differs')
    np.testing.assert_array_equal(root['variable'][:],np.asarray(recipe.variables))
    handles={name:root[name] for name in ('values','pixel_valid','quality','classes','source_support',
        'valid','source_time_ns','source_end_ns','height_known','parallax_applied') if name in root}
    next_row=0
    for batch in state['batches']:
        if batch['start']!=next_row:raise IOError('Derived checkpoint prefix is not contiguous')
        if _arrays_hash(_read_batch(handles,batch['start'],batch['stop']))!=batch['sha256']:
            raise IOError('Derived store differs from its verified values')
        next_row=batch['stop']
    if next_row!=state['verified_slots']:raise IOError('Derived checkpoint length differs')


def validate_store(path):
    import zarr
    path=Path(path);state=json.loads((path/'progress.json').read_text())
    root=zarr.open_group(str(path/'data.zarr'),mode='r')
    _verify_batches(root,state)
    return {'status':'verified','slots':state['verified_slots'],'recipe_id':state['recipe_id']}


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
        self.generation=self.state['verified_slots']
        completion=self.path/'complete.json'
        if completion.exists():
            saved=json.loads(completion.read_text())
            if saved['recipe_id']!=self.state['recipe_id'] or saved['slots']!=self.state['verified_slots']:
                raise IOError('Reader snapshot differs from the verified completion record')

    def __call__(self,time,variable):
        with reader_lease(self.path):
            return self._read(time,variable)

    def _read(self,time,variable):
        def open_snapshot():
            ds=xr.open_zarr(str(self.path/'data.zarr'),
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
            result=chosen['values'].where(chosen.pixel_valid.astype(bool)).load()
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


def export_statistics(unit,train_start='2021-01-01',train_end='2025-01-01',elevation=None,batch_frames=24):
    """Population moments from training pixels only; never normalize saved values."""
    from ecore_weather.core.common import utc
    begin,end=pd.Timestamp(utc(train_start)).tz_localize(None),pd.Timestamp(utc(train_end)).tz_localize(None)
    if begin>=end:raise ValueError('Training end must follow training start')
    unit=Path(unit)
    if not (unit/'complete.json').is_file():raise ValueError('Statistics require a completed materialized store')
    with xr.open_zarr(str(unit/'data.zarr'),consolidated=False,mask_and_scale=False,chunks=None) as ds:
        times=pd.DatetimeIndex(ds.time.values);indices=np.flatnonzero((times>=begin)&(times<end))
        count=np.zeros(ds.sizes['variable'],'int64');mean=np.zeros(len(count),'float64');m2=mean.copy()
        for first in range(0,len(indices),batch_frames):
            chosen=ds.isel(time=indices[first:first+batch_frames])
            values=np.asarray(chosen['values'].values,dtype='float64')
            valid=chosen.pixel_valid.values.astype(bool)&np.isfinite(values)&ds.inside_bbox.values
            for j in range(len(count)):
                sample=values[:,j][valid[:,j]];n=sample.size
                if not n:continue
                average=sample.mean();moment=np.square(sample-average).sum();total=count[j]+n;delta=average-mean[j]
                m2[j]+=moment+delta*delta*count[j]*n/total;mean[j]+=delta*n/total;count[j]=total
        if not len(indices):raise ValueError('No observations lie in the selected training period')
        bad=[str(ds.variable.values[j]) for j in range(len(count)) if count[j]==0 or m2[j]<=0]
        if bad:raise ValueError(f'Cannot normalize empty or constant training channels: {bad}')
        stats={'values':{str(v):{'mean':float(mean[j]),'std':float(np.sqrt(m2[j]/count[j]))}
            for j,v in enumerate(ds.variable.values)}}
        invariants=ds[['latitude','longitude','inside_bbox']].load()
        if elevation:
            from ecore_weather.processing.process_model import regional_topography
            invariants['topography']=(('y','x'),regional_topography(elevation,ds.latitude.values,ds.longitude.values))
        import zarr
        from zarr.codecs import BloscCodec
        target=unit/'invariants.zarr';group=zarr.open_group(str(target),mode='w',zarr_format=3)
        for name in ('x','y'):group.create_array(name,data=ds[name].values,dimension_names=[name])
        for name in invariants:
            group.create_array(name,data=invariants[name].values,dimension_names=['y','x'],compressors=BloscCodec(cname='zstd',clevel=3))
        with xr.open_zarr(str(target),consolidated=False,mask_and_scale=False) as check:
            for name in invariants:np.testing.assert_array_equal(check[name].values,invariants[name].values)
        atomic_json(unit/'stats.json',stats)
        receipt={'status':'complete','training_start':str(begin),'training_end_excluded':str(end),
            'counts':dict(zip(map(str,ds.variable.values),map(int,count))), 'training_slots':len(indices),
            'normalization':'population moments for future training; pretrained inference uses checkpoint normalizers',
            'recipe_id':json.loads((unit/'progress.json').read_text())['recipe_id']}
        atomic_json(unit/'stats-summary.json',receipt)
        return receipt


def training_windows(preparation,split_start,split_end):
    """Six past states plus one target wholly contained in a half-open split."""
    from ecore_weather.processing.transforms import window_index
    from ecore_weather.core.common import utc
    info,aligned,_=load_index(preparation)
    begin,end=utc(split_start),utc(split_end)
    if begin>=end:raise ValueError('Invalid training split')
    selected=aligned[(pd.to_datetime(aligned.slot_time,utc=True)>=begin)&(pd.to_datetime(aligned.slot_time,utc=True)<end)]
    return window_index(selected,begin,end,recipe_from_dict(info['recipe']).variables,targets=1)
