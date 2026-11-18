"""Bounded storage experiments on disposable files and derived arrays."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import shutil
import tempfile
import time

import numpy as np
import pandas as pd

from ecore_weather.processing.transforms import atomic_json


def processing_benchmark(preparation,destination,output,worker_counts=(8,16,32),repeats=1):
    """Run the actual verified writer on a bounded, cached native selection."""
    import threading
    import psutil
    from ecore_weather.processing.process_catalog import load_index
    from ecore_weather.processing.process_store import materialize,_slots
    info,_,_=load_index(preparation)
    count=len(_slots(info))
    if count>293:raise ValueError('Worker benchmarks require a selection of at most two days')
    if not worker_counts or min(worker_counts)<1 or repeats<1:raise ValueError('Use positive worker counts/repeats')
    root=Path(destination);root.mkdir(parents=True,exist_ok=True)
    methods=[('thread',2)]+[('process',n) for n in worker_counts]
    cases=[];reference=None
    receipt=Path(output)/'worker-benchmark.json'
    previous=json.loads(receipt.read_text()) if receipt.exists() else {}
    initial=previous.get('initial_screening',[])
    coordinator=psutil.Process()
    for repeat in range(repeats):
        for executor,workers in methods[repeat%len(methods):]+methods[:repeat%len(methods)]:
            stopped=threading.Event();peak=[0];cpu={}
            def sample():
                while not stopped.is_set():
                    total=0
                    for process in [coordinator,*coordinator.children(recursive=True)]:
                        try:
                            total+=process.memory_info().rss
                            usage=process.cpu_times();cpu[process.pid]=usage.user+usage.system
                        except psutil.Error:pass
                    peak[0]=max(peak[0],total);stopped.wait(.25)
            start_cpu=sum(coordinator.cpu_times()[:2]);monitor=threading.Thread(target=sample,daemon=True)
            with tempfile.TemporaryDirectory(prefix='ecore-process-workers-',dir=root) as temporary:
                monitor.start();began=time.perf_counter()
                try:
                    result=materialize(preparation,temporary,workers=workers,memory_mib=65536,
                        executor=executor,prefetch_batches=4)
                    seconds=time.perf_counter()-began
                finally:stopped.set();monitor.join()
                unit=Path(result['units'][0]['path'])
                state=json.loads((unit/'progress.json').read_text())
                hashes=[b['sha256'] for b in state['batches']]
                if reference is None:reference=hashes
                if hashes!=reference:raise IOError('Parallel transform differs from the reference arrays')
                child_cpu=sum(v for pid,v in cpu.items() if pid!=coordinator.pid)
                used_cpu=sum(coordinator.cpu_times()[:2])-start_cpu+child_cpu
                cases.append({'repeat':repeat,'executor':executor,'workers':workers,'slots':count,
                    'wall_seconds':seconds,'slots_per_second':count/seconds,'peak_summed_rss_bytes':peak[0],
                    'cpu_seconds':used_cpu,'average_cpu_cores':used_cpu/seconds,
                    'metrics':state['metrics'],'verified':True,'prefetch_batches':4})
                atomic_json(receipt,{'status':'running','cases':cases,'initial_screening':initial,
                    'scope':'native reads through close, final read-back and completion; fresh application caches, OS caches not flushed'})
    frame=pd.DataFrame(cases);summary=frame.groupby(['executor','workers']).wall_seconds.median()
    process=summary.loc['process'];best=process.min()
    winner=int(min(process[process<=best*1.05].index))
    result={'status':'complete','cases':cases,'initial_screening':initial,
        'selected_workers':winner,'selected_executor':'process',
        'selection':str(preparation),'temporary_stores_removed':True,
        'scope':'bounded production-path comparison including estimator/startup; no full-study speed guarantee'}
    atomic_json(Path(output)/'worker-benchmark.json',result)
    return result


def disk_benchmark(destination,repeats=3,size_mib=1024):
    """Include flush/close; report cache ambiguity rather than claiming cold disks."""
    root=Path(destination);root.mkdir(parents=True,exist_ok=True)
    if repeats<1 or not 8<=size_mib<=4096:raise ValueError('Use positive repeats and an 8–4096 MiB bounded file')
    rows=[]
    # Random content avoids treating highly compressible zeros as disk speed.
    block=np.random.default_rng(42).integers(0,256,size=4*1024**2,dtype='uint8').tobytes()
    count=size_mib//4;size=count*len(block)
    if shutil.disk_usage(root).free<size*3:raise OSError('Not enough space for disposable storage benchmark')
    with tempfile.TemporaryDirectory(prefix='ecore-disk-benchmark-',dir=root) as directory:
        source=Path(directory)/'source.bin';target=Path(directory)/'target.bin'
        def write(path):
            started=time.perf_counter()
            with path.open('wb',buffering=0) as stream:
                for _ in range(count):stream.write(block)
                os.fsync(stream.fileno())
            return time.perf_counter()-started
        def read():
            started=time.perf_counter();total=0
            with source.open('rb',buffering=0) as stream:
                for data in iter(lambda:stream.read(len(block)),b''):total+=len(data)
            if total!=size:raise IOError('Benchmark read length differs')
            return time.perf_counter()-started
        for repeat in range(repeats):
            seconds=write(source)
            rows.append({'repeat':repeat,'operation':'sequential-write-fsync-close','bytes':size,'seconds':seconds,'GB_per_second':size/seconds/1e9})
            seconds=read()
            rows.append({'repeat':repeat,'operation':'sequential-read','bytes':size,'seconds':seconds,'GB_per_second':size/seconds/1e9})
            started=time.perf_counter()
            with ThreadPoolExecutor(max_workers=2) as pool:
                reading=pool.submit(read);writing=pool.submit(write,target)
                reading.result();writing.result()
            seconds=time.perf_counter()-started
            rows.append({'repeat':repeat,'operation':'simultaneous-read-write','bytes':size*2,'seconds':seconds,'GB_per_second':size*2/seconds/1e9})
    return {'path':str(root.resolve()),'cases':rows,'cache_scope':'OS/filesystem caches not flushed; reads follow writes; write timing includes fsync/close',
        'direct_io':False,'temporary_stores_removed':True,'USB_rating_Gbit_s':10,
        'interpretation':'Measured filesystem throughput; not proof of physical bus saturation'}


def window_benchmark(unit,destination,repeats=3,frames=24,candidates=None,reader_counts=(1,2,4)):
    """Compare whole scientific stores through the same ProcessedSource path."""
    import zarr
    from zarr.codecs import BloscCodec
    from ecore_weather.processing.process_store import ProcessedSource
    unit=Path(unit);source=zarr.open_group(str(unit/'data.zarr'),mode='r')
    n=min(frames,source['values'].shape[0]);arrays={}
    for name in source.keys():
        array=source[name]
        arrays[name]=np.asarray(array[:n] if array.metadata.dimension_names[0]=='time' else array[:])
    if sum(a.nbytes for a in arrays.values())>2*1024**3:raise MemoryError('Layout sample exceeds 2 GiB')
    destination=Path(destination);destination.mkdir(parents=True,exist_ok=True)
    rows=[];candidates=candidates or [(1,0,0),(6,0,0),(1,24,0),(1,0,256)]
    for repeat in range(repeats):
        order=candidates[repeat%len(candidates):]+candidates[:repeat%len(candidates)]
        for chunk,shard,tile in order:
            with tempfile.TemporaryDirectory(prefix='ecore-window-benchmark-',dir=destination) as temp:
                before=time.perf_counter();store=zarr.open_group(str(Path(temp)/'data.zarr'),mode='w',zarr_format=3)
                for name,values in arrays.items():
                    original=source[name];dims=original.metadata.dimension_names;options={}
                    if values.ndim==4:
                        spatial=tuple(min(tile,d) if tile else d for d in values.shape[2:])
                        options['chunks']=(chunk,values.shape[1],*spatial)
                        if shard:options['shards']=(shard,values.shape[1],*spatial)
                    # Arrays are written once as complete time shards.
                    store.create_array(name,data=values,dimension_names=dims,
                        compressors=BloscCodec(cname='zstd',clevel=3),**options)
                store.attrs.update(dict(source.attrs))
                atomic_json(Path(temp)/'progress.json',{'schema':2,'verified_slots':n,'recipe_id':'layout-experiment'})
                atomic_json(Path(temp)/'complete.json',{'slots':n,'recipe_id':'layout-experiment'})
                write=time.perf_counter()-before
                stored=sum(p.stat().st_size for p in Path(temp).rglob('*') if p.is_file())
                files=sum(1 for p in Path(temp).rglob('*') if p.is_file())
                with ProcessedSource(temp) as reader:
                    for length in (1,6,12):
                        if length>n:continue
                        starts=np.random.default_rng(repeat+100).integers(0,n-length+1,size=8)
                        for readers in reader_counts:
                            def load(first):
                                began=time.perf_counter()
                                values=reader(arrays['time'][first:first+length],arrays['variable']).values
                                expected=np.where(arrays['pixel_valid'][first:first+length],arrays['values'][first:first+length],np.nan)
                                np.testing.assert_array_equal(values,expected)
                                return time.perf_counter()-began
                            began=time.perf_counter()
                            with ThreadPoolExecutor(max_workers=readers) as pool:latencies=list(pool.map(load,starts))
                            seconds=time.perf_counter()-began
                            rows.append({'repeat':repeat,'compression':'zstd','chunk_time':chunk,'shard_time':shard,
                                'spatial_chunk':tile,'frames':length,'readers':readers,'write_seconds':write,
                                'stored_bytes':stored,'physical_files':files,'logical_bytes':sum(a.nbytes for a in arrays.values()),
                                'windows_per_second':len(starts)/seconds,'median_window_seconds':float(np.median(latencies)),
                                'wall_seconds':seconds,'verified':True})
    import pandas as pd
    summary=pd.DataFrame(rows);six=summary[(summary.frames==6)&(summary.readers==1)]
    selected=None
    if len(six):
        grouped=six.groupby(['chunk_time','shard_time','spatial_chunk']).agg(seconds=('median_window_seconds','median'),files=('physical_files','median')).reset_index()
        best=grouped.seconds.min();winner=grouped[grouped.seconds<=best*1.05].sort_values(['files','seconds']).iloc[0]
        selected={name:int(winner[name]) for name in ('chunk_time','shard_time','spatial_chunk')}
    return {'cases':rows,'selected_layout':selected,'cache_scope':'fresh application handles; OS caches not flushed',
        'temporary_stores_removed':True,'scope':f'{n} cached regional frames including validity, quality and source times'}


def benchmark(unit,destination,output,repeats=3,size_mib=1024,linux_comparison=None):
    import psutil
    started=time.perf_counter();cpu=time.process_time()
    process=psutil.Process();before=process.io_counters()._asdict()
    result={'disk':disk_benchmark(destination,repeats,size_mib),
            'windows':window_benchmark(unit,destination,repeats),
            'model_consumption':'not measured until the model check succeeds; storage speed alone does not establish GPU starvation'}
    import zarr
    state=json.loads((Path(unit)/'progress.json').read_text())
    metadata=zarr.open_group(str(Path(unit)/'data.zarr'),mode='r')
    preparation=metadata.attrs.get('scientific_selection')
    # Native virtualization is a separate optional experiment, not routine work.
    if linux_comparison:
        result['linux_disk']=disk_benchmark(linux_comparison,repeats,min(size_mib,512))
        result['linux_windows']=window_benchmark(unit,linux_comparison,repeats)
    result['current_snapshot_reads']=compare_snapshot_metadata(unit,repeats)
    result.update(wall_seconds=time.perf_counter()-started,cpu_seconds=time.process_time()-cpu,
        process_io_before=before,process_io_after=process.io_counters()._asdict())
    atomic_json(Path(output)/'storage-benchmark.json',result)
    pd.DataFrame(result['windows']['cases']).to_csv(Path(output)/'window-benchmark.csv',index=False)
    return result


def snapshot_window_benchmark(unit,repeats=3,cache_metadata=True,samples=4):
    """Measure the actual plain-Zarr/ProcessedSource path as well as layout trials."""
    import xarray as xr
    from ecore_weather.processing.process_store import ProcessedSource
    state=json.loads((Path(unit)/'progress.json').read_text())
    with xr.open_zarr(str(Path(unit)/'data.zarr'),
                     consolidated=False,mask_and_scale=False,chunks=None) as dataset:
        times=dataset.time.values[:24];variables=dataset.variable.values.tolist()
    source=ProcessedSource(unit,cache_metadata=cache_metadata)
    expected=source(times,variables).values
    cases=[]
    for repeat in range(repeats):
        for length in (6,12):
            if len(times)<length:continue
            starts=np.random.default_rng(repeat+1000).integers(0,len(times)-length+1,size=samples)
            for readers in (1,2,4):
                def read(first):
                    began=time.perf_counter()
                    actual=source(times[first:first+length],variables).values
                    np.testing.assert_array_equal(actual,expected[first:first+length])
                    return time.perf_counter()-began
                before=time.perf_counter();cpu=time.process_time()
                with ThreadPoolExecutor(max_workers=readers) as pool:latencies=list(pool.map(read,starts))
                seconds=time.perf_counter()-before
                cases.append({'repeat':repeat,'frames':length,'readers':readers,'cache_metadata':cache_metadata,
                    'windows_per_second':len(starts)/seconds,'median_window_seconds':float(np.median(latencies)),
                    'cpu_seconds':time.process_time()-cpu,'wall_seconds':seconds,'verified':True})
    source.close()
    return {'cases':cases,'recipe_id':state['recipe_id'],'unit':str(unit),
        'scope':'actual compressed plain-Zarr store, validity masks and model-facing source; up to 24 observations',
        'cache_scope':('one immutable metadata cache per source' if cache_metadata else 'fresh store handles')+
            '; filesystem caches not flushed; tensors verified against a cached reference'}


def compare_snapshot_metadata(unit,repeats=3):
    cases=[]
    for repeat in range(repeats):
        modes=(False,True) if repeat%2==0 else (True,False)
        for cache in modes:
            result=snapshot_window_benchmark(unit,1,cache_metadata=cache,samples=2)
            for row in result['cases']:row['repeat']=repeat
            cases.extend(result['cases'])
    return {**result,'cases':cases,
        'cache_scope':'rotated metadata-cache methods; filesystem caches not flushed; no image cache; exact returned tensors checked'}


def native_window_benchmark(preparation,repeats=3):
    """Compare native ZIP and virtual references on identical packed C02/C13 data."""
    from ecore_weather.processing.process_catalog import load_index, build_catalog, open_virtual_archive
    from ecore_weather.storage.archives import open_raw
    from ecore_weather.processing.process_store import _arrays_hash
    info,aligned,_=load_index(preparation)
    build_catalog(preparation)
    archives=pd.read_parquet(Path(preparation)/'archives.parquet')
    cases=[]
    for variable in ('abi02c','abi13c'):
        available=aligned[aligned.variable==variable]
        if available.empty:continue
        path=available.path.iloc[0]
        rows=available[available.path==path].head(12)
        indices=rows.row_index.astype(int).tolist()
        record=archives[archives.path==path].iloc[0]
        expected=None
        for repeat in range(repeats):
            modes=['native','virtual'] if repeat%2==0 else ['virtual','native']
            for mode in modes:
                started=time.perf_counter()
                manager=open_raw(path) if mode=='native' else open_virtual_archive(preparation,record.archive_id)
                with manager as dataset:
                    opened=time.perf_counter()
                    names=[n for n in dataset.data_vars if n=='CMI' or n=='DQF' or n.startswith(('CMI_C','DQF_C'))]
                    arrays={n:np.asarray(dataset[n].isel(time=indices).values) for n in names}
                    checksum=_arrays_hash(arrays)
                    if expected is None:expected=checksum
                    if checksum!=expected:raise IOError('Virtual/native benchmark source pixels differ')
                    finished=time.perf_counter()
                cases.append({'variable':variable,'mode':mode,'repeat':repeat,'frames':len(indices),
                    'open_seconds':opened-started,'read_seconds':finished-opened,
                    'wall_seconds':time.perf_counter()-started,'logical_array_bytes':sum(a.nbytes for a in arrays.values()),
                    'verified':True})
    return {'cases':cases,'cache_scope':'fresh application handles; filesystem caches not flushed',
            'scope':'identical native packed measurement/quality arrays; metadata open and read costs separated'}
