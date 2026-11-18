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


def window_benchmark(unit,destination,repeats=3,frames=12):
    """Same transformed tensors, codecs and chunk candidates; clean every trial."""
    import icechunk as ic
    import zarr
    from zarr.codecs import BloscCodec,BloscShuffle
    unit=Path(unit);saved=json.loads((unit/'progress.json').read_text())
    repo=ic.Repository.open(ic.local_filesystem_storage(str(unit/'data.icechunk')))
    source=zarr.open_group(repo.readonly_session(snapshot_id=saved['snapshot_id']).store,mode='r')
    available=source['values'].shape[0]
    # At most 24 frames; bounded enough for the Linux SSD comparison.
    n=min(24,available);tensor=np.asarray(source['values'][:n])
    if tensor.nbytes>2*1024**3:raise MemoryError('Benchmark sample exceeds 2 GiB; reduce variables or ROI')
    destination=Path(destination);destination.mkdir(parents=True,exist_ok=True)
    rows=[]
    candidates=[(codec,chunk) for codec in ('zstd','none') for chunk in (1,6,12)]
    for repeat in range(repeats):
        for codec,chunk in candidates[repeat%len(candidates):]+candidates[:repeat%len(candidates)]:
            with tempfile.TemporaryDirectory(prefix='ecore-window-benchmark-',dir=destination) as temp:
                started=time.perf_counter();store=zarr.open_group(temp,mode='w')
                array=store.create_array('values',data=tensor,chunks=(chunk,1,min(256,tensor.shape[2]),min(256,tensor.shape[3])),
                    compressors=BloscCodec(cname='zstd',clevel=3,shuffle=BloscShuffle.shuffle) if codec=='zstd' else None)
                write=time.perf_counter()-started
                stored=sum(p.stat().st_size for p in Path(temp).rglob('*') if p.is_file())
                for length in (6,12):
                    if length>n:continue
                    starts=np.random.default_rng(100+repeat).integers(0,n-length+1,size=12)
                    for readers in (1,2,4):
                        def load(first):
                            # A fresh application handle does not flush filesystem caches.
                            before=time.perf_counter();read=zarr.open_group(temp,mode='r')['values'][first:first+length]
                            np.testing.assert_array_equal(read,tensor[first:first+length])
                            return time.perf_counter()-before
                        before=time.perf_counter()
                        with ThreadPoolExecutor(max_workers=readers) as pool:latencies=list(pool.map(load,starts))
                        seconds=time.perf_counter()-before
                        rows.append({'repeat':repeat,'compression':codec,'chunk_time':chunk,'frames':length,
                            'readers':readers,'write_seconds':write,'stored_bytes':stored,'logical_bytes':tensor.nbytes,
                            'windows_per_second':len(starts)/seconds,'median_window_seconds':float(np.median(latencies)),
                            'wall_seconds':seconds,'verified':True})
    return {'path':str(destination),'cases':rows,'cache_scope':'fresh application handles; filesystem caches not flushed',
        'temporary_stores_removed':True,'scope':f'cached regional tensors, first {n} observations; no NOAA reads'}


def benchmark(unit,destination,output,repeats=3,size_mib=1024,linux_comparison=None):
    import psutil
    started=time.perf_counter();cpu=time.process_time()
    process=psutil.Process();before=process.io_counters()._asdict()
    result={'disk':disk_benchmark(destination,repeats,size_mib),
            'windows':window_benchmark(unit,destination,repeats),
            'model_consumption':'not measured until the model check succeeds; storage speed alone does not establish GPU starvation'}
    import icechunk as ic,zarr
    state=json.loads((Path(unit)/'progress.json').read_text())
    repo=ic.Repository.open(ic.local_filesystem_storage(str(Path(unit)/'data.icechunk')))
    metadata=zarr.open_group(repo.readonly_session(snapshot_id=state['snapshot_id']).store,mode='r')
    preparation=metadata.attrs.get('scientific_selection')
    if preparation and Path(preparation).is_dir():
        result['native_windows']=native_window_benchmark(preparation,repeats)
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
    """Measure the actual Icechunk/ProcessedSource path as well as layout trials."""
    import icechunk as ic,xarray as xr
    from ecore_weather.processing.process_store import ProcessedSource
    state=json.loads((Path(unit)/'progress.json').read_text())
    repo=ic.Repository.open(ic.local_filesystem_storage(str(Path(unit)/'data.icechunk')))
    with xr.open_zarr(repo.readonly_session(snapshot_id=state['snapshot_id']).store,
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
    return {'cases':cases,'snapshot_id':state['snapshot_id'],'unit':str(unit),
        'scope':'actual compressed Icechunk snapshot, validity masks and model-facing source; up to 24 observations',
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
