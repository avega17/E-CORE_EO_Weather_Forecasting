"""Bounded, reproducible GOES comparisons; distinct stock and archival workloads."""
from __future__ import annotations
import argparse
from dataclasses import replace
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
from statistics import median
from . import goes,monthly
from .common import PeakMemory,PR_BBOX
from .runlog import save
from .transfer_budget import Budget
from .storage import open_raw


def content_fingerprint(archives):
    sha=hashlib.sha256()
    for archive in sorted(archives,key=lambda r:r['band']):
        with open_raw(archive['path']) as ds:
            for name in sorted(ds.variables):
                values=ds[name].values
                sha.update(name.encode());sha.update(str(values.shape).encode());sha.update(str(values.dtype).encode())
                sha.update(values.astype(str).tobytes() if values.dtype.kind=='O' else values.tobytes())
                sha.update(json.dumps(ds[name].attrs,sort_keys=True,default=str).encode())
    return sha.hexdigest()


def _profile(mode='async_pipeline',downloads=16,readers=2,queue=512):
    return {'read_mode':mode,'download_concurrency':downloads,'read_processes':readers,
            'prefetch_mib':queue,'staging_mib':8192,'workers':8,'monthly_writers':2}


def _case(selection,profile,output,stage,repeat,reference):
    from .dataset_report import provenance
    budget=Budget(Path(output)/'budget.json');before=budget.summary()
    with tempfile.TemporaryDirectory(prefix='ecore-final-goes-') as temp:
        with PeakMemory() as memory:
            report=monthly.fetch(selection,temp,backend='obstore',scratch=temp,report_dir=None,index_results=False,**profile)
        if report.get('interrupted'):raise IOError(report['monthly_archives'])
        digest=content_fingerprint(report['monthly_archives'])
        if reference is not None and digest!=reference:raise AssertionError('Scientific array, coordinate or metadata mismatch')
        after=budget.summary()
        row={'stage':stage,'repeat':repeat,'satellite':selection.satellite,'selection_id':selection.id,
             'profile':profile,'wall_seconds':report['wall_s'],'returned_bytes':after['observed']-before['observed'],
             'charged_bytes':after['charged']-before['charged'],'peak_rss_bytes':memory.peak,
             'stored_bytes':report['stored_bytes'],'source_objects':len(selection.assets),
             'listed_source_bytes':sum(a.size for a in selection.assets),'content_sha256':digest,
             'values_equal':True,'monthly_metrics':report['monthly_archives'],
             'application_cache':'fresh isolated directory','filesystem_cache':'not flushed',
             'provenance':provenance()}
        return row,digest


def stock_reference(output):
    """Unmodified installed source with download hook observing calls, not changing behavior."""
    import numpy as np
    import xarray as xr
    from earth2studio.data import GOES
    import earth2studio.data.utils as utils
    from obstore import head_async
    from .transfer_budget import active
    previous=utils.obstore_read_range
    async def counted(store,key,*args,**kwargs):
        metadata=await store.head_async(key)
        length=metadata['size']
        budget=active();reservation=budget.reserve(length)
        try:data=await previous(store,key,*args,**kwargs)
        except BaseException:budget.settle(reservation);raise
        budget.settle(reservation,len(data));return data
    utils.obstore_read_range=counted
    rows=json.loads((Path(output)/'stock.json').read_text()).get('rows',[]) if (Path(output)/'stock.json').exists() else []
    try:
        for satellite,day in ((16,'2022-09-18'),(19,'2025-09-18')):
            if any(r['satellite']==satellite for r in rows):continue
            with tempfile.TemporaryDirectory(prefix='ecore-stock-reference-') as cache:
                old=os.environ.get('EARTH2STUDIO_DATA_CACHE');os.environ['EARTH2STUDIO_DATA_CACHE']=cache
                try:
                    source=GOES(satellite=f'goes{satellite}',verbose=False)
                    captured=[];original=source._fetch_remote_file
                    async def capture(path):
                        local=await original(path);captured.append((path,local));return local
                    source._fetch_remote_file=capture
                    times=[datetime.fromisoformat(day+f'T12:{m:02}:00') for m in (0,10,20,30,40,50)]
                    before=Budget(Path(output)/'budget.json').summary()
                    with PeakMemory() as memory:
                        started=time.perf_counter();values=source(times,[f'abi{b:02}c' for b in goes.STORMSCOPE_BANDS]);fetch=time.perf_counter()-started
                        if memory.peak>24*1024**3:raise MemoryError(f'Stock reference exceeds 24 GiB ceiling: {memory.peak} bytes')
                        crop_start=time.perf_counter()
                        for index,(url,path) in enumerate(captured):
                            with xr.open_dataset(path,engine='h5netcdf',decode_cf=False) as raw:
                                window=goes.native_window(raw,PR_BBOX)
                                roi=goes.subset_dataset(raw,PR_BBOX,goes.STORMSCOPE_BANDS)
                            decoded=goes.decode(roi)
                            # Captured tasks may finish out of order; associate by requested scan identity.
                            scan=Path(url).name.split('_s')[1].split('_')[0]
                            # Equality against stock slices is established by matching file-derived values to a returned time.
                            matched=False
                            for t in range(len(times)):
                                if all(np.array_equal(values.sel(variable=f'abi{b:02}c').isel(time=t,**window).values,
                                        decoded[f'CMI_C{b:02}'].values,equal_nan=True) for b in goes.STORMSCOPE_BANDS):matched=True;break
                            if not matched:raise AssertionError('Stock source crop differs from chosen source')
                            decoded.close();roi.close()
                        crop=time.perf_counter()-crop_start
                    values.close();after=Budget(Path(output)/'budget.json').summary()
                    rows.append({'stage':'stock','satellite':satellite,'source_objects':[p[0] for p in captured],
                        'configured_timestamp_concurrency':24,'exercised_timestamp_tasks':6,
                        'download_decode_seconds':fetch,'crop_verify_seconds':crop,'wall_seconds':fetch+crop,
                        'peak_rss_bytes':memory.peak,'returned_bytes':after['observed']-before['observed'],
                        'product':'ABI-L2-MCMIPF','scope':'full disk decoded float64 -> explicit source-grid ROI crop; no native CMIPF archive',
                        'values_equal':True})
                    save(Path(output)/'stock.json',{'status':'running','rows':rows})
                    del values,source,decoded,roi
                    __import__('gc').collect()
                finally:
                    if old is None:os.environ.pop('EARTH2STUDIO_DATA_CACHE',None)
                    else:os.environ['EARTH2STUDIO_DATA_CACHE']=old
    finally:utils.obstore_read_range=previous
    save(Path(output)/'stock.json',{'status':'complete','rows':rows})
    return rows


def main(argv=None):
    import sys
    inputs=list(sys.argv[1:] if argv is None else argv)
    if '--scheduler' in inputs:
        from .goes_scheduler_benchmark import main as scheduler
        inputs.remove('--scheduler');return scheduler(inputs)
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output',default='results/benchmarks/goes-final')
    p.add_argument('--budget-gib',type=float,default=100)
    p.add_argument('--visible-final',action='store_true',help='Exercise four independent readers on native C02 plus C13')
    p.add_argument('--extend-confirmation',action='store_true',help='Rotate native-band range and staging finalists after screening')
    p.add_argument('--matched-only',action='store_true',help='Compare NVIDIA download utilities on identical MCMIPF files')
    p.add_argument('--skip-stock',action='store_true',help='Resume after a separately completed stock reference')
    args=p.parse_args(argv)
    if not 0<args.budget_gib<=100:p.error('Network budget must be within 100 GiB')
    output=Path(args.output).resolve();output.mkdir(parents=True,exist_ok=True)
    budget=Budget(output/'budget.json',args.budget_gib*1024**3)
    os.environ['ECORE_TRANSFER_BUDGET']=str(budget.path)
    from .jobs_policy import ensure_paused
    ensure_paused()
    if args.visible_final:return visible_final(output)
    if args.extend_confirmation:return extend_confirmation(output)
    if args.matched_only:return matched_transport(output)
    from .dataset_report import provenance
    state={'status':'running','pid':os.getpid(),'rows':[],'provenance':provenance()}
    path=output/'suite.json'
    if path.exists():
        saved=json.loads(path.read_text())
        if saved.get('provenance')!=state['provenance']:raise ValueError('Benchmark code/environment changed; use a new suite directory')
        state['rows']=saved['rows']
    def record(row):
        state['rows'].append(row);state['budget']=budget.summary();save(path,state)
        print(f"{row['stage']} GOES-{row['satellite']}: {row.get('profile','stock')} {row['wall_seconds']:.2f}s {row['returned_bytes']/1024**3:.3f} GiB",flush=True)
    try:
        if not args.skip_stock and (not (output/'stock.json').exists() or json.loads((output/'stock.json').read_text()).get('status')!='complete'):
            for row in stock_reference(output):record(row)
        # Long infrared screening exercises sustained admissions and two independent writers.
        selections={sat:goes.discover(day+'T12:00:00',day+'T18:00:00',bands=(7,13),satellite=sat)
                    for sat,day in ((16,'2022-09-18'),(19,'2025-09-18'))}
        profiles=[_profile('range',8,2),_profile('async_full',8,4)]
        profiles += [_profile(downloads=d) for d in (8,16,24,32)]
        profiles += [_profile(readers=r) for r in (1,4,8)]
        profiles += [_profile(queue=q) for q in (256,1024)]
        references={}
        for profile in profiles:
            existing=next((r for r in state['rows'] if r['stage']=='screen' and r.get('profile')==profile),None)
            if existing:references[16]=existing['content_sha256'];continue
            row,digest=_case(selections[16],profile,output,'screen',1,references.get(16));references[16]=digest;record(row)
            if budget.summary()['charged']>40*1024**3:break # Stock unused budget may move forward.
        candidates=[r for r in state['rows'] if r['stage']=='screen' and r['peak_rss_bytes']<=8*1024**3 and r['profile']['read_mode']=='async_pipeline']
        if not candidates:raise RuntimeError('No improved profile met the production memory limit')
        candidate=min(candidates,key=lambda r:r['wall_seconds'])['profile']
        finalists=[_profile('range',8,2),_profile('async_full',8,4),candidate]
        for sat in (16,19):
            for repeat in range(1,4):
                order=finalists[repeat%3:]+finalists[:repeat%3]
                for profile in order:
                    if any(r['stage']=='final' and r['satellite']==sat and r['repeat']==repeat and r['profile']==profile for r in state['rows']):continue
                    row,digest=_case(selections[sat],profile,output,'final',repeat,references.get(sat));references[sat]=digest;record(row)
        # All native grids, day and night, with direct range baseline as scientific reference.
        confirmations={}
        for sat,day in ((16,'2022-09-18'),(19,'2025-09-18')):
            day_selection=goes.discover(day+'T12:00:00',day+'T12:10:00',satellite=sat,bands=goes.STORMSCOPE_BANDS)
            night=goes.discover(day+'T00:00:00',day+'T00:10:00',satellite=sat,bands=goes.STORMSCOPE_BANDS)
            confirmations[sat]=replace(day_selection,start=night.start,assets=sorted(night.assets+day_selection.assets,key=lambda a:(a.time,a.key)))
        for sat in (16,19):
            ref=None
            for repeat in range(1,4):
                for profile in ([finalists[1],candidate] if repeat%2 else [candidate,finalists[1]]):
                    row,digest=_case(confirmations[sat],profile,output,'confirm',repeat,ref);ref=digest;record(row)
        winners={}
        for resolution,bands in [('2km',(7,8,9,10,13)),('1km',(1,3)),('0.5km',(2,))]:
            # Global candidate is eligible only if complete confirmation meets both-satellite gain.
            gains={}
            for sat in (16,19):
                base=[r for r in state['rows'] if r['stage']=='confirm' and r['satellite']==sat and r['profile']==finalists[1]]
                test=[r for r in state['rows'] if r['stage']=='confirm' and r['satellite']==sat and r['profile']==candidate]
                gains[str(sat)]=1-median(r['wall_seconds'] for r in test)/median(r['wall_seconds'] for r in base)
            valid=min(gains.values())>=.10 and all(r['peak_rss_bytes']<=8*1024**3 for r in state['rows'] if r['stage']=='confirm' and r['profile']==candidate)
            winners[resolution]={'bands':bands,'profile':candidate if valid else finalists[1],'gain':gains,
                'reason':'verified overall native-grid confirmation gain' if valid else 'no qualifying confirmation gain; retain existing profile'}
        state.update(status='complete',profiles=winners,budget=budget.summary());save(path,state)
        return 0
    except BaseException as exc:
        state.update(status='failed',error=f'{type(exc).__name__}: {exc}',budget=budget.summary());save(path,state)
        save(output/f'failed-{int(time.time())}.json',state);raise


def extend_confirmation(output):
    """All native grids with rotated range/synchronous-stage/overlapped methods."""
    state=json.loads((output/'suite.json').read_text())
    old=[r for r in state['rows'] if r['stage']=='confirm']
    candidate=next(r['profile'] for r in old if r['profile']['read_mode']=='async_pipeline')
    baseline=next(r['profile'] for r in old if r['profile']['read_mode']=='async_full')
    range_one=_profile('range',8,4);range_one['workers']=1
    methods=[baseline,candidate,_profile('range',8,2),range_one]
    for row in old:row['stage']='confirm_initial'
    state['status']='running';state['rows']=[r for r in state['rows'] if r['stage']!='confirm']
    references={r['satellite']:r['content_sha256'] for r in old}
    from .jobs_policy import select_profiles
    try:
        for sat,day in ((16,'2022-09-18'),(19,'2025-09-18')):
            day_s=goes.discover(day+'T12:00:00',day+'T12:10:00',satellite=sat,bands=goes.STORMSCOPE_BANDS)
            night=goes.discover(day+'T00:00:00',day+'T00:10:00',satellite=sat,bands=goes.STORMSCOPE_BANDS)
            selection=replace(day_s,start=night.start,assets=sorted(night.assets+day_s.assets,key=lambda a:(a.time,a.key)))
            for repeat in range(1,4):
                order=methods[repeat%4:]+methods[:repeat%4]
                for profile in order:
                    row,digest=_case(selection,profile,output,'confirm',repeat,references[sat]);state['rows'].append(row)
                    state['budget']=Budget(output/'budget.json').summary();save(output/'suite.json',state)
                    print(f"native confirmation {sat} {repeat} {profile['read_mode']} {profile['read_processes']} readers: {row['wall_seconds']:.2f}s",flush=True)
        state.update(status='complete',band_profiles=select_profiles(state['rows'],baseline))
        save(output/'profiles.json',state['band_profiles']);save(output/'suite.json',state)
        from .index import import_operational_evidence
        import_operational_evidence(output)
        return 0
    except BaseException as exc:
        state.update(status='failed',error=f'{type(exc).__name__}: {exc}',budget=Budget(output/'budget.json').summary());save(output/'suite.json',state);raise


def matched_transport(output):
    """Identical MCMIPF objects; separate transfer, local crop and readiness."""
    import asyncio
    import numpy as np
    import xarray as xr
    import threading
    from earth2studio.data import utils
    from obstore.store import S3Store
    from .goes_staging import _stream_one
    from .transfer_budget import active
    from .dataset_report import file_hash
    rows=[];reference={};original=utils.obstore_read_range
    async def counted(store,key,*a,**kwargs):
        metadata=await store.head_async(key);budget=active();reservation=budget.reserve(metadata['size'])
        try:data=await original(store,key,*a,**kwargs)
        except BaseException:budget.settle(reservation);raise
        budget.settle(reservation,len(data));return data
    utils.obstore_read_range=counted
    try:
        for sat,day in ((16,'2022-09-18'),(19,'2025-09-18')):
            selection=goes.discover(day+'T12:00:00',day+'T12:20:00',satellite=sat,product='ABI-L2-MCMIPF',bands=goes.STORMSCOPE_BANDS)
            for repeat in range(1,4):
                for method in (['nvidia','project'] if repeat%2 else ['project','nvidia']):
                    with tempfile.TemporaryDirectory(prefix='ecore-matched-mcmipf-') as temp:
                        folder=Path(temp);before=Budget(output/'budget.json').summary();started=time.perf_counter()
                        async def download():
                            if method=='nvidia':
                                store=utils.obstore_store_from_url(f's3://{selection.assets[0].bucket}')
                                return await utils.gather_with_concurrency([utils.obstore_fetch_to_cache(store,a.key,temp) for a in selection.assets],max_workers=24,verbose=False)
                            store=S3Store(selection.assets[0].bucket,region='us-east-1',skip_signature=True)
                            metrics={k:0 for k in ('cached_full_source_files','active_downloads','peak_active_downloads','range_requests','read_bytes','failed_requests','source_retries','transfer_task_seconds')}
                            semaphore=asyncio.Semaphore(16)
                            return await asyncio.gather(*[_stream_one(a,folder,store,metrics,semaphore,threading.Event()) for a in selection.assets])
                        paths=asyncio.run(download());transfer=time.perf_counter()-started
                        hashes={a.id:file_hash(path) for a,path in zip(selection.assets,paths)}
                        if sat in reference and hashes!=reference[sat]:raise AssertionError('Matched full source bytes differ')
                        reference[sat]=hashes;decode_start=time.perf_counter();roi_bytes=0
                        for path in paths:
                            with xr.open_dataset(path,engine='h5netcdf',decode_cf=False,mask_and_scale=False) as source:
                                roi=goes.subset_dataset(source,PR_BBOX,goes.STORMSCOPE_BANDS);roi_bytes+=roi.nbytes;roi.close()
                        decode=time.perf_counter()-decode_start;after=Budget(output/'budget.json').summary()
                        row={'method':method,'satellite':sat,'repeat':repeat,'source_ids':[a.id for a in selection.assets],
                            'product':'ABI-L2-MCMIPF','download_seconds':transfer,'local_decode_crop_seconds':decode,
                            'readiness_seconds':transfer+decode,'source_hashes_equal':True,
                            'returned_bytes':after['observed']-before['observed'],'logical_roi_bytes':roi_bytes,
                            'effective_timestamp_tasks':len(selection.assets),'configured_concurrency':24 if method=='nvidia' else 16,
                            'scope':'same full source objects, no archive writing; hashes timed separately from readiness'}
                        rows.append(row);save(output/'matched.json',{'status':'running','rows':rows})
                        print(f"matched transport {sat} {repeat} {method}: {transfer:.2f}s transfer + {decode:.2f}s local crop",flush=True)
    finally:utils.obstore_read_range=original
    save(output/'matched.json',{'status':'complete','rows':rows,'budget':Budget(output/'budget.json').summary()})
    return 0

def visible_final(output):
    state=json.loads((output/'suite.json').read_text())
    baseline=next(r['profile'] for r in state['rows'] if r['stage']=='confirm' and r['profile']['read_mode']=='async_full')
    candidate=_profile('range',8,4);candidate['workers']=1
    from .jobs_policy import select_profiles
    references={}
    for sat,day in ((16,'2022-09-18'),(19,'2025-09-18')):
        selection=goes.discover(day+'T12:00:00',day+'T12:50:00',satellite=sat,bands=(2,13))
        for repeat in range(1,4):
            for profile in ([baseline,candidate] if repeat%2 else [candidate,baseline]):
                row,digest=_case(selection,profile,output,'visible_final',repeat,references.get(sat))
                references[sat]=digest;state['rows'].append(row)
                state['budget']=Budget(output/'budget.json').summary();save(output/'suite.json',state)
                print(f"visible final {sat} {repeat} {profile['read_mode']}: {row['wall_seconds']:.2f}s",flush=True)
    state['band_profiles'].update(select_profiles(state['rows'],baseline,stage='visible_final',bands=(2,)))
    save(output/'profiles.json',state['band_profiles']);save(output/'suite.json',state)
    from .index import import_operational_evidence
    import_operational_evidence(output)
    return 0

if __name__=="__main__":raise SystemExit(main())
