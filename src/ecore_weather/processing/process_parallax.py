"""Bounded, explicit cloud-height experiments; no changes to native CMI."""
from __future__ import annotations

import json
from pathlib import Path
import os
import fcntl
from contextlib import contextmanager
import uuid

import numpy as np
import xarray as xr

from ecore_weather.processing.transforms import atomic_json, scientific_id


class MissingHeightError(ValueError):
    """A CMI observation has no causal usable height observation."""


@contextmanager
def _cache_lock(cache):
    cache.mkdir(parents=True,exist_ok=True)
    with (cache/'cache.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        try:yield
        finally:fcntl.flock(lock,fcntl.LOCK_UN)


def area_definition(dataset):
    from ecore_weather.sources import goes
    from pyresample.geometry import AreaDefinition
    crs,height=goes.projection(dataset)
    x,y=goes._physical_coordinate(dataset.x)*height,goes._physical_coordinate(dataset.y)*height
    dx,dy=abs(x[1]-x[0]),abs(y[1]-y[0])
    if x[0]>x[-1] or y[0]<y[-1]:
        raise ValueError('Parallax pilot expects west-east x and north-south y ordering')
    return AreaDefinition('native','Native GOES ABI subset','geos',crs,len(x),len(y),
        (x[0]-dx/2,y[-1]-dy/2,x[-1]+dx/2,y[0]+dy/2))


def mapping_from_height(dataset,height_dataset,grid):
    """Satpy inverse parallax geometry and pyresample bilinear interpolation LUT."""
    from satpy.modifiers.parallax import ParallaxCorrection
    from pyresample.geometry import AreaDefinition
    from pyresample.bilinear import NumpyBilinearResampler
    from pyresample.kd_tree import resample_nearest
    from ecore_weather.sources import goes
    base=area_definition(dataset); harea=area_definition(height_dataset)
    name=next((n for n in ('HT','height','cloud_top_height') if n in height_dataset),None)
    if name is None:raise ValueError('No cloud-top height variable')
    height=height_dataset[name].astype('float64')
    valid=np.isfinite(height.values)&(height.values>=0)
    if 'DQF' in height_dataset:valid &= np.asarray(height_dataset.DQF.values)==0
    units=str(height.attrs.get('units','')).lower()
    if units in ('km','kilometers','kilometres'):height=height*1000
    elif units not in ('m','meters','metres'):raise ValueError('Cloud-height units must be metres or kilometres')
    if not valid.any():
        size=grid.inside_bbox.size
        return {'indices':np.zeros((4,size),'int64'),'weights':np.zeros((4,size),'float32'),
            'nearest':np.zeros(size,'int64'),'support':np.zeros(size,bool),
            'shape':np.array(grid.inside_bbox.shape),'displacement_m':np.full(size,np.nan)}
    crs,satellite_height=goes.projection(dataset)
    import dask.array as da
    height=xr.DataArray(da.from_array(np.where(valid,height.values,np.nan),chunks=512),dims=('y','x'),attrs={
        'area':harea,'orbital_parameters':{'satellite_actual_longitude':crs.to_cf()['longitude_of_projection_origin'],
        'satellite_actual_latitude':0.,'satellite_actual_altitude':satellite_height}})
    corrected=ParallaxCorrection(base).corrected_area(height,cth_radius_of_influence=15000)
    # Satpy can use zero for undefined heights. Independently retain that uncertainty.
    known=resample_nearest(harea,valid.astype('uint8'),base,radius_of_influence=15000,fill_value=0).astype(bool)
    lon,lat=corrected.get_lonlats()
    lon,lat=np.asarray(lon),np.asarray(lat)
    lon=np.where(known,lon,np.nan);lat=np.where(known,lat,np.nan)
    from pyresample.geometry import SwathDefinition
    corrected=SwathDefinition(lons=lon,lats=lat)
    target=AreaDefinition('target','Regional training grid','lcc',grid.attrs['crs_wkt'],
        len(grid.x),len(grid.y),tuple(grid.attrs['bounds_m']))
    sampler=NumpyBilinearResampler(corrected,target,15000,neighbours=32)
    sampler.get_bil_info(nprocs=1)
    s,t=np.asarray(sampler.bilinear_s),np.asarray(sampler.bilinear_t)
    weights=np.stack([(1-s)*(1-t),s*(1-t),(1-s)*t,s*t]).astype('float32')
    indices=(np.asarray(sampler.slices_y)*len(dataset.x)+np.asarray(sampler.slices_x)).T.astype('int64')
    support=(np.isfinite(weights).all(axis=0)&~np.asarray(sampler.mask_slices).any(axis=1)&grid.inside_bbox.values.ravel())
    weights=np.nan_to_num(weights)
    nearest=np.take_along_axis(indices,np.argmax(weights,axis=0)[None],axis=0)[0]
    # Compare original apparent coordinates with the corrected target location.
    from pyproj import Geod
    original_lon,original_lat=base.get_lonlats()
    apparent_lon=(np.asarray(original_lon).ravel()[indices]*weights).sum(axis=0)
    apparent_lat=(np.asarray(original_lat).ravel()[indices]*weights).sum(axis=0)
    _,_,distance=Geod(ellps='WGS84').inv(apparent_lon,apparent_lat,
        grid.longitude.values.ravel(),grid.latitude.values.ravel())
    return {'indices':indices,'weights':weights,'nearest':nearest,'support':support,
        'shape':np.array(grid.inside_bbox.shape),'displacement_m':np.where(support,distance,np.nan)}


def corrected_mapping(dataset,record,grid,cache,budget_gib=10,max_age_minutes=20):
    """Use a matched height receipt; unsupported bands are never silently corrected."""
    if record['variable'] not in ('abi01c','abi02c','abi03c','abi07c','abi13c'):
        raise ValueError('Water-vapor correction is unvalidated; C08/C09/C10 stay unshifted')
    cache=Path(cache)
    receipt=cache/'heights'/f'{record["asset_id"]}.json'
    if not receipt.is_file():
        archived=archived_height(record,max_age_minutes)
        if archived is None:raise MissingHeightError(f'No causal archived cloud height for {record["asset_id"]}; fetch ACHAF or run the parallax pilot')
        atomic_json(receipt,archived)
    evidence=json.loads(receipt.read_text())
    if evidence['satellite']!=record['satellite'] or evidence['source_asset_id']!=record['asset_id']:
        raise ValueError('Height/source satellite or identity differs')
    if not 0<=evidence.get('height_age_seconds',0)<=max_age_minutes*60:
        raise ValueError('Height observation is outside the causal age limit')
    height_identity={name:evidence.get(name) for name in ('height_asset_id','height_etag','path','row_index','satellite')}
    key=scientific_id({'height':height_identity,'grid':grid.attrs,
        'source_x':dataset.x.values.tolist(),'source_y':dataset.y.values.tolist(),
        'source_projection':dataset.goes_imager_projection.attrs,
        'height_quality':'DQF 0, finite nonnegative height',
        'algorithm':'satpy-0.59-inverse-v2-with-displacement'})
    target=cache/(key+'.npz')
    with _cache_lock(cache):
        if target.exists():
            os.utime(target,None)
            with np.load(target,allow_pickle=False) as file:return {k:file[k] for k in file.files}
    with open_height(evidence) as height:
        result=mapping_from_height(dataset,height,grid)
    size=sum(a.nbytes for a in result.values())
    budget=int(budget_gib*1024**3)
    # Only disposable LUT files are evicted; height observations and receipts remain.
    with _cache_lock(cache):
        files=sorted(cache.glob('*.npz'),key=lambda p:p.stat().st_mtime)
        used=sum(p.stat().st_size for p in files)
        while files and used+size>budget:
            old=files.pop(0);used-=old.stat().st_size;old.unlink()
        if size<=budget:
            temporary=target.with_suffix(f'.{uuid.uuid4().hex}.tmp.npz')
            np.savez_compressed(temporary,**result);os.replace(temporary,target)
    return result


def fetch_height(record,bbox,cache,transport,remaining_bytes):
    """Discover a causal same-platform height file and save only its native ROI."""
    from datetime import timedelta
    from ecore_weather.core.common import Asset, iso, list_objects, s3_client, utc
    from ecore_weather.sources.goes import scan_time, native_window
    from io import BytesIO
    when=utc(record['end_time']);bucket=f'noaa-goes{int(record["satellite"])}'
    objects=[]
    for hour in {when.replace(minute=0,second=0,microsecond=0),
                 (when-timedelta(minutes=20)).replace(minute=0,second=0,microsecond=0)}:
        objects.extend(list_objects(bucket,f'ABI-L2-ACHAF/{hour:%Y/%j/%H}/',s3_client()))
    import re
    candidates=[]
    for obj in objects:
        match=re.search(r'_s(\d+)_e(\d+)_c',obj['Key'])
        if match:
            start,end=map(scan_time,match.groups())
            if 0 <= (when-end).total_seconds()<=1200:candidates.append((end,start,obj))
    if not candidates:raise ValueError('No causal same-satellite cloud-height observation within 20 minutes')
    end,start,obj=max(candidates,key=lambda r:r[0])
    if obj['Size']>remaining_bytes:raise OSError('Parallax imagery-transfer budget exhausted')
    asset=Asset(bucket,obj['Key'],obj['Size'],obj['ETag'].strip('"'),iso(start),iso(end))
    data=transport.read(asset)
    if len(data)!=asset.size:raise IOError('Height object length differs from listing')
    with xr.open_dataset(BytesIO(data),engine='h5netcdf') as ds:
        selected=ds.isel(**native_window(ds,height_bbox(bbox))).load()
    # These are decoded, derived height samples. Avoid quantizing decoded
    # coordinates back through their original integer-container encoding.
    for name in selected.variables:
        selected[name].encoding={}
    root=Path(cache)/'heights';root.mkdir(parents=True,exist_ok=True)
    path=root/(asset.id+'.nc')
    if not path.exists():selected.to_netcdf(path,engine='h5netcdf')
    receipt={'path':str(path.resolve()),'source_asset_id':record['asset_id'],
        'satellite':record['satellite'],'height_asset_id':asset.id,'height_source_url':asset.url,
        'height_etag':asset.etag,'height_start':asset.time,'height_end':asset.end_time,
        'height_age_seconds':(when-end).total_seconds(),'returned_bytes':len(data)}
    atomic_json(root/(record['asset_id']+'.json'),receipt)
    return receipt


def height_bbox(bbox,margin_km=50):
    """A conservative geographic margin; native CMI support is never enlarged."""
    west,south,east,north=bbox
    from math import cos,radians
    dy=margin_km/110.;dx=margin_km/(110.*cos(radians(max(abs(south),abs(north)))))
    return (west-dx,south-dy,east+dx,north+dy)


def fetch_height_study(args):
    """Existing monthly writer and scan checkpoints, one auxiliary store at a time."""
    import signal,time
    from ecore_weather.core.common import PR_BBOX,utc,iso
    from ecore_weather.jobs.jobs_goes import _months
    from ecore_weather.sources import goes
    from ecore_weather.ingest.monthly import _relative_path
    from ecore_weather.ingest.monthly_stream import _write_one
    from ecore_weather.storage.catalog import save_selection,load_selection
    from ecore_weather.core.paths import ReceiptPath,store_receipt
    if not args.start or not args.end:raise ValueError('Cloud-height acquisition requires explicit --start and --end')
    start,end=utc(args.start),utc(args.end)
    if start>=end:raise ValueError('End must follow start')
    if args.max_hours<0:raise ValueError('max-hours cannot be negative')
    if not args.max_hours and os.getenv('PBS_JOBID'):raise ValueError('PBS jobs require a positive deadline')
    root=Path(args.output);root.mkdir(parents=True,exist_ok=True)
    deadline=time.monotonic()+args.max_hours*3600 if args.max_hours else float('inf')
    stopped=[False];previous={}
    import threading
    if threading.current_thread() is threading.main_thread():
        for signum in (signal.SIGTERM,signal.SIGINT):
            previous[signum]=signal.getsignal(signum);signal.signal(signum,lambda *unused:stopped.__setitem__(0,True))
    def checkpoint(ids):
        if stopped[0] or time.monotonic()>=deadline:raise InterruptedError('Cloud-height checkpointed pause')
    completed=0;transition=utc('2025-04-07T15:00:00Z')
    try:
        from datetime import timedelta
        from ecore_weather.processing.storm_windows import STORMS
        windows=[]
        if args.storm_heights_first:
            for _,event_start,event_end in STORMS.values():
                begin=max(start,utc(event_start)-timedelta(minutes=100))
                finish=min(end,utc(event_end))
                if begin<finish:windows.append((begin,finish))
        windows.append((start,end))
        segments=[]
        for begin,finish in windows:
            for first,next_month in _months(begin.date(),finish.date()+timedelta(days=1)):
                a=max(begin,utc(first.isoformat()))
                b=min(finish,utc(next_month.isoformat()))
                if a<b:segments.append((a,b))
        for begin,finish in segments:
            edges=sorted({begin,finish,*([transition] if begin<transition<finish else [])})
            for a,b in zip(edges,edges[1:]):
                if stopped[0] or time.monotonic()>=deadline:return 75
                satellite=goes.east_satellite(a,b)
                saved=root/'months'/f'{a:%Y-%m}'/f'goes{satellite}-{a:%dT%H%M}-{b:%dT%H%M}.json'
                if args.dry_run:
                    print(f'ACHAF {iso(a)} to {iso(b)} GOES-{satellite}',flush=True);continue
                catalog=root/'pending'/f'{a:%Y%m%dT%H%M}-{b:%Y%m%dT%H%M}-goes{satellite}'/'collection.json'
                selection=load_selection(catalog) if catalog.is_file() else goes.discover(a,b,bbox=height_bbox(PR_BBOX),bands=(),satellite=satellite,product='ABI-L2-ACHAF')
                if (utc(selection.start),utc(selection.end))!=(a,b):raise ValueError('Saved height interval differs')
                if not catalog.is_file():save_selection(selection,catalog.parent)
                if not selection.assets:
                    store_receipt(saved,{'status':'unavailable','start':iso(a),'end_excluded':iso(b),'satellite':satellite});continue
                target=Path(args.destination)/_relative_path(selection,selection.assets[0])
                result=_write_one(selection,selection.assets,target,None,workers=min(args.workers,8),scratch=args.scratch,
                    checkpoint_callback=checkpoint)
                from ecore_weather.jobs.index import record_fetch
                record_fetch({'source':'goes','selection_id':selection.id,
                    'selection_summary':selection.summary(),'root':str(args.destination),
                    'monthly_archives':[result],'read_bytes':result.get('read_bytes',0),
                    'stored_bytes':result.get('stored_bytes',0)})
                store_receipt(saved,{'status':'complete','start':iso(a),'end_excluded':iso(b),'satellite':satellite,
                    'archives':[result['path']],'observations':result['observations']})
                completed+=1
                import shutil
                shutil.rmtree(catalog.parent)
                atomic_json(root/'height-summary.json',{'status':'running','completed_month_segments':completed,'latest':str(target)})
                print(f'Verified ACHAF {a:%Y-%m} GOES-{satellite}: {result["observations"]} scans',flush=True)
        atomic_json(root/'height-summary.json',{'status':'complete','completed_month_segments':completed})
        return 0
    except InterruptedError as exc:
        atomic_json(root/'height-summary.json',{'status':'paused','reason':str(exc),'completed_month_segments':completed});return 75
    finally:
        for signum,handler in previous.items():signal.signal(signum,handler)


@contextmanager
def open_height(evidence):
    if 'row_index' not in evidence:
        with xr.open_dataset(evidence['path']) as height:yield height
    else:
        from ecore_weather.storage.archives import open_raw
        from ecore_weather.processing.transforms import scan_dataset
        with open_raw(evidence['path']) as raw:
            selected=scan_dataset(raw,evidence['row_index']).load()
            height=xr.decode_cf(selected,mask_and_scale=True)
            yield height


def archived_height(record,max_age_minutes=20):
    """Bounded month lookup; never selects another platform or future heights."""
    from datetime import timedelta
    from ecore_weather.core.common import utc
    path=Path(record['path']);parts=path.parts
    if 'goes' not in parts:return None
    root=Path(*parts[:parts.index('goes')])/'goes/ABI-L2-ACHAF'/f'goes{int(record["satellite"])}'
    when=utc(record['end_time']);candidates=[]
    periods={when.strftime('%Y/%m'),(when-timedelta(minutes=max_age_minutes)).strftime('%Y/%m')}
    for period in periods:
        for marker in root.glob(f'roi-*/{period}/complete.json'):
            data=json.loads(marker.read_text())
            if data.get('satellite')!=int(record['satellite']):continue
            archive=marker.parent/data.get('raw_path','raw.zarr.zip')
            if not archive.is_file():continue
            for index,asset in enumerate(data.get('assets',[])):
                end=asset.get('end_time')
                if not end:
                    import re
                    from ecore_weather.sources.goes import scan_time
                    match=re.search(r'_e(\d+)_c',asset.get('source_url',''))
                    if match:end=scan_time(match[1]).isoformat()
                if not end:continue
                age=(when-utc(end)).total_seconds()
                if 0<=age<=max_age_minutes*60:
                    candidates.append((utc(end),{'path':str(archive),'row_index':index,
                        'source_asset_id':record['asset_id'],'satellite':int(record['satellite']),
                        'height_asset_id':asset['asset_id'],'height_etag':asset.get('etag'),
                        'height_end':end,'height_age_seconds':age,'returned_bytes':0}))
    return max(candidates,key=lambda x:x[0])[1] if candidates else None
