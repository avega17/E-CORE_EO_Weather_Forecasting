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

from .processing import atomic_json, scientific_id


@contextmanager
def _cache_lock(cache):
    cache.mkdir(parents=True,exist_ok=True)
    with (cache/'cache.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        try:yield
        finally:fcntl.flock(lock,fcntl.LOCK_UN)


def area_definition(dataset):
    from . import goes
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
    from . import goes
    base=area_definition(dataset); harea=area_definition(height_dataset)
    name=next((n for n in ('HT','height','cloud_top_height') if n in height_dataset),None)
    if name is None:raise ValueError('No cloud-top height variable')
    height=height_dataset[name].astype('float64')
    valid=np.isfinite(height.values)&(height.values>=0)
    if 'DQF' in height_dataset:valid &= np.asarray(height_dataset.DQF.values)==0
    units=str(height.attrs.get('units','')).lower()
    if units in ('km','kilometers','kilometres'):height=height*1000
    elif units not in ('m','meters','metres'):raise ValueError('Cloud-height units must be metres or kilometres')
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
    return {'indices':indices,'weights':weights,'nearest':nearest,'support':support,
        'shape':np.array(grid.inside_bbox.shape)}


def corrected_mapping(dataset,record,grid,cache,budget_gib=10):
    """Use a matched height receipt; unsupported bands are never silently corrected."""
    if record['variable'] not in ('abi02c','abi13c'):
        raise ValueError('Cloud-height pilot currently supports C02/C13 only; water-vapor correction is unvalidated')
    cache=Path(cache)
    receipt=cache/'heights'/f'{record["asset_id"]}.json'
    if not receipt.is_file():raise ValueError(f'No matched cloud-height receipt for {record["asset_id"]}; run the parallax pilot')
    evidence=json.loads(receipt.read_text())
    if evidence['satellite']!=record['satellite'] or evidence['source_asset_id']!=record['asset_id']:
        raise ValueError('Height/source satellite or identity differs')
    key=scientific_id({'source':record['asset_id'],'height':evidence,'grid':grid.attrs,
        'source_x':dataset.x.values.tolist(),'source_y':dataset.y.values.tolist(),
        'source_projection':dataset.goes_imager_projection.attrs,
        'height_quality':'DQF 0, finite nonnegative height',
        'algorithm':'satpy-0.59-inverse-v1'})
    target=cache/(key+'.npz')
    with _cache_lock(cache):
        if target.exists():
            os.utime(target,None)
            with np.load(target,allow_pickle=False) as file:return {k:file[k] for k in file.files}
    with xr.open_dataset(evidence['path']) as height:
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
    from .common import Asset,iso,list_objects,s3_client,utc
    from .goes import scan_time,native_window
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
        selected=ds.isel(**native_window(ds,bbox)).load()
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
