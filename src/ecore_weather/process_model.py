"""Explicitly experimental Caribbean StormScope engineering adapter.

Physical observations and true regional geometry are used. Checkpoint skill in
this geography is not established. Model packages are never rewritten.
"""
from __future__ import annotations
from copy import copy
import importlib.metadata
import json
from pathlib import Path
import time

import numpy as np

from .processing import atomic_json


def caribbean_adapter(base,latitudes,longitudes,y,x,topography,input_support=None):
    import torch
    if not getattr(base,'_is_flash',False):
        raise ValueError('This engineering adapter requires a Flash model with resizable rotary positions')
    height,width=np.shape(latitudes)
    if min(height,width)<200 or height%4 or width%4:
        raise ValueError('Flash input dimensions must be >=200 and divisible by four')
    if np.shape(longitudes)!=(height,width) or np.shape(topography)!=(height,width):
        raise ValueError('Static fields and geographic coordinates must match the model input grid')
    model=copy(base)
    model._buffers=base._buffers.copy();model._modules=base._modules.copy();model._parameters=base._parameters.copy()
    model._non_persistent_buffers_set=base._non_persistent_buffers_set.copy()
    device=base.latitudes.device
    for name,value in [('latitudes',latitudes),('longitudes',longitudes),('topo',topography)]:
        model.register_buffer(name,torch.as_tensor(value,dtype=torch.float32,device=device))
    support=np.ones((height,width),bool) if input_support is None else np.asarray(input_support,dtype=bool)
    if support.shape!=(height,width) or not support.any():
        raise ValueError('The model crop needs a nonempty, explicitly supported input region')
    for name in ('valid_mask','conditioning_valid_mask'):
        model.register_buffer(name,torch.as_tensor(support,dtype=torch.bool,device=device))
    if base._buffers.get('nexrad_proximity') is not None or base._buffers.get('mrms_coverage_mask') is not None:
        raise ValueError('Radar model needs regional radar invariants and GLM; use the GOES-only engineering test')
    model.y=np.asarray(y).copy();model.x=np.asarray(x).copy()
    model._lat_cpu_copy=np.asarray(latitudes).copy();model._lon_cpu_copy=np.asarray(longitudes).copy()
    model.input_interp=None;model.conditioning_interp=None
    if hasattr(model,'glm_interp'):model.glm_interp=None
    model._flash_plan=None;model._flash_plan_sources=()
    from earth2studio.models.nn.stormscope_flash import configure_region
    configure_region(model,None,0)
    model.amp=base.amp
    model._ecore_engineering_only=True
    return model


def regional_topography(filename,latitude,longitude):
    """Require explicit model static-field scaling; never infer it from range."""
    import rasterio
    from pyproj import Transformer
    path=Path(filename)
    convention=path.with_suffix(path.suffix+'.json')
    if not convention.is_file():
        raise ValueError('Elevation needs a .tif.json sidecar documenting checkpoint topo units, scale_factor, add_offset and ocean treatment')
    info=json.loads(convention.read_text())
    if not {'scale_factor','add_offset','ocean','checkpoint_convention_source'}<=set(info):
        raise ValueError('Regional topography scaling evidence is incomplete')
    with rasterio.open(path) as source:
        xx,yy=Transformer.from_crs('EPSG:4326',source.crs,always_xy=True).transform(longitude,latitude)
        elevation=np.ma.stack(list(source.sample(zip(xx.ravel(),yy.ravel()),masked=True)))[:,0]
        if np.ma.getmaskarray(elevation).any():
            raise ValueError('Regional elevation contains missing or unsupported pixels')
        elevation=np.asarray(elevation).reshape(latitude.shape)
    if info['ocean']=='zero':elevation=np.maximum(elevation,0)
    elif info['ocean']!='retain':raise ValueError('Ocean convention must be zero or retain')
    elevation=elevation*float(info['scale_factor'])+float(info['add_offset'])
    if not np.isfinite(elevation).all():raise ValueError('Regional elevation has unsupported pixels')
    return elevation.astype('float32')


def model_check(unit,output,experimental=False,model_package=None,elevation=None):
    """Run actual prepared Caribbean observations, or retain the precise blocker."""
    import torch
    report={'engineering_only':True,'input_store':str(unit),'status':'blocked',
        'torch':torch.__version__,'cuda_build':torch.version.cuda,'cuda_available':torch.cuda.is_available()}
    def blocked(reason):
        report['reason']=reason
        atomic_json(Path(output)/'model-check.json',report)
        return report
    if not experimental:return blocked('Enable --experimental-caribbean for the explicitly out-of-domain engineering test')
    if not torch.cuda.is_available():return blocked('The model environment cannot access a CUDA GPU')
    files=importlib.metadata.distribution('earth2studio').files or []
    report['flash_api_installed']=any('stormscope_flash' in str(p) for p in files)
    try:
        import physicsnemo,natten
        from earth2studio.models.px import StormScopeGOES
        from earth2studio.models.nn.stormscope_flash import configure_region
    except (ImportError,OSError) as exc:
        return blocked(f'Model dependency/API unavailable: {type(exc).__name__}: {exc}')
    report['versions']={n:importlib.metadata.version(n) for n in ('earth2studio','nvidia-physicsnemo','natten')}
    if getattr(natten,'HAS_LIBNATTEN',True) is False:return blocked('NATTEN imports but its CUDA library is not available')
    if not elevation:return blocked('A regional elevation GeoTIFF and documented checkpoint static-field scaling are required (--elevation)')
    if not model_package:return blocked('Select a local compatible Flash package with --model-package; weights are not silently downloaded')
    from earth2studio.models.auto import Package
    from .process_store import ProcessedSource
    import icechunk as ic,xarray as xr
    path=Path(unit);state=json.loads((path/'progress.json').read_text())
    repo=ic.Repository.open(ic.local_filesystem_storage(str(path/'data.icechunk')))
    dataset=xr.open_zarr(repo.readonly_session(snapshot_id=state['snapshot_id']).store,
        consolidated=False,mask_and_scale=False,chunks=None)
    try:
        if dataset.attrs['grid']['resolution_m']!=3000:return blocked('Engineering model check starts with the 3 km variant')
        needed=['abi01c','abi02c','abi03c','abi07c','abi08c','abi09c','abi10c','abi13c']
        if not set(needed)<=set(map(str,dataset.variable.values)):
            return blocked('All eight study bands are required')
        # Choose an actual complete six-frame history; do not manufacture observations.
        available=np.isfinite(dataset.source_time_ns.sel(variable=needed).values) & (
            dataset.source_time_ns.sel(variable=needed).values!=np.iinfo('int64').min)
        first=next((i for i in range(5,len(available)) if available[i-5:i+1].all()),None)
        if first is None:return blocked('No complete six-frame GOES history exists in this materialized selection')
        times=dataset.time.values[first-5:first+1]
        base=StormScopeGOES.load_model(Package(str(model_package)),model_name='3km_10min_flash',compile=False).eval()
        for width in (256,200):
            try:
                y0=(dataset.sizes['y']-width)//2;x0=(dataset.sizes['x']-width)//2
                if min(y0,x0)<0:continue
                crop=dataset.isel(y=slice(y0,y0+width),x=slice(x0,x0+width))
                inputs=crop['values'].sel(time=times,variable=needed).values
                support=np.isfinite(inputs).all(axis=(0,1))
                if not support.any():
                    return blocked('The chosen history has no jointly valid pixels across all input bands; select a well-observed daylight sample')
                topo=regional_topography(elevation,crop.latitude.values,crop.longitude.values)
                model=caribbean_adapter(base,crop.latitude.values,crop.longitude.values,crop.y.values,crop.x.values,topo,support).to('cuda')
                tensor=torch.as_tensor(inputs[None,None],device='cuda',dtype=torch.float32)
                from collections import OrderedDict
                coords=OrderedDict(batch=np.array([0]),time=times[-1:],lead_time=np.arange(-50,1,10).astype('timedelta64[m]'),
                    variable=np.array(needed),y=crop.y.values,x=crop.x.values)
                torch.cuda.reset_peak_memory_stats();torch.cuda.synchronize();started=time.perf_counter()
                steps=[]
                with torch.inference_mode():
                    iterator=model.create_iterator(tensor,coords)
                    next(iterator)  # Initial state, not a forecast.
                    for _ in range(2):
                        forecast,valid_coords=next(iterator)
                        torch.cuda.synchronize()
                        values=forecast.detach().cpu().numpy()
                        if not np.isfinite(values[...,support]).all():
                            raise ValueError('The engineering forecast is nonfinite inside the supported input region')
                        steps.append({'shape':list(values.shape),'finite_fraction':float(np.isfinite(values).mean()),
                            'lead_time':str(valid_coords['lead_time'][-1])})
                report.update(status='complete',shape_yx=[width,width],forecast_steps=steps,
                    supported_input_fraction=float(support.mean()),
                    runtime_seconds=time.perf_counter()-started,peak_gpu_bytes=torch.cuda.max_memory_allocated(),
                    engineering_adapter='true regional coordinates/invariants; unchanged released weights; no forecast-skill claim')
                break
            except torch.cuda.OutOfMemoryError:
                torch.cuda.empty_cache()
                if width==200:return blocked('The 200×200 Caribbean Flash test exceeds available workstation GPU memory; use the documented Polaris model environment')
        if report['status']!='complete':return blocked('Prepared grid is smaller than the minimum supported Flash crop')
    finally:dataset.close()
    atomic_json(Path(output)/'model-check.json',report)
    return report
