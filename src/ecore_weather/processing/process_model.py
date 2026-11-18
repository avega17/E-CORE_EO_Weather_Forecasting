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

from ecore_weather.processing.transforms import atomic_json


def export_model_sample(unit,output,when,width=256,allow_partial=False):
    """Freeze six committed GOES frames; never expose an unfinished time chunk."""
    import hashlib
    import xarray as xr
    from ecore_weather.processing.process_store import reader_lease
    from ecore_weather.core.common import utc
    path=Path(unit);output=Path(output);output.mkdir(parents=True,exist_ok=True)
    if width<200 or width%4:raise ValueError('Flash crop must be >=200 and divisible by four')
    with reader_lease(path):
        state=json.loads((path/'progress.json').read_text())
        if not allow_partial and not (path/'complete.json').is_file():
            raise ValueError('Use --allow-partial to read only the immutable verified prefix of an active store')
        with xr.open_zarr(str(path/'data.zarr'),consolidated=False,mask_and_scale=False,chunks=None) as ds:
            if ds.attrs['grid']['resolution_m']!=3000:raise ValueError('The engineering check requires the 3 km grid')
            ds=ds.isel(time=slice(0,state['verified_slots']))
            needed=[f'abi{b:02d}c' for b in (1,2,3,7,8,9,10,13)]
            if not set(needed)<=set(map(str,ds.variable.values)):raise ValueError('All eight GOES bands are required')
            finish=np.datetime64(utc(when).replace(tzinfo=None),'ns')
            times=finish+np.arange(-50,1,10).astype('timedelta64[m]')
            if not set(times)<=set(ds.time.values):raise ValueError('The full six-frame history is not in the verified prefix')
            y0=(ds.sizes['y']-width)//2;x0=(ds.sizes['x']-width)//2
            if min(y0,x0)<0:raise ValueError('The requested crop exceeds the prepared grid')
            crop=ds.sel(time=times,variable=needed).isel(y=slice(y0,y0+width),x=slice(x0,x0+width))
            values=np.asarray(crop['values'].values,dtype='float32')
            valid=np.asarray(crop.pixel_valid.values,dtype='uint8')
            starts=np.asarray(crop.source_time_ns.values,dtype='int64')
            ends=np.asarray(crop.source_end_ns.values,dtype='int64')
            absent=np.iinfo('int64').min
            if (starts==absent).any() or (ends==absent).any() or (ends<starts).any() or (ends>times.astype('int64')[:,None]).any():
                raise ValueError('Input history has absent or future source observations')
            support=(np.isfinite(values)&valid.astype(bool)).all(axis=(0,1))&crop.inside_bbox.values
            if not support.any():raise ValueError('No jointly supported pixels; choose a better-observed daylight history')
            arrays=dict(values=values,pixel_valid=valid,support=support,time_ns=times.astype('int64'),
                source_time_ns=starts,source_end_ns=ends,variables=np.asarray(needed),
                latitude=crop.latitude.values,longitude=crop.longitude.values,y=crop.y.values,x=crop.x.values)
            target=output/'caribbean-sample.npz';temporary=target.with_suffix('.tmp.npz')
            np.savez_compressed(temporary,**arrays);temporary.replace(target)
            with target.open('rb') as stream:digest=hashlib.file_digest(stream,'sha256').hexdigest()
            result={'status':'complete','kind':'bounded engineering input, not a completed full-study store',
                'path':str(target.resolve()),'sha256':digest,'bytes':target.stat().st_size,
                'source_unit':str(path.resolve()),'verified_prefix':state['verified_slots'],
                'recipe_id':state['recipe_id'],'times':[np.datetime_as_string(t,unit='s')+'Z' for t in times],
                'variables':needed,'shape':list(values.shape),'supported_fraction':float(support.mean()),
                'per_band_valid_fraction':{v:float((np.isfinite(values[:,j])&valid[:,j].astype(bool)).mean()) for j,v in enumerate(needed)},
                'grid':ds.attrs['grid'],'parallax':ds.attrs.get('recipe',{}).get('parallax','unknown')}
            atomic_json(target.with_suffix('.json'),result)
            return result


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


def regional_topography(filename,latitude,longitude,allow_approximate=False):
    """Require explicit model static-field scaling; never infer it from range."""
    import rasterio
    from pyproj import Transformer
    path=Path(filename)
    convention=path.with_suffix(path.suffix+'.json')
    if not convention.is_file():
        raise ValueError('Elevation needs a .tif.json sidecar documenting checkpoint topo units, scale_factor, add_offset and ocean treatment')
    info=json.loads(convention.read_text())
    if info.get('validation_level')=='empirical-approximation' and not allow_approximate:
        raise ValueError('This topography calibration is approximate; an engineering test requires explicit --allow-approximate-topography')
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


def model_check(unit,output,experimental=False,model_package=None,elevation=None,
                sample=None,when=None,allow_partial=False,width=256,
                allow_approximate=False,download_model=False,seed=0):
    """Bounded regional execution with real observations and explicit static evidence."""
    import torch
    report={'engineering_only':True,'forecast_skill_validated':False,'input_store':str(unit),
        'status':'blocked','phase':'preflight','seed':seed,'torch':torch.__version__,
        'cuda_build':torch.version.cuda,'cuda_available':torch.cuda.is_available()}
    try:
        if not experimental:raise ValueError('Enable --experimental-caribbean for the out-of-domain engineering test')
        if sample is None:
            if not unit or not when:raise ValueError('Supply --sample, or --unit and --sample-time')
            exported=export_model_sample(unit,Path(output)/'model-input',when,width,allow_partial)
            sample=exported['path']
        import hashlib
        sample=Path(sample);manifest=json.loads(sample.with_suffix('.json').read_text())
        with sample.open('rb') as stream:actual=hashlib.file_digest(stream,'sha256').hexdigest()
        if actual!=manifest['sha256']:raise IOError('The portable input differs from its verified sample manifest')
        with np.load(sample,allow_pickle=False) as file:arrays={name:file[name] for name in file.files}
        needed=np.asarray([f'abi{b:02d}c' for b in (1,2,3,7,8,9,10,13)])
        np.testing.assert_array_equal(arrays['variables'],needed)
        times=arrays['time_ns'].astype('datetime64[ns]')
        if len(times)!=6 or not np.all(np.diff(times)==np.timedelta64(10,'m')):
            raise ValueError('The input must contain six consecutive ten-minute observations')
        if (arrays['source_end_ns']>arrays['time_ns'][:,None]).any():raise ValueError('Future observations enter the input')
        report.update(sample_sha256=actual,input_times=manifest['times'],recipe_id=manifest['recipe_id'],
            verified_prefix=manifest['verified_prefix'],parallax=manifest['parallax'],source_unit=manifest['source_unit'])
        if not elevation:raise ValueError('A regional elevation GeoTIFF and documented static-field scaling are required (--elevation)')
        topo_info=json.loads(Path(elevation).with_suffix(Path(elevation).suffix+'.json').read_text())
        report['topography']=topo_info
        topo=regional_topography(elevation,arrays['latitude'],arrays['longitude'],allow_approximate)
        if not torch.cuda.is_available():raise RuntimeError('The model environment cannot access a CUDA GPU')
        torch.manual_seed(seed);torch.cuda.manual_seed_all(seed)
        if not model_package and not download_model:raise ValueError('Provide --model-package or explicitly allow --download-model')
        from earth2studio.models.px import StormScopeGOES
        from earth2studio.models.auto import Package
        from collections import OrderedDict
        report.update(gpu=torch.cuda.get_device_name(),versions={n:importlib.metadata.version(n) for n in
            ('earth2studio','nvidia-physicsnemo','natten','earth2grid')})
        report['phase']='model-load';began=time.perf_counter()
        package=Package(str(model_package)) if model_package else StormScopeGOES.load_default_package()
        base=StormScopeGOES.load_model(package,model_name='3km_10min_flash',compile=False,amp=True).eval().to('cuda')
        report['model_load_seconds']=time.perf_counter()-began
        available=arrays['values'].shape[-1]
        sizes=list(dict.fromkeys([min(width,available),200]))
        for side in sizes:
            try:
                if side>available:continue
                y0=(arrays['values'].shape[-2]-side)//2;x0=(available-side)//2
                crop=(slice(y0,y0+side),slice(x0,x0+side))
                inputs=arrays['values'][:,:,crop[0],crop[1]]
                support=arrays['support'][crop]
                if not support.any():raise ValueError('The input has no jointly supported pixels')
                model=caribbean_adapter(base,arrays['latitude'][crop],arrays['longitude'][crop],
                    arrays['y'][crop[0]],arrays['x'][crop[1]],topo[crop],support)
                tensor=torch.as_tensor(inputs[None,None],device='cuda',dtype=torch.float32)
                coords=OrderedDict(batch=np.array([0]),time=times[-1:],
                    lead_time=np.arange(-50,1,10).astype('timedelta64[m]'),variable=needed,
                    y=arrays['y'][crop[0]],x=arrays['x'][crop[1]])
                report['phase']='forecast';torch.cuda.reset_peak_memory_stats();torch.cuda.synchronize()
                began=time.perf_counter();steps=[];predictions=[]
                with torch.inference_mode():
                    iterator=model.create_iterator(tensor,coords);next(iterator)
                    for _ in range(2):
                        forecast,forecast_coords=next(iterator);torch.cuda.synchronize()
                        values=forecast.detach().cpu().numpy()
                        if not np.isfinite(values[...,support]).all():raise ValueError('Nonfinite forecast inside the supported region')
                        lead=forecast_coords['lead_time'][-1]
                        steps.append({'shape':list(values.shape),'finite_fraction':float(np.isfinite(values).mean()),
                            'lead_minutes':int(lead/np.timedelta64(1,'m')),
                            'valid_time':np.datetime_as_string(np.datetime64(forecast_coords['time'][-1])+lead,unit='s')+'Z'})
                        predictions.append(values[0,0,0])
                forward_seconds=time.perf_counter()-began
                target=Path(output);target.mkdir(parents=True,exist_ok=True)
                np.savez_compressed(target/'caribbean-forecast.npz',predictions=np.stack(predictions),
                    last_observation=inputs[-1],latitude=arrays['latitude'][crop],longitude=arrays['longitude'][crop],
                    variables=needed,support=support,lead_minutes=np.array([10,20]))
                report.update(status='complete',phase='complete',shape_yx=[side,side],forecast_steps=steps,
                    supported_input_fraction=float(support.mean()),runtime_seconds=forward_seconds,
                    peak_gpu_bytes=torch.cuda.max_memory_allocated(),
                    engineering_adapter='true Caribbean coordinates, native checkpoint weights, explicit regional topography; skill unvalidated')
                break
            except torch.cuda.OutOfMemoryError:
                if 'tensor' in locals():del tensor
                if 'model' in locals():del model
                torch.cuda.empty_cache()
                if side==200:raise
    except Exception as exc:
        if report['phase'] in ('model-load','forecast'):report['status']='failed'
        report['reason']=f'{type(exc).__name__}: {exc}'
    atomic_json(Path(output)/'model-check.json',report)
    return report


def loader_check(unit,reference,output):
    """Instantiate the unmodified official XarrayDataset without a training loop."""
    import importlib,sys,types
    path=Path(reference).resolve()
    if not (path/'xarray.py').is_file():raise ValueError('Select the official regional_weather_diffusion datasets directory')
    package=types.ModuleType('_ecore_physicsnemo_reference');package.__path__=[str(path)]
    sys.modules[package.__name__]=package
    module=importlib.import_module(package.__name__+'.xarray')
    unit=Path(unit)
    with xr_open(unit) as dataset:
        channels=[str(v) for v in dataset.variable.values if str(v).startswith('abi')]
        if not channels:channels=list(map(str,dataset.variable.values))
    params={'data_paths':[str(unit/'data.zarr')],'stats_path':str(unit/'stats.json'),
        'invariants_path':str(unit/'invariants.zarr'),'lat_lon_variables':['latitude','longitude'],
        'mask_variable':'inside_bbox','nan_mask':True,'time_step_seconds':600,'num_threads':2,
        'open_dataset_kwargs':{'engine':'zarr','consolidated':False,'mask_and_scale':False},
        'state_access_patterns':{'values':{'channels':channels,'time_steps':[1]}},
        'background_access_patterns':{'values':{'channels':channels,'time_steps':[-5,-4,-3,-2,-1,0]}}}
    loader=module.XarrayDataset(params,train=True)
    if not len(loader):raise ValueError('Stock loader found no complete seven-state training window')
    sample=loader[0]
    result={'status':'complete','samples':len(loader),'channels':channels,
        'shapes':{name:list(np.shape(value)) for name,value in sample.items()},
        'scope':'official stock dataset load only; no model training or forecast-skill claim'}
    atomic_json(Path(output)/'loader-check.json',result)
    return result


def xr_open(unit):
    import xarray as xr
    return xr.open_zarr(str(Path(unit)/'data.zarr'),consolidated=False,mask_and_scale=False,chunks=None)


def evaluate_caribbean_forecast(forecast_path,sample_path,output,figure_dir=None):
    """Compare with committed later observations locally, never as model inputs."""
    import pandas as pd
    from ecore_weather.processing.process_store import reader_lease
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    sample_path=Path(sample_path)
    manifest=json.loads(sample_path.with_suffix('.json').read_text())
    with np.load(sample_path,allow_pickle=False) as archive:sample={k:archive[k] for k in archive.files}
    with np.load(forecast_path,allow_pickle=False) as archive:forecast={k:archive[k] for k in archive.files}
    np.testing.assert_array_equal(forecast['variables'],sample['variables'])
    h,w=forecast['predictions'].shape[-2:]
    y0=(len(sample['y'])-h)//2;x0=(len(sample['x'])-w)//2
    if min(x0,y0)<0:raise ValueError('Forecast extends beyond its input sample')
    ys=sample['y'][y0:y0+h];xs=sample['x'][x0:x0+w]
    for name in ('latitude','longitude'):
        np.testing.assert_array_equal(forecast[name],sample[name][y0:y0+h,x0:x0+w])
    initial=sample['values'][-1,:,y0:y0+h,x0:x0+w]
    np.testing.assert_array_equal(forecast['last_observation'],initial)
    variables=list(map(str,forecast['variables']));rows=[];truth=[]
    with reader_lease(manifest['source_unit']):
        state=json.loads((Path(manifest['source_unit'])/'progress.json').read_text())
        with xr_open(manifest['source_unit']) as dataset:
            dataset=dataset.isel(time=slice(0,state['verified_slots']))
            for index,lead in enumerate(forecast['lead_minutes']):
                when=np.datetime64(manifest['times'][-1].removesuffix('Z'),'ns')+np.timedelta64(int(lead),'m')
                target=dataset.sel(time=when,variable=variables,y=ys,x=xs).load()
                if (target.source_end_ns.values>when.astype('int64')).any():raise ValueError('Evaluation has future source observations')
                observed=target['values'].values;truth.append(observed)
                predicted=forecast['predictions'][index]
                for j,variable in enumerate(variables):
                    valid=(forecast['support'].astype(bool)&target.pixel_valid.values[j].astype(bool)
                        &np.isfinite(observed[j])&np.isfinite(predicted[j])&np.isfinite(initial[j]))
                    count=int(valid.sum());observations=observed[j][valid].astype('float64')
                    error=predicted[j][valid].astype('float64')-observations
                    persistence=initial[j][valid].astype('float64')-observations
                    rows.append({'variable':variable,'units':'1' if int(variable[3:5])<=6 else 'K',
                        'lead_minutes':int(lead),'valid_time':np.datetime_as_string(when,unit='s')+'Z',
                        'pixels':count,'valid_fraction':count/valid.size,
                        'mae':float(np.abs(error).mean()) if count else None,
                        'rmse':float(np.sqrt(np.mean(error**2))) if count else None,
                        'bias':float(error.mean()) if count else None,
                        'persistence_mae':float(np.abs(persistence).mean()) if count else None,
                        'persistence_rmse':float(np.sqrt(np.mean(persistence**2))) if count else None})
    frame=pd.DataFrame(rows);frame.to_csv(output/'caribbean-comparison.csv',index=False)
    report={'status':'complete','scope':'single uncorrected Caribbean example; approximate topography; no forecast-skill validation',
        'input_end':manifest['times'][-1],'source_unit':manifest['source_unit'],
        'source_verified_prefix':manifest['verified_prefix'],'evaluation_verified_prefix':state['verified_slots'],
        'future_observations_sent_to_model':False,'comparisons':rows}
    if figure_dir:
        import matplotlib.pyplot as plt
        figure_dir=Path(figure_dir);figure_dir.mkdir(parents=True,exist_ok=True)
        fig,axes=plt.subplots(2,3,figsize=(13,8),layout='constrained')
        extent=[(xs.min()-1500)/1000,(xs.max()+1500)/1000,(ys.min()-1500)/1000,(ys.max()+1500)/1000]
        origin='upper' if ys[0]>ys[-1] else 'lower'
        for row,variable in enumerate(('abi02c','abi13c')):
            j=variables.index(variable);options=({'cmap':'gray','vmin':0,'vmax':1} if row==0 else {'cmap':'magma_r','vmin':200,'vmax':310})
            for col,(title,values) in enumerate((('Last input',initial[j]),('Forecast +20 min',forecast['predictions'][-1,j]),('Observed +20 min',truth[-1][j]))):
                axis=axes[row,col]
                image=axis.imshow(np.where(forecast['support'],values,np.nan),origin=origin,extent=extent,**options)
                axis.set_title(f'{variable.upper()} · {title}');axis.set_xlabel('East–west position (km)')
                if col==0:axis.set_ylabel('North–south position (km)')
            fig.colorbar(image,ax=axes[row,:],label='Reflectance factor' if row==0 else 'Brightness temperature (K)',shrink=.8)
        fig.suptitle('Experimental Caribbean Flash run · approximate topography · uncorrected imagery\n'+manifest['times'][-1])
        target=figure_dir/'caribbean-flash-check.png';fig.savefig(target,dpi=160);plt.close(fig)
        report['figure']=str(target)
    atomic_json(output/'caribbean-comparison.json',report)
    return report


def stock_flash_check(output,model_package=None):
    """Small supported-CONUS reference using NVIDIA's stock GOES workflow."""
    import torch
    report={'status':'blocked','scope':'supported-CONUS stock reference; separate from Caribbean adaptation',
        'phase':'dependency-preflight'}
    try:
        if not torch.cuda.is_available():raise RuntimeError('A CUDA GPU is required')
        report.update(torch=torch.__version__,cuda_build=torch.version.cuda,gpu=torch.cuda.get_device_name())
        import importlib
        try:importlib.import_module('earth2grid.spatial')
        except (ImportError,OSError) as exc:
            raise RuntimeError('Earth2Studio interpolation requires earth2grid; validate its official source installation in the separate model environment before downloading weights') from exc
        from natten.functional import na2d
        q=torch.randn(1,16,16,2,32,device='cuda',dtype=torch.float16)
        with torch.inference_mode():result=na2d(q,q,q,kernel_size=3)
        if not torch.isfinite(result).all():raise RuntimeError('NATTEN CUDA operation returned nonfinite values')
        report['cuda_kernel_check']='finite NATTEN na2d output'
        report['versions']={name:importlib.metadata.version(name) for name in
            ('earth2studio','nvidia-physicsnemo','natten','earth2grid')}
        from earth2studio.models.px import StormScopeGOES
        from earth2studio.models.auto import Package
        from earth2studio.data import GOES,fetch_data
        from collections import OrderedDict
        began=time.perf_counter()
        report['phase']='model-load'
        package=Package(str(model_package)) if model_package else StormScopeGOES.load_default_package()
        base=StormScopeGOES.load_model(package,model_name='3km_10min_flash',compile=False,amp=True).eval().to('cuda')
        model=base.set_domain(39.,40.,-98.,-97.,padding=25)
        report['model_load_seconds']=time.perf_counter()-began
        report['phase']='source-interpolator'
        source=GOES(satellite='goes16',scan_mode='C',max_workers=2,retries=1)
        latitude,longitude=GOES.grid(satellite='goes16',scan_mode='C')
        model.build_input_interpolator(latitude,longitude)
        # The limited request uses six observations within two source hours.
        from ecore_weather.core.common import s3_client
        client=s3_client();listed=0
        for hour in (15,16):
            response=client.list_objects_v2(Bucket='noaa-goes16',Prefix=f'ABI-L2-MCMIPC/2022/261/{hour:02d}/')
            listed+=sum(row['Size'] for row in response.get('Contents',[]))
        if listed>4*1024**3:raise RuntimeError('Stock source-hour inventory exceeds the 4 GiB reference budget')
        report['phase']='public-source-read'
        before=time.perf_counter()
        from loguru import logger
        observed=[]
        def capture(message):
            text=message.record['message'];prefix='Fetching GOES file: '
            if text.startswith(prefix):observed.append(text[len(prefix):])
        sink=logger.add(capture,level='DEBUG')
        try:
            values,coords=fetch_data(source,time=np.array(['2022-09-18T16:00'],dtype='datetime64[ns]'),
                variable=model.input_coords()['variable'],lead_time=model.input_coords()['lead_time'],device='cuda')
        finally:logger.remove(sink)
        report['source_keys']=list(dict.fromkeys(observed))
        report['source_matching_scope']='Stock nearest-scan behavior, not a causal regional-source validation'
        report['fetch_seconds']=time.perf_counter()-before
        tensor=values.unsqueeze(0);coords=OrderedDict(batch=np.array([0]),**coords)
        report['phase']='forecast'
        torch.cuda.reset_peak_memory_stats();torch.cuda.synchronize();began=time.perf_counter();steps=[]
        with torch.inference_mode():
            iterator=model.create_iterator(tensor,coords);next(iterator)
            for _ in range(2):
                predicted,coordinates=next(iterator);torch.cuda.synchronize()
                if not torch.isfinite(predicted[...,model.valid_mask]).all():raise ValueError('Nonfinite supported forecast pixels')
                lead=coordinates['lead_time'][-1]
                valid_time=np.datetime64(coordinates['time'][-1])+lead
                steps.append({'shape':list(predicted.shape),'lead_minutes':int(lead/np.timedelta64(1,'m')),
                    'valid_time':np.datetime_as_string(valid_time,unit='s')+'Z'})
        report.update(status='complete',phase='complete',steps=steps,forward_seconds=time.perf_counter()-began,
            peak_gpu_bytes=torch.cuda.max_memory_allocated(),gpu=torch.cuda.get_device_name(),
            listed_source_hour_bytes=listed,source='stock GOES16 MCMIPC, requested 2022-09-18T16:00Z')
    except Exception as exc:
        report['reason']=f'{type(exc).__name__}: {exc}'
    atomic_json(Path(output)/'model-reference.json',report)
    return report
