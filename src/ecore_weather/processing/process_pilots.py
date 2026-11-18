"""Small source-backed parallax and historical-availability experiments."""
from __future__ import annotations
from pathlib import Path
import json
import time

import numpy as np
import pandas as pd

from ecore_weather.processing.transforms import NativeReader, atomic_json, decode_observation, regrid, target_grid
from ecore_weather.processing.process_catalog import load_index, recipe_from_dict


def parallax_pilot(preparation,output,budget_bytes=2*1024**3):
    from ecore_weather.core.common import Transport
    from ecore_weather.processing.process_parallax import fetch_height, corrected_mapping
    info,aligned,_=load_index(preparation)
    recipe=recipe_from_dict(info['recipe']);grid=target_grid(recipe)
    cache=Path(preparation)/'parallax'
    rows=[];selected=aligned[aligned.variable.isin(['abi01c','abi02c','abi03c','abi07c','abi13c'])]
    if selected.empty:raise ValueError('Parallax pilot requires a validated cloud band')
    with NativeReader(2) as reader,Transport('obstore') as transport:
        for variable,group in selected.groupby('variable'):
            seen=set()
            for hour in (4,16):
                stamps=pd.to_datetime(group.time,utc=True)
                nearest=int(np.argmin(np.abs(stamps.dt.hour.to_numpy()-hour)))
                record=group.iloc[nearest].to_dict()
                if record['asset_id'] in seen:continue
                seen.add(record['asset_id'])
                started=time.perf_counter()
                try:
                    receipt=cache/'heights'/f'{record["asset_id"]}.json'
                    from ecore_weather.processing.process_parallax import archived_height
                    evidence=json.loads(receipt.read_text()) if receipt.exists() else archived_height(record)
                    if evidence is None:evidence=fetch_height(record,recipe.bbox,cache,transport,budget_bytes-transport.bytes)
                    atomic_json(receipt,evidence)
                    dataset=reader.read(record)
                    mapping=corrected_mapping(dataset,record,grid,cache)
                    transformed=regrid(decode_observation(dataset,quality=recipe.quality),mapping)
                    rows.append({'variable':variable,'time':record['time'],'status':'verified',
                        'height':evidence,'valid_fraction':float(transformed['valid'].mean()),
                        'median_displacement_m':float(np.nanmedian(mapping['displacement_m'])),
                        'max_displacement_m':float(np.nanmax(mapping['displacement_m'])),
                        'seconds':time.perf_counter()-started})
                    _parallax_figure(dataset,record,grid,transformed,Path('figures/dataset-processing'),mapping)
                except (ValueError,OSError,KeyError) as exc:
                    rows.append({'variable':variable,'time':record['time'],'status':'unavailable',
                                 'error':f'{type(exc).__name__}: {exc}'})
        result={'status':'complete' if all(r['status']=='verified' for r in rows) else 'blocked',
            'cases':rows,'returned_bytes':transport.bytes,'budget_bytes':budget_bytes,
            'scope':'C01/C02/C03/C07/C13 selected-hour cloud-height pilot; actual scan times are recorded'}
    Path(output).mkdir(parents=True,exist_ok=True)
    atomic_json(Path(output)/'parallax-pilot.json',result)
    return result


def _parallax_figure(dataset,record,grid,corrected,output,mapping=None):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from ecore_weather.processing.transforms import geometry
    baseline=regrid(decode_observation(dataset),geometry(dataset,grid,'bilinear'))
    figure,axes=plt.subplots(1,3,figsize=(13,4),layout='constrained')
    left,bottom,right,top=grid.attrs['bounds_m'];extent=[left/1000,right/1000,bottom/1000,top/1000]
    samples=np.concatenate([baseline['values'].ravel(),corrected['values'].ravel()])
    finite=samples[np.isfinite(samples)]
    limits=(float(finite.min()),float(finite.max())) if finite.size else (0.,1.)
    difference=corrected['values']-baseline['values']
    delta=max(float(np.nanmax(np.abs(difference))),1e-6) if np.isfinite(difference).any() else 1.
    units='reflectance factor' if int(record['variable'][3:5])<=6 else 'K'
    for axis,values,title in zip(axes,[baseline['values'],corrected['values'],corrected['values']-baseline['values']],
                                ['Uncorrected','Cloud-height corrected','Corrected − uncorrected']):
        is_difference=title.startswith('Corrected −')
        image=axis.imshow(values,extent=extent,origin='upper',
            cmap='RdBu_r' if is_difference else 'viridis',
            vmin=-delta if is_difference else limits[0],vmax=delta if is_difference else limits[1])
        figure.colorbar(image,ax=axis,shrink=.7,label=units)
        axis.set(title=title,xlabel='Projected easting (km)',ylabel='Northing (km)')
    figure.suptitle(f'{record["variable"]} · {record["time"]} · experimental correction')
    output.mkdir(parents=True,exist_ok=True)
    figure.savefig(output/f'parallax-{record["asset_id"]}.png',dpi=150)
    plt.close(figure)
    if mapping is not None:
        fig,ax=plt.subplots(figsize=(6,5),layout='constrained')
        image=ax.imshow(mapping['displacement_m'].reshape(grid.inside_bbox.shape)/1000,extent=extent,origin='upper',cmap='magma')
        fig.colorbar(image,ax=ax,label='Apparent-to-corrected displacement (km)')
        ax.set(title=f'{record["variable"]} · {record["time"]}',xlabel='Projected easting (km)',ylabel='Northing (km)')
        fig.savefig(output/f'parallax-displacement-{record["asset_id"]}.png',dpi=150);plt.close(fig)


def nam_audit(output,budget_bytes=2*1024**3):
    """Representative monthly evidence and bounded selected-message sizing."""
    import re
    from io import BytesIO
    from ecore_weather.core.common import s3_client,PR_BBOX
    client=s3_client();rows=[];samples=[];returned=0
    def selected_field(line):
        parts=line.split(':');name,level=parts[3:5]
        return (name in ('REFC','APCP','PRATE','PWAT') or name=='HGT' and level=='500 mb'
            or name=='TMP' and level=='2 m above ground' or name in ('UGRD','VGRD') and level=='10 m above ground')
    def get(bucket,key,byte_range=None):
        nonlocal returned
        kwargs={'Bucket':bucket,'Key':key}
        if byte_range:kwargs['Range']=byte_range
        response=client.get_object(**kwargs)
        try:
            if byte_range:
                begin,finish=map(int,byte_range.removeprefix('bytes=').split('-'))
                if (response.get('ContentLength')!=finish-begin+1 or
                    not response.get('ContentRange','').startswith(f'bytes {begin}-{finish}/')):
                    raise IOError('NOAA/proxy did not honor the selected GRIB message range; refusing full-file fallback')
            data=response['Body'].read(min(budget_bytes-returned+1,int(response.get('ContentLength',budget_bytes))+1))
            returned+=len(data)
            if returned>budget_bytes:raise OSError('NWP audit transfer budget exhausted')
            return data
        finally:response['Body'].close()
    for month in pd.date_range('2021-01-01','2026-06-01',freq='MS'):
        date=month.strftime('%Y%m%d');prefix=f'nam.{date}/nam.t00z.priconest.'
        try:
            objects=client.list_objects_v2(Bucket='noaa-nam-pds',Prefix=prefix).get('Contents',[])
            keys={o['Key']:o for o in objects};idx=[k for k in keys if k.endswith('.idx')]
            row={'month':str(month.date())[:7],'sample_date':date,'cycle':'00','listed_objects':len(objects),
                'index_files':len(idx),'scope':'first-day 00 UTC cycle only; not complete monthly coverage'}
            chosen=next((k for k in idx if 'hiresf01.' in k),idx[0] if idx else None)
            if chosen:
                text=get('noaa-nam-pds',chosen).decode();lines=[]
                for line in text.splitlines():
                    parts=line.split(':')
                    if len(parts)>5:lines.append((int(parts[1]),line))
                matches=[];gribkey=chosen[:-4]
                size=keys.get(gribkey,{}).get('Size')
                for i,(offset,line) in enumerate(lines):
                    if selected_field(line):
                        following=next((position for position,_ in lines[i+1:] if position>offset),size)
                        stop=following-1 if following else None
                        matches.append({'descriptor':line.split(':',2)[2],'offset':offset,'bytes':stop-offset+1 if stop else None})
                        # Four seasonal examples on the first study year, no whole forecasts.
                        if month.year in (2021,2022) and month.month in (1,4,7,10) and stop and len(samples)<48:
                            try:content=get('noaa-nam-pds',gribkey,f'bytes={offset}-{stop}')
                            except IOError as exc:
                                matches[-1]['sample_error']=str(exc);continue
                            import tempfile,pygrib,zarr
                            from zarr.codecs import BloscCodec
                            with tempfile.NamedTemporaryFile(suffix='.grib2') as tmp:
                                tmp.write(content);tmp.flush()
                                with pygrib.open(tmp.name) as gribs:
                                    field=line.split(':')[3]
                                    aliases={'HGT':('gh',),'REFC':('refc',),'APCP':('tp',),'PRATE':('prate',),
                                        'TMP':('2t','t'),'UGRD':('10u','u'),'VGRD':('10v','v'),'PWAT':('pwat',)}
                                    messages=[m for m in gribs if m.shortName in aliases[field]]
                                    if field in ('HGT','TMP','UGRD','VGRD'):
                                        expected_level={'HGT':500,'TMP':2,'UGRD':10,'VGRD':10}[field]
                                        messages=[m for m in messages if m.level==expected_level]
                                    if not messages:raise ValueError(f'GRIB range did not decode the indexed field {field}')
                                    message=messages[0];values=message.values;lat,lon=message.latlons();lon=(lon+180)%360-180
                                    mask=(lon>=PR_BBOX[0])&(lon<=PR_BBOX[2])&(lat>=PR_BBOX[1])&(lat<=PR_BBOX[3])
                                    ys,xs=np.where(mask)
                                    if not len(ys):continue
                                    crop=np.ma.filled(values[ys.min():ys.max()+1,xs.min():xs.max()+1],np.nan).astype('float32')
                                    memory=zarr.storage.MemoryStore();group=zarr.group(store=memory,zarr_format=3)
                                    arr=group.create_array('sample',data=crop,compressors=BloscCodec(cname='zstd',clevel=3))
                                    samples.append({'month':row['month'],'descriptor':line.split(':',2)[2],'field':message.shortName,'units':message.units,
                                        'grid_type':message.gridType,'full_shape':list(values.shape),'crop_shape':list(crop.shape),
                                        'source_bounds':[float(lon.min()),float(lat.min()),float(lon.max()),float(lat.max())],
                                        'message_bytes':len(content),'crop_logical_bytes':crop.nbytes,'zarr_bytes':arr.nbytes_stored(),
                                        'initialization':str(message.analDate),'valid_time':str(message.validDate),
                                        'forecast_time':message.forecastTime,'step_range':str(message.stepRange)})
                row['selected_messages']=matches
            rows.append(row)
            atomic_json(Path(output)/'nam-availability.json',{'status':'running','months':rows,'samples':samples,'returned_bytes':returned})
        except Exception as exc:
            rows.append({'month':str(month.date())[:7],'status':'unavailable','error':f'{type(exc).__name__}: {exc}'})
            if returned>=budget_bytes:break
    rrfs=[]
    for date in ('20260601','20261103'):
        try:
            found=client.list_objects_v2(Bucket='noaa-rrfs-ops-pds',Prefix=f'rrfs.{date}/',MaxKeys=2).get('Contents',[])
            rrfs.append({'date':date,'examples':[r['Key'] for r in found],'scope':'bounded prefix probe, not a history assertion'})
        except Exception as exc:rrfs.append({'date':date,'error':str(exc)})
    result={'status':'complete' if len(rows)==66 else 'paused','months':rows,'samples':samples,'rrfs':rrfs,
        'returned_bytes':returned,'budget_bytes':budget_bytes,
        'scope':'sampled cycles; range-selected messages only; no bulk NWP ingestion'}
    if samples:
        frame=pd.DataFrame(samples);hours=int((pd.Timestamp('2026-07-01')-pd.Timestamp('2021-01-01')).total_seconds()/3600)
        result['nominal_hourly_storage']={name:{'logical_bytes':int(group.crop_logical_bytes.mean()*hours),
            'compressed_bytes':int(group.zarr_bytes.mean()*hours),'sample_frames':len(group)} for name,group in frame.groupby('field')}
        result['estimate_caveat']='Nominal hourly series only; does not establish availability, cycle stitching or whole-ROI support'
    atomic_json(Path(output)/'nam-availability.json',result);return result


def retire_icechunk(location,replacement,output,index_path=None,apply=False):
    """Retire managed derived assets after a verified plain-Zarr replacement."""
    import shutil,fcntl
    from contextlib import ExitStack
    from ecore_weather.processing.process_store import validate_store,owner
    validate_store(replacement)
    root=Path(location).resolve();deleted=[]
    for selection in sorted((root/'processed').iterdir()):
        if not selection.is_dir() or not list(selection.glob('*/data.icechunk')):continue
        prep=root/'preparation'/selection.name
        sources=pd.read_parquet(prep/'archives.parquet')
        for row in sources.itertuples():
            source=Path(row.path).resolve()
            if not source.is_relative_to(root) or not source.is_file() or source.stat().st_size!=row.stored_bytes:
                raise IOError(f'Cannot rebuild retired asset: missing/changed native source {source}')
        with ExitStack() as stack:
            for store in selection.glob('*/data.icechunk'):
                stack.enter_context(owner(store.parent))
                lease=stack.enter_context((store.parent/'readers.lock').open('a'))
                try:fcntl.flock(lease,fcntl.LOCK_EX|fcntl.LOCK_NB)
                except BlockingIOError as exc:raise RuntimeError(f'Active reader owns {store.parent}') from exc
            size=sum(p.stat().st_size for p in selection.rglob('*') if p.is_file())
            size+=sum(p.stat().st_size for p in prep.rglob('*') if p.is_file())
            deleted.append({'selection':selection.name,'bytes':size})
            if apply:
                shutil.rmtree(selection)
                if prep.is_dir():shutil.rmtree(prep)
    # Also retire obsolete standalone virtual catalogs, keeping usable Parquet indices.
    for catalog in (root/'preparation').glob('*/native.icechunk'):
        parent=catalog.parent
        with owner(parent/'catalog-owner'):
            size=sum(p.stat().st_size for p in catalog.rglob('*') if p.is_file())
            deleted.append({'catalog':str(catalog.relative_to(root)),'bytes':size})
            if apply:
                shutil.rmtree(catalog);(parent/'virtual.json').unlink(missing_ok=True)
    if apply and index_path and Path(index_path).is_file():
        import duckdb
        with duckdb.connect(str(index_path)) as con:
            for row in deleted:
                if row.get('selection'):con.execute('DELETE FROM processing_selections WHERE id=?',[row['selection']])
    result={'applied':apply,'retired':deleted,'deleted_bytes':sum(r['bytes'] for r in deleted)}
    if apply:
        receipt=Path(output)/'retired-stores.json'
        prior=json.loads(receipt.read_text()) if receipt.is_file() else {}
        combined={r.get('selection',r.get('catalog')):r for r in prior.get('retired',[])+deleted}
        atomic_json(receipt,{'applied':True,'retired':list(combined.values()),
            'deleted_bytes':sum(r['bytes'] for r in combined.values()),
            'retired_receipts':prior.get('retired_receipts',[])})
    return result
