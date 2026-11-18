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
    rows=[];selected=aligned[aligned.variable.isin(['abi02c','abi13c'])]
    if selected.empty:raise ValueError('Parallax pilot requires C02 or C13')
    with NativeReader(2) as reader,Transport('obstore') as transport:
        for variable,group in selected.groupby('variable'):
            for hour in (4,16):
                stamps=pd.to_datetime(group.time,utc=True)
                nearest=int(np.argmin(np.abs(stamps.dt.hour.to_numpy()-hour)))
                record=group.iloc[nearest].to_dict()
                started=time.perf_counter()
                try:
                    receipt=cache/'heights'/f'{record["asset_id"]}.json'
                    evidence=json.loads(receipt.read_text()) if receipt.exists() else fetch_height(
                        record,recipe.bbox,cache,transport,budget_bytes-transport.bytes)
                    dataset=reader.read(record)
                    mapping=corrected_mapping(dataset,record,grid,cache)
                    transformed=regrid(decode_observation(dataset,quality=recipe.quality),mapping)
                    rows.append({'variable':variable,'time':record['time'],'status':'verified',
                        'height':evidence,'valid_fraction':float(transformed['valid'].mean()),
                        'seconds':time.perf_counter()-started})
                    _parallax_figure(dataset,record,grid,transformed,Path('figures/dataset-processing'))
                except (ValueError,OSError,KeyError) as exc:
                    rows.append({'variable':variable,'time':record['time'],'status':'unavailable',
                                 'error':f'{type(exc).__name__}: {exc}'})
        result={'status':'complete' if all(r['status']=='verified' for r in rows) else 'blocked',
            'cases':rows,'returned_bytes':transport.bytes,'budget_bytes':budget_bytes,
            'scope':'C02/C13 day/night cloud-height pilot; not a full-study correction'}
    Path(output).mkdir(parents=True,exist_ok=True)
    atomic_json(Path(output)/'parallax-pilot.json',result)
    return result


def _parallax_figure(dataset,record,grid,corrected,output):
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
    units='reflectance factor' if record['variable']=='abi02c' else 'K'
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


def nam_audit(output):
    from ecore_weather.core.common import s3_client
    client=s3_client();rows=[]
    # Listing only: bounded representative dates, never fetch forecast GRIBs.
    for date in ['20210101','20220101','20230101','20240101','20250101','20260630']:
        prefix=f'nam.{date}/nam.t00z.priconest.'
        response=client.list_objects_v2(Bucket='noaa-nam-pds',Prefix=prefix,MaxKeys=8)
        objects=response.get('Contents',[])
        rows.append({'date':date,'prefix':prefix,'listed_examples':len(objects),
            'first_key':objects[0]['Key'] if objects else None,
            'first_size':objects[0]['Size'] if objects else None,
            'scope':'one representative cycle; empty listing is not proof all historical archives lack data'})
    result={'status':'complete','cases':rows,'imagery_download_bytes':0}
    Path(output).mkdir(parents=True,exist_ok=True)
    atomic_json(Path(output)/'nam-availability.json',result)
    return result
