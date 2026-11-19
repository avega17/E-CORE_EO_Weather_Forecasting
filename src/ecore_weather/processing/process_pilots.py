"""Small source-backed parallax and historical-availability experiments."""
from __future__ import annotations
from pathlib import Path
import json
import time

import numpy as np
import pandas as pd

from ecore_weather.processing.transforms import NativeReader, atomic_json, decode_observation, regrid, target_grid
from ecore_weather.processing.process_catalog import load_index, recipe_from_dict


def parallax_pilot(preparation,output,budget_bytes=4*1024**3):
    from ecore_weather.core.common import Transport
    from ecore_weather.core.transfer_budget import Budget
    from tempfile import TemporaryDirectory
    info,aligned,_=load_index(preparation)
    recipe=recipe_from_dict(info['recipe']);grid=target_grid(recipe)
    cache=Path(preparation)/'parallax'
    rows=[];selected=aligned[aligned.variable.isin(['abi01c','abi02c','abi03c','abi07c','abi13c'])]
    if selected.empty:raise ValueError('Parallax pilot requires a validated cloud band')
    with TemporaryDirectory(prefix='ecore-height-investigation-') as temporary:
        budget=Budget(Path(temporary)/'transfer.json',budget_bytes)
        with NativeReader(2) as reader,Transport('obstore',budget=budget) as transport:
            _run_height_samples(selected, cache, recipe, grid, reader, transport, rows, budget)
            result={'status':'complete' if all(r['status']=='verified' for r in rows) else 'blocked',
                'cases':rows,'returned_bytes':transport.bytes,'charged_bytes':budget.summary()['charged'],
                'budget_bytes':budget_bytes,
                'scope':'C01/C02/C03/C07/C13 selected-hour cloud-height pilot; actual scan times are recorded'}
    Path(output).mkdir(parents=True,exist_ok=True)
    atomic_json(Path(output)/'parallax-pilot.json',result)
    return result


def _run_height_samples(selected, cache, recipe, grid, reader, transport, rows, budget):
    from ecore_weather.core.transfer_budget import BudgetExceeded
    from ecore_weather.processing.process_parallax import archived_height, fetch_height, corrected_mapping
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
                evidence=json.loads(receipt.read_text()) if receipt.exists() else archived_height(record)
                if evidence is None:
                    state=budget.summary()
                    evidence=fetch_height(record,recipe.bbox,cache,transport,state['limit']-state['charged'])
                atomic_json(receipt,evidence)
                dataset=reader.read(record)
                mapping=corrected_mapping(dataset,record,grid,cache)
                transformed=regrid(decode_observation(dataset,quality=recipe.quality),mapping)
                rows.append({'variable':variable,'time':record['time'],'status':'verified',
                    'height':evidence,'valid_fraction':float(transformed['valid'].mean()),
                    'median_displacement_m':float(np.nanmedian(mapping['displacement_m'])),
                    'max_displacement_m':float(np.nanmax(mapping['displacement_m'])),
                    'seconds':time.perf_counter()-started})
                from ecore_weather.core.paths import repository_root
                _parallax_figure(dataset,record,grid,transformed,repository_root()/'figures/dataset-processing/parallax',mapping)
            except (ValueError,OSError,KeyError,BudgetExceeded) as exc:
                rows.append({'variable':variable,'time':record['time'],'status':'unavailable',
                             'error':f'{type(exc).__name__}: {exc}'})


def _parallax_stem(record,grid):
    from ecore_weather.core.common import utc
    when=utc(record['time'])
    band=f'C{int(record["variable"][3:5]):02d}'
    satellite=f'goes{int(record["satellite"])}'
    resolution_km=int(round(grid.attrs['resolution_m']/1000))
    return f'{satellite}-{band}-{when:%Y-%m-%dT%H%MZ}-{resolution_km}km'


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
    stem=_parallax_stem(record,grid)
    figure.savefig(output/f'{stem}-comparison.png',dpi=150)
    plt.close(figure)
    if mapping is not None:
        fig,ax=plt.subplots(figsize=(6,5),layout='constrained')
        image=ax.imshow(mapping['displacement_m'].reshape(grid.inside_bbox.shape)/1000,extent=extent,origin='upper',cmap='magma')
        fig.colorbar(image,ax=ax,label='Apparent-to-corrected displacement (km)')
        ax.set(title=f'{record["variable"]} · {record["time"]}',xlabel='Projected easting (km)',ylabel='Northing (km)')
        fig.savefig(output/f'{stem}-displacement.png',dpi=150);plt.close(fig)


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
