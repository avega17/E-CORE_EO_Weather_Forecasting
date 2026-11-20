"""Export verified regional storm blocks and run rolling Flash comparisons."""
from __future__ import annotations

from datetime import timedelta
import hashlib
import json
from pathlib import Path
import shutil

import numpy as np

from ecore_weather.core.common import iso, utc
from ecore_weather.processing.storm_windows import STORMS, flash_windows, storm_hours
from ecore_weather.processing.transforms import BANDS, atomic_json

CHANNELS = tuple(f'abi{b:02d}c' for b in BANDS)
WIDTH = 256


def sha256(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream,'sha256').hexdigest()


def export_demo(units, elevation, output, audit_dir, storms=None):
    """Read committed prefixes and write portable two-hour blocks only."""
    from contextlib import ExitStack
    import xarray as xr
    from ecore_weather.core.transfer_budget import Budget, BudgetExceeded
    from ecore_weather.processing.nwp import ForecastArchive
    from ecore_weather.processing.process_store import reader_lease
    from tempfile import TemporaryDirectory

    root=Path(output)
    root.mkdir(parents=True,exist_ok=True)
    catalog_path=root/'catalog.json'
    prior=json.loads(catalog_path.read_text()) if catalog_path.is_file() else {}
    stem='caribbean-elevation.tif'
    elevation=Path(elevation)
    for src,name in [(elevation,stem),(elevation.with_suffix(elevation.suffix+'.json'),stem+'.json')]:
        shutil.copyfile(src,root/name)
    catalog={'schema':1, 'kind':'Caribbean Flash engineering comparison', 'storms':{},
             'elevation':{name:sha256(root/name) for name in (stem,stem+'.json')}}
    audit=Path(audit_dir)/'nam-availability.json'
    evidence=json.loads(audit.read_text()).get('storms',{}) if audit.is_file() else {}
    with ExitStack() as stack:
        tmp=stack.enter_context(TemporaryDirectory(prefix='ecore-demo-nam-'))
        archive=ForecastArchive(Budget(Path(tmp)/'transfer.json',4*1024**3))
        sources=[]
        for unit in map(Path,units):
            stack.enter_context(reader_lease(unit))
            state=json.loads((unit/'progress.json').read_text())
            ds=stack.enter_context(xr.open_zarr(str(unit/'data.zarr'),consolidated=False,
                                                mask_and_scale=False,chunks=None))
            recipe=ds.attrs['recipe']
            if recipe['resolution_km']!=3 or recipe['parallax']!='none':
                raise ValueError('Demo variants must use the uncorrected 3 km recipe')
            y0=(ds.sizes['y']-WIDTH)//2
            x0=(ds.sizes['x']-WIDTH)//2
            if min(x0,y0)<0:
                raise ValueError('Derived grid is smaller than the model crop')
            cropped=ds.isel(time=slice(0,state['verified_slots']),
                            y=slice(y0,y0+WIDTH),x=slice(x0,x0+WIDTH))
            sources.append((cropped,state))
        if not sources:
            raise ValueError('Select at least one verified 3 km store')
        for slug,(name,start,end) in STORMS.items():
            if storms and slug not in storms:
                continue
            hours=list(storm_hours(start,end))
            summary={'name':name,'start':start,'end_excluded':end,'blocks':[], 'unavailable_hours':[]}
            catalog['storms'][slug]=summary
            existing={tuple(block['hours']):block for block in prior.get('storms',{}).get(slug,{}).get('blocks',[])}
            for offset in range(0,len(hours),2):
                pair=hours[offset:offset+2]
                first=pair[0]-timedelta(minutes=70)
                last=pair[-1]+timedelta(minutes=10)
                low,high=as_time(first),as_time(last)
                chosen=next(((ds,state) for ds,state in sources if ds.sizes['time']
                             and ds.time.values[0]<=low and high<=ds.time.values[-1]),None)
                if chosen is None:
                    summary['unavailable_hours'].extend(iso(t) for t in pair)
                    continue
                ds,state=chosen
                previous=existing.get(tuple(iso(t) for t in pair))
                audit_rows={r['valid_time']:r for r in evidence.get(slug,[])}
                expected=[audit_rows.get(iso(t)) for t in pair]
                nam_current=previous and all(
                    ref is None or (ref['status']==stored['status'] and ref.get('key')==stored.get('key'))
                    for ref,stored in zip(expected,previous['nam']))
                if nam_current and previous['recipe_id']==state['recipe_id']:
                    old=root/previous['file']
                    if old.is_file() and sha256(old)==previous['sha256']:
                        summary['blocks'].append(previous)
                        continue
                requested=np.arange(low,high+np.timedelta64(10,'m'),np.timedelta64(10,'m'))
                subset=ds.sel(time=requested,variable=list(CHANNELS))
                if not np.array_equal(subset.time.values,requested):
                    raise ValueError('Derived store does not provide exact ten-minute slots')
                values=subset['values'].values
                valid=subset.pixel_valid.values.astype(bool)&np.isfinite(values)
                starts=subset.source_time_ns.values
                ends=subset.source_end_ns.values
                if np.any(ends>requested.astype('datetime64[ns]').astype('int64')[:,None]):
                    raise ValueError('A source scan ends after its assigned slot')
                payload={'values':values,'pixel_valid':valid,'source_start_ns':starts,'source_end_ns':ends,
                         'time_ns':requested.astype('datetime64[ns]').astype('int64'),
                         'variables':np.asarray(CHANNELS)}
                payload.update({axis:subset[axis].values for axis in ('latitude','longitude','inside_bbox','x','y')})
                nam=[]
                for i,t in enumerate(pair):
                    audited=audit_rows.get(iso(t))
                    if audited and audited['status']=='unavailable' and all(
                        not reason.startswith('read-failure') for reason in audited.get('skipped',{})):
                        chosen_nam=audited.copy()
                    else:
                        try:
                            chosen_nam=archive.select_nam(t,read=True)
                        except BudgetExceeded as exc:
                            chosen_nam={'status':'unavailable','valid_time':iso(t),'reason':str(exc)}
                    sample=chosen_nam.pop('sample',None)
                    if sample:
                        for key in ('values','latitude','longitude'):
                            payload[f'nam{i}_{key}']=sample[key]
                        chosen_nam.update(units=sample['units'],step_range=sample['step_range'])
                    nam.append(chosen_nam)
                key=f'{slug}/{pair[0]:%Y%m%dT%H%MZ}.npz'
                path=root/key
                path.parent.mkdir(parents=True,exist_ok=True)
                tmp_path=path.with_suffix('.tmp.npz')
                np.savez_compressed(tmp_path,**payload)
                tmp_path.replace(path)
                summary['blocks'].append({'file':key,'sha256':sha256(path),'bytes':path.stat().st_size,
                    'hours':[iso(t) for t in pair], 'nam':nam, 'recipe_id':state['recipe_id']})
                atomic_json(root/'catalog.json',catalog)
            print(f"{name}: {len(summary['blocks'])} blocks, {len(summary['unavailable_hours'])} hours awaiting verified data",flush=True)
    atomic_json(root/'catalog.json',catalog)
    return {'status':'complete' if all(not s['unavailable_hours'] for s in catalog['storms'].values()) else 'partial',
            'catalog':str(root/'catalog.json'),'storms':{slug:len(s['blocks']) for slug,s in catalog['storms'].items()}}


def as_time(stamp):
    return np.datetime64(utc(stamp).replace(tzinfo=None),'ns')


def read_verified(path,digest):
    if sha256(path)!=digest:
        raise IOError(f'Checksum differs for {Path(path).name}')
    with np.load(path,allow_pickle=False) as store:
        return {key:store[key] for key in store.files}


def comparison_inputs(arrays,hour):
    times=arrays['time_ns'].astype('datetime64[ns]')
    np.testing.assert_array_equal(arrays['variables'],CHANNELS)
    for target,history in flash_windows(hour):
        indices=[int(np.flatnonzero(times==as_time(t))[0]) for t in history]
        target_index=int(np.flatnonzero(times==as_time(target))[0])
        actual=arrays['source_end_ns'][indices]
        causal=(actual!=np.iinfo('int64').min)&(actual<=times[indices].astype('int64')[:,None])
        values=arrays['values'][indices]
        valid=arrays['pixel_valid'][indices].astype(bool)&np.isfinite(values)
        support=valid.all(axis=(0,1))&arrays['inside_bbox'].astype(bool)
        if not causal.all():
            support[:]=False
        yield target,history,values,support,target_index


def load_flash():
    from earth2studio.models.px import StormScopeGOES
    import torch
    if not torch.cuda.is_available():
        raise RuntimeError('Choose a GPU runtime and install the Flash model environment')
    package=StormScopeGOES.load_default_package()
    return StormScopeGOES.load_model(package,model_name='3km_10min_flash',
                                      compile=False,amp=True).eval().to('cuda')


def forecast_rows(model, arrays, hour, elevation):
    """Advance one regional Flash iterator from a six-frame history."""
    import torch
    from collections import OrderedDict
    from ecore_weather.processing.process_model import caribbean_adapter, regional_topography

    initial = as_time(hour)
    times = arrays['time_ns'].astype('datetime64[ns]')
    expected = initial + np.arange(-50, 61, 10).astype('timedelta64[m]')
    np.testing.assert_array_equal(times, expected)
    np.testing.assert_array_equal(arrays['variables'], CHANNELS)
    ends = arrays['source_end_ns'][:6]
    if np.any(ends == np.iinfo('int64').min) or np.any(ends > times[:6].astype('int64')[:, None]):
        raise ValueError('The six-frame history needs causal source scans for every GOES band')
    values = arrays['values'][:6]
    support = (arrays['pixel_valid'][:6].astype(bool) & np.isfinite(values)).all(axis=(0, 1))
    support &= arrays['inside_bbox'].astype(bool)
    if not support.any():
        raise ValueError('No pixels have valid observations in all six frames and eight bands')
    topo = regional_topography(elevation, arrays['latitude'], arrays['longitude'], allow_approximate=True)
    regional = caribbean_adapter(model, arrays['latitude'], arrays['longitude'],
                                arrays['y'], arrays['x'], topo, support)
    torch.manual_seed(0)
    torch.cuda.manual_seed_all(0)
    coords = OrderedDict(batch=np.asarray([0]), time=np.asarray([initial]),
                         lead_time=np.arange(-50, 1, 10).astype('timedelta64[m]'),
                         variable=np.asarray(CHANNELS), y=arrays['y'], x=arrays['x'])
    inputs = torch.as_tensor(values[None, None], device='cuda', dtype=torch.float32)
    with torch.inference_mode():
        iterator = regional.create_iterator(inputs, coords)
        next(iterator)  # Earth2Studio 0.20 yields the initial state first.
        for index in range(6, 12):
            predicted, out_coords = next(iterator)
            valid = out_coords['time'][-1] + out_coords['lead_time'][-1]
            if valid != expected[index]:
                raise ValueError(f'Flash returned unexpected valid time {valid}')
            forecast = predicted.detach().cpu().numpy()[0, 0, 0]
            if not np.isfinite(forecast[:, support]).all():
                raise ValueError('Flash produced nonfinite predictions on supported pixels')
            truth = np.where(arrays['pixel_valid'][index], arrays['values'][index], np.nan)
            yield {'valid_time': iso(valid), 'initialized': iso(initial),
                   'truth': truth, 'forecast': np.where(support, forecast, np.nan)}


def serve_forecasts():
    """Keep the GPU checkpoint loaded while the notebook requests comparisons."""
    from contextlib import redirect_stdout
    import sys

    model = None
    for line in sys.stdin:
        try:
            request = json.loads(line)
            with redirect_stdout(sys.stderr):
                with np.load(request['input'], allow_pickle=False) as payload:
                    arrays = dict(payload)
                if model is None:
                    model = load_flash()
                rows = list(forecast_rows(model, arrays, request['hour'], request['elevation']))
                np.savez(request['output'], forecasts=np.stack([r['forecast'] for r in rows]))
            reply = {'status': 'complete', 'times': [r['valid_time'] for r in rows]}
        except Exception as exc:
            reply = {'status': 'error', 'reason': f'{type(exc).__name__}: {exc}'}
        print(json.dumps(reply), flush=True)


if __name__ == '__main__':
    serve_forecasts()
