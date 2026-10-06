"""Bounded benchmarks; successful temporary stores are removed after evidence."""
from __future__ import annotations
from collections import OrderedDict
from contextlib import ExitStack
import hashlib
import json
from pathlib import Path
import tempfile
import time
import numpy as np
from .common import PeakMemory,write_json,PR_BBOX
from .dataset_report import provenance,selected_archives
from .storage import open_raw


def local_reads(location,output='results/dataset-report',repeats=3,**filters):
    if repeats<1:raise ValueError('Positive repeat count required')
    archives=selected_archives(location,**filters);rows=[]
    for archive in archives:
        marker=archive['marker'];bands=([filters['band']] if filters.get('band') else
            [int(n[1:]) for n in marker.get('band_counts',{})] if marker.get('band_counts') else [archive['band']])
        for band in bands:
            grouped=archive['product'] in ('ABI-L2-MCMIPF','ABI-L2-CMI-2KM-HYBRID')
            with open_raw(archive['path'],group=f'C{band:02d}' if grouped else None) as ds:
                names=['measurement','bitmap_valid'] if archive['source']=='mrms' else [n for n in (f'CMI_C{band:02d}',f'DQF_C{band:02d}') if n in ds]
                total=ds.sizes['time']
                methods=[('map',[0]),('six_frame',list(range(min(6,total)))),
                         ('twelve_frame',list(range(min(12,total)))),
                         ('daily_sequence',list(range(min(144,total)))),
                         ('sparse_days',np.linspace(0,total-1,min(48,total),dtype=int).tolist())]
                for repeat in range(repeats):
                    rotated=methods[repeat%len(methods):]+methods[:repeat%len(methods)]
                    for mode,indices in rotated:
                        start=time.perf_counter();logical=0
                        # One frame at a time keeps the daily benchmark bounded.
                        with PeakMemory() as memory:
                            for i in indices:
                                sample=ds[names].isel(time=i).load();logical+=sum(sample[n].nbytes for n in names);sample.close()
                            elapsed=time.perf_counter()-start
                        rows.append({'path':archive['path'],'product':archive['product'],'band':band,
                            'mode':mode,'repeat':repeat+1,'frames':len(indices),'wall_seconds':elapsed,
                            'logical_roi_bytes':logical,'peak_rss_bytes':memory.peak,
                            'cache_scope':'one application store open per archive; OS cache not flushed',
                            'measurement_scope':'read/decompress only; excludes calibration, projection and rendering'})
            if len(rows)>=90:break # initial representative archive cap; specify date/band for expansion
        if len(rows)>=90:break
    result={'status':'complete','rows':rows,'provenance':provenance()}
    write_json(Path(output)/'local-reads.json',result)
    return result


def cross_month_reads(location,output='results/dataset-report',repeats=3,**filters):
    """Read six/twelve frames across one matching archive boundary; no alignment."""
    archives=selected_archives(location,**filters);groups={}
    for archive in archives:
        identity=(archive['product'],archive.get('band'),str(Path(archive['path']).parents[2]))
        groups.setdefault(identity,[]).append(archive)
    rows=[]
    for group in groups.values():
        group.sort(key=lambda a:min(x['time'] for x in a['assets']))
        if len(group)<2:continue
        left,right=group[:2]
        if left['product'] in ('ABI-L2-MCMIPF','ABI-L2-CMI-2KM-HYBRID'):continue
        with ExitStack() as stack:
            a=stack.enter_context(open_raw(left['path']));b=stack.enter_context(open_raw(right['path']))
            for coord in a.coords:
                if coord in a.dims and coord!='time':
                    if coord not in b.coords or not np.array_equal(a[coord].values,b[coord].values):
                        raise ValueError('Cross-month benchmark requires identical native coordinates')
            names=[n for n in a.data_vars if n=='measurement' or n=='bitmap_valid' or n.startswith(('CMI_','DQF_'))]
            for repeat in range(repeats):
                for frames in ([6,12] if repeat%2==0 else [12,6]):
                    half=frames//2
                    if min(a.sizes['time'],b.sizes['time'])<half:continue
                    start=time.perf_counter()
                    with PeakMemory() as memory:
                        for ds,indices in [(a,range(a.sizes['time']-half,a.sizes['time'])),(b,range(half))]:
                            for i in indices:
                                frame=ds[names].isel(time=i).load();frame.close()
                        elapsed=time.perf_counter()-start
                    rows.append({'product':left['product'],'band':left.get('band'),'paths':[left['path'],right['path']],
                        'frames':frames,'repeat':repeat+1,'wall_seconds':elapsed,'peak_rss_bytes':memory.peak,
                        'scope':'read/decompression across two archives, excludes alignment/calibration/rendering; OS cache not flushed'})
        break
    if not rows:raise ValueError('Need two completed months on an identical native band/grid')
    result={'status':'complete','rows':rows,'provenance':provenance()}
    write_json(Path(output)/'cross-month-reads.json',result);return result


def backends(location,output='results/dataset-report',repeats=3,**filters):
    """Identical cached float32 tensor through NVIDIA's three IO configurations."""
    import torch
    from earth2studio.io import ZarrBackend,AsyncZarrBackend
    from zarr.codecs import BloscCodec,BloscShuffle
    import zarr
    archives=selected_archives(location,**filters)
    if not archives:raise ValueError('No completed archives match the benchmark')
    archive=archives[0];marker=archive['marker'];band=filters.get('band') or archive.get('band')
    grouped=archive['product'] in ('ABI-L2-MCMIPF','ABI-L2-CMI-2KM-HYBRID')
    if grouped and not band:band=int(next(iter(marker['band_counts']))[1:])
    with open_raw(archive['path'],group=f'C{band:02d}' if grouped else None) as ds:
        name='measurement' if archive['source']=='mrms' else f'CMI_C{band:02d}'
        array=ds[name].isel(time=slice(0,6)).load()
        # Controlled backend experiment, not a replacement raw preservation test.
        values=np.ascontiguousarray(array.values,dtype='float32')
        coords=OrderedDict((d,np.asarray(array.coords[d].values)) for d in array.dims)
    tensor=torch.from_numpy(values);rows=[]
    with tempfile.TemporaryDirectory(prefix='ecore-report-io-') as temp:
        for repeat in range(repeats):
            methods=['default','compressed','async_compressed'];methods=methods[repeat%3:]+methods[:repeat%3]
            for mode in methods:
                path=Path(temp)/f'{mode}-{repeat}'
                codec=BloscCodec(cname='zstd',clevel=3,shuffle=BloscShuffle.shuffle)
                cls=AsyncZarrBackend if mode=='async_compressed' else ZarrBackend
                kwargs={} if mode=='default' else {'zarr_codecs':codec}
                if mode=='async_compressed':
                    kwargs.update(parallel_coords=OrderedDict(time=coords['time']),blocking=False,
                        chunked_coords={'time':1},shard_coords={'time':6},max_inflight_shards=1)
                with PeakMemory() as memory:
                    start=time.perf_counter()
                    io=cls(str(path),**kwargs) if mode=='async_compressed' else cls(str(path),chunks={'time':1},**kwargs)
                    try:
                        io.add_array(coords,name)
                        io.write(tensor,coords,name)
                    finally:
                        close=getattr(io,'close',None)
                        if close:close()
                    reopened=zarr.open_group(str(path),mode='r')
                    np.testing.assert_array_equal(reopened[name][:],values)
                    elapsed=time.perf_counter()-start
                rows.append({'mode':mode,'repeat':repeat+1,'wall_seconds_including_verify':elapsed,
                    'logical_tensor_bytes':values.nbytes,'stored_bytes':sum(p.stat().st_size for p in path.rglob('*') if p.is_file()),
                    'peak_rss_bytes':memory.peak,'equal':True})
    result={'status':'complete','rows':rows,'provenance':provenance(),'scope':'identical cached float32 tensors; not raw GOES packing or source acquisition'}
    write_json(Path(output)/'backends.json',result);return result


def hf_restore(remote_root,output='results/dataset-report'):
    """Verify one member per MRMS product and smallest complete annual object."""
    from .view_backup import list_bundles,inspect_bundle,restore_month
    from .hf_storage import BucketWriter
    bundles=list_bundles(remote_root,'mrms');rows=[]
    eligible=[]
    with tempfile.TemporaryDirectory(prefix='ecore-hf-report-') as temp:
        for product in sorted({b['product'] for b in bundles}):
            choices=sorted([b for b in bundles if b['product']==product],key=lambda b:b['stored_bytes'])
            if not choices:continue
            bundle=choices[0];members=inspect_bundle(bundle)
            if not members:continue
            member=members[0];start=time.perf_counter()
            metrics={}
            path=restore_month(bundle,member,temp,metrics=metrics)
            restore_seconds=time.perf_counter()-start
            start=time.perf_counter()
            with open_raw(path) as ds:
                opened=time.perf_counter()-start
                start=time.perf_counter();ds.measurement.isel(time=slice(0,6)).load()
                first_window=time.perf_counter()-start
            start=time.perf_counter();restore_month(bundle,member,temp)
            rows.append({'product':product,'kind':'monthly_member','stored_bytes':Path(path).stat().st_size,
                'restore_transfer_hash_extract_seconds':restore_seconds,**metrics,'open_seconds':opened,
                'first_window_seconds':first_window,'cache_reuse_seconds':time.perf_counter()-start,
                'phase_scope':'restore utility streams hash and extraction together; not independently timed'})
        for candidate in sorted([b for b in bundles if b.get('archive_count',0)>=12],key=lambda b:b['stored_bytes']):
            members=inspect_bundle(candidate)
            if len({m['month'] for m in members})==12:
                eligible.append(candidate);break
        if not eligible:raise ValueError('No verified complete twelve-month HF bundle')
        smallest=eligible[0];writer=BucketWriter(remote_root)
        target=Path(temp)/'annual.zip';start=time.perf_counter()
        writer.client.download_file(smallest['bucket'],smallest['key'],str(target))
        transfer=time.perf_counter()-start;start=time.perf_counter()
        from .dataset_report import file_hash
        if file_hash(target)!=smallest['archive_sha256']:raise IOError('HF annual SHA-256 mismatch')
        rows.append({'product':smallest['product'],'year':smallest['year'],'kind':'annual_bundle',
            'stored_bytes':target.stat().st_size,'transfer_seconds':transfer,'hash_seconds':time.perf_counter()-start,
            'compression':'ZIP_STORED container of compressed monthly ZIPs'})
    result={'status':'complete','rows':rows,'provenance':provenance(),'successful_temporary_stores_removed':True}
    write_json(Path(output)/'hf-restore.json',result);return result


def roi_options():
    w, s, e, n = PR_BBOX
    cx, cy, hx, hy = (w+e)/2, (s+n)/2, (e-w)/2, (n-s)/2
    options = {label: [cx-factor*hx, cy-factor*hy, cx+factor*hx, cy+factor*hy]
               for label, factor in [('current', 1), ('linear_125', 1.25),
                                     ('linear_150', 1.5), ('linear_200', 2)]}
    options['current'] = list(PR_BBOX)
    options['east_to_60w'] = [w, s, -60., n]
    return options


def _mrms_read_group(assets, bbox):
    """One independent product-month reader: eight downloads, one GRIB decoder."""
    from concurrent.futures import ThreadPoolExecutor
    from .common import Transport
    from . import mrms
    product = 'MergedReflectivityQCComposite_00.50'
    started = time.perf_counter()
    with Transport('obstore', decode_workers=1) as transport:
        def read(asset):
            ds, phases = mrms.read(asset, bbox, product, transport)
            return {'time': asset.time, 'values': ds.measurement.values,
                    'bitmap': ds.bitmap_valid.values,
                    'latitude': ds.latitude.values, 'longitude': ds.longitude.values,
                    'phases': phases}
        with ThreadPoolExecutor(max_workers=8) as pool:
            frames = list(pool.map(read, assets))
        counters = {'returned_bytes': transport.bytes, 'get_requests': transport.requests,
                    'retries': transport.retries, 'failed_requests': transport.failed_requests}
    return frames, {**counters, 'wall_seconds': time.perf_counter()-started,
                   'summed_download_seconds': sum(f['phases']['download_s'] for f in frames),
                   'summed_decode_crop_seconds': sum(f['phases']['decode_crop_s'] for f in frames)}


def _mrms_compare_frames(reference, frames, assets, bbox):
    """Compare valid decoded pixels; stock NaNs are not our preserved bitmap codes."""
    from .common import utc
    w, s, e, n = bbox
    lat = reference.lat.values
    lon = reference.lon.values
    normalized = (lon+180) % 360-180
    yi = np.flatnonzero((lat >= s) & (lat < n))
    xi = np.flatnonzero((normalized >= w) & (normalized < e))
    if not len(yi) or not len(xi):
        raise AssertionError('Stock reference does not cover the selected ROI')
    reference = reference.isel(lat=yi, lon=xi).sel(variable='refc')
    by_time = {f['time']: f for f in frames}
    differences = []
    for index, asset in enumerate(assets):
        frame = by_time[asset.time]
        expected_time = np.datetime64(utc(asset.time).replace(tzinfo=None), 's')
        if reference.actual_time_refc.values[index] != expected_time:
            raise AssertionError('Stock source substituted another observation')
        values = reference.isel(time=index).values
        valid = frame['bitmap'].astype(bool)
        if values.shape != frame['values'].shape:
            raise AssertionError('Stock and project crops select different pixel windows')
        np.testing.assert_array_equal(values[valid], frame['values'][valid])
        for name, original in [('latitude', lat[yi]), ('longitude', lon[xi])]:
            delta = float(np.max(np.abs(frame[name]-original)))
            # Stock constructs axes from headers; ecCodes distinct axes can
            # differ at GRIB microdegree precision. This comparison allowance
            # does not change the exact source-to-archive coordinate checks.
            if delta > 2e-6:
                raise AssertionError(f'Stock/project {name} differs by {delta} degrees')
            differences.append(delta)
    return {'valid_pixels_equal': True, 'actual_times_equal': True,
            'maximum_coordinate_difference_degrees': max(differences, default=0),
            'coordinate_comparison_tolerance_degrees': 2e-6,
            'coordinate_scope': 'Header-built stock axes versus preserved ecCodes axes; numeric difference reported, not exact coordinate equality.',
            'bitmap_scope': 'Project bitmap retained; stock missing pixels may become NaN. No quality-metadata equivalence claim.'}


def mrms_source_main(argv=None):
    """Brief source comparison, guarded against competing study downloads.

    Stock's default CONUS call is a workload reference. Matched CARIB comparisons
    override only MRMS_REGION in a subclass; all fetching methods stay inherited.
    This times source reading/cropping, not monthly Zarr finalization.
    """
    import argparse
    from concurrent.futures import ProcessPoolExecutor
    import importlib
    import multiprocessing
    import os
    from statistics import median
    from urllib.parse import urlparse
    from .common import Asset, PR_BBOX, utc, write_json
    from .jobs_policy import ensure_paused
    from .transfer_budget import Budget
    p = argparse.ArgumentParser(description=mrms_source_main.__doc__)
    p.add_argument('--location', default='/mnt/p/ecore_eo_datasets')
    p.add_argument('--output', default='results/study-mrms/source-benchmark.json')
    p.add_argument('--months', nargs=4, default=['2022-09', '2024-09', '2025-09', '2026-06'])
    p.add_argument('--frames-per-month', type=int, default=8)
    p.add_argument('--repeats', type=int, default=3)
    p.add_argument('--budget-mib', type=int, default=128)
    args = p.parse_args(argv)
    if not 1 <= args.frames_per_month <= 8 or args.repeats != 3 or not 1 <= args.budget_mib <= 256:
        p.error('Use 1–8 frames/month, three repeats, and a 1–256 MiB transfer budget')
    # Check before creating a result directory or any network client.
    ensure_paused()
    module = importlib.import_module('earth2studio.data.mrms')
    product = 'MergedReflectivityQCComposite_00.50'
    groups, manifests = [], []
    for month in args.months:
        year, mm = month.split('-')
        matches = sorted((Path(args.location)/'mrms'/product).glob(f'roi-*/{year}/{mm}/complete.json'))
        marker_path = next((path for path in matches
                            if json.loads(path.read_text()).get('region') == list(PR_BBOX)), None)
        if marker_path is None:
            raise FileNotFoundError(f'No completed current-ROI {product} archive for {month}')
        marker = json.loads(marker_path.read_text())
        rows = marker['assets']
        indices = np.linspace(0, len(rows)-1, args.frames_per_month, dtype=int)
        assets = []
        for index in indices:
            row = rows[index]
            key = urlparse(row['source_url']).path.lstrip('/')
            assets.append(Asset('noaa-mrms-pds', key, int(row['source_bytes']), row['etag'], row['time']))
        groups.append(assets)
        manifests.append({'path': str(marker_path), 'sha256': file_digest(marker_path)})
    assets = [asset for group in groups for asset in group]
    rows = []
    # Temporary budget/cache records are not additional operational logging.
    with tempfile.TemporaryDirectory(prefix='ecore-mrms-source-') as temp:
        budget = Budget(Path(temp)/'budget.json', args.budget_mib*1024**2)
        previous_budget = os.environ.get('ECORE_TRANSFER_BUDGET')
        previous_cache = os.environ.get('EARTH2STUDIO_DATA_CACHE')
        os.environ['ECORE_TRANSFER_BUDGET'] = str(budget.path)
        original = module.obstore_read_range
        async def counted(store, key, *a, **kw):
            size = (await store.head_async(key))['size']
            reservation = budget.reserve(size)
            try:
                data = await original(store, key, *a, **kw)
            except BaseException:
                budget.settle(reservation)
                raise
            budget.settle(reservation, len(data))
            return data
        module.obstore_read_range = counted
        class CaribbeanReference(module.MRMS):
            MRMS_REGION = 'CARIB'
        try:
            # Unmodified default domain; only verbose output is disabled.
            os.environ['EARTH2STUDIO_DATA_CACHE'] = str(Path(temp)/'stock-conus')
            stock = module.MRMS(verbose=False)
            before = budget.summary()['observed']
            with PeakMemory() as memory:
                started = time.perf_counter()
                reference = stock([utc('2024-09-18T12:00:00').replace(tzinfo=None)], ['refc'])
                fetch_s = time.perf_counter()-started
                started = time.perf_counter()
                crop = reference.sel(lat=slice(40.68, 33), lon=slice(258.99, 266.67))
                if not crop.sizes['lat'] or not crop.sizes['lon']:
                    # Handle sources whose longitudes use -180..180.
                    crop = reference.sel(lat=slice(40.68, 33), lon=slice(-101.01, -93.33))
                crop.load()
                crop_s = time.perf_counter()-started
            rows.append({'method': 'stock_default_CONUS', 'observations': 1,
                         'wall_seconds': fetch_s+crop_s, 'crop_seconds': crop_s,
                         'roi_shape': [crop.sizes['lat'], crop.sizes['lon']],
                         'returned_bytes': budget.summary()['observed']-before,
                         'peak_rss_bytes': memory.peak,
                         'scope': 'Different geographic domain; full CONUS decode followed by similarly sized crop. Not a matched speed comparison.'})
            reference.close()
            del reference, crop
            for repeat in range(3):
                reference = None
                references = None
                for method in (['stock_CARIB', 'project_CARIB'] if repeat % 2 == 0
                               else ['project_CARIB', 'stock_CARIB']):
                    before = budget.summary()['observed']
                    with PeakMemory() as memory:
                        started = time.perf_counter()
                        if method == 'stock_CARIB':
                            os.environ['EARTH2STUDIO_DATA_CACHE'] = str(Path(temp)/f'stock-{repeat}')
                            stock = CaribbeanReference(max_offset_minutes=0, verbose=False)
                            # Both methods use the same preselected exact objects;
                            # inventory cost is excluded, not charged to stock alone.
                            stock._list_cache = {}
                            for asset in assets:
                                prefix = asset.key.rsplit('/', 1)[0]+'/'
                                stock._list_cache.setdefault(prefix, []).append(asset.key)
                            reference = stock([utc(a.time).replace(tzinfo=None) for a in assets], ['refc'])
                            crop = reference.isel(lat=np.flatnonzero((reference.lat >= PR_BBOX[1]) & (reference.lat < PR_BBOX[3])),
                                lon=np.flatnonzero((((reference.lon+180) % 360-180) >= PR_BBOX[0]) &
                                                   (((reference.lon+180) % 360-180) < PR_BBOX[2]))).load()
                        else:
                            with ProcessPoolExecutor(max_workers=4, mp_context=multiprocessing.get_context('spawn')) as pool:
                                results = list(pool.map(_mrms_read_group, groups, [PR_BBOX]*4))
                            references = [frame for frames, _ in results for frame in frames]
                        wall = time.perf_counter()-started
                    row = {'method': method, 'repeat': repeat+1, 'observations': len(assets),
                           'wall_seconds': wall, 'returned_bytes': budget.summary()['observed']-before,
                           'peak_rss_bytes': memory.peak}
                    if method == 'project_CARIB':
                        row['reader_groups'] = [metrics for _, metrics in results]
                    rows.append(row)
                # Verification is identical for both methods and outside timing.
                equality = _mrms_compare_frames(reference, references, assets, PR_BBOX)
                reference.close()
                del reference, references, results, crop
            result = {'status': 'complete', 'rows': rows, 'equality': equality,
                      'selection_manifests': manifests, 'budget': budget.summary(),
                      'project_config': {'product_month_readers': 4, 'downloads_per_reader': 8, 'decoders_per_reader': 1},
                      'stock_config': {'max_workers_parameter': 24, 'matched_timestamp_tasks': len(assets),
                                       'domain_override': 'CARIB for matched rows only'},
                      'scope': 'Composite-reflectivity source read/decode/crop; excludes archive writes, packing and DAS verification; fresh application caches, OS cache not flushed.',
                      'provenance': provenance(),
                      'benchmark_source_sha256': file_digest(Path(__file__)),
                      'installed_stock_source_sha256': file_digest(Path(module.__file__)),
                      'median_seconds': {method: median(r['wall_seconds'] for r in rows if r['method'] == method)
                                         for method in ('stock_CARIB', 'project_CARIB')}}
            write_json(args.output, result)
            print(json.dumps({k: result[k] for k in ('status', 'median_seconds', 'equality', 'budget')}, indent=2))
        except Exception as exc:
            write_json(args.output, {'status': 'failed', 'rows': rows,
                'error': f'{type(exc).__name__}: {exc}', 'budget': budget.summary(),
                'selection_manifests': manifests, 'provenance': provenance()})
            raise
        finally:
            module.obstore_read_range = original
            for name, value in [('ECORE_TRANSFER_BUDGET', previous_budget), ('EARTH2STUDIO_DATA_CACHE', previous_cache)]:
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
    return 0


def file_digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()
