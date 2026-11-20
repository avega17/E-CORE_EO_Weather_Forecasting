"""Portable Zarr v3 storm observations and publication-aware NAM comparisons."""
from contextlib import ExitStack
from datetime import timedelta
import json
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

import numpy as np

from ecore_weather.core.common import iso, utc
from ecore_weather.processing.nam_inventory import CORE, matches
from ecore_weather.processing.storm_demo import CHANNELS, WIDTH, as_time, sha256
from ecore_weather.processing.storm_windows import STORMS, storm_hours
from ecore_weather.processing.transforms import atomic_json

MISSING = np.iinfo('int64').min
FIELDS = {
    'values': ('float32', np.nan), 'pixel_valid': ('uint8', 0),
    'quality': ('uint8', 255), 'height_known': ('uint8', 0),
    'parallax_applied': ('uint8', 0),
}
NAM_LABELS = {
    'surface_pressure': 'Surface pressure', 'sea_level_pressure': 'Sea-level pressure',
    'air_temperature_2m': '2 m air temperature', 'dew_point_2m': '2 m dew point',
    'wind_u_10m': '10 m eastward wind', 'wind_v_10m': '10 m northward wind',
    'precipitable_water': 'Precipitable water', 'precipitation_accumulation': 'Accumulated precipitation',
    'precipitation_rate': 'Precipitation rate', 'composite_reflectivity': 'Composite reflectivity',
}


def sample_times(hour):
    return as_time(hour) + np.arange(-50, 61, 10).astype('timedelta64[m]')


def cube_array(group, name, data, dims, **layout):
    return group.create_array(name, data=np.asarray(data), dimension_names=dims, **layout)


def nam_candidates(archive, hour, cutoff):
    from ecore_weather.processing.nwp import NAM
    valid = utc(hour)
    current = valid.replace(hour=(valid.hour // 6) * 6)
    while valid - current <= timedelta(hours=60):
        lead = (valid - current) // timedelta(hours=1)
        key = f'nam.{current:%Y%m%d}/nam.t{current:%H}z.priconest.hiresf{lead:02}.tm00.grib2'
        if current <= cutoff:
            listing = archive.cycle(current)
            obj, idx = listing.get(key), listing.get(key + '.idx')
            if obj and idx and utc(obj['LastModified']) <= cutoff:
                yield current, lead, obj, lambda obj=obj, idx=idx: archive.fields(NAM, obj, idx)
        current -= timedelta(hours=6)


def nam_pair(archive, hour, cache):
    """Keep both comparisons on the same eligible NAM cycle for each field."""
    from ecore_weather.processing.nwp import NAM

    first = utc(hour)
    second = first + timedelta(hours=1)
    next_hour = {row[0]: row for row in nam_candidates(archive, second, first)}
    pairs = {}
    unavailable = {}
    for cycle, lead, obj, index in nam_candidates(archive, first, first):
        if cycle not in next_hour:
            continue
        _, lead_next, obj_next, index_next = next_hour[cycle]
        records, later = index(), index_next()
        for name, spec in CORE.items():
            if name in pairs:
                continue
            selected = [[r for r in rows if matches(r, spec, offset)]
                        for rows, offset in ((records, lead), (later, lead_next))]
            if any(len(rows) != 1 for rows in selected):
                continue
            frames = []
            for valid, source, offset, (record,) in zip(
                    (first, second), (obj, obj_next), (lead, lead_next), selected):
                key = (name, source['Key'], source['ETag'])
                if key not in cache:
                    try:
                        image = archive.decode(NAM, source, record, expected=(cycle, valid))
                    except (OSError, ValueError) as exc:
                        unavailable[name] = str(exc)
                        break
                    image.update(lead_hours=offset, key=source['Key'], etag=source['ETag'],
                                 available_in_s3=iso(source['LastModified']))
                    cache[key] = image
                frames.append(cache[key])
            if len(frames) == 2:
                pairs[name] = frames
                unavailable.pop(name, None)
        if len(pairs) == len(CORE):
            break
    return pairs, unavailable


def open_baselines(paths, stack):
    import xarray as xr
    from ecore_weather.processing.process_store import reader_lease

    output = []
    for path in map(Path, paths):
        stack.enter_context(reader_lease(path))
        state = json.loads((path / 'progress.json').read_text())
        ds = stack.enter_context(xr.open_zarr(str(path / 'data.zarr'), consolidated=False,
                                             mask_and_scale=False, chunks=None))
        if ds.attrs['recipe']['resolution_km'] != 3 or ds.attrs['recipe']['parallax'] != 'none':
            raise ValueError('Demo baseline requires uncorrected 3 km stores')
        y0, x0 = ((ds.sizes[d] - WIDTH) // 2 for d in ('y', 'x'))
        if min(y0, x0) < 0:
            raise ValueError('The source is smaller than the demo crop')
        ds = ds.isel(time=slice(0, state['verified_slots']),
                     y=slice(y0, y0 + WIDTH), x=slice(x0, x0 + WIDTH))
        output.append((ds.sel(variable=list(CHANNELS)), state['recipe_id'],
                       {int(t): i for i,t in enumerate(ds.time.values.astype('datetime64[ns]').astype('int64'))}))
    return output


def corrected_records(location, first, last, recipe):
    from ecore_weather.processing.transforms import align_observations, observation_index

    obs, _ = observation_index(location, iso(first), iso(last), recipe)
    if obs.empty:
        return {}
    aligned = align_observations(obs, iso(first), iso(last), recipe.variables)
    return {(row['slot_time'], row['variable']): row for row in aligned.to_dict('records')}


def corrected_batch(times, records, recipe, grid, scratch, pool):
    rows = [[records.get((iso(t), name)) for name in CHANNELS] for t in times]
    if pool:
        from ecore_weather.processing.process_store import _worker_transform
        transformed = pool.map(_worker_transform, rows)
    else:
        from ecore_weather.processing.process_store import transform_records
        transformed = (transform_records(r, recipe, grid, scratch / 'geometry', scratch / 'parallax')
                       for r in rows)
    for records_at_time, (frame, _) in zip(rows, transformed):
        yield frame, records_at_time


def write_variant(group, indices, frames):
    for name, frame in frames.items():
        array = group[name]
        array[indices] = frame
        np.testing.assert_array_equal(array[indices], frame)


def export_cube(units, elevation, output, location, storms=None, shard_time=0, workers=4,
                include_nam=True, hours_filter=None, variants=('uncorrected', 'corrected')):
    """Export only requested storm times from verified observations."""
    import multiprocessing
    from concurrent.futures import ProcessPoolExecutor
    import zarr
    from zarr.codecs import BloscCodec
    from ecore_weather.core.transfer_budget import Budget
    from ecore_weather.processing.nwp import ForecastArchive
    from ecore_weather.processing.process_store import _initialize_worker
    from ecore_weather.processing.transforms import Recipe, target_grid

    if shard_time not in (0, 12, 24) or workers < 1:
        raise ValueError('Use shard time 0, 12 or 24 and at least one worker')
    output = Path(output)
    if (output / 'catalog.json').exists() or (output / 'storms.zarr').exists():
        raise FileExistsError('Choose a fresh demo export directory')
    output.mkdir(parents=True, exist_ok=True)
    root = zarr.open_group(str(output / 'storms.zarr'), mode='w', zarr_format=3)
    recipe = Recipe(resolution_km=3, radar=(), parallax='cloud-height')
    grid = target_grid(recipe)
    y0, x0 = ((grid.sizes[d] - WIDTH) // 2 for d in ('y', 'x'))
    catalog = {'schema': 2, 'store': 'storms.zarr', 'elevation': {}, 'storms': {}, 'variants': list(variants)}
    for path in (Path(elevation), Path(str(elevation) + '.json')):
        shutil.copyfile(path, output / path.name)
        catalog['elevation'][path.name] = sha256(path)
    windows = {slug: list(storm_hours(start, end)) for slug, (_, start, end) in STORMS.items()
               if not storms or slug in storms}
    if hours_filter:
        selected_hours = set(map(iso, hours_filter))
        windows = {slug: [h for h in hours if iso(h) in selected_hours]
                   for slug, hours in windows.items()}
        windows = {slug: hours for slug, hours in windows.items() if hours}
        if sum(map(len, windows.values())) != len(selected_hours):
            raise ValueError('Demo hours must occur inside selected storm windows')
    times = np.unique(np.concatenate([sample_times(hour) for hours in windows.values() for hour in hours]))
    time_lookup = {int(t): i for i, t in enumerate(times.astype('int64'))}
    codec = BloscCodec(cname='zstd', clevel=3)
    root.attrs.update(schema=2, grid=grid.attrs, units_by_band={name: ('1' if int(name[3:5]) <= 6 else 'K')
                                                              for name in CHANNELS})
    cube_array(root, 'time_ns', times.astype('int64'), ('time',))
    cube_array(root, 'variant', variants, ('variant',))
    cube_array(root, 'band', CHANNELS, ('band',))
    for name in ('y', 'x', 'latitude', 'longitude', 'inside_bbox'):
        cropped = grid[name].isel(y=slice(y0, y0 + WIDTH), x=slice(x0, x0 + WIDTH), missing_dims='ignore')
        cube_array(root, name, cropped.values, cropped.dims)
    shape = (len(variants), len(times), len(CHANNELS), WIDTH, WIDTH)
    layout = {'chunks': (1, 6, *shape[2:]), 'compressors': codec}
    if shard_time:
        layout['shards'] = (1, shard_time, *shape[2:])
    for name, (dtype, fill) in FIELDS.items():
        root.create_array(name, shape=shape, dtype=dtype, fill_value=fill,
                          dimension_names=('variant', 'time', 'band', 'y', 'x'), **layout)
    for name in ('source_time_ns', 'source_end_ns'):
        root.create_array(name, shape=shape[:3], dtype='int64', fill_value=MISSING,
                          chunks=(1, 6, shape[2]), dimension_names=('variant', 'time', 'band'))
    with ExitStack() as stack:
        work = Path(stack.enter_context(TemporaryDirectory(prefix='ecore-storm-export-', dir=output.parent.resolve())))
        pool = (stack.enter_context(ProcessPoolExecutor(
            max_workers=workers, mp_context=multiprocessing.get_context('spawn'),
            initializer=_initialize_worker, initargs=(recipe, grid, work/'geometry', work/'parallax')))
                if 'corrected' in variants and workers > 1 else None)
        baselines = open_baselines(units, stack)
        records = {}
        if 'corrected' in variants:
            for hours in windows.values():
                records.update(corrected_records(location, sample_times(hours[0])[0],
                                                 sample_times(hours[-1])[-1] + np.timedelta64(10, 'm'), recipe))
        provenance = []
        for first in range(0, len(times), shard_time or 6):
            subset = times[first:first + (shard_time or 6)]
            for variant, name in enumerate(variants):
                frames = {key: np.full((len(subset), *shape[2:]), fill, dtype)
                          for key, (dtype, fill) in FIELDS.items()}
                frames.update({key: np.full((len(subset), len(CHANNELS)), MISSING, 'int64')
                               for key in ('source_time_ns', 'source_end_ns')})
                if name == 'uncorrected':
                    for i, stamp in enumerate(subset):
                        for ds, recipe_id, positions in baselines:
                            position = positions.get(int(stamp.astype('int64')))
                            if position is None:
                                continue
                            frame = ds.isel(time=position)
                            for key in frames.keys() & frame.data_vars.keys():
                                frames[key][i] = frame[key].values
                            provenance.append({'time': iso(stamp), 'variant': name, 'recipe_id': recipe_id})
                            break
                else:
                    for i, (frame, rows) in enumerate(corrected_batch(subset, records, recipe, grid, work, pool)):
                        for key, source in (('values','values'), ('pixel_valid','valid'), ('quality','quality'),
                                            ('height_known','height_known'), ('parallax_applied','parallax_applied')):
                            frames[key][i] = frame[source][:, y0:y0+WIDTH, x0:x0+WIDTH]
                        for j, row in enumerate(rows):
                            if row:
                                for field, source in (('source_time_ns','time'), ('source_end_ns','end_time')):
                                    value = row.get(source) or row.get('time')
                                    frames[field][i,j] = int(as_time(value).astype('int64'))
                                provenance.append({'time': iso(subset[i]), 'variant': name,
                                    'band': CHANNELS[j], 'asset_id': row.get('asset_id'),
                                    'satellite': row.get('satellite')})
                write_variant(root, (variant, slice(first, first+len(subset))), frames)
            print(f'Wrote {first+len(subset)}/{len(times)} storm frames', flush=True)
        archive = ForecastArchive(Budget(work / 'nam-budget.json', 4*1024**3)) if include_nam else None
        add_catalog(root, catalog, windows, time_lookup, archive, codec)
    atomic_json(output / 'provenance.json', provenance)
    atomic_json(output / 'catalog.json', catalog)
    return {'status': 'complete', 'catalog': str(output / 'catalog.json'), 'frames': len(times)}


def add_catalog(root, catalog, windows, time_lookup, archive, codec):
    nam_cache = {}
    coordinates = None
    count = sum(map(len, windows.values()))
    ordinal = 0
    for slug, hours in windows.items():
        storm = {'name': STORMS[slug][0], 'hours': {}}
        catalog['storms'][slug] = storm
        for hour in hours:
            first = time_lookup[int(sample_times(hour)[0].astype('int64'))]
            entry = {'start': first, 'nam_index': ordinal, 'nam': {}}
            storm['hours'][iso(hour)] = entry
            pairs, unavailable = nam_pair(archive, hour, nam_cache) if archive else ({}, {})
            if unavailable:
                entry['nam_read_issues'] = unavailable
            for name, pair in pairs.items():
                group = root.require_group('nam')
                if coordinates is None:
                    coordinates = {k: pair[0][k] for k in ('latitude', 'longitude')}
                    for key, values in coordinates.items():
                        cube_array(group, key, values, ('y', 'x'), chunks=values.shape, compressors=codec)
                for frame in pair:
                    for key, values in coordinates.items():
                        np.testing.assert_array_equal(frame[key], values)
                if name not in group:
                    shape = pair[0]['values'].shape
                    group.create_array(name, shape=(count, 2, *shape), dtype='float32',
                                       chunks=(1, 2, *shape), fill_value=np.nan,
                                       dimension_names=('initialization', 'valid_offset', 'y', 'x'),
                                       compressors=codec)
                group[name][ordinal] = np.stack([frame['values'] for frame in pair])
                entry['nam'][name] = [{key: value for key, value in frame.items()
                                       if key not in ('values', 'latitude', 'longitude')} for frame in pair]
            ordinal += 1
            if len(nam_cache) > 64:
                nam_cache.clear()
        print(f'Cataloged {len(hours)} initialization hours for {slug}', flush=True)


def open_hub_cube(repo, catalog_path, store):
    """Populate the Hub cache on demand via exact Zarr object keys."""
    import asyncio
    import zarr
    from huggingface_hub import hf_hub_download
    from huggingface_hub.errors import RemoteEntryNotFoundError
    from zarr.storage import LocalStore

    revision = catalog_path.parent.name
    hf_hub_download(repo, f'{store}/zarr.json', repo_type='dataset', revision=revision, token=False)

    class HubStore(LocalStore):
        async def get(self, key, prototype=None, byte_range=None):
            if not (self.root / key).is_file():
                try:
                    await asyncio.to_thread(hf_hub_download, repo, f'{store}/{key}',
                                            repo_type='dataset', revision=revision, token=False)
                except RemoteEntryNotFoundError:
                    return None
            return await super().get(key, prototype, byte_range)

        async def get_partial_values(self, prototype, key_ranges):
            return await asyncio.gather(*(self.get(key, prototype, part) for key, part in key_ranges))

        async def exists(self, key):
            return await self.get(key) is not None

    return zarr.open_group(store=HubStore(catalog_path.parent / store, read_only=True), mode='r',
                           use_consolidated=False, zarr_format=3)


def read_window(root, entry, hour, corrected=False):
    variants = root['variant'][:].tolist()
    variant = variants.index('corrected' if corrected else 'uncorrected')
    first = entry['start']
    time = slice(first, first + 12)
    from concurrent.futures import ThreadPoolExecutor

    windows = ['values', 'pixel_valid', 'source_end_ns']
    if corrected:
        windows.append('parallax_applied')
    coordinates = ['latitude', 'longitude', 'inside_bbox', 'x', 'y']
    requests = [(name, (variant, time)) for name in windows]
    requests.extend((name, slice(None)) for name in coordinates)
    requests.extend((('time_ns', time), ('band', slice(None))))

    def read(request):
        name, selection = request
        return name, root[name][selection]

    with ThreadPoolExecutor(max_workers=8) as readers:
        result = dict(readers.map(read, requests))
    result['variables'] = result.pop('band')
    np.testing.assert_array_equal(result['time_ns'].astype('datetime64[ns]'), sample_times(hour))
    return result
