"""Scientific readers, grids and causal windows for derived NOAA observations.

This module never changes a native archive or imports model dependencies.
"""
from __future__ import annotations

from collections import OrderedDict
from dataclasses import asdict, dataclass
from datetime import timedelta
import hashlib
import json
import math
import os
import uuid
from pathlib import Path

import numpy as np
import pandas as pd
from pyproj import CRS, Transformer
import xarray as xr

from .common import PR_BBOX, iso, utc

BANDS = (1, 2, 3, 7, 8, 9, 10, 13)
RADAR_PRODUCTS = {
    'refc': 'MergedReflectivityQCComposite_00.50',
    'refc_base': 'MergedBaseReflectivityQC_00.50',
}


def scientific_id(value):
    """Identify a scientific recipe/input mapping, never source code or Git."""
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()[:24]


def atomic_json(path, value):
    def clean(item):
        if isinstance(item, dict): return {str(k): clean(v) for k,v in item.items()}
        if isinstance(item, (list, tuple, np.ndarray)): return [clean(v) for v in item]
        if isinstance(item, (float, np.floating)) and not np.isfinite(item): return None
        if isinstance(item, np.generic): return item.item()
        return item
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f'.{os.getpid()}.tmp')
    try:
        with temporary.open('w') as stream:
            json.dump(clean(value), stream, indent=2, default=str, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


@dataclass(frozen=True)
class Recipe:
    resolution_km: int = 1
    bbox: tuple = PR_BBOX
    quality: str = 'good'
    parallax: str = 'none'
    bands: tuple = BANDS
    radar: tuple = ('refc', 'refc_base')
    compression: str = 'zstd'
    chunk_time: int = 1
    schema: int = 1

    def __post_init__(self):
        if self.resolution_km not in (1, 2, 3):
            raise ValueError('Resolution must be 1, 2 or 3 km')
        if self.quality not in ('good', 'conditional'):
            raise ValueError('Quality must be good or conditional')
        if self.parallax not in ('none', 'cloud-height'):
            raise ValueError('Parallax must be none or cloud-height')
        if self.compression not in ('zstd', 'none') or self.chunk_time not in (1, 6, 12):
            raise ValueError('Use zstd/none compression and 1/6/12 time chunks')
        if not set(self.bands).issubset(BANDS) or not (self.bands or self.radar):
            raise ValueError('Select study GOES bands and/or reflectivity variables')
        if not set(self.radar).issubset(RADAR_PRODUCTS):
            raise ValueError('Only composite/base reflectivity are in this processing sprint')
        west, south, east, north = self.bbox
        if not (-180 <= west < east <= 180 and -90 < south < north < 90):
            raise ValueError('Invalid geographic bounding box')

    @property
    def variables(self):
        return tuple(f'abi{b:02d}c' for b in self.bands) + tuple(self.radar)

    @property
    def identity(self):
        return scientific_id(asdict(self))


def target_grid(recipe):
    """Common enclosing LCC grid, with unsupported rectangle corners masked."""
    crs = CRS.from_proj4('+proj=lcc +lat_1=15 +lat_2=21 +lat_0=18.2 '
                        '+lon_0=-66.4 +datum=WGS84 +units=m +no_defs')
    transform = Transformer.from_crs('EPSG:4326', crs, always_xy=True)
    bounds = transform.transform_bounds(*recipe.bbox, densify_pts=1000)
    left, bottom = (math.floor(v / 6000) * 6000 for v in bounds[:2])
    right, top = (math.ceil(v / 6000) * 6000 for v in bounds[2:])
    spacing = recipe.resolution_km * 1000
    x = np.arange(left + spacing / 2, right, spacing, dtype='float64')
    y = np.arange(top - spacing / 2, bottom, -spacing, dtype='float64')
    lon, lat = Transformer.from_crs(crs, 'EPSG:4326', always_xy=True).transform(*np.meshgrid(x, y))
    west, south, east, north = recipe.bbox
    inside = (lon >= west) & (lon <= east) & (lat >= south) & (lat <= north)
    return xr.Dataset({'latitude': (('y', 'x'), lat), 'longitude': (('y', 'x'), lon),
                       'inside_bbox': (('y', 'x'), inside)}, coords={'x': x, 'y': y},
                      attrs={'crs_wkt': crs.to_wkt(), 'resolution_m': spacing,
                             'bbox': list(recipe.bbox), 'bounds_m': [left, bottom, right, top]})


def scan_dataset(dataset, index):
    """Restore the selected observation's own calibration and scalar geometry."""
    selected = dataset.isel(time=index, drop=False) if 'time' in dataset.dims else dataset
    selected = selected.copy(deep=False)
    if 'source_metadata_json' in selected.coords:
        value = np.asarray(selected.source_metadata_json.values)
        if value.size != 1:
            raise ValueError('Select one observation before decoding')
        metadata = json.loads(str(value.item()))
        selected.attrs = {**selected.attrs, **metadata.get('dataset', {})}
        for name, info in metadata.get('variables', {}).items():
            targets = [name] if name in selected else [v for v in selected if
                name in ('CMI', 'DQF') and v.startswith(name + '_C')]
            for target in targets:
                selected[target].attrs = info.get('attrs', {}).copy()
            if info.get('dims') == [] and info.get('values') is not None:
                selected[name] = xr.DataArray(np.asarray(info['values'],dtype=info.get('dtype')),
                    attrs=info.get('attrs', {}))
    return selected


def _unsigned(values, attrs):
    values = np.asarray(values)
    return values.view(np.dtype(values.dtype.str.replace('i', 'u'))) if (
        str(attrs.get('_Unsigned', '')).lower() == 'true' and values.dtype.kind == 'i') else values


def decode_observation(dataset, variable=None, quality='good'):
    """Return physical float32 values, validity, original quality, and missing classes.

    Classes: 0 good; 1 fill; 2 no coverage; 3 bitmap; 4 nonfinite;
    5 bad quality; 6 range; 8 quality unknown; 9 conditional; 10 calibration.
    """
    variable = variable or ('measurement' if 'measurement' in dataset else
                            next(n for n in dataset if n == 'CMI' or n.startswith('CMI_C')))
    raw = np.asarray(dataset[variable].values)
    attrs = dataset[variable].attrs
    codes = np.zeros(raw.shape, dtype='uint8')
    quality_values = np.full(raw.shape, 255, dtype='uint8')
    if variable == 'measurement':
        from .mrms import PRODUCTS
        info = PRODUCTS[dataset.attrs['product']]
        if info.get('zero_ambiguous'):
            raise ValueError('Ambiguous shear zeros are outside this reflectivity recipe')
        physical = raw.astype('float32')
        codes[raw == info['missing']] = 1
        codes[raw == info['no_coverage']] = 2
        codes[~np.isfinite(raw)] = 4
        if 'bitmap_valid' not in dataset:
            raise ValueError('MRMS processing requires the saved bitmap')
        quality_values = np.asarray(dataset.bitmap_valid.values, dtype='uint8')
        codes[quality_values == 0] = 3
    else:
        encoded = _unsigned(raw, attrs)
        physical = (encoded.astype('float64') * attrs.get('scale_factor', 1)
                    + attrs.get('add_offset', 0)).astype('float32')
        flag = variable.replace('CMI', 'DQF')
        if flag in dataset:
            quality_values = np.asarray(dataset[flag].values, dtype='uint8')
            # Operational CMIPF definitions: retain the source meanings in provenance.
            codes[quality_values == 1] = 9
            codes[~np.isin(quality_values, [0, 1])] = 5
        else:
            codes[:] = 8
        if 'valid_range' in attrs:
            limits = _unsigned(np.asarray(attrs['valid_range'], dtype=raw.dtype), attrs)
            codes[(encoded < limits[0]) | (encoded > limits[1])] = 6
        if '_FillValue' in attrs:
            fill = attrs['_FillValue']
            # Signed containers can carry an unsigned fill attribute.
            codes[(raw == fill) | (encoded == fill)] = 1
        codes[~np.isfinite(raw)] = 4
        codes[np.isfinite(raw) & ~np.isfinite(physical)] = 10
    valid = (codes == 0) | ((codes == 9) & (quality == 'conditional'))
    values = np.where(valid, physical, np.nan).astype('float32')
    return {'values': values, 'valid': valid, 'quality': quality_values,
            'classes': codes, 'units': attrs.get('units', ''), 'source_attrs': attrs.copy()}


def geometry(dataset, grid, method='bilinear', cache=None):
    """Precompute vectorized source indices/weights, keeping grids independent."""
    from . import goes
    radar = 'measurement' in dataset
    if radar:
        x = (np.asarray(dataset.longitude.values, dtype='float64') + 180) % 360 - 180
        y = np.asarray(dataset.latitude.values, dtype='float64')
        tx, ty = grid.longitude.values, grid.latitude.values
        crs = 'EPSG:4326'
    else:
        crs, height = goes.projection(dataset)
        x, y = goes._physical_coordinate(dataset.x) * height, goes._physical_coordinate(dataset.y) * height
        tx, ty = Transformer.from_crs('EPSG:4326', crs, always_xy=True).transform(
            grid.longitude.values, grid.latitude.values)
    key = scientific_id({'x': x.tolist(), 'y': y.tolist(), 'crs': str(crs),
        'grid': grid.attrs, 'method': method})
    path = Path(cache) / (key + '.npz') if cache else None
    if path and path.exists():
        with np.load(path, allow_pickle=False) as saved:
            return {k: saved[k] for k in saved.files}
    if min(len(x), len(y)) < 2:
        raise ValueError('Processing needs at least two source coordinates per axis')
    for axis in (x,y):
        delta=np.diff(axis)
        if not (np.all(delta>0) or np.all(delta<0)):
            raise ValueError('Native coordinates must be strictly monotonic')
    ix = np.interp(tx.ravel(), x if x[0] < x[-1] else x[::-1],
                   np.arange(len(x)) if x[0] < x[-1] else np.arange(len(x))[::-1], left=np.nan, right=np.nan)
    iy = np.interp(ty.ravel(), y if y[0] < y[-1] else y[::-1],
                   np.arange(len(y)) if y[0] < y[-1] else np.arange(len(y))[::-1], left=np.nan, right=np.nan)
    support = np.isfinite(ix) & np.isfinite(iy) & grid.inside_bbox.values.ravel()
    ix, iy = np.nan_to_num(ix), np.nan_to_num(iy)
    nearest = np.rint(iy).astype('int64') * len(x) + np.rint(ix).astype('int64')
    if method == 'nearest':
        indices, weights = nearest[None], np.ones((1, ix.size), dtype='float32')
    else:
        x0, y0 = np.minimum(ix.astype('int64'), len(x)-2), np.minimum(iy.astype('int64'), len(y)-2)
        dx, dy = ix-x0, iy-y0
        indices = np.stack([y0*len(x)+x0, y0*len(x)+x0+1, (y0+1)*len(x)+x0, (y0+1)*len(x)+x0+1])
        weights = np.stack([(1-dx)*(1-dy), dx*(1-dy), (1-dx)*dy, dx*dy]).astype('float32')
    result = {'indices': indices, 'weights': weights, 'nearest': nearest,
              'support': support, 'shape': np.array(grid.inside_bbox.shape)}
    if path:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_name(path.name + f'.{uuid.uuid4().hex}.tmp.npz')
        np.savez_compressed(temp, **result)
        os.replace(temp, path)
    return result


def regrid(decoded, mapping):
    index, weights = mapping['indices'], mapping['weights']
    valid = decoded['valid'].ravel()[index]
    supported = mapping['support'] & np.all(valid | (weights <= 1e-7), axis=0)
    values = np.sum(np.nan_to_num(decoded['values'].ravel()[index]) * weights, axis=0)
    shape = tuple(mapping['shape'])
    return {'values': np.where(supported, values, np.nan).reshape(shape).astype('float32'),
            'valid': supported.reshape(shape),
            'quality': decoded['quality'].ravel()[mapping['nearest']].reshape(shape),
            'classes': decoded['classes'].ravel()[mapping['nearest']].reshape(shape),
            'source_support': mapping['support'].reshape(shape)}


class NativeReader:
    """Bounded archive-handle cache; closes every raw ZIP on exit."""
    def __init__(self, maximum=4):
        self.maximum, self.opened = maximum, OrderedDict()

    def read(self, record):
        from .storage import open_raw
        path = record['path']
        if path not in self.opened:
            if len(self.opened) >= self.maximum:
                self.opened.popitem(last=False)[1].close()
            self.opened[path] = open_raw(path)
        self.opened.move_to_end(path)
        return scan_dataset(self.opened[path], int(record['row_index']))

    def close(self):
        for dataset in self.opened.values():
            dataset.close()
        self.opened.clear()

    def __enter__(self): return self
    def __exit__(self, *args): self.close()


def observation_index(location, start, end, recipe):
    """Build scientific observation pointers from completed manifests, with halo."""
    from .dataset_report import selected_archives
    begin = utc(start) - timedelta(minutes=70)
    finish = utc(end)
    if utc(start) >= finish:
        raise ValueError('End must be later than start, exclusive')
    rows, archives = [], []
    wanted = {RADAR_PRODUCTS[v]: v for v in recipe.radar}
    for source in ('goes', 'mrms'):
        if source == 'mrms' and not wanted: continue
        for archive in selected_archives(location, source=source, start=begin, end=end):
            band = archive.get('band')
            if source == 'goes' and (archive['product'] != 'ABI-L2-CMIPF' or band not in recipe.bands): continue
            if source == 'mrms' and archive['product'] not in wanted: continue
            marker = archive['marker']
            if tuple(marker.get('region', marker.get('bbox', recipe.bbox))) != tuple(recipe.bbox):
                raise ValueError('Source region differs from the requested processing bbox')
            variable = f'abi{band:02d}c' if source == 'goes' else wanted[archive['product']]
            archive_key = scientific_id({'path': archive['path'], 'sha256': archive['archive_sha256']})
            archives.append({'archive_id': archive_key, 'path': archive['path'],
                'sha256': archive['archive_sha256'], 'stored_bytes': archive['stored_bytes'],
                'source': source, 'product': archive['product'], 'band': band,
                'satellite': marker.get('satellite'), 'variable': variable})
            for i, asset in enumerate(marker['assets']):
                observed = utc(asset['time'])
                if not begin <= observed < finish: continue
                until = utc(asset.get('end_time') or asset['time'])
                if source == 'goes' and not asset.get('end_time'):
                    # Some early manifests keep the end in the selected source filename.
                    import re
                    from .goes import scan_time
                    match = re.search(r'_e(\d+)_c', asset.get('source_url', ''))
                    if not match: raise ValueError('GOES scan end is required for causal processing')
                    until = scan_time(match[1])
                rows.append({'archive_id': archive_key, 'path': archive['path'], 'row_index': i,
                    'variable': variable, 'asset_id': asset['asset_id'], 'time': iso(observed),
                    'end_time': iso(until), 'satellite': marker.get('satellite'),
                    'source': source, 'source_url': asset.get('source_url', '')})
    return pd.DataFrame(rows), pd.DataFrame(archives)


def align_observations(observations, start, end, variables):
    """Latest eligible per-variable scan at/before a slot, without reuse.

    Full-disk scan-end time is the causal bound. Ten-minute age applies to GOES;
    radar keeps its five-minute matching. Actual times remain in the index.
    """
    start_stamp = pd.Timestamp(utc(start)) - pd.Timedelta(minutes=50)
    slots = pd.date_range(start_stamp.ceil('10min'), pd.Timestamp(utc(end)), freq='10min', inclusive='left')
    candidates = {}
    for variable in variables:
        rows = observations[observations.variable == variable].to_dict('records') if len(observations) else []
        candidates[variable] = sorted(rows, key=lambda r: (r['end_time'], r['time'], r['asset_id']))
    positions = {variable: 0 for variable in variables}
    waiting = {variable: [] for variable in variables}
    aligned = []
    for slot in slots:
        for variable in variables:
            source = 'goes' if variable.startswith('abi') else 'mrms'
            tolerance = 600 if source == 'goes' else 300
            source_rows = candidates[variable]
            cursor = positions[variable]
            while cursor < len(source_rows) and utc(source_rows[cursor]['end_time']) <= slot.to_pydatetime():
                waiting[variable].append(source_rows[cursor])
                cursor += 1
            positions[variable] = cursor
            eligible = [r for r in waiting[variable] if
                0 <= (slot.to_pydatetime()-utc(r['end_time'])).total_seconds() <= tolerance]
            waiting[variable] = eligible
            if eligible:
                row = max(eligible, key=lambda r: (r['end_time'], r['time'], r['asset_id']))
                waiting[variable].remove(row)
                aligned.append({**row, 'slot_time': iso(slot.to_pydatetime()),
                    'offset_seconds': (utc(row['time'])-slot.to_pydatetime()).total_seconds()})
    return pd.DataFrame(aligned)


def window_index(aligned, start, end, variables, history=6, targets=0):
    """Stable scientific window IDs; report missing observations explicitly."""
    rows = []
    available = {(r['slot_time'], r['variable']): r for r in aligned.to_dict('records')}
    for stamp in pd.date_range(pd.Timestamp(utc(start)).ceil('10min'), pd.Timestamp(utc(end)),
                               freq='10min', inclusive='left'):
        times = [stamp + pd.Timedelta(minutes=10*i) for i in range(-(history-1), targets+1)]
        entries = [available.get((iso(t.to_pydatetime()), v)) for t in times for v in variables]
        missing = sum(r is None for r in entries)
        source_ids = [r['asset_id'] if r else None for r in entries]
        rows.append({'window_id': scientific_id({'time': str(stamp), 'variables': variables,
                     'history': history, 'targets': targets, 'sources': source_ids}),
                     'time': iso(stamp.to_pydatetime()), 'eligible': missing == 0,
                     'missing_observations': missing, 'history': history, 'targets': targets})
    return pd.DataFrame(rows)
