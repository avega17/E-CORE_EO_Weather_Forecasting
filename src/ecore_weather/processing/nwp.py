"""Bounded GRIB range reads and publication-aware NAM selection."""
from __future__ import annotations

from datetime import timedelta
from functools import lru_cache
from pathlib import Path
import re
import tempfile
from concurrent.futures import ThreadPoolExecutor

import numpy as np

from ecore_weather.core.common import PR_BBOX, iso, utc
from ecore_weather.core.transfer_budget import Budget, BudgetExceeded
from ecore_weather.processing.storm_windows import STORMS, storm_hours
from ecore_weather.processing.transforms import atomic_json

NAM = 'noaa-nam-pds'
RRFS = 'noaa-rrfs-ops-pds'
FIELDS = {
    'PRES': ('Surface pressure', 'surface', ('sp', 'pres')),
    'PRMSL': ('Sea-level pressure', 'mean sea level', ('prmsl', 'msl')),
    'DPT': ('2 m dew point', '2 m above ground', ('2d', 'dpt', 'd')),
    'REFC': ('Composite reflectivity', 'entire atmosphere', ('refc',)),
    'HGT': ('500 hPa geopotential height', '500 mb', ('gh', 'z')),
    'APCP': ('Precipitation accumulation', 'surface', ('tp', 'apcp')),
    'PRATE': ('Precipitation rate', 'surface', ('prate',)),
    'TMP': ('2 m temperature', '2 m above ground', ('2t', 't')),
    'UGRD': ('10 m eastward wind', '10 m above ground', ('10u', 'u')),
    'VGRD': ('10 m northward wind', '10 m above ground', ('10v', 'v')),
    'PWAT': ('Precipitable water', 'entire atmosphere', ('pwat', 'tcwv')),
}
LEVELS = {'HGT': 500, 'TMP': 2, 'DPT': 2, 'UGRD': 10, 'VGRD': 10}


def parse_index(text, size):
    records = []
    for line in text.splitlines():
        parts = line.split(':')
        if len(parts) >= 6 and parts[1].isdigit():
            records.append((int(parts[1]), parts[3], parts[4], parts[5]))
    offsets = sorted({offset for offset, *_ in records})
    if not offsets or offsets[0] < 0 or offsets[-1] >= size:
        raise ValueError('Malformed GRIB index offsets')
    ends = dict(zip(offsets, offsets[1:] + [size]))
    return [{'start': start, 'stop': ends[start], 'field': field, 'level': level,
             'descriptor': descriptor} for start, field, level, descriptor in records]


class ForecastArchive:
    """Reuse public cycle listings and exact object identities across hours."""

    def __init__(self, budget, client=None):
        self.budget = budget
        if client is None:
            from botocore import UNSIGNED
            from botocore.config import Config
            import boto3
            client = boto3.client('s3', region_name='us-east-1',
                config=Config(signature_version=UNSIGNED, retries={'total_max_attempts': 1},
                              connect_timeout=8, read_timeout=30, max_pool_connections=16))
        self.client = client

    def list(self, bucket, prefix):
        pages = self.client.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix)
        return {row['Key']: row for page in pages for row in page.get('Contents', [])}

    @lru_cache(maxsize=128)
    def cycle(self, initialization):
        return self.list(NAM, f'nam.{initialization:%Y%m%d}/nam.t{initialization:%H}z.priconest.')

    def get(self, bucket, obj, start=None, stop=None):
        size = obj['Size'] if start is None else stop - start
        args = dict(Bucket=bucket, Key=obj['Key'], IfMatch=obj['ETag'])
        if start is not None:
            args['Range'] = f'bytes={start}-{stop-1}'
        expected = None if start is None else f'bytes {start}-{stop-1}/{obj["Size"]}'
        for attempt in range(3):
            charge = self.budget.reserve(size)
            data = None
            try:
                response = self.client.get_object(**args)
                with response['Body'] as stream:
                    if response.get('ContentLength') != size or (
                        start is not None and response.get('ContentRange') != expected
                    ):
                        raise IOError('Server did not return the requested GRIB range')
                    data = stream.read(size)
                if len(data) != size:
                    raise IOError('Incomplete NOAA response')
                return data
            except Exception:
                if attempt == 2:
                    raise
            finally:
                self.budget.settle(charge, None if data is None else len(data))

    @lru_cache(maxsize=96)
    def records(self, bucket, key, size, idx_key, idx_size, idx_etag):
        text = self.get(bucket, {'Key': idx_key, 'Size': idx_size, 'ETag': idx_etag}).decode('ascii')
        return parse_index(text, size)

    def fields(self, bucket, obj, idx):
        return self.records(bucket, obj['Key'], obj['Size'], idx['Key'], idx['Size'], idx['ETag'])

    def select_nam(self, when, field='REFC', cutoff=None, read=False):
        valid = utc(when)
        if valid.minute or valid.second or valid.microsecond:
            raise ValueError('Choose an exact UTC forecast hour')
        cutoff = utc(cutoff) if cutoff is not None else valid - timedelta(minutes=10)
        if field not in FIELDS:
            raise ValueError(f'Unsupported NAM field {field}')
        cycle = valid.replace(hour=(valid.hour // 6) * 6)
        failures = {}
        while valid - cycle <= timedelta(hours=60):
            lead = int((valid-cycle).total_seconds()//3600)
            key = f'nam.{cycle:%Y%m%d}/nam.t{cycle:%H}z.priconest.hiresf{lead:02}.tm00.grib2'
            try:
                if cycle > cutoff:
                    reason = 'cycle-after-cutoff'
                else:
                    objects = self.cycle(cycle)
                    obj, idx = objects.get(key), objects.get(key+'.idx')
                    if obj is None or idx is None:
                        reason = 'missing-object-or-index'
                    elif not obj.get('LastModified'):
                        reason = 'unknown-availability'
                    elif utc(obj['LastModified']) > cutoff:
                        reason = 'archive-object-after-cutoff'
                    else:
                        records = [r for r in self.fields(NAM, obj, idx) if matches_field(r, field)]
                        if not records:
                            reason = 'missing-field'
                        else:
                            record = records[0]
                            result = {'status': 'available', 'valid_time': iso(valid),
                                      'initialization': iso(cycle), 'lead_hours': lead,
                                      'available_in_s3': iso(utc(obj['LastModified'])),
                                      'cutoff': iso(cutoff), 'key': key, 'etag': obj['ETag'],
                                      'field': field, 'record': record, 'skipped': failures}
                            if read:
                                result['sample'] = self.decode(NAM, obj, record, expected=(cycle, valid))
                            return result
            except BudgetExceeded:
                raise
            except Exception as exc:
                reason = f'read-failure: {type(exc).__name__}'
            failures[reason] = failures.get(reason, 0)+1
            cycle -= timedelta(hours=6)
        return {'status': 'unavailable', 'valid_time': iso(valid), 'field': field,
                'cutoff': iso(cutoff), 'skipped': failures}

    def decode(self, bucket, obj, record, bbox=PR_BBOX, expected=None):
        return self.decode_range(bucket, obj, [record], bbox, expected)[record['field']]

    def decode_range(self, bucket, obj, records, bbox=PR_BBOX, expected=None):
        """Decode fields sharing one physical GRIB message after one range read."""
        import pygrib

        start, stop = records[0]['start'], records[0]['stop']
        if any((r['start'], r['stop']) != (start, stop) for r in records):
            raise ValueError('Fields in one decode must share the same GRIB byte range')
        content = self.get(bucket, obj, start, stop)
        if not content.startswith(b'GRIB') or not content.endswith(b'7777'):
            raise ValueError('Invalid GRIB message boundaries')
        output = {}
        with tempfile.NamedTemporaryFile(suffix='.grib2') as tmp:
            tmp.write(content)
            tmp.flush()
            with pygrib.open(tmp.name) as messages:
                for message in messages:
                    for record in records:
                        field = record['field']
                        if field in output or message.shortName not in FIELDS[field][2] or (
                            field in LEVELS and message.level != LEVELS[field]
                        ):
                            continue
                        output[field] = crop_message(message, record, bbox, expected, len(content))
        if output.keys() != {r['field'] for r in records}:
            raise ValueError(f'GRIB field or level does not match index: {sorted({r["field"] for r in records} - output.keys())}')
        return output


def crop_message(message, record, bbox, expected, transferred):
    initialization = utc(message.analDate)
    valid_time = utc(message.validDate)
    if message.stepType == 'accum' and message.stepUnits == 1:
        valid_time = initialization + timedelta(hours=int(message.endStep))
    if expected and (initialization, valid_time) != expected:
        raise ValueError('GRIB initialization or valid time differs from selected forecast')
    values = np.ma.asarray(message.values).astype('float32').filled(np.nan)
    lat, lon = message.latlons()
    lon = (lon + 180) % 360 - 180
    west, south, east, north = bbox
    inside = (lon >= west) & (lon <= east) & (lat >= south) & (lat <= north)
    yy, xx = np.where(inside)
    if not yy.size:
        raise ValueError('Forecast grid does not intersect the acquisition box')
    crop = np.s_[yy.min():yy.max()+1, xx.min():xx.max()+1]
    data = np.where(inside[crop], values[crop], np.nan)
    return {'values': data, 'latitude': lat[crop], 'longitude': lon[crop],
            'source_bounds': [float(lon.min()), float(lat.min()), float(lon.max()), float(lat.max())],
            'units': message.units, 'field': record['field'], 'level': record['level'],
            'initialization': iso(initialization), 'valid_time': iso(valid_time),
            'lead': message.forecastTime, 'step_range': str(message.stepRange),
            'step_type': str(message.stepType), 'transfer_bytes': transferred}


def matches_field(record, field):
    return record['field'] == field and record['level'].startswith(FIELDS[field][1])


def sample_size(sample):
    import zarr
    from zarr.codecs import BloscCodec
    values = sample['values']
    root = zarr.group(store=zarr.storage.MemoryStore(), zarr_format=3)
    array = root.create_array('values', data=values, compressors=BloscCodec(cname='zstd', clevel=3))
    return {k:v for k,v in sample.items() if k not in ('values','latitude','longitude')} | {
        'shape': list(values.shape), 'logical_bytes': values.nbytes, 'compressed_bytes': array.nbytes_stored()}


def rrfs_sample(archive):
    prefix='rrfs.20261104/00/'
    errors=[]
    try:
        objects=archive.list(RRFS,prefix)
        keys=[k for k in sorted(objects) if re.search(r'\.f00[01]\.pr\.grib2$',k) and k+'.idx' in objects]
        for key in keys:
            obj=objects[key]
            records=archive.fields(RRFS,obj,objects[key+'.idx'])
            for field in ('HGT','TMP','REFC'):
                record=next((r for r in records if matches_field(r,field)),None)
                if record:
                    try:
                        sample=sample_size(archive.decode(RRFS,obj,record))
                        return {'status':'decoded','key':key,'sample':sample,'prior_errors':errors}
                    except (ValueError,OSError) as exc:
                        errors.append(f'{key}: {field}: {exc}')
    except BudgetExceeded:
        raise
    except Exception as exc:
        errors.append(f'{type(exc).__name__}: {exc}')
    return {'status':'read-failed' if errors else 'unavailable','probe':prefix,'errors':errors}


def audit_report(result):
    lines = ['# NAM and RRFS availability for Puerto Rico', '',
             'Availability uses sampled NAM cycles and every requested storm hour. '
             'S3 timestamps describe the archived object. Missing evidence leaves an explicit gap.', '',
             f"Returned payload: {result['returned_bytes']:,} bytes. Budget: {result['budget_bytes']:,} bytes.", '',
             '## Storm windows', '',
             '| Storm | Hours checked | NAM at cutoff | NAM gaps | All three GOES rows available |',
             '|---|---:|---:|---:|---:|']
    for name, rows in result['storms'].items():
        eligible = sum(row['status']=='available' for row in rows)
        goes = sum(row.get('available_goes_rows')==3 for row in rows) if any('available_goes_rows' in r for r in rows) else 'Not checked'
        lines.append(f'| {name} | {len(rows)} | {eligible} | {len(rows)-eligible} | {goes} |')
    lines += ['', '## Monthly NAM samples', '', '| Month | Cycles with objects | Indexed forecasts sampled |',
              '|---|---:|---:|']
    for month in result['months']:
        cycles = month['cycles']
        lines.append(f"| {month['month']} | {sum(c.get('objects',0)>0 for c in cycles)} | {sum(len(c.get('leads',[])) for c in cycles)} |")
    lines += ['', '## Decoded fields', '', '| Field | Unit | Samples | Median message bytes | Median logical crop bytes | Median compressed array bytes |',
              '|---|---|---:|---:|---:|---:|']
    grouped = {}
    for row in result['samples']:
        grouped.setdefault((row['field'],row['units']),[]).append(row)
    for (field, unit), rows in sorted(grouped.items()):
        sizes = [round(np.median([r[key] for r in rows])) for key in ('transfer_bytes','logical_bytes','compressed_bytes')]
        lines.append(f'| {field} | {unit} | {len(rows)} | {sizes[0]:,} | {sizes[1]:,} | {sizes[2]:,} |')
    if result['samples']:
        from ecore_weather.processing.transforms import Recipe, target_grid
        bounds = result['samples'][0]['source_bounds']
        grid=target_grid(Recipe(resolution_km=3,radar=()))
        lon,lat=grid.longitude.values,grid.latitude.values
        grid_supported=(lon>=bounds[0])&(lat>=bounds[1])&(lon<=bounds[2])&(lat<=bounds[3])
        inside=grid.inside_bbox.values.astype(bool)
        y0,x0=((n-256)//2 for n in inside.shape)
        crop=np.s_[y0:y0+256,x0:x0+256]
        region_fraction=(grid_supported&inside).sum()/inside.sum()
        crop_fraction=grid_supported[crop].mean()
        lines += ['', f'Sampled NAM grid bounds (west, south, east, north): {bounds}. '
                  f'The requested acquisition box is {PR_BBOX}. '
                  f'The sampled NAM bounds contain {region_fraction:.1%} of the 3 km cells inside the '
                  f'acquisition box and {crop_fraction:.1%} of the centered 256-cell model crop. '
                  'This is a grid-footprint estimate. Field masks can reduce observed support.']
    rrfs=result.get('rrfs',{})
    lines += ['', '## Operational RRFS', '', f"Puerto Rico sample status: {rrfs.get('status','not checked')}."]
    if 'sample' in rrfs:
        sample=rrfs['sample']
        lines += [f"{sample['field']} ({sample['units']}), valid {sample['valid_time']}. "
                  f"{sample['transfer_bytes']:,} message bytes and {sample['compressed_bytes']:,} compressed crop bytes."]
    if rrfs.get('errors'):
        lines += ['']+[f'- {error}' for error in rrfs['errors']]
    return '\n'.join(lines)


def goes_coverage(location, start, end):
    from ecore_weather.processing.storm_windows import flash_windows
    from ecore_weather.processing.transforms import Recipe, align_observations, observation_index
    recipe = Recipe(resolution_km=3, radar=())
    first = utc(start)-timedelta(minutes=20)
    observations, _ = observation_index(location, first, end, recipe)
    if observations.empty:
        return {}
    aligned = align_observations(observations, first, end, recipe.variables)
    available = aligned.groupby('slot_time').variable.nunique()
    return {iso(hour):sum(all(available.get(iso(slot),0)==len(recipe.variables)
                              for slot in (*history,target))
                          for target,history in flash_windows(hour))
            for hour in storm_hours(start,end)}


def audit(output, budget_gib=4, location=None):
    import pandas as pd
    from uuid import uuid4
    root = Path(output)
    root.mkdir(parents=True, exist_ok=True)
    limit = int(budget_gib*1024**3)
    ledger = root/f'.nam-transfer-{uuid4().hex}.json'
    budget = Budget(ledger, limit)
    archive = ForecastArchive(budget)
    result = {'status':'complete', 'months':[], 'storms':{}, 'samples':[], 'budget_bytes':limit}
    try:
        with ThreadPoolExecutor(max_workers=8) as threads:
            for slug, (_, start, end) in STORMS.items():
                result['storms'][slug] = list(threads.map(archive.select_nam, storm_hours(start,end)))
                if location and Path(location).is_dir():
                    observed = goes_coverage(location,start,end)
                    for row in result['storms'][slug]:
                        row['available_goes_rows'] = observed.get(row['valid_time'],0)
                print(f"{slug}: {sum(r['status']=='available' for r in result['storms'][slug])}"
                      f"/{len(result['storms'][slug])} exact-hour NAM forecasts",flush=True)
        for stamp in pd.date_range('2021-01-01','2026-06-01',freq='MS'):
            month = {'month':stamp.strftime('%Y-%m'), 'cycles':[]}
            result['months'].append(month)
            for day, hour in ((1,0),(15,12),(25,18)):
                dt = utc(stamp.replace(day=day,hour=hour).isoformat())
                entry = {'initialization':iso(dt), 'leads':[]}
                month['cycles'].append(entry)
                try:
                    objects = archive.cycle(dt)
                    entry['objects'] = len(objects)
                    for lead in (1,6):
                        key = f'nam.{dt:%Y%m%d}/nam.t{hour:02}z.priconest.hiresf{lead:02}.tm00.grib2'
                        if key not in objects or key+'.idx' not in objects:
                            continue
                        obj = objects[key]
                        records = archive.fields(NAM,obj,objects[key+'.idx'])
                        fields = [r for r in records if r['field'] in FIELDS and matches_field(r,r['field'])]
                        entry['leads'].append({'lead':lead,'object_bytes':obj['Size'],
                                              'fields':sorted({r['field'] for r in fields})})
                        if day == 1 and lead == 1 and stamp.month in (1,4,7,10):
                            for field in FIELDS:
                                record = next((r for r in fields if r['field']==field),None)
                                if record:
                                    try:
                                        result['samples'].append(sample_size(archive.decode(NAM,obj,record,
                                            expected=(dt,dt+timedelta(hours=lead)))))
                                    except (ValueError,OSError) as exc:
                                        entry.setdefault('sample_errors',{})[field]=str(exc)
                except BudgetExceeded:
                    raise
                except Exception as exc:
                    entry['error']=f'{type(exc).__name__}: {exc}'
            print(f"NAM audit {month['month']}",flush=True)
        result['rrfs']=rrfs_sample(archive)
    except BudgetExceeded as exc:
        result.update(status='paused',reason=str(exc))
    except Exception as exc:
        result.update(status='failed',reason=f'{type(exc).__name__}: {exc}')
        raise
    finally:
        counts=budget.summary()
        result.update(returned_bytes=counts['observed'],charged_bytes=counts['charged'])
        atomic_json(root/'nam-availability.json',result)
        (root/'nam-rrfs-audit.md').write_text(audit_report(result))
        ledger.unlink(missing_ok=True)
    return result
