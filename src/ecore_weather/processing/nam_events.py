"""Plan and archive the NAM Puerto Rico forecasts for selected tropical cyclones."""
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
import csv
import gzip
from itertools import islice
import json
from shutil import disk_usage
from time import perf_counter
from pathlib import Path

import numpy as np

from ecore_weather.core.common import PR_BBOX, iso, utc
from ecore_weather.core.transfer_budget import Budget, BudgetExceeded
from ecore_weather.processing.nam_inventory import CORE, listed_leads, matches
from ecore_weather.processing.nwp import ForecastArchive, NAM
from ecore_weather.processing.storm_events import HOUR, event_hours, select_events, plot_events
from ecore_weather.processing.transforms import atomic_json, scientific_id

CYCLE = 6 * HOUR
GIB = 1024**3


def requests_for(events, study_end, max_event_hours=None):
    requests = defaultdict(set)
    for event in events:
        hours = event_hours(event)
        if max_event_hours is not None:
            hours = islice(hours, max_event_hours)
        for hour in hours:
            for valid in (hour, hour + HOUR):
                if valid < study_end:
                    requests[valid].add((hour, event['event_id']))
    return requests


def candidate_cycles(valid):
    cycle = valid.replace(hour=(valid.hour // 6) * 6)
    while valid - cycle <= 60 * HOUR:
        yield cycle, (valid - cycle) // HOUR
        cycle -= CYCLE


def select_forecasts(requests, archive, workers=8):
    """Enumerate archived cycles once, keeping every timely forecast and request."""
    targets = defaultdict(list)
    coverage = defaultdict(set)
    gaps = Counter()
    for valid, members in requests.items():
        for cycle, lead in candidate_cycles(valid):
            eligible = sorted((hour, event) for hour, event in members if cycle <= hour)
            if eligible:
                targets[cycle].append((valid, lead, eligible))
    rows = []
    index_bytes = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        listings = pool.map(lambda cycle: listed_leads(archive, cycle), targets)
        indexed = []
        for (cycle, targets_for_cycle), leads in zip(targets.items(), listings):
            for valid, lead, members in targets_for_cycle:
                obj, idx = leads.get(lead, (None, None))
                if obj is None or idx is None:
                    gaps['missing-object-or-index'] += len(members)
                    continue
                if not obj.get('LastModified'):
                    gaps['unknown-archive-time'] += len(members)
                    continue
                available = utc(obj['LastModified'])
                eligible = [(iso(hour), event) for hour, event in members if available <= hour]
                gaps['archived-after-cutoff'] += len(members) - len(eligible)
                if eligible:
                    indexed.append((cycle, valid, lead, obj, idx, eligible))

        def inspect(target):
            cycle, valid, lead, obj, idx, eligible = target
            records = archive.fields(NAM, obj, idx)
            fields = {}
            for name, spec in CORE.items():
                found = [r for r in records if matches(r, spec, lead)]
                if len(found) == 1:
                    fields[name] = found[0]
            return dict(initialization=iso(cycle), valid=iso(valid), lead=lead, key=obj['Key'],
                        etag=obj['ETag'], size=obj['Size'], available=iso(utc(obj['LastModified'])),
                        members=eligible, fields=fields), idx['Size']

        for row, nbytes in pool.map(inspect, indexed):
            index_bytes += nbytes
            gaps['missing-or-ambiguous-field'] += len(CORE) - len(row['fields'])
            if row['fields']:
                rows.append(row)
                for hour, event in row['members']:
                    if hour == row['valid']:
                        coverage[event].add(hour)
    return sorted(rows, key=lambda r:(r['valid'], r['initialization'])), coverage, dict(gaps), index_bytes


def plan_events(start, end, source, limits, output, map_path, workers, rain_location=None):
    cohort = select_events(start, end, source, limits['top'])
    if limits['storm']:
        names = {n.casefold() for n in limits['storm']}
        cohort['events'] = [row for row in cohort['events']
                            if row['name'].casefold() in names or row['sid'].casefold() in names]
        selected = {row['sid'] for row in cohort['events']}
        cohort['tracks'] = {sid: track for sid, track in cohort['tracks'].items() if sid in selected}
    if not cohort['events']:
        raise ValueError('No selected IBTrACS encounters')
    end = utc(cohort['end_excluded'])
    requests = requests_for(cohort['events'], end, limits['hours'])
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    archive = ForecastArchive(Budget(output/'plan-transfer.json', GIB))
    rows, coverage, gaps, index_bytes = select_forecasts(requests, archive, workers)
    for event in cohort['events']:
        hours = [iso(h) for h in islice(event_hours(event), limits['hours'])]
        event['requested_hours'] = len(hours)
        event['available_hours'] = len(coverage[event['event_id']])
        event['missing_hours'] = [h for h in hours if h not in coverage[event['event_id']]]
    if rain_location:
        add_rain_context(cohort['events'], rain_location, limits['hours'])
    counts = Counter((r['valid'], r['initialization']) for r in rows)
    if any(n != 1 for n in counts.values()):
        raise ValueError('Forecast plan contains duplicate cycle and valid pairs')
    stored_fields = sum(len(row['fields']) for row in rows)
    summary = dict(status='planned', storms=len(cohort['tracks']), events=len(cohort['events']),
                   forecast_objects=len(rows), forecast_fields=stored_fields, index_bytes=index_bytes,
                   selected_grib_bytes=sum(b-a for row in rows for a,b in ranges(row)),
                   logical_crop_bytes=stored_fields*310*321*4, gaps=gaps)
    identity = scientific_id({'bbox': PR_BBOX, 'rows': rows})
    path = output/'selection.json.gz'
    tmp = output/'selection.tmp'
    with gzip.open(tmp, 'wt', encoding='utf8') as stream:
        json.dump({'schema':1, 'identity':identity, 'cohort':cohort, 'rows':rows, 'summary':summary}, stream)
    tmp.replace(path)
    with (output/'events.csv').open('w', newline='') as stream:
        keys = [k for k in cohort['events'][0] if k != 'missing_hours']
        writer = csv.DictWriter(stream, keys, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(cohort['events'])
    write_cycle_table(rows, output/'cycles.csv')
    plot_events(cohort, map_path)
    return summary | {'selection':str(path), 'cycle_table':str(output/'cycles.csv'), 'event_table':str(output/'events.csv'), 'map':str(map_path)}


def ranges(row):
    groups = defaultdict(list)
    for name, record in row['fields'].items():
        groups[(record['start'], record['stop'])].append((name, record))
    return groups


def latest_cycle(rows, hour, field):
    """Newest retained run with both exact valid hours and availability by H."""
    return next(iter(paired_cycles(rows, hour, field)), None)


def paired_cycles(rows, hour, field):
    hour = utc(hour)
    first, second = iso(hour), iso(hour + HOUR)
    times = defaultdict(set)
    for row in rows:
        if (row['valid'] in (first, second) and row['available'] <= first
                and field in row['fields']):
            times[row['initialization']].add(row['valid'])
    return sorted((init for init, valid in times.items() if len(valid) == 2), reverse=True)


def write_cycle_table(rows, path):
    hourly = defaultdict(list)
    for row in rows:
        for hour in {h for h, _ in row['members']}:
            hourly[hour].append(row)
    with path.open('w', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(('initialization_hour', 'field', 'latest_pair_cycle', 'eligible_pair_cycles'))
        for hour, forecasts in sorted(hourly.items()):
            for field in CORE:
                cycles = paired_cycles(forecasts, hour, field)
                writer.writerow((hour, field, next(iter(cycles), ''), ' '.join(cycles)))


def add_rain_context(events, location, max_event_hours=None):
    """Summarize optional verified local Pass2 observations only."""
    from ecore_weather.processing.transforms import decode_observation, scan_dataset
    from ecore_weather.storage.archives import open_raw

    product = 'MultiSensor_QPE_01H_Pass2_00.00'
    archive = Path(location)/'mrms'/product
    hours = defaultdict(list)
    for row in events:
        row.update(mrms_hours_observed=0, mrms_peak_hourly_mm=None, mrms_valid_fraction=0.)
        for hour in islice(event_hours(row), max_event_hours):
            hours[hour.strftime('%Y/%m')].append((hour, row))
    for month, requests in hours.items():
        candidates = sorted(archive.glob(f'*/{month}/complete.json'))
        # Explicitly require a unique local regional archive for rain context.
        if len(candidates) != 1:
            continue
        raw = candidates[0].with_name('raw.zarr.zip')
        if not raw.is_file():
            continue
        with open_raw(raw) as ds:
            times = np.asarray(ds['time'].values).astype('datetime64[ns]')
            for hour, event in requests:
                cutoff = np.datetime64(hour.replace(tzinfo=None), 'ns')
                matches = np.flatnonzero((times <= cutoff) & (times >= cutoff-np.timedelta64(5, 'm')))
                if not matches.size:
                    continue
                i = matches[times[matches].argmax()]
                frame = decode_observation(scan_dataset(ds, i))
                coverage = frame['valid'].mean()
                if not coverage:
                    continue
                peak = float(frame['values'][frame['valid']].max())
                event['mrms_hours_observed'] += 1
                event['mrms_valid_fraction'] += float(coverage)
                previous = event['mrms_peak_hourly_mm']
                event['mrms_peak_hourly_mm'] = peak if previous is None else max(peak, previous)
    for event in events:
        if event['mrms_hours_observed']:
            event['mrms_valid_fraction'] /= event['mrms_hours_observed']

def read_forecast(archive, row):
    output = {}
    obj = {'Key':row['key'], 'ETag':row['etag'], 'Size':row['size']}
    expected = utc(row['initialization']), utc(row['valid'])
    for group in ranges(row).values():
        records = [record for _, record in group]
        samples = archive.decode_range(NAM, obj, records, expected=expected)
        output.update({name:samples[record['field']] for name, record in group})
    return output


def archive_events(selection, destination, output, workers, max_gib):
    """Resume verified month prefixes and publish complete forecast stores."""
    import shutil
    import zarr
    from ecore_weather.processing.process_store import owner, _arrays_hash
    from zarr.codecs import BloscCodec

    with gzip.open(selection, 'rt', encoding='utf8') as file:
        plan = json.load(file)
    if plan.get('schema') != 1 or not plan['rows']:
        raise ValueError('The saved selection contains no eligible NAM forecasts')
    identity = scientific_id({'bbox': PR_BBOX, 'rows': plan['rows']})
    if identity != plan['identity']:
        raise ValueError('Saved NAM selection checksum differs')
    estimate = plan['summary']['selected_grib_bytes']
    limit = int(max_gib * GIB) if max_gib is not None else int(1.25 * estimate)
    if limit <= 0:
        raise ValueError('Transfer ceiling must be positive')
    root = Path(destination)/'nwp'/'nam'/'priconest'/identity
    root.mkdir(parents=True, exist_ok=True)
    budget = Budget(Path(output)/f'transfer-{identity}.json', limit)
    archive = ForecastArchive(budget)
    months = defaultdict(list)
    for row in plan['rows']:
        months[row['valid'][:7]].append(row)
    reports = []
    copied = root/'selection.json.gz'
    with owner(root):
        with budget.state() as state:
            state['limit'] = limit
            state['inflight'] = 0
        if not copied.exists():
            tmp = copied.with_suffix('.tmp')
            shutil.copyfile(selection, tmp)
            tmp.replace(copied)
        try:
            for month, rows in sorted(months.items()):
                path = root/month
                path.mkdir(exist_ok=True)
                result = write_month(path, rows, archive, workers, zarr, BloscCodec, _arrays_hash)
                reports.append(result)
                print(f"Verified NAM {month} | {result['rows']} forecasts", flush=True)
        except (BudgetExceeded, KeyboardInterrupt) as exc:
            return {'status':'paused', 'reason':str(exc) or 'interrupted', 'location':str(root),
                    'months_completed':len(reports), 'transfer':budget.summary()}
        report = {'status':'complete', 'location':str(root), 'months_completed':len(reports),
                  'stored_bytes':sum(r['stored_bytes'] for r in reports), 'transfer':budget.summary()}
        atomic_json(root/'complete.json', report)
        return report


def write_month(path, rows, archive, workers, zarr, codec_class, checksum):
    from ecore_weather.processing.process_store import _sync_batch

    start = perf_counter()
    progress = path/'progress.json'
    done = path/'complete.json'
    state = json.loads(progress.read_text()) if progress.is_file() else {'rows':[]}
    if not done.exists() and disk_usage(path).free < (len(rows)-len(state['rows']))*len(CORE)*310*321*4:
        raise OSError(f'Insufficient capacity near {path} for the uncompressed NAM month')
    store = path/'data.zarr'
    root = zarr.open_group(store, mode='a', zarr_format=3)
    offset = len(state['rows'])

    def verify(group, saved):
        if not saved:
            return
        coords = {name:group[name][:] for name in ('latitude','longitude')}
        if checksum(coords) != state['grid_checksum']:
            raise IOError('NAM stored native coordinates differ')
        for name in ('initialization','valid'):
            times = np.array([r[name].removesuffix('Z') for r in rows], dtype='datetime64[ns]')
            np.testing.assert_array_equal(group[name][:], times)
        np.testing.assert_array_equal(group['lead_hours'][:], [r['lead'] for r in rows])
        np.testing.assert_array_equal(group['field'][:], list(CORE))
        for i, row in enumerate(saved):
            data = {'fields':group['values'][i]}
            if row['key'] != rows[i]['key'] or checksum(data) != row['checksum']:
                raise IOError(f'NAM store differs from the verified forecast at row {i}')

    def initialize(sample):
        codec = codec_class(cname='zstd', clevel=3)
        grid = sample['latitude']
        for name in list(root.keys()):
            del root[name]
        root.create_array('values', shape=(len(rows), len(CORE), *grid.shape), dtype='float32',
                          chunks=(1, 1, *grid.shape), fill_value=np.nan, compressors=codec,
                          dimension_names=('forecast','field','y','x'))
        for name in ('latitude', 'longitude'):
            root.create_array(name, data=sample[name], dimension_names=('y','x'))
        for name in ('initialization', 'valid'):
            times = np.array([r[name].removesuffix('Z') for r in rows], dtype='datetime64[ns]')
            root.create_array(name, data=times, dimension_names=('forecast',))
        root.create_array('lead_hours', data=np.array([r['lead'] for r in rows], dtype='int16'),
                          dimension_names=('forecast',))
        root.create_array('field', data=np.array(list(CORE)), dimension_names=('field',))
        root.attrs.update(product='NAM priconest', bbox=PR_BBOX, schema=1,
                          coverage='Native NAM grid within the acquisition region')
        coords = {name:root[name][:] for name in ('latitude','longitude')}
        for name, values in coords.items():
            np.testing.assert_array_equal(values, sample[name])
        state['grid_checksum'] = checksum(coords)
        return coords['latitude'], coords['longitude']

    try:
        if offset:
            verify(root, state['rows'])
            coords = root['latitude'][:], root['longitude'][:]
        if done.is_file():
            if offset != len(rows):
                raise IOError('Completed NAM store has an incomplete checkpoint')
            return json.loads(done.read_text())
        # Limit in-flight full-domain message decode and cropped payloads.
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for first in range(offset, len(rows), workers):
                stop = min(first+workers, len(rows))
                results = pool.map(lambda row: read_forecast(archive, row), rows[first:stop])
                for i, output in enumerate(results, first):
                    sample = next(iter(output.values()))
                    if i == 0:
                        coords = initialize(sample)
                    values = np.full((len(CORE), *coords[0].shape), np.nan, dtype='float32')
                    metadata = {}
                    for j, field in enumerate(CORE):
                        if field not in output:
                            continue
                        image = output[field]
                        np.testing.assert_array_equal(image['latitude'], coords[0])
                        np.testing.assert_array_equal(image['longitude'], coords[1])
                        values[j] = image['values']
                        metadata[field] = {k:image[k] for k in ('units','step_type','step_range','level')}
                    root['values'][i] = values
                    zarr_values = root['values'][i]
                    np.testing.assert_array_equal(zarr_values, values)
                    state['rows'].append(dict(key=rows[i]['key'], fields=metadata,
                                              checksum=checksum({'fields':zarr_values})))
                _sync_batch(path, {'values': root['values']}, first, stop)
                atomic_json(progress, state)
    finally:
        root.store.close()
    # Read the completed archive back through a newly opened store.
    root = zarr.open_group(store, mode='r', zarr_format=3)
    try:
        verify(root, state['rows'])
    finally:
        root.store.close()
    result = {'rows':len(rows), 'attempt_seconds':perf_counter()-start, 'stored_bytes':sum(p.stat().st_size for p in path.rglob('*') if p.is_file())}
    atomic_json(done, result)
    return result
