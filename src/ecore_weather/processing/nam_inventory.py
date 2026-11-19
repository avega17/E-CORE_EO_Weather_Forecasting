"""Inventory archived NAM Puerto Rico forecasts without reading GRIB payloads."""
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from itertools import islice
import re

import numpy as np

from ecore_weather.core.common import PR_BBOX, iso, utc
from ecore_weather.processing.nwp import NAM, ForecastArchive, parse_index

HOUR = timedelta(hours=1)
CYCLE = 6 * HOUR
LEADS = tuple(range(61))
LEAD_SAMPLES = (0, 1, 2, 3, 6, 12, 24, 36, 60)
NEST_BOUNDS = (-75.5, 15.0, -62.509, 22.005)  # NCEP grid 194, gridpoint centers.
KEY = re.compile(r'\.hiresf(\d{2})\.tm00\.grib2$')


def timeline(start, end, step):
    while start < end:
        yield start
        start += step


def listed_leads(archive, cycle):
    prefix = f'nam.{cycle:%Y%m%d}/nam.t{cycle:%H}z.priconest.'
    objects = archive.list(NAM, prefix)
    return {int(match[1]): (obj, objects.get(key + '.idx'))
            for key, obj in objects.items() if (match := KEY.search(key))}


def availability(cycles, hour):
    """Return an indexed forecast using the same archive cutoff as the storm demo."""
    candidate = hour.replace(hour=(hour.hour // 6) * 6)
    cutoff = hour - timedelta(minutes=10)
    latest = None
    indexed = False
    listed = False
    while hour - candidate <= 60 * HOUR:
        lead = int((hour - candidate) / HOUR)
        pair = cycles.get(candidate, {}).get(lead)
        if pair:
            listed = True
            obj, idx = pair
            indexed |= idx is not None
            modified = obj.get('LastModified')
            if latest is None and idx is not None and modified is not None and utc(modified) <= cutoff:
                latest = candidate, lead, pair
        candidate -= CYCLE
    return listed, indexed, latest


def sample_index(archive, selection):
    cycle, lead, pair, roles = selection
    obj, idx = pair
    # Keep this guard next to the only GET in the audit.
    if idx['Key'] != obj['Key'] + '.idx' or not idx['Key'].endswith('.grib2.idx'):
        raise ValueError(f"Expected a GRIB text index for {obj['Key']}")
    response = archive.client.get_object(Bucket=NAM, Key=idx['Key'], IfMatch=idx['ETag'])
    with response['Body'] as body:
        data = body.read(idx['Size'] + 1)
    if len(data) != idx['Size']:
        raise IOError(f"Incomplete NAM index {idx['Key']}")
    return cycle, lead, parse_index(data.decode('ascii'), obj['Size']), len(data), roles


def spatial_coverage():
    from ecore_weather.processing.transforms import Recipe, target_grid

    west, south, east, north = NEST_BOUNDS
    grid = target_grid(Recipe(resolution_km=3, radar=()))
    inside = grid.inside_bbox.values.astype(bool)
    lat, lon = grid.latitude.values, grid.longitude.values
    supported = (lon >= west) & (lon <= east) & (lat >= south) & (lat <= north)
    y, x = ((n - 256) // 2 for n in supported.shape)
    return {
        'acquisition_bbox': PR_BBOX, 'nam_gridpoint_bounds': NEST_BOUNDS,
        'native_shape': [310, 544], 'target_shape': list(inside.shape),
        'roi_target_cells': int(inside.sum()), 'roi_supported_cells': int((inside & supported).sum()),
        'model_crop_cells': 256**2, 'model_crop_supported_cells': int(supported[y:y+256, x:x+256].sum()),
    }


def inventory(start, end):
    from boto3 import client
    from botocore import UNSIGNED
    from botocore.config import Config

    first, last = utc(start), utc(end)
    if first >= last or any(t.hour or t.minute or t.second or t.microsecond for t in (first, last)):
        raise ValueError('NAM inventory requires increasing midnight UTC dates')
    config = Config(signature_version=UNSIGNED, connect_timeout=8, read_timeout=30,
                    max_pool_connections=32, retries={'mode': 'standard', 'total_max_attempts': 4})
    archive = ForecastArchive(None, client('s3', region_name='us-east-1', config=config))
    cycles = {}
    scheduled = iter(timeline(first - 60 * HOUR, last, CYCLE))
    with ThreadPoolExecutor(max_workers=32) as pool:
        while batch := tuple(islice(scheduled, 128)):
            cycles.update(zip(batch, pool.map(lambda c: listed_leads(archive, c), batch)))
            print(f'NAM listings through {iso(batch[-1])} ({len(cycles):,} prefixes)', flush=True)
        return summarize(first, last, cycles, pool, archive)


def summarize(first, last, cycles, pool, archive):
    monthly = defaultdict(Counter)
    by_month = defaultdict(list)
    monthly_hours = defaultdict(list)
    leads = Counter()
    eligible_leads = Counter()
    absent_hours = []
    for cycle, records in cycles.items():
        if cycle < first:
            continue
        month = cycle.strftime('%Y-%m')
        by_month[month].append(cycle)
        row = monthly[month]
        row.update(cycles_expected=1, cycles_listed=int(bool(records)),
                   complete_cycles=int(all(lead in records for lead in LEADS)),
                   grib_objects=len(records), indexed_objects=sum(idx is not None for _, idx in records.values()),
                   listed_bytes=sum(obj['Size'] for obj, _ in records.values()))
        leads.update(records.keys())
    first_listed = last_listed = None
    for hour in timeline(first, last, HOUR):
        month = hour.strftime('%Y-%m')
        listed, indexed, eligible = availability(cycles, hour)
        monthly[month].update(hours_expected=1, hours_listed=int(listed), hours_indexed=int(indexed),
                              hours_asof=int(eligible is not None))
        if indexed:
            first_listed = first_listed or hour
            last_listed = hour
        if eligible is None:
            absent_hours.append(hour)
        else:
            cycle, lead, (obj, _) = eligible
            eligible_leads[lead] += 1
            monthly[month]['asof_whole_object_bytes'] += obj['Size']
            monthly_hours[month].append((hour, *eligible))

    selections = {}
    for month, entries in by_month.items():
        # Alternate initialization hour at four evenly spaced places in each month.
        for offset in range(4):
            candidates = [t for t in entries if t.hour == offset * 6 and cycles[t]]
            if candidates:
                cycle = candidates[len(candidates) * offset // 4]
                for lead in LEAD_SAMPLES:
                    pair = cycles[cycle].get(lead)
                    if pair and pair[1] is not None:
                        selections[cycle, lead] = (*pair, {'cycle'})
        # Sample each UTC hour independently so a 30- or 31-day month cannot
        # oversample one hour of the six-hour cycle or one accumulation length.
        eligible_by_hour = defaultdict(list)
        for row in monthly_hours[month]:
            eligible_by_hour[row[0].hour].append(row)
        for hour, eligible in sorted(eligible_by_hour.items()):
            _, cycle, lead, pair = eligible[len(eligible) * hour // 24]
            if (cycle, lead) in selections:
                selections[cycle, lead][2].add('hourly')
            else:
                selections[cycle, lead] = (*pair, {'hourly'})

    result = {
        'status': 'complete', 'start': iso(first), 'end_excluded': iso(last),
        'source': NAM, 'grib_payload_bytes': 0, 'listing_prefixes': len(cycles),
        'totals': dict(sum(monthly.values(), Counter())), 'months': {m: dict(r) for m, r in monthly.items()},
        'lead_object_counts': dict(sorted(leads.items())), 'asof_lead_counts': dict(sorted(eligible_leads.items())),
        'first_indexed_valid_hour': iso(first_listed) if first_listed else None,
        'last_indexed_valid_hour': iso(last_listed) if last_listed else None,
        'asof_gaps': gap_runs(absent_hours),
        'spatial': spatial_coverage(),
        'storms': storm_coverage(cycles, first, last),
    }
    result.update(summarize_indexes(pool.map(lambda s: sample_index(archive, s),
        ((cycle, lead, (obj, idx), roles) for (cycle, lead), (obj, idx, roles) in selections.items()))))
    return result


def gap_runs(hours):
    gaps = []
    for hour in hours:
        if gaps and gaps[-1][1] == hour:
            gaps[-1][1] = hour + HOUR
        else:
            gaps.append([hour, hour + HOUR])
    return [{'start': iso(lo), 'end_excluded': iso(hi), 'hours': int((hi-lo)/HOUR)}
            for lo, hi in gaps]


def storm_coverage(cycles, first, last):
    from ecore_weather.processing.storm_windows import STORMS, storm_hours
    result = {}
    for slug, (_, start, end) in STORMS.items():
        hours = [t for t in storm_hours(start, end) if first <= utc(t) < last]
        if hours:
            coverage = [availability(cycles, utc(t)) for t in hours]
            result[slug] = dict(hours=len(hours), indexed=sum(i for _, i, _ in coverage),
                                asof=sum(p is not None for _, _, p in coverage))
    return result


# One explicitly chosen level and time statistic per pilot channel.
CORE = {
    'surface_pressure': ('PRES', 'surface', 'instant'),
    'sea_level_pressure': ('PRMSL', 'mean sea level', 'instant'),
    'air_temperature_2m': ('TMP', '2 m above ground', 'instant'),
    'dew_point_2m': ('DPT', '2 m above ground', 'instant'),
    'wind_u_10m': ('UGRD', '10 m above ground', 'instant'),
    'wind_v_10m': ('VGRD', '10 m above ground', 'instant'),
    'precipitable_water': ('PWAT', 'entire atmosphere', 'instant'),
    'precipitation_accumulation': ('APCP', 'surface', 'native-accumulation'),
    'precipitation_rate': ('PRATE', 'surface', 'instant'),
    'composite_reflectivity': ('REFC', 'entire atmosphere', 'instant'),
}
UPPER = {
    'height_500': ('HGT', '500 mb', 'instant'),
    'temperature_850': ('TMP', '850 mb', 'instant'),
    'humidity_850': ('SPFH', '850 mb', 'instant'),
    'wind_u_850': ('UGRD', '850 mb', 'instant'),
    'wind_v_850': ('VGRD', '850 mb', 'instant'),
    'pressure_velocity_700': ('VVEL', '700 mb', 'instant'),
}


def matches(record, spec, lead):
    field, level, statistic = spec
    if record['field'] != field or not (
        record['level'] == level or (level == 'entire atmosphere' and record['level'].startswith(level))
    ):
        return False
    descriptor = record['descriptor']
    if statistic == 'instant':
        return descriptor == ('anl' if lead == 0 else f'{lead} hour fcst')
    window = re.fullmatch(r'(\d+)-(\d+) hour acc fcst', descriptor)
    return bool(window and int(window[2]) == lead and 1 <= lead-int(window[1]) <= 3)


def byte_quantiles(values):
    return [round(x) for x in np.quantile(values, [0.1, 0.5, 0.9])] if values else None


def summarize_indexes(samples):
    fields = defaultdict(lambda: {'samples': 0, 'levels': set(), 'descriptors': set()})
    selected = defaultdict(list)
    sums = defaultdict(list)
    hourly_count = 0
    ambiguous = Counter()
    count = index_bytes = 0
    lead_sample_counts = Counter()
    precipitation_windows = Counter()
    for cycle, lead, records, size, roles in samples:
        count += 1
        index_bytes += size
        lead_sample_counts[lead] += 1
        for field in {r['field'] for r in records}:
            fields[field]['samples'] += 1
        for record in records:
            field = fields[record['field']]
            field['levels'].add(record['level'])
            field['descriptors'].add(re.sub(r'\d+(?:-\d+)? hour', '<lead> hour', record['descriptor']))
        if 'hourly' not in roles:
            continue
        hourly_count += 1
        for record in records:
            if matches(record, CORE['precipitation_accumulation'], lead):
                start = int(record['descriptor'].split('-', 1)[0])
                precipitation_windows[lead-start] += 1
        pilot = {name: {(r['start'], r['stop']) for r in records if matches(r, spec, lead)}
                 for name, spec in (CORE | UPPER).items()}
        for name, ranges in pilot.items():
            if len(ranges) == 1:
                lo, hi = next(iter(ranges))
                selected[name].append(hi-lo)
            elif ranges:
                ambiguous[name] += 1
        for profile, names in [('core', CORE), ('core_and_upper_air', CORE | UPPER)]:
            if all(len(pilot[name]) == 1 for name in names):
                unique_ranges = set().union(*(pilot[name] for name in names))
                sums[profile].append(sum(hi-lo for lo, hi in unique_ranges))
    return {
        'index_samples': count, 'index_sample_bytes': index_bytes,
        'index_sample_counts_by_lead': dict(sorted(lead_sample_counts.items())),
        'hourly_index_samples': hourly_count,
        'hourly_index_precipitation_windows': dict(sorted(precipitation_windows.items())),
        'variables': {key: dict(samples=row['samples'], levels=sorted(row['levels']),
                                descriptors=sorted(row['descriptors'])) for key, row in sorted(fields.items())},
        'pilot': {name: dict(code=spec[0], level=spec[1], statistic=spec[2], hourly_samples=len(selected[name]),
                             ambiguous_samples=ambiguous[name],
                             message_bytes_p10_median_p90=byte_quantiles(selected[name]))
                  for name, spec in (CORE | UPPER).items()},
        'pilot_profiles': {name: dict(hourly_samples=len(values), message_bytes_p10_median_p90=byte_quantiles(values))
                           for name, values in sums.items()},
    }
