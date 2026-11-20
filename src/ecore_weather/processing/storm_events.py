"""Select retrospective IBTrACS storm encounters for the NAM baseline."""
from collections import defaultdict
import csv
from datetime import timedelta
import hashlib
import io
from pathlib import Path
from urllib.request import urlopen

from ecore_weather.core.common import PR_BBOX, iso, utc

TRACK_URL = ('https://www.ncei.noaa.gov/data/'
             'international-best-track-archive-for-climate-stewardship-ibtracs/'
             'v04r01/access/csv/ibtracs.NA.list.v04r01.csv')
START, END = '2021-01-01', '2026-07-01'
HOUR = timedelta(hours=1)


def select_events(start=START, end=END, source=None, limit=None):
    """Return independent padded encounters and a small map-ready track selection."""
    from pyproj import Transformer
    from shapely.geometry import LineString, Point, box
    from shapely.ops import transform

    start, end = utc(start), utc(end)
    if start >= end or (limit is not None and limit < 1):
        raise ValueError('Choose increasing dates and a positive storm limit')
    if source:
        data = Path(source).read_bytes()
        snapshot = {'source': str(source)}
    else:
        with urlopen(TRACK_URL, timeout=60) as response:
            data = response.read(100_000_001)
            snapshot = {'source': TRACK_URL, 'last_modified': response.headers.get('Last-Modified')}
    if len(data) > 100_000_000:
        raise ValueError('IBTrACS CSV exceeded the North Atlantic input limit')
    snapshot.update(sha256=hashlib.sha256(data).hexdigest(), retrieved_at=iso(utc_now()))
    tracks = defaultdict(dict)
    conflicts = set()
    for row in csv.DictReader(io.StringIO(data.decode('utf-8-sig'))):
        sid = row['SID'].strip()
        stamp = row['ISO_TIME'].strip()
        if (not sid or not stamp[:4].isdigit() or row['TRACK_TYPE'].strip().upper() != 'MAIN'):
            continue
        try:
            when = utc(stamp)
        except ValueError:
            continue
        if not start - 6 * HOUR <= when < end + 6 * HOUR:
            continue
        try:
            lon, lat = float(row['USA_LON']), float(row['USA_LAT'])
        except ValueError:
            continue
        if not -180 <= lon <= 360 or not -90 <= lat <= 90:
            continue
        fix = ((lon + 180) % 360 - 180, lat, row['NAME'].strip())
        if when in tracks[sid] and tracks[sid][when][:2] != fix[:2]:
            conflicts.add((sid, when))
        else:
            tracks[sid][when] = fix
    project = Transformer.from_crs(4326, '+proj=aeqd +lat_0=18.2 +lon_0=-66.4 +datum=WGS84',
                                   always_xy=True).transform
    region = transform(project, box(*PR_BBOX).segmentize(.1))
    reference = Point(*project(-66.4, 18.2))
    events, nearby, map_tracks = [], [], {}
    for sid, rows in tracks.items():
        points = [(when, Point(*fix[:2])) for when, fix in sorted(rows.items())
                  if (sid, when) not in conflicts]
        name = next((fix[2] for fix in rows.values() if fix[2]), sid)
        spans, distances = [], []
        for when, point in points:
            if start <= when < end:
                projected = transform(project, point)
                distances.append((projected.distance(region), projected.distance(reference)))
                if region.covers(projected):
                    spans.append((when, when))
        for (a, p), (b, q) in zip(points, points[1:]):
            if b - a > 6 * HOUR or a >= end or b <= start:
                continue
            if any((sid, t) in conflicts for t in rows if a < t < b):
                continue
            first, last = max(start, a), min(end, b)
            segment = LineString([transform(project, p), transform(project, q)])
            if not segment.length:
                if region.covers(segment.centroid):
                    spans.append((first, last))
                continue
            left = segment.interpolate((first-a)/(b-a), normalized=True)
            right = segment.interpolate((last-a)/(b-a), normalized=True)
            line = LineString([left, right])
            distances.append((line.distance(region), line.distance(reference)))
            intersection = line.intersection(region)
            if not intersection.is_empty:
                endpoints = [part for part in intersection.geoms] if hasattr(intersection, 'geoms') else [intersection]
                for part in endpoints:
                    fractions = [line.project(Point(xy), normalized=True) for xy in part.coords]
                    entry, exit = (first + (last-first)*f for f in (min(fractions), max(fractions)))
                    if entry < end:
                        spans.append((entry, exit))
        if not distances:
            continue
        distance = min(d[1] for d in distances)/1000
        if not spans:
            edge = min(d[0] for d in distances)/1000
            if edge <= 100:
                nearby.append({'sid': sid, 'name': name, 'distance_to_roi_km': round(edge, 2)})
            continue
        windows = []
        for entry, exit in sorted(spans):
            first = max(start, floor_hour(entry) - 12 * HOUR)
            last = min(end, floor_hour(exit + HOUR - timedelta(microseconds=1)) + 24 * HOUR)
            if windows and first <= windows[-1][3]:
                old = windows[-1]
                windows[-1] = (old[0], max(old[1], exit), old[2], max(old[3], last))
            else:
                windows.append((entry, exit, first, last))
        map_tracks[sid] = [p.coords[0] for when, p in points if start <= when < end]
        for encounter, (entry, exit, first, last) in enumerate(windows, 1):
            events.append(dict(event_id=f'{sid}-{encounter}', sid=sid, name=name, entry=iso(entry),
                               exit=iso(exit), start=iso(first), end_excluded=iso(last),
                               closest_to_reference_km=round(distance, 2)))
    order = sorted({(e['closest_to_reference_km'], e['sid']) for e in events})
    selected = {sid for _, sid in order[:limit]} if limit else set(map_tracks)
    return {'snapshot': snapshot, 'start': iso(start), 'end_excluded': iso(end),
            'track_conflicts': len(conflicts), 'nearby': sorted(nearby, key=lambda r:r['distance_to_roi_km']),
            'events': sorted((e for e in events if e['sid'] in selected), key=lambda e:e['start']),
            'tracks': {sid: map_tracks[sid] for sid in sorted(selected)}}


def floor_hour(when):
    return when.replace(minute=0, second=0, microsecond=0)


def utc_now():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc)


def event_hours(event):
    start, end = utc(event['start']), utc(event['end_excluded'])
    hour = floor_hour(start)
    while hour < end:
        if hour >= start:
            yield hour
        hour += HOUR


def plot_events(cohort, path):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle

    w, s, e, n = PR_BBOX
    fig, ax = plt.subplots(figsize=(9, 8), layout='constrained')
    ax.add_patch(Rectangle((w, s), e-w, n-s, fill=False, edgecolor='black', linewidth=1.6))
    for sid, track in cohort['tracks'].items():
        if not track:
            continue
        x, y = zip(*track)
        row = next(row for row in cohort['events'] if row['sid'] == sid)
        ax.plot(x, y, '.-', markersize=2, linewidth=1, label=f"{row['name']} {row['entry'][:4]}")
    ax.plot(-66.4, 18.2, 'k+', label='Puerto Rico reference')
    ax.set(xlim=(w-5, e+5), ylim=(s-4, n+4), xlabel='Longitude (°)', ylabel='Latitude (°)',
           title='IBTrACS center tracks crossing the Puerto Rico acquisition box')
    ax.grid(alpha=.2)
    ax.legend(loc='upper left', bbox_to_anchor=(1, 1), fontsize='small')
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150)
    plt.close(fig)
