"""Small validation helpers shared by the manually submitted PBS jobs."""
from __future__ import annotations

import re


def interval(start, end):
    from .common import utc
    pattern = r"\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|\+00:00))?"
    if not all(re.fullmatch(pattern, str(value)) for value in (start, end)):
        raise ValueError('Use ISO dates or UTC timestamps, e.g. 2021-01-01 or 2021-01-01T12:00:00Z')
    begin, stop = utc(start), utc(end)
    if begin >= stop:
        raise ValueError('End must be later than start; end is excluded')
    return begin, stop


def quota_free_gib(text, project, filesystem='eagle'):
    """A conservative estimate from one printed myprojectquotas project row.

    Explicit SI/IEC suffixes are honored. For ambiguous shorthand G/T/etc.,
    take the smaller available-space estimate under SI and IEC conventions.
    Reserve another GiB for display rounding; this is not an exact quota query.
    """
    import math
    from pathlib import PurePosixPath

    def size(token, base):
        match = re.fullmatch(r'(\d+(?:\.\d+)?)([KMGTPE]?)(I?B)?', token.upper())
        if not match:
            raise ValueError(f'Unrecognized quota size {token!r}; inspect myprojectquotas')
        number, prefix, suffix = match.groups()
        power = ' KMGTPE'.index(prefix) if prefix else 0
        multiplier = 1024 if suffix == 'IB' else 1000 if suffix == 'B' else base
        return float(number) * multiplier**power

    rows = [parts for line in text.splitlines()
            if len(parts := line.split()) >= 5 and parts[0] == project
            and parts[1].lower() == 'project'
            and PurePosixPath(parts[2]).name == PurePosixPath(filesystem).name]
    if len(rows) != 1:
        raise ValueError(f'Expected one {project} Project row on {filesystem}; found {len(rows)}')
    used, quota = rows[0][3:5]
    available = min(size(quota, base)-size(used, base) for base in (1000, 1024))
    return max(0, math.floor(available / 1024**3)-1)


def check_noaa(start='2021-01-01T12:00:00Z', minutes=60, satellite='auto', polaris_proxy=False):
    """Discover at most an hour of study-band GOES metadata; no image GETs."""
    import os
    import time
    from datetime import timedelta
    from . import goes
    from .common import iso, utc
    if not 1 <= minutes <= 60:
        raise ValueError('Discovery checks are limited to 1–60 minutes')
    begin = utc(start)
    stop = begin + timedelta(minutes=minutes)
    interval(start, iso(stop))
    if polaris_proxy:
        for name in ('HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy'):
            os.environ[name] = 'http://proxy.alcf.anl.gov:3128'
    before = time.perf_counter()
    selection = goes.discover(iso(begin), iso(stop), satellite=satellite)
    if not selection.assets:
        raise ValueError('NOAA listing succeeded but no matching observations were found; check dates/satellite')
    return {'operation': 'discovery-only', 'source': 'goes', 'product': selection.product,
            'satellite': selection.satellite, 'start': selection.start, 'end_excluded': selection.end,
            'bands': list(selection.bands), 'selected_band_files': len(selection.assets),
            'distinct_scan_times': len({asset.time for asset in selection.assets}),
            'listed_source_bytes': sum(asset.size for asset in selection.assets),
            'imagery_bytes_downloaded': 0, 'wall_seconds': time.perf_counter()-before}


def main():
    import argparse
    import json
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'quota':
        parser = argparse.ArgumentParser(description='Estimate available project GiB from myprojectquotas on stdin')
        parser.add_argument('--project', required=True)
        parser.add_argument('--filesystem', default='eagle')
        args = parser.parse_args(sys.argv[2:])
        try:
            print(quota_free_gib(sys.stdin.read(), args.project, args.filesystem))
        except ValueError as exc:
            parser.exit(1, f'Quota lookup failed: {exc}\n')
        return
    if len(sys.argv) > 1 and sys.argv[1] == 'check-noaa':
        parser = argparse.ArgumentParser(description='List a bounded GOES interval without downloading imagery')
        parser.add_argument('--start', default='2021-01-01T12:00:00Z')
        parser.add_argument('--minutes', type=int, default=60)
        parser.add_argument('--satellite', default='auto', choices=('auto','16','17','18','19'))
        parser.add_argument('--polaris-proxy', action='store_true')
        args = parser.parse_args(sys.argv[2:])
        try:
            result = check_noaa(args.start, args.minutes, args.satellite, args.polaris_proxy)
        except Exception as exc:
            parser.exit(1, f'NOAA discovery check failed ({type(exc).__name__}): {exc}\n')
        print(json.dumps(result, indent=2))
        return
    begin, stop = interval(*sys.argv[1:3])
    if len(sys.argv) > 3 and sys.argv[3] == 'pilot' and (stop-begin).total_seconds() > 86400:
        raise ValueError('Pilot interval must be at most one day; pass short PBS date variables')
    print(begin.isoformat().replace('+00:00','Z'), stop.isoformat().replace('+00:00','Z'))


if __name__ == '__main__':
    main()
