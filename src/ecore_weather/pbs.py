"""Small validation helpers shared by the manually submitted PBS jobs."""
from __future__ import annotations

import re
from .common import utc, iso


def interval(start, end):
    pattern = r"\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|\+00:00))?"
    if not all(re.fullmatch(pattern, str(value)) for value in (start, end)):
        raise ValueError('Use ISO dates or UTC timestamps, e.g. 2021-01-01 or 2021-01-01T12:00:00Z')
    begin, stop = utc(start), utc(end)
    if begin >= stop:
        raise ValueError('End must be later than start; end is excluded')
    return begin, stop


def main():
    import sys
    begin, stop = interval(*sys.argv[1:3])
    if len(sys.argv) > 3 and sys.argv[3] == 'pilot' and (stop-begin).total_seconds() > 86400:
        raise ValueError('Pilot interval must be at most one day; pass short PBS date variables')
    print(iso(begin), iso(stop))


if __name__ == '__main__':
    main()
