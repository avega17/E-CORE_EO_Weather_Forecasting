"""Choose GOES reader counts from verified, repeated local pipeline evidence."""
from collections import defaultdict
from statistics import median

READER_METHODS = ((1, 1), (2, 8), (2, 1), (4, 1), (8, 1))
READER_CONFIGS = tuple(("range",p,t) for p,t in READER_METHODS)+(("async_full",4,1),)


def choose_readers(report, minimum_gain=0.10, memory_limit_bytes=8 * 1024**3):
    """Require a gain on both satellites; retain two readers otherwise.

    Writer counts, archive layout and result-queue budgets never change here.
    These short native infrared comparisons do not prove an optimum for C02.
    """
    if report.get('status') != 'complete':
        raise ValueError('GOES pipeline benchmark is not complete')
    groups = defaultdict(list)
    for row in report['rows']:
        groups[(row.get('read_mode','range'), row['read_processes'], row['source_threads'], row['satellite'])].append(row)
    baseline = ("range",2,8)
    satellites = (16, 19)

    def valid(rows):
        return (len({r['repeat'] for r in rows}) >= 3 and
                all(r.get('values_equal') is True and r['wall_seconds'] > 0 and
                    0 < r.get('peak_rss_bytes', 0) <= memory_limit_bytes and
                    (r.get('read_mode','range') != 'async_full' or
                     (r.get('download_concurrency') == 8 and r.get('staging_mib') == 4096 and
                      0 < r.get('scratch_peak_bytes',0) <= 2*4096*1024**2)) for r in rows))

    if not all(valid(groups[(*baseline, satellite)]) for satellite in satellites):
        raise ValueError('Three verified baseline repetitions on both satellites are required')
    base = {s: median(r['wall_seconds'] for r in groups[(*baseline, s)]) for s in satellites}
    candidates = []
    for mode, processes, threads in sorted({(r.get('read_mode','range'),r['read_processes'],r['source_threads']) for r in report['rows']}):
        if (mode, processes, threads) not in READER_CONFIGS:
            continue
        if (mode, processes, threads) == baseline:
            continue
        if not all(valid(groups[(mode, processes, threads, s)]) for s in satellites):
            continue
        gains = {str(s): 1 - median(r['wall_seconds'] for r in groups[(mode, processes, threads, s)])/base[s]
                 for s in satellites}
        if min(gains.values()) >= minimum_gain:
            candidates.append((min(gains.values()), processes, threads, gains, mode))
    if candidates:
        gain, processes, threads, gains, mode = max(candidates, key=lambda r: (r[0], -r[1], -r[2]))
        return {'read_mode': mode, 'download_concurrency': 8, 'staging_mib':4096,
                'read_processes': processes, 'source_threads': threads,
                'per_satellite_wall_time_reduction': gains, 'minimum_gain': minimum_gain,
                'memory_limit_bytes': memory_limit_bytes, 'reason': 'verified gain on both satellites'}
    return {'read_mode':'range','download_concurrency':8,'staging_mib':4096,
            'read_processes': 2, 'source_threads': 8, 'minimum_gain': minimum_gain,
            'memory_limit_bytes': memory_limit_bytes, 'reason': 'no qualifying gain; retain baseline'}
