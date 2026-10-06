"""Production launch policy based on repeated verified archive timings."""
from statistics import median
from pathlib import Path


def ensure_paused(processes=None):
    """Reject controlled tests while a NOAA study fetch is active."""
    if processes is None:
        import psutil
        processes=[]
        for process in psutil.process_iter(['pid','cmdline']):
            try:processes.append(process.info)
            except psutil.Error:continue
    active=[]
    for process in processes:
        cmd=process.get('cmdline') or []
        if (any(Path(arg).name in ('01_mrms.py','02_goes.py') for arg in cmd) and 'fetch' in cmd) or any(Path(arg).name=='fetch_goes_staged.py' for arg in cmd) or (any(Path(arg).name=='dataset_jobs.py' for arg in cmd) and any(arg in ('fetch','resume') for arg in cmd)):
            active.append(process['pid'])
    if active:raise RuntimeError(f'Controlled benchmark requires verified fetch pause; active coordinators: {active}')


def select_profiles(rows,baseline,minimum_gain=.10,memory_limit=8*1024**3,stage="confirm",bands=(1,2,3,7,8,9,10,13)):
    """Use per-band archive readiness; fallback requires no extrapolated speed claim."""
    import json
    selected={}
    profiles={json.dumps(r['profile'],sort_keys=True):r['profile'] for r in rows if r.get('stage')==stage}
    baseline_key=json.dumps(baseline,sort_keys=True)
    def cases(profile,satellite):return [r for r in rows if r.get('stage')==stage and r['satellite']==satellite and r['profile']==profile]
    def timings(profile,satellite,band):
        result=[]
        for r in cases(profile,satellite):
            if not r.get('values_equal') or r.get('peak_rss_bytes',0)>memory_limit or r.get('peak_rss_bytes',0)<=0:return []
            metrics=[m for m in r['monthly_metrics'] if m.get('band')==band and m.get('status')=='saved']
            if len(metrics)!=1:return []
            result.append(metrics[0]['write_seconds'])
        return result
    for band in bands:
        if not all(len({r['repeat'] for r in cases(baseline,s)})>=3 and len(timings(baseline,s,band))>=3 for s in (16,19)):
            raise ValueError(f'Band C{band:02} requires three verified baseline repetitions on both satellites')
        candidates=[]
        for key,profile in profiles.items():
            if key==baseline_key:continue
            if not all(len({r['repeat'] for r in cases(profile,s)})>=3 and len(timings(profile,s,band))>=3 for s in (16,19)):continue
            gains={str(s):1-median(timings(profile,s,band))/median(timings(baseline,s,band)) for s in (16,19)}
            if min(gains.values())>=minimum_gain:candidates.append((min(gains.values()),profile,gains))
        winner=max(candidates,key=lambda c:c[0]) if candidates else None
        selected[str(band)]={'profile':winner[1] if winner else baseline,'gain':winner[2] if winner else {},
            'reason':'verified per-band readiness gain on both satellites' if winner else 'no qualifying gain; retain current profile',
            'measurement_scope':stage+' matched native scans; close/pack/verify included; best tested configuration'}
    return selected


def validate_profiles(profiles):
    allowed = {'read_mode','download_concurrency','read_processes','prefetch_mib','staging_mib','workers','monthly_writers','block_size'}
    for band, entry in profiles.items():
        if not 1 <= int(band) <= 16: raise ValueError('Invalid ABI band profile')
        profile = entry.get('profile', entry)
        if set(profile)-allowed: raise ValueError('Unknown read profile settings')
        if profile.get('read_mode','range') not in ('range','async_full','async_pipeline'): raise ValueError('Invalid read mode')
        if any(int(v)<1 for k,v in profile.items() if k!='read_mode'): raise ValueError('Profile limits must be positive')
