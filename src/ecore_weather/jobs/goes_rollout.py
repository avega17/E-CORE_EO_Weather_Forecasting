"""Production timing summaries and measured storage preflight."""
from __future__ import annotations
from pathlib import Path

def storage_preflight(suite,choice,destination,scratch):
    """Reserve space for admitted native stores using measured crop sizes.

    This is a conservative working-space check, not a full-study storage forecast.
    Keep the observed per-band sizes separate from listed full-source bytes.
    """
    import shutil
    from statistics import median
    samples={int(b):[value] for b,value in choice.get('empirical_roi_bytes_per_observation',{}).items()}
    for row in suite['rows']:
        if row['stage'] not in ('final','writer_final','confirm','writer_confirm'):continue
        for archive in row.get('monthly_metrics',[]):
            if archive.get('observations',0)>0 and archive.get('stored_bytes',0)>0:
                samples.setdefault(archive['band'],[]).append(archive['stored_bytes']/archive['observations'])
    if set(samples)!={1,2,3,7,8,9,10,13}:
        raise ValueError('Storage preflight requires measured native crops for all eight bands')
    per_band={str(band):median(values) for band,values in samples.items()}
    month_bytes=int(sum(per_band.values())*31*24*6*1.5)
    config=choice.get('global_config',{})
    owners=config.get('month_writers',2)+config.get('tail_months',0)
    scratch=Path(scratch).resolve();scratch.mkdir(parents=True,exist_ok=True)
    stage=config.get('staging_mib',8192)*1024**2
    # Open stores and packed candidates may coexist during verification.
    scratch_required=2*owners*month_bytes+stage
    destination_required=owners*month_bytes
    scratch_free=shutil.disk_usage(scratch).free
    destination_free=shutil.disk_usage(destination).free
    if scratch_free<scratch_required:
        raise OSError(f'Linux scratch needs {scratch_required} bytes; only {scratch_free} available')
    if destination_free<destination_required:
        raise OSError(f'DAS needs {destination_required} bytes for admitted months; only {destination_free} available')
    return {'measured_compressed_bytes_per_observation_by_band':per_band,
        'month_bytes_with_50pct_margin':month_bytes,'month_slots_including_tails':owners,
        'scratch_path':str(scratch),'scratch_required_bytes':scratch_required,
        'scratch_available_bytes':scratch_free,'destination_required_bytes':destination_required,
        'destination_available_bytes':destination_free,
        'scope':'31-day, six scans/hour native crop; admitted work only; empirical benchmark ZIP sizes'}

def completion_summary(row):
    """Derive bounded-case completion and tail timings from archive receipts.

    Offsets start at the earliest writer attempt, not the coordinator's start.
    C02-only remaining time is distinct from occupancy of a granted tail slot.
    """
    groups={};starts=[];ends=[]
    for archive in row.get('monthly_metrics',[]):
        start=archive.get('attempt_started_at')
        duration=archive.get('write_seconds')
        if start is None or duration is None:continue
        end=archive.get('attempt_ended_at',start+duration)
        starts.append(start);ends.append(end)
        groups.setdefault(archive['month'],{})[archive['band']]=end
    if not starts:return {'completion_timing_scope':'unavailable'}
    tails={month:max(0,bands[2]-max(v for band,v in bands.items() if band!=2))
        for month,bands in groups.items() if 2 in bands and len(bands)>1}
    complete=[max(bands.values()) for bands in groups.values() if set(bands)=={1,2,3,7,8,9,10,13}]
    return {'first_band_seconds_from_first_writer':min(ends)-min(starts),
        'first_month_seconds_from_first_writer':min(complete)-min(starts) if complete else None,
        'c02_only_remaining_seconds_by_month':tails,
        'completion_timing_scope':'writer-attempt timestamps; bounded sample months; tail-slot occupancy not inferred'}

def production_review(month,choice):
    """Compact first verified production-month evidence, without extrapolated gains."""
    archives=month['archives'];elapsed=month.get('wall_seconds',0)
    return {'status':'verified','start':month['start'],'product':month['product'],
        'global_config':month['global_config'],'benchmark_reference':choice.get('benchmark_reference',{}),
        'attempt_wall_seconds':elapsed,'attempt_returned_bytes':month['read_bytes'],
        'attempt_average_mbps':month['read_bytes']*8/elapsed/1e6 if elapsed else None,
        'compressed_zip_bytes':month['stored_bytes'],
        'bands':[{'band':a['band'],'observations':a['observations'],
            'attempt_seconds':a.get('write_seconds'),'attempt_returned_bytes':a.get('read_bytes'),
            'resumed_observations':a.get('resumed_observations',0),
            'compressed_zip_bytes':a['stored_bytes'],'retries':a.get('source_retries',0)} for a in archives],
        'comparison_scope':'production month shares global resources with other months; bounded benchmark timings are not whole-month forecasts or matched speedup measurements'}

