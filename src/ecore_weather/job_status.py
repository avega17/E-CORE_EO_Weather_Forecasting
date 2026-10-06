"""Read-only fetch monitoring with compact, explicitly scoped rate samples."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
import time


def filter_tasks(tasks, source=None, start=None, end=None, product=None, band=None):
    """Date filters select intersecting UTC months, never partial-month counts."""
    from .common import utc
    lower=utc(start) if start else None
    upper=utc(end) if end else None
    if lower and upper and lower>=upper:raise ValueError('Start must precede the exclusive end.')
    result=[]
    for task in tasks:
        if source and task['source']!=source:continue
        if product and task['product']!=product:continue
        if band is not None and task['band']!=band:continue
        month=task.get('month')
        if (lower or upper) and not month:continue
        if month:
            begin=utc(month+'-01')
            stop=begin.replace(year=begin.year+(begin.month==12),month=1 if begin.month==12 else begin.month+1)
            if lower and stop<=lower or upper and begin>=upper:continue
        result.append(task)
    return result


def observe(data, cache, now):
    """Average actual counter deltas over the monitor's observed window.

    The cache contains one aggregate sample per archive task, no source objects.
    It is separate from the coordinator's database and cannot alter fetch state.
    """
    runs={run['run_id']:run for run in data.get('runs',[])}
    samples=cache.setdefault('samples',{})
    active_months={(t.get('source'),t.get('product'),t.get('month'))
        for t in data.get('tasks',[]) if runs.get(t.get('run_id'),{}).get('status')=='running'}
    for task in data.get('tasks',[]):
        metrics=task.get('metrics',{})
        counter=metrics.get('read_bytes')
        run=runs.get(task.get('run_id'),{})
        complete=task['status']=='complete'
        state=task['status']
        if not complete and state!='queued' and run.get('status') in ('interrupted','paused','failed'):
            state=run['status']
        elif state=='checkpointed':state='fetching' if run.get('process_alive') else 'checkpointed'
        # Reused scratch retains the previous attempt's metrics until this band
        # is admitted. A new run heartbeat does not mean that band is reading.
        old_attempt=(not complete and run.get('status')=='running'
            and metrics.get('attempt_started_at') is not None
            and run.get('started_at') is not None
            and metrics['attempt_started_at'] < run['started_at'])
        recovered_wait=(not complete and run.get('status') in ('failed','paused','interrupted')
            and (task.get('source'),task.get('product'),task.get('month')) in active_months)
        if old_attempt or recovered_wait:state='waiting'
        task['display_status']=state
        task.update(elapsed_seconds=None,average_mbps=None,returned_bytes=counter,timing_scope='unavailable')
        if old_attempt or recovered_wait:
            task['timing_scope']='prior attempt'
            continue
        attempt=task.get('attempt_metrics',{})
        if complete and attempt.get('write_seconds',0)>0:
            elapsed=attempt['write_seconds'];returned=attempt.get('read_bytes')
            task.update(elapsed_seconds=elapsed,returned_bytes=returned,timing_scope='attempt',
                average_mbps=returned*8/elapsed/1e6 if returned is not None else None)
            continue
        if metrics.get('attempt_started_at') is not None and metrics.get('attempt_read_bytes') is not None:
            elapsed=metrics.get('attempt_elapsed_seconds',max(0,now-metrics['attempt_started_at']))
            returned=metrics['attempt_read_bytes']
            task.update(elapsed_seconds=elapsed,returned_bytes=returned,timing_scope='attempt',
                average_mbps=returned*8/elapsed/1e6 if elapsed>0 else None)
            continue
        if state=='queued' or counter is None:continue
        key=task['task_id']
        old=samples.get(key)
        # New run, reset counter, or a resumed sample means a new monitor window.
        if complete and old is None:continue  # Never invent a zero-second completed runtime.
        if old is None or old['run_id']!=task.get('run_id') or counter<old['last_bytes'] or (old.get('stopped') and state=='fetching'):
            old={'run_id':task.get('run_id'),'first_seen':now,'base_bytes':counter,'last_bytes':counter,'last_seen':now}
            samples[key]=old
        if old.get('stopped'):
            until=old['last_seen']
        else:
            until=now
            old.update(last_seen=now,last_bytes=counter)
            if complete or state in ('interrupted','paused','failed'):old['stopped']=True
        elapsed=max(0,until-old['first_seen'])
        task.update(elapsed_seconds=elapsed,timing_scope='watch',
            average_mbps=(old['last_bytes']-old['base_bytes'])*8/elapsed/1e6 if elapsed>0 else None)
    # Keep at most the latest 2,000 archive-level samples; no unbounded history.
    if len(samples)>2000:
        cache['samples']=dict(sorted(samples.items(),key=lambda kv:kv[1]['last_seen'])[-2000:])
    return data


def duration(seconds):
    if seconds is None:return '--'
    value=max(0,int(seconds));hours,left=divmod(value,3600);minutes,seconds=divmod(left,60)
    return f'{hours:02d}:{minutes:02d}:{seconds:02d}'


def completed_manifest_status(source=None, roots=None):
    """Read compact durable month receipts when no index snapshot is available.

    Never traverse scratch or source-object selections. Counts cover verified
    completed months only; live progress and unrecorded timings stay unknown.
    """
    if roots is None:
        roots={'goes':{Path('results/study-goes-native')},'mrms':{Path('results/study-mrms')}}
        for path in Path('results/runs').glob('*/run.json'):
            try:
                config=json.loads(path.read_text()).get('config',{})
                kind=config.get('source');arguments=config.get('arguments',[])
                if kind in roots and '--output' in arguments:
                    roots[kind].add(Path(arguments[arguments.index('--output')+1]))
            except (OSError,ValueError,IndexError):continue
    tasks={}
    for kind,folders in roots.items():
        if source and kind!=source:continue
        for folder in folders:
            pattern='checkpoints/*/*.json' if kind=='goes' else 'months/*/*.json'
            for path in Path(folder).glob(pattern):
                try:
                    receipt=json.loads(path.read_text())
                    if receipt.get('status')!='complete':continue
                    for archive in receipt.get('archives',[]):
                        target=Path(archive['path'])
                        if not target.is_file() or not (target.parent/'complete.json').is_file():continue
                        count=archive.get('observations',0)
                        row={'task_id':str(target),'run_id':'durable-month-manifest',
                            'source':kind,'product':receipt.get('product'),
                            'month':receipt.get('start',receipt.get('month',''))[:7],
                            'band':archive.get('band'),'selected':count,'checkpointed':count,
                            'completed':count,'status':'complete','updated_at':path.stat().st_mtime,
                            'metrics':archive}
                        if archive.get('write_seconds',0)>0:row['attempt_metrics']=archive
                        tasks[str(target)]=row
                except (OSError,ValueError,KeyError):continue
    return {'runs':[],'tasks':sorted(tasks.values(),key=lambda row:(row['month'],row['product'] or '',row['band'] or 0)),
        'fallback_scope':'durable completed month manifests only; live progress unavailable during lock'}


def render(data, now):
    stamp=datetime.fromtimestamp(now,timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    lines=[f'Fetch status | {stamp}']
    for run in data.get('runs',[])[:5]:
        age=max(0,now-(run.get('heartbeat') or now))
        started=run.get('started_at')
        elapsed=(run.get('ended_at') or now)-started if started is not None else None
        lines.append(f"Run {run['run_id'][:12]}  {run['status']}  PID {run['pid']}  elapsed {duration(elapsed)}  heartbeat {int(age)}s ago")
    if data.get('status')=='index_unavailable':
        lines.append('Index temporarily unavailable: '+data.get('reason',''))
    if data.get('cached_at') is not None:
        lines.append(f"Showing cached snapshot ({int(max(0,now-data['cached_at']))}s old); rates are not refreshed.")
    if data.get('fallback_scope'):lines.append(data['fallback_scope'])
    def timestamp(value):
        return datetime.fromtimestamp(value,timezone.utc).strftime('%m-%d %H:%M:%S') if value is not None else '--'
    rows=[]
    for task in data.get('tasks',[]):
        source=task['source'].upper()
        sat=re.search(r'/goes(\d+)/',task.get('task_id',''))
        if sat:source+=sat[1]
        product=task['product'] or '--'
        if product.startswith('ABI-L2-'):product=product.removeprefix('ABI-L2-')
        done=max(task.get('checkpointed') or 0,task.get('completed') or 0);total=task.get('selected') or 0
        byte_count=task.get('returned_bytes')
        rate=task.get('average_mbps')
        rows.append([task.get('month') or '--',source,product,
            f"C{task['band']:02d}" if task.get('band') is not None else '--',
            f'{done:,}/{total:,}',f'{100*done/total:.1f}%' if total else '--',
            task.get('display_status',task['status']),task.get('metrics',{}).get('writer_role','--'),duration(task.get('elapsed_seconds')),
            f'{rate:.1f}' if rate is not None else '--',
            f'{byte_count/1024**3:.2f}' if byte_count is not None else '--',
            timestamp(task.get('metrics',{}).get('attempt_started_at')),timestamp(task.get('updated_at'))])
    headers=['Month','Source','Product','Band','Verified/Selected','Done','State','Role','Elapsed','Avg Mb/s','Returned GiB','Started UTC','Updated UTC']
    if rows:
        widths=[max(len(str(row[i])) for row in [headers,*rows]) for i in range(len(headers))]
        def line(row):return '  '.join(str(value).ljust(width) for value,width in zip(row,widths))
        lines.extend([line(headers),line(['-'*width for width in widths]),*(line(row) for row in rows)])
    else:lines.append('No matching band-month or product-month tasks.')
    lines.extend(['','Elapsed and speed: attempt = recorded writer wall time (last attempt, including crop/write/verify);',
        'watch = time and byte change since monitoring began, including waits. Not summed parallel download time.',
        'Returned GiB: attempt bytes, or cumulative build counter for watch/unavailable. Watch rates use its change.',
        'Counts and counters may lag up to 30s. Date filters select whole intersecting months; end is exclusive.'])
    return '\n'.join(lines)


def main(argv=None, *, index_path=None, source=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start',help='UTC date/time; includes intersecting month tasks')
    parser.add_argument('--end',help='Exclusive UTC date/time; includes intersecting month tasks')
    parser.add_argument('--product',help='Exact NOAA product name')
    parser.add_argument('--band',type=int,help='GOES band number')
    parser.add_argument('--json',action='store_true',help='Machine-readable output instead of the default table')
    parser.add_argument('--cache',help='Small independent monitor cache; default beside the index')
    args=parser.parse_args(argv)
    from .index import database_path
    from .jobs import status
    filename=database_path(index_path)
    cachefile=Path(args.cache) if args.cache else filename.with_name('status-monitor.json')
    try:cache=json.loads(cachefile.read_text())
    except (OSError,ValueError):cache={}
    identity=hashlib.sha256(str(filename).encode()).hexdigest()
    if cache.get('index_identity')!=identity:cache={'index_identity':identity}
    now=time.time();data=status(index_path)
    if data.get('status')=='index_unavailable':
        # Never write the fetch database or walk per-source inventories during a lock.
        if cache.get('snapshot'):
            data={**cache['snapshot'],**data,'cached_at':cache['snapshot_at']}
        else:
            fallback=completed_manifest_status(source)
            data={**data,**observe(fallback,{},now)}
    else:
        data=observe(data,cache,now)
        cache.update(snapshot=data,snapshot_at=now)
        temporary=None
        try:
            cachefile.parent.mkdir(parents=True,exist_ok=True)
            with tempfile.NamedTemporaryFile(mode='w',dir=cachefile.parent,delete=False) as file:
                temporary=Path(file.name)
                json.dump(cache,file,separators=(',',':'),allow_nan=False)
            os.replace(temporary,cachefile)
        except OSError as exc:data['cache_warning']=str(exc)
        finally:
            if temporary is not None:temporary.unlink(missing_ok=True)
    try:data['tasks']=filter_tasks(data.get('tasks',[]),source,args.start,args.end,args.product,args.band)
    except ValueError as exc:parser.error(str(exc))
    print(json.dumps(data,indent=2,default=str) if args.json else render(data,now))
    if data.get('cache_warning') and not args.json:print('Monitor cache unavailable; averages require a writable --cache path.')
    return 0
