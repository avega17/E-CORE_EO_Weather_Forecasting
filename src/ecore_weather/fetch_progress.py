"""Fresh progress from durable selections/manifests and optional scan checkpoints."""
from collections import defaultdict
from datetime import datetime,timezone
import json
import re
from pathlib import Path
import pandas as pd
from .common import utc


def _json(path):
    try:return json.loads(Path(path).read_text())
    except (FileNotFoundError,json.JSONDecodeError,OSError):return None


def collect_progress(results='results',destination='/mnt/p/ecore_eo_datasets',source='goes',product=None,start=None,end=None):
    from .catalog import load_selection
    from .monthly import _band
    results=Path(results);objects={};catalogs=set();index_state='manifests'
    try:
        from .index import connect
        with connect(results/'archive_index.duckdb',read_only=True) as db:
            catalogs.update(Path(r[0]) for r in db.execute(
                'SELECT catalog_path FROM selections WHERE source=? AND catalog_path IS NOT NULL',[source]).fetchall())
        index_state='read-only index plus manifests'
    except Exception:
        pass  # File absent or coordinator lock: durable selections remain usable.
    for root in results.glob('study-*'):
        if root.is_dir() and root.name not in ('study-code-snapshots','study-scratch'):
            catalogs.update(root.rglob('collection.json'))
            catalogs.update(root.rglob('items.json.gz'))
            catalogs.update(root.rglob('items.json'))
    def eligible(p,t):
        return (not product or p==product) and (not start or utc(t)>=utc(start)) and (not end or utc(t)<utc(end))
    for path in sorted(catalogs):
        if not path.is_file():continue
        month=next((part for part in reversed(path.parts) if re.fullmatch(r'\d{4}-\d{2}',part)),None)
        if month and ((start and month<str(start)[:7]) or (end and month>str(end)[:7])):
            continue
        if product and path.parent.name.startswith('ABI-') and path.parent.name!=product:
            continue
        try:s=load_selection(path)
        except (OSError,ValueError,KeyError,TypeError):continue
        if s.source!=source or (product and s.product!=product):continue
        for asset in s.assets:
            if not eligible(s.product,asset.time):continue
            key=(s.product,asset.id)
            objects.setdefault(key,{'product':s.product,'satellite':s.satellite,'month':asset.time[:7],
                'band':_band(asset) if source=='goes' else None,'asset_id':asset.id,
                'state':'pending','checkpoint_at':None})
    base=Path(destination)/source
    prefix=product or '*'
    patterns=([f'{prefix}/*/roi-*/????/??/complete.json',
               f'{prefix}/*/C??/roi-*/????/??/complete.json'] if source=='goes' else
              [f'{prefix}/roi-*/????/??/complete.json'])
    markers=set(path for pattern in patterns for path in base.glob(pattern))
    for path in sorted(markers):
        if product and product not in path.parts:continue
        if len(path.parts)>3 and re.fullmatch(r'\d{4}',path.parts[-3]) and re.fullmatch(r'\d{2}',path.parts[-2]):
            month=path.parts[-3]+'-'+path.parts[-2]
            if (start and month<str(start)[:7]) or (end and month>str(end)[:7]):continue
        marker=_json(path)
        if not marker or marker.get('source')!=source:continue
        p=marker.get('product')
        for asset in marker.get('assets',[]):
            t=asset.get('time') or asset.get('actual_time')
            aid=asset.get('asset_id') or asset.get('id')
            if not t or not aid or not eligible(p,t):continue
            key=(p,aid)
            row=objects.setdefault(key,{'product':p,'satellite':marker.get('satellite'),'month':t[:7],
                'band':marker.get('band'),'asset_id':aid,'state':'pending','checkpoint_at':None})
            row['state']='corrupt_source' if asset.get('status')=='corrupt_source' else 'archived'
    for path in (results/'study-scratch').glob('*/progress.json'):
        progress=_json(path)
        if not progress:continue
        ids=progress.get('asset_ids',[])
        count=progress.get('next_index',0)
        if not isinstance(ids,list) or not 0<=count<=len(ids):continue
        p=progress.get('product')
        try:stamp=datetime.fromtimestamp(path.stat().st_mtime,timezone.utc).isoformat()
        except FileNotFoundError:continue
        for aid in ids[:count]:
            if isinstance(aid,dict):aid=aid.get('asset_id') or aid.get('id')
            keys=[(p,aid)] if p else [key for key in objects if key[1]==aid]
            for key in keys:
                row=objects.get(key)
                if row and row['state']=='pending':row.update(state='checkpointed',checkpoint_at=stamp)
    grouped=defaultdict(list)
    for row in objects.values():grouped[(row['product'],row['satellite'],row['month'],row['band'])].append(row)
    report=[]
    for (p,sat,month,band),rows in grouped.items():
        counts={state:sum(r['state']==state for r in rows) for state in ('archived','corrupt_source','checkpointed','pending')}
        total=len(rows);accounted=total-counts['pending']
        report.append({'source':source,'product':p,'satellite':sat,'month':month,'band':band,
            'selected_objects':total,'archived_objects':counts['archived'],
            'corrupt_source_objects':counts['corrupt_source'],'checkpointed_objects':counts['checkpointed'],
            'remaining_objects':counts['pending'],'accounted_pct':round(100*accounted/total,2),
            'state':'complete' if accounted==total and not counts['checkpointed'] else
                    'in_progress' if counts['checkpointed'] else 'pending',
            'last_checkpoint_utc':max((r['checkpoint_at'] for r in rows if r['checkpoint_at']),default=None)})
    columns=['source','product','satellite','month','band','selected_objects','archived_objects',
        'corrupt_source_objects','checkpointed_objects','remaining_objects','accounted_pct','state','last_checkpoint_utc']
    return pd.DataFrame(report,columns=columns).sort_values(['product','month','band'],na_position='first'),index_state


def register_progress(db,**kwargs):
    """Refresh a stable DuckDB relation; no SQL references to ephemeral files."""
    frame,state=collect_progress(**kwargs)
    db.register('fetch_progress',frame)
    db.register('fetch_jobs',collect_jobs(kwargs.get('results','results')))
    return state


def collect_jobs(results='results'):
    """Report recorded launch states alongside current process evidence."""
    import psutil
    processes=[]
    for process in psutil.process_iter(['pid','cmdline']):
        try:
            command=process.info['cmdline'] or []
            if command and 'python' in Path(command[0]).name:
                processes.append((process.pid,command))
        except (psutil.AccessDenied,psutil.NoSuchProcess):pass
    from .jobs import status
    current=status(Path(results)/'archive_index.duckdb')
    rows=[{'run':r['run_id'],'recorded_status':r['status'],'process_alive':r['process_alive'],'pids':str(r['pid']),'error':None} for r in current.get('runs',[])]
    for path in Path(results).glob('study-*/launch.json'):
        state=_json(path)
        if not state:continue
        snapshot=state.get('snapshot',{}).get('snapshot')
        matches=[pid for pid,command in processes if snapshot and any(
            str(snapshot) in argument and Path(argument).name in
            ('fetch_goes_staged.py','benchmark_goes_pipeline.py','run_report_network_checks.py')
            for argument in command)]
        rows.append({'run':path.parent.name,'recorded_status':state.get('status'),
            'process_alive':bool(matches),'pids':','.join(map(str,matches)),
            'error':state.get('error')})
    return pd.DataFrame(rows,columns=['run','recorded_status','process_alive','pids','error'])
