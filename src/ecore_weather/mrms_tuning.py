"""Bounded, repeatable MRMS machine-profile comparisons on real NOAA selections."""
from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import replace, asdict
import gzip
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import statistics
import shutil
import tempfile
import time

from . import mrms
from .common import PeakMemory, PR_BBOX, write_json
from .jobs_mrms import _fetch_selection
from .mrms_profile import resolve, hardware, code_identity, job_memory_limit
from .transfer_budget import Budget

PRODUCTS=('MergedReflectivityQCComposite_00.50','MergedBaseReflectivityQC_00.50')


def resource_key(resources):
    return hashlib.sha256(json.dumps(resources,sort_keys=True).encode()).hexdigest()[:12]


def choose(rows):
    """Prefer fewer resources within five percent of the fastest measured median."""
    grouped={}
    for row in rows:
        if row.get('status')=='complete' and row.get('equal'):
            grouped.setdefault(resource_key(row['resources']),[]).append(row)
    candidates=[]
    for group in grouped.values():
        resources=group[0]['resources']
        candidates.append((statistics.median(r['wall_seconds'] for r in group),resources))
    if not candidates:
        raise ValueError('No verified configurations')
    fastest=min(t for t,_ in candidates)
    return min((r for t,r in candidates if t<=1.05*fastest),
               key=lambda r:(r['monthly_writers']*r['workers'],r['monthly_writers']*r['decode_workers'],
                             r['roi_queue_mib']*r['monthly_writers'],r['max_inflight']))


def finalists(rows):
    """Repeat the resource-preferred candidate and its fastest alternative."""
    preferred = choose(rows)
    unique = {resource_key(r['resources']): r['resources'] for r in rows
              if r.get('status') == 'complete' and r.get('equal')}
    alternatives = [r for r in unique.values() if r != preferred]
    if not alternatives:
        return [preferred]
    fastest = min(alternatives, key=lambda resources: statistics.median(
        r['wall_seconds'] for r in rows if r['resources'] == resources))
    return [preferred, fastest]


def selections(output, frames):
    cache=Path(output)/'selection.json.gz'
    if cache.exists():
        from .common import Asset,Selection
        with gzip.open(cache,'rt') as stream:data=json.load(stream)
        if data['frames']!=frames:
            raise ValueError('Existing tuning selection uses a different frame count')
        result=[]
        for row in data['selections']:
            row['assets']=[Asset(**a) for a in row['assets']]
            result.append(Selection(**row))
        return result
    result=[]
    for month in (1,3,5,7,9,11):
        for product in PRODUCTS:
            selection=mrms.discover(f'2022-{month:02d}-10',f'2022-{month:02d}-14',PR_BBOX,product)
            if len(selection.assets)<frames:
                raise ValueError(f'Insufficient matched observations: {product} 2022-{month:02d}')
            assets=selection.assets[:frames]
            ids={a.id for a in assets}
            matches=tuple(r for r in selection.hourly_matches if r['asset_id'] in ids)
            result.append(replace(selection,assets=assets,hourly_matches=matches,
                                  expected_times=tuple(r['slot_time'] for r in matches)))
    payload={'frames':frames,'selections':[asdict(s) for s in result]}
    cache.parent.mkdir(parents=True,exist_ok=True)
    temporary=cache.with_suffix('.tmp')
    temporary.write_bytes(gzip.compress(json.dumps(payload,sort_keys=True).encode(),mtime=0))
    os.replace(temporary,cache)
    return result


def run_case(selected,resources,destination,scratch,reference,progress=None):
    """Fresh application stores; includes worker startup, packing and destination verification."""
    from .storage import open_raw,fingerprint_streaming,metadata_fingerprint
    target=Path(tempfile.mkdtemp(prefix='mrms-tune-',dir=destination))
    working=Path(tempfile.mkdtemp(prefix='mrms-tune-',dir=scratch))
    success=False
    try:
        began=time.perf_counter()
        with PeakMemory() as memory,job_memory_limit(resources['memory_gib']):
            with ProcessPoolExecutor(max_workers=resources['monthly_writers'],
                    mp_context=multiprocessing.get_context('spawn')) as pool:
                futures={pool.submit(_fetch_selection,s,target,resources['workers'],working,
                    resources['decode_workers'],resources['max_inflight'],resources['roi_queue_mib']):s
                    for s in selected}
                reports=[]
                for f in as_completed(futures):
                    reports.append((futures[f],f.result()))
                    if progress:progress('archives_verified',len(reports),len(futures))
            elapsed=time.perf_counter()-began
        signatures={}
        metrics=[]
        for task_number,(selection,report) in enumerate(reports,1):
            for row in report['monthly_archives']:
                if row['status']!='saved':
                    raise IOError(f'Unverified tuning archive: {row}')
                with open_raw(row['path']) as ds:
                    signatures[selection.id+'|'+row['month']]=(fingerprint_streaming(ds),metadata_fingerprint(ds))
                metrics.append({k:v for k,v in row.items() if k not in {'assets','path'}})
            if progress:progress('content_compared',task_number,len(reports))
        if reference and signatures!=reference:
            raise AssertionError('Tuning changed source values, coordinates, quality or metadata')
        row={'status':'complete','equal':True,'resources':resources,'wall_seconds':elapsed,
             'comparison_hash_seconds':time.perf_counter()-began-elapsed,
             'returned_bytes':sum(r['read_bytes'] for _,r in reports),
             'stored_bytes':sum(r['stored_bytes'] for _,r in reports),
             'peak_job_rss_bytes':memory.peak,'monthly_metrics':metrics,
             'scope':'fresh application stores; startup through destination verification; comparison hashing separate; filesystem cache not flushed'}
        success=True
        return row,signatures
    except Exception as exc:
        write_json(target/'failure.json',{'error':str(exc),'scratch':str(working)})
        raise
    finally:
        if success:
            shutil.rmtree(target)
            shutil.rmtree(working)


def main(argv=None):
    from .jobs_policy import ensure_paused
    ensure_paused()
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--tune-profile',required=True)
    p.add_argument('--output',default='results/benchmarks/mrms-polaris')
    p.add_argument('--destination',required=True,help='Isolated benchmark archive parent; never the research root')
    p.add_argument('--scratch',required=True)
    p.add_argument('--budget-gib',type=float,default=10)
    p.add_argument('--max-minutes',type=float,default=45)
    p.add_argument('--frames',type=int,default=512)
    args=p.parse_args(argv)
    if min(args.budget_gib,args.max_minutes,args.frames)<=0:p.error('Positive limits required')
    output=Path(args.output);output.mkdir(parents=True,exist_ok=True)
    destination=Path(args.destination);destination.mkdir(parents=True,exist_ok=True)
    scratch=Path(args.scratch);scratch.mkdir(parents=True,exist_ok=True)
    config=resolve(argparse.Namespace(profile=args.tune_profile))
    revision,snapshot=code_identity()
    selected=selections(output,args.frames)
    selection_digest=hashlib.sha256(json.dumps([s.id for s in selected]).encode()).hexdigest()
    suite_path=output/'suite.json'
    previous=json.loads(suite_path.read_text()) if suite_path.exists() else {}
    if previous and (previous.get('revision')!=revision or previous.get('selection_digest')!=selection_digest):
        raise ValueError('Tuning evidence belongs to another revision/selection; choose a new output')
    rows=previous.get('rows',[])
    reference={k:tuple(v) for k,v in previous.get('signatures',{}).items()}
    budget=Budget(output/'transfer-budget.json',int(args.budget_gib*1024**3))
    if budget.summary()['limit']!=int(args.budget_gib*1024**3):raise ValueError('Do not change a resumed byte budget')
    started=time.monotonic();deadline=started+args.max_minutes*60
    original_budget=os.environ.get('ECORE_TRANSFER_BUDGET')
    os.environ['ECORE_TRANSFER_BUDGET']=str(budget.path.resolve())
    summary={'schema':1,'status':'running','revision':revision,'snapshot_sha256':snapshot,
        'hardware':hardware(),'source_profile':config,'selection_digest':selection_digest,
        'rows':rows,'signatures':reference,'assumptions':'Identical CARIB objects and ROI; fresh application caches; no claim that OS caches are flushed'}
    def save():
        summary.update(rows=rows,signatures=reference,budget=budget.summary())
        write_json(suite_path,summary)
    def case(resources,stage,repeat=1):
        nonlocal reference
        identity=(resource_key(resources),stage,repeat)
        if any((resource_key(r['resources']),r['stage'],r['repeat'])==identity and r.get('status')=='complete' for r in rows):
            return
        if time.monotonic()>=deadline or budget.summary()['charged']+sum(a.size for s in selected for a in s.assets)>budget.summary()['limit']:
            raise TimeoutError('Tuning limit reached; resume the same suite without resetting its byte budget')
        print(f'MRMS tuning {stage} {repeat}: {resources}',flush=True)
        def progress(phase,completed,total):
            summary['active_case']={'resources':resources,'stage':stage,'repeat':repeat,
                                    'phase':phase,'completed_tasks':completed,'total_tasks':total}
            save()
            print(f'MRMS tuning {phase}: {completed}/{total} product-month tasks',flush=True)
        progress('starting',0,len(selected))
        row,signatures=run_case(selected,resources,destination,scratch,reference,progress)
        summary.pop('active_case',None)
        reference=reference or signatures
        rows.append({**row,'stage':stage,'repeat':repeat})
        save()
    try:
        base=dict(config['resources'])
        for writers in (4,8,12):case({**base,'monthly_writers':writers},'writers')
        best=choose(rows)
        for downloads in (4,16):case({**best,'workers':downloads},'downloads')
        best=choose(rows)
        best_rows=[r for r in rows if r['resources']==best]
        decode_wait=sum(m.get('summed_decode_wait_seconds',0) for r in best_rows for m in r['monthly_metrics'])
        if decode_wait>.25*sum(r['wall_seconds']*best['monthly_writers'] for r in best_rows):
            case({**best,'decode_workers':2},'decode')
            best=choose(rows)
        waits=sum(m.get('source_wait_seconds',0) for r in rows if r['resources']==best for m in r['monthly_metrics'])
        if waits>.1*sum(r['wall_seconds']*best['monthly_writers'] for r in rows if r['resources']==best):
            for depth,queue in ((8,128),(32,512)):
                case({**best,'max_inflight':depth,'roi_queue_mib':queue},'queue')
        ranked=finalists(rows)
        for repeat in (1,2,3):
            for resources in ranked if repeat%2 else list(reversed(ranked)):
                case(resources,'final',repeat)
        finals=[r for r in rows if r['stage']=='final']
        selected_resources=choose(finals)
        summary.update(status='complete',selected_resources=selected_resources,
            reason='Best tested median verified archive runtime; prefer lower resources within 5%')
        save()
        write_json(output/'selected-profile.json',{'schema':1,'source':'mrms','name':'polaris-measured',
            'resources':selected_resources,'evidence':{'revision':revision,'selection_digest':selection_digest,
                'suite_sha256':hashlib.sha256(suite_path.read_bytes()).hexdigest()}})
        save()
        return 0
    except TimeoutError as exc:
        summary.update(status='paused',reason=str(exc));save();return 75
    except Exception as exc:
        summary.update(status='failed',error=f'{type(exc).__name__}: {exc}');save();raise
    finally:
        if original_budget is None:os.environ.pop('ECORE_TRANSFER_BUDGET',None)
        else:os.environ['ECORE_TRANSFER_BUDGET']=original_budget
