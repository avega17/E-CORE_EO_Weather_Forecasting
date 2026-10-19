"""Bounded four-month validation for the initial Polaris GOES resource profile."""
from __future__ import annotations

import argparse
from dataclasses import fields, replace
import hashlib
import json
import os
from pathlib import Path
import shutil
import time

from .common import PeakMemory, iso
from .goes_shared import SharedConfig, fetch_contexts
from .pbs import interval
from .runlog import save, closed_snapshot
from .transfer_budget import Budget


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    parser.add_argument('--shared-profile', required=True)
    parser.add_argument('--destination', required=True)
    parser.add_argument('--scratch', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--budget-gib', type=float, default=32)
    parser.add_argument('--max-hours', type=float, default=.8)
    args=parser.parse_args(argv)
    begin,stop=interval(args.start,args.end)
    if (stop-begin).total_seconds() > 86400 or args.budget_gib <= 0 or args.budget_gib > 32:
        parser.error('Pilot needs at most one day and a positive budget no larger than 32 GiB')
    profile=Path(args.shared_profile)
    profile_data=json.loads(profile.read_text())
    data=profile_data['global_config']
    config=SharedConfig(**{k:v for k,v in data.items() if k in {f.name for f in fields(SharedConfig)}}).validate()
    if config.month_writers != 4:
        parser.error('This pilot must exercise four monthly contexts')
    output=Path(args.output);output.mkdir(parents=True,exist_ok=True)
    scratch=Path(args.scratch);scratch.mkdir(parents=True,exist_ok=True)
    destination=Path(args.destination)
    if any(p.is_file() for p in destination.rglob('*')):
        parser.error('Pilot destination must be empty; inspect previous failure evidence before retrying')
    from .jobs_mrms import preflight
    preflight(destination)
    from . import goes
    from .goes_limits import configure
    configure(destination,output,scratch,400)
    # Keep the small experiment ledger on Eagle so failed PBS attempts cannot
    # reset the cumulative transfer ceiling when node-local SSD is erased.
    ledger=output/'pilot-transfer.json'
    budget=Budget(ledger,int(args.budget_gib*1024**3))
    if budget.summary()['limit'] != int(args.budget_gib*1024**3):
        parser.error('Existing pilot transfer ledger has a different limit; do not reset failed-attempt accounting')
    previous_rss=os.environ.get('ECORE_RSS_LIMIT_BYTES')
    os.environ['ECORE_RSS_LIMIT_BYTES']=str(int(profile_data.get('job_memory_gib',64)*1024**3))
    previous=os.environ.get('ECORE_TRANSFER_BUDGET')
    os.environ['ECORE_TRANSFER_BUDGET']=str(ledger)
    selections=[]
    started=time.perf_counter()
    try:
        # Same UTC time-of-day window in four seasons; no multi-year inventory.
        for month in (1,4,7,10):
            left=begin.replace(month=month,day=1)
            right=left+(stop-begin)
            selection=goes.discover(iso(left),iso(right),satellite=16,bands=goes.STORMSCOPE_BANDS)
            picked=[]
            for band in goes.STORMSCOPE_BANDS:
                assets=[a for a in selection.assets if f'C{band:02d}_' in a.key]
                if len(assets)<8:
                    raise ValueError(f'Pilot {month:02} C{band:02} has fewer than eight available scans')
                picked.extend(sorted(assets,key=lambda a:(a.time,a.key))[:8])
            selections.append(replace(selection,assets=sorted(picked,key=lambda a:(a.time,a.key))))
        requests=[(f'pilot-{i}',s,None) for i,s in enumerate(selections)]
        deadline=time.monotonic()+args.max_hours*3600
        with PeakMemory() as memory:
            result=fetch_contexts(requests,str(destination),config,str(scratch),True,deadline=deadline)
        elapsed=time.perf_counter()-started
        archives=[a for r in result['reports'] for a in r['monthly_archives']]
        if len(archives)!=32 or any(a.get('status')!='saved' or a.get('observations')!=8 for a in archives):
            raise AssertionError('Expected 32 verified native band stores with eight observations each')
        # Monthly finalization already checks every array, coordinate and metadata
        # against its source-read checksums. Hashes additionally protect reuse.
        before=budget.summary()
        reused=fetch_contexts(requests,str(destination),config,str(scratch),True,deadline=deadline)
        if budget.summary()['observed']!=before['observed']:
            raise AssertionError('Completed band reuse unexpectedly downloaded NOAA data')
        if any(a['status']!='reused' for r in reused['reports'] for a in r['monthly_archives']):
            raise AssertionError('Fresh pipeline did not reuse every verified pilot band')
        from .mrms_profile import code_identity, hardware
        revision,snapshot=code_identity()
        report={'status':'complete','code_revision':revision,'snapshot_sha256':snapshot,
            'profile_sha256':hashlib.sha256(profile.read_bytes()).hexdigest(),
            'hardware':hardware(),'global_config':config.manifest(),
            'wall_seconds_including_inventory':elapsed,'pipeline_wall_seconds':result['wall_s'],
            'peak_job_rss_bytes':memory.peak,'budget':budget.summary(),
            'global_metrics':result['global_metrics'],'verified_archives':32,
            'verified_observations':256,'completed_reuse_no_downloads':True,
            'selections':[{'start':s.start,'end_excluded':s.end,'asset_identity_sha256':hashlib.sha256('\n'.join(a.id for a in s.assets).encode()).hexdigest()} for s in selections],
            'archives':[{'band':a['band'],'month':a.get('month'),'archive_sha256':json.loads((Path(a['path']).parent/'complete.json').read_text())['archive_sha256'],
                         'stored_bytes':a['stored_bytes']} for a in archives],
            'scope':'Four seasonal GOES-16 contexts; eight scans per band; initial profile, not an optimum'}
        save(output/'pilot-summary.json',report)
        closed_snapshot(output/'archive_index.duckdb')
        # Delete only this isolated successful pilot's files, not study archives.
        for a in archives:
            folder=Path(a['path']).parent
            folder.resolve().relative_to(destination.resolve())
            shutil.rmtree(folder)
        print(json.dumps({k:report[k] for k in ('status','pipeline_wall_seconds','peak_job_rss_bytes','verified_archives')}))
        return 0
    except BaseException as exc:
        save(output/'pilot-failure.json',{'status':'failed','error':f'{type(exc).__name__}: {exc}',
                                        'budget':budget.summary()})
        raise
    finally:
        if previous_rss is None:os.environ.pop('ECORE_RSS_LIMIT_BYTES',None)
        else:os.environ['ECORE_RSS_LIMIT_BYTES']=previous_rss
        if previous is None:os.environ.pop('ECORE_TRANSFER_BUDGET',None)
        else:os.environ['ECORE_TRANSFER_BUDGET']=previous
