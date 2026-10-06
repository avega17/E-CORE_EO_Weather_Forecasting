"""Repeatable shared-month scheduler comparisons with one transfer ledger."""
from __future__ import annotations
import argparse
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
from statistics import median
import tempfile
import time
from . import goes,monthly
from .common import PeakMemory,PR_BBOX
from .goes_shared import SharedConfig
from .runlog import save
from .transfer_budget import Budget,BudgetExceeded

CANDIDATES={'month_writers':(1,2,4),'download_concurrency':(16,32,64,96),
    'local_readers':(2,4,8),'range_readers':(2,4,8),
    'roi_budget_mib':(512,1024,2048,4096),'staging_mib':(8192,16384,32768)}
LEGACY_PROFILES=json.loads((Path(__file__).resolve().parents[2]/'jobs/goes_band_baseline.json').read_text()) if (Path(__file__).resolve().parents[2]/'jobs/goes_band_baseline.json').is_file() else {
    str(b):{'profile':{'read_mode':'range','read_processes':4,'workers':1,'prefetch_mib':512}}
    if b==2 else {'profile':{'read_mode':'async_pipeline','read_processes':2,'workers':8,
        'download_concurrency':16,'staging_mib':8192,'prefetch_mib':1024}}
    for b in goes.STORMSCOPE_BANDS}


def fingerprint(archives):
    from .storage import open_raw
    import numpy as np
    result={}
    for row in archives:
        with open_raw(row['path']) as ds:
            key=(str(ds.time.values[0]),row['band'])
            sha=hashlib.sha256(json.dumps(ds.attrs,sort_keys=True,default=str).encode())
            for name in sorted(ds.variables):
                values=np.asarray(ds[name].values)
                sha.update(name.encode());sha.update(str(values.shape).encode());sha.update(str(values.dtype).encode())
                sha.update(values.astype(str).tobytes() if values.dtype.kind=='O' else values.tobytes())
                sha.update(json.dumps(ds[name].attrs,sort_keys=True,default=str).encode())
            result[str(key)]=sha.hexdigest()
    return result


def choose(rows,baseline='legacy',memory_limit=48*1024**3):
    """Require three verified matched repeats on each satellite; permit <=5% regression."""
    valid=[r for r in rows if r.get('values_equal') and 0<r.get('peak_rss_bytes',0)<=memory_limit]
    by={}
    for row in valid:by.setdefault(row['profile_id'],{}).setdefault(row['satellite'],[]).append(row)
    def qualified(entry):return all(len({r['repeat'] for r in entry.get(s,[])})>=3 for s in (16,19))
    if baseline not in by or not qualified(by[baseline]):raise ValueError('Three verified baseline repetitions per satellite required')
    candidates=[]
    for key,entry in by.items():
        if key==baseline or not qualified(entry):continue
        gains={str(s):1-median(r['wall_seconds'] for r in entry[s])/median(r['wall_seconds'] for r in by[baseline][s]) for s in (16,19)}
        if min(gains.values())<-.05:continue
        config=entry[16][0]['config'];resource=sum(config[k] for k in ('month_writers','local_readers','range_readers'))
        candidates.append((min(gains.values()),resource,key,config,gains))
    if not candidates:return {'pipeline':'legacy','reason':'No correct shared profile within 5% of baseline on both satellites'}
    fastest=max(candidates,key=lambda r:r[0])
    # Compare relative readiness on both satellites, not percentage-point gains.
    # A four-point gain difference can still exceed a five-percent time penalty.
    tied=[r for r in candidates if all((1-r[4][str(s)])/(1-fastest[4][str(s)])<=1.05
                                      for s in (16,19))]
    winner=min(tied,key=lambda r:(r[1],r[3]['roi_budget_mib'],r[3]['staging_mib'],r[3]['download_concurrency']))
    return {'pipeline':'shared','global_config':winner[3],'gain':winner[4],
        'reason':'demonstrated >=10% gain on both satellites' if min(winner[4].values())>=.10 else 'non-regressing configuration; no demonstrated speedup',
        'scope':'best tested matched native CMIPF archive-readiness configuration'}


def inventory(output,satellite,bands,frames,months):
    from . import catalog
    from dataclasses import replace
    selections=[];year=2022 if satellite==16 else 2025
    for month in range(9,9+months):
        path=output/'selections'/f'goes{satellite}-{month:02}'
        saved=path/'collection.json'
        if saved.exists():selection=catalog.load_selection(saved)
        else:
            day=f'{year}-{month:02}-18'
            day_s=goes.discover(day+'T12:00:00Z',day+'T14:00:00Z',satellite=satellite,bands=goes.STORMSCOPE_BANDS)
            night=goes.discover(day+'T00:00:00Z',day+'T02:00:00Z',satellite=satellite,bands=goes.STORMSCOPE_BANDS)
            selection=replace(day_s,start=night.start,assets=sorted(day_s.assets+night.assets,key=lambda a:(a.time,a.key)))
            catalog.save_selection(selection,path,index_results=False)
        from .monthly import _band
        assets=[]
        for band in bands:
            available=[a for a in selection.assets if _band(a)==band]
            day=[a for a in available if 'T12:' in a.time or 'T13:' in a.time][:frames//2]
            night=[a for a in available if 'T00:' in a.time or 'T01:' in a.time][:frames-frames//2]
            if len(day)+len(night)!=frames:raise ValueError('Insufficient matched day/night observations')
            assets.extend(day+night)
        selections.append(replace(selection,bands=tuple(bands),assets=sorted(assets,key=lambda a:(a.time,a.key))))
    return selections


def combined(selections):
    return replace(selections[0],end=selections[-1].end,
                   assets=[a for s in selections for a in s.assets])


def case(selection,config,output,stage,repeat,reference,destination):
    budget=Budget(output/'budget.json');before=budget.summary();profile_id='legacy' if config is None else hashlib.sha256(json.dumps(config.manifest(),sort_keys=True).encode()).hexdigest()[:12]
    folder=Path(tempfile.mkdtemp(prefix='.ecore-scheduler-',dir=destination));scratch=Path(tempfile.mkdtemp(prefix='ecore-scheduler-'))
    # Failed stores remain discoverable in the compact suite failure record.
    success=False;started=time.perf_counter()
    try:
        with PeakMemory() as memory:
            report=monthly.fetch(selection,str(folder),scratch=str(scratch),backend='obstore',report_dir=None,index_results=False,
                read_profiles=LEGACY_PROFILES if config is None else None,
                monthly_writers=2,shared_config=config)
        if report.get('interrupted'):raise OSError('Benchmark archive did not verify')
        content=fingerprint(report['monthly_archives'])
        if reference is not None and content!=reference:raise AssertionError('Native arrays/coordinates/calibration/source metadata differ')
        after=budget.summary()
        row={'stage':stage,'repeat':repeat,'satellite':selection.satellite,'profile_id':profile_id,
            'config':config.manifest() if config else None,'wall_seconds':report['wall_s'],
            'returned_bytes':after['observed']-before['observed'],'charged_bytes':after['charged']-before['charged'],
            'peak_rss_bytes':memory.peak,'values_equal':True,'fingerprint':content,
            'source_objects':len(selection.assets),'listed_source_bytes':sum(a.size for a in selection.assets),
            'stored_bytes':report['stored_bytes'],'monthly_metrics':report['monthly_archives'],
            'global_metrics':report.get('global_metrics',{}),
            'application_cache':'fresh isolated directory','filesystem_cache':'not flushed',
            'timing_scope':'source acquisition through closed, packed, destination-verified monthly ZIPs'}
        success=True;return row,content
    finally:
        if success:
            import shutil
            shutil.rmtree(folder);shutil.rmtree(scratch)
        else:save(output/'failure.json',{'stage':stage,'repeat':repeat,'profile_id':profile_id,
            'destination_scratch':str(folder),'source_scratch':str(scratch),'elapsed_seconds':time.perf_counter()-started})


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output',default='results/benchmarks/goes-shared')
    p.add_argument('--destination',default='/mnt/p/ecore_eo_datasets')
    p.add_argument('--wait-for-pause',action='store_true',help='Wait for the active pinned job to drain and verify its month')
    p.add_argument('--resume-study',action='store_true',help='Pin the completed selected profile and launch chronological continuation')
    p.add_argument('--handoff-only',action='store_true',help='Retry final-code validation and launch from a completed suite; do not repeat comparisons')
    p.add_argument('--prefer-concurrent-runtime',action='store_true',help='Use the best tested concurrent profile after exact checks; treat sub-five-second small-case differences as inconclusive, as authorized for this rollout')
    p.add_argument('--budget-gib',type=float,default=512)
    args=p.parse_args(argv)
    if args.handoff_only and not args.resume_study:p.error('--handoff-only requires --resume-study')
    if not 0<args.budget_gib<=512:p.error('Cumulative network budget must be within 512 GiB')
    if args.wait_for_pause:
        from .goes_rollout import wait_for_pause
        wait_for_pause(args.output)
    from .jobs_policy import ensure_paused
    ensure_paused()
    from .jobs_mrms import preflight
    preflight(args.destination)
    output=Path(args.output).resolve();output.mkdir(parents=True,exist_ok=True)
    budget=Budget(output/'budget.json',int(args.budget_gib*1024**3))
    os.environ['ECORE_TRANSFER_BUDGET']=str(budget.path)
    os.environ['ECORE_RSS_LIMIT_BYTES']=str(48*1024**3)
    if args.handoff_only:
        from .goes_rollout import launch
        if json.loads((output/'suite.json').read_text()).get('status')!='complete':
            raise ValueError('A completed benchmark suite is required for handoff-only')
        if args.prefer_concurrent_runtime:
            approve_concurrent_runtime(output)
        elif (output/'production-fairness.json').exists():validate_fairness(output,args.destination)
        print(json.dumps(launch(output/'selected.json')),flush=True)
        return 0
    from .dataset_report import provenance
    identity=provenance()
    from .dataset_report import file_hash
    identity['scheduler_hashes']={name:file_hash(Path(__file__).with_name(name)) for name in
        ('goes_shared.py','goes_staging.py','goes_native.py','monthly_stream.py','monthly.py','goes_scheduler_benchmark.py')}
    path=output/'suite.json'
    state=json.loads(path.read_text()) if path.exists() else {'status':'running','rows':[],'provenance':identity}
    if state['provenance']!=identity:raise ValueError('Code/environment changed; use a new benchmark output')
    state.update(status='running',pid=os.getpid(),legacy_profiles=LEGACY_PROFILES,limits={'returned_gib':args.budget_gib,'rss_gib':48})
    refs={}
    def run(selection,config,stage,repeat):
        key='legacy' if config is None else hashlib.sha256(json.dumps(config.manifest(),sort_keys=True).encode()).hexdigest()[:12]
        existing=next((r for r in state['rows'] if (r['stage'],r['repeat'],r['satellite'],r['profile_id'])==(stage,repeat,selection.satellite,key)),None)
        refkey=(stage,selection.satellite)
        if existing:refs[refkey]=existing['fingerprint'];return existing
        row,content=case(selection,config,output,stage,repeat,refs.get(refkey),args.destination)
        refs[refkey]=content;state['rows'].append(row);state['budget']=budget.summary();save(path,state)
        print(f"{stage} GOES-{selection.satellite} {key}: {row['wall_seconds']:.2f}s, {row['returned_bytes']/1024**3:.3f} GiB, {row['peak_rss_bytes']/1024**3:.2f} GiB RSS",flush=True)
        return row
    try:
        leader=SharedConfig();screens={s:combined(inventory(output,s,(7,13),8,4)) for s in (16,19)}
        # Candidate axis controls are screened around the current fastest valid leader.
        run(screens[16],None,'screen',1)
        best=run(screens[16],leader,'screen',1)
        screened=[]
        for name,values in CANDIDATES.items():
            for value in values:
                profile=replace(leader,**{name:value})
                if profile==leader:continue
                if budget.summary()['charged']>=96*1024**3:break
                row=run(screens[16],profile,'screen',1)
                screened.append((row,profile))
                if row['peak_rss_bytes']<=48*1024**3 and row['wall_seconds']<best['wall_seconds']:
                    leader=profile;best=row
        # Inspect every axis, including C02 reader counts, with matched all-band samples.
        all_samples={s:combined(inventory(output,s,goes.STORMSCOPE_BANDS,8,4)) for s in (16,19)}
        second=min(screened,key=lambda item:item[0]['wall_seconds'])[1] if screened else SharedConfig()
        if second==leader:second=SharedConfig()
        finalists=[None,leader,second] if second!=leader else [None,leader]
        for s in (16,19):
            for repeat in range(1,4):
                order=finalists[repeat%len(finalists):]+finalists[:repeat%len(finalists)]
                for profile in order:run(all_samples[s],profile,'final',repeat)
        # Recheck C02 pool counts on all native bands; infrared screening cannot rank them.
        for readers in (2,4,8):
            profile=replace(leader,range_readers=readers)
            for s in (16,19):
                for repeat in range(1,4):run(all_samples[s],profile,'final',repeat)
        finals=[r for r in state['rows'] if r['stage']=='final']
        choice=choose(finals)
        if choice['pipeline']=='shared':
            config=SharedConfig(**{k:v for k,v in choice['global_config'].items() if k in SharedConfig.__dataclass_fields__})
            for s in (16,19):
                selection=combined(inventory(output,s,goes.STORMSCOPE_BANDS,16,4))
                run(selection,None,'confirm',1);run(selection,config,'confirm',1)
            confirmations=[r for r in state['rows'] if r['stage']=='confirm']
            gains={str(s):1-next(r['wall_seconds'] for r in confirmations if r['satellite']==s and r['profile_id']!='legacy')/next(r['wall_seconds'] for r in confirmations if r['satellite']==s and r['profile_id']=='legacy') for s in (16,19)}
            choice['confirmation_gain']=gains
            if min(gains.values())<-.05:
                choice={'pipeline':'legacy','reason':'Longer confirmation exceeded 5% regression limit','confirmation_gain':gains}
        save(output/'selected.json',choice)
        state.update(status='complete',choice=choice,budget=budget.summary(),completed_at=time.time());save(path,state)
        from .index import import_operational_evidence
        import_operational_evidence(output)
        write_table(state,output/'comparison.md')
        if args.resume_study:
            from .goes_rollout import launch
            print(json.dumps(launch(output/'selected.json')),flush=True)
        return 0
    except BaseException as exc:
        # Later validation stages own newer rows; never replace them with this
        # driver's earlier in-memory summary when the launch gate fails.
        if path.exists():
            saved=json.loads(path.read_text())
            if len(saved.get('rows',[]))>len(state['rows']):state=saved
        state.update(status='failed',error=f'{type(exc).__name__}: {exc}',budget=budget.summary());save(path,state);raise


def validate_fairness(output,destination):
    """Repeated post-fix checks within the original cumulative byte allowance.

    One month and four native day/night frames keep the paired comparisons
    within the remaining allowance. Earlier multi-month measurements retain
    their own scope; deterministic long-stream tests cover probe starvation.
    """
    import ast
    from .dataset_report import file_hash
    output=Path(output);path=output/'suite.json';state=json.loads(path.read_text())
    source=Path(__file__).with_name('goes_shared.py');revision=file_hash(source)
    if state.get('fairness_revision_sha256')==revision:return
    old_choice=state['choice']
    config=SharedConfig(**{k:v for k,v in old_choice['global_config'].items() if k in SharedConfig.__dataclass_fields__})
    references={}
    for repeat in (1,2,3):
        for sat in ((16,19) if repeat%2 else (19,16)):
            selection=combined(inventory(output,sat,goes.STORMSCOPE_BANDS,4,1))
            for profile in ((None,config) if (repeat+sat)%2 else (config,None)):
                key='legacy' if profile is None else hashlib.sha256(json.dumps(profile.manifest(),sort_keys=True).encode()).hexdigest()[:12]
                existing=next((r for r in state['rows'] if r['stage']=='fairness_final' and r['satellite']==sat and r['repeat']==repeat and r['profile_id']==key and r.get('scheduler_revision_sha256')==revision),None)
                if existing:references[sat]=existing['fingerprint'];continue
                row,digest=case(selection,profile,output,'fairness_final',repeat,references.get(sat),destination)
                references[sat]=digest;row['scheduler_revision_sha256']=revision
                state['rows'].append(row);state['budget']=Budget(output/'budget.json').summary();save(path,state)
                print(f'fairness_final GOES-{sat} {key}, repeat {repeat}: {row["wall_seconds"]:.2f}s',flush=True)
    current=[r for r in state['rows'] if r['stage']=='fairness_final' and r.get('scheduler_revision_sha256')==revision]
    choice=choose(current)
    if choice['pipeline']=='shared':
        chosen=[r for r in current if r['profile_id']!='legacy']
        choice['selection_stage']='fairness_final'
        choice['benchmark_reference']={str(s):{
            'median_archive_readiness_seconds':median(r['wall_seconds'] for r in chosen if r['satellite']==s),
            'median_returned_bytes':median(r['returned_bytes'] for r in chosen if r['satellite']==s),
            'peak_job_rss_bytes':max(r['peak_rss_bytes'] for r in chosen if r['satellite']==s),
            'sample':'one monthly context; four native day/night observations per band; post-fairness fix'} for s in (16,19)}
        # The final revision itself has three paired archive-equality checks per
        # satellite; it needs no additional downloads after this gate.
        receipt=json.loads((output/'path-cleanup-revision.json').read_text())
        text=source.read_text();names=receipt['read_write_source_sha256']
        receipt.update(after_sha256=revision,read_write_components_unchanged=False,
            verified_scheduler_revision=True,validation_stage='fairness_final',
            paired_repeats_per_satellite=3,
            scope='First-probe drain fairness correction; exact native archives on both satellites; original reader/writer scientific operations preserved')
        receipt['read_write_source_sha256']={n.name:hashlib.sha256(ast.get_source_segment(text,n).encode()).hexdigest()
            for n in ast.parse(text).body if getattr(n,'name',None) in names}
        save(output/'path-cleanup-revision.json',receipt)
        save(Path(__file__).resolve().parents[2]/'jobs/goes_workstation.json',choice)
    state.update(status='complete',choice=choice,pre_fairness_choice=old_choice,
                 fairness_revision_sha256=revision,launch_revision_sha256=revision,
                 launch_revision_scope='three paired four-frame native archive checks on each satellite after probe fairness fix')
    save(output/'selected.json',choice);save(path,state);write_table(state,output/'comparison.md')
    from .index import import_operational_evidence
    import_operational_evidence(output)


def write_table(state,path):
    lines=['# Shared GOES scheduler measurements','','Archive readiness includes source acquisition, close, ZIP packing and destination verification. Fresh application caches; filesystem caches were not flushed.','',
        '| Phase | Satellite | Profile | Repeats | Median seconds | Median returned GiB | Peak RSS GiB |',
        '|---|---|---|---:|---:|---:|---:|']
    groups={}
    for row in state['rows']:groups.setdefault((row['stage'],row['satellite'],row['profile_id']),[]).append(row)
    for (phase,sat,profile),rows in sorted(groups.items()):
        lines.append(f"| {phase} | GOES-{sat} | {profile} | {len(rows)} | {median(r['wall_seconds'] for r in rows):.2f} | {median(r['returned_bytes'] for r in rows)/1024**3:.3f} | {max(r['peak_rss_bytes'] for r in rows)/1024**3:.2f} |")
    lines += ['', 'Selected outcome: '+state.get('choice',{}).get('reason','not selected'),'',
        'These are bounded matched samples, not completed study months or a global concurrency optimum.']
    Path(path).write_text('\n'.join(lines)+'\n')


def approve_concurrent_runtime(output):
    """Record the revised user criterion without inventing new measurements."""
    import ast
    from .dataset_report import file_hash
    output=Path(output);state=json.loads((output/'suite.json').read_text())
    rows=[r for r in state['rows'] if r['stage']=='fairness_final']
    revision=state['fairness_revision_sha256']
    rows=[r for r in rows if r.get('scheduler_revision_sha256')==revision]
    for sat in (16,19):
        selected=[r for r in rows if r['satellite']==sat and r['profile_id']!='legacy']
        if len({r['repeat'] for r in selected})!=3 or any(not r.get('values_equal') or not 0<r['peak_rss_bytes']<=48*1024**3 for r in selected):
            raise ValueError('Concurrent adoption requires three exact, resource-bounded checks on both satellites')
    choice=dict(state['pre_fairness_choice'])
    choice.update(reason='User revised rollout criterion: retain best tested concurrent runtime configuration; small post-fix differences are inconclusive',
        qualification='Exact post-fix checks passed; longer-runtime gains measured before fairness fix, not yet established for corrected full months',
        production_pipeline='shared',selection_rule='user-authorized concurrent-runtime preference; no claim of post-fix speedup on both satellites')
    source=Path(__file__).with_name('goes_shared.py');text=source.read_text()
    receipt=json.loads((output/'path-cleanup-revision.json').read_text())
    names=receipt['read_write_source_sha256']
    # The launch snapshot was created after the paired equality gate. Default/UI
    # changes must not alter the tested scientific reader or scheduler classes.
    launch=json.loads(Path('results/study-goes-native/launch.json').read_text())
    frozen=(Path(launch['snapshot'])/'src/ecore_weather/goes_shared.py').read_text()
    def parts(value):return {n.name:hashlib.sha256(ast.get_source_segment(value,n).encode()).hexdigest()
        for n in ast.parse(value).body if getattr(n,'name',None) in names}
    if parts(text)!=parts(frozen):raise ValueError('Read/write components changed since post-fix equality checks')
    digest=file_hash(source)
    receipt.update(after_sha256=digest,read_write_source_sha256=parts(text),verified_scheduler_revision=True,
        paired_repeats_per_satellite=3,read_write_components_unchanged=False,
        scope='Post-fix paired equality with interface-only default changes; user revised performance criterion')
    state.update(choice=choice,launch_revision_sha256=digest,launch_revision_scope=receipt['scope'])
    save(output/'path-cleanup-revision.json',receipt);save(output/'selected.json',choice)
    save(output/'suite.json',state);save(Path(__file__).resolve().parents[2]/'jobs/goes_workstation.json',choice)
    write_table(state,output/'comparison.md')
