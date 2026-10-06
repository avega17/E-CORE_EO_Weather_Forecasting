"""Verified benchmark-to-pinned-study handoff; no bespoke shell watchers."""
from __future__ import annotations
import contextlib
import io
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time


def wait_for_pause(output, interval=30):
    from .jobs_policy import ensure_paused
    from .runlog import save
    root=Path(output)
    while True:
        try:ensure_paused();break
        except RuntimeError:
            save(root/'handoff.json',{'status':'waiting_for_verified_pause','pid':os.getpid(),'heartbeat':time.time()})
            time.sleep(interval)
    # An intentional exit alone does not prove monthly archives verified.
    native=Path('results/study-goes-native')
    config=json.loads((native/'run_config.json').read_text()) if (native/'run_config.json').exists() else {}
    paused=native/'paused.json'
    if config and not paused.is_file():raise RuntimeError('No durable month-boundary pause receipt; inspect study completion before tests')
    if paused.is_file():
        pause=json.loads(paused.read_text())
        if pause.get('status')!='paused':raise IOError('Pause receipt does not record intentional verified drain')
        for month in pause.get('months',[]):
            if month.get('status')=='unavailable':continue
            for archive in month.get('archives',[]):
                path=Path(archive['path']);marker=path.parent/'complete.json'
                if not path.is_file() or not marker.is_file():raise IOError('Drained archive is missing')
                receipt=json.loads(marker.read_text())
                if (receipt.get('product')!='ABI-L2-CMIPF' or not receipt.get('archive_sha256')
                        or receipt.get('observations')!=archive.get('observations')
                        or receipt.get('stored_bytes')!=path.stat().st_size):
                    raise IOError('Drained archive receipt differs from the completed month')
        next_month=pause.get('next_month')
        if next_month:
            from .jobs_goes import STAGES,_months,checkpoint_matches
            predecessors=[(stage,start) for stage,a,b in STAGES for start,_ in _months(a,b) if start.isoformat()<next_month]
            if predecessors:
                stage,start=predecessors[-1]
                path=native/'checkpoints'/stage/f'{start.isoformat()}.json'
                if not path.is_file() or not checkpoint_matches(json.loads(path.read_text()),'ABI-L2-CMIPF'):
                    raise IOError('The drained predecessor month does not have verified native archives')
    save(root/'handoff.json',{'status':'paused_verified','pid':os.getpid(),'heartbeat':time.time()})


def launch(choice_path, output='results/study-goes-native', scratch='results/study-scratch', configuration_only=False, snapshot_record=None):
    from .jobs_policy import ensure_paused
    from .jobs_mrms import preflight
    from .jobs_goes import STAGES,_months,checkpoint_matches
    from .runlog import save
    ensure_paused();preflight('/mnt/p/ecore_eo_datasets')
    choice_path=Path(choice_path).resolve()
    try:
        if not configuration_only:
            refine_month_writers(choice_path.parent)
            validate_launch_revision(choice_path.parent)
    except BaseException as exc:
        # The original driver also records its failure; retain later-phase rows
        # before that older in-memory state can replace the suite summary.
        current=choice_path.parent/'suite.json'
        save(choice_path.parent/'launch-failure-evidence.json',{
            'error':f'{type(exc).__name__}: {exc}',
            'suite':json.loads(current.read_text()) if current.exists() else {}})
        raise
    choice=json.loads(choice_path.read_text())
    if configuration_only:
        from .goes_shared import SharedConfig
        if choice.get('pipeline')!='shared':raise ValueError('Operational handoff requires a shared native profile')
        SharedConfig(**{k:v for k,v in choice['global_config'].items() if k in SharedConfig.__dataclass_fields__}).validate()
        suite={'rows':[]}
    else:
        suite=json.loads((choice_path.parent/'suite.json').read_text())
        if suite.get('status')!='complete' or suite.get('choice')!=choice:raise ValueError('Completed suite and selection must agree before launch')
    begin=None
    for stage,a,b in STAGES:
        for start,end in _months(a,b):
            path=Path(output)/'checkpoints'/stage/f'{start.isoformat()}.json'
            if not path.is_file() or not checkpoint_matches(json.loads(path.read_text()),'ABI-L2-CMIPF'):
                begin=f'{start:%Y-%m}';break
        if begin:break
    if begin is None:return {'status':'complete','reason':'Every study month already verified'}
    capacity=storage_preflight(suite,choice,'/mnt/p/ecore_eo_datasets',scratch)
    save(Path(output)/'storage-preflight.json',capacity)
    if snapshot_record is None:
        from .jobs_snapshot import main as snapshot
        capture=io.StringIO()
        with contextlib.redirect_stdout(capture):snapshot([])
        frozen=json.loads(capture.getvalue())
    else:frozen=snapshot_record
    root=Path(frozen['snapshot'])
    mode=choice['pipeline']
    command=[sys.executable,str(root/'scripts/dataset_jobs.py'),'resume','goes',
        '--index-path',str(Path('results/archive_index.duckdb').resolve()),
        '--phase','all','--begin-month',begin,'--product','ABI-L2-CMIPF',
        '--pipeline',mode,'--destination','/mnt/p/ecore_eo_datasets',
        '--output',str(Path(output).resolve()),'--scratch',str(Path(scratch).resolve()),'--max-hours','720']
    if mode=='shared':
        pinned=root/'jobs'/choice_path.name if configuration_only else root/'results/benchmarks/goes-shared/selected.json'
        if not pinned.exists() or json.loads(pinned.read_text())!=choice:
            raise IOError('Snapshot does not contain the selected shared profile')
        command += ['--shared-profile',str(pinned)]
    else:command += ['--monthly-writers','2','--read-profiles',str(root/'jobs/goes_band_baseline.json')]
    session='ecore_goes_chronological'
    if subprocess.run(['tmux','has-session','-t',session],capture_output=True).returncode==0:
        raise RuntimeError('Existing chronological session must exit before launch')
    shell='unset ECORE_TRANSFER_BUDGET ECORE_RSS_LIMIT_BYTES; exec '+shlex.join(command)
    subprocess.run(['tmux','new-session','-d','-s',session,'-c',str(Path.cwd()),shell],check=True)
    pid=int(subprocess.check_output(['tmux','display-message','-p','-t',session,'#{pane_pid}'],text=True).strip())
    record={'status':'launched','launched_at':time.time(),'snapshot':str(root),
        'content_sha256':frozen['sha256'],'tmux':session,'pid':pid,'command':command,
        'begin_month':begin,'pipeline':mode,'selection':choice,
        'storage_preflight':capacity,
        'scope':'chronological native CMIPF; selected profile; archive reuse; separate band grids'}
    save(Path(output)/'launch.json',record)
    save(choice_path.parent/'handoff.json',record)
    return record


def validate_reviewed_source(source,receipt):
    """Use portable source hashes; AST dumps differ between Python versions."""
    import ast,hashlib
    text=Path(source).read_text()
    names=receipt['read_write_source_sha256']
    hashes={node.name:hashlib.sha256(ast.get_source_segment(text,node).encode()).hexdigest()
        for node in ast.parse(text).body if getattr(node,'name',None) in names}
    if (hashes!=names or hashlib.sha256(Path(source).read_bytes()).hexdigest()!=receipt['after_sha256']
            or not (receipt.get('read_write_components_unchanged') or
                    receipt.get('verified_scheduler_revision') and receipt.get('paired_repeats_per_satellite')==3)):
        raise ValueError('Final launch source/read/write revision differs from the reviewed path cleanup')


def validate_launch_revision(output):
    """Check the final coordinator revision without mixing benchmark revisions.

    Monthly path/ID caching is coordinator bookkeeping; all source and writer
    component hashes must remain equal to the measured algorithms.
    """
    import importlib
    from . import goes_shared,goes
    from .dataset_report import file_hash
    from .goes_scheduler_benchmark import case,inventory,combined
    from .runlog import save
    from .transfer_budget import Budget
    output=Path(output);state=json.loads((output/'suite.json').read_text())
    if state['choice']['pipeline']!='shared':return
    receipt_path=output/'path-cleanup-revision.json'
    if not receipt_path.exists():return
    receipt=json.loads(receipt_path.read_text());source=Path(goes_shared.__file__)
    validate_reviewed_source(source,receipt)
    if state.get('launch_revision_sha256')==receipt['after_sha256']:return
    # Every benchmark case has closed its pools before the module is refreshed.
    module=importlib.reload(goes_shared)
    config=module.SharedConfig(**{k:v for k,v in state['choice']['global_config'].items() if k in module.SharedConfig.__dataclass_fields__})
    for sat in (16,19):
        selection=combined(inventory(output,sat,goes.STORMSCOPE_BANDS,8,2))
        full=next(r['fingerprint'] for r in state['rows'] if r['stage']=='final' and r['satellite']==sat and r['profile_id']=='legacy')
        year=2022 if sat==16 else 2025
        reference={key:value for key,value in full.items() if any(f'{year}-{m:02}-' in key for m in (9,10))}
        row,_=case(selection,config,output,'launch_verify',1,reference,'/mnt/p/ecore_eo_datasets')
        state['rows'].append(row);state['budget']=Budget(output/'budget.json').summary();save(output/'suite.json',state)
    state['launch_revision_sha256']=receipt['after_sha256'];state['launch_revision_scope']=receipt['scope']
    save(output/'suite.json',state)
    from .index import import_operational_evidence
    import_operational_evidence(output)


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


def refine_month_writers(output):
    """Confirm 1/2/4 normal owners on all native bands, not infrared alone.

    Uses the existing suite ledger, identical finalist sources, rotated repeats,
    and the same full archive-readiness measurement. No research fetch overlaps.
    """
    from . import goes
    # The running driver retains its measured case functions. Refresh only the
    # decision policy after its pools close; source/read/write algorithms remain
    # unchanged and their final revision is checked separately before launch.
    import importlib
    from . import goes_scheduler_benchmark as benchmark
    importlib.reload(benchmark)
    from .goes_shared import SharedConfig
    from .goes_scheduler_benchmark import case,choose,inventory,combined,write_table
    from .dataset_report import file_hash
    from .transfer_budget import Budget
    from .runlog import save
    from dataclasses import replace
    import hashlib
    output=Path(output);state=json.loads((output/'suite.json').read_text())
    if state.get('month_writer_validation')=='complete':return
    old_choice=state['choice']
    if old_choice['pipeline']!='shared':return
    config=SharedConfig(**{k:v for k,v in old_choice['global_config'].items() if k in SharedConfig.__dataclass_fields__})
    profiles=[replace(config,month_writers=n) for n in (1,2,4)]
    state.update(status='validating_all_band_writer_counts',refinement_hash=file_hash(__file__),
                 selection_policy_hash=file_hash(benchmark.__file__))
    save(output/'suite.json',state)
    try:
        for sat in (16,19):
            selection=combined(inventory(output,sat,goes.STORMSCOPE_BANDS,8,4))
            reference=next(r['fingerprint'] for r in state['rows'] if r['stage']=='final' and r['satellite']==sat and r['profile_id']=='legacy')
            for repeat in (1,2,3):
                for profile in profiles[repeat%3:]+profiles[:repeat%3]:
                    key=hashlib.sha256(json.dumps(profile.manifest(),sort_keys=True).encode()).hexdigest()[:12]
                    if any(r['stage'] in ('final','writer_final') and r['satellite']==sat and r['repeat']==repeat and r['profile_id']==key for r in state['rows']):continue
                    row,digest=case(selection,profile,output,'writer_final',repeat,reference,'/mnt/p/ecore_eo_datasets')
                    state['rows'].append(row);state['budget']=Budget(output/'budget.json').summary();save(output/'suite.json',state)
                    print(f"writer_final GOES-{sat}, {profile.month_writers} months, repeat {repeat}: {row['wall_seconds']:.2f}s",flush=True)
        choice=choose([r for r in state['rows'] if r['stage'] in ('final','writer_final')])
        if choice['pipeline']=='shared':
            # Infrared-only screening cannot exercise 96 requests with just 64
            # objects. Recheck the download axis on all eight native bands.
            from statistics import median
            base=SharedConfig(**{k:v for k,v in choice['global_config'].items() if k in SharedConfig.__dataclass_fields__})
            contenders=[]
            def short_reference(sat):
                full=next(r['fingerprint'] for r in state['rows'] if r['stage']=='final' and r['satellite']==sat and r['profile_id']=='legacy')
                year=2022 if sat==16 else 2025
                return {key:value for key,value in full.items() if any(f'{year}-{m:02}-' in key for m in (9,10))}
            # Two contexts offer 112 whole-object tasks, enough to exercise 96
            # download requests; owner counts were independently tested on four.
            for count in (16,32,64,96):
                profile=replace(base,download_concurrency=count)
                key=hashlib.sha256(json.dumps(profile.manifest(),sort_keys=True).encode()).hexdigest()[:12]
                trials=[]
                for sat in (16,19):
                    selection=combined(inventory(output,sat,goes.STORMSCOPE_BANDS,8,2))
                    row=next((r for r in state['rows'] if r['stage']=='download_screen' and r['satellite']==sat and r['profile_id']==key),None)
                    if row is None:
                        row,_=case(selection,profile,output,'download_screen',1,short_reference(sat),'/mnt/p/ecore_eo_datasets')
                        state['rows'].append(row);state['budget']=Budget(output/'budget.json').summary();save(output/'suite.json',state)
                        print(f"download_screen GOES-{sat}, {count} global downloads: {row['wall_seconds']:.2f}s",flush=True)
                    trials.append(row['wall_seconds'])
                contenders.append((max(trials),profile))
            promising=min(contenders,key=lambda entry:entry[0])[1]
            # Fresh paired baselines, rotated method order and three repeats.
            for repeat in (1,2,3):
                for sat in ((16,19) if repeat%2 else (19,16)):
                    order=(None,promising) if (repeat+sat)%2 else (promising,None)
                    for profile in order:
                        key='legacy' if profile is None else hashlib.sha256(json.dumps(profile.manifest(),sort_keys=True).encode()).hexdigest()[:12]
                        if any(r['stage']=='download_final' and r['repeat']==repeat and r['satellite']==sat and r['profile_id']==key for r in state['rows']):continue
                        selection=combined(inventory(output,sat,goes.STORMSCOPE_BANDS,8,2))
                        row,_=case(selection,profile,output,'download_final',repeat,short_reference(sat),'/mnt/p/ecore_eo_datasets')
                        state['rows'].append(row);state['budget']=Budget(output/'budget.json').summary();save(output/'suite.json',state)
                        print(f"download_final GOES-{sat}, {key}, repeat {repeat}: {row['wall_seconds']:.2f}s",flush=True)
            download_choice=choose([r for r in state['rows'] if r['stage']=='download_final'])
            if download_choice['pipeline']=='shared':
                choice=download_choice;choice['selection_stage']='download_final'
            else:choice['download_refinement']='No qualifying paired gain; retain validated four-context profile'
        if choice.get('global_config')!=old_choice.get('global_config') and choice['pipeline']=='shared':
            config=SharedConfig(**{k:v for k,v in choice['global_config'].items() if k in SharedConfig.__dataclass_fields__})
            gains={}
            for sat in (16,19):
                selection=combined(inventory(output,sat,goes.STORMSCOPE_BANDS,16,4))
                baseline=next(r for r in state['rows'] if r['stage']=='confirm' and r['satellite']==sat and r['profile_id']=='legacy')
                row,digest=case(selection,config,output,'writer_confirm',1,baseline['fingerprint'],'/mnt/p/ecore_eo_datasets')
                state['rows'].append(row);state['budget']=Budget(output/'budget.json').summary();save(output/'suite.json',state)
                gains[str(sat)]=1-row['wall_seconds']/baseline['wall_seconds']
            choice['confirmation_gain']=gains
            if min(gains.values())<-.05:choice=old_choice
        else:choice.setdefault('confirmation_gain',old_choice.get('confirmation_gain',{}))
        from statistics import median
        selected_stage=choice.get('selection_stage')
        selected_rows=[r for r in state['rows'] if (r['stage']==selected_stage if selected_stage else r['stage'] in ('final','writer_final'))
            and r.get('config')==choice.get('global_config')]
        contexts=2 if selected_stage=='download_final' else 4
        choice['benchmark_reference']={str(sat):{
            'median_archive_readiness_seconds':median(r['wall_seconds'] for r in selected_rows if r['satellite']==sat),
            'median_returned_bytes':median(r['returned_bytes'] for r in selected_rows if r['satellite']==sat),
            'sample':f'{contexts} monthly contexts; eight native day/night observations per band; fresh application caches',
            'peak_job_rss_bytes':max(r['peak_rss_bytes'] for r in selected_rows if r['satellite']==sat)}
            for sat in (16,19)} if choice['pipeline']=='shared' else {}
        state.update(status='complete',month_writer_validation='complete',choice=choice,budget=Budget(output/'budget.json').summary())
        save(output/'selected.json',choice);save(output/'suite.json',state);write_table(state,output/'comparison.md')
        from .index import import_operational_evidence
        import_operational_evidence(output)
    except BaseException as exc:
        state.update(status='failed',error=f'{type(exc).__name__}: {exc}',budget=Budget(output/'budget.json').summary());save(output/'suite.json',state);raise


def main(argv=None):
    """Schedule a configuration-only handoff without rerunning benchmarks."""
    import argparse
    parser=argparse.ArgumentParser(description='Resume native GOES with a pinned operational profile after verified monthly drain')
    parser.add_argument('--profile',default='jobs/goes_workstation.json')
    parser.add_argument('--output',default='results/study-goes-native')
    parser.add_argument('--scratch',default='results/study-scratch')
    parser.add_argument('--wait-for-pause',action='store_true')
    parser.add_argument('--mrms-source-benchmark',action='store_true',
        help='After verified pause, run the brief MRMS reference comparison before GOES resumes')
    args=parser.parse_args(argv)
    if args.mrms_source_benchmark and not args.wait_for_pause:
        parser.error('--mrms-source-benchmark requires --wait-for-pause and its verified boundary receipt')
    frozen=None;profile=args.profile
    if args.wait_for_pause:
        from .jobs_snapshot import main as snapshot
        from .runlog import save
        capture=io.StringIO()
        with contextlib.redirect_stdout(capture):snapshot([])
        frozen=json.loads(capture.getvalue())
        profile=str(Path(frozen['snapshot'])/'jobs'/Path(args.profile).name)
        if not Path(profile).exists() or json.loads(Path(profile).read_text())!=json.loads(Path(args.profile).read_text()):
            raise ValueError('Handoff profile must be included in the pinned jobs configuration')
        save(Path(args.output)/'handoff/replacement.json',{'snapshot':frozen['snapshot'],'sha256':frozen['sha256'],'profile':profile})
        wait_for_pause(Path(args.output)/'handoff')
    if args.mrms_source_benchmark:
        from .jobs_policy import ensure_paused
        ensure_paused()
        command=[sys.executable,str(Path(frozen['snapshot'])/'scripts/dataset_jobs.py')
                 if frozen else str(Path('scripts/dataset_jobs.py').resolve()),
                 'benchmark','mrms','--output',str(Path('results/study-mrms/source-benchmark.json').resolve())]
        subprocess.run(command,check=True)
    print(json.dumps(launch(profile,args.output,args.scratch,configuration_only=True,snapshot_record=frozen)),flush=True)
    return 0
