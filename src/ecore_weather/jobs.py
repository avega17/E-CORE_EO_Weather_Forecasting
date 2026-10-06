"""Portable operational entry point for NOAA research datasets."""
from __future__ import annotations
import argparse
import contextlib
import json
import os
from pathlib import Path
import signal
import threading
from .runlog import Run,scratch_root,task_progress,closed_snapshot,BoundedStream,queue_saved_selections


def _option(arguments,name,default):
    try:return arguments[arguments.index(name)+1]
    except ValueError:return default


def run_fetch(source,arguments,index_path=None,durable_state=None):
    from . import jobs_goes,jobs_mrms
    module=jobs_goes if source=='goes' else jobs_mrms
    if '--dry-run' in arguments:return module.main(arguments)
    output=Path(_option(arguments,'--output',f'results/study-{source}-native' if source=='goes' else 'results/study-mrms'))
    scratch=Path(_option(arguments,'--scratch',str(scratch_root())))
    if '--scratch' not in arguments:arguments=[*arguments,'--scratch',str(scratch)]
    output.mkdir(parents=True,exist_ok=True);scratch.mkdir(parents=True,exist_ok=True)
    pause=output/'pause.request';pause.unlink(missing_ok=True)
    # A prior intentional pause must not authorize tests after a newer failure.
    receipt=output/'paused.json'
    if receipt.exists():os.replace(receipt,output/'previous-paused.json')
    if durable_state:os.environ['ECORE_DURABLE_STATE']=str(Path(durable_state).resolve())
    snapshot=Path(__file__).resolve().parents[2]/'snapshot.json'
    identity=json.loads(snapshot.read_text()) if snapshot.exists() else None
    import platform,importlib.metadata
    environment={'python':__import__('sys').version,'platform':platform.platform(),'packages':{name:importlib.metadata.version(name) for name in ('earth2studio','zarr','duckdb','obstore','h5py','xarray','numpy')}}
    run=Run({'environment':environment,'code_snapshot':identity,'source':source,'arguments':arguments,'index':index_path,'scratch':str(scratch)},output='results/runs',index_path=index_path)
    os.environ['ECORE_RUN_ID']=run.id
    stop=threading.Event()
    def heartbeat():
        while not stop.wait(30):
            try:
                if source=='goes':queue_saved_selections(run,output,_option(arguments,'--destination','/mnt/p/ecore_eo_datasets'))
                
                if os.getenv('ECORE_SHARED_GOES')!='1':task_progress(run,scratch)
                run.update()
            except Exception as exc:run.state['heartbeat_error']=f'{type(exc).__name__}: {exc}'
    thread=threading.Thread(target=heartbeat,daemon=True);thread.start()
    def request_pause(signum,frame):pause.write_text('Pause at the next verified month boundary.\n')
    handlers={s:signal.getsignal(s) for s in (signal.SIGINT,signal.SIGTERM)}
    for sig in handlers:signal.signal(sig,request_pause)
    code=1
    import sys
    stdout=BoundedStream(sys.stdout,run.output/'run.log');stderr=BoundedStream(sys.stderr,run.output/'run.log',handler=stdout.handler)
    try:
        with contextlib.redirect_stdout(stdout),contextlib.redirect_stderr(stderr):code=module.main(arguments)
        if code and code!=75 and pause.exists() and os.getenv('ECORE_SHARED_GOES')!='1':
            run.state['interrupted_exit_code']=code
            code=75
        return code
    except Exception as exc:
        run.state['error']=f'{type(exc).__name__}: {exc}'
        if pause.exists() and os.getenv('ECORE_SHARED_GOES')!='1':
            code=75
            return code
        raise
    finally:
        stdout.close()
        stop.set();thread.join(10)
        for sig,handler in handlers.items():signal.signal(sig,handler)
        try:
            if os.getenv('ECORE_SHARED_GOES')!='1':task_progress(run,scratch)
        except Exception as exc:run.state["progress_error"]=str(exc)
        run.finish(code)
        if durable_state:closed_snapshot(Path(durable_state)/'archive_index.duckdb',index_path)


def status(index_path=None):
    import socket
    import psutil
    from .index import connect
    try:
        with connect(index_path,read_only=True) as con:
            rows=con.execute('SELECT run_id,status,pid,host,heartbeat,config_json FROM jobs ORDER BY started_at DESC').fetchall()
            cursor=con.execute('SELECT task_id,run_id,source,product,month,band,selected,checkpointed,completed,status,updated_at,metrics_json FROM tasks ORDER BY month,product,band')
            columns=[column[0] for column in cursor.description]
            tasks=[dict(zip(columns,row)) for row in cursor.fetchall()]
            attempts={}
            for (report,) in con.execute('SELECT report_json FROM fetch_runs ORDER BY started_at').fetchall():
                for task in json.loads(report).get('monthly_archives',[]):
                    if task.get('write_seconds',0)>0 and task.get('status')!='reused':
                        attempts[task.get('path')]=task
            for task in tasks:
                task['metrics']=json.loads(task.pop('metrics_json') or '{}')
                if task['task_id'] in attempts:task['attempt_metrics']=attempts[task['task_id']]
    except Exception as exc:return {'status':'index_unavailable','reason':str(exc).splitlines()[0],'fallback':'completed manifests and saved STAC selections'}
    runs=[]
    for rid,state,pid,host,heartbeat,payload in rows:
        alive=False
        if host==socket.gethostname() and psutil.pid_exists(pid):
            try:
                proc=psutil.Process(pid);alive=proc.create_time()<=json.loads(payload).get('started_at',0) and 'dataset_jobs' in ' '.join(proc.cmdline())
            except psutil.Error:pass
        runs.append({'run_id':rid,'recorded_status':state,'status':state if state!='running' or alive else 'interrupted',
                     'pid':pid,'process_alive':alive,'heartbeat':heartbeat,
                     'started_at':json.loads(payload).get('started_at'),
                     'ended_at':json.loads(payload).get('ended_at')})
    return {'runs':runs,'tasks':tasks}


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('operation',choices=['fetch','resume','status','pause','estimate','benchmark','backup','cleanup','index','snapshot','handoff'])
    p.add_argument('--index-path',help='Linux-local DuckDB path')
    p.add_argument('--durable-state',help='Destination for a closed index snapshot')
    import sys
    inputs=list(sys.argv[1:] if argv is None else argv)
    source=inputs.pop(1) if len(inputs)>1 and inputs[1] in ('goes','mrms') else None
    args,remaining=p.parse_known_args(inputs)
    args.source=source
    if args.index_path:os.environ['ECORE_INDEX_PATH']=args.index_path
    if args.operation in ('fetch','resume'):
        if args.source not in ('goes','mrms'):p.error('Choose goes or mrms')
        return run_fetch(args.source,remaining,args.index_path,args.durable_state)
    if args.operation=='status':
        from .job_status import main as monitor
        return monitor(remaining,index_path=args.index_path,source=args.source)
    if args.operation=='pause':
        output=Path(_option(remaining,'--output',f'results/study-{args.source}-native'))
        output.mkdir(parents=True,exist_ok=True);(output/'pause.request').write_text('Requested month-boundary pause\n');return 0
    if args.operation=='benchmark':
        if args.source=='mrms':
            from .report_benchmarks import mrms_source_main
            return mrms_source_main(remaining)
        from .jobs_benchmarks import main as handler
    elif args.operation=='estimate':
        from .jobs_estimate import main as handler
    elif args.operation=='backup':
        from .jobs_backup import main as handler
    elif args.operation=='handoff':
        if args.source!='goes':p.error('handoff currently supports GOES only')
        from .goes_rollout import main as handler
    elif args.operation=='snapshot':
        from .jobs_snapshot import main as handler
    elif args.operation=='cleanup':
        from .jobs_cleanup import main as handler
    else:
        from .index import rebuild
        paths=([args.source] if args.source else [])+remaining or ['/mnt/p/ecore_eo_datasets'];print(rebuild(paths,args.index_path));return 0
    return handler(remaining)
