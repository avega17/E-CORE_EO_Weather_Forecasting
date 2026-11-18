"""Process verified native archives; notebook and CLI use this same interface."""
from __future__ import annotations
import argparse
from dataclasses import asdict
import json
import os
from pathlib import Path
import signal
import sys

from ecore_weather.processing.transforms import BANDS, Recipe, atomic_json


def parser():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('operation',choices=['inventory','index','catalog','estimate','materialize','validate','benchmark','parallax','model-check','nam-audit','report'])
    p.add_argument('--config',help='JSON settings; explicitly supplied options override the file')
    p.add_argument('--location',default='/mnt/p/ecore_eo_datasets')
    p.add_argument('--preparation-root',default='/mnt/p/ecore_eo_datasets/preparation')
    p.add_argument('--destination',default='/mnt/p/ecore_eo_datasets/processed')
    p.add_argument('--selection',help='Existing preparation selection directory')
    p.add_argument('--unit',help='Materialized full/block directory')
    p.add_argument('--start',default='2022-09-18');p.add_argument('--end',default='2022-09-19')
    p.add_argument('--resolution-km',type=int,choices=[1,2,3],default=1)
    p.add_argument('--bands',nargs='*',type=int,choices=BANDS,default=list(BANDS))
    p.add_argument('--radar',nargs='*',choices=['refc','refc_base'],default=['refc','refc_base'])
    p.add_argument('--quality',choices=['good','conditional'],default='good')
    p.add_argument('--correction',choices=['none','cloud-height'],default='none')
    p.add_argument('--compression',choices=['zstd','none'],default='zstd')
    p.add_argument('--chunk-time',type=int,choices=[1,6,12],default=1)
    p.add_argument('--mode',choices=['auto','full','blocks'],default='auto')
    p.add_argument('--reserve-percent',type=float,default=5)
    p.add_argument('--workers',type=int,default=2,help='Bounded processing threads; independent native ZIP handles')
    p.add_argument('--memory-mib',type=int,default=8192)
    p.add_argument('--block-hours',type=int,default=24)
    p.add_argument('--max-blocks',type=int,help='Build only this many temporal blocks per invocation')
    p.add_argument('--max-hours',type=float,default=0,help='Checkpointed time limit; zero is continuous local processing')
    p.add_argument('--output',default='results/dataset-processing')
    p.add_argument('--index-path',default='results/dataset-processing/process.duckdb')
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--disk-mib',type=int,default=1024)
    p.add_argument('--linux-comparison',help='Small optional Linux SSD benchmark directory')
    p.add_argument('--allow-contended',action='store_true',help='Explicitly label measurements alongside active fetching')
    p.add_argument('--experimental-caribbean',action='store_true',help='Opt in to geographic model adaptation for an engineering test')
    p.add_argument('--model-package',help='Local model package directory (registry, normalizers, expert weights)')
    p.add_argument('--elevation',help='Regional elevation GeoTIFF with model static-field convention metadata')
    return p


def main(argv=None):
    arguments=list(sys.argv[1:] if argv is None else argv)
    p=parser()
    if '--config' in arguments:
        filename=arguments[arguments.index('--config')+1]
        config=json.loads(Path(filename).read_text())
        allowed={a.dest for a in p._actions}
        if set(config)-allowed:p.error(f'Unknown configuration keys: {sorted(set(config)-allowed)}')
        p.set_defaults(**config)
    args=p.parse_args(arguments)
    print(f'E-CORE processing | {args.operation}',flush=True)
    try:
        result=run(args)
    except Exception as exc:
        atomic_json(Path(args.output)/'latest.json',{'operation':args.operation,'status':'failed',
            'error':f'{type(exc).__name__}: {exc}'})
        print(f'Processing failed | {type(exc).__name__}: {exc}',file=sys.stderr,flush=True)
        raise
    Path(args.output).mkdir(parents=True,exist_ok=True)
    atomic_json(Path(args.output)/'latest.json',{'operation':args.operation,'result':result})
    printable={k:v for k,v in result.items() if k not in ('entries','cases')}
    if args.operation=='benchmark':printable={k:v for k,v in result.items() if k not in ('disk','windows','linux_disk','linux_windows','native_windows','current_snapshot_reads')}
    print(json.dumps(printable,indent=2,default=str),flush=True)
    return 75 if result.get('status')=='paused' else 2 if result.get('status')=='blocked' else 0


def run(args):
    if min(args.workers,args.memory_mib,args.block_hours,args.repeats)<=0 or args.max_hours<0:
        raise ValueError('Workers, memory, block hours and repeats must be positive; max-hours cannot be negative')
    if args.max_blocks is not None and args.max_blocks<1:
        raise ValueError('max-blocks must be positive')
    if not 0<=args.reserve_percent<100:
        raise ValueError('reserve-percent must be between 0 and 100')
    from ecore_weather.processing.process_catalog import build_index, build_catalog
    from ecore_weather.processing.process_store import estimate, materialize, validate_store
    if args.operation in ('validate','benchmark','model-check'):
        if not args.unit:raise ValueError('--unit must select a materialized full/block directory')
        if args.operation=='validate':return validate_store(args.unit)
        if args.operation=='benchmark':
            if not args.allow_contended:
                from ecore_weather.jobs.jobs_policy import ensure_paused
                ensure_paused()
            from ecore_weather.processing.process_benchmarks import benchmark
            result=benchmark(args.unit,args.destination,args.output,args.repeats,args.disk_mib,args.linux_comparison)
            result['contention']='other fetching permitted' if args.allow_contended else 'fetch pause checked'
            return result
        from ecore_weather.processing.process_model import model_check
        return model_check(args.unit,args.output,args.experimental_caribbean,args.model_package,args.elevation)
    if args.operation=='nam-audit':
        from ecore_weather.processing.process_pilots import nam_audit
        return nam_audit(args.output)
    if args.operation=='report':
        from ecore_weather.ui.process_ui import export_report
        return export_report(args.output)
    recipe=Recipe(resolution_km=args.resolution_km,bands=tuple(args.bands),radar=tuple(args.radar),
        quality=args.quality,parallax=args.correction,compression=args.compression,chunk_time=args.chunk_time)
    if args.selection:
        info=json.loads((Path(args.selection)/'selection.json').read_text())
        preparation=Path(args.selection)
    else:
        info=build_index(args.location,args.preparation_root,args.start,args.end,recipe,args.index_path)
        preparation=Path(info['preparation_path'])
    if args.operation in ('inventory','index'):return info
    if args.operation=='catalog':return build_catalog(preparation)
    if args.operation=='estimate':return estimate(preparation,args.destination,memory_mib=args.memory_mib,reserve_percent=args.reserve_percent)
    if args.operation=='parallax':
        from ecore_weather.processing.process_pilots import parallax_pilot
        return parallax_pilot(preparation,args.output)
    stopped=[False]
    import time
    started=time.monotonic()
    previous={}
    import threading
    if threading.current_thread() is threading.main_thread():
        for signum in (signal.SIGTERM,signal.SIGINT):
            previous[signum]=signal.getsignal(signum)
            signal.signal(signum,lambda *unused:stopped.__setitem__(0,True))
    try:
        return materialize(preparation,args.destination,args.mode,args.reserve_percent,args.workers,args.memory_mib,
            args.block_hours,stop=lambda:stopped[0] or (args.max_hours>0 and time.monotonic()-started>=args.max_hours*3600),max_blocks=args.max_blocks,
            progress=lambda done,total,path:print(f'Verified derived slots {done}/{total} | {path}',flush=True))
    finally:
        for signum,handler in previous.items():signal.signal(signum,handler)


if __name__=='__main__':raise SystemExit(main())
