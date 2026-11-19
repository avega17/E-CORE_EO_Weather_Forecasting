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
    p.add_argument('operation',choices=['inventory','index','catalog','estimate','materialize','validate','benchmark','parallax','model-sample','model-check','model-compare','loader-check','nam-audit','demo-export','report','stats','retire-icechunk','status'])
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
    p.add_argument('--shard-time',type=int,choices=[0,24],default=0,help='0: ordinary chunks; 24: complete 24-frame time shards')
    p.add_argument('--spatial-chunk',type=int,choices=[0,256],default=0,help='0: full ROI; 256: spatial tiles')
    p.add_argument('--correction-bands',type=int,nargs='+',default=[1,2,3,7,13],help='Validated cloud bands; water-vapor channels remain unshifted')
    p.add_argument('--height-age-minutes',type=int,default=20)
    p.add_argument('--train-start',default='2021-01-01');p.add_argument('--train-end',default='2025-01-01')
    p.add_argument('--apply',action='store_true',help='Apply retirement after replacement verification; default previews')
    p.add_argument('--mode',choices=['auto','full','blocks'],default='auto')
    p.add_argument('--reserve-percent',type=float,default=5)
    p.add_argument('--workers',type=int,default=0,help='Independent transform processes; 0 uses every CPU allowed by job affinity')
    p.add_argument('--executor',choices=['process','thread'],default='process',help='Processes avoid Python interpreter contention; thread is a comparison option')
    p.add_argument('--prefetch-batches',type=int,default=4,help='Bounded batches transformed ahead of the single verified writer')
    p.add_argument('--memory-mib',type=int,default=32768)
    p.add_argument('--block-hours',type=int,default=24)
    p.add_argument('--max-blocks',type=int,help='Build only this many temporal blocks per invocation')
    p.add_argument('--max-hours',type=float,default=0,help='Checkpointed time limit; zero is continuous local processing')
    from ecore_weather.core.paths import result_path
    p.add_argument('--output',default=str(result_path('dataset-processing')))
    p.add_argument('--index-path',default=str(result_path('dataset-processing/process.duckdb')))
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--worker-counts',type=int,nargs='+',help='Benchmark actual transform processes on a saved selection of at most two days, instead of layout trials')
    p.add_argument('--disk-mib',type=int,default=1024)
    p.add_argument('--linux-comparison',help='Small optional Linux SSD benchmark directory')
    p.add_argument('--allow-contended',action='store_true',help='Explicitly label measurements alongside active fetching')
    p.add_argument('--metadata-only',action='store_true',
                   help='For nam-audit, inventory every cycle between --start and --end and sample text indexes without reading forecasts')
    p.add_argument('--experimental-caribbean',action='store_true',help='Opt in to geographic model adaptation for an engineering test')
    p.add_argument('--reference-conus',action='store_true',help='Run a bounded stock Flash reference in its supported CONUS domain; downloads model weights and six source scans')
    p.add_argument('--model-package',help='Local model package directory (registry, normalizers, expert weights)')
    p.add_argument('--physicsnemo-reference',help='Official regional_weather_diffusion datasets directory for stock-loader checks')
    p.add_argument('--elevation',help='Regional elevation GeoTIFF with model static-field convention metadata')
    p.add_argument('--sample-time',help='UTC final observation slot for a six-frame model input')
    p.add_argument('--sample',help='Portable verified model-sample NPZ and its JSON manifest')
    p.add_argument('--forecast',help='Bounded regional forecast NPZ returned by model-check')
    p.add_argument('--demo-unit',action='append',help='Verified 3 km store for demo export; repeat for disjoint periods')
    p.add_argument('--demo-storm',action='append',help='Export only this storm slug; repeat to select more')
    p.add_argument('--demo-root',help='Output directory for the portable storm catalog and blocks')
    from ecore_weather.core.paths import repository_root
    p.add_argument('--figure-dir',default=str(repository_root()/'figures/dataset-processing/model-checks'),help='Directory for model comparison figures')
    p.add_argument('--crop-size',type=int,default=256,help='Bounded square model crop in prepared 3 km pixels')
    p.add_argument('--allow-partial',action='store_true',help='Freeze only committed frames from the active materialization prefix')
    p.add_argument('--allow-approximate-topography',action='store_true',help='Explicit engineering-only use of a documented empirical topography calibration')
    p.add_argument('--download-model',action='store_true',help='Allow public model weights to be downloaded when no local package is supplied')
    p.add_argument('--model-seed',type=int,default=0,help='Recorded random seed for the regional engineering forecast')
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
    printable={k:v for k,v in result.items() if k not in ('entries','cases','months','samples')}
    if args.operation=='nam-audit':
        printable.pop('storms',None)
        if args.metadata_only:
            printable={k:v for k,v in printable.items() if k in
                       ('status','start','end_excluded','totals','index_samples','index_sample_bytes',
                        'grib_payload_bytes','report')}
    if args.operation=='benchmark':
        printable={k:v for k,v in result.items() if k not in ('cases','initial_screening','disk','windows','linux_disk','linux_windows','native_windows','current_snapshot_reads')}
        if 'cases' in result:printable['case_count']=len(result['cases'])
    atomic_json(Path(args.output)/'latest.json',{'operation':args.operation,'result':printable})
    print(json.dumps(printable,indent=2,default=str),flush=True)
    return 75 if result.get('status')=='paused' else 2 if result.get('status')=='blocked' else 1 if result.get('status')=='failed' else 0


def run(args):
    if min(args.memory_mib,args.block_hours,args.repeats,args.prefetch_batches)<=0 or min(args.workers,args.max_hours)<0:
        raise ValueError('Memory, block hours, repeats and prefetch must be positive; workers/max-hours cannot be negative')
    if args.max_blocks is not None and args.max_blocks<1:
        raise ValueError('max-blocks must be positive')
    if not 0<=args.reserve_percent<100:
        raise ValueError('reserve-percent must be between 0 and 100')
    from ecore_weather.processing.process_catalog import build_index, build_catalog
    from ecore_weather.processing.process_store import estimate, materialize, validate_store
    if args.operation=='model-check' and args.reference_conus:
        from ecore_weather.processing.process_model import stock_flash_check
        return stock_flash_check(args.output,args.model_package)
    if args.operation=='model-check':
        from ecore_weather.processing.process_model import model_check
        return model_check(args.unit,args.output,args.experimental_caribbean,args.model_package,args.elevation,
            args.sample,args.sample_time,args.allow_partial,args.crop_size,
            args.allow_approximate_topography,args.download_model,args.model_seed)
    if args.operation=='model-sample':
        if not args.unit or not args.sample_time:raise ValueError('--unit and --sample-time are required')
        from ecore_weather.processing.process_model import export_model_sample
        return export_model_sample(args.unit,args.output,args.sample_time,args.crop_size,args.allow_partial)
    if args.operation=='model-compare':
        if not args.forecast or not args.sample:raise ValueError('--forecast and --sample are required')
        from ecore_weather.processing.process_model import evaluate_caribbean_forecast
        return evaluate_caribbean_forecast(args.forecast,args.sample,args.output,args.figure_dir)
    if args.operation=='status':
        if not args.unit:raise ValueError('--unit must select a full/block directory')
        path=Path(args.unit);state=json.loads((path/'progress.json').read_text())
        return {name:state.get(name) for name in ('verified_slots','slots','eligible_slots','updated_at','elapsed_seconds','execution','metrics')} | {
            'status':'complete' if (path/'complete.json').is_file() else 'checkpointed',
            'percent':round(100*state['verified_slots']/state['slots'],2)}
    if args.operation=='retire-icechunk':
        if not args.unit:raise ValueError('--unit must name the verified plain-Zarr replacement')
        from ecore_weather.processing.process_pilots import retire_icechunk
        return retire_icechunk(args.location,args.unit,args.output,args.index_path,args.apply)
    if args.operation=='benchmark' and args.worker_counts:
        if not args.selection:raise ValueError('--selection is required for the worker benchmark')
        from ecore_weather.jobs.jobs_policy import ensure_paused
        ensure_paused()
        from ecore_weather.processing.process_benchmarks import processing_benchmark
        return processing_benchmark(args.selection,args.destination,args.output,args.worker_counts,args.repeats)
    if args.operation in ('validate','benchmark','loader-check','stats'):
        if not args.unit:raise ValueError('--unit must select a materialized full/block directory')
        if args.operation=='validate':return validate_store(args.unit)
        if args.operation=='stats':
            from ecore_weather.processing.process_store import export_statistics
            return export_statistics(args.unit,args.train_start,args.train_end,args.elevation)
        if args.operation=='loader-check':
            if not args.physicsnemo_reference:raise ValueError('--physicsnemo-reference is required')
            from ecore_weather.processing.process_model import loader_check
            return loader_check(args.unit,args.physicsnemo_reference,args.output)
        if args.operation=='benchmark':
            if not args.allow_contended:
                from ecore_weather.jobs.jobs_policy import ensure_paused
                ensure_paused()
            from ecore_weather.processing.process_benchmarks import benchmark
            result=benchmark(args.unit,args.destination,args.output,args.repeats,args.disk_mib,args.linux_comparison)
            result['contention']='other fetching permitted' if args.allow_contended else 'fetch pause checked'
            return result
    if args.operation=='nam-audit':
        if args.metadata_only:
            from ecore_weather.processing.nam_inventory import inventory
            result=inventory(args.start,args.end)
            report=Path(args.output)/'nam-metadata-inventory.json'
            Path(args.output).mkdir(parents=True,exist_ok=True)
            atomic_json(report,result)
            return result | {'report':str(report)}
        from ecore_weather.processing.nwp import audit
        return audit(args.output, location=args.location)
    if args.operation=='demo-export':
        from ecore_weather.processing.storm_demo import export_demo
        from ecore_weather.processing.storm_windows import STORMS
        if not args.demo_unit or not args.demo_root or not args.elevation:
            raise ValueError('Supply --demo-unit, --demo-root, and --elevation')
        if set(args.demo_storm or ())-STORMS.keys():
            raise ValueError(f"Choose a storm from {', '.join(STORMS)}")
        return export_demo(args.demo_unit,args.elevation,args.demo_root,args.output,args.demo_storm)
    if args.operation=='report':
        from ecore_weather.ui.process_ui import export_report
        return export_report(args.output)
    recipe=Recipe(resolution_km=args.resolution_km,bands=tuple(args.bands),radar=tuple(args.radar),
        quality=args.quality,parallax=args.correction,compression=args.compression,chunk_time=args.chunk_time,
        shard_time=args.shard_time,spatial_chunk=args.spatial_chunk,correction_bands=tuple(args.correction_bands),height_age_minutes=args.height_age_minutes)
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
            progress=lambda done,total,path:print(f'Verified derived slots {done}/{total} | {path}',flush=True),
            executor=args.executor,prefetch_batches=args.prefetch_batches)
    finally:
        for signum,handler in previous.items():signal.signal(signum,handler)


if __name__=='__main__':raise SystemExit(main())
