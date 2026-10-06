"""Diagnostics and performance report from completed, read-only research archives."""
from __future__ import annotations
import argparse
from pathlib import Path
import sys

from ecore_weather import dataset_report as report
from ecore_weather.common import PATCHES


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('operation',choices=['inventory','diagnose','benchmark','report'])
    p.add_argument('--location',default='/mnt/p/ecore_eo_datasets')
    p.add_argument('--output',default=report.DEFAULT_OUTPUT)
    p.add_argument('--source',choices=['goes','mrms'])
    p.add_argument('--product');p.add_argument('--band',type=int)
    p.add_argument('--start');p.add_argument('--end',help='UTC exclusive end')
    p.add_argument('--patch',nargs='+',choices=['Whole ROI',*PATCHES],
                   help='Named Caribbean patches; whole ROI is always included. Omit to include all patches.')
    p.add_argument('--full',action='store_true',help='Audit every selected completed observation; otherwise sample by quarter')
    p.add_argument('--batch-frames',type=int,default=8);p.add_argument('--memory-mib',type=int,default=512)
    p.add_argument('--benchmark-kind',choices=['local','cross-month','backend','goes-network','mentor','hf'],default='local')
    p.add_argument('--hf-root',help='HF bucket root, without credentials; restore verified backup members first')
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--logs',nargs='*',default=[])
    args=p.parse_args(argv)
    filters={k:getattr(args,k) for k in ('source','product','band','start','end')}
    if args.operation=='inventory':result=report.inventory(args.location,args.output,**filters)[0]
    elif args.operation=='diagnose':
        result=report.diagnose(args.location,args.output,full=args.full,batch_frames=args.batch_frames,
            memory_mib=args.memory_mib,patches=None if args.patch is None else {name:PATCHES[name] for name in args.patch if name!='Whole ROI'},
            progress=lambda done,reused,path,stamp:print(f'{done} diagnosed; {reused} reused; {stamp} {path}',flush=True),**filters)
    elif args.operation=='report':
        report.inventory(args.location,args.output,**filters)
        report.index_measurements(args.output)
        if args.logs:report.import_measurements(args.logs,args.output)
        result=report.export_report(args.output)
    else:
        from ecore_weather import report_benchmarks as bench
        if args.benchmark_kind=='local':result=bench.local_reads(args.location,args.output,args.repeats,**filters)
        elif args.benchmark_kind=='cross-month':result=bench.cross_month_reads(args.location,args.output,args.repeats,**filters)
        elif args.benchmark_kind=='backend':result=bench.backends(args.location,args.output,args.repeats,**filters)
        elif args.benchmark_kind=='hf':
            if not args.hf_root:p.error('--hf-root required')
            result=bench.hf_restore(args.hf_root,args.output)
        else:
            from .jobs_policy import ensure_paused
            ensure_paused()
            if args.benchmark_kind=='goes-network':
                from .jobs_benchmarks import main as benchmark_main
                return benchmark_main(['--output',str(Path(args.output)/'goes-final')])
            from ecore_weather import benchmark,mrms
            results=[]
            for begin,end in [('2022-09-18','2022-09-25'),('2024-09-15','2024-09-22')]:
                selection=benchmark.comparable_hours(mrms.discover(begin,end,product=mrms.DEFAULT_PRODUCT,cadence_minutes=60))
                table,records=benchmark.run_mrms(selection,report_dir=Path(args.output)/'mentor',repeats=args.repeats,variants=['legacy','s3fs-1','obstore-4','obstore-process-4'])
                if not records.matches_legacy.all():raise AssertionError('Mentor output equality failed')
                results.append(table.to_dict('records'))
            from ecore_weather.common import write_json
            result={'weeks':results};write_json(Path(args.output)/'mentor.json',result)
    print(result if args.operation!='diagnose' else {'processed':result['processed'],'reused':result['reused']})
    return 0

if __name__=='__main__':raise SystemExit(main())
