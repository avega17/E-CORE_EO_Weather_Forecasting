"""Fetch the GOES ROI in resumable monthly batches, in chronological half-year stages."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
import json
import os
from pathlib import Path
import subprocess
import sys
import time



from ecore_weather.runlog import save as write_json
from ecore_weather.common import utc


def snapshot_environment():
    """Keep CLI imports and spawned readers on this runner's code snapshot."""
    root = Path(__file__).resolve().parents[2]
    env = os.environ.copy()
    env['ECORE_REPO_ROOT'] = str(root)
    env['PYTHONPATH'] = str(root / 'src') + (os.pathsep + env['PYTHONPATH'] if env.get('PYTHONPATH') else '')
    return env


STAGES = [
    ("h1-2026", date(2026, 1, 1), date(2026, 7, 1)),
    ("h2-2025", date(2025, 7, 1), date(2026, 1, 1)),
    ("h1-2025", date(2025, 1, 1), date(2025, 7, 1)),
    ("h2-2024", date(2024, 7, 1), date(2025, 1, 1)),
    ("h1-2024", date(2024, 1, 1), date(2024, 7, 1)),
    ("h2-2023", date(2023, 7, 1), date(2024, 1, 1)),
    ("h1-2023", date(2023, 1, 1), date(2023, 7, 1)),
    ("h2-2022", date(2022, 7, 1), date(2023, 1, 1)),
    ("h1-2022", date(2022, 1, 1), date(2022, 7, 1)),
    ("h2-2021", date(2021, 7, 1), date(2022, 1, 1)),
    ("h1-2021", date(2021, 1, 1), date(2021, 7, 1)),
]
STAGES = list(reversed(STAGES))
BANDS = (1, 2, 3, 7, 8, 9, 10, 13)


def _months(start, end):
    current = start
    while current < end:
        nxt = date(current.year + (current.month == 12),
                   1 if current.month == 12 else current.month + 1, 1)
        yield current, min(nxt, end)
        current = nxt


def _verify_month(report_dir):
    # The CLI writes product reports under <month>/<product>/, so a direct
    # glob at the month level misses successfully fetched monthly collections.
    reports = sorted(Path(report_dir).rglob("*-monthly.json"),
                     key=lambda path: path.stat().st_mtime_ns)
    if not reports:
        raise FileNotFoundError(f"No monthly fetch report found under {report_dir}")
    report = json.loads(reports[-1].read_text())
    if report.get("source") != "goes" or report.get("interrupted"):
        raise IOError("GOES fetch report marks the month as failed or incomplete")
    archives = []
    for entry in report.get("monthly_archives", []):
        archive = Path(entry.get("path", ""))
        if entry.get("status") not in {"saved", "reused"}:
            raise IOError(f"GOES monthly archive did not complete: {entry}")
        if not archive.is_file() or not (archive.parent / "complete.json").is_file():
            raise IOError(f"GOES archive or completion marker missing: {archive}")
        marker = json.loads((archive.parent / "complete.json").read_text())
        if marker.get("observations") != entry.get("observations"):
            raise IOError(f"GOES observation count differs from marker: {archive}")
        archives.append({"path": str(archive), "band": entry.get("band"),
            "observations": entry.get("observations"), "stored_bytes": entry.get("stored_bytes")})
    if not archives:
        raise IOError("GOES fetch report contains no completed monthly archives")
    available_bands = sorted({int(row["band"]) for row in report.get("monthly_archives", [])
                              if row.get("band") is not None})
    return report, archives, available_bands


def _reusable_selection(report_dir, start, end, product):
    """Reuse a saved STAC selection after interruption before its month fetch."""
    path = Path(report_dir) / product / "collection.json"
    if not path.is_file():
        path = Path(report_dir) / product / "items.json"
    if not path.is_file():
        return None
    from ecore_weather.catalog import load_selection
    selection = load_selection(path)
    if (selection.source != "goes" or selection.product != product
            or utc(selection.start) != utc(start) or utc(selection.end) != utc(end)
            or tuple(selection.bbox) != (-70.24, 14.36, -62.56, 22.04)
            or tuple(selection.bands) != BANDS or selection.scans_per_hour is not None):
        return None
    return path


def satellite_segments(month):
    """Split the operational handoff without combining different native grids."""
    transition=utc('2025-04-07T15:00:00Z')
    if not utc(month['start'])<transition<utc(month['end_excluded']):return [month]
    segments=[]
    for satellite,start,end in [(16,month['start'],'2025-04-07T15:00:00Z'),(19,'2025-04-07T15:00:00Z',month['end_excluded'])]:
        command=list(month['command']);output=str(Path(month['report_dir'])/f'goes{satellite}')
        for option,value in [('--start',start),('--end',end),('--output',output),('--satellite',str(satellite))]:
            command[command.index(option)+1]=value
        segments.append({**month,'start':start,'end_excluded':end,'report_dir':output,'command':command})
    return segments


def checkpoint_matches(saved, product):
    """Old checkpoints may omit product, but every marker and ZIP must prove it."""
    if saved.get("status") != "complete" or not saved.get("archives"):
        return False
    if saved.get("product") not in (None, product):
        return False
    from ecore_weather.storage import open_raw
    from ecore_weather.goes_monthly import _file_sha256
    for item in saved["archives"]:
        path = Path(item["path"])
        marker_path = path.parent / "complete.json"
        if not path.is_file() or not marker_path.is_file():
            return False
        marker = json.loads(marker_path.read_text())
        if marker.get("product") != product or marker.get("source") != "goes":
            return False
        if marker.get("archive_sha256") and _file_sha256(path) != marker["archive_sha256"]:
            return False
        # Product-less historical checkpoints require direct archive identity.
        if saved.get("product") is None:
            with open_raw(path) as ds:
                if ds.attrs.get("product") != product:
                    return False
                if any(product not in str(a.get("source_url", "")) for a in marker.get("assets", [])):
                    return False
    return True


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=[row[0] for row in STAGES] + ["all"], default="all")
    parser.add_argument("--destination", default="/mnt/p/ecore_eo_datasets")
    parser.add_argument("--output", default="results/study-goes-native")
    parser.add_argument("--scratch", default="results/study-scratch")
    parser.add_argument("--workers", type=int, default=8,
                        help="Source threads/batch limit per reader; HDF5 serializes calls within each process. Use --read-processes for independent HDF5 reads.")
    from .goes_shared import add_arguments
    add_arguments(parser)
    parser.add_argument("--decode-workers", type=int, default=1,
                        help="Legacy CMIPF compatibility only; unused by MCMIPF")
    parser.add_argument("--product", choices=("ABI-L2-MCMIPF", "ABI-L2-CMIPF"),
                        default="ABI-L2-CMIPF")
    parser.add_argument("--read-profiles", help="Pinned per-band benchmark profiles JSON")
    parser.add_argument("--read-processes", type=int, default=2)
    parser.add_argument('--read-mode', choices=['range','async_full','async_pipeline'], default='range')
    parser.add_argument('--download-concurrency', type=int, default=32)
    parser.add_argument('--staging-mib', type=int, default=16384)
    parser.add_argument("--prefetch-mib", type=int, default=512)
    parser.add_argument("--block-size-kib", type=int, default=1024)
    parser.add_argument("--reuse-cmipf-2km-from", metavar="ARCHIVE_ROOT",
                        help="Build a separately labeled 2 km hybrid using matching CMIPF bands")
    parser.add_argument("--stop-after-month", help="Pause with exit 75 after the verified YYYY-MM month")
    parser.add_argument("--begin-month", help="Begin at this YYYY-MM month in the staged order")
    parser.add_argument("--max-hours",type=float,default=12.)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    from .goes_shared import config_from_args
    try:shared_config=config_from_args(args)
    except ValueError as exc:parser.error(str(exc))
    args.monthly_writers=shared_config.month_writers
    if args.pipeline=='shared' and args.product=='ABI-L2-CMIPF' and args.read_profiles:
        parser.error('Schema-1 per-band profiles require --pipeline legacy; shared limits are global')
    if args.read_mode in ('async_full', 'async_pipeline') and args.product != 'ABI-L2-CMIPF':
        parser.error('Async staging is supported only for native CMIPF')
    if min(args.workers, args.monthly_writers, args.decode_workers,
           args.read_processes, args.prefetch_mib, args.block_size_kib,
           args.download_concurrency, args.staging_mib) < 1:
        parser.error("Worker counts must be positive")
    if args.stop_after_month and not any(
            start.strftime("%Y-%m") == args.stop_after_month
            for _, stage_start, stage_end in STAGES for start, _ in _months(stage_start, stage_end)):
        parser.error("--stop-after-month must be a study month in YYYY-MM format")
    if args.reuse_cmipf_2km_from and args.product != "ABI-L2-MCMIPF":
        parser.error("CMIPF 2 km reuse requires --product ABI-L2-MCMIPF")
    archive_product = "ABI-L2-CMI-2KM-HYBRID" if args.reuse_cmipf_2km_from else args.product
    if not Path(args.destination).is_dir():
        parser.error(f"Local dataset destination does not exist: {args.destination}")
    stages = [row for row in STAGES if args.phase == "all" or row[0] == args.phase]
    command_base = [sys.executable, str(Path(__file__).resolve().parents[2] / "notebooks" / "02_goes.py"),
        "--operation", "fetch", "--destination", args.destination,
        "--bands", *map(str, BANDS), "--scans-per-hour", "0", "--satellite", "auto",
        "--workers", str(args.workers), "--decode-workers", str(args.decode_workers),
        "--monthly-writers", str(1 if args.product == "ABI-L2-MCMIPF" else args.monthly_writers),
        "--scratch", args.scratch,
        "--product", args.product, "--read-processes", str(args.read_processes),
        "--prefetch-mib", str(args.prefetch_mib), "--block-size-kib", str(args.block_size_kib),
        '--read-mode', args.read_mode, '--download-concurrency', str(args.download_concurrency),
        '--staging-mib', str(args.staging_mib),
        "--skip-index", "--pipeline", "legacy"]
    if args.read_profiles:
        from .jobs_policy import validate_profiles
        validate_profiles(json.loads(Path(args.read_profiles).read_text()))
        command_base += ["--read-profiles", str(Path(args.read_profiles).resolve())]
    if args.reuse_cmipf_2km_from:
        command_base += ["--reuse-cmipf-2km-from", args.reuse_cmipf_2km_from]
    plan = []
    for name, start, end in stages:
        for month_start, month_end in _months(start, end):
            report_dir = Path(args.output) / name / f"{month_start:%Y-%m}"
            command = command_base + ["--start", month_start.isoformat(), "--end", month_end.isoformat(),
                "--output", str(report_dir)]
            plan.append({"stage": name, "start": month_start.isoformat(),
                "end_excluded": month_end.isoformat(), "report_dir": str(report_dir),
                "command": command})
    if args.begin_month:
        begin_index = next((i for i, month in enumerate(plan)
                            if month["start"][:7] == args.begin_month), None)
        if begin_index is None:
            parser.error("--begin-month is not part of the chosen --phase")
        plan = plan[begin_index:]
    if args.stop_after_month:
        stop_index = next((i for i, month in enumerate(plan)
                           if month["start"][:7] == args.stop_after_month), None)
        if stop_index is None:
            parser.error("--stop-after-month is not part of the chosen --phase")
        plan = plan[:stop_index + 1]
    if args.pipeline=='shared' and args.product=='ABI-L2-CMIPF':
        if args.dry_run:
            print(json.dumps({'monthly_runs':[{k:v for k,v in row.items() if k!='command'} for row in plan],'global_config':shared_config.manifest(),
                'scope':'one store-owner process per month; separate C02 tail slots'},indent=2))
            return 0
        return run_shared(plan,args,shared_config)
    month_parallel = min(args.monthly_writers, 2) if args.product == "ABI-L2-MCMIPF" else 1
    if args.dry_run:
        print(json.dumps({"stages": [row[0] for row in stages], "bands": BANDS,
            "scans_per_hour": "all available", "product": args.product,
            "stop_after_month": args.stop_after_month, "monthly_runs": plan}, indent=2))
        return 0

    from .jobs_mrms import preflight
    preflight(args.destination)
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    write_json(output / "run_config.json", {"stages": [row[0] for row in stages],
        "bands": BANDS, "scan_selection": "all available", "satellite": "auto",
        "destination": str(Path(args.destination).resolve()), "workers": args.workers,
        "monthly_writers": args.monthly_writers, "decode_workers": args.decode_workers,
        "product": args.product, "archive_product": archive_product,
        "read_processes": args.read_processes,
        'read_mode': args.read_mode, 'download_concurrency': args.download_concurrency,
        'staging_mib': args.staging_mib,
        "prefetch_mib": args.prefetch_mib, "block_size_kib": args.block_size_kib,
        "read_profiles": json.loads(Path(args.read_profiles).read_text()) if args.read_profiles else None,
        "stop_after_month": args.stop_after_month,
        "begin_month": args.begin_month,
        "reuse_cmipf_2km_from": args.reuse_cmipf_2km_from,
        "month_parallel": month_parallel,
        "start_order": "H1 2021 through H1 2026, ascending half-year stages"})
    completed = []
    start_time = time.perf_counter()
    def run_single(month):
        report_dir = Path(month["report_dir"])
        command = month["command"]
        saved_selection = _reusable_selection(report_dir, month["start"], month["end_excluded"], args.product)
        if saved_selection:
            command = [*command, "--selection", str(saved_selection)]
            print(f"Reusing saved STAC selection for {month['start']}", flush=True)
        print(f"Fetching GOES {month['start']} through {month['end_excluded']}", flush=True)
        with subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,env=snapshot_environment()) as child:
            for line in child.stdout:print(line,end='',flush=True)
            returncode=child.wait()
        result=__import__('types').SimpleNamespace(returncode=returncode)
        if result.returncode:
            raise RuntimeError(f"GOES {month['start']} exited {result.returncode}: {command}")
        return _verify_month(report_dir)

    def run_month(month):
        segments=satellite_segments(month)
        if len(segments)==1:return run_single(month)
        results=[run_single(segment) for segment in segments]
        reports=[result[0] for result in results]
        from .common import digest
        combined={**reports[0],'selection_id':digest([r['selection_id'] for r in reports]),
            'selection_summary':{'source':'goes','product':args.product,'start':month['start'],'end':month['end_excluded'],
                'satellite':'separate GOES-16/19 archives','bands':BANDS},
            'monthly_archives':[a for r in reports for a in r['monthly_archives']],
            'records_count':sum(r.get('records_count',len(r.get('records',[]))) for r in reports),
            'read_bytes':sum(r.get('read_bytes',0) for r in reports),
            'stored_bytes':sum(r.get('stored_bytes',0) for r in reports),
            'wall_s':sum(r.get('wall_s',0) for r in reports),
            'selection_catalogs':[str(Path(seg['report_dir'])/args.product/'collection.json') for seg in segments],
            'scope':'two sequential satellite segments; separate native stores, no alignment'}
        return combined,[a for result in results for a in result[1]],sorted({b for result in results for b in result[2]})

    for offset in range(0, len(plan), month_parallel):
        if (output/'pause.request').exists() or time.perf_counter()-start_time>=args.max_hours*3600:
            write_json(output/'paused.json',{'status':'paused','next_month':plan[offset]['start'],'reason':'signal/pause or wall-time boundary'})
            return 75
        batch = plan[offset:offset + month_parallel]
        pending = []
        for month in batch:
            checkpoint = Path(args.output) / "checkpoints" / month["stage"] / f"{month['start']}.json"
            if checkpoint.is_file():
                saved = json.loads(checkpoint.read_text())
                valid = bool(saved.get("archives")) and all(
                    Path(item["path"]).is_file() and
                    (Path(item["path"]).parent / "complete.json").is_file()
                    for item in saved["archives"])
                if valid and checkpoint_matches(saved, archive_product):
                    completed.append(saved)
                    print(f"Reused checkpoint {month['start']}", flush=True)
                    continue
            pending.append(month)
        if pending:
            with ThreadPoolExecutor(max_workers=len(pending)) as pool:
                futures = {pool.submit(run_month, month): month for month in pending}
                failed = []
                for future in as_completed(futures):
                    month = futures[future]
                    try:
                        report, archives, available_bands = future.result()
                        from ecore_weather.index import record_fetch
                        record_fetch(report)  # The coordinator alone owns DuckDB writes.
                        row = {"status": "complete", "product": archive_product,
                            "stage": month["stage"], "start": month["start"],
                            "end_excluded": month["end_excluded"],
                            "selection_id": report["selection_id"],
                            "selected_assets": report.get("records_count", len(report.get("records", []))),
                            "available_bands": available_bands,
                            "missing_bands": sorted(set(BANDS) - set(available_bands)),
                            "archives": archives, "read_bytes": report.get("read_bytes", 0),
                            "stored_bytes": report.get("stored_bytes", 0),
                            "wall_seconds": report.get("wall_s")}
                        checkpoint = Path(args.output) / "checkpoints" / month["stage"] / f"{month['start']}.json"
                        write_json(checkpoint, row)
                        completed.append(row)
                        print(f"Completed {month['start']}: {row['selected_assets']} source scans, "
                              f"{len(archives)} archive(s)", flush=True)
                    except Exception as exc:
                        failed.append({"stage": month["stage"], "month": month["start"],
                                       "error": f"{type(exc).__name__}: {exc}"})
                if failed:
                    write_json(output / "failure.json", {"failed_months": failed})
                    return 1
        for stage_name, stage_start, stage_end in stages:
            stage_rows = [r for r in completed if r['stage'] == stage_name]
            if stage_rows:
                expected = len(list(_months(stage_start,stage_end)))
                write_json(output / stage_name / 'summary.json', {'stage':stage_name,
                    'product':archive_product,'status':'complete' if len(stage_rows)==expected else 'partial',
                    'verified_months':len(stage_rows),'expected_months':expected,'months':stage_rows})
        if args.stop_after_month and batch[-1]["start"][:7] == args.stop_after_month:
            write_json(output / "paused.json", {"status": "paused", "reason": "stop-after-month",
                "month": batch[-1]["start"], "product": archive_product})
            return 75
    for stage_name, stage_start, stage_end in stages:
        stage_rows = [r for r in completed if r["stage"] == stage_name]
        write_json(output / stage_name / "summary.json", {"stage": stage_name,
            "product": archive_product, "months": stage_rows,
            "status": "complete" if len(stage_rows) == len(list(_months(stage_start,stage_end))) else "partial"})
    completed.sort(key=lambda row: row["start"])
    write_json(output / "summary.json", {"status": "complete", "stages": [r[0] for r in stages],
        "completed_months": len(completed), "elapsed_seconds": time.perf_counter()-start_time,
        "months": completed})
    return 0





def run_shared(plan,args,config):
    """Chronological admission without a whole-month completion barrier."""
    from . import catalog,goes
    from .goes_shared import fetch_contexts
    from .jobs_mrms import preflight
    preflight(args.destination)
    output=Path(args.output);output.mkdir(parents=True,exist_ok=True)
    os.environ['ECORE_SHARED_GOES']='1'
    write_json(output/'run_config.json',{'schema':2,'product':args.product,'bands':BANDS,
        'global_config':config.manifest(),'begin_month':args.begin_month,
        'stop_after_month':args.stop_after_month,'start_order':'ascending half-year stages'})
    started=time.perf_counter();completed=[];requests=[];metadata={};segments_done={}
    def checkpoint(month):return output/'checkpoints'/month['stage']/f"{month['start']}.json"
    def monthly_requests():
        # Validate completed months when chronological admission reaches them,
        # not by rereading every future ZIP before the first unfinished month.
        for month in plan:
            path=checkpoint(month)
            if path.is_file() and checkpoint_matches(json.loads(path.read_text()),args.product):
                completed.append(json.loads(path.read_text()));continue
            segments=satellite_segments(month)
            segments_done[month['start']]=[]
            for i,segment in enumerate(segments):
                key=f"{month['start'][:7]}-{i}";metadata[key]=(month,segment,len(segments))
                def load(segment=segment):
                    saved=_reusable_selection(segment['report_dir'],segment['start'],segment['end_excluded'],args.product)
                    if saved:selection=catalog.load_selection(saved)
                    else:
                        satellite=16 if utc(segment['start'])<utc('2025-04-07T15:00:00Z') else 19
                        selection=goes.discover(segment['start'],segment['end_excluded'],bands=BANDS,satellite=satellite)
                    catalog.save_selection(selection,Path(segment['report_dir'])/args.product)
                    return selection
                yield (key,load,str(Path(segment['report_dir'])/args.product))
    requests=monthly_requests()
    def finished(key,report):
        if report.get('interrupted'):raise OSError(f"Failed GOES month {key}: {report['monthly_archives']}")
        month,segment,count=metadata[key]
        segments_done[month['start']].append(report)
        if len(segments_done[month['start']])!=count:return
        reports=segments_done.pop(month['start']);archives=[a for r in reports for a in r['monthly_archives']]
        bands=sorted({a['band'] for a in archives})
        row={'status':'complete' if archives else 'unavailable','product':args.product,
            'stage':month['stage'],'start':month['start'],'end_excluded':month['end_excluded'],
            'selected_assets':sum(r.get('selection_summary',{}).get('files',0) for r in reports),
            'archives':archives,'available_bands':bands,'missing_bands':sorted(set(BANDS)-set(bands)),
            'read_bytes':sum(r.get('read_bytes',0) for r in reports),
            'stored_bytes':sum(r.get('stored_bytes',0) for r in reports),
            'wall_seconds':max((r.get('wall_s',0) for r in reports),default=0),
            'timing_scope':'current attempt; maximum satellite-context wall time; concurrent global resources',
            'global_config':config.manifest()}
        write_json(checkpoint(month),row);completed.append(row)
        if args.shared_profile and any(a.get('status')=='saved' for a in archives):
            review=output/'first-month-review.json'
            previous=json.loads(review.read_text()) if review.exists() else {}
            if previous.get('global_config')!=config.manifest():
                from .goes_rollout import production_review
                choice=json.loads(Path(args.shared_profile).read_text())
                write_json(review,production_review(row,choice))
        print(f"Completed and verified GOES {month['start']}: {len(archives)} archives",flush=True)
        stage_rows=[r for r in completed if r['stage']==month['stage']]
        expected=sum(m['stage']==month['stage'] for m in plan)
        write_json(output/month['stage']/'summary.json',{'stage':month['stage'],
            'verified_months':len(stage_rows),'expected_months':expected,
            'status':'complete' if len(stage_rows)==expected else 'partial','months':stage_rows})
    def paused():return (output/'pause.request').exists() or time.perf_counter()-started>=args.max_hours*3600
    try:
        result=fetch_contexts(requests,args.destination,config,args.scratch,True,paused,finished)
    except BaseException as exc:
        write_json(output/'failure.json',{'status':'failed','error':f'{type(exc).__name__}: {exc}'})
        raise
    summary={'status':'paused' if result['paused'] or args.stop_after_month else 'complete',
        'completed_months':len(completed),'months':sorted(completed,key=lambda r:r['start']),
        'elapsed_seconds':time.perf_counter()-started,'global_config':config.manifest(),
        'global_metrics':result['global_metrics']}
    write_json(output/'summary.json',summary)
    if summary['status']=='paused':write_json(output/'paused.json',summary);return 75
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
