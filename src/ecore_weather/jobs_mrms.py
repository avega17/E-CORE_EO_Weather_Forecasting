"""Resumable CARIB MRMS study fetch, one product-month per checkpoint."""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
from dataclasses import replace
from datetime import datetime, timezone, timedelta
import hashlib
import json
import multiprocessing
import os
import threading
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time



import numpy as np
import xarray as xr

from ecore_weather import catalog, monthly, mrms
from ecore_weather.common import PR_BBOX, Transport, iso, utc
from ecore_weather.runlog import save as write_json
from ecore_weather.earth2_io import write_dataset
from ecore_weather.storage import open_raw
from ecore_weather.study_goes import months

STUDY_START = datetime(2021, 1, 1, tzinfo=timezone.utc)
STUDY_END = datetime(2026, 7, 1, tzinfo=timezone.utc)


def partition_empty_sources(selection):
    """Exclude catalog-listed empty objects from decoding while retaining provenance."""
    if any(asset.size < 0 for asset in selection.assets):
        raise ValueError("NOAA catalog returned a source object with a negative size")
    empty = [asset for asset in selection.assets if asset.size == 0]
    readable = [asset for asset in selection.assets if asset.size > 0]
    invalid = [{"asset_id": asset.id, "bucket": asset.bucket, "key": asset.key,
        "source_url": asset.url, "observation_time": asset.time, "size": asset.size,
        "etag": asset.etag, "reason": "zero_byte_noaa_object"}
        for asset in empty]
    return replace(selection, assets=readable), invalid


def _fetch_selection(selection, destination, workers, scratch, decode_workers, max_inflight=8, roi_queue_mib=128):
    """A worker owns one product-month store; only the parent writes DuckDB."""
    from .mrms_profile import cap_library_threads
    cap_library_threads()
    return monthly.fetch(selection, destination=destination, workers=workers,
        backend="obstore", scratch=scratch, decode_workers=decode_workers,
        monthly_writers=1, report_dir=None, index_results=False, max_inflight=max_inflight, roi_queue_mib=roi_queue_mib)


def preflight(destination):
    """Verify the actual mounted destination and current Earth2Studio writer."""
    root = Path(destination)
    if str(root.resolve()).startswith('/mnt/p/'):
        mount=subprocess.check_output(['findmnt','-n','-o','SOURCE,FSTYPE','-T',str(root)],text=True)
        if 'P:' not in mount or not any(fs in mount for fs in ('9p','drvfs')):
            raise OSError('/mnt/p must be the mounted Windows P: drive, not a root-disk directory')
    if not root.is_dir():
        raise FileNotFoundError(f"Mount the P: dataset directory first: {root}")
    with tempfile.TemporaryDirectory(prefix=".ecore-preflight-", dir=root) as location:
        test = Path(location)
        probe = test / "write-probe"
        probe.write_bytes(b"ecore")
        if probe.read_bytes() != b"ecore":
            raise IOError("DAS write/read probe failed")
        ds = xr.Dataset({"measurement": (("time", "latitude", "longitude"),
            np.array([[[0., -1.], [2., 3.]]], dtype="float32")),
            "bitmap_valid": (("time", "latitude", "longitude"),
            np.array([[[1, 1], [0, 1]]], dtype="uint8"))},
            coords={"time": np.array(["2021-01-01"], dtype="datetime64[ns]"),
                    "latitude": [18., 17.], "longitude": [-67., -66.]})
        path = test / "earth2.zarr"
        write_dataset(ds, path)
        with open_raw(path) as reopened:
            for name in ("measurement", "bitmap_valid"):
                np.testing.assert_array_equal(reopened[name].values, ds[name].values)
                if reopened[name].dtype != ds[name].dtype:
                    raise IOError(f"Earth2Studio changed {name} dtype")


def _checkpoint_ok(path):
    if not path.is_file():
        return False
    try:
        report = json.loads(path.read_text())
        if report.get("status") == "unavailable":
            return True
        if report.get("status") != "complete":
            return False
        from .goes_monthly import _file_sha256
        for location in report.get("archives", []):
            if not Path(location).is_file() or not (Path(location).parent / "complete.json").is_file():
                return False
            marker=json.loads((Path(location).parent/"complete.json").read_text())
            if marker.get("archive_sha256") and _file_sha256(location)!=marker["archive_sha256"]:return False
        return bool(report.get("archives"))
    except (ValueError, KeyError, OSError):
        return False


def _nominal_items(begin, end, products=None):
    hours = (utc(end)-utc(begin)).total_seconds()/3600
    return int(hours*sum(1 if mrms.PRODUCTS[p]["frequency_minutes"] == 60 else 6
                         for p in (products or mrms.DEFAULT_PRODUCTS)))


def _year_metrics(output, year):
    rows = []
    for path in (Path(output) / "months").glob(f"{year}-*/*.json"):
        try:
            row = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if row.get("status") == "complete":
            rows.append(row)
    return (sum(row.get("wall_seconds", 0) for row in rows),
            sum(row.get("matched_slots", 0) for row in rows),
            sum(row.get("stored_bytes", 0) for row in rows))


def _record_run_config(output, config):
    """Keep one current request; coordinator run records carry reproducibility."""
    current=json.loads(json.dumps(config,sort_keys=True,default=str))
    write_json(Path(output)/'run_config.json',current)


def _continuation(output, measured_seconds, measured_items, measured_bytes, destination, scratch=None, products=None):
    remaining = _nominal_items(datetime(2022,1,1,tzinfo=timezone.utc), STUDY_END, products)
    if measured_items < 1:
        raise ValueError("No successful 2021 items; cannot estimate continuation")
    seconds = 1.3*measured_seconds/measured_items*remaining
    bytes_needed = 1.3*measured_bytes/measured_items*remaining
    scratch_allowance = 2*measured_bytes/12
    free = shutil.disk_usage(destination).free
    scratch_free = shutil.disk_usage(scratch or tempfile.gettempdir()).free
    space_ok = free >= 1.25*bytes_needed and scratch_free >= scratch_allowance
    mode = "single-monthly-continuation" if seconds <= 12*3600 and space_ok else "annual"
    result = {"mode": mode, "remaining_nominal_items": remaining,
        "estimated_seconds_with_30pct_margin": seconds,
        "estimated_stored_bytes_with_30pct_margin": bytes_needed,
        "scratch_allowance_bytes": scratch_allowance,
        "free_bytes": free, "scratch_free_bytes": scratch_free,
        "space_gate_passed": space_ok,
        "next_command": f"{sys.executable} scripts/dataset_jobs.py fetch mrms --phase remaining --output {output}"}
    write_json(Path(output) / "continuation_plan.json", result)
    return result


def _request_identity(begin, end, product, bbox):
    return {"start": iso(begin), "end_excluded": iso(end), "product": product, "bbox": list(bbox)}


def source_month_schedule(start,end,products):
    """Partition by actual source month so no simultaneous tasks share a ZIP."""
    first=(utc(start)-timedelta(minutes=5)).replace(day=1,hour=0,minute=0,second=0,microsecond=0)
    return [(begin,stop,product) for begin,stop in months(first,end) for product in products]


def discover_source_month(begin,stop,product,bbox,study_start,study_end):
    """Retain causal request slots and assign boundary observations exactly once."""
    cadence=60 if mrms.PRODUCTS[product]['frequency_minutes']==60 else 10
    requested_start=max(begin,study_start)
    requested_end=min(stop+timedelta(minutes=cadence),study_end)
    original=mrms.discover(requested_start,requested_end,bbox=bbox,product=product,
                           tolerance_minutes=5,time_match='previous')
    assets=[a for a in original.assets if begin<=utc(a.time)<stop]
    ids={a.id for a in assets}
    matched={r['slot_time']:r for r in original.hourly_matches}
    # Matched boundary slots belong to their source month. Unmatched slots
    # belong to their requested month, preserving global gap accounting.
    expected=tuple(t for t in original.expected_times
        if (matched[t]['asset_id'] in ids if t in matched else begin<=utc(t)<stop))
    matches=tuple(r for r in original.hourly_matches if r['asset_id'] in ids)
    return replace(original,assets=assets,expected_times=expected,hourly_matches=matches)


def _receipt_matches(path, request):
    if not _checkpoint_ok(path):
        return False
    row = json.loads(path.read_text())
    return row.get("request") == request


def _terminate(pool):
    """Do not let executor shutdown wait beyond PBS wall time."""
    processes = list((getattr(pool, "_processes", None) or {}).values())
    for process in processes:
        if process.is_alive():
            process.terminate()
    for process in processes:
        process.join(timeout=3)
        if process.is_alive():
            process.kill()
            process.join(timeout=3)
    pool.shutdown(wait=False, cancel_futures=True)


def main(argv=None):
    from .mrms_profile import add_arguments, resolve, hardware, code_identity, cap_library_threads
    p = argparse.ArgumentParser(description="Fetch native CARIB MRMS subsets to verified monthly archives")
    p.add_argument("--phase", choices=["auto", "first-year", "remaining", "all"], default="all")
    p.add_argument("--start", help="UTC start override")
    p.add_argument("--end", help="Excluded UTC end override")
    p.add_argument("--destination", default="/mnt/p/ecore_eo_datasets")
    p.add_argument("--output", default="results/study-mrms")
    p.add_argument("--scratch", help="Fast Linux-local monthly build directory")
    p.add_argument("--bbox", nargs=4, type=float, default=PR_BBOX, metavar=("WEST", "SOUTH", "EAST", "NORTH"))
    p.add_argument("--products", nargs="+", type=mrms.parse_product_argument, help="NOAA product keys or readable aliases")
    add_arguments(p)
    p.add_argument("--max-hours", type=float, default=12., help="Hard application runtime; PBS should leave an additional minute")
    p.add_argument("--drain-minutes", type=float, default=20., help="Minimum admission-stop margin before hard deadline")
    p.add_argument("--durable-progress", help="Atomic aggregate status JSON on shared storage; no source-object rows")
    p.add_argument("--dry-run", action="store_true", help="Show configuration and schedule without NOAA/storage writes")
    args = p.parse_args(argv)
    try:
        resource_config = resolve(args)
    except (OSError, ValueError) as exc:
        p.error(str(exc))
    if args.max_hours <= 0 or args.drain_minutes < 0:
        p.error("Runtime must be positive and drain margin nonnegative")
    if bool(args.start) != bool(args.end):
        p.error("Provide both --start and --end")
    selected_products = tuple(dict.fromkeys(args.products or mrms.DEFAULT_PRODUCTS))
    custom = bool(args.start)
    start = utc(args.start) if custom else (datetime(2022,1,1,tzinfo=timezone.utc) if args.phase == 'remaining' else STUDY_START)
    end = utc(args.end) if custom else (datetime(2022,1,1,tzinfo=timezone.utc) if args.phase == 'first-year' else STUDY_END)
    if start >= end:
        p.error("Invalid study interval")
    revision, snapshot_sha256 = code_identity()
    config = {"phase": args.phase, "start": iso(start), "end_excluded": iso(end), "bbox": list(args.bbox),
        "products": selected_products, "destination": args.destination, **resource_config['resources'],
        "resource_profile": resource_config, "hardware": hardware(), "max_hours": args.max_hours,
        "drain_minutes": args.drain_minutes, "git_revision": revision, "code_snapshot_sha256": snapshot_sha256,
        "pbs_job_id": os.getenv('PBS_JOBID'), "python": sys.executable}
    schedule = source_month_schedule(start,end,selected_products)
    if args.dry_run:
        print(json.dumps({**config, "nominal_slots": _nominal_items(start,end,selected_products), "product_month_tasks":len(schedule)}, indent=2))
        return 0
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    scratch = Path(args.scratch or tempfile.gettempdir()).resolve()
    scratch.mkdir(parents=True,exist_ok=True)
    args.scratch = str(scratch)
    _record_run_config(output,config)
    try:
        preflight(args.destination)
        if os.getenv('PBS_JOBID') and len(os.sched_getaffinity(0)) < 32:
            raise RuntimeError('Polaris coordinator needs access to at least 32 CPUs; inspect mpiexec binding')
    except Exception as exc:
        write_json(output/'failure.json', {'stage':'preflight','error':str(exc)})
        print(f'Preflight failed: {exc}',file=sys.stderr)
        return 1
    cap_library_threads()
    began = time.monotonic()
    deadline = began+args.max_hours*3600
    pending = {}
    summary = []
    task_times = []
    progress_lock = threading.Lock()
    progress_stop = threading.Event()
    peak_rss = 0
    def publish():
        if not args.durable_progress:
            return
        from .jobs import status
        from .runlog import compact
        from .common import write_json as atomic_json
        data = compact(status(os.getenv('ECORE_INDEX_PATH')))
        if data.get('status')=='index_unavailable':
            return
        data.update(snapshot_at=time.time(), source='mrms', pbs_job_id=os.getenv('PBS_JOBID'))
        atomic_json(Path(args.durable_progress),data)
    def heartbeat():
        while not progress_stop.wait(30):
            try:
                with progress_lock:
                    publish()
            except Exception as exc:
                print(f'Durable progress update: {exc}',file=sys.stderr,flush=True)
    progress_thread = threading.Thread(target=heartbeat,daemon=True)
    progress_thread.start()
    import psutil
    process = psutil.Process()
    pool = ProcessPoolExecutor(max_workers=args.monthly_writers, mp_context=multiprocessing.get_context('spawn'))
    forced = False
    paused = False
    iterator = iter(schedule)
    next_task = next(iterator,None)
    def finish(future):
        begin,stop,product,checkpoint,original,selection,invalid,started = pending.pop(future)
        report = future.result()
        failures = [r for r in report['monthly_archives'] if r['status']=='failed']
        if failures:
            raise IOError(f"{product} {begin:%Y-%m}: {failures}")
        from .index import record_fetch
        record_fetch(report)
        wall = time.monotonic()-started
        rows = report['monthly_archives']
        row = dict(status='complete',month=f'{begin:%Y-%m}',product=product,
            request={**_request_identity(begin,stop,product,args.bbox),
                'study_start':iso(start),'study_end_excluded':iso(end),'partition':'source-month-v1'}, expected_slots=len(original.expected_times),
            listed_slots=len(original.assets), matched_slots=len(selection.assets),
            missing_slots=len(original.expected_times)-len(selection.assets), invalid_source_files=invalid,
            source_listed_bytes=sum(a.size for a in original.assets),source_read_bytes=report['read_bytes'],
            stored_bytes=report['stored_bytes'],wall_seconds=wall, archives=[r['path'] for r in rows],
            monthly_archives=rows,selection_id=original.id,finished_at=time.time())
        write_json(checkpoint,row)
        selection_path=checkpoint.parent/f'{product}-selection'
        if selection_path.is_dir():
            shutil.rmtree(selection_path)
        summary.append(row)
        task_times.append(wall)
        print(f"Saved {begin:%Y-%m} {product}: {len(selection.assets)} observations, {wall:.1f}s",flush=True)
        if os.getenv('ECORE_DURABLE_STATE'):
            from .runlog import closed_snapshot
            closed_snapshot(Path(os.environ['ECORE_DURABLE_STATE'])/'archive_index.duckdb')
        with progress_lock:
            publish()
    try:
        while next_task is not None or pending:
            remaining = deadline-time.monotonic()
            if remaining <= 0:
                paused=True
                forced=True
                _terminate(pool)
                break
            rss = 0
            for proc in [process,*process.children(recursive=True)]:
                try:
                    rss += proc.memory_info().rss
                except psutil.Error:
                    pass
            peak_rss=max(peak_rss,rss)
            if rss > args.memory_gib*1024**3:
                forced=True
                _terminate(pool)
                raise MemoryError(f'Job RSS {rss} exceeds {args.memory_gib} GiB ceiling')
            margin = max(args.drain_minutes*60,1.3*max(task_times,default=0))
            paused = paused or (output/'pause.request').exists() or remaining <= margin
            if next_task is not None and not paused and len(pending)<args.monthly_writers:
                begin,stop,product = next_task
                next_task=next(iterator,None)
                checkpoint=output/'months'/f'{begin:%Y-%m}'/f'{product}.json'
                request={**_request_identity(begin,stop,product,args.bbox),
                         'study_start':iso(start),'study_end_excluded':iso(end),'partition':'source-month-v1'}
                if _receipt_matches(checkpoint,request):
                    print(f'Reused {begin:%Y-%m} {product}',flush=True)
                    continue
                started=time.monotonic()
                selection_path=checkpoint.parent/f'{product}-selection'
                saved=selection_path/'collection.json'
                original=catalog.load_selection(saved) if saved.is_file() else discover_source_month(begin,stop,product,args.bbox,start,end)
                requested_start=max(begin,start)
                cadence=60 if mrms.PRODUCTS[product]['frequency_minutes']==60 else 10
                requested_end=min(stop+timedelta(minutes=cadence),end)
                if (original.start,original.end,original.product,tuple(original.bbox)) != (iso(requested_start),iso(requested_end),product,tuple(args.bbox)):
                    raise ValueError(f'Saved selection differs for {product} {begin:%Y-%m}')
                if not original.assets:
                    row=dict(status='unavailable',month=f'{begin:%Y-%m}',product=product,request=request,
                        expected_slots=len(original.expected_times),matched_slots=0,reason='No historical CARIB objects matched')
                    write_json(checkpoint,row)
                    summary.append(row)
                    continue
                if not saved.exists():
                    catalog.save_selection(original,selection_path)
                selection,invalid=partition_empty_sources(original)
                if not selection.assets:
                    raise IOError(f'{product} {begin:%Y-%m}: all listed NOAA objects are empty')
                future=pool.submit(_fetch_selection,selection,args.destination,args.workers,args.scratch,
                    args.decode_workers,args.max_inflight,args.roi_queue_mib)
                pending[future]=(begin,stop,product,checkpoint,original,selection,invalid,started)
                continue
            if pending:
                done,_=wait(pending,timeout=min(1,max(0,remaining)),return_when=FIRST_COMPLETED)
                for future in done:
                    finish(future)
            elif paused:
                break
        elapsed=time.monotonic()-began
        result=dict(status='paused' if paused else 'complete',config=config,elapsed_seconds=elapsed,
            peak_job_rss_bytes=peak_rss,new_successful_items=sum(r.get('matched_slots',0) for r in summary),
            new_stored_bytes=sum(r.get('stored_bytes',0) for r in summary),months=summary,
            unfinished_product_months=[dict(month=f'{v[0]:%Y-%m}',product=v[2]) for v in pending.values()],
            hard_deadline_reached=forced)
        write_json(output/('paused.json' if paused else 'summary.json'),result)
        return 75 if paused else 0
    except Exception as exc:
        if not forced:
            _terminate(pool)
            forced=True
        write_json(output/'failure.json',dict(stage='fetch',error=f'{type(exc).__name__}: {exc}',completed_this_run=summary))
        print(f'MRMS job stopped: {type(exc).__name__}: {exc}',file=sys.stderr,flush=True)
        return 1
    finally:
        if not forced:
            pool.shutdown(wait=True,cancel_futures=True)
        progress_stop.set()
        progress_thread.join(10)


if __name__ == '__main__':
    raise SystemExit(main())
