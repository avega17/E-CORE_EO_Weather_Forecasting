"""Resumable CARIB MRMS study fetch, one product-month per checkpoint."""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json
import multiprocessing
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


def _fetch_selection(selection, destination, workers, scratch, decode_workers):
    """A worker owns one product-month store; only the parent writes DuckDB."""
    return monthly.fetch(selection, destination=destination, workers=workers,
        backend="obstore", scratch=scratch, decode_workers=decode_workers,
        monthly_writers=1, report_dir=None, index_results=False)


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


def _nominal_items(begin, end):
    hours = (utc(end)-utc(begin)).total_seconds()/3600
    return int(hours*sum(1 if mrms.PRODUCTS[p]["frequency_minutes"] == 60 else 6
                         for p in mrms.DEFAULT_PRODUCTS))


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


def _continuation(output, measured_seconds, measured_items, measured_bytes, destination, scratch=None):
    remaining = _nominal_items(datetime(2022,1,1,tzinfo=timezone.utc), STUDY_END)
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


def main(argv=None):
    p = argparse.ArgumentParser(description="Fetch native CARIB MRMS subsets to verified monthly archives")
    p.add_argument("--phase", choices=["auto", "first-year", "remaining", "all"], default="all")
    p.add_argument("--start", help="UTC start override for a short validation run")
    p.add_argument("--end", help="Excluded UTC end override for a short validation run")
    p.add_argument("--destination", default="/mnt/p/ecore_eo_datasets")
    p.add_argument("--output", default="results/study-mrms")
    p.add_argument("--scratch", help="Fast Linux-local monthly build directory")
    p.add_argument("--bbox", nargs=4, type=float, default=PR_BBOX,
                   metavar=("WEST", "SOUTH", "EAST", "NORTH"))
    p.add_argument("--products", nargs="+", choices=mrms.DEFAULT_PRODUCTS,
                   help="Limit a validation/resume run to selected default products")
    p.add_argument("--workers", type=int, default=8,
                   help="Concurrent NOAA source reads per monthly archive (bounded at 16); does not add writer processes")
    p.add_argument("--decode-workers", type=int, default=1,
                   help="Concurrent MRMS GRIB decode slots within each monthly writer process")
    p.add_argument("--monthly-writers", type=int, default=4)
    p.add_argument("--max-hours", type=float, default=12.)
    p.add_argument("--dry-run", action="store_true", help="Show schedule without NOAA or DAS writes")
    args = p.parse_args(argv)
    if args.workers < 1 or args.decode_workers < 1 or args.monthly_writers < 1 or args.max_hours <= 0:
        p.error("Worker counts and max-hours must be positive")
    if bool(args.start) != bool(args.end):
        p.error("Provide both --start and --end for a validation window")
    selected_products = tuple(args.products or mrms.DEFAULT_PRODUCTS)
    custom = bool(args.start)
    start = utc(args.start) if custom else STUDY_START if args.phase != "remaining" else datetime(2022,1,1,tzinfo=timezone.utc)
    end = utc(args.end) if custom else (datetime(2022,1,1,tzinfo=timezone.utc)
        if args.phase == "first-year" else STUDY_END)
    if not custom and args.phase == "remaining":
        plan_file = Path(args.output) / "continuation_plan.json"
        if plan_file.is_file() and json.loads(plan_file.read_text()).get("mode") == "annual":
            for year in range(2022, 2027):
                annual_start = datetime(year, 1, 1, tzinfo=timezone.utc)
                annual_end = min(datetime(year+1, 1, 1, tzinfo=timezone.utc), STUDY_END)
                complete = all(_checkpoint_ok(Path(args.output) / "months" /
                    f"{month_start:%Y-%m}" / f"{product}.json")
                    for month_start, _ in months(annual_start, annual_end)
                    for product in mrms.DEFAULT_PRODUCTS)
                if not complete:
                    start, end = annual_start, annual_end
                    break
            else:
                print("All remaining study months already have verified checkpoints.")
                return 0
    if start >= end or (not custom and (start < STUDY_START or end > STUDY_END)):
        p.error("Invalid study interval")
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    snapshot_manifest = Path(__file__).resolve().parents[2] / "snapshot.json"
    snapshot_sha256 = (json.loads(snapshot_manifest.read_text())["sha256"]
                       if snapshot_manifest.is_file() else None)
    config = {"phase": args.phase, "start": iso(start), "end_excluded": iso(end),
        "bbox": args.bbox, "products": selected_products, "destination": args.destination,
        "workers": args.workers, "decode_workers": args.decode_workers,
        "monthly_writers": args.monthly_writers, "max_hours": args.max_hours,
        "git_revision": revision, "code_snapshot_sha256": snapshot_sha256,
        "python": sys.executable}
    if args.dry_run:
        print(json.dumps({**config, "nominal_slots": _nominal_items(start, end)}, indent=2))
        return 0
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    _record_run_config(output, config)
    try:
        preflight(args.destination)
    except Exception as exc:
        write_json(output / "failure.json", {"stage": "preflight", "error": str(exc)})
        print(f"Preflight failed: {exc}", file=sys.stderr)
        return 1
    began = time.monotonic()
    successful_items = 0
    successful_seconds = 0.
    stored_bytes = 0
    summary = []
    try:
        with ProcessPoolExecutor(max_workers=args.monthly_writers,
                mp_context=multiprocessing.get_context("spawn")) as pool:
          for month_start, month_end in months(start, end):
            if (output/'pause.request').exists() or time.monotonic()-began > args.max_hours*3600:
                write_json(output / "paused.json", {"reason": "max-hours reached at month boundary",
                    "next_month": iso(month_start), "summary": summary})
                print(f"Paused before {month_start:%Y-%m}; resume with the same command", flush=True)
                return 75
            # A partially specified month would violate canonical month layout.
            if month_start.day != 1 or month_start.hour != 0 or month_start.minute != 0:
                if not custom:
                    raise ValueError("Study month boundary is not UTC midnight")
            pending = []

            def finish(task):
                nonlocal successful_items, successful_seconds, stored_bytes
                selection, original_selection, invalid_sources, checkpoint, product, started, future = task
                report = future.result()
                from ecore_weather.index import record_fetch
                record_fetch(report)
                failures = [archive for archive in report["monthly_archives"]
                            if archive["status"] == "failed"]
                if failures:
                    raise IOError(f"{product} {month_start:%Y-%m}: {failures}")
                archives = [archive["path"] for archive in report["monthly_archives"]]
                row = {"status": "complete", "month": f"{month_start:%Y-%m}",
                    "product": product, "expected_slots": len(original_selection.expected_times),
                    "listed_slots": len(original_selection.assets),
                    "matched_slots": len(selection.assets),
                    "missing_slots": max(0, len(original_selection.expected_times)-len(selection.assets)),
                    "invalid_source_files": invalid_sources,
                    "source_listed_bytes": sum(asset.size for asset in original_selection.assets),
                    "source_read_bytes": report["read_bytes"],
                    "stored_bytes": report["stored_bytes"], "wall_seconds": time.perf_counter()-started,
                    "archives": archives, "selection_id": original_selection.id}
                write_json(checkpoint, row)
                summary.append(row)
                successful_items += len(selection.assets)
                successful_seconds += row["wall_seconds"]
                stored_bytes += report["stored_bytes"]
                print(f"Saved {month_start:%Y-%m} {product}: {len(selection.assets)} observations, "
                      f"{row['wall_seconds']:.1f}s", flush=True)

            for product in selected_products:
                checkpoint = output / "months" / f"{month_start:%Y-%m}" / f"{product}.json"
                if _checkpoint_ok(checkpoint):
                    print(f"Reused {month_start:%Y-%m} {product}", flush=True)
                    continue
                started = time.perf_counter()
                original_selection = mrms.discover(month_start, month_end, bbox=args.bbox,
                    product=product, tolerance_minutes=5, time_match="previous")
                if not original_selection.assets:
                    row = {"status": "unavailable", "month": f"{month_start:%Y-%m}",
                        "product": product, "expected_slots": len(original_selection.expected_times),
                        "matched_slots": 0, "reason": "No historical CARIB objects matched this product-month"}
                    write_json(checkpoint, row)
                    summary.append(row)
                    print(f"Unavailable {month_start:%Y-%m} {product}", flush=True)
                    continue
                # Keep the full catalog selection. Empty NOAA keys are unavailable
                # files, not valid zero-rain observations or pixel sentinels.
                catalog.save_selection(original_selection,
                    checkpoint.parent / f"{product}-selection")
                selection, invalid_sources = partition_empty_sources(original_selection)
                if not selection.assets:
                    raise IOError(f"{product} {month_start:%Y-%m}: all listed NOAA objects are empty")
                # A real source read must succeed before starting the monthly build.
                with Transport("obstore") as transport:
                    sample, _ = mrms.read(selection.assets[0], args.bbox, product, transport)
                    if "measurement" not in sample or "bitmap_valid" not in sample:
                        raise IOError(f"Representative source lacks data or bitmap: {product}")
                future = pool.submit(_fetch_selection, selection, args.destination,
                    args.workers, args.scratch, args.decode_workers)
                pending.append((selection, original_selection, invalid_sources,
                                checkpoint, product, started, future))
                if len(pending) >= args.monthly_writers:
                    finish(pending.pop(0))
            for task in pending:
                finish(task)
            if not custom and args.phase in {"auto", "first-year"} and month_end.year == 2022 and month_end.month == 1:
                seconds_2021, items_2021, bytes_2021 = _year_metrics(output, 2021)
                plan = _continuation(output, seconds_2021, items_2021,
                                     bytes_2021, args.destination, args.scratch)
                print(f"2021 complete; recommended continuation: {plan['mode']}", flush=True)
                if args.phase == "auto" and plan["mode"] == "annual":
                    print("Stopping after the first year; use the saved continuation command for later years.", flush=True)
                    return 75
        write_json(output / "summary.json", {"status": "complete", "config": config,
            "new_successful_items": successful_items, "new_stored_bytes": stored_bytes,
            "elapsed_seconds": time.monotonic()-began, "months": summary})
        return 0
    except Exception as exc:
        write_json(output / "failure.json", {"stage": "fetch", "error": f"{type(exc).__name__}: {exc}",
            "completed_this_run": summary})
        print(f"MRMS job stopped: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
