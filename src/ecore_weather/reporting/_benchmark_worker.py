"""Internal worker: isolate the mentor's temporary files and global state."""
from ecore_weather.core.paths import repository_root

import contextlib
import importlib.util
import io
import os
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing
from pathlib import Path
from unittest.mock import patch

from ecore_weather.reporting.benchmark import raster_summary
from ecore_weather.storage.catalog import load_selection
from ecore_weather.core.common import PR_BBOX, Transport, utc, write_json


def init_reader(backend):
    import atexit
    global _reader_transport
    _reader_transport = Transport(backend)
    atexit.register(_reader_transport.close)


def processed_asset(args):
    asset, bbox, product = args
    from ecore_weather.sources import mrms
    transport = _reader_transport
    with tempfile.TemporaryDirectory(prefix="ecore-comparison-") as temp:
        before = time.perf_counter()
        try:
            ds, timings = mrms.read(asset, bbox, product, transport, halo=1)
            start = time.perf_counter()
            processed = mrms.process(ds, recipe="legacy_exact")
            processing = time.perf_counter()-start
            path = Path(temp) / f"{asset.id}.tif"
            start = time.perf_counter()
            processed.rio.to_raster(path)
            writing = time.perf_counter()-start
            row = {"asset_id": asset.id, "time": asset.time, "status": "ok", **timings,
                   "decode_process_s": timings["decode_crop_s"]+processing,
                   "write_s": writing, "raster": raster_summary(path), "written_bytes": path.stat().st_size,
                   "elapsed_s": time.perf_counter()-before}
            path.unlink()
            ds.close()
            row["read_bytes"] = asset.size
            return row
        except Exception as e:
            return {"asset_id": asset.id, "time": asset.time, "status": "failed", "error": str(e)}


def run(selection, variant):
    from ecore_weather.sources import mrms
    transport = Transport("obstore" if variant.startswith("obstore") else "s3fs")
    workers = int(variant.rsplit("-", 1)[1]) if variant != "legacy" else 1
    rows = []
    matches = {m["asset_id"]: m for m in selection.hourly_matches}
    with transport, tempfile.TemporaryDirectory(prefix="ecore-legacy-") as temp:
        original_cwd = Path.cwd()
        os.chdir(temp)
        try:
            if variant == "legacy":
                # Load the original file without rewriting it or its calculations.
                repo_root = Path(os.getenv("ECORE_REPO_ROOT", original_cwd))
                source = repo_root / "PR_rain_512_crop/download_radar_hourly_768_pr.py"
                if not source.exists():
                    source = repository_root() / "PR_rain_512_crop/download_radar_hourly_768_pr.py"
                spec = importlib.util.spec_from_file_location("mentor_hourly", source)
                mentor = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mentor)
                # The reference run keeps the original geographic constants.
                if tuple(selection.bbox) != tuple(PR_BBOX):
                    raise ValueError("Use the mentor's original region for the unchanged legacy benchmark.")
                import s3fs
                from rioxarray.raster_array import RasterArray
                original_get = s3fs.S3FileSystem.get
                original_exists = s3fs.S3FileSystem.exists
                original_write = RasterArray.to_raster
                timers = {}

                def timed(name, fn):
                    def wrapped(*args, **kwargs):
                        start = time.perf_counter()
                        try:
                            if name in {"download_s", "exists_s"} and len(args) > 1 and str(args[1]).startswith("noaa-mrms-pds/CARIB/"):
                                # Adapt source selection, never the mentor's calculations.
                                args = (args[0], f"{asset.bucket}/{asset.key}", *args[2:])
                            return fn(*args, **kwargs)
                        finally:
                            timers[name] = timers.get(name, 0.0) + time.perf_counter()-start
                    return wrapped

                started = time.perf_counter()
                with patch.object(s3fs.S3FileSystem, "get", timed("download_s", original_get)), \
                     patch.object(s3fs.S3FileSystem, "exists", timed("exists_s", original_exists)), \
                     patch.object(RasterArray, "to_raster", timed("write_s", original_write)):
                    for asset in selection.assets:
                        timers.clear()
                        before = time.perf_counter()
                        when = utc(matches.get(asset.id, {}).get("slot_time", asset.time)).replace(tzinfo=None)
                        with contextlib.redirect_stdout(io.StringIO()) as log:
                            ok = mentor.fetch_mrms_radar_hour_768(when, output_dir="outputs")
                        elapsed = time.perf_counter() - before
                        row = {"asset_id": asset.id, "time": asset.time, "status": "ok" if ok else "failed",
                               "slot_time": matches.get(asset.id, {}).get("slot_time", asset.time),
                               "source_selection_override": when != utc(asset.time).replace(tzinfo=None),
                               "elapsed_s": elapsed, **timers}
                        row["decode_process_s"] = max(0, elapsed-sum(timers.values()))
                        if ok:
                            path = next(Path("outputs").rglob(f"pr_radar_768_{when:%Y%m%d_%H0000}.tif"))
                            row.update(raster=raster_summary(path), written_bytes=path.stat().st_size)
                            path.unlink()
                        else:
                            row["error"] = log.getvalue()[-2000:]
                        rows.append(row)
                elapsed = time.perf_counter()-started
                read_bytes = sum(a.size for a, r in zip(selection.assets, rows) if r["status"] == "ok")
            else:
                global _reader_transport
                _reader_transport = transport
                def task(asset):
                    return processed_asset((asset, selection.bbox, selection.product))
                started = time.perf_counter()
                if "-process-" in variant:
                    with ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context("spawn"),
                            initializer=init_reader, initargs=("obstore" if variant.startswith("obstore") else "s3fs",)) as pool:
                        rows = list(pool.map(processed_asset, ((a, selection.bbox, selection.product) for a in selection.assets), chunksize=8))
                else:
                    with ThreadPoolExecutor(max_workers=workers) as pool:
                        rows = list(pool.map(task, selection.assets))
                elapsed, read_bytes = time.perf_counter()-started, sum(r.get("read_bytes", 0) for r in rows)
        finally:
            os.chdir(original_cwd)
    return {"wall_s": elapsed, "read_bytes": read_bytes,
            "succeeded": sum(r["status"] == "ok" for r in rows),
            "download_s": sum(r.get("download_s", 0) for r in rows),
            "decode_process_s": sum(r.get("decode_process_s", 0) for r in rows),
            "write_s": sum(r.get("write_s", 0) for r in rows),
            "written_bytes": sum(r.get("written_bytes", 0) for r in rows), "records": rows}


if __name__ == "__main__":
    selection_file, variant, result_file = sys.argv[1:]
    write_json(result_file, run(load_selection(selection_file), variant))
