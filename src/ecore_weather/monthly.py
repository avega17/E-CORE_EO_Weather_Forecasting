"""Merge fetched native subsets into stable, time-indexed monthly Zarr archives."""

from __future__ import annotations

from contextlib import ExitStack
from dataclasses import replace
from collections import OrderedDict
from concurrent.futures import ProcessPoolExecutor, as_completed
import json
import os
from pathlib import Path
import shutil
import tempfile
import time
import zipfile
import warnings
from datetime import datetime, timezone

import numpy as np
import xarray as xr

from .common import digest, iso, jsonable, utc, write_json
from .storage import (RAW_SCHEMA_VERSION, destination_root, fingerprint_streaming,
                      metadata_fingerprint, open_raw)


def _band(asset):
    import re
    match = re.search(r"-M\dC(\d{2})_", asset.key)
    if match:
        return int(match[1])
    if "ABI-L2-CMIP" in asset.key:
        import re
        match = re.search(r"C(\d{2})_G\d+_", asset.key)
        if match:
            return int(match[1])
    return None


def _identity(selection, band=None):
    return digest({"source": selection.source, "product": selection.product,
                   "satellite": selection.satellite, "bbox": selection.bbox,
                   "band": band})


def _relative_path(selection, asset):
    band = _band(asset) if selection.source == "goes" else None
    base = f"{selection.source}/{selection.product}"
    if selection.satellite:
        base += f"/goes{selection.satellite}"
    if band:
        base += f"/C{band:02d}"
    stamp = utc(asset.time)
    return f"{base}/roi-{_identity(selection, band)}/{stamp:%Y}/{stamp:%m}"


def _marker(location, remote, bucket_root=None):
    marker_url = location.rstrip("/") + "/complete.json"
    if remote:
        from .hf_storage import BucketWriter
        writer = BucketWriter(bucket_root or location)
        key = (location.removeprefix(bucket_root.rstrip("/") + "/")
               if bucket_root else "").rstrip("/")
        return writer, writer.marker(key)
    path = Path(marker_url)
    return None, json.loads(path.read_text()) if path.is_file() else None


def _read_source(path, band, stack):
    ds = stack.enter_context(open_raw(path))
    if band is not None:
        from .goes import science_variables
        variables = science_variables(ds)
        chosen = [name for name in variables if name.endswith(f"C{band:02d}") or name == "CMI"]
        if not chosen:
            raise ValueError(f"The source archive has no C{band:02d} pixels: {path}")
        ancillary = [name for name in ds.data_vars if name.startswith("DQF") and
                     (name.endswith(f"C{band:02d}") or name == "DQF")]
        keep = chosen + ancillary + [name for name in ds.data_vars if ds[name].ndim == 0]
        ds = ds[keep]
        rename = {name: f"{name}_C{band:02d}" for name in keep if name in {"CMI", "DQF"}}
        if rename:
            ds = ds.rename(rename)
    return ds


def _month_store(selection, band, target, month_data, asset_rows, remote, writer):
    """Merge old and new source records, atomically replace, and validate values."""
    import pandas as pd

    parent = Path(tempfile.mkdtemp(prefix="ecore-month-build-"))
    working = parent / "raw.zarr"
    archive = parent / "raw.zarr.zip"
    started = time.perf_counter()
    upload_before = getattr(writer, "upload_seconds", 0.0) if writer else 0.0
    upload_bytes_before = getattr(writer, "upload_bytes", 0) if writer else 0
    readback_before = getattr(writer, "readback_bytes", 0) if writer else 0
    sources = []
    writer_backend = "earth2studio-zarr-backend"
    try:
        with ExitStack() as stack:
            existing_path = target.rstrip("/") + "/raw.zarr.zip"
            if target and ((remote and _marker(target, True, destination_root("hf"))[1])
                           or (not remote and Path(existing_path).exists())):
                existing = _read_source(existing_path, band, stack)
                existing_sources = set(map(str, existing.get("source_asset_id", []).values)) if "source_asset_id" in existing else set()
                sources.append(existing)
            else:
                existing_sources = set()
            for row in month_data:
                if row["asset_id"] in existing_sources:
                    continue
                source = _read_source(row["url"], band, stack).copy(deep=False)
                stamp = utc(row["time"])
                source = source.expand_dims(time=[np.datetime64(stamp.replace(tzinfo=None))])
                source = source.assign_coords(
                    source_asset_id=("time", [row["asset_id"]]),
                    source_url=("time", [row["source_url"]]),
                    source_etag=("time", [row["etag"]]),
                    request_slot_time=("time", [row.get("slot_time") or ""]),
                    request_offset_seconds=("time", [float(row["offset_seconds"])
                        if row.get("offset_seconds") is not None else np.nan]),
                    source_metadata_json=("time", [json.dumps({"dataset": jsonable(source.attrs),
                        "variables": {name: jsonable(source[name].attrs) for name in source.variables}},
                        sort_keys=True)]))
                source.attrs = {"source": selection.source, "product": selection.product,
                    "satellite": selection.satellite, "requested_bbox": list(selection.bbox),
                    "storage_layout": "one product, native region/grid/band, and UTC month per Zarr archive",
                    "preservation": "Source pixel values are unchanged; per-observation metadata is in source_metadata_json."}
                sources.append(source)

            if not sources:
                raise ValueError("Monthly archive has no records to write.")
            if len(sources) == 1:
                combined = sources[0]
            else:
                combined = xr.concat(sources, dim="time", data_vars="all", coords="minimal",
                                     compat="override", join="exact")
            combined = combined.sortby("time")
            if "source_asset_id" in combined.coords:
                _, unique = np.unique(combined.source_asset_id.values.astype(str), return_index=True)
                combined = combined.isel(time=np.sort(unique))
            for name in combined.variables:
                combined[name].encoding = {}
            expected = fingerprint_streaming(combined)
            expected_meta = metadata_fingerprint(combined)
            from .earth2_io import write_dataset
            write_dataset(combined, working)
            with open_raw(working) as reopened:
                if fingerprint_streaming(reopened) != expected or metadata_fingerprint(reopened) != expected_meta:
                    raise IOError("Earth2Studio monthly Zarr read-back changed source values or metadata.")
            asset_rows_all = []
            for source in sources:
                if "source_asset_id" in source.coords:
                    for i in range(source.sizes["time"]):
                        asset_rows_all.append({"asset_id": str(source.source_asset_id.values[i]),
                            "time": iso(source.time.values[i].astype("datetime64[us]").astype(object)),
                            "source_url": str(source.source_url.values[i]),
                            "etag": str(source.source_etag.values[i]),
                            "slot_time": str(source.request_slot_time.values[i]) if "request_slot_time" in source.coords else "",
                            "offset_seconds": (float(source.request_offset_seconds.values[i])
                                if "request_offset_seconds" in source.coords and np.isfinite(source.request_offset_seconds.values[i]) else None)})
            # Source dates are stored as timestamps in the Zarr time coordinate;
            # provenance lists each immutable NOAA key and ETag in the marker.
            with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_STORED, allowZip64=True) as output:
                for file in sorted(working.rglob("*")):
                    if file.is_file():
                        output.write(file, file.relative_to(working).as_posix())
            marker = {"raw_schema_version": RAW_SCHEMA_VERSION, "raw_path": "raw.zarr.zip",
                "source": selection.source, "product": selection.product, "satellite": selection.satellite,
                "band": band, "region": selection.bbox, "asset_ids": [a["asset_id"] for a in asset_rows_all],
                "assets": asset_rows_all, "array_sha256": expected, "metadata_sha256": expected_meta,
                "stored_bytes": archive.stat().st_size, "observations": len(asset_rows_all),
                "writer_backend": writer_backend,
                "updated_at": iso(pd.Timestamp.now(tz="UTC").to_pydatetime())}
        if remote:
            relative = target.removeprefix(destination_root("hf") + "/")
            writer.publish_month(archive, relative + "/raw.zarr.zip", marker)
            # Completion metadata is written by publish_month only after read-back.
            final = target.rstrip("/") + "/raw.zarr.zip"
        else:
            target_path = Path(target)
            target_path.mkdir(parents=True, exist_ok=True)
            temp_archive = target_path / ".raw.zarr.zip.tmp"
            shutil.copyfile(archive, temp_archive)
            os.replace(temp_archive, target_path / "raw.zarr.zip")
            final = str(target_path / "raw.zarr.zip")
            with open_raw(final) as reopened:
                if fingerprint_streaming(reopened) != marker["array_sha256"] or metadata_fingerprint(reopened) != marker["metadata_sha256"]:
                    raise IOError("Published local monthly archive failed read-back validation.")
            write_json(target_path / "complete.json", marker)
        return {"path": final, "marker": marker, "write_seconds": time.perf_counter()-started,
                "hf_upload_seconds": getattr(writer, "upload_seconds", 0.0)-upload_before,
                "hf_upload_bytes": getattr(writer, "upload_bytes", 0)-upload_bytes_before,
                "hf_readback_bytes": getattr(writer, "readback_bytes", 0)-readback_before}
    finally:
        shutil.rmtree(parent, ignore_errors=True)


def _build_month_group(selection, target, items, remote=False, bucket_root=None):
    """Worker entry point: merge one independent product/month archive."""
    from .storage import object_writer
    with object_writer(target):
        writer, marker = _marker(target, remote, bucket_root)
        done = set(marker.get("asset_ids", [])) if marker else set()
        new_items = [item for item in items if item["asset_id"] not in done]
        if not new_items:
            return {"month": utc(items[0]["time"]).strftime("%Y/%m"),
                "band": items[0].get("band"), "path": target + "/raw.zarr.zip",
                "observations": len(done), "status": "reused", "cleanup": []}
        built = _month_store(selection, items[0].get("band"), target, new_items,
                             items, remote, writer)
        # The sources came from isolated temporary staging and are removed only
        # after the monthly archive passed an exact local or remote read-back.
        cleanup = []
        for item in new_items:
            old_root = Path(item["url"]).parent
            if old_root.exists():
                shutil.rmtree(old_root)
            cleanup.append(item["asset_id"])
        return {"month": utc(items[0]["time"]).strftime("%Y/%m"),
            "band": items[0].get("band"), "path": built["path"],
            "observations": built["marker"]["observations"], "status": "saved",
            "stored_bytes": built["marker"]["stored_bytes"],
            "write_seconds": built["write_seconds"], "cleanup": cleanup,
            "hf_upload_seconds": built["hf_upload_seconds"],
            "hf_upload_bytes": built["hf_upload_bytes"],
            "hf_readback_bytes": built["hf_readback_bytes"],
            "writer_backend": built["marker"].get("writer_backend")}


def compact(selection, report, destination="hf", monthly_writers=2):
    """Compact temporary per-source Zarrs into request-independent monthly files."""
    remote = str(destination) == "hf"
    final_root = destination_root(destination)
    from .hf_storage import BucketWriter
    records = report.get("records", [])
    matches = {m.get("asset_id"): m for m in selection.hourly_matches}
    groups = {}
    assets_by_id = {asset.id: asset for asset in selection.assets}
    for row in records:
        if row.get("status") not in {"saved", "reused"} or not row.get("url"):
            continue
        asset = assets_by_id.get(row["asset_id"])
        if asset is None:
            continue
        band = _band(asset) if selection.source == "goes" else None
        timestamp = utc(asset.time)
        base = f"{selection.source}/{selection.product}"
        if selection.satellite:
            base += f"/goes{selection.satellite}"
        group_key = (timestamp.strftime("%Y/%m"), band)
        rel = (f"{base}/C{band:02d}/roi-{_identity(selection, band)}/{timestamp:%Y/%m}" if band
               else f"{base}/roi-{_identity(selection)}/{timestamp:%Y/%m}")
        groups.setdefault((group_key, rel), []).append({**row, "etag": asset.etag, "band": band,
            "subset_id": _identity(selection, band),
            "slot_time": matches.get(asset.id, {}).get("slot_time"),
            "offset_seconds": matches.get(asset.id, {}).get("offset_seconds")})
    tasks = [(f"{final_root}/{relative}", items) for (_, relative), items
             in sorted(groups.items(), key=lambda item: str(item[0]))]
    effective_writers = 1 if remote else max(1, min(int(monthly_writers), len(tasks) or 1))
    writer_mode = "sequential"
    if effective_writers > 1 and tasks:
        import __main__
        import multiprocessing
        from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
        interactive = not getattr(__main__, "__file__", None) or "ipykernel" in str(getattr(__main__, "__file__", ""))
        executor = ThreadPoolExecutor if interactive else ProcessPoolExecutor
        options = {} if interactive else {"mp_context": multiprocessing.get_context("spawn")}
        writer_mode = "threads" if interactive else "processes"
        with executor(max_workers=effective_writers, **options) as pool:
            futures = [pool.submit(_build_month_group, selection, target, items, False, final_root)
                       for target, items in tasks]
            month_rows = [future.result() for future in futures]
    else:
        month_rows = [_build_month_group(selection, target, items, remote, final_root)
                      for target, items in tasks]
    cleaned = {asset_id for row in month_rows for asset_id in row.get("cleanup", [])}
    for row in report.get("records", []):
        if row.get("asset_id") in cleaned:
            row["temporary_source_zarr_removed"] = True
    report["monthly_archives"] = month_rows
    report["storage_layout"] = "monthly Zarr v3 ZIP; one archive per product, sensor, region/grid, month, and native band grid"
    report["hf_upload_bytes"] = sum(row.get("hf_upload_bytes", 0) for row in month_rows)
    report["hf_upload_seconds"] = sum(row.get("hf_upload_seconds", 0) for row in month_rows)
    report["hf_readback_bytes"] = sum(row.get("hf_readback_bytes", 0) for row in month_rows)
    report["monthly_writers"] = effective_writers
    report["monthly_writer_mode"] = writer_mode
    return report


def fetch_local_streaming(selection, destination, workers=None, backend="obstore",
                          scratch=None, report_dir="artifacts/runs", progress=None,
                          decode_workers=1, block_size=1024*1024, monthly_writers=2,
                          index_results=True, read_processes=2, prefetch_mib=512,
                          read_mode="range", download_concurrency=8, staging_mib=4096, read_profiles=None):
    """Read bounded source batches into independent Earth2Studio month stores."""
    from .common import default_workers
    from .monthly_stream import _write_one
    from .storage import destination_root
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import __main__
    import multiprocessing

    started = time.perf_counter()
    started_at=iso(datetime.now(timezone.utc))
    root = destination_root(destination)
    groups = {}
    for asset in selection.assets:
        target = str(Path(root) / _relative_path(selection, asset))
        groups.setdefault((target, _band(asset) if selection.source == "goes" else None), []).append(asset)
    tasks = sorted(groups.items(), key=lambda item: item[0][0])
    count = min(max(1, int(monthly_writers)), len(tasks) or 1)
    main_file = getattr(__main__, "__file__", None)
    interactive = not main_file or not Path(main_file).is_file() or "ipykernel" in str(main_file)
    executor = ThreadPoolExecutor if interactive else ProcessPoolExecutor
    options = {} if interactive else {"mp_context": multiprocessing.get_context("spawn")}
    outcomes = []
    def reader_args(band):
        profile = (read_profiles or {}).get(str(band), {})
        profile = profile.get('profile', profile)
        return (profile.get('workers', workers or default_workers()), backend,
            decode_workers, profile.get('block_size', block_size), scratch,
            profile.get('read_processes', read_processes), profile.get('prefetch_mib', prefetch_mib),
            profile.get('read_mode', read_mode), profile.get('download_concurrency', download_concurrency),
            profile.get('staging_mib', staging_mib))
    def collect(target, band, assets, future=None):
        try:
            result = (future.result() if future else _write_one(selection, assets, target,
                band, *reader_args(band)))
            result.update(month=utc(assets[0].time).strftime("%Y/%m"), band=band)
        except Exception as exc:
            result = {"path": target, "month": utc(assets[0].time).strftime("%Y/%m"),
                "band": band, "status": "failed", "error": f"{type(exc).__name__}: {exc}",
                "read_bytes": 0, "stored_bytes": 0}
        outcomes.append((assets, result))
        if progress:
            progress(sum(len(a) for a, _ in outcomes), len(selection.assets), result)
    if count == 1:
        for (target, band), assets in tasks:
            collect(target, band, assets)
    else:
        with executor(max_workers=count, **options) as pool:
            futures = [(target, band, assets, pool.submit(_write_one, selection, assets, target,
                band, *reader_args(band)))
                for (target, band), assets in tasks]
            by_future = {f: (target,band,assets) for target,band,assets,f in futures}
            for future in as_completed(by_future):
                target, band, assets = by_future[future]
                collect(target, band, assets, future)
    matches = {row["asset_id"]: row for row in selection.hourly_matches}
    records = []
    for assets, result in outcomes:
        for asset in assets:
            match = matches.get(asset.id, {})
            records.append({"asset_id": asset.id, "time": asset.time, "source_url": asset.url,
                "source_bytes": asset.size, "etag": asset.etag, "product": selection.product,
                "url": result["path"] if result["status"] != "failed" else None,
                "status": "archived" if result["status"] == "saved" else result["status"],
                "band": result["band"], "subset_id": _identity(selection, result["band"]),
                "slot_time": match.get("slot_time"), "offset_seconds": match.get("offset_seconds")})
    months = [row for _, row in outcomes]
    report = {"started_at":started_at,"source": selection.source, "selection_id": selection.id,
        "selection_summary": selection.summary(), "root": root,
        "records": records, "monthly_archives": months, "monthly_writers": count,
        "read_bytes": sum(row.get("read_bytes", 0) for row in months),
        "stored_bytes": sum(row.get("stored_bytes", 0) for row in months),
        "wall_s": time.perf_counter()-started, "destination": destination,
        "interrupted": any(row["status"] == "failed" for row in months),
        "storage_layout": "monthly native-grid Earth2Studio Zarr v3 ZIP"}
    if index_results:
        from .index import record_fetch
        record_fetch(report)
    if report_dir is not None:
        from .runlog import save
        save(Path(report_dir) / f"{selection.id}-monthly.json", report)
    return report


def fetch_remote_streaming(selection, workers=None, backend="obstore", scratch=None,
                           report_dir="artifacts/runs", progress=None,
                           decode_workers=1, block_size=1024*1024,
                           index_results=True, remote_root=None):
    """Write verified monthly Zarr directly through Earth2Studio and HF S3."""
    from .remote_async import write_selection_month
    from .index import record_fetch

    started = time.perf_counter()
    root = remote_root or destination_root("hf")
    groups = {}
    for asset in selection.assets:
        relative = _relative_path(selection, asset)
        groups.setdefault((relative, _band(asset) if selection.source == "goes" else None), []).append(asset)
    matches = {row["asset_id"]: row for row in selection.hourly_matches}
    records, months = [], []
    done = 0
    for (relative, band), assets in sorted(groups.items()):
        result = write_selection_month(selection, assets, root, relative, band,
            workers=workers or 4, backend=backend,
            decode_workers=decode_workers, block_size=block_size)
        result.update(month=utc(assets[0].time).strftime("%Y/%m"), band=band)
        months.append(result)
        for asset in assets:
            match = matches.get(asset.id, {})
            records.append({"asset_id": asset.id, "time": asset.time,
                "source_url": asset.url, "source_bytes": asset.size,
                "etag": asset.etag, "product": selection.product, "url": result["path"],
                "status": "archived" if result["status"] == "saved" else "reused",
                "band": band, "subset_id": _identity(selection, band),
                "slot_time": match.get("slot_time"),
                "offset_seconds": match.get("offset_seconds")})
        done += len(assets)
        if progress:
            progress(done, len(selection.assets), result)
    report = {"source": selection.source, "selection_id": selection.id,
        "selection_summary": selection.summary(), "root": root,
        "records": records, "monthly_archives": months, "monthly_writers": 1,
        "read_bytes": sum(row["read_bytes"] for row in months),
        "stored_bytes": sum(row["stored_bytes"] for row in months),
        "hf_upload_bytes": sum(row.get("hf_upload_bytes", 0) for row in months),
        "hf_upload_seconds": sum(row.get("hf_upload_seconds", 0) for row in months),
        "hf_readback_bytes": None,
        "wall_s": time.perf_counter()-started, "destination": "hf",
        "storage_layout": "monthly native-grid Earth2Studio async Zarr v3 objects"}
    if index_results:
        record_fetch(report)
    if report_dir is not None:
        from .runlog import save
        save(Path(report_dir) / f"{selection.id}-monthly.json", report)
    return report


def fetch(selection, destination="hf", workers=None, backend="s3fs", scratch=None,
          report_dir="artifacts/runs", progress=None, decode_workers=1,
          read_processes=0, block_size=1024*1024, monthly_writers=2,
          index_results=True, prefetch_mib=512, reuse_cmipf_root=None,
          read_mode="range", download_concurrency=8, staging_mib=4096, read_profiles=None, shared_config=None):
    """Stage raw objects locally, compact to monthly archives, then clean staging."""
    if shared_config is not None:
        if read_profiles:
            raise ValueError("Legacy per-band resource profiles cannot be used with global shared limits")
        if selection.product != "ABI-L2-CMIPF" or str(destination).startswith("hf"):
            raise ValueError("Shared pipeline requires local native CMIPF")
        from .goes_shared import fetch as shared_fetch
        return shared_fetch(selection, destination, shared_config, scratch, report_dir, index_results, progress)
    if read_profiles:
        if selection.source != 'goes' or selection.product != 'ABI-L2-CMIPF' or str(destination).startswith('hf'):
            raise ValueError('Band read profiles require local native CMIPF')
        from .jobs_policy import validate_profiles
        validate_profiles(read_profiles)
    if read_mode != "range" and (selection.source != "goes" or selection.product != "ABI-L2-CMIPF" or str(destination).startswith("hf")):
        raise ValueError("Async whole-file staging requires native CMIPF and a local destination")
    if selection.source == "goes" and selection.product == "ABI-L2-MCMIPF":
        from . import goes_monthly
        return goes_monthly.fetch(selection, destination,
            workers=workers or 8, reader_processes=read_processes or 2,
            prefetch_mib=prefetch_mib, block_size=block_size,
            monthly_writers=monthly_writers, scratch=scratch,
            report_dir=report_dir, progress=progress, index_results=index_results,
            reuse_cmipf_root=reuse_cmipf_root)
    if str(destination) == "hf":
        return fetch_remote_streaming(selection, workers=workers, backend=backend,
            scratch=scratch, report_dir=report_dir, progress=progress,
            decode_workers=decode_workers, block_size=block_size,
            index_results=index_results)
    else:
        return fetch_local_streaming(selection, destination, workers=workers,
            backend=backend, scratch=scratch, report_dir=report_dir, progress=progress,
            decode_workers=decode_workers, block_size=block_size,
            monthly_writers=monthly_writers, index_results=index_results,
            read_processes=read_processes or 2, prefetch_mib=prefetch_mib,
            read_mode=read_mode, download_concurrency=download_concurrency, staging_mib=staging_mib, read_profiles=read_profiles)
    from . import storage
    final_root = destination_root(destination)
    remote = str(destination) == "hf"
    from .hf_storage import BucketWriter
    existing_ids = set()
    marker_cache = {}
    for asset in selection.assets:
        location = final_root + "/" + _relative_path(selection, asset)
        if location not in marker_cache:
            marker_cache[location] = _marker(location, remote, final_root)[1]
        marker = marker_cache[location]
        if marker and asset.id in marker.get("asset_ids", []):
            existing_ids.add(asset.id)
    pending = [asset for asset in selection.assets if asset.id not in existing_ids]
    pending_ids = {asset.id for asset in pending}
    if progress:
        for index, asset in enumerate((asset for asset in selection.assets if asset.id in existing_ids), start=1):
            progress(index, len(selection.assets), {"asset_id": asset.id, "status": "reused"})
    stage_progress = (lambda done, total, row: progress(len(existing_ids) + done,
        len(selection.assets), row)) if progress else None
    subset = replace(selection, assets=pending,
        hourly_matches=tuple(m for m in selection.hourly_matches if m.get("asset_id") in pending_ids))
    started = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="ecore-month-stage-", dir=scratch) as stage:
        stage_root = Path(stage) / "objects"
        if pending:
            report = storage.fetch(subset, destination=stage_root, workers=workers,
                backend=backend, scratch=scratch, report_dir=None, progress=stage_progress,
                decode_workers=decode_workers, layout="readable", read_processes=read_processes,
                container="zip", block_size=block_size, index_results=False)
        else:
            report = {"selection_id": selection.id, "source": selection.source,
                "root": str(stage_root), "records": [], "read_bytes": 0,
                "stored_bytes": 0, "logical_array_bytes": 0, "peak_rss_bytes": 0,
                "workers": workers, "decode_workers": decode_workers,
                "read_processes": read_processes, "selection_summary": selection.summary(),
                "hf_upload_bytes": 0, "hf_upload_seconds": 0, "hf_readback_bytes": 0}
        report["source_selection_id"] = selection.id
        for asset in selection.assets:
            if asset.id not in existing_ids:
                continue
            location = final_root + "/" + _relative_path(selection, asset)
            report["records"].append({"asset_id": asset.id, "time": asset.time,
                "source_url": asset.url, "source_bytes": asset.size, "etag": asset.etag,
                "product": selection.product, "url": location + "/raw.zarr.zip",
                "band": _band(asset) if selection.source == "goes" else None,
                "subset_id": _identity(selection, _band(asset) if selection.source == "goes" else None),
                "slot_time": next((m.get("slot_time") for m in selection.hourly_matches
                                   if m.get("asset_id") == asset.id), None),
                "status": "reused"})
        report = compact(selection, report, destination=destination, monthly_writers=monthly_writers)
        # Replace source-object paths with their monthly container path in the
        # result rows, while retaining each original NOAA URL and timestamp.
        archives = {(row["month"], row.get("band")): row for row in report["monthly_archives"]}
        for row in report["records"]:
            asset = next((a for a in selection.assets if a.id == row["asset_id"]), None)
            if asset is None:
                continue
            key = (utc(asset.time).strftime("%Y/%m"), _band(asset) if selection.source == "goes" else None)
            archive = archives.get(key)
            if archive:
                row["url"] = archive["path"]
                row["status"] = "archived" if archive["status"] == "saved" else "reused"
                row["band"] = key[1]
                row["subset_id"] = _identity(selection, key[1])
        report["wall_s"] = time.perf_counter() - started
        report["destination"] = destination
        report["selection_id"] = selection.id
        report["selection_summary"] = selection.summary()
        report["source_read_decode_crop_seconds"] = sum(
            row.get("read_decode_crop_s", 0.0) for row in report.get("records", []))
        report["source_stage_write_seconds"] = sum(
            row.get("write_s", 0.0) for row in report.get("records", []))
        report["monthly_archive_write_seconds"] = sum(
            row.get("write_seconds", 0.0) for row in report.get("monthly_archives", []))
        try:
            from .index import record_fetch
            record_fetch(report)
        except ImportError:
            pass
        if report_dir is not None:
            from .common import write_json
            from .runlog import save
        save(Path(report_dir) / f"{selection.id}-monthly.json", report)
        return report
