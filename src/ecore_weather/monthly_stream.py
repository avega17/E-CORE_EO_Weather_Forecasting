"""Bounded local monthly writer using Earth2Studio's ZarrBackend."""

from __future__ import annotations

from collections import OrderedDict, deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
import time
import zipfile

import numpy as np

from .common import PeakMemory, Transport, iso, jsonable, utc, write_json
from .storage import open_raw, object_writer
from .runlog import scratch_root


# Keep the queue finite even if callers request a very large download pool.
# The MRMS study uses eight source tasks per archive; a caller can request up
# to this ceiling. Writer-process and GRIB-decode limits remain independent.
MAX_SOURCE_READ_WORKERS = 16


def source_read_workers(workers):
    """Return the bounded per-archive NOAA read concurrency."""
    return max(1, min(int(workers), MAX_SOURCE_READ_WORKERS))


def _science(ds, band):
    if band is None:
        return ds[["measurement", "bitmap_valid"]]
    names = [name for name in ds.data_vars if name == "CMI" or name == "DQF"
             or name in (f"CMI_C{band:02d}", f"DQF_C{band:02d}")]
    renamed = ds[names].rename({name: f"{name}_C{band:02d}" for name in names
                                if name in ("CMI", "DQF")})
    if not any(name.startswith("CMI") for name in renamed):
        raise ValueError(f"GOES source has no C{band:02d} image")
    return renamed



def _zarr_attrs(array):
    """Encode floating fill attributes as required by xarray's Zarr v3 reader.

    The sidecar and per-scan source metadata retain the original NOAA encoding.
    """
    attrs = jsonable(array.attrs)
    if '_FillValue' in attrs and array.dtype.kind in 'fc':
        from xarray.backends.zarr import FillValueCoder
        attrs['_FillValue'] = FillValueCoder.encode(attrs['_FillValue'], array.dtype)
    return attrs

def _source_metadata(ds):
    return json.dumps({"dataset": jsonable(ds.attrs),
        "variables": {name: {"attrs": jsonable(ds[name].attrs),
            "dims": list(ds[name].dims), "dtype": ds[name].dtype.str,
            "values": jsonable(np.asarray(ds[name].values).tolist())
                      if ds[name].ndim <= 1 and name not in ("x", "y", "latitude", "longitude")
                      else None}
            for name in ds.variables}}, sort_keys=True, default=str)


def _read_new(asset, selection, band, transport, block_size):
    from .earth2_sources import read_selected_asset
    dataset, timing = read_selected_asset(asset, selection.source, selection.bbox,
        selection.product, (band,) if band else selection.bands, transport, block_size)
    return dataset, timing


def _archive_rows(selection, assets, prior):
    matches = {row["asset_id"]: row for row in selection.hourly_matches}
    rows = [{**row, "kind": "old", "old_index": index}
            for index, row in enumerate(prior.get("assets", []))]
    done = {row["asset_id"] for row in rows}
    for asset in assets:
        if asset.id in done:
            continue
        match = matches.get(asset.id, {})
        rows.append({"kind": "new", "asset": asset, "asset_id": asset.id,
            "time": asset.time, "source_url": asset.url, "etag": asset.etag,
            "source_bytes": asset.size, "slot_time": match.get("slot_time") or "",
            "offset_seconds": match.get("offset_seconds")})
    rows.sort(key=lambda row: (utc(row["time"]), row["asset_id"]))
    return rows


def _write_one(selection, assets, target, band, workers=16, backend="obstore",
               decode_workers=1, block_size=1024*1024, scratch=None,
               read_processes=2, prefetch_mib=512, read_mode="range",
               download_concurrency=8, staging_mib=4096, reader_factory=None,
               checkpoint_callback=None, finalizer=None):
    """Build one archive; no source observation is held past its Zarr write."""
    from earth2studio.io import ZarrBackend
    from zarr.codecs import BloscCodec, BloscShuffle
    import torch
    import zarr

    target = Path(target)
    with object_writer(target):
        marker_path = target / "complete.json"
        backup = target / ".raw.zarr.zip.backup"
        if backup.is_file():
            if not marker_path.is_file():
                raise IOError(f"Unmarked backup needs inspection: {backup}")
            from .goes_monthly import _file_sha256
            previous = json.loads(marker_path.read_text())
            current = target / "raw.zarr.zip"
            if current.is_file() and previous.get("archive_sha256") == _file_sha256(current):
                backup.unlink()  # Replacement committed; only backup cleanup was interrupted.
            else:
                os.replace(backup, current)  # Replacement marker was not committed.
        prior = json.loads(marker_path.read_text()) if marker_path.is_file() else {}
        if prior:
            if (prior.get("product") != selection.product or prior.get("band") != band
                    or prior.get("region") != list(selection.bbox)):
                raise IOError("Existing monthly marker has a different scientific identity")
            archive_path = target / "raw.zarr.zip"
            if not archive_path.is_file():
                raise IOError("Verified monthly archive is missing")
            if prior.get("archive_sha256"):
                from .goes_monthly import _file_sha256
                if _file_sha256(archive_path) != prior["archive_sha256"]:
                    raise IOError("Existing monthly archive hash mismatch")
        rows = _archive_rows(selection, assets, prior)
        if rows and all(row["kind"] == "old" for row in rows):
            return {"path": str(target / "raw.zarr.zip"), "status": "reused",
                    "observations": len(rows), "stored_bytes": prior.get("stored_bytes", 0),
                    "read_bytes": 0, "write_seconds": 0, "assets": prior.get("assets", [])}
        if not rows:
            raise ValueError("No available source observations for this month")
        if prior and not (target / "raw.zarr.zip").is_file():
            raise IOError(f"Completion marker has no archive: {target}")

        started = time.perf_counter()
        native = selection.source == "goes" and selection.product == "ABI-L2-CMIPF"
        resumable = native or selection.source == "mrms"
        identity = hashlib.sha256(json.dumps({"target": str(target.resolve()),
            "ids": [r["asset_id"] for r in rows]}, sort_keys=True).encode()).hexdigest()[:24]
        parent = (Path(scratch or scratch_root()) / f"{'goes-cmipf' if native else 'mrms'}-{identity}"
                  if resumable else Path(tempfile.mkdtemp(prefix="ecore-month-", dir=scratch)))
        parent.mkdir(parents=True, exist_ok=True)
        saved = {}
        if resumable:
            from .checkpoint_recovery import recover
            recovery=recover(parent,rows)
            if recovery:print(f"Recovered scratch: {recovery}",flush=True)
        if resumable:
            for batch_path in sorted(parent.glob("batch-*.json")):
                batch = json.loads(batch_path.read_text())
                for item in batch["rows"]:
                    if item["index"] != len(saved) or rows[item["index"]]["asset_id"] != item["row"]["asset_id"]:
                        raise IOError("Non-contiguous or changed native-band scratch selection")
                    saved[item["index"]] = item
        saved_asset_ids={item["row"]["asset_id"] for item in saved.values()}
        metrics = json.loads((parent / "progress.json").read_text()) if resumable and (parent / "progress.json").is_file() else {}
        metrics = {k:v for k,v in metrics.items() if k not in ("asset_ids","next_index","product","band")}
        metrics.update(attempt_started_at=time.time())
        initial_read_bytes = metrics.get('read_bytes', 0)
        success = False
        directory, packed = parent / "raw.zarr", parent / "raw.zarr.zip"
        checksums = []
        observed_metadata = []
        actual_rows = []
        old = None
        io = None
        codec = BloscCodec(cname="zstd", clevel=3, shuffle=BloscShuffle.shuffle)
        read_bytes = 0
        memory = PeakMemory()
        cpu_started = time.process_time()
        memory.__enter__()
        try:
            with ExitStack() as stack:
                if prior:
                    old = stack.enter_context(open_raw(target / "raw.zarr.zip"))
                transport = stack.enter_context(Transport(backend, decode_workers))
                read_workers = source_read_workers(workers)
                pool = stack.enter_context(ThreadPoolExecutor(max_workers=read_workers))
                resumed = stack.enter_context(open_raw(directory)) if saved else None
                if native:
                    from .goes_native import ordered_reads
                    new_rows = [{**r, "archive_index": i} for i,r in enumerate(rows) if i not in saved and r["kind"] == "new"]
                    native_reads = (reader_factory(new_rows, selection, band, parent/"source-cache", metrics)
                        if reader_factory else ordered_reads(new_rows, selection, band, workers,
                        read_processes, prefetch_mib, block_size, metrics, backend,
                        read_mode, download_concurrency, staging_mib, parent/"source-cache"))
                    stack.callback(native_reads.close)
                # NOAA reads overlap the serial archive writer. Downloads and
                # completed datasets in this queue are bounded independently
                # from the per-process decode semaphore in Transport.
                queue = deque()
                iterator = iter(rows)

                def enqueue():
                    try:
                        row = next(iterator)
                    except StopIteration:
                        return False
                    future = (pool.submit(_read_new, row["asset"], selection, band,
                        transport, block_size) if row["kind"] == "new" and not native and row["asset_id"] not in saved_asset_ids else None)
                    queue.append((row, future))
                    return True

                for _ in range(min(len(rows), read_workers)):
                    enqueue()
                dimensions = None
                arrays = None
                for index in range(len(rows)):
                    row, future = queue.popleft()
                    enqueue()
                    if index in saved:
                        ds = resumed.isel(time=index, drop=True).load()
                        raw_meta = saved[index]["metadata"]
                        source = _science(ds, band)
                        for name, expected in saved[index]["checksums"].items():
                            if hashlib.sha256(np.ascontiguousarray(source[name].values).tobytes()).hexdigest() != expected:
                                raise IOError(f"Native scratch read-back mismatch at {index} {name}")
                    elif native and row["kind"] == "new":
                        read_row, (ds, _) = next(native_reads)
                        if read_row["asset_id"] != row["asset_id"]:
                            raise IOError("Native read ordering changed")
                        raw_meta = _source_metadata(ds)
                        source = _science(ds, band)
                    elif future is None:
                        ds = old.isel(time=row["old_index"], drop=True).load()
                        raw_meta = str(old.source_metadata_json.values[row["old_index"]])
                        source = _science(ds, band)
                    else:
                        try:
                            ds, _ = future.result()
                        except Exception as exc:
                            raise OSError(f"Could not read {selection.source.upper()} source {row['asset'].key} "
                                f"at {row['asset'].time}: {type(exc).__name__}: {exc}") from exc
                        raw_meta = _source_metadata(ds)
                        source = _science(ds, band)
                    if dimensions is None:
                        dimensions = OrderedDict((name, np.asarray(source.coords[name].values))
                            for name in source.dims)
                        arrays = {name: source[name] for name in source.data_vars}
                        times = np.asarray([np.datetime64(utc(r["time"]).replace(tzinfo=None), "ns")
                                            for r in rows])
                        chunks = {"time": 1, **{name: (len(values) if native else min(256, len(values)))
                            for name, values in dimensions.items()}}
                        io = ZarrBackend(str(directory), chunks=chunks,
                            backend_kwargs={"overwrite": not bool(saved)}, zarr_codecs=codec)
                        for name, array in arrays.items():
                            coords = OrderedDict([("time", times),
                                *((dim, dimensions[dim]) for dim in array.dims)])
                            if name not in io.root:
                                io.add_array(coords, name)
                            if io.root[name].dtype != array.dtype:
                                del io.root[name]
                                io.root.create_array(name, shape=(len(times), *array.shape),
                                    chunks=(1, *(chunks[dim] for dim in array.dims)),
                                    dtype=array.dtype, dimension_names=["time", *array.dims],
                                    compressors=codec, fill_value=None)
                        for coord in dimensions:
                            io.root[coord].attrs.update(_zarr_attrs(source.coords[coord]))
                        io.root.attrs.update({"source": selection.source,
                            "product": selection.product, "satellite": selection.satellite,
                            "requested_bbox": list(selection.bbox),
                            "preservation": "Native measurements; per-observation metadata in source_metadata_json"})
                    for name, values in dimensions.items():
                        if not np.array_equal(source.coords[name].values, values):
                            raise ValueError(f"Native {name} grid changed within monthly archive")
                    if set(source.data_vars) != set(arrays):
                        raise ValueError("Native measurement/quality array set changed within monthly archive")
                    for name, template in arrays.items():
                        if name not in source or source[name].dims != template.dims or source[name].dtype != template.dtype:
                            raise ValueError(f"Native {name} layout changed within monthly archive")
                        value = np.ascontiguousarray(source[name].values)
                        coords = OrderedDict([("time", times[index:index+1]),
                            *((dim, dimensions[dim]) for dim in template.dims)])
                        if index not in saved:
                            io.write(torch.from_numpy(value[np.newaxis]), coords, name)
                        io.root[name].attrs.update(_zarr_attrs(template))
                        checksums.append((index, name, hashlib.sha256(value.tobytes()).hexdigest()))
                    observed_metadata.append(raw_meta)
                    actual_rows.append({key: row.get(key) for key in
                        ("asset_id", "time", "source_url", "etag", "source_bytes",
                         "slot_time", "offset_seconds")})
                    # Reopening a verified prefix must not rewrite its batches
                    # or make the durable progress counter move backwards.
                    if resumable and index not in saved and ((index+1) % 8 == 0 or index+1 == len(rows)):
                        if not native:metrics["read_bytes"]=initial_read_bytes+transport.bytes
                        begin = (index//8)*8
                        batch = {"rows": [{"index": j, "row": actual_rows[j],
                            "metadata": observed_metadata[j],
                            "checksums": {name: expected for idx,name,expected in checksums if idx == j}}
                            for j in range(begin,index+1)]}
                        # Verify written pixels before making the batch reusable.
                        for item in batch["rows"]:
                            for name,expected in item["checksums"].items():
                                value = np.ascontiguousarray(io.root[name][item["index"]])
                                if hashlib.sha256(value.tobytes()).hexdigest() != expected:
                                    raise IOError("Native scan-batch verification failed")
                        metrics.update(attempt_elapsed_seconds=time.time()-metrics["attempt_started_at"],
                            attempt_read_bytes=metrics.get("read_bytes",0)-initial_read_bytes)
                        from .goes_monthly import _checkpoint
                        _checkpoint(parent / f"batch-{begin:06d}.json", batch)
                        _checkpoint(parent / "progress.json", {"asset_ids": [r["asset_id"] for r in rows],
                            "next_index": index+1, "product": selection.product, "band": band, "source":selection.source,
                            "target":str(target), "month":rows[0]["time"][:7], "selected":len(rows), **metrics})
                        if checkpoint_callback:
                            checkpoint_callback([r["asset_id"] for r in actual_rows[begin:index+1]])
                        if read_mode == "async_pipeline":
                            from .goes_staging import retire_committed
                            retire_committed(parent/"source-cache", [r["asset_id"] for r in actual_rows[begin:index+1]])
                close = getattr(io, "close", None)
                if close:
                    close()
                io = None
                root = zarr.open_group(str(directory), mode="a")
                for name, array in arrays.items():
                    root[name].attrs.update(_zarr_attrs(array))
                for name, values in dimensions.items():
                    root[name].attrs.update(_zarr_attrs(source.coords[name]))
                auxiliary = {
                    "source_asset_id": [r["asset_id"] for r in actual_rows],
                    "source_url": [r["source_url"] for r in actual_rows],
                    "source_etag": [r["etag"] for r in actual_rows],
                    "request_slot_time": [r["slot_time"] or "" for r in actual_rows],
                    "request_offset_seconds": [r["offset_seconds"] if r["offset_seconds"] is not None
                                               else float("nan") for r in actual_rows],
                    "source_metadata_json": observed_metadata,
                }
                metadata = {"dataset_attrs": dict(root.attrs),
                    "variable_attrs": {name: jsonable(array.attrs) for name, array in arrays.items()},
                    "coordinate_attrs": {name: jsonable(source.coords[name].attrs)
                        for name in dimensions},
                    "auxiliary_variables": {},
                    "auxiliary_coords": {name: {"dims": ["time"], "values": values, "attrs": {}}
                        for name, values in auxiliary.items()}}
                (directory / "ecore_metadata.json").write_text(json.dumps(metadata, sort_keys=True))
                zarr.consolidate_metadata(str(directory))
                if not native:metrics["read_bytes"]=initial_read_bytes+transport.bytes
                read_bytes = metrics.get("read_bytes", transport.bytes)
                source_retries = metrics.get("source_retries", transport.retries)
                failed_requests = metrics.get("failed_requests", transport.failed_requests)
            arguments = (directory, packed, checksums, dimensions, times, observed_metadata,
                         actual_rows, selection, target, band, prior)
            metrics.update(finalizer(*arguments) if finalizer else _finalize_archive(*arguments))
            success = True
            metrics.update(peak_rss_bytes=memory.peak,writer_cpu_seconds=time.process_time()-cpu_started,
                timing_scope="write_seconds includes source acquisition, packing and verification; per-writer lineage RSS")
            return {"path": str(target / "raw.zarr.zip"), "status": "saved", **metrics,
                "observations": len(rows), "stored_bytes": metrics["stored_bytes"],
                "read_bytes": read_bytes-initial_read_bytes if resumable else read_bytes,
                "build_read_bytes": read_bytes, "resumed_observations": len(saved),
                "source_retries": source_retries,
                "failed_requests": failed_requests,
                "write_seconds": time.perf_counter()-started,
                "assets": actual_rows}
        finally:
            memory.__exit__()
            if io is not None and hasattr(io, "close"):
                io.close()
            if success or not resumable:
                shutil.rmtree(parent, ignore_errors=True)


def _finalize_archive(directory, packed, checksums, dimensions, times, observed_metadata,
                      actual_rows, selection, target, band, prior):
    """Pack a closed store, verify every array, and atomically publish its ZIP.

    The calling monthly process keeps object_writer(target) locked until this
    function returns. No open backend or HDF5 handle crosses a process boundary.
    """
    metrics = {}
    target = Path(target)
    marker_path = target / "complete.json"
    backup = target / ".raw.zarr.zip.backup"
    pack_started = time.perf_counter()
    with zipfile.ZipFile(packed, "w", compression=zipfile.ZIP_STORED,
                         allowZip64=True) as archive:
        for file in sorted(directory.rglob("*")):
            if file.is_file():
                archive.write(file, file.relative_to(directory).as_posix())
    metrics["pack_seconds"] = time.perf_counter()-pack_started
    verify_started = time.perf_counter()
    with open_raw(packed) as verified:
        if not np.array_equal(verified.time.values, times):
            raise IOError("Monthly observation timestamps changed in read-back")
        for axis,values in dimensions.items():
            if not np.array_equal(verified[axis].values, values):
                raise IOError("Monthly native coordinates changed in read-back")
        if len(verified.time) != len(actual_rows):
            raise IOError("Monthly archive has the wrong observation count")
        for index, name, expected in checksums:
            value = np.ascontiguousarray(verified[name].isel(time=index).values)
            if hashlib.sha256(value.tobytes()).hexdigest() != expected:
                raise IOError(f"Monthly read-back differs at {index} {name}")
        if verified.source_metadata_json.values.tolist() != observed_metadata:
            raise IOError("Monthly source metadata changed in read-back")
    metrics["verify_seconds"] = time.perf_counter()-verify_started
    target.mkdir(parents=True, exist_ok=True)
    staged = target / ".raw.zarr.zip.tmp"
    copy_started=time.perf_counter()
    shutil.copyfile(packed, staged)
    marker = {"raw_path": "raw.zarr.zip", "source": selection.source,
        "product": selection.product, "satellite": selection.satellite,
        "band": band, "region": list(selection.bbox),
        "asset_ids": [r["asset_id"] for r in actual_rows],
        "assets": actual_rows, "stored_bytes": packed.stat().st_size,
        "observations": len(actual_rows), "writer_backend": "earth2studio-zarr-backend",
        "updated_at": iso(datetime.now(timezone.utc))}
    digest = hashlib.sha256()
    with packed.open("rb") as stream:
        for block in iter(lambda: stream.read(8*1024*1024), b""):
            digest.update(block)
    marker["archive_sha256"] = digest.hexdigest()
    from .goes_monthly import _file_sha256
    if _file_sha256(staged) != marker["archive_sha256"]:
        raise IOError("Destination ZIP copy did not pass read-back hash verification")
    metrics["destination_copy_hash_seconds"] = time.perf_counter()-copy_started
    old_archive = target / "raw.zarr.zip"
    if old_archive.is_file() and prior:
        os.replace(old_archive, backup)
    try:
        os.replace(staged, old_archive)
        from .goes_monthly import _checkpoint
        _checkpoint(marker_path, marker)
    except Exception:
        if backup.is_file():
            os.replace(backup, old_archive)
        raise
    if backup.is_file():
        backup.unlink()
    metrics["stored_bytes"] = marker["stored_bytes"]
    return metrics
