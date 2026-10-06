"""Resumable, ROI-first MCMIPF month archives with one group per ABI band.

The NOAA multiband product has one 2 km grid. Each scan is read once and its
selected channels are written unchanged. Separate band groups permit a channel
to be absent in a scan without inserting synthetic raw pixels.
"""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, as_completed, wait
from datetime import datetime, timezone
import gzip
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import shutil
import threading
import time
import zipfile

import numpy as np

from .common import Asset, Transport, digest, iso, jsonable, utc, write_json
from .monthly_stream import _source_metadata
from .storage import destination_root


_transport = None
_read_pool = None
_read_threads = None
_reuse_cache = {}
_reuse_lock = threading.Lock()
REUSABLE_2KM_BANDS = frozenset((7, 8, 9, 10, 13))
HYBRID_PRODUCT = "ABI-L2-CMI-2KM-HYBRID"


def _checkpoint(path, value):
    """Replace a scratch checkpoint only after its complete JSON is on disk."""
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    write_json(temporary, value)
    with temporary.open('rb') as stream:os.fsync(stream.fileno())
    os.replace(temporary, path)
    descriptor=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
    try:os.fsync(descriptor)
    finally:os.close(descriptor)


def _init_reader():
    global _transport, _read_pool, _read_threads, _reuse_cache
    _transport = Transport("obstore")
    _read_pool = None
    _read_threads = None
    _reuse_cache = {}


def _cmipf_band(root, satellite, bbox, stamp, band):
    """Read a verified 2 km CMIPF band at the exact MCMIPF scan start.

    The cache lives in one reader process. A lock protects ZipStore reads
    across its source-read threads; other processes may read other months.
    """
    month = utc(stamp).strftime("%Y/%m")
    key = (str(root), int(satellite), month, int(band))
    with _reuse_lock:
        if key not in _reuse_cache:
            from .storage import open_raw
            base = Path(root) / "goes" / "ABI-L2-CMIPF" / f"goes{satellite}" / f"C{band:02d}"
            choice = None
            for path in base.glob(f"roi-*/{month}/complete.json"):
                marker = json.loads(path.read_text())
                if (marker.get("region") == list(bbox) and marker.get("product") == "ABI-L2-CMIPF"
                        and (path.parent / "raw.zarr.zip").is_file()):
                    if choice is not None:
                        raise IOError(f"More than one matching CMIPF C{band:02d} month: {base}")
                    choice = (path.parent / "raw.zarr.zip", marker)
            if choice is None:
                _reuse_cache[key] = None
            else:
                path, marker = choice
                dataset = open_raw(path)
                positions = {row["time"]: i for i, row in enumerate(marker["assets"])}
                _reuse_cache[key] = (dataset, positions, marker)
        cached = _reuse_cache[key]
        if cached is None:
            return None
        dataset, positions, marker = cached
        index = positions.get(stamp)
        if index is None:
            return None
        name = f"C{band:02d}"
        image = dataset[[f"CMI_{name}", f"DQF_{name}"]].isel(time=index).load()
        source_meta = json.loads(str(dataset.source_metadata_json.values[index]))
        source_row = marker["assets"][index]
        return {"x": np.asarray(dataset.x.values), "y": np.asarray(dataset.y.values),
            "cmi": np.ascontiguousarray(image[f"CMI_{name}"].values),
            "dqf": np.ascontiguousarray(image[f"DQF_{name}"].values),
            "cmi_attrs": source_meta["variables"]["CMI"]["attrs"],
            "dqf_attrs": source_meta["variables"]["DQF"]["attrs"],
            "metadata": source_meta,
            "origin": {"product": "ABI-L2-CMIPF", "asset_id": source_row["asset_id"],
                       "source_url": source_row["source_url"],
                       "etag": source_row["etag"]}}


def _read_scan(asset, bbox, bands, block_size, reuse_root=None):
    from . import goes

    try:
        reused = {band: found for band in bands if reuse_root and band in REUSABLE_2KM_BANDS
                  if (found := _cmipf_band(reuse_root, int(asset.bucket.removeprefix("noaa-goes")),
                                            bbox, asset.time, band)) is not None}
        fetch_bands = tuple(band for band in bands if band not in reused) or (1,)
        try:
            ds, timing = goes.read(asset, bbox, fetch_bands, _transport,
                                   block_size=block_size, allow_missing=True)
        except Exception as exc:
            if not goes.hdf_integrity_error(exc):
                raise
            # A local full-object read distinguishes remote-range defects from
            # source corruption. Network errors and unverified objects still fail.
            ds, timing = goes.read(asset, bbox, fetch_bands, _transport,
                                   full_file=True, allow_missing=True)
        images = {}
        for band in bands:
            cmi, dqf = f"CMI_C{band:02d}", f"DQF_C{band:02d}"
            if cmi in ds and dqf in ds:
                images[band] = {
                    "cmi": np.ascontiguousarray(ds[cmi].values),
                    "dqf": np.ascontiguousarray(ds[dqf].values),
                    "cmi_attrs": jsonable(ds[cmi].attrs),
                    "dqf_attrs": jsonable(ds[dqf].attrs),
                    "origin": {"product": "ABI-L2-MCMIPF", "asset_id": asset.id,
                               "source_url": asset.url, "etag": asset.etag},
                }
        metadata = json.loads(_source_metadata(ds))
        for band, old in reused.items():
            if not np.array_equal(ds.x.values, old["x"]) or not np.array_equal(ds.y.values, old["y"]):
                raise ValueError(f"C{band:02d} CMIPF grid differs from MCMIPF for {asset.time}")
            images[band] = {key: old[key] for key in ("cmi", "dqf", "cmi_attrs", "dqf_attrs", "origin")}
            for source_name, target_name in (("CMI", f"CMI_C{band:02d}"),
                                             ("DQF", f"DQF_C{band:02d}"),
                                             ("band_id", f"band_id_C{band:02d}"),
                                             ("band_wavelength", f"band_wavelength_C{band:02d}")):
                if source_name in old["metadata"]["variables"]:
                    metadata["variables"][target_name] = old["metadata"]["variables"][source_name]
        metadata["pixel_origins"] = {f"C{band:02d}": image["origin"]
                                      for band, image in images.items()}
        result = {"status": "ok", "asset_id": asset.id, "time": asset.time,
                  "source_url": asset.url, "etag": asset.etag,
                  "source_bytes": asset.size, "bucket": asset.bucket,
                  "key": asset.key, "end_time": asset.end_time,
                  "metadata": json.dumps(metadata, sort_keys=True),
                  "x": np.asarray(ds.x.values), "y": np.asarray(ds.y.values),
                  "x_attrs": jsonable(ds.x.attrs), "y_attrs": jsonable(ds.y.attrs),
                  "images": images, "read_decode_crop_s": timing["read_decode_crop_s"]}
        ds.close()
        return result
    except Exception as exc:
        confirmed = isinstance(exc, goes.ConfirmedCorruptSourceError)
        return {"status": "corrupt_source" if confirmed else "source_error",
                "integrity_evidence": exc.evidence if confirmed else None,
                "asset_id": asset.id, "time": asset.time,
                "source_url": asset.url, "etag": asset.etag, "source_bytes": asset.size,
                "bucket": asset.bucket, "key": asset.key, "end_time": asset.end_time,
                "error": f"{type(exc).__name__}: {exc}"}


def _read_batch(assets, bbox, bands, block_size, threads, reuse_root=None):
    global _transport, _read_pool, _read_threads
    if _transport is None:
        _init_reader()
    if _read_pool is None or _read_threads != threads:
        if _read_pool is not None:
            _read_pool.shutdown(wait=True)
        _read_pool = ThreadPoolExecutor(max_workers=threads)
        _read_threads = threads
    before = (_transport.bytes, _transport.requests, _transport.seconds,
              _transport.retries, _transport.failed_requests)
    started = time.perf_counter()
    cpu_started = time.process_time()
    results = list(_read_pool.map(lambda asset: _read_scan(asset, bbox, bands, block_size,
                                                           reuse_root), assets))
    payload_bytes = sum(image[kind].nbytes for row in results if row["status"] == "ok"
                        for image in row["images"].values() for kind in ("cmi", "dqf"))
    return {"results": results, "payload_bytes": payload_bytes,
            "read_bytes": _transport.bytes - before[0],
            "range_requests": _transport.requests - before[1],
            "transfer_task_seconds": _transport.seconds - before[2],
            "source_retries": _transport.retries - before[3],
            "failed_requests": _transport.failed_requests - before[4],
            "batch_wall_seconds": time.perf_counter() - started,
            "reader_cpu_seconds": time.process_time() - cpu_started}


def _codec():
    from zarr.codecs import BloscCodec, BloscShuffle
    return BloscCodec(cname="zstd", clevel=3, shuffle=BloscShuffle.shuffle)


def _band_group(root, band, scan):
    name = f"C{band:02d}"
    if name in root:
        group = root[name]
        if not np.array_equal(group["x"][:], scan["x"]) or not np.array_equal(group["y"][:], scan["y"]):
            raise ValueError(f"The MCMIPF 2 km grid changed for {name}; use a separate grid archive")
        if group.attrs.get("pixel_origin_product") != scan["images"][band]["origin"]["product"]:
            group.attrs["pixel_origin_product"] = "mixed"
        return group
    group = root.create_group(name)
    group.attrs.update({"band": band, "source_product": "ABI-L2-MCMIPF",
                        "pixel_origin_product": scan["images"][band]["origin"]["product"],
                        "x_attrs": scan["x_attrs"], "y_attrs": scan["y_attrs"]})
    for axis in ("x", "y"):
        values = scan[axis]
        arr = group.create_array(axis, data=values, chunks=values.shape,
                                 dimension_names=[axis], compressors=_codec())
        arr.attrs.update(scan[f"{axis}_attrs"])
        group.attrs[f"{axis}_sha256"] = hashlib.sha256(np.ascontiguousarray(values).tobytes()).hexdigest()
    group.create_array("time", shape=(0,), chunks=(256,), dtype="datetime64[ns]",
                       dimension_names=["time"])
    for kind in ("cmi", "dqf"):
        values = scan["images"][band][kind]
        var = f"{kind.upper()}_C{band:02d}"
        arr = group.create_array(var, shape=(0, *values.shape),
            chunks=(1, *values.shape), dtype=values.dtype,
            dimension_names=["time", "y", "x"], compressors=_codec(), fill_value=None)
        arr.attrs.update(scan["images"][band][f"{kind}_attrs"])
    return group


def _append(group, band, scan):
    index = group["time"].shape[0]
    stamp = np.datetime64(utc(scan["time"]).replace(tzinfo=None), "ns")
    group["time"].resize((index + 1,))
    group["time"][index] = stamp
    checksums = {}
    for kind in ("cmi", "dqf"):
        name = f"{kind.upper()}_C{band:02d}"
        value = scan["images"][band][kind]
        arr = group[name]
        if arr.dtype != value.dtype or tuple(arr.shape[1:]) != tuple(value.shape):
            raise ValueError(f"Native {name} dtype or grid changed in this month")
        arr.resize((index + 1, *arr.shape[1:]))
        arr[index] = value
        checksums[name] = hashlib.sha256(value.tobytes()).hexdigest()
    checksums["time_ns"] = int(stamp.astype("datetime64[ns]").astype("int64"))
    return index, checksums


def _stage_path(scratch, selection, assets, archive_product):
    key = digest({"source": "goes", "product": selection.product,
        "archive_product": archive_product,
        "satellite": selection.satellite, "bbox": selection.bbox,
        "bands": selection.bands, "asset_ids": [a.id for a in assets]})[:24]
    return Path(scratch or "results/study-scratch") / f"goes-mcmipf-{key}"


def _existing_assets(marker):
    return [Asset(row["bucket"], row["key"], row["source_bytes"], row["etag"],
                  row["time"], row.get("end_time")) for row in marker.get("assets", [])]


def _asset_record(scan):
    return {key: scan.get(key) for key in ("asset_id", "time", "source_url", "etag",
        "source_bytes", "bucket", "key", "end_time", "status", "error",
        "pixel_origins", "integrity_evidence") if key in scan}


def _write_batch(root, batch, bands):
    scans = batch["results"]
    rows = []
    for scan in scans:
        row = {**_asset_record(scan), "available_bands": [], "checksums": {}}
        if scan["status"] == "ok":
            row.update(pixel_origins={}, metadata=scan["metadata"],
                       read_decode_crop_s=scan["read_decode_crop_s"])
        rows.append(row)
    # Resize and write each band once per completed source batch. Individual
    # scan hashes still make the batch checkpoint independently verifiable.
    for band in bands:
        present = [(index, scan) for index, scan in enumerate(scans)
                   if scan["status"] == "ok" and band in scan["images"]]
        if not present:
            continue
        group = _band_group(root, band, present[0][1])
        for _, scan in present[1:]:
            if (not np.array_equal(group["x"][:], scan["x"])
                    or not np.array_equal(group["y"][:], scan["y"])):
                raise ValueError(f"The MCMIPF 2 km grid changed for C{band:02d}")
            if group.attrs.get("pixel_origin_product") != scan["images"][band]["origin"]["product"]:
                group.attrs["pixel_origin_product"] = "mixed"
        start = group["time"].shape[0]
        stamps = np.array([np.datetime64(utc(scan["time"]).replace(tzinfo=None), "ns")
                           for _, scan in present], dtype="datetime64[ns]")
        group["time"].resize((start + len(present),))
        group["time"][start:start + len(present)] = stamps
        for kind in ("cmi", "dqf"):
            name = f"{kind.upper()}_C{band:02d}"
            values = [scan["images"][band][kind] for _, scan in present]
            array = group[name]
            if any(value.dtype != array.dtype or value.shape != array.shape[1:]
                   for value in values):
                raise ValueError(f"Native {name} dtype or grid changed in this month")
            array.resize((start + len(present), *array.shape[1:]))
            array[start:start + len(present)] = np.stack(values)
            for offset, (row_index, _) in enumerate(present):
                rows[row_index]["checksums"].setdefault(str(band),
                    {"index": start + offset, "time_ns": int(stamps[offset].astype("int64"))})[name] = (
                        hashlib.sha256(values[offset].tobytes()).hexdigest())
        for row_index, scan in present:
            rows[row_index]["available_bands"].append(band)
            rows[row_index]["pixel_origins"][f"C{band:02d}"] = scan["images"][band]["origin"]
    return rows


def _verify_stage(root, batches):
    for name, group in root.groups():
        if name.startswith("C"):
            for axis in ("x", "y"):
                actual = hashlib.sha256(np.ascontiguousarray(group[axis][:]).tobytes()).hexdigest()
                if actual != group.attrs[f"{axis}_sha256"]:
                    raise IOError(f"GOES {name}/{axis} coordinate changed in the archive")
    for batch in batches:
        for row in batch["rows"]:
            for band, info in row["checksums"].items():
                group = root[f"C{int(band):02d}"]
                actual_time = int(np.asarray(group["time"][info["index"]]).astype("datetime64[ns]").astype("int64"))
                if actual_time != info["time_ns"]:
                    raise IOError(f"Staged GOES time differs: {row['asset_id']} C{band}")
                for name, expected in info.items():
                    if name in ("index", "time_ns"):
                        continue
                    value = np.ascontiguousarray(group[name][info["index"]])
                    if hashlib.sha256(value.tobytes()).hexdigest() != expected:
                        raise IOError(f"Staged GOES array differs: {row['asset_id']} {name}")


def _pack(directory, path):
    import zarr
    zarr.consolidate_metadata(str(directory))
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_STORED, allowZip64=True) as out:
        for file in sorted(directory.rglob("*")):
            if file.is_file():
                out.write(file, file.relative_to(directory).as_posix())


def _file_sha256(path):
    sha = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            sha.update(block)
    return sha.hexdigest()


def _write_one(selection, assets, target, reader_processes=2, source_threads=8,
               prefetch_mib=512, block_size=1024*1024, scratch=None,
               reuse_cmipf_root=None):
    import zarr
    from zarr.storage import ZipStore

    target = Path(target)
    target.mkdir(parents=True, exist_ok=True)
    archive_product = HYBRID_PRODUCT if reuse_cmipf_root else selection.product
    marker_path = target / "complete.json"
    old = target / "raw.zarr.zip"
    backup = target / ".raw.zarr.zip.backup"
    if backup.exists():
        # A process may have stopped after moving the previous verified ZIP
        # aside but before committing its replacement marker.
        previous = json.loads(marker_path.read_text()) if marker_path.is_file() else None
        if previous and old.is_file() and _file_sha256(old) == previous.get("archive_sha256"):
            backup.unlink()
        else:
            os.replace(backup, old)
    marker = json.loads(marker_path.read_text()) if marker_path.is_file() else None
    if marker:
        if marker.get("product") != archive_product or not (target / "raw.zarr.zip").is_file():
            raise IOError(f"Invalid existing GOES month marker: {marker_path}")
        if _file_sha256(old) != marker.get("archive_sha256"):
            raise IOError(f"GOES month archive differs from its completion marker: {old}")
        old_ids = set(marker.get("asset_ids", []))
        if all(asset.id in old_ids for asset in assets):
            return {"path": str(target / "raw.zarr.zip"), "status": "reused",
                    "observations": len(marker["assets"]), "stored_bytes": marker["stored_bytes"],
                    "read_bytes": 0, "range_requests": 0, "assets": marker["assets"],
                    "band_counts": marker["band_counts"]}
        assets = sorted({a.id: a for a in (*_existing_assets(marker), *assets)}.values(),
                        key=lambda asset: (utc(asset.time), asset.key))
    stage = _stage_path(scratch, selection, assets, archive_product)
    stage.mkdir(parents=True, exist_ok=True)
    working = stage / "raw.zarr"
    progress_path = stage / "progress.json"
    progress = json.loads(progress_path.read_text()) if progress_path.is_file() else None
    if progress and progress.get("asset_ids") != [a.id for a in assets]:
        raise IOError(f"Scratch selection differs; inspect {stage}")
    root = zarr.open_group(str(working), mode="a")
    if not progress:
        root.attrs.update({"source": "goes", "product": archive_product,
                           "source_selection_product": selection.product,
                           "satellite": selection.satellite,
                           "requested_bbox": list(selection.bbox),
                           "preservation": ("2 km packed CMI and DQF from per-band recorded origins; no processing"
                                            if reuse_cmipf_root else
                                            "MCMIPF 2 km packed CMI and DQF, no processing")})
        progress = {"asset_ids": [a.id for a in assets], "next_index": 0,
                    "counts": {}, "read_bytes": 0, "range_requests": 0,
                    "transfer_task_seconds": 0.0, "read_decode_crop_seconds": 0.0,
                    "reader_batch_seconds": 0.0, "reader_cpu_seconds": 0.0,
                    "source_retries": 0, "failed_requests": 0,
                    "writer_seconds": 0.0, "writer_idle_seconds": 0.0,
                    "max_batch_payload_bytes": 0}
        _checkpoint(progress_path, progress)
    batches = []
    for path in sorted(stage.glob("batch-*.json")):
        batch = json.loads(path.read_text())
        if batch["end_index"] <= progress["next_index"]:
            batches.append(batch)
    batches.sort(key=lambda row: row["start_index"])
    if batches:
        if batches[0]["start_index"] != 0 or batches[-1]["end_index"] != progress["next_index"] or any(
                a["end_index"] != b["start_index"] for a, b in zip(batches, batches[1:])):
            raise IOError(f"Inconsistent GOES scratch checkpoints: {stage}")
        _verify_stage(root, batches)
    for name, group in root.groups():
        if not name.startswith("C"):
            continue
        expected = progress["counts"].get(name, 0)
        for variable in ("time", f"CMI_{name}", f"DQF_{name}"):
            array = group[variable]
            if array.shape[0] < expected:
                raise IOError(f"Scratch {name}/{variable} is shorter than the checkpoint")
            if array.shape[0] > expected:
                array.resize((expected, *array.shape[1:]))

    # Reserve 16 MiB per scan (the measured 768 km ROI uses about 4 MiB of
    # packed CMI/DQF arrays). Verify each completed batch against its reserve
    # so a larger user-selected region cannot silently exceed this bound.
    reserve_bytes = 16 * 1024 * 1024
    budget_bytes = int(prefetch_mib) * 1024 * 1024
    scan_slots = max(1, budget_bytes // reserve_bytes)
    processes = max(1, int(reader_processes))
    batch_size = max(1, min(int(source_threads), scan_slots // processes or 1))
    max_queued = max(1, scan_slots // batch_size)
    remaining = [(i, assets[i:i+batch_size]) for i in range(progress["next_index"], len(assets), batch_size)]
    started = time.perf_counter()
    if remaining:
        context = multiprocessing.get_context("spawn")
        with ProcessPoolExecutor(max_workers=processes, mp_context=context,
                                 initializer=_init_reader) as pool:
            pending, ready = {}, {}
            source = iter(remaining)

            def submit():
                try:
                    index, portion = next(source)
                except StopIteration:
                    return False
                future = pool.submit(_read_batch, portion, selection.bbox,
                    selection.bands, block_size, int(source_threads), reuse_cmipf_root)
                pending[future] = (index, len(portion))
                return True

            for _ in range(min(max_queued, len(remaining))):
                submit()
            next_index = progress["next_index"]
            while pending:
                was_idle = next_index not in ready
                wait_started = time.perf_counter()
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                if was_idle:
                    progress["writer_idle_seconds"] += time.perf_counter() - wait_started
                for future in done:
                    index, length = pending.pop(future)
                    payload = future.result()
                    if payload["payload_bytes"] > length * reserve_bytes:
                        raise MemoryError(f"GOES crop needs {payload['payload_bytes']} bytes for "
                            f"{length} scan(s); the {reserve_bytes}-byte per-scan queue "
                            "reservation is too small for this region")
                    ready[index] = (length, payload)
                    progress["max_batch_payload_bytes"] = max(
                        progress["max_batch_payload_bytes"], payload["payload_bytes"])
                while next_index in ready:
                    length, payload = ready.pop(next_index)
                    failed_reads = [row for row in payload["results"]
                                    if row["status"] == "source_error"]
                    if failed_reads:
                        raise OSError(f"GOES source batch failed before checkpoint; "
                            f"restart will retry its scans: {failed_reads[:3]}")
                    write_started = time.perf_counter()
                    rows = _write_batch(root, payload, selection.bands)
                    progress["writer_seconds"] += time.perf_counter() - write_started
                    end_index = next_index + length
                    batch = {"start_index": next_index, "end_index": end_index, "rows": rows}
                    _checkpoint(stage / f"batch-{next_index:06d}.json", batch)
                    batches.append(batch)
                    progress["next_index"] = end_index
                    for row in rows:
                        for band in row["available_bands"]:
                            name = f"C{band:02d}"
                            progress["counts"][name] = progress["counts"].get(name, 0) + 1
                        progress["read_decode_crop_seconds"] += row.get("read_decode_crop_s", 0.0)
                    for key in ("read_bytes", "range_requests", "transfer_task_seconds",
                                "reader_cpu_seconds", "source_retries", "failed_requests"):
                        progress[key] += payload[key]
                    progress["reader_batch_seconds"] += payload["batch_wall_seconds"]
                    _checkpoint(progress_path, progress)
                    next_index = end_index
                while len(pending) + len(ready) < max_queued and submit():
                    pass
    rows = [row for batch in batches for row in batch["rows"]]
    if len(rows) != len(assets):
        raise IOError("GOES source coverage differs from the saved selection")
    failed = [row for row in rows if row["status"] == "source_error"]
    if failed:
        raise OSError(f"{len(failed)} MCMIPF source scan(s) failed; scratch retained: "
                      f"{failed[:3]}")
    metadata = {"dataset_attrs": jsonable(dict(root.attrs)),
        "groups": {f"C{band:02d}": {
        "asset_ids": [row["asset_id"] for row in rows if band in row["available_bands"]]}
        for band in selection.bands},
        "source_metadata": {row["asset_id"]: row.get("metadata") for row in rows}}
    with gzip.open(working / "ecore_metadata.json.gz", "wt") as out:
        json.dump(metadata, out, separators=(",", ":"))
    packed = stage / "raw.zarr.zip"
    pack_started = time.perf_counter()
    _pack(working, packed)
    pack_seconds = time.perf_counter() - pack_started
    verify_started = time.perf_counter()
    with ZipStore(str(packed), mode="r") as store:
        verified = zarr.open_group(store, mode="r")
        _verify_stage(verified, batches)
        with zipfile.ZipFile(packed) as archive:
            recovered = json.loads(gzip.decompress(archive.read("ecore_metadata.json.gz")))
        if recovered != metadata:
            raise IOError("GOES per-scan metadata changed during packing")
    verify_seconds = time.perf_counter() - verify_started
    archive_sha256 = _file_sha256(packed)
    saved = {"raw_path": "raw.zarr.zip", "source": "goes", "product": archive_product,
             "source_selection_product": selection.product,
             "satellite": selection.satellite, "region": list(selection.bbox),
             "band_counts": progress["counts"], "assets": [{**_asset_record(row),
                 "available_bands": row["available_bands"]} for row in rows],
             "asset_ids": [a.id for a in assets], "observations": len(rows),
             "corrupt_source_scans": sum(row['status'] == 'corrupt_source' for row in rows),
             "stored_bytes": packed.stat().st_size, "archive_sha256": archive_sha256,
             "writer_backend": "zarr-v3-earth2studio-compatible-band-groups",
             "hybrid_reuse": bool(reuse_cmipf_root),
             "updated_at": iso(datetime.now(timezone.utc))}
    staged = target / ".raw.zarr.zip.tmp"
    shutil.copyfile(packed, staged)
    if _file_sha256(staged) != archive_sha256:
        staged.unlink(missing_ok=True)
        raise IOError(f"GOES archive did not survive the destination copy: {target}")
    if marker:
        os.replace(old, backup)
    try:
        os.replace(staged, old)
        write_json(marker_path, saved)
    except Exception:
        if backup.exists():
            os.replace(backup, old)
        raise
    if backup.exists():
        backup.unlink()
    shutil.rmtree(stage)
    return {"path": str(old), "status": "saved", "observations": len(rows),
        "stored_bytes": saved["stored_bytes"], "read_bytes": progress["read_bytes"],
        "range_requests": progress["range_requests"], "transfer_task_seconds": progress["transfer_task_seconds"],
        "source_retries": progress["source_retries"],
        "failed_requests": progress["failed_requests"],
        "read_decode_crop_seconds": progress["read_decode_crop_seconds"],
        "reader_batch_seconds": progress["reader_batch_seconds"],
        "reader_cpu_seconds": progress["reader_cpu_seconds"],
        "zarr_write_seconds": progress["writer_seconds"],
        "writer_idle_seconds": progress["writer_idle_seconds"],
        "max_batch_payload_bytes": progress["max_batch_payload_bytes"],
        "pack_seconds": pack_seconds, "verify_seconds": verify_seconds,
        "write_seconds": time.perf_counter() - started, "assets": saved["assets"],
        "band_counts": saved["band_counts"],
        "corrupt_source_scans": saved["corrupt_source_scans"]}


def fetch(selection, destination, workers=8, reader_processes=2, prefetch_mib=512,
          block_size=1024*1024, monthly_writers=2, scratch=None,
          report_dir=None, progress=None, index_results=True,
          reuse_cmipf_root=None):
    """Fetch distinct MCMIPF months; only the coordinator updates DuckDB."""
    if selection.source != "goes" or selection.product != "ABI-L2-MCMIPF":
        raise ValueError("The grouped GOES writer requires ABI-L2-MCMIPF")
    if str(destination).startswith("hf"):
        raise ValueError("Grouped MCMIPF months currently require an explicit local destination")
    if min(workers, reader_processes, monthly_writers, block_size) < 1 or prefetch_mib < 16:
        raise ValueError("GOES reader, writer, block, and prefetch limits must be positive")
    from .monthly import _relative_path, _identity

    root = destination_root(destination)
    if reuse_cmipf_root and not Path(reuse_cmipf_root).is_dir():
        raise FileNotFoundError(f"CMIPF reuse root is not a directory: {reuse_cmipf_root}")
    archive_product = HYBRID_PRODUCT if reuse_cmipf_root else selection.product
    groups = {}
    for asset in selection.assets:
        relative = _relative_path(selection, asset)
        if reuse_cmipf_root:
            relative = relative.replace("ABI-L2-MCMIPF", HYBRID_PRODUCT, 1)
        target = str(Path(root) / relative)
        groups.setdefault(target, []).append(asset)
    started = time.perf_counter()
    started_at = iso(datetime.now(timezone.utc))
    outcomes = []
    completed_assets = 0
    tasks = sorted(groups.items())
    with ProcessPoolExecutor(max_workers=min(monthly_writers, len(tasks) or 1),
                             mp_context=multiprocessing.get_context("spawn")) as pool:
        futures = {pool.submit(_write_one, selection, assets, target, reader_processes,
                    workers, prefetch_mib, block_size, scratch,
                    reuse_cmipf_root): (target, assets)
                   for target, assets in tasks}
        for future in as_completed(futures):
            target, assets = futures[future]
            try:
                outcome = future.result()
            except Exception as exc:
                outcome = {"path": target, "status": "failed", "error": f"{type(exc).__name__}: {exc}",
                           "read_bytes": 0, "stored_bytes": 0, "assets": []}
            outcome.update(month=utc(assets[0].time).strftime("%Y/%m"), band=None)
            outcomes.append(outcome)
            completed_assets += len(assets)
            if progress:
                progress(completed_assets, len(selection.assets), outcome)
    outcomes.sort(key=lambda row: (row["month"], row["path"]))
    records = []
    for outcome in outcomes:
        for row in outcome.get("assets", []):
            for band in selection.bands:
                available = band in row.get("available_bands", [])
                records.append({"asset_id": row["asset_id"], "time": row["time"],
                    "source_url": row["source_url"], "etag": row["etag"],
                    "source_bytes": row["source_bytes"], "product": archive_product,
                    "url": outcome["path"] if available else None,
                    "status": "archived" if available and outcome["status"] == "saved" else
                              "reused" if available and outcome["status"] == "reused" else
                              "corrupt_source" if row.get("status") == "corrupt_source" else "missing_band",
                    "error": row.get("error"), "integrity_evidence": row.get("integrity_evidence"),
                    "band": band, "subset_id": _identity(selection, band) +
                        ("-HYBRID" if reuse_cmipf_root else "")})
    report = {"source": "goes", "selection_id": selection.id,
        "run_id": f"{selection.id}:{archive_product}:{started_at}",
        "started_at": started_at,
        "selection_summary": selection.summary(), "root": root, "records": records,
        "archive_product": archive_product,
        "reuse_cmipf_root": str(reuse_cmipf_root) if reuse_cmipf_root else None,
        "source_inventory": getattr(selection, "discovery_facts", {}),
        "monthly_archives": outcomes, "monthly_writers": min(monthly_writers, len(tasks) or 1),
        "read_bytes": sum(row.get("read_bytes", 0) for row in outcomes),
        "range_requests": sum(row.get("range_requests", 0) for row in outcomes),
        "source_retries": sum(row.get("source_retries", 0) for row in outcomes),
        "failed_requests": sum(row.get("failed_requests", 0) for row in outcomes),
        "stored_bytes": sum(row.get("stored_bytes", 0) for row in outcomes),
        "wall_s": time.perf_counter() - started, "destination": destination,
        "interrupted": any(row["status"] == "failed" for row in outcomes),
        "storage_layout": f"{archive_product} 2 km monthly Zarr v3 ZIP with per-band groups"}
    if index_results:
        from .index import record_fetch
        record_fetch(report)
    if report_dir is not None:
        suffix = f"-{archive_product}" if reuse_cmipf_root else ""
        write_json(Path(report_dir) / f"{selection.id}{suffix}-monthly.json", report)
    return report
