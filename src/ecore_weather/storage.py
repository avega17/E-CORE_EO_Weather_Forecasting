"""Save raw subsets once, locally or in the configured Hugging Face bucket."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
import time
import warnings
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from contextlib import nullcontext, contextmanager
import multiprocessing
from pathlib import Path

import numpy as np
import xarray as xr

from .common import PeakMemory, Selection, Transport, jsonable, write_json, default_workers, digest

RAW_SCHEMA_VERSION = 1


def valid_raw_name(name):
    """Accept canonical legacy names and verified versioned monthly stores."""
    return name in {"raw.zarr", "raw.zarr.zip"} or bool(
        re.fullmatch(r"raw-[0-9a-f]{32}\.zarr", str(name)))


def metadata_fingerprint(ds):
    metadata = {"dataset": jsonable(ds.attrs),
                "variables": {name: jsonable(ds[name].attrs) for name in sorted(ds.variables)}}
    return hashlib.sha256(json.dumps(metadata, sort_keys=True).encode()).hexdigest()


def configured_bucket():
    from dotenv import load_dotenv
    load_dotenv()
    name = os.getenv("HF_BUCKET_NAME") or os.getenv("HF_DATASET_REPO")
    if not name:
        raise ValueError("Set HF_BUCKET_NAME (namespace/bucket), or choose a local destination.")
    if "/" not in name:
        user = os.getenv("HF_USER")
        if not user:
            raise ValueError("Set HF_USER or include the namespace in HF_BUCKET_NAME.")
        name = f"{user}/{name}"
    return name


def destination_root(destination):
    if not str(destination).strip():
        raise ValueError("Choose 'hf' or an explicit local directory.")
    if str(destination) == "hf":
        return f"hf://buckets/{configured_bucket()}/noaa-subsets"
    if "://" in str(destination) and not str(destination).startswith("hf://buckets/"):
        raise ValueError("Use a Hugging Face bucket or a local directory.")
    return str(destination).rstrip("/") if str(destination).startswith("hf://") else str(Path(destination).expanduser().resolve())


def _hf_fs():
    from dotenv import load_dotenv
    from huggingface_hub import HfFileSystem
    load_dotenv()
    return HfFileSystem(token=os.getenv("HF_TOKEN") or os.getenv("HF_API_KEY"))


def fingerprint(ds):
    """Hash the complete decoded array content, including numeric sentinels."""
    h = hashlib.sha256()
    for name in sorted(ds.variables):
        var = ds[name]
        values = np.ascontiguousarray(var.values)
        h.update(name.encode())
        h.update(str((var.dims, values.dtype.str, values.shape)).encode())
        h.update(values.tobytes())
    return h.hexdigest()


def fingerprint_streaming(ds):
    """Equivalent to :func:`fingerprint`, reading time-dependent arrays in slices."""
    h = hashlib.sha256()
    for name in sorted(ds.variables):
        var = ds[name]
        h.update(name.encode())
        h.update(str((var.dims, var.dtype.str, var.shape)).encode())
        if "time" in var.dims and var.dims[0] == "time":
            for index in range(var.shape[0]):
                h.update(np.ascontiguousarray(var.isel(time=index).values).tobytes())
        else:
            h.update(np.ascontiguousarray(var.values).tobytes())
    return h.hexdigest()


def write_raw(ds, path):
    # Zarr format 3 stores; the Blosc codec and chunk rule are unchanged from the
    # earlier format 2 stores, so content bytes and fingerprints stay comparable.
    from zarr.codecs import BloscCodec, BloscShuffle
    stored = ds.copy(deep=False)
    stored.attrs = jsonable(ds.attrs)
    encoding = {}
    for name in stored.variables:
        stored[name].attrs = jsonable(ds[name].attrs)
        stored[name].encoding = {}
        var = stored[name]
        encoding[name] = {"compressors": BloscCodec(cname="zstd", clevel=3, shuffle=BloscShuffle.shuffle)}
        if var.ndim:
            encoding[name]["chunks"] = tuple(min(256, size) for size in var.shape)
        # Do not introduce float NaN fill codes into a raw source array.
        if "_FillValue" not in var.attrs:
            encoding[name]["_FillValue"] = None
    # Consolidated metadata is deliberate (small stores, fast open); zarr-python 3
    # warns per store that it is not yet in the format 3 spec, which is noise on a
    # long run. Silence only that specific warning around the intentional write.
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Consolidated metadata is currently not part",
                                category=UserWarning, module=r"zarr.*")
        stored.to_zarr(str(path), mode="w", zarr_format=3, consolidated=True, encoding=encoding)
    with open_raw(path) as reopened:
        if fingerprint(reopened) != fingerprint(stored):
            raise ValueError("The saved raw arrays differ from the source subset.")
        if metadata_fingerprint(reopened) != metadata_fingerprint(stored):
            raise ValueError("The saved raw metadata differ from the source subset.")


def open_raw(path, group=None):
    temporary = None
    archive = None
    metadata = None
    zip_path = None
    if str(path).endswith(".zip"):
        from zarr.storage import ZipStore
        if str(path).startswith("hf://"):
            from .hf_storage import BucketWriter
            bucket_root, name = str(path).rsplit("/", 1)
            writer = BucketWriter(bucket_root)
            temporary = tempfile.TemporaryDirectory(prefix="ecore-raw-read-")
            try:
                zip_path = Path(temporary.name) / name
                writer.client.download_file(writer.config["bucket"], writer.key(name), str(zip_path))
                archive = ZipStore(str(zip_path), mode="r")
            except Exception:
                temporary.cleanup()
                raise
        else:
            zip_path = Path(path)
            archive = ZipStore(str(path), mode="r")
        try:
            import zipfile
            with zipfile.ZipFile(zip_path, "r") as zipped:
                if "ecore_metadata.json.gz" in zipped.namelist():
                    import gzip
                    metadata = json.loads(gzip.decompress(zipped.read("ecore_metadata.json.gz")))
                else:
                    metadata = json.loads(zipped.read("ecore_metadata.json"))
        except (KeyError, OSError, zipfile.BadZipFile):
            metadata = None
        mapper = archive
    elif str(path).startswith("hf://"):
        # Monthly remote Zarr uses the same path-style HF S3 gateway as the
        # Earth2Studio writer. The Hub filesystem can list buckets but is not
        # the reader for these native object-store arrays.
        from .earth2_io import hf_store
        from .hf_storage import BucketWriter
        from zarr.storage import ObjectStore
        writer = BucketWriter(str(path))
        mapper = ObjectStore(hf_store(writer.bucket_id, writer.prefix), read_only=True)
        try:
            sidecar = writer.client.get_object(Bucket=writer.config["bucket"],
                Key=writer.key("ecore_metadata.json"))
            metadata = json.loads(sidecar["Body"].read())
        except Exception as error:
            code = getattr(error, "response", {}).get("Error", {}).get("Code")
            if code not in {"NoSuchKey", "404", "NotFound"}:
                raise
    else:
        mapper = str(path)
        metadata_path = Path(path) / "ecore_metadata.json"
        if metadata_path.is_file():
            metadata = json.loads(metadata_path.read_text())
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="Consolidated metadata is currently not part",
                                    category=UserWarning, module=r"zarr.*")
            ds = xr.open_zarr(mapper, group=group, consolidated=None,
                              decode_cf=False, mask_and_scale=False, chunks=None)
        if metadata:
            ds.attrs = metadata.get("dataset_attrs", ds.attrs)
            if group and metadata.get("groups"):
                ids = metadata["groups"][group]["asset_ids"]
                if len(ids) != ds.sizes.get("time", 0):
                    raise IOError(f"Band-group provenance length differs from Zarr time: {group}")
                ds = ds.assign_coords(source_asset_id=("time", ids),
                    source_metadata_json=("time", [metadata["source_metadata"][key] for key in ids]))
            for name, attrs in metadata.get("variable_attrs", {}).items():
                if name in ds.variables:
                    ds[name].attrs = attrs
            for name, info in metadata.get("auxiliary_variables", {}).items():
                values = np.asarray(info["values"], dtype=np.dtype(info["dtype"]))
                ds[name] = (tuple(info["dims"]), values)
                ds[name].attrs = metadata.get("variable_attrs", {}).get(name, {})
            for name, attrs in metadata.get("coordinate_attrs", {}).items():
                if name in ds.coords:
                    ds[name].attrs = attrs
            for name, info in metadata.get("auxiliary_coords", {}).items():
                values = np.asarray(info["values"])
                ds = ds.assign_coords({name: (tuple(info["dims"]), values)})
                ds[name].attrs = info.get("attrs", {})
    except Exception:
        if archive: archive.close()
        if temporary: temporary.cleanup()
        raise
    original_close = ds._close
    def close():
        if original_close: original_close()
        if archive: archive.close()
        if temporary: temporary.cleanup()
    ds.set_close(close)
    return ds


def pack_raw(staging):
    """Container-only ZIP: retain existing Zarr compression and every byte."""
    import zipfile
    source = Path(staging)/"raw.zarr"
    path = Path(staging)/"raw.zarr.zip"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_STORED, allowZip64=True) as archive:
        for file in sorted(source.rglob("*")):
            if file.is_file(): archive.write(file, file.relative_to(source).as_posix())
    with open_raw(source) as before, open_raw(path) as after:
        if fingerprint(before) != fingerprint(after) or metadata_fingerprint(before) != metadata_fingerprint(after):
            raise IOError("ZIP container changed raw data or metadata.")
    shutil.rmtree(source)
    return path


def _read_marker(root):
    if root.startswith("hf://"):
        from .hf_storage import BucketWriter
        return BucketWriter(root).marker()
    path = Path(root) / "complete.json"
    return json.loads(path.read_text()) if path.exists() else None


def _publish(staging, root, marker):
    """Publish the arrays before the completion marker; never sync deletions."""
    if root.startswith("hf://"):
        from .hf_storage import BucketWriter
        BucketWriter(root).publish(staging, "", marker)
    else:
        target = Path(root)
        target.parent.mkdir(parents=True, exist_ok=True)
        # An incomplete prior attempt may be replaced; no other dataset is touched.
        if target.exists():
            shutil.rmtree(target)
        # DrvFS/NTFS may reject POSIX chmod/copystat. Scientific metadata live
        # inside Zarr; copy bytes without imposing Linux filesystem attributes.
        target.mkdir(parents=True)
        for source in Path(staging).rglob("*"):
            relative = source.relative_to(staging)
            if relative.as_posix() == "complete.json": continue
            if source.is_dir(): (target/relative).mkdir(parents=True, exist_ok=True)
            else:
                (target/relative).parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target/relative)
        with open_raw(target/marker.get("raw_path", "raw.zarr")) as actual:
            if fingerprint(actual) != marker["array_sha256"] or metadata_fingerprint(actual) != marker["metadata_sha256"]:
                raise IOError("Local archive copy failed read-back validation.")
        write_json(target / "complete.json", marker)


def _process_read(asset, source, bbox, product, bands, backend, block_size=1024 * 1024):
    """Independent HDF5 reader, avoiding h5py's process-wide thread lock."""
    from .earth2_sources import read_selected_asset
    with Transport(backend) as transport:
        ds, timings = read_selected_asset(asset, source, bbox, product, bands,
                                          transport, block_size=block_size)
        return ds, timings, transport.bytes, transport.requests


def subset_specification(selection):
    """Identity of requested native pixels, independent of dates or worker options."""
    # A CMIP object already identifies its single band, including its native grid.
    bands = () if selection.product.startswith("ABI-L2-CMIP") else tuple(sorted(set(selection.bands)))
    return {"source": selection.source, "product": selection.product,
            "satellite": selection.satellite, "bbox": tuple(map(float, selection.bbox)), "bands": bands}


def subset_identity(selection):
    return digest(subset_specification(selection))


def product_path(selection):
    return f"{selection.source}/{selection.product}" + (f"/goes{selection.satellite}" if selection.satellite else "")


def infer_goes_product(destination, satellite, bands, products=("ABI-L2-MCMIPF", "ABI-L2-CMIPF")):
    """Choose MCMIPF for a fresh period, CMIPF when bands are already stored.

    The multiband MCMIPF store fetches every requested band in one file, so it
    suits a new period. The single-band CMIPF store reuses band subsets already
    present, so it suits adding bands over time. We look for any requested band
    already completed under either product (band is in the CMIP source name) and
    prefer CMIPF only when one is found; otherwise MCMIPF. Explicit --product
    always wins; this only fills in a missing choice.
    """
    import re
    base = Path(destination) / "goes"
    wanted = set(map(int, bands)) if bands else set()
    found = set()
    for product in products:
        root = base / product / f"goes{satellite}"
        if not root.is_dir():
            continue
        for marker in root.rglob("complete.json"):
            try:
                url = json.loads(marker.read_text()).get("source_url", "")
            except (OSError, json.JSONDecodeError):
                continue
            match = re.search(r"-M\dC(\d{2})_", url)
            if match:
                found.add(int(match[1]))
            else:
                # A multiband subset: any requested band counts as present.
                found |= wanted
    present = (wanted & found) if wanted else found
    return "ABI-L2-CMIPF" if present else "ABI-L2-MCMIPF"


@contextmanager
def object_writer(path):
    """Coordinate repeated requests on this workstation, one source subset at a time."""
    import fcntl
    folder = Path(tempfile.gettempdir()) / "ecore-object-locks"
    folder.mkdir(exist_ok=True)
    with (folder / (digest(str(path)) + ".lock")).open("a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _older_local_stores(destination, selection):
    """Index earlier dated roots once; never scan inside the Zarr chunks."""
    base = Path(destination) / product_path(selection)
    found = {}
    if base.exists():
        for period in sorted(base.iterdir()):
            if not period.is_dir() or period.name.startswith("roi-"):
                continue
            for marker in period.glob("*/*/*/*/complete.json"):
                asset_id = marker.parent.name.rsplit("-", 1)[-1]
                found.setdefault(asset_id, []).append(marker.parent)
    return found


def _adopt_local(candidates, out, selection, asset, subset_id):
    """Move only a verified matching old store; preserve all original metadata."""
    from .goes import science_variables
    for candidate in candidates:
        with object_writer(candidate):
            marker = _read_marker(str(candidate))
            if not marker or marker.get("asset_id") != asset.id:
                continue
            raw_path = marker.get("raw_path", "raw.zarr")
            if raw_path not in {"raw.zarr", "raw.zarr.zip"}:
                continue
            with open_raw(candidate/raw_path) as ds:
                if tuple(ds.attrs.get("requested_bbox", ())) != tuple(selection.bbox):
                    continue
                if selection.source == "goes" and selection.product.startswith("ABI-L2-MCMIP"):
                    if set(science_variables(ds)) != {f"CMI_C{b:02d}" for b in selection.bands}:
                        continue
                if fingerprint(ds) != marker["array_sha256"] or metadata_fingerprint(ds) != marker["metadata_sha256"]:
                    raise IOError(f"Existing subset failed verification: {candidate}")
            target = Path(out)
            if target.exists():
                # Never remove an unfinished or unknown folder during adoption.
                return False
            target.parent.mkdir(parents=True, exist_ok=True)
            marker.update(subset_id=subset_id, previous_location=str(candidate))
            write_json(candidate/"complete.json", marker)
            candidate.rename(target)  # Same archive filesystem: no data copy.
            return True
    return False


def fetch(selection: Selection, destination="hf", workers=None, backend="s3fs", scratch=None,
          report_dir="artifacts/runs", progress=None, decode_workers=1, validate_only=False, inspect=None,
          layout="readable", read_processes=0, container="directory", block_size=1024 * 1024,
          index_results=True):
    """Fetch one small store per source file, with bounded memory and simple resume.

    The returned rows include array locations, timings, and failures. A rerun
    verifies existing stores before skipping them. No legacy files are retained.
    """
    from . import goes, mrms
    workers = default_workers() if workers is None else workers
    if read_processes < 0:
        raise ValueError("read_processes cannot be negative.")
    read_pool = None
    if workers < 1:
        raise ValueError("workers must be positive.")
    if container not in {"directory", "zip"}:
        raise ValueError("container must be directory or zip.")
    raw_name = "raw.zarr.zip" if container == "zip" else "raw.zarr"
    if layout not in {"readable", "legacy"}:
        raise ValueError("layout must be readable or legacy.")
    selection_id = selection.id
    subset_id = subset_identity(selection)
    label = selection_id if layout == "legacy" else f"{product_path(selection)}/roi-{subset_id}"
    root = "validation-only" if validate_only else destination_root(destination) + "/" + label
    older = {} if validate_only or root.startswith("hf://") or layout == "legacy" else _older_local_stores(destination_root(destination), selection)
    publisher = None
    transport = Transport(backend, decode_workers=decode_workers)
    start = time.perf_counter()
    rows = []
    matches = {m["asset_id"]: m for m in selection.hourly_matches}
    if scratch:
        Path(scratch).mkdir(parents=True, exist_ok=True)
    if root.startswith("hf://"):
        from .hf_storage import BucketWriter
        publisher = BucketWriter(root)
        publisher.check()
    if not validate_only:
        definition = {**subset_specification(selection), "subset_id": subset_id,
                      "note": "Shared native-pixel subsets; dates and hourly matching belong to selection/run records. CMIP band identity is in each source file."}
        if publisher:
            publisher.put_json("subset.json", definition)
        else:
            Path(root).mkdir(parents=True, exist_ok=True)
            # Atomic metadata replacement, including on the Windows-mounted archive.
            with tempfile.NamedTemporaryFile(mode="w", dir=root, prefix=".subset-", delete=False) as file:
                json.dump(definition, file, indent=2)
                name = file.name
            try:
                os.replace(name, Path(root)/"subset.json")
            finally:
                if Path(name).exists(): Path(name).unlink()

    def task_locked(asset):
        stamp = asset.time[:19].replace("-", "").replace(":", "")
        relative = asset.id if layout == "legacy" else f"{stamp[:4]}/{stamp[4:6]}/{stamp[6:8]}/{stamp[9:]}-{asset.id}"
        out = root + "/" + relative
        row = {"asset_id": asset.id, "time": asset.time, "url": out + "/" + raw_name,
               "source_url": asset.url, "relative_path": relative+"/"+raw_name,
               "source_bytes": asset.size, "etag": asset.etag, "product": selection.product}
        if asset.id in matches:
            row.update(slot_time=matches[asset.id]["slot_time"], offset_seconds=matches[asset.id]["offset_seconds"])
        try:
            if publisher:
                existing_path = publisher.resume(relative, asset.id, RAW_SCHEMA_VERSION,
                                                 subset_id=subset_id if layout != "legacy" else None)
                if existing_path:
                    return {**row, "url": out+"/"+existing_path, "relative_path": relative+"/"+existing_path, "status": "reused"}
            if not validate_only and not publisher and not Path(out).exists():
                _adopt_local(older.get(asset.id, ()), out, selection, asset, subset_id)
            marker = None if validate_only or publisher else _read_marker(out)
            if (marker and marker.get("raw_schema_version") == RAW_SCHEMA_VERSION and
                    marker.get("asset_id") == asset.id and
                    (marker.get("subset_id") == subset_id if layout != "legacy" else marker.get("selection_id") == selection_id)):
                existing_path = marker.get("raw_path", "raw.zarr")
                if existing_path not in {"raw.zarr", "raw.zarr.zip"}:
                    raise ValueError("Invalid raw path in existing completion marker.")
                row.update(url=out+"/"+existing_path, relative_path=relative+"/"+existing_path)
                with open_raw(row["url"]) as existing:
                    if (fingerprint(existing) == marker["array_sha256"] and
                            metadata_fingerprint(existing) == marker["metadata_sha256"]):
                        return {**row, "status": "reused"}
            with tempfile.TemporaryDirectory(prefix="ecore-subset-", dir=scratch) as temp:
                staging = Path(temp)
                if shutil.disk_usage(staging).free < max(64 * 1024**2, asset.size * 2):
                    raise OSError("Not enough free scratch space for this source file.")
                if read_pool is not None:
                    ds, timings, read_bytes, calls = read_pool.submit(_process_read, asset, selection.source,
                        selection.bbox, selection.product, selection.bands, backend, block_size).result()
                    with transport._lock:
                        transport.bytes += read_bytes
                        transport.requests += calls
                elif selection.source == "mrms":
                    from .earth2_sources import read_selected_asset
                    ds, timings = read_selected_asset(asset, selection.source, selection.bbox,
                                                       selection.product, selection.bands, transport)
                else:
                    from .earth2_sources import read_selected_asset
                    ds, timings = read_selected_asset(asset, selection.source, selection.bbox,
                                                       selection.product, selection.bands, transport,
                                                       block_size=block_size)
                with ds:
                    before = time.perf_counter()
                    write_raw(ds, staging / "raw.zarr")
                    if container == "zip": pack_raw(staging)
                    row.update(timings, write_s=time.perf_counter() - before,
                               logical_array_bytes=ds.nbytes,
                               stored_bytes=sum(p.stat().st_size for p in staging.rglob("*") if p.is_file()))
                    marker = {"asset_id": asset.id, "selection_id": selection_id, "subset_id": subset_id,
                              "array_sha256": fingerprint(ds), "source_url": asset.url,
                              "metadata_sha256": metadata_fingerprint(ds),
                              "raw_schema_version": RAW_SCHEMA_VERSION, "source_etag": asset.etag,
                              "raw_path": raw_name, "time": asset.time, "product": selection.product,
                              "source_bytes": asset.size}
                    before = time.perf_counter()
                    if inspect is not None:
                        inspection = ds.copy(deep=False)
                        if asset.id in matches:
                            inspection.attrs = {**ds.attrs, "hourly_slot": matches[asset.id]["slot_time"],
                                                "hourly_slot_offset_seconds": matches[asset.id]["offset_seconds"]}
                        row["diagnostics"] = inspect(inspection)
                    row["diagnostics_s"] = time.perf_counter() - before
                    before = time.perf_counter()
                    if not validate_only:
                        if publisher:
                            publisher.publish(staging, relative, marker)
                        else:
                            _publish(staging, out, marker)
                    row["publish_s"] = time.perf_counter() - before
            row["status"] = "validated" if validate_only else "saved"
            if validate_only:
                row["url"] = None
        except Exception as e:
            row.update(status="failed", error=f"{type(e).__name__}: {e}")
        return row

    def task(asset):
        # Lock the stable identity through read, validation and completion.
        with object_writer(root + "/" + asset.id):
            return task_locked(asset)

    readers = (ProcessPoolExecutor(max_workers=min(read_processes, workers),
               mp_context=multiprocessing.get_context("spawn")) if read_processes else nullcontext(None))
    interrupted = None
    with readers as read_pool, transport, PeakMemory() as memory, ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(task, a) for a in selection.assets]
        try:
            for f in as_completed(futures):
                rows.append(f.result())
                if progress:
                    progress(len(rows), len(futures), rows[-1])
        except BaseException as error:
            # Do not let notebook interruption drain months of queued downloads.
            for future in futures:
                future.cancel()
            if not isinstance(error, KeyboardInterrupt):
                raise
            interrupted = error
    if interrupted is not None:
        # The bounded in-flight tasks finished and cleaned scratch on context exit.
        rows = [future.result() for future in futures if not future.cancelled()]
    rows.sort(key=lambda r: (r["time"], r["asset_id"]))
    report = {"selection_id": selection_id, "source": selection.source, "root": root,
              "started_at": datetime.now(timezone.utc).isoformat(),
              "interrupted": interrupted is not None, "not_started": len(selection.assets)-len(rows),
              "wall_s": time.perf_counter() - start, "read_bytes": transport.bytes,
              "data_read_calls": transport.requests, "peak_rss_bytes": memory.peak,
              "backend": backend, "workers": workers, "decode_workers": decode_workers,
              "read_processes": min(read_processes, workers),
              "selection_summary": selection.summary(), "records": rows, "layout": layout, "container": container,
              "effective_MB_s": transport.bytes / max(time.perf_counter()-start, 1e-9) / 1e6,
              "source_read_seconds": transport.seconds,
              "source_read_MB_s": transport.bytes / max(transport.seconds, 1e-9) / 1e6,
              "stored_bytes": sum(r.get("stored_bytes", 0) for r in rows),
              "logical_array_bytes": sum(r.get("logical_array_bytes", 0) for r in rows),
              "hf_batch_calls": 0,
              "hf_readback_bytes": publisher.readback_bytes if publisher else 0,
              "hf_upload_bytes": publisher.upload_bytes if publisher else 0,
              "hf_upload_seconds": publisher.upload_seconds if publisher else 0}
    if index_results:
        try:
            from .index import record_fetch
            record_fetch(report)
        except ImportError:
            pass
    if report_dir is not None:
        from .runlog import save
        save(Path(report_dir) / f"{selection_id}-{backend}-{workers}.json", report)
    if interrupted is not None:
        raise interrupted
    return report
