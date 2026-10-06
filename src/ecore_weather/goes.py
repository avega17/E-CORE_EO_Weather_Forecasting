"""GOES CMI imagery: select bands and a native-grid window before loading."""

from __future__ import annotations

import json
import hashlib
import re
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
from pyproj import CRS, Transformer

from .common import (Asset, PR_BBOX, Selection, Transport, digest, hours, iso,
                     jsonable, list_objects, remote_file, s3_client, utc, validate_request, write_json)

PRODUCTS = ("ABI-L2-CMIPF", "ABI-L2-CMIPC", "ABI-L2-MCMIPF", "ABI-L2-MCMIPC")
STORMSCOPE_BANDS = (1, 2, 3, 7, 8, 9, 10, 13)


def hdf_integrity_error(exc):
    return any(message in str(exc).lower() for message in (
        'incorrect metadata checksum', 'filter returned failure', 'data error detected by fletcher32'))


class ConfirmedCorruptSourceError(OSError):
    """An exact, checksum-verified NOAA object cannot supply the requested pixels."""

    def __init__(self, error, evidence):
        super().__init__(str(error))
        self.evidence = evidence


def east_satellite(start, end):
    # NOAA OSPO reports GOES-19 became operational East at 15:00 UTC.
    transition = utc("2025-04-07T15:00:00Z")
    if utc(start) < transition < utc(end):
        raise ValueError("Split a GOES-East request at the GOES-16/19 transition, or choose a satellite explicitly.")
    return 19 if utc(start) >= transition else 16


def scan_time(text):
    return datetime.strptime(text[:13], "%Y%j%H%M%S").replace(
        tzinfo=timezone.utc, microsecond=int(text[13:].ljust(6, "0") or 0))


def hourly_scans(assets, count):
    """Keep at most count scans per UTC hour, nearest to evenly spaced marks.

    count=1 selects the scan nearest the top of the hour; count=2 the scans
    nearest :00 and :30, and so on. No scan is reused for two marks, and ties
    break toward the earlier time then key, so the result is deterministic.
    CMIP products apply the rule per band: each band keeps its own count scans.
    """
    if count < 1:
        raise ValueError("scans_per_hour must be at least 1 (0 requests all scans).")
    groups = {}
    for asset in assets:
        band_match = re.search(r"-M\dC(\d\d)_", asset.key)
        band = int(band_match[1]) if band_match else None
        groups.setdefault((utc(asset.time).replace(minute=0, second=0, microsecond=0), band), []).append(asset)
    picked = []
    for (hour, _), candidates in groups.items():
        marks = [hour + timedelta(minutes=60 * i / count) for i in range(count)]
        remaining = sorted(candidates, key=lambda a: (a.time, a.key))
        for mark in marks:
            if not remaining:
                break
            best = min(remaining, key=lambda a: (abs((utc(a.time) - mark).total_seconds()), utc(a.time), a.key))
            picked.append(best)
            remaining.remove(best)
    return sorted(picked, key=lambda a: (a.time, a.key))


def discover(start, end, bbox=PR_BBOX, bands=STORMSCOPE_BANDS, satellite="auto", product="ABI-L2-CMIPF", scans_per_hour=None):
    start, end, bbox = validate_request(start, end, bbox)
    satellite = east_satellite(start, end) if satellite == "auto" else int(satellite)
    if product not in PRODUCTS:
        raise ValueError(f"Supported CMI products: {', '.join(PRODUCTS)}")
    if scans_per_hour is not None and not 1 <= int(scans_per_hour) <= 6:
        raise ValueError("Choose 1-6 scans per hour, or 0/None for every available scan.")
    bands = tuple(sorted(set(map(int, bands))))
    if not bands or any(b < 1 or b > 16 for b in bands):
        raise ValueError("Select one or more ABI bands from 1 to 16.")
    if satellite not in (16, 17, 18, 19):
        raise ValueError("Select GOES-16, 17, 18, or 19.")
    client = s3_client()
    bucket = f"noaa-goes{satellite}"

    def listing(hour):
        return list(list_objects(bucket, f"{product}/{hour:%Y/%j/%H}/", client))

    candidates = {}
    duplicates = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        for objects in pool.map(listing, hours(start, end)):
            for obj in objects:
                key = obj["Key"]
                match = re.search(r"_s(\d+)_e(\d+)_c(\d+)\.nc$", key)
                if not match:
                    continue
                when, until = scan_time(match[1]), scan_time(match[2])
                band_match = re.search(r"-M\dC(\d\d)_", key)
                band = int(band_match[1]) if band_match else None
                if band is not None and band not in bands:
                    continue
                if start <= when < end:
                    # Keep the newest processing of a scan if duplicates are listed.
                    identity = (when, band)
                    asset = Asset(bucket, key, obj["Size"], obj["ETag"].strip('"'), iso(when), iso(until))
                    if identity in candidates:
                        previous = candidates[identity]
                        duplicates.append({"time": iso(when), "band": band,
                            "kept_key": max(previous.key, key),
                            "discarded_key": min(previous.key, key)})
                    if identity not in candidates or key > candidates[identity].key:
                        candidates[identity] = asset
    assets = sorted(candidates.values(), key=lambda a: (a.time, a.key))
    if scans_per_hour is not None:
        assets = hourly_scans(assets, int(scans_per_hour))
    selection = Selection("goes", product, iso(start), iso(end), bbox, assets,
                          bands=bands, satellite=satellite, scans_per_hour=scans_per_hour)
    coverage = acquisition_coverage(selection)
    selection.discovery_facts = {
        "duplicate_processing_versions": duplicates,
        "hours_without_files": [iso(row.hour) for row in coverage.itertuples()
                                if row.observed_files == 0],
        "undersubscribed_hours": [{"hour": iso(row.hour),
            "observed_files": int(row.observed_files),
            "expected_files": int(row.expected_files)} for row in coverage.itertuples()
            if pd.notna(row.expected_files) and row.observed_files < row.expected_files],
        "scan_mode_changes": [{"time": assets[i].time, "from": modes[i-1],
            "to": modes[i]} for i in range(1, len(assets)) if modes[i] != modes[i-1]]
            if (modes := [re.search(r"-M(\d)", asset.key)[1] for asset in assets]) else [],
    }
    return selection
def acquisition_coverage(selection):
    """Expected versus observed slots for the supported full-disk/CONUS modes.

    A decimated selection (scans_per_hour set) counts only its own scans, so the
    hourly totals are not a statement about archive completeness.
    """
    from collections import defaultdict
    by_hour = defaultdict(list)
    for asset in selection.assets:
        by_hour[utc(asset.time).replace(minute=0, second=0, microsecond=0)].append(asset)
    rows = []
    for hour in hours(selection.start, selection.end):
        assets = by_hour[hour]
        modes = {re.search(r"-M(\d)", a.key)[1] for a in assets}
        if selection.product.endswith("C"):
            expected = 12
        elif modes == {"6"}:
            expected = 6
        elif modes == {"3"}:
            expected = 4
        else:
            expected = None
        multiplier = len(selection.bands) if "MCMIP" not in selection.product else 1
        rows.append({"hour": iso(hour), "observed_files": len(assets),
                     "scan_modes": ",".join(sorted(modes)),
                     "expected_files": expected * multiplier if expected else None,
                     "note": "" if expected else "Missing-file count not inferred for absent, mixed, or unsupported scan modes."})
    return pd.DataFrame(rows)


def _physical_coordinate(var):
    return var.values.astype("float64") * var.attrs.get("scale_factor", 1) + var.attrs.get("add_offset", 0)


def projection(ds):
    if "goes_imager_projection" in ds:
        attrs = ds["goes_imager_projection"].attrs
    elif "source_metadata_json" in ds.coords:
        # Monthly native-band archives keep per-scan scalar metadata in a
        # sidecar so calibration epochs cannot be silently merged. A selected
        # observation has one scalar JSON record to recover its projection.
        value = np.asarray(ds.source_metadata_json.values)
        if value.size != 1:
            raise ValueError("Select one GOES observation before using its projection")
        metadata = json.loads(str(value.item()))
        attrs = metadata["variables"]["goes_imager_projection"]["attrs"]
    else:
        raise KeyError("GOES projection metadata is unavailable")
    return CRS.from_cf(attrs), float(attrs["perspective_point_height"])


def native_window(ds, bbox, halo=0):
    crs, height = projection(ds)
    transform = Transformer.from_crs("EPSG:4326", crs, always_xy=True)
    left, bottom, right, top = transform.transform_bounds(*bbox, densify_pts=41)
    if not np.isfinite([left, bottom, right, top]).all():
        raise ValueError("Part of the requested region is outside the satellite view.")
    x = _physical_coordinate(ds.x) * height
    y = _physical_coordinate(ds.y) * height
    if left < x.min() or right > x.max() or bottom < y.min() or top > y.max():
        raise ValueError("The requested region is outside this sector. Try full disk or a smaller region.")
    xi, yi = np.flatnonzero((x >= left) & (x <= right)), np.flatnonzero((y >= bottom) & (y <= top))
    if not len(xi) or not len(yi):
        raise ValueError("The region contains no source pixels.")
    return {"x": slice(max(0, xi.min()-halo), min(len(x), xi.max()+1+halo)),
            "y": slice(max(0, yi.min()-halo), min(len(y), yi.max()+1+halo))}


def selected_variables(ds, bands, allow_missing=False):
    if "CMI" in ds:
        band = int(ds.band_id.values.item())
        if band not in bands:
            raise ValueError("The single-band file does not contain a requested band.")
        names = ["CMI", "DQF", "band_id", "band_wavelength"]
    else:
        names = [f"{prefix}_C{b:02d}" for b in bands for prefix in ("CMI", "DQF")]
        missing = [name for name in names if name not in ds]
        if missing and not allow_missing:
            raise ValueError(f"The file is missing requested variables: {missing}")
        names = [name for name in names if name in ds]
        if allow_missing and not any(name.startswith("CMI_C") for name in names):
            raise ValueError("The multiband file has none of the requested CMI channels")
        names += [name for name in ds.variables if any(name.endswith(f"_C{b:02d}") for b in bands) and ds[name].ndim == 0]
    names += ["goes_imager_projection", "t"]
    for name in names:
        if name in ds:
            for attr in ("coordinates", "bounds", "grid_mapping"):
                names.extend(n for n in str(ds[name].attrs.get(attr, "")).split() if n in ds and n not in names)
    return list(dict.fromkeys(n for n in names if n in ds))


def subset_dataset(ds, bbox, bands, allow_missing=False):
    window = native_window(ds, bbox)
    return ds[selected_variables(ds, bands, allow_missing=allow_missing)].isel(window).load()


def read(asset, bbox=PR_BBOX, bands=(8, 13), transport=None, full_file=False,
         block_size=1024 * 1024, allow_missing=False):
    if transport is None:
        with Transport() as owned:
            return read(asset, bbox, bands, owned, full_file, block_size, allow_missing)
    before = time.perf_counter()
    if full_file:
        with tempfile.TemporaryDirectory(prefix="ecore-goes-") as temp:
            path = Path(temp) / "source.nc"
            path.write_bytes(transport.read(asset))
            if path.stat().st_size != asset.size:
                raise IOError("Incomplete source download.")
            try:
                with xr.open_dataset(path, engine="h5netcdf", decode_cf=False, mask_and_scale=False) as ds:
                    subset = subset_dataset(ds, bbox, bands, allow_missing)
            except Exception as exc:
                # Only classify HDF corruption after verifying the complete
                # object's content, not a failed range request or partial download.
                if hdf_integrity_error(exc):
                    checksum = hashlib.md5()
                    with path.open('rb') as stream:
                        for block in iter(lambda: stream.read(4 * 1024**2), b''):
                            checksum.update(block)
                    if checksum.hexdigest() == asset.etag.strip('"').lower():
                        raise ConfirmedCorruptSourceError(exc, {
                            'verification': 'full-object-size-and-single-part-etag-md5',
                            'download_bytes': path.stat().st_size,
                            'md5': checksum.hexdigest(),
                            'error': f'{type(exc).__name__}: {exc}'}) from exc
                raise
    else:
        with remote_file(asset, transport, block_size=block_size) as source:
            with xr.open_dataset(source, engine="h5netcdf", decode_cf=False, mask_and_scale=False) as ds:
                subset = subset_dataset(ds, bbox, bands, allow_missing)
    elapsed = time.perf_counter() - before
    subset.attrs.update(source_url=asset.url, source_etag=asset.etag,
                        observation_time=asset.time, scan_end=asset.end_time, requested_bbox=list(bbox))
    return subset, {"read_decode_crop_s": elapsed}


def decode(ds):
    """A new, physically decoded view. Packed raw arrays remain unchanged."""
    return xr.decode_cf(ds.copy(deep=True))


def science_variables(ds):
    return [name for name in ds.data_vars if name == "CMI" or name.startswith("CMI_C")]


def process(ds, variable="CMI_C13", mask_quality=True, reproject=False, bbox=PR_BBOX, shape=(768, 768)):
    import rioxarray  # noqa: F401
    from rasterio.enums import Resampling
    from rasterio.transform import from_bounds
    decoded = decode(ds)
    arr = decoded[variable]
    flag = variable.replace("CMI", "DQF")
    if mask_quality and flag in ds:
        arr = arr.where(decoded[flag] == 0)
    if not reproject:
        return arr
    crs, height = projection(ds)
    arr = arr.assign_coords(x=_physical_coordinate(ds.x)*height, y=_physical_coordinate(ds.y)*height)
    arr = arr.rio.write_crs(crs).rio.write_nodata(np.nan)
    return arr.rio.reproject("EPSG:4326", shape=shape, transform=from_bounds(*bbox, shape[1], shape[0]),
                             resampling=Resampling.bilinear)


def lonlat(ds):
    crs, height = projection(ds)
    x, y = np.meshgrid(_physical_coordinate(ds.x)*height, _physical_coordinate(ds.y)*height)
    return Transformer.from_crs(crs, "EPSG:4326", always_xy=True).transform(x, y)


def benchmark_assets(selection):
    """Actual scans nearest midnight/noon on first, middle, final included days.

    Partial-day requests use the available scans in that day. Single-band products
    retain each requested band; duplicate choices from short windows are removed.
    """
    first = utc(selection.start).replace(hour=0, minute=0, second=0, microsecond=0)
    last = utc(selection.end) - timedelta(microseconds=1)
    days = (last.date() - first.date()).days + 1
    picked = {}
    for day in sorted({0, days//2, days-1}):
        date = (first + timedelta(days=day)).date()
        available = [a for a in selection.assets if utc(a.time).date() == date]
        groups = {}
        for asset in available:
            band = re.search(r"-M\dC(\d\d)_", asset.key)
            groups.setdefault(band[1] if band else "multiband", []).append(asset)
        for hour in (0, 12):
            target = first + timedelta(days=day, hours=hour)
            for candidates in groups.values():
                asset = min(candidates, key=lambda a: (abs((utc(a.time)-target).total_seconds()), a.time, a.key))
                picked[asset.id] = asset
    return sorted(picked.values(), key=lambda a: (a.time, a.key))


def build_virtual(selection, directory, assets=None):
    """Write Kerchunk references with VirtualiZarr, grouping matching encodings.

    References describe full native variables. The small index records the crop,
    applied lazily when opened; no local legacy files are needed or retained.
    """
    from obstore.store import S3Store
    from obspec_utils.registry import ObjectStoreRegistry
    from virtualizarr import open_virtual_dataset
    from virtualizarr.parsers import HDFParser
    start = time.perf_counter()
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    groups, records = {}, []
    for asset in assets if assets is not None else selection.assets:
        bucket_url = f"s3://{asset.bucket}"
        registry = ObjectStoreRegistry({bucket_url: S3Store(asset.bucket, region="us-east-1", skip_signature=True)})
        with open_virtual_dataset(f"{bucket_url}/{asset.key}", registry=registry, parser=HDFParser(),
                                  loadable_variables=["x", "y", "t", "goes_imager_projection"] +
                                  (["band_id", "band_wavelength"] if "MCMIP" not in selection.product else []), decode_times=False) as vds:
            names = selected_variables(vds, selection.bands)
            chosen = vds[names]
            chosen = chosen.drop_vars([n for n in chosen.variables if n not in names and n not in {"x", "y"}])
            # VirtualiZarr's loadable path applies CF decoding, and its HDF parser
            # can omit compact scalar payloads. Inline these small raw variables
            # from h5netcdf; the large image arrays remain remote references.
            with Transport() as metadata_transport, remote_file(asset, metadata_transport) as source:
                with xr.open_dataset(source, engine="h5netcdf", decode_cf=False) as raw:
                    for name in list(chosen.variables):
                        if raw[name].ndim < 2:
                            small = raw[name].load().copy(deep=True)
                            small.encoding = {}
                            if "_FillValue" not in small.attrs:
                                small.encoding["_FillValue"] = None
                            chosen[name] = small
                    chosen.attrs = dict(raw.attrs)
            signature = {name: {"shape": chosen[name].shape, "dtype": str(chosen[name].dtype),
                                # Per-scan quality percentages are observations,
                                # not changes in the flag schema. Each reference
                                # retains them, but they do not split a group.
                                "attrs": jsonable({k: v for k, v in chosen[name].attrs.items()
                                                   if not (k.startswith("percent_") and k.endswith("_qf"))}),
                                "encoding": str(getattr(chosen[name].data, "metadata", ""))}
                         for name in names if chosen[name].ndim > 0}
            signature["projection"] = jsonable(chosen.goes_imager_projection.attrs)
            if "band_id" in chosen:
                signature["band_id_value"] = jsonable(chosen.band_id.values)
                signature["band_wavelength_value"] = jsonable(chosen.band_wavelength.values)
            signature["x"] = digest(jsonable(chosen.x.values))
            signature["y"] = digest(jsonable(chosen.y.values))
            group = digest(signature)
            references = chosen.vz.to_kerchunk(format="dict")
            # VirtualiZarr 2.7.3 writes inline scalars at "name/"; Zarr v2
            # requires "name/0". Correct the key, keeping the payload unchanged.
            for name in chosen.variables:
                if chosen[name].ndim == 0 and f"{name}/" in references["refs"]:
                    references["refs"][f"{name}/0"] = references["refs"].pop(f"{name}/")
            groups.setdefault(group, []).append(asset.id)
            records.append({"id": asset.id, "reference": references, "source_url": asset.url, "etag": asset.etag,
                            "size": asset.size, "time": asset.time, "group": group})
    index = {"selection_id": selection.id, "bbox": selection.bbox, "bands": selection.bands,
             "groups": groups, "records": records, "build_s": time.perf_counter()-start,
             "reference_bytes": sum(len(json.dumps(r["reference"])) for r in records)}
    write_json(directory / "index.json", index)
    return directory / "index.json"


def open_virtual(index_path, record=0):
    """Open one selected reference lazily; do not download full source files."""
    import fsspec
    index_path = Path(index_path)
    index = json.loads(index_path.read_text())
    entry = index["records"][record]
    from urllib.parse import urlparse
    source = urlparse(entry["source_url"])
    bucket, key = source.netloc.split(".s3.")[0], source.path.lstrip("/")
    info = s3_client().head_object(Bucket=bucket, Key=key, IfMatch=f'"{entry["etag"]}"')
    if info["ContentLength"] != entry["size"]:
        raise IOError("The referenced NOAA object changed size. Rebuild the selection and references.")
    reference = entry.get("reference")
    if reference is None:
        reference = json.loads((index_path.parent / entry["file"]).read_text())
    fs = fsspec.filesystem("reference", fo=reference, remote_protocol="s3", skip_instance_cache=True,
                          remote_options={"anon": True, "asynchronous": True, "skip_instance_cache": True}, asynchronous=True)
    ds = xr.open_dataset(fs.get_mapper(""), engine="zarr", consolidated=False,
                         decode_cf=False, mask_and_scale=False, chunks={})
    closed = False
    def close():
        nonlocal closed
        if closed:
            return
        closed = True
        ds.close()
        remote = fs.fss["s3"]
        creator = getattr(remote, "_s3creator", None)
        if creator is not None:
            from zarr.core.sync import sync
            sync(creator.__aexit__(None, None, None))
    try:
        subset = ds[selected_variables(ds, index["bands"])].isel(native_window(ds, index["bbox"]))
        subset.attrs.update(source_url=entry["source_url"], source_etag=entry["etag"],
                            observation_time=entry["time"], requested_bbox=index["bbox"])
        subset.set_close(close)
        return subset
    except Exception:
        close()
        raise


def open_virtual_group(index_path, group=None):
    """Lazily concatenate one compatible group along an explicit scan dimension."""
    index = json.loads(Path(index_path).read_text())
    if group is None:
        if len(index["groups"]) != 1:
            raise ValueError("Choose a group explicitly; these files have different encodings or grids.")
        group = next(iter(index["groups"]))
    if group not in index["groups"]:
        raise KeyError(group)
    opened = []
    try:
        for i, record in enumerate(index["records"]):
            if record["group"] == group:
                ds = open_virtual(index_path, i)
                opened.append(ds)
        combined = xr.concat(opened, dim=pd.Index([r["time"] for r in index["records"]
                            if r["group"] == group], name="scan"), join="exact", compat="equals",
                            coords="minimal", combine_attrs="drop_conflicts")
        combined.set_close(lambda: [ds.close() for ds in opened])
        return combined
    except Exception:
        for ds in opened:
            ds.close()
        raise
