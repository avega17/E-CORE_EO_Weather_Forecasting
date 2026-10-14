"""Read CARIB GRIB fields without changing values or missing-value codes."""

from __future__ import annotations

import contextlib
import gzip
import hashlib
import os
import sys
import time
import threading
import zlib
from collections import OrderedDict
from bisect import bisect_left, bisect_right
from datetime import timedelta

import numpy as np
import xarray as xr

from .common import (Asset, PR_BBOX, Selection, Transport, hours, time_slots, iso,
                     list_objects, s3_client, validate_request, utc)

DEFAULT_PRODUCT = "MultiSensor_QPE_01H_Pass2_00.00"
DEFAULT_PRODUCTS = (
    "PrecipRate_00.00", "MergedReflectivityQCComposite_00.50",
    "MergedAzShear_0-2kmAGL_00.50", DEFAULT_PRODUCT,
)
_GRID_COORDINATES = OrderedDict()
_GRID_LOCK = threading.Lock()
_ECCODES_NOTICE_LOCK = threading.Lock()
_METADATA_TIME_KEYS = ("dataDate", "dataTime", "validityDate", "validityTime")
# NOAA's GRIB2 tables: https://www.nssl.noaa.gov/projects/mrms/operational/tables.php
PRODUCTS = {
    DEFAULT_PRODUCT: {"unit": "mm", "missing": -1, "no_coverage": -3,
                      "frequency_minutes": 60, "accumulation_minutes": 60, "interval_minutes": 60},
    "MultiSensor_QPE_01H_Pass1_00.00": {"unit": "mm", "missing": -1, "no_coverage": -3,
                      "frequency_minutes": 60, "accumulation_minutes": 60, "interval_minutes": 60},
    # RadarOnly QPE is a rolling one-hour accumulation updated every two minutes.
    "RadarOnly_QPE_01H_00.00": {"unit": "mm", "missing": -1, "no_coverage": -3,
                      "frequency_minutes": 2, "accumulation_minutes": 60, "interval_minutes": 60},
    "PrecipRate_00.00": {"unit": "mm h-1", "missing": -1, "no_coverage": -3,
                      "frequency_minutes": 2, "accumulation_minutes": 0, "interval_minutes": 0},
    "MergedReflectivityQCComposite_00.50": {"unit": "dBZ", "missing": -99, "no_coverage": -999,
                      "frequency_minutes": 2, "accumulation_minutes": 0, "interval_minutes": 0},
    "MergedBaseReflectivityQC_00.50": {"unit": "dBZ", "missing": -99, "no_coverage": -999,
                      "frequency_minutes": 2, "accumulation_minutes": 0, "interval_minutes": 0},
    "RadarQualityIndex_00.00": {"unit": "1", "missing": -1, "no_coverage": -3,
                      "frequency_minutes": 2, "accumulation_minutes": 0, "interval_minutes": 0},
    # NOAA encodes no coverage and missing for these fields as zero; retain the
    # bitmap and never infer coverage from a zero value alone.
    "MergedAzShear_0-2kmAGL_00.50": {"unit": "0.001 s-1", "missing": 0,
        "no_coverage": 0, "frequency_minutes": 2, "accumulation_minutes": 0,
        "interval_minutes": 0, "zero_ambiguous": True},
    "MergedAzShear_3-6kmAGL_00.50": {"unit": "0.001 s-1", "missing": 0,
        "no_coverage": 0, "frequency_minutes": 2, "accumulation_minutes": 0,
        "interval_minutes": 0, "zero_ambiguous": True},
}

# Human-facing CLI names. Archives, selections, and metadata keep NOAA's exact
# product key so their source identity stays explicit.
PRODUCT_ALIASES = {
    "precipitation-rate": "PrecipRate_00.00",
    "composite-reflectivity": "MergedReflectivityQCComposite_00.50",
    "base-reflectivity": "MergedBaseReflectivityQC_00.50",
    "radar-only-qpe-1h": "RadarOnly_QPE_01H_00.00",
    "low-level-azimuthal-shear": "MergedAzShear_0-2kmAGL_00.50",
    "mid-level-azimuthal-shear": "MergedAzShear_3-6kmAGL_00.50",
    "radar-quality-index": "RadarQualityIndex_00.00",
    "multisensor-qpe-pass1": "MultiSensor_QPE_01H_Pass1_00.00",
    "multisensor-qpe-pass2": "MultiSensor_QPE_01H_Pass2_00.00",
}
PRODUCT_ARGUMENTS = {
    **PRODUCT_ALIASES,
    **{product: product for product in PRODUCTS},
}


def parse_product_argument(value):
    """Resolve a readable CLI name or an exact NOAA key to the NOAA key."""
    from argparse import ArgumentTypeError

    product = PRODUCT_ARGUMENTS.get(value.lower(), PRODUCT_ARGUMENTS.get(value))
    if product is None:
        options = ", ".join(PRODUCT_ALIASES)
        raise ArgumentTypeError(f"unknown MRMS product {value!r}; choose: {options}")
    return product


def match_slots(assets, expected_times, tolerance_minutes=5, method="previous", cadence_minutes=10):
    """Assign at most one source observation per requested slot, without reuse."""
    if (cadence_minutes < 1 or not 0 <= tolerance_minutes <= cadence_minutes / 2
            or method not in {"previous", "nearest", "exact"}):
        raise ValueError("Use a tolerance no greater than half the cadence and previous, nearest, or exact matching.")
    tolerance = timedelta(minutes=0 if method == "exact" else tolerance_minutes)
    assets = sorted(assets, key=lambda a: (a.time, a.key))
    times = [utc(a.time) for a in assets]
    chosen, matches, used = [], [], set()
    for stamp in expected_times:
        slot = utc(stamp)
        high = slot + tolerance if method == "nearest" else slot
        candidates = assets[bisect_left(times, slot-tolerance):bisect_right(times, high)]
        candidates = [a for a in candidates if a.id not in used]
        if not candidates:
            continue
        best = min(candidates, key=lambda a: (abs((utc(a.time)-slot).total_seconds()), utc(a.time)>slot, a.key))
        chosen.append(best)
        used.add(best.id)
        matches.append({"slot_time": stamp, "source_time": best.time, "asset_id": best.id,
                        "offset_seconds": (utc(best.time)-slot).total_seconds()})
    return chosen, tuple(matches)


def match_hours(assets, expected_times, tolerance_minutes=5, method="previous"):
    """Backward-compatible matcher for hourly products."""
    return match_slots(assets, expected_times, tolerance_minutes, method, cadence_minutes=60)


def discover(start, end, bbox=PR_BBOX, product=DEFAULT_PRODUCT, tolerance_minutes=5, time_match="previous"):
    start, end, bbox = validate_request(start, end, bbox)
    if product not in PRODUCTS:
        raise ValueError(f"Choose a supported product: {', '.join(PRODUCTS)}")
    if not (-90 <= bbox[0] < bbox[2] <= -60 and 10 <= bbox[1] < bbox[3] <= 25):
        raise ValueError("The region must fit within the CARIB grid (-90 to -60 longitude, 10 to 25 latitude).")
    client, assets = s3_client(), []
    hourly = PRODUCTS[product]["frequency_minutes"] == 60
    cadence_minutes = 60 if hourly else 10
    # Check matching parameters before making any network request.
    match_slots([], [], tolerance_minutes, time_match, cadence_minutes=cadence_minutes)
    lookup_start = start-timedelta(minutes=tolerance_minutes)
    dates = sorted({h.strftime("%Y%m%d") for h in hours(lookup_start, end)})
    for date in dates:
        prefix = f"CARIB/{product}/{date}/"
        for obj in list_objects("noaa-mrms-pds", prefix, client):
            key = obj["Key"]
            if not key.endswith(".grib2.gz"):
                continue
            from datetime import datetime, timezone
            stamp = key.rsplit("_", 1)[-1].removesuffix(".grib2.gz")
            when = datetime.strptime(stamp, "%Y%m%d-%H%M%S").replace(tzinfo=timezone.utc)
            if lookup_start <= when < end:
                assets.append(Asset("noaa-mrms-pds", key, obj["Size"], obj["ETag"].strip('"'), iso(when)))
    assets.sort(key=lambda a: (a.time, a.key))
    # Exact clock hours are expected only for the two multisensor hourly products.
    expected = tuple(iso(h) for h in (hours(start, end) if hourly else time_slots(start, end, cadence_minutes)))
    matches = ()
    assets, matches = match_slots(assets, expected, tolerance_minutes, time_match,
                                  cadence_minutes=cadence_minutes)
    return Selection("mrms", product, iso(start), iso(end), bbox, assets, expected_times=expected,
                     hourly_matches=matches, time_tolerance_minutes=tolerance_minutes,
                     time_match=time_match, cadence_minutes=cadence_minutes)


def discover_defaults(start, end, bbox=PR_BBOX, products=DEFAULT_PRODUCTS,
                      tolerance_minutes=5, time_match="previous"):
    """Discover default fast radar fields and retain slow QPE at native cadence."""
    return [discover(start, end, bbox=bbox, product=product,
                     tolerance_minutes=tolerance_minutes, time_match=time_match)
            for product in products]


@contextlib.contextmanager
def _quiet_eccodes_notices():
    """Silence ecCodes' direct stderr notices while holding a process-wide lock.

    ecCodes truncates non-zero seconds in HHMM time keys and prints a notice to
    file descriptor 2 for every off-hour file (for example, a 16:58 file filling
    the 17:00 slot). The truncated HHMM value is still returned, and hourly
    matching uses filename timestamps, so the notice is noise in notebooks.
    Decoding runs in worker threads, so the redirect is locked to stay thread-safe.
    """
    with _ECCODES_NOTICE_LOCK:
        sys.stderr.flush()
        saved = os.dup(2)
        try:
            with open(os.devnull, "w") as devnull:
                os.dup2(devnull.fileno(), 2)
                yield
        finally:
            sys.stderr.flush()
            os.dup2(saved, 2)
            os.close(saved)


def _metadata_get(ec, handle, key):
    """Read one optional GRIB key, quieting ecCodes' time-truncation notices."""
    if key in _METADATA_TIME_KEYS:
        with _quiet_eccodes_notices():
            return ec.codes_get(handle, key)
    return ec.codes_get(handle, key)


def decode_grib(data: bytes, product=DEFAULT_PRODUCT):
    """Decode a single regular latitude/longitude GRIB message with ecCodes.

    Numeric sentinels stay numeric. GRIB bitmap missingness is retained as a
    separate mask, because absent bitmap cells do not contain measurements.
    """
    import eccodes as ec
    if data[:4] != b"GRIB" or int.from_bytes(data[8:16], "big") != len(data):
        raise ValueError("Expected one complete GRIB2 message.")
    handle = ec.codes_new_from_message(data)
    try:
        nx, ny = ec.codes_get_long(handle, "Ni"), ec.codes_get_long(handle, "Nj")
        if ec.codes_get(handle, "gridType") != "regular_ll" or ec.codes_get_long(handle, "jPointsAreConsecutive"):
            raise ValueError("This reader requires the regular CARIB latitude/longitude grid.")
        if ec.codes_get_long(handle, "alternativeRowScanning"):
            raise ValueError("Alternating GRIB row scans are not supported.")
        values = ec.codes_get_values(handle).reshape(ny, nx)
        # Use the same source-coordinate accessor as cfgrib. Reconstructing these
        # from rounded header increments changes interpolation near coverage gaps.
        grid_id = ec.codes_get(handle, "md5GridSection")
        with _GRID_LOCK:
            if grid_id not in _GRID_COORDINATES:
                _GRID_COORDINATES[grid_id] = (ec.codes_get_array(handle, "distinctLongitudes"),
                                             ec.codes_get_array(handle, "distinctLatitudes"))
                if len(_GRID_COORDINATES) > 16:
                    _GRID_COORDINATES.popitem(last=False)
            lon, lat = (v.copy() for v in _GRID_COORDINATES[grid_id])
        bitmap = ec.codes_get_array(handle, "bitmap").reshape(ny, nx).astype(bool) if ec.codes_get_long(handle, "bitmapPresent") else np.ones((ny, nx), bool)
        metadata = {}
        for key in ("discipline", "parameterCategory", "parameterNumber", "shortName", "name", "units",
                    "dataDate", "dataTime", "validityDate", "validityTime", "stepType", "stepRange",
                    "packingType", "bitsPerValue", "binaryScaleFactor", "decimalScaleFactor",
                    "referenceValue", "missingValue", "bitmapPresent"):
            try:
                metadata[key] = _metadata_get(ec, handle, key)
            except ec.CodesInternalError:
                pass
        if metadata.get("discipline") != 209:
            raise ValueError("The file is not an MRMS GRIB message (discipline 209).")
        info = PRODUCTS[product]
        ds = xr.Dataset({
            "measurement": (("latitude", "longitude"), values,
                            {**metadata, "source_grib_units": metadata.get("units", "unknown"),
                             "units": info["unit"], "product": product}),
            "bitmap_valid": (("latitude", "longitude"), bitmap.astype("uint8"),
                             {"long_name": "1 where the original GRIB bitmap contains a measurement"}),
        }, coords={"latitude": lat, "longitude": lon}, attrs={"source": "NOAA MRMS CARIB", "product": product})
        ds.attrs["source_grid"] = {"shape": [ny, nx], "latitude_endpoints": [float(lat[0]), float(lat[-1])],
                                    "longitude_endpoints": [float(lon[0]), float(lon[-1])]}
        ds.latitude.attrs["units"] = "degrees_north"
        ds.longitude.attrs["units"] = "degrees_east"
        return ds
    finally:
        ec.codes_release(handle)


def crop_native(ds, bbox, halo=0):
    west, south, east, north = bbox
    normalized = (ds.longitude.values + 180) % 360 - 180
    xi = np.flatnonzero((normalized >= west) & (normalized < east))
    yi = np.flatnonzero((ds.latitude.values >= south) & (ds.latitude.values < north))
    if not len(xi) or not len(yi):
        raise ValueError("The selected region contains no source pixels.")
    row, col = max(0, int(yi.min()) - halo), max(0, int(xi.min()) - halo)
    cropped = ds.isel(latitude=slice(row, min(ds.sizes["latitude"], yi.max() + 1 + halo)),
                      longitude=slice(col, min(ds.sizes["longitude"], xi.max() + 1 + halo)))
    cropped.attrs = {**ds.attrs, "source_window_origin": [row, col]}
    return cropped


def read(asset: Asset, bbox=PR_BBOX, product=DEFAULT_PRODUCT, transport=None, halo=0):
    if f"CARIB/{product}/" not in asset.key:
        raise ValueError("The selected file does not match the requested CARIB product.")
    if transport is None:
        with Transport() as owned:
            return read(asset, bbox, product, owned, halo)
    before = time.perf_counter()
    compressed = payload = None
    failure = None
    for attempt in range(3):
        try:
            candidate = transport.read(asset)
            if len(candidate) != asset.size:
                raise IOError(f"received {len(candidate)} of {asset.size} listed bytes")
            # S3 ETags for these small, single-part NOAA objects are MD5 hashes.
            # Only enforce the comparison when the ETag has that exact form.
            if len(asset.etag) == 32 and all(c in "0123456789abcdefABCDEF" for c in asset.etag):
                actual = hashlib.md5(candidate, usedforsecurity=False).hexdigest()
                if actual.lower() != asset.etag.lower():
                    raise IOError(f"ETag mismatch (received MD5 {actual})")
            decoded = gzip.decompress(candidate)
            declared = int.from_bytes(decoded[8:16], "big") if len(decoded) >= 16 else -1
            if decoded[:4] != b"GRIB" or declared != len(decoded):
                raise ValueError(f"not one complete GRIB2 message (decompressed {len(decoded)} bytes, "
                                 f"declared {declared}, magic {decoded[:4].hex()})")
            compressed, payload = candidate, decoded
            break
        except (OSError, EOFError, ValueError, zlib.error) as exc:
            failure = exc
            if attempt < 2:
                time.sleep(0.25 * (attempt + 1))
    transfer = time.perf_counter() - before
    if compressed is None:
        raise IOError(f"Source integrity check failed after 3 reads for {asset.key} "
                      f"(listed size {asset.size}, ETag {asset.etag}): {failure}") from failure
    decode_wait_started = time.perf_counter()
    with transport.decode_slots:
        decode_wait = time.perf_counter()-decode_wait_started
        before = time.perf_counter()
        try:
            raw = decode_grib(payload, product)
        except ValueError as exc:
            raise ValueError(f"Invalid GRIB payload for {asset.key} "
                             f"(ETag {asset.etag}, compressed={len(compressed)}, "
                             f"decompressed={len(payload)} bytes): {exc}") from exc
        subset = (raw if bbox is None else crop_native(raw, bbox, halo=halo)).copy(deep=True)
        decoding = time.perf_counter() - before
    subset.attrs.update(source_url=asset.url, source_etag=asset.etag, observation_time=asset.time,
                        requested_bbox=list(bbox) if bbox else None, interval_minutes=PRODUCTS[product]["interval_minutes"])
    if PRODUCTS[product]["interval_minutes"]:
        subset.attrs.update(accumulation_end=asset.time,
                            accumulation_start=iso(utc(asset.time)-timedelta(minutes=PRODUCTS[product]["interval_minutes"])))
    return subset, {"download_s": transfer, "decode_crop_s": decoding,
                    "decode_wait_s": decode_wait, "compressed_bytes":len(compressed),
                    "inflated_grib_bytes":len(payload), "decoded_grid_bytes":raw.nbytes,
                    "roi_bytes":subset.nbytes}


def process(ds, recipe="quality_aware", bbox=PR_BBOX, shape=(768, 768)):
    """Return a processed view; neither recipe changes ds or writes files."""
    import rioxarray  # noqa: F401 - registers .rio
    from rasterio.enums import Resampling
    from rasterio.transform import from_bounds, from_origin
    if recipe not in {"legacy_exact", "quality_aware"}:
        raise ValueError("Choose legacy_exact or quality_aware.")
    arr = ds.measurement.copy(deep=True)
    if recipe == "legacy_exact":
        # The mentor's cfgrib reader exposes its data variable as float32.
        arr = arr.astype("float32")
    if recipe == "quality_aware":
        info = PRODUCTS[ds.attrs["product"]]
        valid = (ds.bitmap_valid == 1) & np.isfinite(arr) & (arr != info["missing"]) & (arr != info["no_coverage"])
        arr = arr.where(valid)
    elif "bitmap_valid" in ds:
        # cfgrib exposes bitmap-missing cells as NaN in the mentor script.
        arr = arr.where(ds.bitmap_valid == 1)
    if recipe == "legacy_exact" and "source_grid" in ds.attrs:
        # GDAL derives its affine from the full field's endpoints. Cropping first
        # changes that affine at floating-point precision and affects zero/gap
        # boundaries. Restore its grid in memory for this comparison recipe only.
        grid = ds.attrs["source_grid"]
        ny, nx = grid["shape"]
        row, col = ds.attrs.get("source_window_origin", [0, 0])
        values = np.full((ny, nx), np.nan, dtype="float32")
        values[row:row+arr.shape[0], col:col+arr.shape[1]] = arr.values
        arr = xr.DataArray(values, dims=("latitude", "longitude"), attrs=arr.attrs,
                           coords={"latitude": np.linspace(*grid["latitude_endpoints"], ny),
                                   "longitude": np.linspace(*grid["longitude_endpoints"], nx)})
    if arr.latitude[0] < arr.latitude[-1]:
        arr = arr.reindex(latitude=arr.latitude[::-1])
    arr = arr.rio.set_spatial_dims(x_dim="longitude", y_dim="latitude").rio.write_crs("EPSG:4326")
    if recipe == "quality_aware":
        arr = arr.rio.write_nodata(np.nan)
    transform = from_bounds(*bbox, shape[1], shape[0])
    if tuple(bbox) == PR_BBOX and shape == (768, 768):
        transform = from_origin(-66.4 - 3.84, 18.2 + 3.84, .01, .01)
    out = arr.rio.reproject("EPSG:4326", shape=shape,
                           transform=transform, resampling=Resampling.bilinear)
    if recipe == "legacy_exact":
        out = xr.where((out < 0) | out.isnull(), np.nan, out.clip(min=0.0))
    out = out.rio.write_nodata(np.nan, encoded=True)
    out.attrs.update(processing_recipe=recipe, units=ds.measurement.attrs["units"])
    return out


def centered_crop(arr, size=512):
    y, x = arr.dims[-2:]
    if min(arr.sizes[y], arr.sizes[x]) < size:
        raise ValueError("The requested crop is larger than the image.")
    return arr.isel({y: slice((arr.sizes[y]-size)//2, (arr.sizes[y]-size)//2+size),
                     x: slice((arr.sizes[x]-size)//2, (arr.sizes[x]-size)//2+size)})
