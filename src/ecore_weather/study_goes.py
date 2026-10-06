"""Checkpointed GOES-East metadata inventory and bounded ROI size sampling."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
import statistics
import tempfile
import time

import numpy as np

from . import goes
from .common import Asset, PR_BBOX, Transport, iso, list_objects, s3_client, utc, write_json
from .earth2_io import write_dataset

START = datetime(2021, 1, 1, tzinfo=timezone.utc)
END = datetime(2026, 7, 1, tzinfo=timezone.utc)
TRANSITION = datetime(2025, 4, 7, 15, tzinfo=timezone.utc)
SCENARIOS = (("eight_6ph", goes.STORMSCOPE_BANDS, 6),
             ("eight_3ph", goes.STORMSCOPE_BANDS, 3),
             ("eight_1ph", goes.STORMSCOPE_BANDS, 1),
             ("sixteen_6ph", tuple(range(1, 17)), 6))
_KEY = re.compile(r"-M\dC(\d{2})_.*_s(\d+)_e(\d+)_c(\d+)\.nc$")


def months(start=START, end=END):
    current = utc(start).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while current < utc(end):
        next_month = (current.replace(year=current.year+1, month=1) if current.month == 12
                      else current.replace(month=current.month+1))
        yield max(current, utc(start)), min(next_month, utc(end))
        current = next_month


def _day_assets(day, client):
    """List a day's CMIPF keys once; keep final processing of each scan/band."""
    result = {}
    for satellite in ((16, 19) if day <= TRANSITION < day+timedelta(days=1)
                      else (16,) if day < TRANSITION else (19,)):
        bucket = f"noaa-goes{satellite}"
        prefix = f"ABI-L2-CMIPF/{day:%Y/%j}/"
        for obj in list_objects(bucket, prefix, client):
            key = obj["Key"]
            match = _KEY.search(key)
            if not match:
                continue
            band = int(match[1])
            stamp = goes.scan_time(match[2])
            if (stamp < TRANSITION) != (satellite == 16):
                continue
            if not day <= stamp < day+timedelta(days=1):
                continue
            identity = (satellite, band, stamp)
            asset = Asset(bucket, key, obj["Size"], obj["ETag"].strip('"'),
                          iso(stamp), iso(goes.scan_time(match[3])))
            if identity not in result or asset.key > result[identity].key:
                result[identity] = asset
    return list(result.values())


def _band(asset):
    return int(_KEY.search(asset.key)[1])


def _sample_candidates(assets, month_start):
    if (month_start.year, month_start.month) not in {
        (year, month) for year in (2022, 2025) for month in (1, 4, 7, 10)}:
        return []
    day = month_start.replace(day=15)
    choices = []
    for hour in (0, 12):
        target = day.replace(hour=hour)
        anchors = [asset for asset in assets if _band(asset) == 13 and
                   utc(asset.time).date() == day.date()]
        if not anchors:
            continue
        anchor = min(anchors, key=lambda asset: abs((utc(asset.time)-target).total_seconds()))
        peers = [asset for asset in assets if abs((utc(asset.time)-utc(anchor.time)).total_seconds()) <= 90]
        for band in range(1, 17):
            options = [asset for asset in peers if _band(asset) == band]
            if options:
                choices.append(asdict(min(options,
                    key=lambda asset: abs((utc(asset.time)-utc(anchor.time)).total_seconds()))))
    return choices


def inventory_month(start, end, client=None):
    client = client or s3_client()
    started = time.perf_counter()
    assets = []
    day = utc(start).replace(hour=0, minute=0, second=0, microsecond=0)
    while day < utc(end):
        assets.extend(_day_assets(day, client))
        day += timedelta(days=1)
    assets = [asset for asset in assets if utc(start) <= utc(asset.time) < utc(end)]
    scenario_rows = {}
    hours = int((utc(end)-utc(start)).total_seconds() // 3600)
    for name, bands, cadence in SCENARIOS:
        selected = goes.hourly_scans([asset for asset in assets if _band(asset) in bands], cadence)
        scenario_rows[name] = {"band_files": len(selected),
            "distinct_scan_times": len({asset.time for asset in selected}),
            "listed_full_file_bytes": sum(asset.size for asset in selected),
            "nominal_band_file_opportunities": hours*cadence*len(bands),
            "nominal_missing_band_files": max(0, hours*cadence*len(bands)-len(selected)),
            "by_satellite_band": {f"{satellite}:C{band:02d}": sum(
                asset.bucket == f"noaa-goes{satellite}" and _band(asset) == band
                for asset in selected) for satellite in (16, 19) for band in bands}}
    return {"start": iso(start), "end_excluded": iso(end),
        "listed_files_all_bands": len(assets), "inventory_seconds": time.perf_counter()-started,
        "scenarios": scenario_rows, "sample_candidates": _sample_candidates(assets, utc(start))}


UNCOMPRESSED_NETCDF_METHOD = "packed-uncompressed-v1"


def write_uncompressed_netcdf(dataset, path):
    """Write packed pixels without carrying source HDF compression into the crop."""
    plain = dataset.copy(deep=False)
    for name in plain.variables:
        plain[name].encoding = {}
    plain.to_netcdf(path, engine="h5netcdf")


def sample_crop(asset, bbox=PR_BBOX, scratch=None):
    """One NOAA range read; temporary raw crop and compressed Zarr are removed."""
    band = _band(asset)
    with tempfile.TemporaryDirectory(prefix="ecore-goes-estimate-", dir=scratch) as directory:
        directory = Path(directory)
        with Transport("obstore") as transport:
            started = time.perf_counter()
            subset, _ = goes.read(asset, bbox, (band,), transport)
            read_seconds = time.perf_counter()-started
            transfer = transport.bytes
        netcdf = directory / "crop.nc"
        # The crop is an uncompressed NetCDF serialization of the same packed
        # source pixels, not an estimate from the full NOAA object size.
        write_uncompressed_netcdf(subset, netcdf)
        netcdf_bytes = netcdf.stat().st_size
        zarr_dir = directory / "crop.zarr"
        stamp = np.datetime64(utc(asset.time).replace(tzinfo=None), "ns")
        started = time.perf_counter()
        write_dataset(subset.expand_dims(time=[stamp]), zarr_dir)
        write_seconds = time.perf_counter()-started
        # Chunk payload scales per observation; dimension metadata is paid once
        # per month and recorded separately rather than multiplied per file.
        payload = sum(file.stat().st_size for file in zarr_dir.rglob("*")
                      if file.is_file() and "/c/" in file.as_posix())
        return {"satellite": int(asset.bucket.removeprefix("noaa-goes")),
            "band": band, "source_url": asset.url, "time": asset.time,
            "roi_bbox": list(bbox),
            "listed_full_file_bytes": asset.size, "roi_transfer_bytes": transfer,
            "uncompressed_crop_netcdf_bytes": netcdf_bytes,
            "netcdf_size_method": UNCOMPRESSED_NETCDF_METHOD,
            "compressed_zarr_chunk_bytes": payload,
            "sample_zarr_total_bytes": sum(file.stat().st_size for file in zarr_dir.rglob("*") if file.is_file()),
            "read_seconds": read_seconds, "write_seconds": write_seconds,
            "roi_shape": list(subset["CMI"].shape)}


def _quantile(values, fraction):
    return float(np.quantile(values, fraction)) if values else None


def summarize(months_data, samples, start=START, end=END):
    by_band = defaultdict(list)
    for sample in samples:
        by_band[(sample["satellite"], sample["band"])].append(sample)
    results = {}
    for name, bands, cadence in SCENARIOS:
        totals = {key: sum(month["scenarios"][name][key] for month in months_data)
            for key in ("band_files", "listed_full_file_bytes", "nominal_band_file_opportunities",
                        "nominal_missing_band_files")}
        totals["distinct_scan_times"] = sum(month["scenarios"][name]["distinct_scan_times"]
            for month in months_data)
        for metric, output_name in (("roi_transfer_bytes", "estimated_roi_transfer_bytes"),
            ("uncompressed_crop_netcdf_bytes", "estimated_uncompressed_crop_netcdf_bytes"),
            ("compressed_zarr_chunk_bytes", "estimated_compressed_zarr_bytes"),
            ("read_seconds", "estimated_read_seconds"),
            ("write_seconds", "estimated_write_seconds")):
            values = {}
            for label, q in (("low", .1), ("central", .5), ("high", .9)):
                total = 0.0
                complete = True
                for month in months_data:
                    for satellite in (16, 19):
                        for band in bands:
                            count = month["scenarios"][name]["by_satellite_band"].get(
                                f"{satellite}:C{band:02d}", 0)
                            if not count:
                                continue
                            group = by_band.get((satellite, band), [])
                            if metric == "uncompressed_crop_netcdf_bytes":
                                # Older caches retained source compression and
                                # cannot support an uncompressed-size claim.
                                group = [row for row in group if row.get(
                                    "netcdf_size_method") == UNCOMPRESSED_NETCDF_METHOD]
                            if not group:
                                complete = False
                                continue
                            total += count*_quantile([row[metric] for row in group], q)
                values[label] = total if complete else None
            totals[output_name] = values
        if totals["estimated_compressed_zarr_bytes"]["central"] is not None:
            # Sidecar provenance is a small per-observation cost; report it as
            # an explicit additive allowance rather than calling chunks alone
            # the full store size.
            for label in ("low", "central", "high"):
                totals["estimated_compressed_zarr_bytes"][label] += totals["band_files"]*2048
        if totals["estimated_read_seconds"]["high"] is not None:
            totals["estimated_total_local_seconds"] = {
                label: totals["estimated_read_seconds"][label] +
                       totals["estimated_write_seconds"][label] for label in ("low", "central", "high")}
            totals["estimated_total_local_seconds"]["high"] *= 1.3
            # The sampled operation durations add to serial task work. Give
            # workstation scheduling examples separately; effective worker
            # counts are assumptions, not measured network scaling.
            totals["estimated_local_wall_seconds_assumed"] = {
                "low": totals["estimated_total_local_seconds"]["low"] / 8,
                "central": totals["estimated_total_local_seconds"]["central"] / 4,
                "high": totals["estimated_total_local_seconds"]["high"],
            }
        results[name] = totals
    return {"study_start": iso(start), "study_end_excluded": iso(end),
        "months_complete": len(months_data), "sample_files": len(samples),
        "inventory_seconds": sum(month["inventory_seconds"] for month in months_data),
        "scenarios": results,
        "assumptions": ["GOES-East split at 2025-04-07 15:00 UTC",
            "Nominal opportunities assume at most six full-disk scans per hour",
            "Crop NetCDF is uncompressed serialization of native packed pixels",
            "Unversioned crop samples are excluded from uncompressed NetCDF estimates; use a fresh output directory to resample",
            "Zarr estimate sums sampled compressed chunks plus 2 KiB provenance allowance per band file",
            "High local time uses sampled 90th percentile plus 30 percent margin; inventory time is separate",
            "Wall-time examples assume eight, four, or one effective concurrent task for low, central, or high; these are not measured parallel speedups"]}


def run(output, bbox=PR_BBOX, sample=True, scratch=None, start=START, end=END):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    month_dir = output / "inventory"
    month_dir.mkdir(exist_ok=True)
    sample_path = output / "samples.json"
    prior = {row["source_url"]: row for row in json.loads(sample_path.read_text())} if sample_path.exists() else {}
    if any(row.get("roi_bbox") != list(bbox) for row in prior.values()):
        raise ValueError("Existing crop samples use a different ROI; select a new output directory")
    if sample and not prior:
        # Fail before a multi-year metadata inventory if native ROI reads or
        # Earth2Studio sample storage cannot work in this launch environment.
        client = s3_client()
        day = utc(start).replace(hour=0, minute=0, second=0, microsecond=0)
        for _ in range(3):
            candidates = [a for a in _day_assets(day, client) if _band(a) == 13]
            if candidates:
                asset = candidates[0]
                prior[asset.url] = sample_crop(asset, bbox, scratch)
                write_json(sample_path, list(prior.values()))
                break
            day += timedelta(days=1)
        else:
            raise FileNotFoundError("No representative GOES C13 source in the first three study days")
    monthly = []
    for begin, finish in months(start, end):
        path = month_dir / f"{begin:%Y-%m}.json"
        if path.is_file():
            row = json.loads(path.read_text())
            if row["start"] != iso(begin) or row["end_excluded"] != iso(finish):
                raise ValueError(f"Checkpoint does not match request: {path}")
        else:
            row = inventory_month(begin, finish)
            write_json(path, row)
        monthly.append(row)
        print(f"Inventoried {begin:%Y-%m}: {row['listed_files_all_bands']} band files", flush=True)
    sampled = []
    if sample:
        candidates = {Asset(**asset).url: Asset(**asset) for month in monthly
            for asset in month["sample_candidates"]}
        for url, asset in candidates.items():
            if url not in prior:
                prior[url] = sample_crop(asset, bbox, scratch)
                write_json(sample_path, list(prior.values()))
                print(f"Sampled {asset.time} C{_band(asset):02d}", flush=True)
        sampled = list(prior.values())
    report = summarize(monthly, sampled, start, end)
    sample_complete = bool(sampled) and all(
        row["estimated_uncompressed_crop_netcdf_bytes"]["central"] is not None and
        row["estimated_compressed_zarr_bytes"]["central"] is not None
        for row in report["scenarios"].values())
    report.update({"roi_bbox": list(bbox), "sample_complete": sample_complete,
                   "inventory_only": not sample})
    write_json(output / "summary.json", report)
    return report
