"""Stream MRMS sources into a small, reproducible Caribbean radar-quality figure.

The saved monthly reflectivity ZIPs identify the hourly source objects. This
module reads those full CARIB objects and the matching Radar Quality Index (RQI)
objects, but persists only derived counts and a static figure.
"""

from __future__ import annotations

from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import time
from urllib.parse import urlsplit

import numpy as np

from . import mrms
from .common import Asset, PR_BBOX, Transport, iso, utc

REFLECTIVITY = "MergedReflectivityQCComposite_00.50"
RQI = "RadarQualityIndex_00.00"
CARIB_BBOX = (-90.0, 10.0, -60.0, 25.0)
SCHEMA = 2
SAMPLE_MINUTE = 10  # the :10 slot stays within its source-month archive


def _atomic_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(payload)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _month_dates(year: int):
    for month in range(1, 13):
        start = datetime(year, month, 1, tzinfo=timezone.utc)
        end = datetime(year + (month == 12), month % 12 + 1, 1, tzinfo=timezone.utc)
        yield f"{year:04d}-{month:02d}", start, end


def _interval(year=2022, start=None, end=None):
    if bool(start) != bool(end):
        raise ValueError("Provide both start and exclusive end")
    begin = utc(start) if start else datetime(year, 1, 1, tzinfo=timezone.utc)
    stop = utc(end) if end else datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    if begin >= stop or begin.minute or begin.second or stop.minute or stop.second:
        raise ValueError("Figure dates must be increasing UTC hour boundaries")
    label = (str(begin.year) if begin == datetime(begin.year, 1, 1, tzinfo=timezone.utc)
             and stop == datetime(begin.year + 1, 1, 1, tzinfo=timezone.utc)
             else f"{begin.year}-h1" if begin == datetime(begin.year, 1, 1, tzinfo=timezone.utc)
             and stop == datetime(begin.year, 7, 1, tzinfo=timezone.utc)
             else f"{begin:%Y%m%dT%H}-{stop:%Y%m%dT%H}")
    return begin, stop, label


def _period_months(start, end):
    from .study_goes import months
    for begin, stop in months(start, end):
        yield f"{begin:%Y-%m}", begin, stop


def _marker_roots(location: str | Path, year: int, start=None, end=None):
    base = Path(location).expanduser().resolve()
    if base.name.startswith("roi-"):
        roots = [base]
    elif base.name == REFLECTIVITY:
        roots = sorted(base.glob("roi-*"))
    else:
        roots = sorted((base / "mrms" / REFLECTIVITY).glob("roi-*"))
    begin, stop, _ = _interval(year, start, end)
    periods = list(_period_months(begin, stop))
    qualified = []
    for root in roots:
        markers = [root / key[:4] / key[5:] / "complete.json" for key, _, _ in periods]
        if not all(path.is_file() for path in markers):
            continue
        rows = [json.loads(path.read_text()) for path in markers]
        if all(row.get("source") == "mrms" and row.get("product") == REFLECTIVITY
               and tuple(row.get("region", ())) == tuple(PR_BBOX)
               and row.get("archive_sha256")
               and (path.parent / row.get("raw_path", "raw.zarr.zip")).is_file()
               and (path.parent / row.get("raw_path", "raw.zarr.zip")).stat().st_size == row.get("stored_bytes")
               for path, row in zip(markers, rows)):
            qualified.append((root, markers, rows))
    if len(qualified) != 1:
        raise ValueError(f"Expected one complete {year} {REFLECTIVITY} ROI; found {len(qualified)} in {base}")
    return qualified[0]


def _archived_asset(record: dict) -> Asset:
    url = urlsplit(record["source_url"])
    if url.scheme != "https" or url.netloc != "noaa-mrms-pds.s3.amazonaws.com":
        raise ValueError("Reflectivity provenance does not identify the anonymous NOAA bucket")
    key = url.path.lstrip("/")
    if not key.startswith(f"CARIB/{REFLECTIVITY}/"):
        raise ValueError(f"Unexpected reflectivity source key: {key}")
    return Asset("noaa-mrms-pds", key, int(record["source_bytes"]),
                 record["etag"], record["time"])


def _month_reflectivity(records: list[dict], start: datetime, end: datetime):
    selected = {}
    for record in records:
        slot = utc(record["slot_time"])
        if not start <= slot < end or slot.minute != SAMPLE_MINUTE or slot.second:
            continue
        stamp = iso(slot)
        if stamp in selected:
            raise ValueError(f"Duplicate archived reflectivity hour: {stamp}")
        asset = _archived_asset(record)
        if not timedelta(0) <= slot - utc(asset.time) <= timedelta(minutes=5):
            raise ValueError(f"Reflectivity source falls outside its requested hour: {stamp}")
        selected[stamp] = asset
    return selected


def _month_rqi(start: datetime, end: datetime):
    selection = mrms.discover(start, end, bbox=CARIB_BBOX, product=RQI,
                              tolerance_minutes=5, time_match="previous")
    assets = {asset.id: asset for asset in selection.assets}
    matched = {}
    for record in selection.hourly_matches:
        slot = utc(record["slot_time"])
        if slot.minute == SAMPLE_MINUTE and slot.second == 0:
            matched[iso(slot)] = assets[record["asset_id"]]
    return matched


def build_selection(location: str | Path, year: int, output: str | Path, start=None, end=None):
    """Keep one compressed source selection; reuse it on interrupted runs."""
    output = Path(output)
    begin, stop, label = _interval(year, start, end)
    periods = list(_period_months(begin, stop))
    source_root, markers, marker_rows = _marker_roots(location, year, begin, stop)
    marker_hashes = {key: hashlib.sha256(path.read_bytes()).hexdigest()
                     for (key, _, _), path in zip(periods, markers)}
    path = output / "source-selection.json.gz"
    if path.is_file():
        with gzip.open(path, "rt") as stream:
            selection = json.load(stream)
        if (selection.get("schema") != SCHEMA or selection.get("year") != year
                or selection.get("marker_hashes") != marker_hashes
                or selection.get("reflectivity_root") != str(source_root)
                or selection.get("start", iso(datetime(year, 1, 1, tzinfo=timezone.utc))) != iso(begin)
                or selection.get("end_excluded", iso(datetime(year + 1, 1, 1, tzinfo=timezone.utc))) != iso(stop)):
            raise ValueError("Saved figure selection differs from the verified reflectivity archives")
    else:
        months = {}
        for (key, start, end), marker in zip(periods, marker_rows):
            reflectivity = _month_reflectivity(marker["assets"], start, end)
            quality = _month_rqi(start, end)
            slots = []
            for hour in mrms.hours(start, end):
                stamp = iso(hour + timedelta(minutes=SAMPLE_MINUTE))
                slots.append({"slot": stamp,
                              "reflectivity": asdict(reflectivity[stamp]) if stamp in reflectivity else None,
                              "rqi": asdict(quality[stamp]) if stamp in quality else None})
            if not any(row["rqi"] for row in slots):
                raise ValueError(f"No RQI observations matched any hour in {key}; cannot make an annual RQI map")
            months[key] = slots
            print(f"Selected {key}: {sum(bool(r['reflectivity']) for r in slots)} reflectivity, "
                  f"{sum(bool(r['rqi']) for r in slots)} RQI / {len(slots)} hours", flush=True)
        selection = {"schema": SCHEMA, "year": year, "sample_minutes": 60,
                     "start": iso(begin), "end_excluded": iso(stop), "period": label,
                     "sample_minute_in_hour": SAMPLE_MINUTE,
                     "reflectivity_product": REFLECTIVITY, "rqi_product": RQI,
                     "reflectivity_root": str(source_root), "marker_hashes": marker_hashes,
                     "months": months}
        payload = json.dumps(selection, sort_keys=True, separators=(",", ":")).encode()
        _atomic_bytes(path, gzip.compress(payload, compresslevel=6, mtime=0))
    digest = hashlib.sha256(json.dumps(selection, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return selection, digest


def _read_task(kind: str, asset_record: dict, transport: Transport):
    asset = Asset(**asset_record)
    product = REFLECTIVITY if kind == "reflectivity" else RQI
    ds, _ = mrms.read(asset, bbox=None, product=product, transport=transport)
    values = np.asarray(ds.measurement.values)
    bitmap = np.asarray(ds.bitmap_valid.values) == 1
    info = mrms.PRODUCTS[product]
    finite = np.isfinite(values)
    missing = bitmap & finite & (values == info["missing"])
    no_coverage = bitmap & finite & (values == info["no_coverage"])
    valid = bitmap & finite & ~missing & ~no_coverage
    out_of_range = np.zeros(values.shape, dtype=bool)
    if kind == "rqi":
        out_of_range = valid & ((values < 0) | (values > 1))
        # Historical files can contain additional undocumented negative codes
        # (for example -285.3 in January 2022). Keep their count separate;
        # do not silently treat them as valid quality scores or relabel them.
        valid &= ~out_of_range
        contribution = np.where(valid, values, 0).astype("float64", copy=False)
    else:
        contribution = None
    pixel_counts = {"total": int(values.size),
                    "bitmap_gaps": int(np.count_nonzero(~bitmap)),
                    "nonfinite": int(np.count_nonzero(bitmap & ~finite)),
                    "missing_sentinel": int(np.count_nonzero(missing)),
                    "no_coverage_sentinel": int(np.count_nonzero(no_coverage)),
                    "out_of_range": int(np.count_nonzero(out_of_range)),
                    "valid": int(np.count_nonzero(valid)),
                    "valid_zero": int(np.count_nonzero(valid & (values == 0))),
                    "valid_negative": int(np.count_nonzero(valid & (values < 0)))}
    return kind, valid, contribution, np.asarray(ds.latitude.values), np.asarray(ds.longitude.values), pixel_counts


def _empty_state(selection_hash: str, year: int):
    return {"selection_sha256": selection_hash, "year": year, "months_completed": [],
            "rqi_reads": 0, "reflectivity_reads": 0,
            "pixel_counts": {"rqi": {}, "reflectivity": {}},
            "returned_bytes": 0, "requests": 0, "retries": 0, "elapsed_seconds": 0.0}


def _save_summary(path: Path, counts: dict, metadata: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.stem}.{os.getpid()}.npz")
    try:
        np.savez_compressed(temporary, latitude=counts["latitude"], longitude=counts["longitude"],
                            rqi_sum=counts["rqi_sum"], rqi_count=counts["rqi_count"],
                            reflectivity_count=counts["reflectivity_count"],
                            metadata=json.dumps(metadata, sort_keys=True))
        with np.load(temporary, allow_pickle=False) as opened:
            if (any(not np.array_equal(opened[name], values)
                    for name, values in counts.items())
                    or json.loads(str(opened["metadata"].item())) != metadata):
                raise IOError("Annual summary failed read-back verification")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def load_summary(path: str | Path):
    """Load a derived quality summary, never a raw observation archive."""
    with np.load(path, allow_pickle=False) as stored:
        counts = {name: stored[name].copy() for name in
                  ("latitude", "longitude", "rqi_sum", "rqi_count", "reflectivity_count")}
        metadata = json.loads(str(stored["metadata"].item()))
    return counts, metadata


def _accumulate_month(slots: list[dict], counts: dict | None, transport: Transport,
                      workers: int, month: str):
    tasks = [(kind, row[kind]) for row in slots for kind in ("reflectivity", "rqi") if row[kind]]
    iterator = iter(tasks)
    read_counts = {"reflectivity": 0, "rqi": 0,
                   "pixel_counts": {"rqi": {}, "reflectivity": {}}}

    def admit(pool, pending):
        try:
            kind, asset = next(iterator)
        except StopIteration:
            return False
        pending.append(pool.submit(_read_task, kind, asset, transport))
        return True

    with ThreadPoolExecutor(max_workers=workers) as pool:
        pending = deque()
        for _ in range(workers):
            admit(pool, pending)
        while pending:
            kind, valid, contribution, latitude, longitude, pixel_counts = pending.popleft().result()
            if counts is None:
                counts = {"latitude": latitude, "longitude": longitude,
                          "rqi_sum": np.zeros(valid.shape, dtype="float64"),
                          "rqi_count": np.zeros(valid.shape, dtype="uint32"),
                          "reflectivity_count": np.zeros(valid.shape, dtype="uint32")}
            elif (not np.array_equal(counts["latitude"], latitude)
                  or not np.array_equal(counts["longitude"], longitude)
                  or valid.shape != counts["rqi_sum"].shape):
                raise ValueError(f"MRMS source grid changed during {month}; no implicit alignment is allowed")
            if kind == "rqi":
                counts["rqi_sum"] += contribution
                counts["rqi_count"] += valid
            else:
                counts["reflectivity_count"] += valid
            read_counts[kind] += 1
            for key, value in pixel_counts.items():
                current = read_counts["pixel_counts"][kind]
                current[key] = current.get(key, 0) + value
            done = read_counts["reflectivity"] + read_counts["rqi"]
            if done % 120 == 0:
                print(f"{month}: {done}/{len(tasks)} source images processed", flush=True)
            admit(pool, pending)
    return counts, read_counts


def pilot_day(location: str | Path, day: str, output: str | Path = "results/dataset-report",
              workers: int = 4):
    """Read one UTC day and compare one full-grid image with its saved ROI pixels."""
    from .storage import open_raw

    start = utc(day)
    if start.time() != datetime.min.time():
        raise ValueError("Choose a UTC calendar date, YYYY-MM-DD")
    end = start + timedelta(days=1)
    root, _, markers = _marker_roots(location, start.year)
    marker = markers[start.month - 1]
    archived = _month_reflectivity(marker["assets"], start, end)
    quality = _month_rqi(start, end)
    slots = []
    for hour in mrms.hours(start, end):
        stamp = iso(hour + timedelta(minutes=SAMPLE_MINUTE))
        slots.append({"slot": stamp,
                      "reflectivity": asdict(archived[stamp]) if stamp in archived else None,
                      "rqi": asdict(quality[stamp]) if stamp in quality else None})
    if not archived or not quality:
        raise ValueError("Pilot day has no matching reflectivity or RQI sources")
    zip_path = root / str(start.year) / f"{start.month:02d}" / marker.get("raw_path", "raw.zarr.zip")
    first_stamp = next(row["slot"] for row in slots if row["reflectivity"])
    asset = archived[first_stamp]
    index = next(i for i, row in enumerate(marker["assets"])
                 if row["slot_time"] == first_stamp and row["source_url"] == asset.url)
    with Transport(backend="obstore", decode_workers=1) as transport:
        source, _ = mrms.read(asset, bbox=None, product=REFLECTIVITY, transport=transport)
        cropped = mrms.crop_native(source, PR_BBOX)
        with open_raw(zip_path) as saved:
            for name in ("measurement", "bitmap_valid"):
                if not np.array_equal(cropped[name].values, saved[name].isel(time=index).values,
                                      equal_nan=name == "measurement"):
                    raise AssertionError(f"Saved ROI {name} differs from NOAA full-grid source")
            for name in ("latitude", "longitude"):
                if not np.array_equal(cropped[name].values, saved[name].values):
                    raise AssertionError(f"Saved ROI {name} differs from NOAA full-grid source")
        counts, reads = _accumulate_month(slots, None, transport, workers, iso(start)[:10])
        report = {"day": iso(start)[:10], "source_to_archive_equal": True,
                  "compared_source": asset.url, "compared_archive": str(zip_path),
                  "expected_hours": len(slots), "reflectivity_reads": reads["reflectivity"],
                  "rqi_reads": reads["rqi"],
                  "missing_reflectivity_hours": [row["slot"] for row in slots if row["reflectivity"] is None],
                  "missing_rqi_hours": [row["slot"] for row in slots if row["rqi"] is None],
                  "pixel_counts": reads["pixel_counts"],
                  "returned_bytes": transport.bytes,
                  "grid_shape": list(counts["rqi_sum"].shape),
                  "quality_valid_pixels": int(np.count_nonzero(counts["rqi_count"])),
                  "reflectivity_valid_pixels": int(np.count_nonzero(counts["reflectivity_count"]))}
    target = Path(output) / f"mrms-radar-{start.year}" / f"pilot-{start:%Y-%m-%d}.json"
    _atomic_bytes(target, (json.dumps(report, indent=2) + "\n").encode())
    return {**report, "report": str(target)}


def summarize_year(location: str | Path, year: int = 2022,
                   output: str | Path = "results/dataset-report", workers: int = 4, start=None, end=None):
    """Resume an hourly full-CARIB RQI/reflectivity sample at month boundaries."""
    if workers < 1 or workers > 8:
        raise ValueError("Choose one to eight bounded source readers")
    begin, stop, label = _interval(year, start, end)
    folder = Path(output) / f"mrms-radar-{label}"
    selection, selection_hash = (build_selection(location, year, folder) if start is None else
                                 build_selection(location, year, folder, begin, stop))
    summary = folder / "annual-summary.npz"
    if summary.exists():
        counts, metadata = load_summary(summary)
        if (metadata.get("selection_sha256") != selection_hash or metadata.get("year") != year
                or "pixel_counts" not in metadata):
            raise ValueError("Existing annual summary belongs to another source selection")
    else:
        counts, metadata = None, _empty_state(selection_hash, year)
        metadata.update(start=iso(begin), end_excluded=iso(stop), period=label)
    with Transport(backend="obstore", decode_workers=1) as transport:
        for month, slots in selection["months"].items():
            if month in metadata["months_completed"]:
                continue
            started = time.perf_counter()
            before_bytes, before_requests, before_retries = transport.bytes, transport.requests, transport.retries
            # The annual state changes only after a complete month. On failure,
            # the partially updated arrays are discarded with this process.
            counts, read = _accumulate_month(slots, counts, transport, workers, month)
            metadata["months_completed"].append(month)
            metadata["rqi_reads"] += read["rqi"]
            metadata["reflectivity_reads"] += read["reflectivity"]
            for kind in ("rqi", "reflectivity"):
                for key, value in read["pixel_counts"][kind].items():
                    current = metadata["pixel_counts"][kind]
                    current[key] = current.get(key, 0) + value
            metadata["returned_bytes"] += transport.bytes - before_bytes
            metadata["requests"] += transport.requests - before_requests
            metadata["retries"] += transport.retries - before_retries
            metadata["elapsed_seconds"] += time.perf_counter() - started
            _save_summary(summary, counts, metadata)
            print(f"Verified derived {month} checkpoint: {read['reflectivity']} reflectivity, "
                  f"{read['rqi']} RQI images", flush=True)
    if set(metadata["months_completed"]) != set(selection["months"]):
        raise RuntimeError("Only a partial quality summary is available for the requested period")
    return summary, selection, metadata


def square_outline(center: tuple[float, float], side_km: float, points_per_side: int = 32):
    """An illustrative physical-distance square, returned in display lon/lat."""
    from pyproj import CRS, Transformer
    if side_km <= 0 or points_per_side < 2:
        raise ValueError("Square side and point count must be positive")
    lon, lat = center
    local = CRS.from_proj4(f"+proj=aeqd +lon_0={lon} +lat_0={lat} +datum=WGS84 +units=m +no_defs")
    inverse = Transformer.from_crs(local, "EPSG:4326", always_xy=True)
    half = side_km * 500
    positions = np.linspace(-half, half, points_per_side)
    x = np.r_[positions, np.full(points_per_side, half), positions[::-1], np.full(points_per_side, -half)]
    y = np.r_[np.full(points_per_side, -half), positions, np.full(points_per_side, half), positions[::-1]]
    return inverse.transform(x, y)


def plot_quality(summary: str | Path, output: str | Path,
                 center: tuple[float, float] | None = None,
                 training_side_km: float = 512, future_side_km: float = 1024,
                 view: str = "both"):
    """Export independent overview and regional maps from cached derived arrays."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.patheffects as effects
    from matplotlib.lines import Line2D
    from matplotlib.patches import Rectangle
    from .maps import add_context

    if view not in {"macro", "regional", "both"}:
        raise ValueError("Choose macro, regional, or both")
    counts, metadata = load_summary(summary)
    begin, stop, label = _interval(metadata["year"], metadata.get("start"), metadata.get("end_excluded"))
    if set(metadata["months_completed"]) != {key for key, _, _ in _period_months(begin, stop)}:
        raise ValueError("Figure requires every requested derived monthly checkpoint")
    center = center or ((PR_BBOX[0] + PR_BBOX[2]) / 2, (PR_BBOX[1] + PR_BBOX[3]) / 2)
    lat = counts["latitude"]
    lon = (counts["longitude"] + 180) % 360 - 180
    if not (np.all(np.diff(lon) > 0) and (np.all(np.diff(lat) > 0) or np.all(np.diff(lat) < 0))):
        raise ValueError("Expected one monotonic CARIB latitude/longitude grid")
    rqi = np.divide(counts["rqi_sum"], counts["rqi_count"],
                    out=np.full(counts["rqi_sum"].shape, np.nan), where=counts["rqi_count"] > 0)
    if metadata["reflectivity_reads"] == 0 or not np.any(counts["rqi_count"]):
        raise ValueError("No valid reflectivity/RQI samples are available to plot")
    availability = counts["reflectivity_count"] / metadata["reflectivity_reads"]
    dx, dy = abs(lon[1] - lon[0]), abs(lat[1] - lat[0])
    extent = (float(lon[0] - dx/2), float(lon[-1] + dx/2),
              float(min(lat) - dy/2), float(max(lat) + dy/2))
    origin = "lower" if lat[0] < lat[-1] else "upper"
    training = square_outline(center, training_side_km)
    future = square_outline(center, future_side_km)
    detail = (min(-72, float(np.min(future[0])) - .5), min(13, float(np.min(future[1])) - .5),
              max(-60.7, float(np.max(future[0])) + .5), max(23.5, float(np.max(future[1])) + .5))
    colors = {"acquisition": "#c01b46", "training": "#e69f00", "future": "#9c179e", "reflectivity": "#202020"}
    target = Path(output)
    target.mkdir(parents=True, exist_ok=True)
    paths = []
    for mode in (["macro", "regional"] if view == "both" else [view]):
        macro = mode == "macro"
        bounds = (-91, 9, -59, 26) if macro else detail
        fig, ax = plt.subplots(figsize=(11, 7.4) if macro else (9, 9))
        fig.subplots_adjust(left=.09, right=.86, top=.88, bottom=.25)
        add_context(ax, bounds, labels=False)
        image = ax.imshow(rqi, extent=extent, origin=origin, cmap="YlGnBu", vmin=0, vmax=1,
                          interpolation="nearest", alpha=.95, zorder=1, rasterized=True)
        ax.add_patch(Rectangle((PR_BBOX[0], PR_BBOX[1]), PR_BBOX[2]-PR_BBOX[0], PR_BBOX[3]-PR_BBOX[1],
                               facecolor="none", edgecolor=colors["acquisition"], linewidth=2.1, zorder=7))
        ax.plot(*future, color=colors["future"], ls="-.", lw=1.8, alpha=.8, zorder=8)
        legend = [Line2D([0],[0],color=colors["acquisition"],lw=2.1,label="Current acquisition region"),
                  Line2D([0],[0],color=colors["future"],ls="-.",alpha=.8,
                         label=f"Approximate future region: {future_side_km:g} × {future_side_km:g} km")]
        if macro:
            ax.add_patch(Rectangle((CARIB_BBOX[0], CARIB_BBOX[1]), 30, 15,
                                   fill=False, edgecolor="#444444", ls=":", lw=1.3, zorder=6))
            legend.append(Line2D([0],[0],color="#444444",ls=":",label="MRMS CARIB source grid"))
        else:
            ax.plot(*training, color=colors["training"], ls="--", lw=1.9, alpha=.9, zorder=8)
            legend.insert(1, Line2D([0],[0],color=colors["training"],ls="--",alpha=.9,
                                   label=f"Illustrative training patch: {training_side_km:g} × {training_side_km:g} km"))
            rows = np.flatnonzero((lat >= bounds[1] - .5) & (lat <= bounds[3] + .5))
            columns = np.flatnonzero((lon >= bounds[0] - .5) & (lon <= bounds[2] + .5))
            field = availability[np.ix_(rows, columns)]
            if field.size and np.nanmin(field) <= .01 <= np.nanmax(field):
                contour = ax.contour(lon[columns], lat[rows], field, levels=[.01],
                                     colors=[colors["reflectivity"]], linewidths=1.25, zorder=9)
                contour.set_path_effects([effects.Stroke(linewidth=2.6, foreground="white"), effects.Normal()])
                legend.append(Line2D([0],[0],color=colors["reflectivity"],label="Valid reflectivity in ≥1% of sampled hours"))
        ax.set_xlim(bounds[0], bounds[2]); ax.set_ylim(bounds[1], bounds[3])
        fig.colorbar(image, ax=ax, fraction=.045, pad=.025, label="Mean Radar Quality Index (0–1)")
        title = "Caribbean radar-quality overview" if macro else "Puerto Rico radar quality and study regions"
        fig.suptitle(f"{title} — {label.upper()}", fontsize=14, y=.97)
        ax.set_title(f"{metadata['rqi_reads']:,} RQI and {metadata['reflectivity_reads']:,} reflectivity hourly samples", fontsize=10)
        fig.legend(handles=legend, loc="upper center", bbox_to_anchor=(.5, .205), ncol=1 if macro else 2,
                   fontsize=9, frameon=False, handlelength=3.2, columnspacing=1.4)
        caption = ("RQI describes radar precipitation-quality uncertainty from beam blockage and beam height relative to the melting layer.\n"
                   "Higher scores indicate more favorable sampling; the mean includes valid zero scores and excludes missing pixels.")
        if not macro:
            caption += ("\nThe black boundary marks valid reflectivity in at least 1% of successfully read hourly samples;\n"
                        "its position depends on weather and data availability, rather than a fixed radar-range limit.")
        fig.text(.5, .025, caption, ha="center", va="bottom", fontsize=8.5, linespacing=1.5)
        with plt.rc_context({"svg.fonttype": "none"}):
            for suffix in ("png", "svg"):
                path = target / f"mrms-rqi-{mode}-{label}.{suffix}"
                fig.savefig(path, dpi=180, bbox_inches="tight", pad_inches=.25)
                paths.append(path)
        plt.close(fig)
    return paths


def run(location: str | Path, year: int = 2022, output: str | Path = "results/dataset-report",
        workers: int = 4, center: tuple[float, float] | None = None,
        training_side_km: float = 512, future_side_km: float = 1024,
        start=None, end=None, figures_dir="figures", view="both", render_only=False):
    begin, stop, label = _interval(year, start, end)
    target = Path(output) / f"mrms-radar-{label}"
    if render_only:
        summary = target / "annual-summary.npz"
        counts, metadata = load_summary(summary)
        with gzip.open(target / "source-selection.json.gz", "rt") as stream:
            selection = json.load(stream)
        digest = hashlib.sha256(json.dumps(selection, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        if metadata.get("selection_sha256") != digest:
            raise ValueError("Cached summary and source selection differ")
        cached_begin, cached_stop, _ = _interval(metadata["year"], metadata.get("start"), metadata.get("end_excluded"))
        if (begin, stop) != (cached_begin, cached_stop):
            raise ValueError("Cached summary covers a different interval")
    else:
        summary, selection, metadata = summarize_year(location, year, output, workers, start, end)
    figures = plot_quality(summary, Path(figures_dir) / f"mrms-{label}", center,
                           training_side_km, future_side_km, view=view)
    counts, _ = load_summary(summary)
    reflectivity_frequency = counts["reflectivity_count"] / metadata["reflectivity_reads"]
    missing_reflectivity = [row["slot"] for slots in selection["months"].values()
                            for row in slots if row["reflectivity"] is None]
    missing_rqi = [row["slot"] for slots in selection["months"].values()
                   for row in slots if row["rqi"] is None]
    offsets = {}
    listed_bytes = 0
    for kind in ("reflectivity", "rqi"):
        seconds = []
        for slots in selection["months"].values():
            for row in slots:
                if row[kind] is None:
                    continue
                offset = (utc(row["slot"]) - utc(row[kind]["time"])).total_seconds()
                if not 0 <= offset <= 300:
                    raise ValueError(f"{kind} source falls outside the five-minute lookback")
                seconds.append(offset)
                listed_bytes += int(row[kind]["size"])
        offsets[kind] = {"min": min(seconds), "median": float(np.median(seconds)),
                         "max": max(seconds)} if seconds else None
    report = {**metadata, "expected_hours": sum(map(len, selection["months"].values())),
              "carib_grid_shape": list(counts["rqi_count"].shape),
              "carib_grid_bounds_lonlat": list(CARIB_BBOX),
              "maximum_valid_reflectivity_frequency": float(np.max(reflectivity_frequency)),
              "cells_valid_reflectivity_at_least_1_percent": int(np.count_nonzero(reflectivity_frequency >= .01)),
              "cells_valid_reflectivity_at_least_50_percent": int(np.count_nonzero(reflectivity_frequency >= .5)),
              "listed_source_bytes": listed_bytes,
              "source_offset_seconds_before_slot": offsets,
              "missing_reflectivity_hours": missing_reflectivity,
              "missing_reflectivity_hour_count": len(missing_reflectivity),
              "missing_rqi_hours": missing_rqi,
              "missing_rqi_hour_count": len(missing_rqi),
              "center_longitude_latitude": center or ((PR_BBOX[0]+PR_BBOX[2])/2,(PR_BBOX[1]+PR_BBOX[3])/2),
              "training_side_km": training_side_km, "future_side_km": future_side_km,
              "summary": str(summary), "figures": [str(path) for path in figures]}
    _atomic_bytes(target / "report.json", (json.dumps(report, indent=2) + "\n").encode())
    return report
