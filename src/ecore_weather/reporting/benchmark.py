"""Run comparable experiments and keep small timing reports, not legacy files."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import time
import re
from dataclasses import asdict, replace
from pathlib import Path

import numpy as np
import pandas as pd

from ecore_weather.core.common import PeakMemory, write_json, default_workers


def validate_mentor_week(selection, report_dir="artifacts/benchmarks/mentor-validation"):
    """Call the unchanged hourly downloader for every expected clock hour."""
    from ecore_weather.core.common import Asset, utc
    by_time = {a.time: a for a in selection.assets}
    assets = []
    for stamp in selection.expected_times:
        when = utc(stamp)
        key = (f"CARIB/{selection.product}/{when:%Y%m%d}/"
               f"MRMS_{selection.product}_{when:%Y%m%d-%H%M%S}.grib2.gz")
        assets.append(by_time.get(stamp, Asset("noaa-mrms-pds", key, 0, "not-listed", stamp)))
    return run_mrms(replace(selection, assets=assets, hourly_matches=(), time_tolerance_minutes=0,
                            time_match="exact"), report_dir=report_dir, variants=["legacy"])


def comparable_hours(selection):
    """The unchanged mentor API accepts whole hours, not off-hour source files."""
    expected = set(selection.expected_times)
    selected = [a for a in selection.assets if a.time in expected]
    omitted = [a.time for a in selection.assets if a.time not in expected]
    if omitted:
        print("Off-hour files excluded only from the legacy comparison:", omitted)
    return replace(selection, assets=selected)


def raster_summary(path):
    import hashlib
    import rasterio
    with rasterio.open(path) as raster:
        values = raster.read(1)
        valid = np.isfinite(values)
        if raster.nodata is not None and np.isfinite(raster.nodata):
            valid &= values != raster.nodata
        canonical = values.astype("float32")
        canonical[~valid] = np.nan
        return {"shape": list(values.shape), "dtype": str(values.dtype), "crs": str(raster.crs),
                "transform": list(raster.transform), "units_in_file": raster.tags().get("units"),
                "valid_pixels": int(valid.sum()), "zero_pixels": int((values[valid] == 0).sum()),
                "min": float(values[valid].min()) if valid.any() else None,
                "max": float(values[valid].max()) if valid.any() else None,
                "array_sha256": hashlib.sha256(canonical.tobytes()).hexdigest()}


def goes_throughput(selection, report_dir, variants, repeats=2):
    """Measure GOES fetch throughput across backend, block size and workers.

    Each variant writes to an isolated temporary local destination that is
    deleted after timing. The first variant's per-asset fingerprints are the
    reference; every later variant must match them, so speed differences never
    come from changed content. Timing is sensitive to caches and network
    conditions; repeat promising variants before drawing conclusions.

    variants: iterable of dicts with backend, block_size and workers keys.
    Returns a DataFrame with one row per (variant, repeat).
    """
    from ecore_weather.storage import archives as storage
    report_dir = Path(report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    reference = None
    rows = []
    for variant in variants:
        backend, block_size, workers = variant["backend"], int(variant["block_size"]), int(variant["workers"])
        for attempt in range(int(repeats)):
            with tempfile.TemporaryDirectory(prefix="ecore-throughput-") as temp:
                before = time.perf_counter()
                report = storage.fetch(selection, destination=temp, workers=workers, backend=backend,
                                       read_processes=workers, report_dir=None, container="zip",
                                       block_size=block_size)
                wall = time.perf_counter() - before
                saved = [r for r in report["records"] if r["status"] in ("saved", "reused")]
                failed = [r for r in report["records"] if r["status"] == "failed"]
                fps = {}
                for r in saved:
                    with storage.open_raw(r["url"]) as ds:
                        fps[r["asset_id"]] = storage.fingerprint(ds)
                if reference is None:
                    reference = fps
                equal = fps == reference
                row = {"backend": backend, "block_size": block_size, "workers": workers,
                       "repeat": attempt, "wall_s": wall, "files": len(saved), "failed": len(failed),
                       "read_s": sum(r.get("read_decode_crop_s", 0) for r in saved),
                       "write_s": sum(r.get("write_s", 0) for r in saved),
                       "publish_s": sum(r.get("publish_s", 0) for r in saved),
                       "bytes": report.get("read_bytes"), "requests": report.get("requests"),
                       "fingerprints_equal": equal}
                row["effective_MBps"] = (report.get("read_bytes", 0) / 1e6 / wall) if wall else None
                rows.append(row)
                print(row, flush=True)
    table = pd.DataFrame(rows)
    write_json(report_dir / "goes-throughput.json", {"variants": [dict(v) for v in variants],
                                                      "repeats": repeats, "rows": rows,
                                                      "files": len(selection.assets)})
    return table


def run_mrms(selection, report_dir="artifacts/benchmarks", repeats=1, variants=None, workers=None, read_processes=0):
    """Run the original and revised readers in separate fresh Python processes.

    Each variant writes the same temporary GeoTIFF output. The raw-Zarr/HF run
    is deliberately measured through storage.fetch, not called a fetch speedup.
    """
    from ecore_weather.storage.catalog import save_selection
    from ecore_weather.sources.mrms import DEFAULT_PRODUCT
    if selection.source != "mrms" or selection.product != DEFAULT_PRODUCT:
        raise ValueError("The unchanged mentor benchmark supports its original CARIB Pass2 QPE product.")
    if not selection.assets:
        raise ValueError("No source files were selected.")
    if any(a.time[14:19] != "00:00" for a in selection.assets) and not selection.hourly_matches:
        raise ValueError("Off-hour files require an explicit hourly matching manifest.")
    workers = default_workers() if workers is None else workers
    if read_processes < 0:
        raise ValueError("read_processes cannot be negative.")
    default_variants = ["legacy", "s3fs-1", f"s3fs-{workers}", f"obstore-{workers}"]
    if read_processes:
        default_variants.append(f"obstore-process-{min(read_processes, workers)}")
    variants = list(dict.fromkeys(variants or default_variants))
    if any(v != "legacy" and not re.fullmatch(r"(s3fs|obstore)(-process)?-[1-9]\d*", v) for v in variants):
        raise ValueError("Unknown benchmark variant.")
    report_dir = Path(report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    rows, all_records = [], []
    with tempfile.TemporaryDirectory(prefix="ecore-benchmark-") as temp:
        selection_file = save_selection(selection, Path(temp) / "selection").resolve()
        for repeat in range(repeats):
            order = variants if repeat % 2 == 0 else list(reversed(variants))
            for variant in order:
                result_path = Path(temp) / f"{variant}.json"
                before = time.perf_counter()
                with PeakMemory() as memory:
                    result = subprocess.run([sys.executable, "-m", "ecore_weather.reporting._benchmark_worker",
                        str(selection_file), variant, str(result_path)], text=True, capture_output=True)
                if result.returncode:
                    raise RuntimeError(f"{variant} failed: {result.stderr[-3000:]}")
                data = json.loads(result_path.read_text())
                summary = {k: v for k, v in data.items() if k != "records"}
                summary.update(variant=variant, repeat=repeat+1, selection_id=selection.id,
                               process_wall_s=time.perf_counter()-before, peak_rss_bytes=memory.peak,
                               start=selection.start, end=selection.end)
                rows.append(summary)
                all_records.extend([{**r, "variant": variant, "repeat": repeat+1} for r in data["records"]])
                print(f"{variant}, repeat {repeat+1}: {data['succeeded']}/{len(selection.assets)} files, {data['wall_s']:.2f} s", flush=True)
    baseline = {r["asset_id"]: r for r in all_records if r["variant"] == "legacy" and r["status"] == "ok"}
    for record in all_records:
        ref = baseline.get(record["asset_id"])
        record["matches_legacy"] = bool(ref and record["status"] == "ok" and
                                        record["raster"]["array_sha256"] == ref["raster"]["array_sha256"] and
                                        record["raster"]["transform"] == ref["raster"]["transform"])
    table = pd.DataFrame(rows)
    table.to_csv(report_dir / f"mrms-{selection.id}.csv", index=False)
    write_json(report_dir / f"mrms-{selection.id}-details.json", {
        "selection": asdict(selection), "summary": rows, "records": all_records,
        "notes": "Fresh processes and local scratch; provider-side caches are uncontrolled. Read bytes exclude HTTP/TLS headers. Stage seconds are summed task times for concurrent runs.",
    })
    return table, pd.DataFrame(all_records)


def run_goes(selection, report_dir="artifacts/benchmarks", assets=None, repeats=1):
    """Compare full files, ranged subsets, and VirtualiZarr/Kerchunk reads."""
    from ecore_weather.sources import goes
    from ecore_weather.core.common import Transport
    assets = list(assets) if assets is not None else goes.benchmark_assets(selection)
    if not assets:
        raise ValueError("No GOES scans selected.")
    report_dir = Path(report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    virtual_dir = report_dir / f"goes-{selection.id}-references"
    index = goes.build_virtual(selection, virtual_dir, assets=assets)
    index_data = json.loads(index.read_text())
    rows = []
    for repeat in range(repeats):
        for i, asset in enumerate(assets):
            baseline = None
            for mode in ("full_file", "range", "virtual"):
                transport = Transport("s3fs")
                before = time.perf_counter()
                with transport, PeakMemory() as memory:
                    if mode == "virtual":
                        with goes.open_virtual(index, record=i) as vds:
                            ds = vds.load()
                        bytes_read = calls = None  # fsspec owns reference reads; do not invent counts.
                    else:
                        ds, _ = goes.read(asset, selection.bbox, selection.bands, transport,
                                          full_file=mode == "full_file")
                        bytes_read, calls = transport.bytes, transport.requests
                    arrays = {name: ds[name].values.copy() for name in ds.variables}
                    if baseline is None:
                        baseline = arrays
                    matches = arrays.keys() == baseline.keys() and all(
                        np.array_equal(values, baseline[name], equal_nan=True) for name, values in arrays.items())
                    elapsed = time.perf_counter() - before
                rows.append({"selection_id": selection.id, "asset_id": asset.id, "time": asset.time,
                             "mode": mode, "repeat": repeat+1, "wall_s": elapsed, "read_bytes": bytes_read,
                             "data_read_calls": calls, "peak_rss_bytes": memory.peak, "matches_full_file": matches})
                ds.close()
                if not matches:
                    raise AssertionError(f"{mode} did not match the full source subset: {asset.key}")
            print(f"GOES scan {i+1}/{len(assets)}, repeat {repeat+1}: values match", flush=True)
    table = pd.DataFrame(rows)
    table.to_csv(report_dir / f"goes-{selection.id}.csv", index=False)
    write_json(report_dir / f"goes-{selection.id}-references-summary.json",
               {k: v for k, v in index_data.items() if k != "records"})
    return table


def plot_timings(table, label="variant"):
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(8, 4), constrained_layout=True)
    table.groupby(label).wall_s.median().plot.bar(ax=ax)
    ax.set(ylabel="Median elapsed seconds", xlabel="", title="Measured runtime for the same selection")
    ax.tick_params(axis="x", rotation=20)
    return fig



from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, replace
from pathlib import Path
import tempfile
import time

import pandas as pd

from ecore_weather.reporting import benchmark
from ecore_weather.reporting import diagnostics
from ecore_weather.sources import goes
from ecore_weather.storage import archives as storage
from ecore_weather.core.common import default_workers, write_json


def validate(selection, workers=None, report_path=None, compare_mentor=True, save_figures=None):
    """Validate every selected MRMS file, or representative GOES scans.

    Mentor comparisons use independent, isolated batches in parallel. Their
    timings are correctness-test timings, not sequential fetch speed benchmarks.
    Only the concise result and explicitly requested figures survive success.
    """
    workers = default_workers() if workers is None else workers
    start = time.perf_counter()
    result = {"selection_id": selection.id, "selection": selection.summary(), "workers": workers,
              "test_kind": "full MRMS period" if selection.source == "mrms" else "representative GOES scans"}
    with tempfile.TemporaryDirectory(prefix="ecore-validation-") as temp:
        root = Path(temp)
        if selection.source == "mrms":
            raw = storage.fetch(selection, workers=workers, backend="obstore", report_dir=None,
                                validate_only=True, scratch=root,
                                inspect=lambda ds: diagnostics.describe(ds).to_dict("records"),
                                progress=lambda done, total, row: print(f"Raw validation {done}/{total}", flush=True)
                                if done % 100 == 0 or done == total else None)
            result["raw"] = {k: raw[k] for k in ("wall_s", "read_bytes", "data_read_calls", "peak_rss_bytes")}
            result["raw"].update(validated=sum(r["status"] == "validated" for r in raw["records"]),
                stored_bytes=sum(r.get("stored_bytes", 0) for r in raw["records"]),
                logical_array_bytes=sum(r.get("logical_array_bytes", 0) for r in raw["records"]),
                failures=[r for r in raw["records"] if r["status"] == "failed"])
            table = pd.DataFrame([d for r in raw["records"] for d in r.get("diagnostics", [])])
            if not table.empty:
                result["patches"] = table.groupby("patch")[["valid_pct", "valid_zero_pct", "missing_fill_pct",
                    "no_coverage_pct", "bitmap_missing_pct", "nonfinite_pct"]].mean().to_dict("index")
                result["coverage_outages"] = diagnostics.outage_lengths(table).to_dict("records")
                if save_figures:
                    import matplotlib.pyplot as plt
                    path = Path(save_figures); path.mkdir(parents=True, exist_ok=True)
                    fig = diagnostics.plot_coverage(table, selection.expected_times)
                    fig.savefig(path / "coverage.png", dpi=150)
                    plt.close(fig)
            if compare_mentor and selection.assets:
                # Bound both the number of child processes and the data per batch.
                batch_size = max(1, (len(selection.assets)+workers-1)//workers)
                batches = [selection.assets[i:i+batch_size] for i in range(0, len(selection.assets), batch_size)]
                def check(i, assets):
                    ids = {a.id for a in assets}
                    matches = tuple(m for m in selection.hourly_matches if m["asset_id"] in ids)
                    expected = tuple(m["slot_time"] for m in matches) or tuple(a.time for a in assets)
                    batch = replace(selection, assets=assets, hourly_matches=matches, expected_times=expected)
                    _, rows = benchmark.run_mrms(batch, report_dir=root/f"batch-{i}",
                                                   variants=["legacy", "obstore-1"])
                    revised = rows[rows.variant != "legacy"]
                    return {"files": len(assets), "matched": int(revised.matches_legacy.sum()),
                            "failures": revised[~revised.matches_legacy].to_dict("records")}
                comparisons = []
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    futures = [pool.submit(check, i, batch) for i, batch in enumerate(batches)]
                    for future in as_completed(futures):
                        comparisons.append(future.result())
                        print(f"Mentor comparison batches {len(comparisons)}/{len(batches)}", flush=True)
                result["mentor_comparison"] = {"files": sum(r["files"] for r in comparisons),
                    "matched": sum(r["matched"] for r in comparisons),
                    "failures": [f for r in comparisons for f in r["failures"]],
                    "note": "Parallel correctness verification; not a sequential-download speed benchmark."}
        else:
            coverage = goes.acquisition_coverage(selection)
            unusual = coverage[(coverage.observed_files != coverage.expected_files) | coverage.expected_files.isna()]
            result["acquisition_coverage"] = {
                "hours": len(coverage), "observed_files": int(coverage.observed_files.sum()),
                "empty_hours": coverage.loc[coverage.observed_files == 0, "hour"].tolist(),
                "unusual_hours": unusual.to_dict("records"),
                "note": "Counts describe discovered objects; equality is checked only on representative scans."}
            assets = goes.benchmark_assets(selection)
            table = benchmark.run_goes(selection, report_dir=root, assets=assets)
            result["tested_objects"] = [asdict(a) for a in assets]
            result["comparisons"] = table.to_dict("records")
            result["all_variables_match"] = bool(table.matches_full_file.all())
            import json
            references = json.loads(next(root.glob("*-references/index.json")).read_text())
            result["reference_build_s"] = references["build_s"]
            result["reference_bytes"] = references["reference_bytes"]
            result["encoding_groups"] = len(references["groups"])
            sample = replace(selection, assets=assets)
            raw = storage.fetch(sample, workers=workers, backend="s3fs", report_dir=None,
                                validate_only=True, scratch=root)
            result["raw"] = {"validated": sum(r["status"] == "validated" for r in raw["records"]),
                             "failures": [r for r in raw["records"] if r["status"] == "failed"]}
            if save_figures and assets:
                from ecore_weather.ui.maps import show_or_save
                ds, _ = goes.read(assets[0], selection.bbox, selection.bands)
                with ds:
                    show_or_save(ds, directory=save_figures, display=False)
        result["wall_s"] = time.perf_counter()-start
        result["passed"] = (bool(selection.assets) and not result["raw"]["failures"] and
            (result.get("all_variables_match", True)) and not result.get("mentor_comparison", {}).get("failures"))
    if report_path:
        write_json(report_path, result)
    return result
