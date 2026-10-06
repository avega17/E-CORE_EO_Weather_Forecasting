"""Grouped MCMIPF archive contracts using tiny packed arrays."""

import gzip
import json
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pytest
import zarr

from ecore_weather.goes_monthly import _append, _band_group, _pack, _verify_stage
from ecore_weather import goes_monthly
from ecore_weather.storage import open_raw
from ecore_weather.view_index import inventory
from ecore_weather.catalog import load_selection, save_selection
from ecore_weather.common import Asset, Selection
from ecore_weather.index import record_fetch, search


@pytest.mark.parametrize("archive_product", ("ABI-L2-MCMIPF", "ABI-L2-CMI-2KM-HYBRID"))
def test_grouped_month_preserves_missing_channel_and_viewer_rows(tmp_path, archive_product):
    folder = tmp_path / "goes" / archive_product / "goes16" / "roi-example" / "2022" / "09"
    folder.mkdir(parents=True)
    working = folder / "working.zarr"
    root = zarr.open_group(str(working), mode="a")
    root.attrs.update({"source": "goes", "product": archive_product,
                       "requested_bbox": [-70.24, 14.36, -62.56, 22.04]})
    x, y = np.array([1., 2.]), np.array([3., 4.])
    scans = []
    batches = []
    for i, bands in enumerate(((1, 13), (13,))):
        stamp = f"2022-09-18T12:{i}0:00Z"
        images = {band: {"cmi": np.full((2, 2), i + band, dtype="int16"),
                         "dqf": np.zeros((2, 2), dtype="int8"),
                         "cmi_attrs": {"scale_factor": 0.1}, "dqf_attrs": {},
                         "origin": {"product": "ABI-L2-CMIPF" if
                                    archive_product == "ABI-L2-CMI-2KM-HYBRID" and band == 13
                                    else "ABI-L2-MCMIPF"}}
                  for band in bands}
        scan = {"x": x, "y": y, "x_attrs": {}, "y_attrs": {},
                "time": stamp, "images": images}
        checksums = {}
        for band in bands:
            group = _band_group(root, band, scan)
            index, hashes = _append(group, band, scan)
            checksums[str(band)] = {"index": index, **hashes}
        asset_id = f"scan-{i}"
        scans.append({"asset_id": asset_id, "time": stamp,
                      "available_bands": list(bands), "source_bytes": 100})
        batches.append({"rows": [{"asset_id": asset_id, "checksums": checksums}]})
    _verify_stage(root, batches)
    with gzip.open(working / "ecore_metadata.json.gz", "wt") as output:
        json.dump({"dataset_attrs": dict(root.attrs),
                   "groups": {"C01": {"asset_ids": ["scan-0"]},
                              "C13": {"asset_ids": ["scan-0", "scan-1"]}},
                   "source_metadata": {row["asset_id"]: json.dumps({"variables": {}})
                                       for row in scans}}, output)
    archive = folder / "raw.zarr.zip"
    _pack(working, archive)
    (folder / "complete.json").write_text(json.dumps({"source": "goes",
        "product": archive_product, "raw_path": archive.name,
        "assets": scans, "stored_bytes": archive.stat().st_size}))
    with open_raw(archive, group="C01") as band1:
        assert band1.sizes["time"] == 1
        np.testing.assert_array_equal(band1.CMI_C01.values[0], np.full((2, 2), 1))
    with open_raw(archive, group="C13") as band13:
        assert band13.sizes["time"] == 2
        np.testing.assert_array_equal(band13.CMI_C13.values[1], np.full((2, 2), 14))
    found = inventory(tmp_path, source="goes")
    assert [(row["asset_id"], row["band"]) for row in found] == [
        ("scan-0", 1), ("scan-0", 13), ("scan-1", 13)]


def test_goes_stac_keeps_discovery_facts_without_changing_selection_id(tmp_path):
    asset = Asset("noaa-goes16", "ABI-L2-MCMIPF/example.nc", 100, "etag",
                  "2022-09-18T12:00:00Z")
    selection = Selection("goes", "ABI-L2-MCMIPF", "2022-09-18T12:00:00Z",
        "2022-09-18T13:00:00Z", (-70.24, 14.36, -62.56, 22.04), [asset],
        bands=(1, 13), satellite=16)
    before = selection.id
    selection.discovery_facts = {"duplicate_processing_versions": [],
        "hours_without_files": [], "undersubscribed_hours": [],
        "scan_mode_changes": []}
    save_selection(selection, tmp_path, index_results=False)
    loaded = load_selection(tmp_path / "items.json.gz")
    assert loaded.id == before
    assert loaded.discovery_facts == selection.discovery_facts


def test_hybrid_and_pure_band_records_coexist_in_local_index(tmp_path, monkeypatch):
    monkeypatch.setenv("ECORE_INDEX_PATH", str(tmp_path / "index.duckdb"))
    for product, suffix in (("ABI-L2-MCMIPF", ""),
                            ("ABI-L2-CMI-2KM-HYBRID", "-HYBRID")):
        folder=tmp_path/product
        folder.mkdir();archive=folder/'raw.zarr.zip';archive.write_bytes(b'archive')
        from ecore_weather.common import write_json
        write_json(folder/'complete.json',{'source':'goes','product':product,'raw_path':archive.name,
            'observations':1,'assets':[{'asset_id':'same-scan','time':'2026-01-01T00:00:00Z','available_bands':[13]}]})
        record_fetch({"source": "goes", "monthly_archives":[{'path':str(archive),'observations':1}], "selection_id": "same-source-selection",
            "archive_product": product, "run_id": product,
            "selection_summary": {"product": "ABI-L2-MCMIPF"},
            "root": str(tmp_path), "wall_s": 1,
            "records": [{"asset_id": "same-scan", "subset_id": "same-roi-C13" + suffix,
                         "time": "2026-01-01T00:00:00Z", "band": 13,
                         "source_url": "https://noaa-goes19.s3.amazonaws.com/example.nc",
                         "source_bytes": 100, "url": str(tmp_path / product / "raw.zarr.zip"),
                         "status": "archived"}]})
    found = search(tmp_path, source="goes", band=13)
    assert {row["product"] for row in found} == {
        "ABI-L2-MCMIPF", "ABI-L2-CMI-2KM-HYBRID"}


def test_failed_scan_batch_is_retried_on_resume(tmp_path, monkeypatch):
    """A transient source error must not become a completed scratch checkpoint."""
    class LocalPool(ThreadPoolExecutor):
        def __init__(self, max_workers, mp_context=None, initializer=None):
            super().__init__(max_workers=max_workers)

    monkeypatch.setattr(goes_monthly, "ProcessPoolExecutor", LocalPool)
    assets = [Asset("noaa-goes16", f"ABI-L2-MCMIPF/scan-{i}.nc", 100, "etag",
                    f"2022-09-18T12:{i}0:00Z") for i in range(2)]
    selection = Selection("goes", "ABI-L2-MCMIPF", "2022-09-18T12:00:00Z",
        "2022-09-18T13:00:00Z", (-70.24, 14.36, -62.56, 22.04), assets,
        bands=(13,), satellite=16)
    attempts = {assets[1].id: 0}

    def read_batch(portion, *_args):
        rows = []
        for asset in portion:
            if asset.id in attempts:
                attempts[asset.id] += 1
                if attempts[asset.id] == 1:
                    rows.append({"status": "source_error", "asset_id": asset.id,
                        "time": asset.time, "source_url": asset.url, "etag": asset.etag,
                        "source_bytes": asset.size, "bucket": asset.bucket,
                        "key": asset.key, "error": "temporary read failure"})
                    continue
            rows.append({"status": "ok", "asset_id": asset.id, "time": asset.time,
                "source_url": asset.url, "etag": asset.etag, "source_bytes": asset.size,
                "bucket": asset.bucket, "key": asset.key, "end_time": None,
                "metadata": json.dumps({"variables": {}}),
                "x": np.array([1., 2.]), "y": np.array([3., 4.]),
                "x_attrs": {}, "y_attrs": {}, "read_decode_crop_s": 0.01,
                "images": {13: {"cmi": np.full((2, 2), 13, dtype="int16"),
                    "dqf": np.zeros((2, 2), dtype="int8"), "cmi_attrs": {},
                    "dqf_attrs": {}, "origin": {"product": "ABI-L2-MCMIPF",
                    "asset_id": asset.id, "source_url": asset.url, "etag": asset.etag}}}})
        return {"results": rows, "payload_bytes": 40, "read_bytes": 100,
                "range_requests": 1, "transfer_task_seconds": 0.01,
                "source_retries": 0, "failed_requests": 0,
                "batch_wall_seconds": 0.01, "reader_cpu_seconds": 0.01}

    monkeypatch.setattr(goes_monthly, "_read_batch", read_batch)
    target, scratch = tmp_path / "month", tmp_path / "scratch"
    kwargs = dict(reader_processes=1, source_threads=1, prefetch_mib=16,
                  scratch=scratch)
    with pytest.raises(OSError, match="restart will retry"):
        goes_monthly._write_one(selection, assets, target, **kwargs)
    stage = goes_monthly._stage_path(scratch, selection, assets, selection.product)
    progress = json.loads((stage / "progress.json").read_text())
    assert progress["next_index"] == 1
    result = goes_monthly._write_one(selection, assets, target, **kwargs)
    assert result["status"] == "saved" and result["observations"] == 2
    assert attempts[assets[1].id] == 2
    assert not stage.exists()


def test_batch_writer_keeps_per_band_times_and_missing_channel(tmp_path):
    root = zarr.open_group(str(tmp_path / "batch.zarr"), mode="w")
    scans = []
    for i, available in enumerate(((1, 13), (13,), (1, 13))):
        scans.append({"status": "ok", "asset_id": f"scan-{i}",
            "time": f"2022-09-18T12:{i}0:00Z", "source_url": f"noaa/scan-{i}",
            "etag": "etag", "source_bytes": 100, "bucket": "noaa-goes16",
            "key": f"scan-{i}.nc", "end_time": None, "metadata": "{}",
            "read_decode_crop_s": 0.01, "x": np.array([1., 2.]),
            "y": np.array([3., 4.]), "x_attrs": {}, "y_attrs": {},
            "images": {band: {"cmi": np.full((2, 2), i + band, dtype="int16"),
                "dqf": np.zeros((2, 2), dtype="int8"), "cmi_attrs": {},
                "dqf_attrs": {}, "origin": {"product": "ABI-L2-MCMIPF"}}
                for band in available}})
    rows = goes_monthly._write_batch(root, {"results": scans}, (1, 13))
    goes_monthly._verify_stage(root, [{"rows": rows}])
    assert [row["available_bands"] for row in rows] == [[1, 13], [13], [1, 13]]
    assert root["C01/time"].shape == (2,)
    assert root["C13/time"].shape == (3,)
    np.testing.assert_array_equal(root["C01/CMI_C01"][:, 0, 0], [1, 3])
