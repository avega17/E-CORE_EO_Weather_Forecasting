from dataclasses import replace
import json
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from ecore_weather import diagnostics, goes, mrms
from ecore_weather.catalog import load_selection, save_selection
from ecore_weather.cli import parser as cli_parser
from ecore_weather.common import Asset, Selection, validate_request
from ecore_weather.storage import fingerprint, open_raw, valid_raw_name, write_raw


@pytest.mark.parametrize(("alias", "product"), mrms.PRODUCT_ALIASES.items())
def test_mrms_cli_product_aliases(alias, product):
    args = cli_parser("mrms").parse_args(["--product", alias])
    assert args.product == [product]
    assert cli_parser("mrms").parse_args(["--product", alias.upper()]).product == [product]


def test_mrms_cli_keeps_exact_noaa_product_names():
    for product in mrms.PRODUCTS:
        assert mrms.parse_product_argument(product) == product


def test_record_selection_hashes_large_selection_once(tmp_path):
    from datetime import datetime, timezone
    from ecore_weather import index

    class CountedSelection:
        source = "goes"
        product = "ABI-L2-CMIPF"
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end = datetime(2026, 2, 1, tzinfo=timezone.utc)
        hourly_matches = ()

        def __init__(self):
            self.id_calls = 0
            self.assets = [Asset("noaa-goes19", f"ABI-L2-CMIPF/C13/{i}.nc", 100,
                f"etag-{i}", "2026-01-01T00:00:00Z") for i in range(3)]

        @property
        def id(self):
            self.id_calls += 1
            return "selection-id"

        def summary(self):
            return {"files": len(self.assets)}

    selection = CountedSelection()
    database = tmp_path / "archive-index.duckdb"
    index.record_selection(selection, "selection.json", database)

    assert selection.id_calls == 1
    with index.connect(database) as db:
        rows = db.execute("SELECT count(*) FROM selections").fetchone()
        assert rows == (1,)
        assert "observations" not in {r[0] for r in db.execute("SHOW TABLES").fetchall()}


def test_mrms_study_records_empty_catalog_objects_but_does_not_fetch_them():
    from ecore_weather.jobs_mrms import partition_empty_sources

    readable = Asset("noaa-mrms-pds", "CARIB/a.grib2.gz", 120, "abc",
                     "2023-05-24T06:48:57Z")
    empty = Asset("noaa-mrms-pds", "CARIB/empty.grib2.gz", 0,
                  "d41d8cd98f00b204e9800998ecf8427e", "2023-05-24T06:58:57Z")
    selection = Selection("mrms", mrms.DEFAULT_PRODUCT, "2023-05-24", "2023-05-25",
                          (-70, 14, -62, 22), [readable, empty],
                          expected_times=(readable.time, empty.time))

    fetch_selection, invalid = partition_empty_sources(selection)

    assert selection.assets == [readable, empty]
    assert fetch_selection.assets == [readable]
    assert fetch_selection.expected_times == selection.expected_times
    assert invalid == [{"asset_id": empty.id, "bucket": empty.bucket, "key": empty.key,
        "source_url": empty.url, "observation_time": empty.time, "size": 0,
        "etag": empty.etag, "reason": "zero_byte_noaa_object"}]


def test_mrms_retries_transient_incomplete_grib_read(monkeypatch):
    import gzip
    import threading

    valid = bytearray(20)
    valid[:4] = b"GRIB"
    valid[8:16] = len(valid).to_bytes(8, "big")
    invalid = bytearray(valid)
    invalid[:4] = b"BAD!"
    bad_bytes, good_bytes = gzip.compress(invalid), gzip.compress(valid)
    assert len(bad_bytes) == len(good_bytes)
    dataset = xr.Dataset({
        "measurement": (("latitude", "longitude"), [[1.0]], {"units": "mm h-1"}),
        "bitmap_valid": (("latitude", "longitude"), [[1]], {}),
    }, coords={"latitude": [18.0], "longitude": [-66.0]})
    monkeypatch.setattr(mrms, "decode_grib", lambda payload, product: dataset.copy(deep=True))

    class FlakyTransport:
        decode_slots = threading.BoundedSemaphore(1)

        def __init__(self):
            self.reads = 0

        def read(self, asset):
            self.reads += 1
            return bad_bytes if self.reads == 1 else good_bytes

    asset = Asset("noaa-mrms-pds", "CARIB/PrecipRate_00.00/test.grib2.gz",
                  len(good_bytes), "", "2023-05-01T00:00:00Z")
    transport = FlakyTransport()
    result, _ = mrms.read(asset, bbox=None, product="PrecipRate_00.00", transport=transport)
    assert transport.reads == 2
    np.testing.assert_array_equal(result.measurement.values, [[1.0]])


def test_mrms_study_defaults_are_the_four_requested_fields():
    assert mrms.DEFAULT_PRODUCTS == (
        "PrecipRate_00.00",
        "MergedReflectivityQCComposite_00.50",
        "MergedAzShear_0-2kmAGL_00.50",
        "MultiSensor_QPE_01H_Pass2_00.00",
    )


def test_yearly_mrms_bundle_keeps_month_zip_bytes_and_manifest(tmp_path):
    import hashlib
    import zipfile
    from ecore_weather.jobs_backup import _build_bundle

    product = mrms.DEFAULT_PRODUCTS[0]
    archive = tmp_path / "local" / "mrms" / product / "2021" / "01" / "raw.zarr.zip"
    archive.parent.mkdir(parents=True)
    archive.write_bytes(b"already-compressed-month-archive")
    marker_path = archive.parent / "complete.json"
    marker = {"source": "mrms", "product": product, "raw_path": archive.name,
        "observations": 1, "asset_ids": ["asset-1"],
        "stored_bytes": archive.stat().st_size,
        "archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest()}
    marker_path.write_text(json.dumps(marker))
    entries = {archive.relative_to(tmp_path / "local").as_posix(): {
        "archive": archive, "marker_path": marker_path, "marker": marker,
        "member": "data/month.zip", "marker_member": "metadata/month/complete.json",
        "selected_by": ["2021-01"]}}
    bundle = _build_bundle(2021, product, entries, [], tmp_path / "out", tmp_path / "local")

    with zipfile.ZipFile(bundle["path"]) as package:
        assert package.read("data/month.zip") == archive.read_bytes()
        assert json.loads(package.read("metadata/month/complete.json")) == marker
        manifest = json.loads(package.read("year_manifest.json"))
    assert manifest["archive_count"] == 1
    assert manifest["archives"][0]["sha256"] == marker["archive_sha256"]
    assert bundle["sha256"] == hashlib.sha256(Path(bundle["path"]).read_bytes()).hexdigest()


def test_mrms_final_study_year_bundle_records_h1_period(tmp_path):
    import json
    import hashlib
    import zipfile
    from ecore_weather.jobs_backup import _build_bundle
    from ecore_weather.jobs_backup import _year_months

    product = mrms.DEFAULT_PRODUCTS[0]
    archive = tmp_path / "local" / "raw.zarr.zip"
    archive.parent.mkdir(parents=True)
    archive.write_bytes(b"monthly")
    marker_path = archive.parent / "complete.json"
    marker = {"stored_bytes": archive.stat().st_size,
        "archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
        "asset_ids": ["asset"], "observations": 1}
    marker_path.write_text(json.dumps(marker))
    bundle = _build_bundle(2026, product,
        {"mrms/example/2026/06/raw.zarr.zip": {
            "archive": archive, "marker_path": marker_path, "marker": marker,
            "member": "data/example.zip", "marker_member": "metadata/example.json",
            "selected_by": ["2026-06"]}}, [], tmp_path / "out", tmp_path)
    with zipfile.ZipFile(bundle["path"]) as package:
        manifest = json.loads(package.read("year_manifest.json"))
    assert manifest["requested_study_period"] == [
        "2026-01-01T00:00:00Z", "2026-07-01T00:00:00Z"]
    assert len(_year_months(2026)) == 6


def test_yearly_bundle_retains_zero_byte_noaa_source_as_coverage_metadata(tmp_path):
    import hashlib
    import json
    from ecore_weather.jobs_backup import _collect_year

    product = mrms.DEFAULT_PRODUCTS[1]
    study = tmp_path / "study"
    source_root = tmp_path / "local"
    for month in range(1, 13):
        key = f"2023-{month:02d}"
        folder = study / "months" / key
        folder.mkdir(parents=True, exist_ok=True)
        for name in mrms.DEFAULT_PRODUCTS:
            row = {"status": "unavailable", "month": key, "product": name}
            if key == "2023-05" and name == product:
                good_id, empty_id = "good-id", "empty-id"
                rows = [good_id]
                archive = source_root / "mrms" / name / "2023" / "05" / "raw.zarr.zip"
                archive.parent.mkdir(parents=True, exist_ok=True)
                archive.write_bytes(b"archive")
                marker = {"source": "mrms", "product": name, "raw_path": archive.name,
                    "observations": 1, "asset_ids": rows,
                    "stored_bytes": archive.stat().st_size,
                    "archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest()}
                (archive.parent / "complete.json").write_text(json.dumps(marker))
                row = {"status": "complete", "month": key, "product": name,
                    "archives": [str(archive)], "listed_slots": 2, "matched_slots": 1,
                    "invalid_source_files": [{"asset_id": empty_id,
                        "key": "CARIB/empty.grib2.gz", "observation_time": "2023-05-24T07:00:00Z",
                        "etag": "d41d8cd98f00b204e9800998ecf8427e", "size": 0,
                        "reason": "zero_byte_noaa_object"}]}
                selection = {"features": [
                    {"id": good_id, "properties": {"ecore:source": {"size": 12}}},
                    {"id": empty_id, "properties": {"ecore:source": {
                        "key": "CARIB/empty.grib2.gz", "time": "2023-05-24T07:00:00Z",
                        "etag": "d41d8cd98f00b204e9800998ecf8427e", "size": 0}}}]}
                (folder / f"{name}-selection").mkdir()
                (folder / f"{name}-selection" / "items.json").write_text(json.dumps(selection))
            (folder / f"{name}.json").write_text(json.dumps(row))

    grouped, selections = _collect_year(2023, source_root, study)

    assert set(grouped[product])
    assert {entry["member"].split("/", 1)[0] for entry in selections[product]} == {
        "selections", "coverage"}




def test_run_config_history_preserves_previous_product_selection(tmp_path):
    from ecore_weather.jobs_mrms import _record_run_config

    old = {"products": ["PrecipRate_00.00", "MultiSensor_QPE_01H_Pass1_00.00"]}
    new = {"products": list(mrms.DEFAULT_PRODUCTS)}
    _record_run_config(tmp_path, old)
    _record_run_config(tmp_path, new)

    assert json.loads((tmp_path / "run_config.json").read_text()) == new
    history = list((tmp_path / "run-config-history").glob("*.json"))
    assert len(history) == 1
    assert json.loads(history[0].read_text()) == old




def test_goes_stager_reuses_only_matching_saved_selection(tmp_path):
    from ecore_weather.jobs_goes import _reusable_selection

    start, end = "2026-01-01", "2026-02-01"
    asset = Asset("noaa-goes19", "ABI-L2-CMIPF/2026/001/00/OR_ABI-L2-CMIPF-M6C01_G19_s20260010000200.nc",
                  1024, "etag", "2026-01-01T00:00:20Z")
    selection = Selection("goes", "ABI-L2-CMIPF", start, end,
        (-70.24, 14.36, -62.56, 22.04), [asset], bands=(1, 2, 3, 7, 8, 9, 10, 13),
        satellite=19)
    saved = tmp_path / "ABI-L2-CMIPF"
    save_selection(selection, saved, index_results=False)

    assert _reusable_selection(tmp_path, start, end, "ABI-L2-CMIPF") == saved / "collection.json"
    assert _reusable_selection(tmp_path, start, "2026-03-01", "ABI-L2-CMIPF") is None


def radar(values, product=mrms.DEFAULT_PRODUCT):
    values = np.asarray(values, dtype="float64")
    ds = xr.Dataset({"measurement": (("latitude", "longitude"), values),
                     "bitmap_valid": (("latitude", "longitude"), np.ones(values.shape, "uint8"))},
                    coords={"latitude": np.linspace(19, 17, values.shape[0]),
                            "longitude": np.linspace(292, 295, values.shape[1])},
                    attrs={"product": product, "observation_time": "2022-09-18T00:00:00Z"})
    ds.measurement.attrs["units"] = mrms.PRODUCTS[product]["unit"]
    return ds


def test_sentinels_and_valid_zero():
    ds = radar([[0, 2, -1], [-3, np.nan, 6]])
    table = diagnostics.describe(ds, {"all": (-69, 16, -64, 20)})
    row = table.iloc[0]
    assert row.valid_count == 3
    assert row.valid_zero_count == 1
    assert row.missing_fill_count == row.no_coverage_count == row.nonfinite_count == 1
    assert row["mean"] == pytest.approx(8/3)
    assert sum(row[f"{name}_count"] for name in diagnostics.CLASS_NAMES.values()) == 6


def test_negative_reflectivity_is_valid_and_bitmap_is_separate():
    ds = radar([[-10, -99], [-999, 3]], "MergedReflectivityQCComposite_00.50")
    ds.bitmap_valid.values[1, 1] = 0
    codes, _ = diagnostics.classify(ds, "measurement")
    np.testing.assert_array_equal(codes, [[0, 1], [2, 3]])


def test_azimuthal_shear_zero_remains_ambiguous():
    ds = radar([[0, 0], [1, -1]], "MergedAzShear_0-2kmAGL_00.50")
    ds.bitmap_valid.values[0, 1] = 0
    codes, _ = diagnostics.classify(ds, "measurement")
    np.testing.assert_array_equal(codes, [[7, 3], [0, 0]])
    row = diagnostics.describe(ds, {"all": (-69, 16, -64, 20)}).iloc[0]
    assert row.ambiguous_shear_zero_count == 1
    assert row.valid_zero_count == 0


def test_all_invalid_patch_and_outside_patch():
    ds = radar([[-1, -3], [-1, -3]])
    table = diagnostics.describe(ds, {"all": (-69, 16, -64, 20), "outside": (-85, 0, -80, 5)})
    assert table.iloc[0].note == "no valid measurements"
    assert np.isnan(table.iloc[0]["mean"])
    assert table.iloc[1].pixels == 0
    assert np.isnan(table.iloc[1].valid_pct)


def test_raw_roundtrip_preserves_sentinels_and_metadata(tmp_path):
    ds = radar([[0, -1], [-3, 12.3]])
    ds.measurement.attrs.update(scale_note="No new rounding", missingValue=9999)
    before = fingerprint(ds)
    write_raw(ds, tmp_path / "raw.zarr")
    with open_raw(tmp_path / "raw.zarr") as actual:
        assert fingerprint(actual) == before
        assert actual.measurement.attrs == ds.measurement.attrs


def test_packed_goes_roundtrip_and_quality_mask(tmp_path):
    raw = xr.Dataset({"CMI_C13": (("y", "x"), np.array([[100, -1], [200, 300]], "int16")),
                      "DQF_C13": (("y", "x"), np.array([[0, 3], [1, 0]], "int8"))},
                     coords={"x": [0, 1], "y": [0, 1]})
    raw.CMI_C13.attrs = {"_FillValue": np.int16(-1), "scale_factor": np.float32(.1),
                        "add_offset": np.float32(200), "_Unsigned": "true", "units": "K"}
    raw.x.attrs = {"scale_factor": .000056, "add_offset": -.151844}
    raw.y.attrs = {"scale_factor": -.000056, "add_offset": .151844}
    before = fingerprint(raw)
    write_raw(raw, tmp_path / "goes.zarr")
    with open_raw(tmp_path / "goes.zarr") as actual:
        assert fingerprint(actual) == before
        processed = goes.process(actual)
        assert processed.shape == (2, 2)
        assert processed.values[0, 0] == pytest.approx(210)
        assert np.isnan(processed.values[0, 1])
        assert np.isnan(processed.values[1, 0])
    assert fingerprint(raw) == before


def test_interpolation_recipes_do_not_mutate_raw():
    ds = radar([[1, -3, 3], [1, 2, 3], [0, 0, 0]])
    before = fingerprint(ds)
    for recipe in ["legacy_exact", "quality_aware"]:
        processed = mrms.process(ds, recipe=recipe, bbox=(-68, 17, -65, 19), shape=(8, 8))
        assert processed.shape == (8, 8)
        assert fingerprint(ds) == before


def test_stac_selection_roundtrip_and_half_open_interval(tmp_path):
    a = Asset("bucket", "a.grib2.gz", 123, "etag", "2022-09-18T00:00:00Z")
    selection = Selection("mrms", mrms.DEFAULT_PRODUCT, a.time, "2022-09-19T00:00:00Z",
                          (-70, 15, -62, 22), [a], expected_times=(a.time,))
    path = save_selection(selection, tmp_path / "catalog")
    actual = load_selection(path)
    assert actual.id == selection.id
    assert actual.assets == selection.assets
    assert load_selection(tmp_path / "catalog/items.json.gz").id == selection.id
    with pytest.raises(ValueError):
        validate_request("2022-09-19", "2022-09-18", selection.bbox)


def test_native_crop_keeps_source_order_and_values():
    ds = radar(np.arange(12).reshape(3, 4))
    crop = mrms.crop_native(ds, (-67.1, 16.5, -64.9, 19.5))
    np.testing.assert_array_equal(crop.longitude, [293, 294, 295])
    np.testing.assert_array_equal(crop.measurement, ds.measurement.values[:, 1:])


def test_goes_scan_time_and_sample_selection():
    stamp = goes.scan_time("20222610000202")
    assert stamp.second == 20 and stamp.microsecond == 200000
    assets = [Asset("b", f"a{i}", 1, "e", f"2022-09-{day:02d}T{hour:02d}:00:20Z")
              for i, (day, hour) in enumerate((d, h) for d in range(18, 25) for h in range(24))]
    s = Selection("goes", "ABI-L2-MCMIPF", "2022-09-18", "2022-09-25", (-70, 15, -62, 22), assets)
    picked = goes.benchmark_assets(s)
    assert len(picked) == 6
    assert [a.time[:10] for a in picked] == ["2022-09-18"]*2+["2022-09-21"]*2+["2022-09-24"]*2


def test_monthly_zarr_source_exposes_saved_native_values(monkeypatch):
    from contextlib import nullcontext
    from ecore_weather import earth2_sources
    ds = radar([[0, -1], [2, 3]])
    ds = ds.expand_dims(time=[np.datetime64("2024-09-15T00:00:00")])
    monkeypatch.setattr("ecore_weather.storage.open_raw", lambda _path: nullcontext(ds))
    source = earth2_sources.MonthlyZarrSource("archive.zip", "mrms",
        product=mrms.DEFAULT_PRODUCT)
    actual = source(["2024-09-15T00:02:00"], ["qpe_1h"])
    assert actual.dims == ("time", "variable", "latitude", "longitude")
    assert actual.coords["variable"].values.tolist() == ["qpe_1h"]
    np.testing.assert_array_equal(actual.isel(time=0, variable=0), ds.measurement.isel(time=0))

    packed = xr.Dataset({"CMI_C13": (("y", "x"), np.array([[100, -1], [200, 300]], "int16")),
                         "DQF_C13": (("y", "x"), np.array([[0, 3], [1, 0]], "int8"))},
                        coords={"x": [0, 1], "y": [0, 1]}).expand_dims(
                            time=[np.datetime64("2025-09-15T12:00:00")])
    monkeypatch.setattr("ecore_weather.storage.open_raw", lambda _path: nullcontext(packed))
    source = earth2_sources.MonthlyZarrSource("goes.zip", "goes", band=13)
    actual = source(["2025-09-15T12:02:00"], ["abi13c"])
    assert actual.dims == ("time", "variable", "y", "x")
    assert actual.coords["variable"].values.tolist() == ["abi13c"]
    assert actual.dtype == np.dtype("int16")
    np.testing.assert_array_equal(actual.isel(time=0, variable=0), packed.CMI_C13.isel(time=0))


def test_live_datasource_reports_actual_observation_time(monkeypatch):
    from ecore_weather import earth2_sources
    source = earth2_sources.MRMSCaribbeanSource(product="PrecipRate_00.00")
    raw = radar([[1, 2]], product="PrecipRate_00.00")
    raw.attrs["observation_time"] = "2021-01-01T00:08:00Z"
    monkeypatch.setattr(source, "read_dataset", lambda _time: raw)
    result = source("2021-01-01T00:10:00Z", "precip_rate")
    assert str(result.time.values[0]).startswith("2021-01-01T00:08:00")

    goes_source = earth2_sources.GOESCaribbeanSource(band=13)
    packed = xr.Dataset({"CMI": (("y", "x"), np.array([[10]], dtype="int16"))},
                        coords={"y": [0], "x": [0]},
                        attrs={"observation_time": "2021-01-01T00:00:20Z"})
    monkeypatch.setattr(goes_source, "read_dataset", lambda _time: packed)
    result = goes_source("2021-01-01T00:02:00Z", "abi13c")
    assert str(result.time.values[0]).startswith("2021-01-01T00:00:20")


def test_earth2studio_writer_keeps_native_dtype_and_bitmap(tmp_path):
    pytest.importorskip("earth2studio")
    from ecore_weather.earth2_io import write_dataset
    source = radar([[0, -1], [2, 3]]).expand_dims(
        time=[np.datetime64("2021-01-01T00:00:00")])
    source.bitmap_valid.values[0, 1, 0] = 0
    path = tmp_path / "native.zarr"
    write_dataset(source, path)
    with open_raw(path) as reopened:
        for name in ("measurement", "bitmap_valid", "time", "latitude", "longitude"):
            np.testing.assert_array_equal(reopened[name].values, source[name].values)
            assert reopened[name].dtype == source[name].dtype


def test_streaming_month_merges_without_duplicate_observations(tmp_path, monkeypatch):
    pytest.importorskip("earth2studio")
    import json
    from ecore_weather import monthly_stream
    first = Asset("bucket", "CARIB/PrecipRate_00.00/20210101/a.grib2.gz", 5, "a",
                  "2021-01-01T00:00:00Z")
    second = Asset("bucket", "CARIB/PrecipRate_00.00/20210101/b.grib2.gz", 5, "b",
                   "2021-01-01T00:10:00Z")
    selection = Selection("mrms", "PrecipRate_00.00", "2021-01-01", "2021-02-01",
                          (-70, 14, -62, 22), [first, second])

    def fake_read(asset, *_args):
        ds = radar([[0, -1], [2, 3]])
        ds.measurement.values[0, 0] = 1 if asset.id == first.id else 2
        ds.bitmap_valid.values[0, 1] = 0
        ds["calibration_note"] = xr.DataArray(np.int16(7))
        return ds, {}

    monkeypatch.setattr(monthly_stream, "_read_new", fake_read)
    target = tmp_path / "month"
    assert monthly_stream._write_one(selection, [first], target, None, workers=1)["status"] == "saved"
    assert monthly_stream._write_one(selection, [first, second], target, None, workers=1)["status"] == "saved"
    assert monthly_stream._write_one(selection, [first, second], target, None, workers=1)["status"] == "reused"
    with open_raw(target / "raw.zarr.zip") as stored:
        assert stored.sizes["time"] == 2
        assert stored.measurement.values[:, 0, 0].tolist() == [1, 2]
        assert stored.bitmap_valid.values[:, 0, 1].tolist() == [0, 0]
        meta = json.loads(str(stored.source_metadata_json.values[1]))
        assert meta["variables"]["calibration_note"]["values"] == 7


def test_monthly_stream_bounds_source_read_concurrency():
    from ecore_weather.monthly_stream import source_read_workers
    assert [source_read_workers(n) for n in (1, 4, 16, 32)] == [1, 4, 16, 16]


def test_goes_study_inventory_scenarios_use_one_listing(monkeypatch):
    from ecore_weather import study_goes
    from datetime import datetime, timedelta, timezone
    day = datetime(2021, 1, 1, tzinfo=timezone.utc)
    assets = []
    for band in range(1, 17):
        for minute in range(0, 60, 10):
            stamp = day + timedelta(minutes=minute)
            code = stamp.strftime("%Y%j%H%M%S") + "0"
            key = f"ABI-L2-CMIPF/2021/001/00/OR_ABI-L2-CMIPF-M6C{band:02d}_G16_s{code}_e{code}_c{code}.nc"
            assets.append(Asset("noaa-goes16", key, 100, "etag", stamp.isoformat()))
    listed = []
    monkeypatch.setattr(study_goes, "_day_assets", lambda d, _client: listed.append(d) or assets)
    row = study_goes.inventory_month(day, day + timedelta(days=1), client=object())
    assert len(listed) == 1
    assert [row["scenarios"][name]["band_files"] for name in
            ("eight_6ph", "eight_3ph", "eight_1ph", "sixteen_6ph")] == [48, 24, 8, 96]
    assert row["scenarios"]["eight_6ph"]["listed_full_file_bytes"] == 4800


def test_fetch_resume_and_failure_cleanup(tmp_path, monkeypatch):
    from ecore_weather import storage
    good = Asset('b', 'good', 1, 'e', '2024-09-15T00:00:00Z')
    bad = Asset('b', 'bad', 1, 'e', '2024-09-15T01:00:00Z')
    selection = Selection('mrms', mrms.DEFAULT_PRODUCT, good.time, '2024-09-15T02:00:00Z',
                          (-69, 16, -64, 20), [good, bad])
    calls = []
    def read(asset, *args, **kwargs):
        calls.append(asset.key)
        if asset.key == 'bad':
            raise IOError('deliberately unavailable source')
        return radar([[0, -1], [-3, 12.3]]), {'download_s': 0, 'decode_crop_s': 0}
    monkeypatch.setattr(mrms, 'read', read)
    scratch = tmp_path / 'scratch'
    report = storage.fetch(selection, destination=tmp_path/'data', scratch=scratch,
                           report_dir=tmp_path/'reports', workers=2)
    assert [r['status'] for r in report['records']] == ['saved', 'failed']
    assert list(scratch.iterdir()) == []
    assert not list((tmp_path/'data'/selection.id/bad.id).glob('complete.json'))
    again = storage.fetch(selection, destination=tmp_path/'data', scratch=scratch,
                          report_dir=tmp_path/'reports', workers=1)
    assert again['records'][0]['status'] == 'reused'
    assert calls.count('good') == 1
    assert list(scratch.iterdir()) == []


def test_legacy_comparison_rejects_off_hour_file():
    from ecore_weather.benchmark import comparable_hours, run_mrms
    asset = Asset('b', 'a', 1, 'e', '2022-09-24T16:58:00Z')
    selection = Selection('mrms', mrms.DEFAULT_PRODUCT, '2022-09-24', '2022-09-25',
                          (-70.24, 14.36, -62.56, 22.04), [asset],
                          expected_times=('2022-09-24T17:00:00Z',))
    assert not comparable_hours(selection).assets
    with pytest.raises(ValueError, match='Off-hour'):
        run_mrms(selection)


def test_storage_requires_explicit_destination():
    from ecore_weather.storage import destination_root
    with pytest.raises(ValueError):
        destination_root('')
    with pytest.raises(ValueError):
        destination_root('s3://unsupported-durable-destination')


def test_remote_month_name_is_limited_to_verified_versioned_zarr():
    assert valid_raw_name("raw.zarr.zip")
    assert valid_raw_name("raw-" + "a" * 32 + ".zarr")
    assert not valid_raw_name("raw-../other.zarr")
    assert not valid_raw_name("raw-" + "g" * 32 + ".zarr")


def test_default_hf_fetch_dispatches_to_direct_async_writer(monkeypatch):
    from ecore_weather import monthly
    asset = Asset("b", "CARIB/PrecipRate_00.00/20210101/x.grib2.gz", 1, "e",
                  "2021-01-01T00:00:00Z")
    selection = Selection("mrms", "PrecipRate_00.00", "2021-01-01", "2021-02-01",
                          (-70, 14, -62, 22), [asset])
    calls = []
    def write(*args, **kwargs):
        calls.append((args, kwargs))
        return {"status": "saved", "path": "hf://test/raw-" + "a" * 32 + ".zarr",
                "stored_bytes": 123, "observations": 1, "read_bytes": 10,
                "hf_upload_bytes": 123, "hf_upload_seconds": 1.5}
    monkeypatch.setattr("ecore_weather.remote_async.write_selection_month", write)
    report = monthly.fetch_remote_streaming(selection,
        remote_root="hf://buckets/test/bucket/noaa-subsets", report_dir=None,
        index_results=False)
    assert len(calls) == 1
    assert report["hf_upload_bytes"] == 123
    assert report["monthly_archives"][0]["path"].endswith(".zarr")
    assert "async Zarr" in report["storage_layout"]


def test_remote_cleanup_only_targets_inactive_writer_archives(monkeypatch):
    from ecore_weather import remote_async
    active = "raw-" + "a" * 32 + ".zarr"
    old = "raw-" + "b" * 32 + ".zarr"
    class Client:
        def list_objects_v2(self, **_kwargs):
            return {"CommonPrefixes": [
                {"Prefix": f"prefix/month/{active}/"},
                {"Prefix": f"prefix/month/{old}/"},
                {"Prefix": "prefix/month/other.zarr/"}],
                "Contents": [{"Key": "prefix/month/complete.json"},
                             {"Key": "prefix/month/raw.zarr.zip"}]}
    class Writer:
        client = Client()
        config = {"bucket": "bucket"}
        def key(self, value):
            return "prefix/" + value
    removed = []
    monkeypatch.setattr(remote_async, "_delete_owned", lambda _writer, path: removed.append(path))
    count = remote_async._cleanup_old_versions(Writer(), "month", active)
    assert count == 2
    assert sorted(removed) == sorted([f"month/{old}", "month/raw.zarr.zip"])


def test_remote_object_listing_keeps_directory_boundary():
    from ecore_weather.remote_async import _objects

    class Client:
        def list_objects_v2(self, **kwargs):
            assert kwargs["Prefix"] == "prefix/month/raw-example.zarr/"
            return {"Contents": [{"Key": kwargs["Prefix"] + "zarr.json"}]}

    class Writer:
        client = Client()
        config = {"bucket": "bucket"}

        def key(self, value):
            return "prefix/" + value.strip("/")

    assert len(list(_objects(Writer(), "month/raw-example.zarr/"))) == 1


def test_earth2studio_async_partial_time_shard_flushes_on_close(tmp_path):
    pytest.importorskip("earth2studio")
    from collections import OrderedDict
    from earth2studio.io import AsyncZarrBackend
    from zarr.codecs import BloscCodec
    import torch
    import zarr
    stamps = np.array([np.datetime64("2021-01-01T00:00") + np.timedelta64(10*i, "m")
                       for i in range(13)])
    coords = OrderedDict(time=stamps, latitude=np.array([18., 17.]),
                         longitude=np.array([-67., -66.]))
    io = AsyncZarrBackend(None, parallel_coords=OrderedDict(time=stamps),
        store=str(tmp_path / "async.zarr"), blocking=False, pool_size=1,
        shard_coords={"time": 12}, max_inflight_shards=2,
        zarr_codecs=BloscCodec(cname="zstd", clevel=3, shuffle="shuffle"))
    io.add_array(coords, "measurement", dtype=np.int16)
    for i in range(13):
        selected = OrderedDict((name, value[i:i+1] if name == "time" else value)
                               for name, value in coords.items())
        io.write(torch.full((1, 2, 2), i, dtype=torch.int16), selected, "measurement")
    io.close()
    group = zarr.open_group(str(tmp_path / "async.zarr"), mode="r")
    np.testing.assert_array_equal(group["measurement"][:, 0, 0], np.arange(13))


def test_hour_matching_preserves_actual_time_and_prevents_future_default():
    assets = [Asset("b", str(i), 1, "e", t) for i, t in enumerate([
        "2022-09-24T16:58:00Z", "2022-09-24T18:02:00Z"])]
    slots = ("2022-09-24T17:00:00Z", "2022-09-24T18:00:00Z")
    picked, matches = mrms.match_hours(assets, slots)
    assert picked == [assets[0]]
    assert matches[0]["offset_seconds"] == -120
    assert matches[0]["source_time"] == assets[0].time
    assert not mrms.match_hours(assets, slots, method="exact")[0]
    assert len(mrms.match_hours(assets, slots, method="nearest")[0]) == 2
    with pytest.raises(ValueError):
        mrms.match_hours(assets, slots, tolerance_minutes=31)


def test_compact_catalog_retains_hour_matches(tmp_path):
    asset = Asset("b", "a", 1, "e", "2022-09-24T16:58:00Z")
    slots = ("2022-09-24T17:00:00Z",)
    assets, matches = mrms.match_hours([asset], slots)
    selection = Selection("mrms", mrms.DEFAULT_PRODUCT, slots[0], "2022-09-24T18:00:00Z",
                          (-70, 15, -62, 22), assets, expected_times=slots,
                          hourly_matches=matches, time_tolerance_minutes=5, time_match="previous")
    actual = load_selection(save_selection(selection, tmp_path))
    assert actual.id == selection.id
    assert actual.summary()["missing_times"] == []
    import pystac
    from ecore_weather.common import utc
    collection = pystac.Collection.from_file(str(tmp_path / "collection.json"))
    assert collection.extent.temporal.intervals[0][0] == utc(asset.time)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["collection.json", "items.json.gz"]


def test_cli_and_satellite_defaults():
    import multiprocessing
    from ecore_weather.cli import parser
    from ecore_weather.common import default_workers
    assert default_workers() == max(1, multiprocessing.cpu_count()//2)
    args = parser("mrms").parse_args(["--period", "2025", "--workers", "3", "--save-figures", "plots"])
    assert args.workers == 3 and args.save_figures == "plots"
    assert args.monthly_writers == 2
    assert args.product is None
    assert parser("goes").parse_args([]).bands == [1, 2, 3, 7, 8, 9, 10, 13]
    assert parser("goes").parse_args([]).product is None
    assert parser("goes").parse_args([]).scans_per_hour == 0
    assert goes.east_satellite("2022-09-01", "2022-12-01") == 16
    assert goes.east_satellite("2025-09-01", "2025-12-01") == 19
    with pytest.raises(ValueError):
        goes.east_satellite("2025-04-01", "2025-05-01")


def test_validation_only_cleans_data_and_preserves_slot(tmp_path, monkeypatch):
    from ecore_weather import storage
    asset = Asset("b", "a", 1, "e", "2022-09-24T16:58:00Z")
    slots = ("2022-09-24T17:00:00Z",)
    assets, matches = mrms.match_hours([asset], slots)
    selection = Selection("mrms", mrms.DEFAULT_PRODUCT, slots[0], "2022-09-24T18:00:00Z",
                          (-70, 15, -62, 22), assets, expected_times=slots, hourly_matches=matches)
    ds = radar([[0, 1], [-1, -3]])
    ds.attrs["observation_time"] = asset.time
    monkeypatch.setattr(mrms, "read", lambda *a, **kw: (ds, {}))
    report = storage.fetch(selection, workers=1, validate_only=True, scratch=tmp_path,
                           report_dir=None, inspect=lambda d: diagnostics.describe(d).to_dict("records"))
    row = report["records"][0]
    assert row["status"] == "validated" and row["url"] is None
    assert row["diagnostics"][0]["time"] == asset.time
    assert row["diagnostics"][0]["slot_time"] == slots[0]
    assert not list(tmp_path.iterdir())


def test_coverage_uses_matched_slot_without_losing_actual_time():
    import matplotlib.pyplot as plt
    ds = radar([[0, 1], [2, 3]])
    ds.attrs.update(observation_time="2022-09-24T16:58:00Z", hourly_slot="2022-09-24T17:00:00Z")
    table = diagnostics.describe(ds, {"all": (-69, 16, -64, 20)})
    fig = diagnostics.plot_coverage(table, (ds.attrs["hourly_slot"], "2022-09-24T18:00:00Z"))
    assert len(fig.axes[0].lines[0].get_ydata()) == 2
    np.testing.assert_array_equal(fig.axes[0].lines[0].get_ydata(), [100, np.nan])
    assert table.iloc[0].time == ds.attrs["observation_time"]
    plt.close(fig)


def test_json_reports_use_null_for_unavailable_statistics(tmp_path):
    import json
    from ecore_weather.common import write_json
    path = tmp_path / "report.json"
    write_json(path, {"median": np.nan, "bytes": None})
    assert "NaN" not in path.read_text()
    assert json.loads(path.read_text()) == {"median": None, "bytes": None}


def test_goes_coverage_reports_empty_hours_and_known_scan_counts():
    assets = [Asset("b", f"OR_ABI-L2-MCMIPF-M6_G16_{i}", 1, "e",
                    f"2022-09-01T00:{i*10:02d}:20Z") for i in range(6)]
    selection = Selection("goes", "ABI-L2-MCMIPF", "2022-09-01", "2022-09-01T02:00:00Z",
                          (-70, 15, -62, 22), assets, bands=(8,13))
    coverage = goes.acquisition_coverage(selection)
    assert coverage.observed_files.tolist() == [6, 0]
    assert coverage.iloc[0].expected_files == 6
    assert np.isnan(coverage.iloc[1].expected_files)


def test_goes_partial_day_sample_uses_available_scans_and_each_band():
    assets = [Asset("b", f"OR_ABI-L2-CMIPF-M6C{band:02d}_G19", 1, "e",
                    "2025-09-01T13:10:20Z") for band in (8,13)]
    selection = Selection("goes", "ABI-L2-CMIPF", "2025-09-01T13:00:00Z", "2025-09-01T14:00:00Z",
                          (-70, 15, -62, 22), assets, bands=(8,13))
    assert goes.benchmark_assets(selection) == assets


def test_script_returns_failure_for_benchmark_mismatch(tmp_path, monkeypatch):
    import pandas as pd
    from ecore_weather import benchmark, cli
    asset = Asset("b", "a", 1, "e", "2022-09-01T00:00:00Z")
    selection = Selection("mrms", mrms.DEFAULT_PRODUCT, asset.time, "2022-09-01T01:00:00Z",
                          (-70, 15, -62, 22), [asset])
    monkeypatch.setattr(mrms, "discover", lambda **kw: selection)
    monkeypatch.setattr(benchmark, "run_mrms", lambda *a, **kw: (pd.DataFrame(), pd.DataFrame({"matches_legacy": [False]})))
    assert cli.main("mrms", ["--operation", "benchmark", "--output", str(tmp_path)]) == 1


def test_hf_batch_verifies_before_completion_and_resumes(tmp_path):
    from types import SimpleNamespace
    from ecore_weather.hf_storage import Publisher
    from ecore_weather.storage import metadata_fingerprint
    class API:
        def __init__(self): self.files = {}; self.adds = []; self.corrupt = False
        def batch_bucket_files(self, bucket, *, add):
            self.adds.append([name for _, name in add])
            for source, name in add:
                self.files[name] = source if isinstance(source, bytes) else source.read_bytes()
        def get_bucket_paths_info(self, bucket, paths):
            return [SimpleNamespace(path=p) for p in paths if p in self.files]
        def download_bucket_files(self, bucket, files, **kw):
            for remote, local in files:
                data = self.files[remote if isinstance(remote, str) else remote.path]
                local.write_bytes(b'corrupt' if self.corrupt else data)
    api = API(); pub = Publisher('hf://buckets/u/b/test', api=api)
    ds = radar([[0, -1], [-3, 4]])
    write_raw(ds, tmp_path/'raw.zarr')
    marker = dict(selection_id='s', asset_id='a', raw_schema_version=1,
                  array_sha256=fingerprint(ds), metadata_sha256=metadata_fingerprint(ds))
    pub.publish(tmp_path, 'hf://buckets/u/b/test', marker)
    assert api.adds[-1] == ['test/complete.json']
    assert pub.resume('hf://buckets/u/b/test','s','a',1)
    api.corrupt = True
    with pytest.raises(Exception): pub.publish(tmp_path, 'hf://buckets/u/b/bad', marker)
    assert 'bad/complete.json' not in api.files


def test_hf_writer_serializes_threads():
    from concurrent.futures import ThreadPoolExecutor
    from ecore_weather.hf_storage import bucket_writer
    import time
    active = peak = 0
    def work(_):
        nonlocal active, peak
        with bucket_writer('unit-test/bucket'):
            active += 1; peak = max(peak, active)
            time.sleep(.005)
            active -= 1
    with ThreadPoolExecutor(16) as pool: list(pool.map(work, range(16)))
    assert peak == 1


def test_hf_scoped_writer_allows_disjoint_months_but_locks_same_month():
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier, Lock
    from ecore_weather.hf_storage import bucket_writer
    import time

    barrier = Barrier(2)
    def disjoint(scope):
        with bucket_writer("unit-test/scoped", scope=scope):
            barrier.wait(timeout=2)
    with ThreadPoolExecutor(2) as pool:
        list(pool.map(disjoint, ("mrms/p/2021/01", "mrms/p/2021/02")))

    active = peak = 0
    guard = Lock()
    def same_month(_):
        nonlocal active, peak
        with bucket_writer("unit-test/scoped", scope="mrms/p/2021/01"):
            with guard:
                active += 1
                peak = max(peak, active)
            time.sleep(.01)
            with guard:
                active -= 1
    with ThreadPoolExecutor(2) as pool:
        list(pool.map(same_month, range(2)))
    assert peak == 1

    # A scoped writer must also coordinate with older bucket-wide publishers.
    active = peak = 0
    global_guard = Lock()
    def global_and_month(scope):
        with bucket_writer("unit-test/global-scope", scope=scope):
            nonlocal active, peak
            with global_guard:
                active += 1
                peak = max(peak, active)
            time.sleep(.01)
            with global_guard:
                active -= 1
    with ThreadPoolExecutor(2) as pool:
        list(pool.map(global_and_month, (None, "mrms/p/2021/01")))
    assert peak == 1




def test_zip_container_preserves_arrays_metadata_and_removes_directory(tmp_path):
    from ecore_weather.storage import pack_raw, metadata_fingerprint
    ds = radar([[0,-1],[-3,4]])
    write_raw(ds,tmp_path/'raw.zarr')
    path = pack_raw(tmp_path)
    assert not (tmp_path/'raw.zarr').exists()
    with open_raw(path) as actual:
        assert fingerprint(actual) == fingerprint(ds)
        assert metadata_fingerprint(actual) == metadata_fingerprint(ds)


def test_quiet_eccodes_notices_suppresses_fd2_and_restores(capfd):
    import os
    from ecore_weather.mrms import _ECCODES_NOTICE_LOCK, _quiet_eccodes_notices
    os.write(2, b"before\n")
    with _quiet_eccodes_notices():
        os.write(2, b"hidden\n")
    os.write(2, b"after\n")
    err = capfd.readouterr().err
    assert "hidden" not in err
    assert "before" in err and "after" in err
    assert not _ECCODES_NOTICE_LOCK.locked()


def test_quiet_eccodes_notices_restores_after_exception(capfd):
    import os
    from ecore_weather.mrms import _ECCODES_NOTICE_LOCK, _quiet_eccodes_notices
    with pytest.raises(RuntimeError):
        with _quiet_eccodes_notices():
            raise RuntimeError("boom")
    assert not _ECCODES_NOTICE_LOCK.locked()
    os.write(2, b"recovered\n")
    assert "recovered" in capfd.readouterr().err


def test_metadata_get_quiets_only_time_keys(capfd):
    import os
    from ecore_weather.mrms import _metadata_get

    class FakeEC:
        @staticmethod
        def codes_get(handle, key):
            if key in mrms._METADATA_TIME_KEYS:
                os.write(2, f"ECCODES ERROR   :  Key {key} (unpack_long): Truncating time\n".encode())
            return 1658

    for key in ("dataDate", "dataTime", "validityDate", "validityTime"):
        assert _metadata_get(FakeEC, None, key) == 1658
    assert "ECCODES ERROR" not in capfd.readouterr().err
    assert _metadata_get(FakeEC, None, "discipline") == 1658


def test_widget_products_hide_conus_and_cheatsheet_collapsed():
    import ipywidgets as widgets
    from ecore_weather import ui
    controls = ui.selection_controls("goes")
    options = controls["product"].options
    product_values = [option[1] for option in options]
    assert "ABI-L2-MCMIPF" in product_values and "ABI-L2-CMIPF" in product_values
    assert not any(option.endswith("C") for option in product_values)
    cheatsheet = controls["cheatsheet"]
    assert isinstance(cheatsheet, widgets.Accordion)
    assert cheatsheet.selected_index is None
    html = cheatsheet.children[0].value
    assert "ABI-L2-MCMIPF" in html and "CONUS" in html and "C13" in html
    assert controls["product"].value == "ABI-L2-CMIPF"
    assert tuple(controls["bands"].value) == goes.STORMSCOPE_BANDS
    assert all("µm" in label for label, _ in controls["bands"].options)


def test_mrms_cheatsheet_lists_products_and_sentinels():
    from ecore_weather import ui
    controls = ui.selection_controls("mrms")
    assert set(value for _, value in controls["product"].options) == set(mrms.DEFAULT_PRODUCTS)
    cheatsheet = controls["cheatsheet"]
    assert cheatsheet.selected_index is None
    html = cheatsheet.children[0].value
    for product in mrms.PRODUCTS:
        assert product in html
    assert "dBZ" in html and "−1 / −3" in html and "−99 / −999" in html


def test_overlapping_dates_reuse_raw_and_container(tmp_path, monkeypatch):
    from ecore_weather import storage
    asset = Asset('b', 'good', 1, 'e', '2024-09-15T00:00:00Z')
    selection = Selection('mrms', mrms.DEFAULT_PRODUCT, asset.time, '2024-09-16', (-69,16,-64,20), [asset])
    calls = []
    def read(*args, **kwargs):
        calls.append(1)
        return radar([[0,-1],[-3,2]]), {}
    monkeypatch.setattr(mrms, 'read', read)
    first = storage.fetch(selection, tmp_path, workers=1, report_dir=None, container='zip')
    wider = replace(selection, start='2024-09-01', end='2024-12-01')
    second = storage.fetch(wider, tmp_path, workers=1, report_dir=None, container='directory')
    assert second['records'][0]['status'] == 'reused'
    assert first['records'][0]['url'] == second['records'][0]['url']
    assert calls == [1]
    assert not list(tmp_path.rglob('raw.zarr'))
    assert storage.subset_identity(replace(selection, bbox=(-70,16,-64,20))) != storage.subset_identity(selection)


def test_cmip_identity_does_not_depend_on_other_requested_bands():
    from ecore_weather.storage import subset_identity
    request = Selection('goes', 'ABI-L2-CMIPF', '2023-01-01', '2023-02-01', (-69,16,-64,20), [], bands=(1,13), satellite=16)
    assert subset_identity(request) == subset_identity(replace(request, bands=(1,2,3,13)))
    multi = replace(request, product='ABI-L2-MCMIPF')
    assert subset_identity(multi) != subset_identity(replace(multi, bands=(1,2,3,13)))


def test_earlier_dated_store_adoption_checks_raw(tmp_path, monkeypatch):
    from ecore_weather import storage
    asset = Asset('b', 'good', 1, 'e', '2024-09-15T00:00:00Z')
    selection = Selection('mrms', mrms.DEFAULT_PRODUCT, asset.time, '2024-09-16', (-69,16,-64,20), [asset])
    old = tmp_path/storage.product_path(selection)/'2024-09-15_2024-09-16-old'/'2024/09/15'/f'000000-{asset.id}'
    ds = radar([[0,-1],[-3,2]]); ds.attrs['requested_bbox'] = list(selection.bbox)
    storage.write_raw(ds, old/'raw.zarr')
    storage.write_json(old/'complete.json', dict(asset_id=asset.id, selection_id='old', raw_schema_version=1,
        array_sha256=storage.fingerprint(ds), metadata_sha256=storage.metadata_fingerprint(ds)))
    monkeypatch.setattr(mrms, 'read', lambda *a, **kw: pytest.fail('must reuse existing raw'))
    result = storage.fetch(selection, tmp_path, workers=1, report_dir=None)
    assert result['records'][0]['status'] == 'reused'
    assert not old.exists()
    assert len(list(tmp_path.rglob('complete.json'))) == 1


def test_worker_tooltips_and_viewer_preserve_raw(tmp_path, monkeypatch):
    import matplotlib
    matplotlib.use('Agg')
    from ecore_weather import ui, viewer, maps
    monkeypatch.setattr(maps, "_land_polygons", lambda: [])
    controls = ui.selection_controls('mrms')
    assert controls['read_processes'].description == 'Readers'
    assert '0 uses threads' in controls['read_processes'].tooltip
    assert 'Hugging Face' in controls['workers'].tooltip
    ds = radar([[0,1],[-1,-3]])
    ds.attrs['requested_bbox'] = [-69,16,-64,20]
    path = tmp_path/'raw.zarr'
    write_raw(ds, path)
    before = fingerprint(ds)
    assert viewer.stores(path) == [str(path)]
    viewer.draw(path, output=tmp_path/'rain.png', display=False, hide_zero=True)
    assert (tmp_path/'rain.png').stat().st_size > 1000
    with open_raw(path) as actual:
        assert fingerprint(actual) == before


def test_single_band_goes_accepts_one_element_band_dimension():
    ds = xr.Dataset({'CMI':(('y','x'),np.ones((2,2),'int16')),
                     'DQF':(('y','x'),np.zeros((2,2),'int8')),
                     'band_id':('band',[2]), 'band_wavelength':('band',[.64])})
    names = goes.selected_variables(ds, (1,2,3,7,8,9,10,13))
    assert {'CMI','DQF','band_id','band_wavelength'} <= set(names)
    with pytest.raises(ValueError, match='requested band'):
        goes.selected_variables(ds, (8,13))


def test_merge_duplicate_archive_retains_unrelated_files(tmp_path):
    import importlib.util
    from pathlib import Path
    from ecore_weather import storage
    from ecore_weather import archive_maintenance as module
    asset = Asset('b','good',1,'e','2024-09-15T00:00:00Z')
    selection = Selection('mrms',mrms.DEFAULT_PRODUCT,asset.time,'2024-09-16',(-69,16,-64,20),[asset])
    ds=radar([[0,-1],[-3,2]]);ds.attrs['requested_bbox']=list(selection.bbox)
    old=[]
    for period in ('2024-09-01_2024-10-01-a','2024-09-15_2024-09-16-b'):
        path=tmp_path/storage.product_path(selection)/period/'2024/09/15'/f'000000-{asset.id}'
        storage.write_raw(ds,path/'raw.zarr')
        storage.write_json(path/'complete.json',dict(asset_id=asset.id,raw_schema_version=1,
            array_sha256=storage.fingerprint(ds),metadata_sha256=storage.metadata_fingerprint(ds)))
        old.append(path)
    (old[1]/'research-note.txt').write_text('Keep this note')
    report=module.merge(selection,tmp_path)
    assert report['complete']
    assert report['counts']['moved']==1 and report['counts']['duplicates_removed']==1
    notes=list(tmp_path.rglob('research-note.txt'))
    assert len(notes)==1 and notes[0].read_text()=='Keep this note'
    assert len(list(tmp_path.rglob('raw.zarr')))==1
    again=module.merge(selection,tmp_path)
    assert again['counts']['already_shared']==1 and again['complete']


def test_concurrent_overlapping_runs_publish_once(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    import time
    from ecore_weather import storage
    asset=Asset('b','good',1,'e','2024-09-15T00:00:00Z')
    selection=Selection('mrms',mrms.DEFAULT_PRODUCT,asset.time,'2024-09-16',(-69,16,-64,20),[asset])
    calls=[]
    def read(*args, **kwargs):
        calls.append(1)
        time.sleep(.05)
        return radar([[0,-1],[-3,2]]),{}
    monkeypatch.setattr(mrms,'read',read)
    with ThreadPoolExecutor(2) as pool:
        futures=[pool.submit(storage.fetch,s,tmp_path,workers=1,report_dir=None) for s in
                 (selection,replace(selection,start='2024-09-01',end='2024-12-01'))]
        outcomes=[f.result()['records'][0]['status'] for f in futures]
    assert sorted(outcomes)==['reused','saved']
    assert calls==[1]


def test_interrupt_cancels_queued_fetches_and_records_progress(tmp_path, monkeypatch):
    import time,json
    from ecore_weather import storage
    assets=[Asset('b',f'file-{i}',1,'e',f'2024-09-15T{i:02d}:00:00Z') for i in range(20)]
    selection=Selection('mrms',mrms.DEFAULT_PRODUCT,'2024-09-15','2024-09-16',(-69,16,-64,20),assets)
    calls=[]
    def read(*args,**kwargs):
        calls.append(1);time.sleep(.02)
        return radar([[0,-1],[-3,2]]),{}
    monkeypatch.setattr(mrms,'read',read)
    def interrupt(*args):raise KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):
        storage.fetch(selection,tmp_path/'data',scratch=tmp_path/'scratch',workers=1,
                      report_dir=tmp_path/'reports',progress=interrupt)
    report=json.loads(next((tmp_path/'reports').glob('*.json')).read_text())
    assert report['interrupted'] and report['not_started']>0
    assert 1 <= len(calls) < len(assets)
    assert report['records_count']==len(calls)
    assert not list((tmp_path/'scratch').iterdir())
    assert 'records' not in report
    assert len(list((tmp_path/'data').rglob('complete.json')))==len(calls)


def _scan(key_time, minute, band=None):
    band_key = f"-M6C{band:02d}" if band else "-M6"
    key = f"ABI-L2-MCMIPF/2022/250/00/OR_ABI-L2-MCMIPF{band_key}_G16_s202225000{minute:02d}00_e202225000{minute:02d}59_c202225000{minute:02d}99.nc"
    return Asset("noaa-goes16", key, 10, "e", key_time)


def test_hourly_scans_picks_nearest_to_marks_without_reuse():
    from ecore_weather.goes import hourly_scans
    # Scans at :02, :11, :28, :41, :50, :58 within one hour (multiband keys).
    minutes = [2, 11, 28, 41, 50, 58]
    assets = [_scan(f"2022-09-07T00:{m:02d}:00Z", m) for m in minutes]
    one = hourly_scans(assets, 1)
    assert [a.time for a in one] == ["2022-09-07T00:02:00Z"]  # nearest to :00
    two = hourly_scans(assets, 2)
    assert [a.time for a in two] == ["2022-09-07T00:02:00Z", "2022-09-07T00:28:00Z"]  # nearest to :00 and :30
    six = hourly_scans(assets, 6)
    assert [a.time for a in six] == [f"2022-09-07T00:{m:02d}:00Z" for m in [2, 11, 28, 41, 50, 58]]
    # More marks than available scans: keep all, no reuse.
    sparse = [_scan("2022-09-07T00:05:00Z", 5), _scan("2022-09-07T00:40:00Z", 40)]
    assert len(hourly_scans(sparse, 6)) == 2


def test_hourly_scans_applies_per_band_for_single_band_products():
    from ecore_weather.goes import hourly_scans
    assets = ([_scan(f"2022-09-07T00:{m:02d}:00Z", m, band=8) for m in (2, 28)] +
              [_scan(f"2022-09-07T00:{m:02d}:00Z", m, band=13) for m in (2, 28)])
    assert len(hourly_scans(assets, 1)) == 2  # one per band


def test_selection_scans_per_hour_roundtrip(tmp_path):
    bbox = (-70.24, 14.36, -62.56, 22.04)
    asset = _scan("2022-09-07T00:02:00Z", 2)
    selection = Selection("goes", "ABI-L2-MCMIPF", "2022-09-07", "2022-09-08", bbox, [asset],
                          bands=(8, 13), satellite=16, scans_per_hour=3)
    save_selection(selection, tmp_path)
    loaded = load_selection(tmp_path / "collection.json")
    assert loaded.scans_per_hour == 3
    assert loaded.summary()["scans_per_hour"] == 3
    # Earlier manifests without the field still load with the None default.
    legacy = Selection("goes", "ABI-L2-MCMIPF", "2022-09-07", "2022-09-08", bbox, [asset],
                       bands=(8, 13), satellite=16)
    assert legacy.scans_per_hour is None


def test_cli_scans_per_hour_default_and_all():
    from ecore_weather.cli import parser
    goes_parser = parser("goes")
    args = goes_parser.parse_args([])
    assert args.scans_per_hour == 0
    assert goes_parser.parse_args(["--scans-per-hour", "0"]).scans_per_hour == 0
    controls = __import__("ecore_weather.ui", fromlist=["ui"]).selection_controls("goes")
    assert controls["scans_per_hour"].value == 0
    assert controls["bands"].value == goes.STORMSCOPE_BANDS


def _synthetic_frame(time="2022-09-07T00:00:00Z"):
    import numpy as np
    values = np.linspace(0, 60, 16, dtype="float32").reshape(4, 4)
    return {"values": values, "bbox": (-70, 14, -62, 22), "extent": (0, 1, 0, 1),
            "time": time, "label": "CMI C13", "units": "K", "path": "x"}


def test_save_animation_html_and_gif_and_bad_extension(tmp_path):
    from ecore_weather.view_frames import save_animation
    frames = [_synthetic_frame(), _synthetic_frame("2022-09-07T00:10:00Z")]
    html = save_animation(frames, tmp_path / "a.html")
    assert (tmp_path / "a.html").read_text() and html.endswith(".html")
    gif = save_animation(frames, tmp_path / "a.gif")
    assert (tmp_path / "a.gif").stat().st_size > 0 and gif.endswith(".gif")
    with pytest.raises(ValueError):
        save_animation(frames, tmp_path / "a.txt")


def test_save_animation_mp4_requires_ffmpeg(tmp_path, monkeypatch):
    from ecore_weather import view_frames
    monkeypatch.setattr(view_frames, "ffmpeg_available", lambda: False)
    with pytest.raises(RuntimeError):
        view_frames.save_animation([_synthetic_frame()], tmp_path / "a.mp4")


def _goes_subset(path, bands=(8, 13)):
    import numpy as np
    from ecore_weather.storage import write_raw
    coords = {"x": np.linspace(-0.01, 0.01, 4), "y": np.linspace(-0.01, 0.01, 4)}
    ds = xr.Dataset(coords=coords)
    for b in bands:
        ds[f"CMI_C{b:02d}"] = (("y", "x"), np.full((4, 4), 100 + b, "int16"), {"scale_factor": 0.1, "add_offset": 200.0, "units": "K", "_FillValue": np.int16(-1)})
        ds[f"DQF_C{b:02d}"] = (("y", "x"), np.zeros((4, 4), "int8"))
    ds["goes_imager_projection"] = ((), 0, {"grid_mapping_name": "geostationary", "semi_major_axis": 6378137.0, "semi_minor_axis": 6356752.31414,
        "perspective_point_height": 35786023.0, "longitude_of_projection_origin": -75.0, "latitude_of_projection_origin": 0.0,
        "sweep_angle_axis": "x"})
    ds.attrs["observation_time"] = "2022-09-07T00:00:00Z"
    ds.attrs["requested_bbox"] = [-66.6, 17.9, -66.2, 18.3]
    write_raw(ds, path)


def test_viewer_main_band_decoupled_on_multiband(tmp_path):
    from ecore_weather.viewer import main
    marker_dir = tmp_path / "goes" / "ABI-L2-MCMIPF" / "roi-x" / "2022" / "09" / "07" / "000000-scan"
    marker_dir.mkdir(parents=True)
    _goes_subset(marker_dir / "raw.zarr")
    (marker_dir / "complete.json").write_text('{"raw_path":"raw.zarr","asset_id":"scan"}')
    out = tmp_path / "view.png"
    # Band 13 is a display variable, not a search filter, on a multiband store.
    assert main([str(tmp_path), "--source", "goes", "--band", "13", "--output", str(out)]) == 0
    assert out.exists()
