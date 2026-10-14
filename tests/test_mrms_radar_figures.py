import numpy as np
import pytest
import xarray as xr
from pyproj import CRS, Transformer

from ecore_weather import mrms, mrms_radar_figures as figures
from ecore_weather.common import Asset, Selection


def test_distance_squares_share_center_and_have_requested_sides():
    center = (-66.4, 18.2)
    local = CRS.from_proj4("+proj=aeqd +lon_0=-66.4 +lat_0=18.2 +datum=WGS84 +units=m")
    forward = Transformer.from_crs("EPSG:4326", local, always_xy=True)
    for side in (512, 1024):
        lon, lat = figures.square_outline(center, side)
        x, y = forward.transform(lon, lat)
        assert np.isclose(x.min(), -side * 500, atol=0.01)
        assert np.isclose(x.max(), side * 500, atol=0.01)
        assert np.isclose(y.min(), -side * 500, atol=0.01)
        assert np.isclose(y.max(), side * 500, atol=0.01)
    lon, lat = figures.square_outline(center, 512)
    assert lon.min() > -70.24 and lon.max() < -62.56
    assert lat.min() > 14.36 and lat.max() < 22.04
    future_lon, future_lat = figures.square_outline(center, 1024)
    assert future_lon.min() < -70.24 and future_lon.max() > -62.56
    assert future_lat.min() < 14.36 and future_lat.max() > 22.04


def test_hourly_sample_uses_ten_minute_slot_without_crossing_month(monkeypatch):
    source = Asset("noaa-mrms-pds",
                   "CARIB/MergedReflectivityQCComposite_00.50/20220101/source.grib2.gz",
                   100, "etag", "2022-01-01T00:08:56Z")
    record = {"slot_time": "2022-01-01T00:10:00Z", "source_url": source.url,
              "source_bytes": 100, "etag": "etag", "time": source.time}
    found = figures._month_reflectivity([record], figures.utc("2022-01-01"),
                                         figures.utc("2022-02-01"))
    assert list(found) == ["2022-01-01T00:10:00Z"]

    rqi = Asset("noaa-mrms-pds", "CARIB/RadarQualityIndex_00.00/20220101/rqi.grib2.gz",
                50, "etag2", "2022-01-01T00:10:00Z")
    selection = Selection("mrms", figures.RQI, "2022-01-01T00:00:00Z",
                          "2022-01-01T01:00:00Z", figures.CARIB_BBOX, [rqi],
                          hourly_matches=({"slot_time": "2022-01-01T00:10:00Z",
                                           "asset_id": rqi.id},))
    monkeypatch.setattr(mrms, "discover", lambda *args, **kwargs: selection)
    assert figures._month_rqi(figures.utc("2022-01-01"),
                              figures.utc("2022-01-01T01:00:00Z")) == {
                                  "2022-01-01T00:10:00Z": rqi}


def test_product_specific_missing_values_and_valid_zero(monkeypatch):
    lon = [294, 295, 296]
    lat = [18, 19]

    def fake_read(asset, bbox, product, transport):
        if product == figures.RQI:
            values = np.array([[0, .5, 1], [-1, -3, -285.3]])
            bitmap = np.array([[1, 1, 0], [1, 1, 1]])
        else:
            values = np.array([[-5, 0, 10], [-99, -999, np.nan]])
            bitmap = np.array([[1, 1, 0], [1, 1, 1]])
        return xr.Dataset({"measurement": (("latitude", "longitude"), values),
                           "bitmap_valid": (("latitude", "longitude"), bitmap)},
                          coords={"latitude": lat, "longitude": lon}), {}

    monkeypatch.setattr(mrms, "read", fake_read)
    for kind, product in (("rqi", figures.RQI), ("reflectivity", figures.REFLECTIVITY)):
        asset = Asset("noaa-mrms-pds", f"CARIB/{product}/file.grib2.gz", 5, "etag",
                      "2022-09-18T00:00:00Z")
        result = figures._read_task(kind, vars(asset), None)
        assert result[1].sum() == 2
        assert result[1][0, 0] and result[1][0, 1]
        if kind == "rqi":
            assert result[-1]["out_of_range"] == 1
            assert result[-1]["valid_zero"] == 1
            assert result[-1]["missing_sentinel"] == 1
            assert result[-1]["no_coverage_sentinel"] == 1
            assert result[-1]["bitmap_gaps"] == 1
            assert result[2][0, 0] == 0
        else:
            assert result[-1]["out_of_range"] == 0
            assert result[-1]["valid_negative"] == 1


def test_month_summary_readback_and_plot_labels(tmp_path, monkeypatch):
    from ecore_weather import maps
    monkeypatch.setattr(maps, "add_context", lambda ax, bbox, **kwargs: None)
    latitude = np.linspace(24.995, 10.005, 6)
    longitude = np.linspace(270.005, 299.995, 7)
    field = np.tile(np.linspace(0, 1, 7), (6, 1))
    counts = {"latitude": latitude, "longitude": longitude,
              "rqi_sum": field, "rqi_count": np.ones((6, 7), dtype="uint32"),
              "reflectivity_count": np.tile(np.arange(7), (6, 1)).astype("uint32")}
    metadata = {"selection_sha256": "sample", "year": 2022,
                "months_completed": [f"2022-{m:02d}" for m in range(1, 13)],
                "rqi_reads": 1, "reflectivity_reads": 20}
    summary = tmp_path / "summary.npz"
    figures._save_summary(summary, counts, metadata)
    loaded, recorded = figures.load_summary(summary)
    np.testing.assert_array_equal(loaded["rqi_sum"], field)
    assert recorded == metadata
    macro_png, macro_svg, regional_png, regional_svg = figures.plot_quality(summary, tmp_path)
    assert all(path.is_file() and path.stat().st_size > 0 for path in (macro_png, macro_svg, regional_png, regional_svg))
    macro, regional = macro_svg.read_text(), regional_svg.read_text()
    assert 'MRMS CARIB source grid' in macro and 'Illustrative training patch' not in macro
    assert 'MRMS CARIB source grid' not in regional and 'Illustrative training patch' in regional
    assert 'Approximate future region' in regional
    assert 'no cells' not in regional and 'Absent source hours' not in regional
    assert '#e69f00' in regional and '#9c179e' in regional


@pytest.mark.parametrize("year,period,label,month_count", [
    (2022, {}, "2022", 12),
    (2026, {"start": "2026-01-01", "end": "2026-07-01"}, "2026-h1", 6),
])
def test_annual_summary_resumes_after_only_completed_months(tmp_path, monkeypatch, year, period, label, month_count):
    selection = {"months": {f"{year}-{m:02d}": [] for m in range(1, month_count+1)}}
    def selected(location, requested_year, *args):
        assert requested_year == year
        return selection, "selection-hash"
    monkeypatch.setattr(figures, "build_selection", selected)

    class FakeTransport:
        bytes = requests = retries = 0

        def __init__(self, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    monkeypatch.setattr(figures, "Transport", FakeTransport)
    calls = []
    fail_once = True

    def accumulate(slots, counts, transport, workers, month):
        nonlocal fail_once
        calls.append(month)
        if month == f"{year}-02" and fail_once:
            fail_once = False
            raise IOError("temporary source failure")
        if counts is None:
            counts = {"latitude": np.array([18.]), "longitude": np.array([294.]),
                      "rqi_sum": np.zeros((1, 1)), "rqi_count": np.zeros((1, 1), dtype="uint32"),
                      "reflectivity_count": np.zeros((1, 1), dtype="uint32")}
        counts["rqi_sum"] += 1
        return counts, {"rqi": 1, "reflectivity": 1,
                        "pixel_counts": {"rqi": {"valid": 1}, "reflectivity": {"valid": 1}}}

    monkeypatch.setattr(figures, "_accumulate_month", accumulate)
    with pytest.raises(IOError, match="temporary"):
        figures.summarize_year(tmp_path, output=tmp_path, workers=1, **period)
    summary = tmp_path / f"mrms-radar-{label}" / "annual-summary.npz"
    _, first = figures.load_summary(summary)
    assert first["months_completed"] == [f"{year}-01"]
    _, _, final = figures.summarize_year(tmp_path, output=tmp_path, workers=1, **period)
    assert final["year"] == year
    assert len(final["months_completed"]) == month_count
    assert calls.count(f"{year}-01") == 1


def test_explicit_period_and_leap_year():
    begin,end,label=figures._interval(2026,'2026-01-01','2026-07-01')
    assert label=='2026-h1'
    assert len(list(figures._period_months(begin,end)))==6
    begin,end,_=figures._interval(2024)
    assert (end-begin).total_seconds()/3600==8784
    with pytest.raises(ValueError):figures._interval(2026,'2026-01-01',None)


def test_pilot_does_not_require_unrequested_future_months(tmp_path, monkeypatch):
    import json
    folder = tmp_path / "mrms" / figures.REFLECTIVITY / "roi-test" / "2026" / "01"
    folder.mkdir(parents=True)
    (folder / "raw.zarr.zip").write_bytes(b"x")
    (folder / "complete.json").write_text(json.dumps(dict(
        source="mrms", product=figures.REFLECTIVITY, region=list(figures.PR_BBOX),
        archive_sha256="test", stored_bytes=1, assets=[])))
    monkeypatch.setattr(figures, "_month_rqi", lambda *args: {})
    with pytest.raises(ValueError, match="Pilot day has no matching"):
        figures.pilot_day(tmp_path, "2026-01-01", output=tmp_path)
