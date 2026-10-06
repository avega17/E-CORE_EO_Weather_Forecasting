"""Tests for resumable study-run checkpoint verification."""

import json

from ecore_weather.jobs_goes import _verify_month


def test_verify_month_finds_product_nested_goes_report(tmp_path):
    report_dir = tmp_path / "2026-01"
    product_dir = report_dir / "ABI-L2-CMIPF"
    archive_dir = tmp_path / "dataset" / "C01" / "2026" / "01"
    product_dir.mkdir(parents=True)
    archive_dir.mkdir(parents=True)
    archive = archive_dir / "raw.zarr.zip"
    archive.write_bytes(b"verified test archive")
    (archive_dir / "complete.json").write_text(json.dumps({"observations": 2}))
    report = {
        "source": "goes",
        "interrupted": False,
        "selection_id": "selection-id",
        "records": [{"band": 1}, {"band": 1}],
        "monthly_archives": [{
            "path": str(archive), "band": 1, "status": "saved",
            "observations": 2, "stored_bytes": len(b"verified test archive"),
        }],
    }
    (product_dir / "selection-id-monthly.json").write_text(json.dumps(report))

    verified, archives, bands = _verify_month(report_dir)

    assert verified["selection_id"] == "selection-id"
    assert len(archives) == 1
    assert archives[0]["path"] == str(archive)
    assert bands == [1]


def test_operational_handoff_keeps_satellite_stores_separate():
    from ecore_weather.jobs_goes import satellite_segments
    month={'start':'2025-04-01','end_excluded':'2025-05-01','report_dir':'results/april',
        'command':['python','02_goes.py','--start','2025-04-01','--end','2025-05-01','--output','results/april','--satellite','auto']}
    segments=satellite_segments(month)
    assert len(segments)==2 and segments[0]['end_excluded']==segments[1]['start']=='2025-04-07T15:00:00Z'
    assert segments[0]['command'][-1]=='16' and segments[1]['command'][-1]=='19'
    assert segments[0]['report_dir']!=segments[1]['report_dir']
    assert month['command'][-1]=='auto'
