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


def test_unlimited_time_is_local_shared_only(tmp_path,monkeypatch):
    import pytest
    from ecore_weather import jobs_goes
    args=['--start','2023-03-01','--end','2023-04-01','--max-hours','0',
          '--dry-run','--output',str(tmp_path),'--destination',str(tmp_path)]
    monkeypatch.delenv('PBS_JOBID',raising=False)
    assert jobs_goes.main(args)==0
    monkeypatch.setenv('PBS_JOBID','123.polaris')
    with pytest.raises(SystemExit):jobs_goes.main(args)
    monkeypatch.delenv('PBS_JOBID')
    with pytest.raises(SystemExit):jobs_goes.main([*args,'--pipeline','legacy'])


def test_unlimited_shared_run_has_no_deadline(tmp_path,monkeypatch):
    from ecore_weather import jobs_goes,jobs_mrms,goes_shared
    monkeypatch.delenv('PBS_JOBID',raising=False)
    monkeypatch.setattr(jobs_mrms,'preflight',lambda _:None)
    seen=[]
    def fetch(*args,**kwargs):
        seen.append(kwargs['deadline'])
        assert args[5]() is False
        return {'paused':False,'global_metrics':{}}
    monkeypatch.setattr(goes_shared,'fetch_contexts',fetch)
    assert jobs_goes.main(['--start','2023-03-01','--end','2023-04-01',
        '--output',str(tmp_path),'--destination',str(tmp_path),
        '--scratch',str(tmp_path/'scratch'),'--max-hours','0'])==0
    assert seen==[None]


def test_documented_empty_selection_reuses_unavailable_month():
    from ecore_weather.jobs_goes import checkpoint_matches,BANDS
    month={'start':'2023-03-01','end_excluded':'2023-04-01'}
    saved={**month,'status':'unavailable','product':'ABI-L2-CMIPF','bands':BANDS,
           'selected_assets':0,'archives':[],'coverage':[{'files':0}]}
    assert checkpoint_matches(saved,'ABI-L2-CMIPF',month)
    saved['coverage']=[]
    assert not checkpoint_matches(saved,'ABI-L2-CMIPF',month)
