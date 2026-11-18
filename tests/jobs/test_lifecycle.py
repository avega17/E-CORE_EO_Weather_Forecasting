from __future__ import annotations
import pytest

import json
from pathlib import Path

from ecore_weather.jobs.jobs_cleanup import prune_current_studies


def write(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value))


def test_cleanup_preserves_live_scratch_snapshot_and_research(tmp_path,monkeypatch):
    monkeypatch.chdir(tmp_path)
    results=tmp_path/'results';research=tmp_path/'research'
    (research/'archive.zip').parent.mkdir();(research/'archive.zip').write_bytes(b'data')
    write(results/'study-goes-native/launch.json',{'snapshot':str(results/'study-code-snapshots/current')})
    write(results/'study-code-snapshots/current/snapshot.json',{})
    write(results/'study-code-snapshots/old/snapshot.json',{})
    write(results/'study-scratch/current/progress.json',{'next_index':8})
    write(results/'study-goes-old/smoke.json',{})
    write(results/'runs/older/run.json',{'status':'complete','started_at':1,'config':{'source':'goes'}})
    write(results/'runs/latest/run.json',{'status':'complete','started_at':3,'config':{'source':'goes'}})
    write(results/'runs/unresolved/run.json',{'status':'failed','started_at':2,'config':{'source':'goes'}})
    preview=prune_current_studies(source_root=research)
    assert not preview['applied'] and (results/'study-goes-old').exists()
    applied=prune_current_studies(source_root=research,apply=True)
    assert applied['deleted_bytes']>0
    assert (results/'study-goes-old').exists()  # Unknown owners are protected, not guessed obsolete.
    assert not (results/'study-code-snapshots/old').exists()
    assert (results/'study-code-snapshots/current/snapshot.json').exists()
    assert (results/'study-scratch/current/progress.json').exists()
    assert (research/'archive.zip').read_bytes()==b'data'
    assert not (results/'runs/older').exists()
    assert (results/'runs/latest/run.json').is_file()
    assert (results/'runs/unresolved/run.json').is_file()


def test_only_completed_selection_copies_are_removed(tmp_path,monkeypatch):
    monkeypatch.chdir(tmp_path);results=tmp_path/'results';research=tmp_path/'research'
    archive=research/'month/raw.zarr.zip';archive.parent.mkdir(parents=True);archive.write_bytes(b'raw')
    write(archive.parent/'complete.json',{'archive_sha256':'verified'})
    write(results/'study-mrms/months/2021-01/ok.json',{'status':'complete','archives':[str(archive)]})
    write(results/'study-mrms/months/2021-01/ok-selection/items.json',{'items':[]})
    write(results/'study-mrms/months/2021-02/pending.json',{'status':'failed','archives':[]})
    write(results/'study-mrms/months/2021-02/pending-selection/items.json',{'items':[]})
    prune_current_studies(source_root=research,apply=True)
    assert not (results/'study-mrms/months/2021-01/ok-selection').exists()
    assert (results/'study-mrms/months/2021-01/ok.json').exists()
    assert (results/'study-mrms/months/2021-02/pending-selection/items.json').exists()


def test_annual_working_copy_removed_only_with_retained_originals(tmp_path,monkeypatch):
    monkeypatch.chdir(tmp_path);results=tmp_path/'results';research=tmp_path/'research'
    archive=research/'month/raw.zarr.zip';archive.parent.mkdir(parents=True);archive.write_bytes(b'raw')
    write(archive.parent/'complete.json',{'archive_sha256':'verified'})
    annual=results/'mirror-mrms-yearly/2021/product-2021.zip';annual.parent.mkdir(parents=True);annual.write_bytes(b'copy')
    write(annual.with_name('product-local-bundle.json'),{'size':4,'archives':[{'local_path':'month/raw.zarr.zip','stored_bytes':3,'sha256':'verified'}]})
    prune_current_studies(source_root=research,apply=True)
    assert not annual.exists()
    assert (results/'study-mrms/backups/2021/product-local-bundle.json').exists()
    assert archive.exists()






"""Tests for resumable study-run checkpoint verification."""

import json

from ecore_weather.jobs.jobs_goes import _verify_month


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
    from ecore_weather.jobs.jobs_goes import satellite_segments
    month={'start':'2025-04-01','end_excluded':'2025-05-01','report_dir':'results/april',
        'command':['python','02_goes.py','--start','2025-04-01','--end','2025-05-01','--output','results/april','--satellite','auto']}
    segments=satellite_segments(month)
    assert len(segments)==2 and segments[0]['end_excluded']==segments[1]['start']=='2025-04-07T15:00:00Z'
    assert segments[0]['command'][-1]=='16' and segments[1]['command'][-1]=='19'
    assert segments[0]['report_dir']!=segments[1]['report_dir']
    assert month['command'][-1]=='auto'


def test_unlimited_time_is_local_shared_only(tmp_path,monkeypatch):
    import pytest
    from ecore_weather.jobs import jobs_goes
    args=['--start','2023-03-01','--end','2023-04-01','--max-hours','0',
          '--dry-run','--output',str(tmp_path),'--destination',str(tmp_path)]
    monkeypatch.delenv('PBS_JOBID',raising=False)
    assert jobs_goes.main(args)==0
    monkeypatch.setenv('PBS_JOBID','123.polaris')
    with pytest.raises(SystemExit):jobs_goes.main(args)
    monkeypatch.delenv('PBS_JOBID')
    with pytest.raises(SystemExit):jobs_goes.main([*args,'--pipeline','legacy'])


def test_unlimited_shared_run_has_no_deadline(tmp_path,monkeypatch):
    from ecore_weather.jobs import jobs_goes
    from ecore_weather.jobs import jobs_mrms
    from ecore_weather.ingest import goes_shared
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
    from ecore_weather.jobs.jobs_goes import checkpoint_matches,BANDS
    month={'start':'2023-03-01','end_excluded':'2023-04-01'}
    saved={**month,'status':'unavailable','product':'ABI-L2-CMIPF','bands':BANDS,
           'selected_assets':0,'archives':[],'coverage':[{'files':0}]}
    assert checkpoint_matches(saved,'ABI-L2-CMIPF',month)
    saved['coverage']=[]
    assert not checkpoint_matches(saved,'ABI-L2-CMIPF',month)


def test_portable_receipts_read_old_files_and_reuse_journal(tmp_path):
    from ecore_weather.core.paths import ReceiptPath, store_receipt
    old=tmp_path/'checkpoints/h1-2021/2021-01-01.json'
    write(old,{'status':'complete','observations':8})
    assert ReceiptPath(old).is_file()
    assert store_receipt(old,{'status':'complete','observations':16})
    assert not old.is_file()
    assert json.loads(ReceiptPath(old).read_text())['observations']==16
    assert store_receipt(tmp_path/'months/2021-02/radar.json',{'status':'unavailable'})
    assert len(json.loads((tmp_path/'study.json').read_text())['tasks'])==2


def test_report_metadata_never_requires_git(monkeypatch):
    from ecore_weather.reporting import dataset_report
    original=dataset_report.subprocess.check_output
    def check(command,*args,**kwargs):
        if command and command[0]=='git':pytest.fail('Git is not required')
        return original(command,*args,**kwargs)
    monkeypatch.setattr(dataset_report.subprocess,'check_output',check)
    assert dataset_report.provenance()['algorithm_version']=='native-quality-v1'


def test_default_output_paths_do_not_follow_notebook_cwd(tmp_path,monkeypatch):
    from ecore_weather.core.paths import result_path
    monkeypatch.setenv('ECORE_REPO_ROOT',str(tmp_path))
    notebooks=tmp_path/'notebooks';notebooks.mkdir();monkeypatch.chdir(notebooks)
    assert result_path('dataset-report')==tmp_path/'results/dataset-report'


def test_cleanup_preview_does_not_create_evidence(tmp_path):
    from ecore_weather.jobs.jobs_cleanup import execute
    results=tmp_path/'results';results.mkdir()
    write(results/'report.json',{'status':'complete','read_bytes':10})
    before={p.relative_to(results):p.read_bytes() for p in results.rglob('*') if p.is_file()}
    execute(results,source_root=tmp_path/'research',apply=False)
    assert before=={p.relative_to(results):p.read_bytes() for p in results.rglob('*') if p.is_file()}


def test_cleanup_preview_does_not_contact_backup_storage(tmp_path,monkeypatch):
    from ecore_weather.jobs.jobs_cleanup import execute
    from ecore_weather.storage import hf_storage
    root=tmp_path/'results';work=root/'mirror-mrms-yearly/2021/year.zip'
    work.parent.mkdir(parents=True);work.write_bytes(b'pending backup')
    monkeypatch.setattr(hf_storage,'s3_client',lambda *a,**kw:pytest.fail('No credentials or network in preview'))
    preview=execute(root,apply=False)
    assert preview['protected'] and work.read_bytes()==b'pending backup'
