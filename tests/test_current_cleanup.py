import json
from pathlib import Path

from ecore_weather.jobs_cleanup import prune_current_studies


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
    preview=prune_current_studies(source_root=research)
    assert not preview['applied'] and (results/'study-goes-old').exists()
    applied=prune_current_studies(source_root=research,apply=True)
    assert applied['deleted_bytes']>0
    assert not (results/'study-goes-old').exists()
    assert not (results/'study-code-snapshots/old').exists()
    assert (results/'study-code-snapshots/current/snapshot.json').exists()
    assert (results/'study-scratch/current/progress.json').exists()
    assert (research/'archive.zip').read_bytes()==b'data'


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


def test_waiting_handoff_pins_profile_before_drain(tmp_path,monkeypatch):
    from ecore_weather import goes_rollout,jobs_snapshot
    monkeypatch.chdir(tmp_path)
    profile=tmp_path/'jobs/goes_workstation.json';value={'pipeline':'shared','global_config':{'month_writers':2,'tail_months':0}}
    write(profile,value)
    frozen=tmp_path/'results/study-code-snapshots/frozen';write(frozen/'jobs/goes_workstation.json',value)
    snapshot={'snapshot':str(frozen),'sha256':'fixed'}
    def freeze(argv):print(json.dumps(snapshot))
    monkeypatch.setattr(jobs_snapshot,'main',freeze)
    def drain(output):
        # Workspace changes after pinning cannot change the launch profile.
        write(profile,{'changed':True})
    monkeypatch.setattr(goes_rollout,'wait_for_pause',drain)
    calls=[]
    def launch(profile_path,output,scratch,**kwargs):
        calls.append((profile_path,kwargs));assert json.loads(Path(profile_path).read_text())==value
        return {'status':'launched'}
    monkeypatch.setattr(goes_rollout,'launch',launch)
    assert goes_rollout.main(['--wait-for-pause'])==0
    assert calls[0][1]['snapshot_record']==snapshot
    assert json.loads(Path('results/study-goes-native/handoff/replacement.json').read_text())['sha256']=='fixed'


def test_handoff_mrms_benchmark_runs_after_pause_before_launch(tmp_path, monkeypatch):
    from ecore_weather import goes_rollout, jobs_snapshot, jobs_policy
    monkeypatch.chdir(tmp_path)
    profile = tmp_path/'jobs/goes_workstation.json'
    value = {'pipeline': 'shared', 'global_config': {'month_writers': 2, 'tail_months': 1}}
    write(profile, value)
    frozen = tmp_path/'results/study-code-snapshots/frozen'
    write(frozen/'jobs/goes_workstation.json', value)
    order = []
    monkeypatch.setattr(jobs_snapshot, 'main', lambda args: print(json.dumps({'snapshot': str(frozen), 'sha256': 'fixed'})))
    monkeypatch.setattr(goes_rollout, 'wait_for_pause', lambda path: order.append('verified-pause'))
    monkeypatch.setattr(jobs_policy, 'ensure_paused', lambda: order.append('no-fetch'))
    def benchmark(command, check):
        assert command[1] == str(frozen/'scripts/dataset_jobs.py')
        assert command[2:4] == ['benchmark', 'mrms'] and check
        order.append('benchmark')
    monkeypatch.setattr(goes_rollout.subprocess, 'run', benchmark)
    monkeypatch.setattr(goes_rollout, 'launch', lambda *a, **kw: order.append('launch') or {'status': 'launched'})
    assert goes_rollout.main(['--wait-for-pause', '--mrms-source-benchmark']) == 0
    assert order == ['verified-pause', 'no-fetch', 'benchmark', 'launch']
