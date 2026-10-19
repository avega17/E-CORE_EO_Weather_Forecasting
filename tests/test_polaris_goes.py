import json
import os
from pathlib import Path
import subprocess

import pytest

from ecore_weather import jobs_goes, goes_limits
from ecore_weather.common import Asset
from ecore_weather.pbs import interval


@pytest.fixture(autouse=True)
def isolate_limits(monkeypatch):
    monkeypatch.delenv('ECORE_GOES_LIMIT_LEDGER', raising=False)
    monkeypatch.delenv('ECORE_GOES_ACCOUNTING_ROOTS', raising=False)


@pytest.mark.parametrize('begin,end', [('01-01-2021','2023-01-01'),
    ('2021-01-01','2021-01-01'), ('2022-01-01','2021-01-01'),
    ('2021-01-01T12:00:00-04:00','2023-01-01'), ('2021-02-30','2023-01-01')])
def test_iso_dates_reject_ambiguous_invalid_or_non_utc(begin,end):
    with pytest.raises(ValueError):interval(begin,end)


def test_cli_clips_partial_months_and_excludes_2023(tmp_path,capsys):
    assert jobs_goes.main(['--start','2021-01-19T12:00:00Z','--end','2023-01-01',
                           '--destination',str(tmp_path),'--dry-run'])==0
    plan=json.loads(capsys.readouterr().out)['monthly_runs']
    assert len(plan)==24
    assert plan[0]['start']=='2021-01-19T12:00:00+00:00'
    assert plan[-1]['start'].startswith('2022-12-01')
    assert plan[-1]['end_excluded'].startswith('2023-01-01')
    assert all(a['end_excluded']==b['start'] for a,b in zip(plan,plan[1:]))


@pytest.mark.parametrize('arguments', [['--start','2021-01-01'],
    ['--start','2021-01-01','--end','2023-01-01','--phase','h1-2021'],
    ['--start','2020-01-01','--end','2023-01-01']])
def test_cli_rejects_incomplete_or_conflicting_intervals(tmp_path,arguments):
    with pytest.raises(SystemExit):jobs_goes.main(arguments+['--dry-run','--destination',str(tmp_path)])


def test_physical_accounting_deduplicates_roots_and_hardlinks(tmp_path):
    root=tmp_path/'goes';root.mkdir();(root/'one').write_bytes(b'1234')
    os.link(root/'one',root/'two')
    assert goes_limits.physical_bytes([root,root])==4


def test_reservations_block_other_publishers_and_keep_old_data(tmp_path):
    dest=tmp_path/'data';state=tmp_path/'state';scratch=tmp_path/'scratch'
    target=dest/'goes'/'band';target.mkdir(parents=True);(target/'raw.zarr.zip').write_bytes(b'old')
    goes_limits.configure(dest,state,scratch,(goes_limits.STATE_ALLOWANCE+103)/1e9)
    with goes_limits.publication(target,70):
        with pytest.raises(goes_limits.IntentionalPause,match='storage cap'):
            with goes_limits.publication(dest/'goes'/'other',40):pass
    assert (target/'raw.zarr.zip').read_bytes()==b'old'
    with goes_limits.locked() as data:
        assert str(target.resolve()) not in data['reservations']


def test_admission_reserves_before_any_download(tmp_path):
    goes_limits.configure(tmp_path/'data',tmp_path/'state',tmp_path/'scratch',.6)
    assets=[Asset('noaa-goes16',str(i),50,'etag','2021-01-01') for i in range(100)]
    with pytest.raises(goes_limits.IntentionalPause):
        goes_limits.reserve_groups([((str(tmp_path/'data/goes/band'),2),assets)])
    with goes_limits.locked() as data:assert not data['reservations']


def test_proxy_factory_keeps_noaa_anonymous(monkeypatch):
    from obstore import store
    from ecore_weather.common import anonymous_noaa_store
    captured={}
    monkeypatch.setenv('HTTPS_PROXY','http://proxy.example:3128')
    monkeypatch.setattr(store,'S3Store',lambda bucket,**kw:captured.update(bucket=bucket,**kw))
    anonymous_noaa_store('noaa-goes16')
    assert captured=={'bucket':'noaa-goes16','region':'us-east-1','skip_signature':True,
                      'client_options':{'proxy_url':'http://proxy.example:3128'}}


def test_goes_profile_is_global_and_not_mrms_threads():
    profile=json.loads(Path('jobs/goes_polaris.json').read_text())
    config=profile['global_config']
    assert (config['month_writers'],config['tail_months'])==(4,1)
    assert (config['download_concurrency'],config['local_readers'],config['range_readers'])==(64,8,16)
    assert profile['job_memory_gib']==64


def test_pbs_scripts_parse_and_mrms_uses_six_hours():
    for path in ['jobs/polaris_goes.pbs','jobs/polaris_mrms.pbs']:
        subprocess.run(['bash','-n',path],check=True)
    script=Path('jobs/polaris_mrms.pbs').read_text()
    assert '#PBS -l walltime=06:00:00' in script
    assert 'PBS_MRMS_START' in script and 'start_date=2021-01-01' not in script


def test_checkpoint_dates_checked_before_opening_archives():
    saved={'status':'complete','archives':[{'path':'not-read'}],
        'product':'ABI-L2-CMIPF','start':'2021-01-01','end_excluded':'2021-02-01',
        'bands':jobs_goes.BANDS}
    assert not jobs_goes.checkpoint_matches(saved,'ABI-L2-CMIPF',
        {'start':'2021-01-02','end_excluded':'2021-02-01'})


@pytest.mark.parametrize('source,mode,start,end', [
    ('goes','study','2021-01-01','2023-01-01'),
    ('mrms','study','2021-01-01','2026-07-01'),
    ('mrms','pilot','2022-09-18T12:00:00Z','2022-09-18T12:40:00Z')])
def test_mocked_pbs_passes_dates_and_deadline_without_fetch(tmp_path,source,mode,start,end):
    import sys
    repo=Path.cwd();bin_dir=tmp_path/'bin';bin_dir.mkdir()
    def executable(name,body):
        p=bin_dir/name;p.write_text('#!/bin/bash\n'+body+'\n');p.chmod(0o755);return p
    executable('git','if test "$1" = rev-parse; then echo pinned; fi')
    executable('qstat','echo "Resource_List.walltime = 06:00:00"')
    executable('timeout','exec "$@"')  # Replaced below to consume timeout switches.
    executable('timeout','shift 3; exec "$@"')
    executable('mpiexec','printf "%s\\n" "$@" > "$CAPTURE"')
    python=executable('python',f'if test "$1" = -; then cat >/dev/null; exit 0; fi\nexec {sys.executable} "$@"')
    env={**os.environ,'PATH':str(bin_dir)+os.pathsep+os.environ['PATH'],
         'PYTHONPATH':str(repo/'src'),'PBS_JOBID':'mock.1','PBS_O_WORKDIR':str(repo),
         'ECORE_PROJECT_ROOT':str(tmp_path/'project'),'ECORE_PYTHON':str(python),
         'ECORE_REVISION':'pinned','ECORE_QUOTA_FREE_GIB':'800','ECORE_PBS_MODE':mode,
         'ECORE_NODE_SCRATCH':str(tmp_path/'scratch'),'CAPTURE':str(tmp_path/'args'),
         f'PBS_{source.upper()}_START':start,f'PBS_{source.upper()}_END':end}
    # Wrappers intentionally reject dirty tracked files; the git stub isolates
    # this shell-interface test from the developer checkout's current edits.
    (tmp_path/'project').mkdir()
    subprocess.run(['bash',f'jobs/polaris_{source}.pbs'],env=env,check=True,capture_output=True,text=True)
    args=(tmp_path/'args').read_text().splitlines()
    assert args[args.index('--start')+1].startswith(start[:10])
    assert args[args.index('--end')+1].startswith(end[:10])
    hours=float(args[args.index('--max-hours')+1])
    assert 5<hours<6
    if source=='goes':assert args[args.index('--storage-cap-gb')+1]=='400'


def test_deadline_reached_before_admission_does_not_load_sources(tmp_path):
    import time
    from ecore_weather.goes_shared import fetch_contexts
    def requests():
        pytest.fail('Expired job must not discover new data')
        yield
    with pytest.raises(goes_limits.IntentionalPause,match='wall-time'):
        fetch_contexts(requests(),str(tmp_path),scratch=tmp_path,index_results=False,
                       deadline=time.monotonic()-1)


def test_manual_pilot_four_contexts_reuses_and_cleans_own_archives(tmp_path,monkeypatch):
    import hashlib
    from ecore_weather import goes_polaris,goes,jobs_mrms
    from ecore_weather.common import Selection
    from ecore_weather.goes_shared import SharedConfig
    discovered=[]
    def discover(start,end,**kwargs):
        discovered.append((start,end))
        assets=[Asset('noaa-goes16',f'OR_ABI-L2-CMIPF-M6C{b:02}_G16_{i}.nc',10,'etag',start)
                for b in goes.STORMSCOPE_BANDS for i in range(8)]
        return Selection('goes','ABI-L2-CMIPF',start,end,(-70.24,14.36,-62.56,22.04),
                         assets,satellite=16,bands=goes.STORMSCOPE_BANDS)
    monkeypatch.setattr(goes,'discover',discover)
    monkeypatch.setattr(jobs_mrms,'preflight',lambda _:None)
    monkeypatch.setattr(goes_polaris,'closed_snapshot',lambda *_:None)
    calls=[]
    def fetch(requests,destination,config,scratch,*args,**kwargs):
        calls.append(len(requests));reports=[]
        assert len(requests)==4 and config.month_writers==4
        for key,selection,_ in requests:
            archives=[]
            for band in goes.STORMSCOPE_BANDS:
                folder=Path(destination)/key/str(band);folder.mkdir(parents=True,exist_ok=True)
                archive=folder/'raw.zarr.zip';archive.write_bytes(b'checked')
                (folder/'complete.json').write_text(json.dumps({'archive_sha256':hashlib.sha256(b'checked').hexdigest()}))
                archives.append({'path':str(archive),'band':band,'month':selection.start[:7],
                    'status':'saved' if len(calls)==1 else 'reused','observations':8,'stored_bytes':7})
            reports.append({'monthly_archives':archives})
        return {'reports':reports,'wall_s':.1,'global_metrics':{}}
    monkeypatch.setattr(goes_polaris,'fetch_contexts',fetch)
    destination=tmp_path/'pilot';output=tmp_path/'state'
    assert goes_polaris.main(['--start','2021-01-01T12:00:00Z','--end','2021-01-01T13:20:00Z',
        '--shared-profile','jobs/goes_polaris.json','--destination',str(destination),
        '--scratch',str(tmp_path/'scratch'),'--output',str(output)])==0
    assert [start[5:7] for start,end in discovered]==['01','04','07','10']
    assert calls==[4,4] and not list(destination.rglob('raw.zarr.zip'))
    report=json.loads((output/'pilot-summary.json').read_text())
    assert report['verified_observations']==256 and report['completed_reuse_no_downloads']
    assert len(report['selections'])==4


def test_progress_snapshot_survives_index_lock_and_is_serialized(tmp_path,monkeypatch):
    import threading,time
    from concurrent.futures import ThreadPoolExecutor
    from ecore_weather import jobs,runlog
    path=tmp_path/'progress.json'
    monkeypatch.setattr(jobs,'status',lambda *_:{'status':'index_unavailable'})
    path.write_text('{"old": true}')
    runlog.publish_progress('goes',path)
    assert json.loads(path.read_text())=={'old':True}
    active=peak=0
    def status(*args):
        nonlocal active,peak
        active+=1;peak=max(active,peak);time.sleep(.005);active-=1
        return {'runs':[],'tasks':[]}
    monkeypatch.setattr(jobs,'status',status)
    with ThreadPoolExecutor(4) as pool:list(pool.map(lambda _:runlog.publish_progress('goes',path),range(8)))
    assert peak==1 and json.loads(path.read_text())['source']=='goes'


def test_initial_cap_pause_returns_75_without_discovery(tmp_path,monkeypatch):
    from ecore_weather import jobs_mrms,goes
    monkeypatch.setattr(jobs_mrms,'preflight',lambda _:None)
    monkeypatch.setattr(goes,'discover',lambda *a,**kw:pytest.fail('No source reads after cap failure'))
    assert jobs_goes.main(['--start','2021-01-01','--end','2021-02-01',
        '--destination',str(tmp_path),'--output',str(tmp_path/'state'),
        '--scratch',str(tmp_path/'scratch'),'--storage-cap-gb','.01'])==75
    assert json.loads((tmp_path/'state/paused.json').read_text())['status']=='paused'


@pytest.mark.parametrize('extra',[['--pipeline','legacy'],['--product','ABI-L2-MCMIPF']])
def test_cap_cannot_be_silently_ignored_by_other_pipelines(tmp_path,extra):
    with pytest.raises(SystemExit):
        jobs_goes.main(['--destination',str(tmp_path),'--storage-cap-gb','400','--dry-run',*extra])
