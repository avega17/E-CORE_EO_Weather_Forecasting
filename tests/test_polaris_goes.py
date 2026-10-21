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


@pytest.fixture
def pbs_environment(tmp_path):
    import sys
    repo=Path.cwd();bin_dir=tmp_path/'bin';bin_dir.mkdir()
    def executable(name,body):
        p=bin_dir/name;p.write_text('#!/bin/bash\n'+body+'\n');p.chmod(0o755);return p
    # Any Git call is a regression, including with missing/mismatched revision
    # metadata or tracked edits in the actual developer checkout.
    executable('git','printf "Git must not be called\\n" >&2; exit 99')
    executable('qstat','printf "Resource_List.walltime = %s\\n" "${MOCK_WALLTIME-06:00:00}"')
    executable('timeout','shift 3; exec "$@"')
    executable('mpiexec','printf "%s\\n" "$@" > "$CAPTURE"; exit "${MOCK_APPLICATION_EXIT:-0}"')
    python=executable('python',f'if test "$1" = -; then cat >/dev/null; exit "${{MOCK_STORAGE_EXIT:-0}}"; fi\n'
                      'if test "$1" = -m && test "${MOCK_IMPORT_FAILURE:-0}" = 1; then '
                      'printf "Simulated import failure\\n" >&2; exit 42; fi\n'
                      f'exec {sys.executable} "$@"')
    env={**os.environ,'PATH':str(bin_dir)+os.pathsep+os.environ['PATH'],
         'PYTHONPATH':str(repo/'src'),'PBS_JOBID':'mock.1','PBS_O_WORKDIR':str(repo),
         'ECORE_PROJECT_ROOT':str(tmp_path/'project'),'ECORE_PYTHON':str(python),
         'ECORE_QUOTA_FREE_GIB':'800','ECORE_PBS_MODE':'study',
         'ECORE_NODE_SCRATCH':str(tmp_path/'scratch'),'CAPTURE':str(tmp_path/'args')}
    for name in ('ECORE_REVISION','ECORE_CODE_ROOT','ECORE_RECORD_CODE_IDENTITY'):
        env.pop(name,None)
    for source in ('GOES','MRMS'):
        env[f'PBS_{source}_START']='2021-01-01'
        env[f'PBS_{source}_END']='2021-01-02'
    (tmp_path/'project').mkdir()
    return env


@pytest.mark.parametrize('source,mode,start,end', [
    ('goes','study','2021-01-01','2023-01-01'),
    ('mrms','study','2021-01-01','2026-07-01'),
    ('mrms','pilot','2022-09-18T12:00:00Z','2022-09-18T12:40:00Z')])
def test_mocked_pbs_passes_dates_and_deadline_without_fetch(tmp_path,pbs_environment,source,mode,start,end):
    env={**pbs_environment,'ECORE_PBS_MODE':mode,
         f'PBS_{source.upper()}_START':start,f'PBS_{source.upper()}_END':end}
    subprocess.run(['bash',f'jobs/polaris_{source}.pbs'],env=env,check=True,capture_output=True,text=True)
    args=(tmp_path/'args').read_text().splitlines()
    assert args[args.index('--start')+1].startswith(start[:10])
    assert args[args.index('--end')+1].startswith(end[:10])
    hours=float(args[args.index('--max-hours')+1])
    assert 5<hours<6
    if source=='goes':assert args[args.index('--storage-cap-gb')+1]=='400'


@pytest.mark.parametrize('source',['goes','mrms'])
@pytest.mark.parametrize('revision',[None,'unrelated-revision'])
def test_pbs_has_no_git_or_code_revision_gate(pbs_environment,source,revision):
    env=dict(pbs_environment)
    if revision is not None:env['ECORE_REVISION']=revision
    result=subprocess.run(['bash',f'jobs/polaris_{source}.pbs'],env=env,capture_output=True,text=True)
    assert result.returncode==0,result.stderr
    assert 'wrapper entered' in result.stdout
    assert 'Git must not be called' not in result.stderr


@pytest.mark.parametrize('source',['goes','mrms'])
@pytest.mark.parametrize('failure,expected',[
    ('missing-variable','ECORE_QUOTA_FREE_GIB'),
    ('missing-dates','END'),
    ('bad-dates','End must be later'),
    ('missing-python','Python is not executable'),
    ('import','Simulated import failure'),
    ('storage','storage-preflight'),
    ('missing-walltime','valid Resource_List.walltime'),
    ('short-walltime','safe shutdown'),
    ('profile','profile is not readable')])
def test_pbs_startup_failures_are_visible(tmp_path,pbs_environment,source,failure,expected):
    env=dict(pbs_environment)
    if failure=='missing-variable':env.pop('ECORE_QUOTA_FREE_GIB')
    elif failure=='missing-dates':env.pop(f'PBS_{source.upper()}_END')
    elif failure=='bad-dates':env[f'PBS_{source.upper()}_END']='2020-01-01'
    elif failure=='missing-python':env['ECORE_PYTHON']=str(tmp_path/'absent-python')
    elif failure=='import':env['MOCK_IMPORT_FAILURE']='1'
    elif failure=='storage':env['MOCK_STORAGE_EXIT']='19'
    elif failure=='missing-walltime':env['MOCK_WALLTIME']=''
    elif failure=='short-walltime':env['MOCK_WALLTIME']='00:02:00'
    elif failure=='profile':
        if source=='mrms':pytest.skip('MRMS validates its profile in the application')
        env['ECORE_PROFILE']=str(tmp_path/'missing-profile')
    result=subprocess.run(['bash',f'jobs/polaris_{source}.pbs'],env=env,capture_output=True,text=True)
    assert result.returncode!=0
    assert 'wrapper entered' in result.stdout
    assert expected in result.stdout+result.stderr
    assert f'exit {result.returncode}' in result.stderr
    assert not Path(env['CAPTURE']).exists(), 'Do not launch application after failed preflight'


@pytest.mark.parametrize('source',['goes','mrms'])
@pytest.mark.parametrize('code',[0,1,75,124])
def test_pbs_preserves_status_and_one_small_startup_record(pbs_environment,source,code):
    env={**pbs_environment,'MOCK_APPLICATION_EXIT':str(code),'UNRELATED_SECRET':'never-print-this-value'}
    script=f'jobs/polaris_{source}.pbs'
    root=Path(env['ECORE_PROJECT_ROOT'])/'ecore-weather/state'
    log=root/('goes/study' if source=='goes' else 'study')/'startup.log'
    log.parent.mkdir(parents=True);log.write_text('OLD STARTUP\n')
    result=subprocess.run(['bash',script],env=env,capture_output=True,text=True)
    assert result.returncode==code
    assert f'exit {code}' in result.stderr
    assert 'OLD STARTUP' not in log.read_text()
    assert log.stat().st_size<4096
    assert 'never-print-this-value' not in result.stdout+result.stderr+log.read_text()
    if code==75:
        assert 'intentionally paused' in result.stdout
        assert 'application failed' not in result.stdout


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
