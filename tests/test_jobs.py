import json
from pathlib import Path
import pytest
from ecore_weather.jobs_policy import ensure_paused,select_profiles
from ecore_weather import jobs,index


def test_notebook_cli_passes_json_band_profiles_to_fetch(tmp_path,monkeypatch):
    from ecore_weather import cli,catalog,monthly
    from ecore_weather.common import Asset,Selection
    selection=Selection('goes','ABI-L2-CMIPF','2021-05-01','2021-06-01',(-70,14,-62,22),
        [Asset('noaa-goes16','OR_ABI-L2-CMIPF-M6C02_G16.nc',1,'e','2021-05-01T00:00:00Z')],satellite=16,bands=(2,))
    profiles={'2':{'profile':{'read_mode':'range','read_processes':4,'workers':1}}}
    path=tmp_path/'profiles.json';path.write_text(json.dumps(profiles))
    monkeypatch.setattr(catalog,'load_selection',lambda _:selection)
    monkeypatch.setattr(catalog,'save_selection',lambda *a,**k:None)
    received=[]
    def fetch(*args,**kwargs):
        received.append(kwargs['read_profiles'])
        return {'records':[]}
    monkeypatch.setattr(monthly,'fetch',fetch)
    assert cli.main('goes',['--operation','fetch','--selection','saved.json','--read-profiles',str(path),'--pipeline','legacy',
        '--output',str(tmp_path/'run'),'--skip-index'])==0
    assert received==[profiles]


def test_requested_pause_preserves_exit_75_when_child_returns_failure(tmp_path,monkeypatch):
    from ecore_weather import jobs_goes
    monkeypatch.chdir(tmp_path)
    def interrupted(arguments):
        (tmp_path/'run'/'pause.request').write_text('intentional checkpoint pause')
        return 1
    monkeypatch.setattr(jobs_goes,'main',interrupted)
    assert jobs.run_fetch('goes',['--output',str(tmp_path/'run'),'--scratch',str(tmp_path/'scratch')],str(tmp_path/'db.duckdb'))==75
    row=json.loads(next((tmp_path/'results/runs').glob('*/run.json')).read_text())
    assert row['status']=='paused' and row['interrupted_exit_code']==1


def test_network_gate_rejects_live_fetch_not_stale_launcher():
    ensure_paused([{'pid':1,'cmdline':['python','scripts/dataset_jobs.py','status']}])
    with pytest.raises(RuntimeError,match='verified fetch pause'):
        ensure_paused([{'pid':2,'cmdline':['python','scripts/dataset_jobs.py','resume','goes']}])


def test_profile_selection_requires_both_satellites_and_native_bands():
    baseline={'read_mode':'async_full'};candidate={'read_mode':'async_pipeline'}
    rows=[{'stage':'confirm','repeat':r,'satellite':s,'profile':p,'values_equal':True,'peak_rss_bytes':1024**3,
        'monthly_metrics':[{'band':b,'status':'saved','write_seconds':100 if p==baseline or (s==19 and b==2) else 60}
                            for b in (1,2,3,7,8,9,10,13)]}
        for r in (1,2,3) for s in (16,19) for p in (baseline,candidate)]
    profiles=select_profiles(rows,baseline)
    assert profiles['13']['profile']==candidate and profiles['2']['profile']==baseline
    for row in rows:
        if row['profile']==candidate:row['peak_rss_bytes']=9*1024**3
    assert all(v['profile']==baseline for v in select_profiles(rows,baseline).values())
    with pytest.raises(ValueError):select_profiles([r for r in rows if r['repeat']!=3],baseline)


def test_new_index_rebuild_drops_asset_logs_preserves_run_and_discovery(tmp_path):
    import hashlib
    root=tmp_path/'data';folder=root/'mrms/PrecipRate_00.00/roi-test/2021/05';folder.mkdir(parents=True)
    archive=folder/'raw.zarr.zip';archive.write_bytes(b'archive')
    marker={'source':'mrms','product':'PrecipRate_00.00','raw_path':archive.name,'observations':1,
        'stored_bytes':7,'assets':[{'asset_id':'a','time':'2021-05-01T00:00:00Z','source_bytes':100}],
        'archive_sha256':hashlib.sha256(archive.read_bytes()).hexdigest()}
    (folder/'complete.json').write_text(json.dumps(marker))
    db=tmp_path/'index.duckdb'
    with index.connect(db) as con:
        con.execute('CREATE TABLE observations (asset_id VARCHAR)');con.execute("INSERT INTO observations VALUES ('a')")
    index.record_fetch({'source':'mrms','run_id':'run','selection_summary':{'product':'PrecipRate_00.00'},
        'records':[{'asset_id':'a'}],'monthly_archives':[{'path':str(archive),'observations':1,'stored_bytes':7}]},db)
    before=index.search(root,'mrms',path=db)
    index.rebuild([root,root],db)
    with index.connect(db,read_only=True) as con:
        assert 'observations' not in {r[0] for r in con.execute('SHOW TABLES').fetchall()}
        assert con.execute('SELECT count(*) FROM archives').fetchone()[0]==1
        report=json.loads(con.execute('SELECT report_json FROM fetch_runs').fetchone()[0])
        assert report['records_count']==1 and 'records' not in report
    assert index.search(root,'mrms',path=db)==before


def test_mrms_portable_defaults_and_index_path(capsys,tmp_path):
    assert jobs.main(['fetch','mrms','--dry-run','--index-path',str(tmp_path/'index.duckdb')])==0
    data=json.loads(capsys.readouterr().out)
    assert data['start']=='2021-01-01T00:00:00Z' and data['end_excluded']=='2026-07-01T00:00:00Z'
    assert (data['monthly_writers'],data['workers'],data['decode_workers'])==(4,8,1)


def test_mrms_partial_month_resume_keeps_bitmap_and_verified_batches(tmp_path,monkeypatch):
    import numpy as np
    import xarray as xr
    from ecore_weather import monthly_stream,mrms
    from ecore_weather.common import Asset,Selection
    from ecore_weather.storage import open_raw
    assets=[Asset('noaa-mrms-pds',f'{i}.grib2.gz',10,'etag',f'2021-01-01T{i:02}:00:00Z') for i in range(16)]
    selection=Selection('mrms',mrms.DEFAULT_PRODUCT,assets[0].time,'2021-02-01',(-70,14,-62,22),assets)
    calls=[];fail=[True]
    def reader(asset,*args):
        i=int(asset.key.split('.')[0]);calls.append(i)
        if fail[0] and i==9:raise OSError('interrupted source')
        ds=xr.Dataset({'measurement':(('latitude','longitude'),np.array([[float(i),-1]],dtype='float32')),
                       'bitmap_valid':(('latitude','longitude'),np.array([[1,0]],dtype='uint8'))},
                       coords={'latitude':[18.],'longitude':[-67.,-66.]},attrs={'product':mrms.DEFAULT_PRODUCT})
        return ds,{}
    monkeypatch.setattr(monthly_stream,'_read_new',reader)
    target=tmp_path/'month';scratch=tmp_path/'scratch'
    with pytest.raises(OSError,match='interrupted source'):
        monthly_stream._write_one(selection,assets,target,None,workers=1,scratch=scratch)
    assert next(scratch.glob('*/progress.json')).is_file()
    calls.clear();fail[0]=False
    actual=monthly_stream._write_one(selection,assets,target,None,workers=1,scratch=scratch)
    assert actual['resumed_observations']==8 and not any(i<8 for i in calls)
    with open_raw(actual['path']) as ds:
        np.testing.assert_array_equal(ds.measurement[:,0,0],np.arange(16,dtype='float32'))
        np.testing.assert_array_equal(ds.bitmap_valid[:,0,1],np.zeros(16,dtype='uint8'))
    assert not list(scratch.iterdir())


def test_band_profile_arguments_are_validated():
    from ecore_weather.jobs_policy import validate_profiles
    validate_profiles({'2':{'profile':{'read_mode':'range','read_processes':4,'workers':1}}})
    with pytest.raises(ValueError):validate_profiles({'2':{'profile':{'read_processes':0}}})
    with pytest.raises(ValueError):validate_profiles({'2':{'profile':{'interpolate':True}}})


def test_cleanup_requires_remote_receipt_and_unchanged_monthly_originals(tmp_path):
    import hashlib,io
    from ecore_weather.jobs_cleanup import yearly_working_copy
    archive=tmp_path/'product-2021-work.zip';archive.write_bytes(b'backup')
    monthly=tmp_path/'data/month/raw.zarr.zip';monthly.parent.mkdir(parents=True);monthly.write_bytes(b'raw')
    sha=hashlib.sha256(b'backup').hexdigest()
    local={'sha256':sha,'manifest_sha256':'manifest','archives':[{'local_path':'month/raw.zarr.zip','stored_bytes':3,'sha256':'original'}]}
    receipt={'status':'saved','readback_seconds':1,'archive_sha256':sha,'stored_bytes':6,'remote_key':'year/raw.zip'}
    archive.with_name('product-2021-local-bundle.json').write_text(json.dumps(local))
    archive.with_name('product-2021-remote-bundle.json').write_text(json.dumps(receipt))
    (monthly.parent/'complete.json').write_text(json.dumps({'archive_sha256':'original'}))
    class Client:
        def head_object(self,**kw):return {'ContentLength':6,'ETag':'etag'}
        def get_object(self,**kw):return {'Body':io.BytesIO(json.dumps({'archive_sha256':sha,'manifest_sha256':'manifest'}).encode())}
    assert yearly_working_copy(archive,tmp_path/'data',Client(),'bucket')['monthly_originals']==1
    (monthly.parent/'complete.json').write_text(json.dumps({'archive_sha256':'updated'}))
    with pytest.raises(ValueError,match='identity changed'):yearly_working_copy(archive,tmp_path/'data',Client(),'bucket')
    assert archive.exists() and monthly.exists()


def test_progress_survives_verified_scratch_removal(tmp_path,monkeypatch):
    from ecore_weather.runlog import Run,task_progress
    db=tmp_path/'db.duckdb';run=Run({'source':'goes'},tmp_path/'runs',db)
    scratch=tmp_path/'scratch/task';scratch.mkdir(parents=True)
    target=tmp_path/'data/month';target.mkdir(parents=True)
    progress={'source':'goes','target':str(target),'product':'ABI-L2-CMIPF','band':13,'month':'2021-05','selected':8,'next_index':8}
    (scratch/'progress.json').write_text(json.dumps(progress));task_progress(run,scratch.parent)
    (target/'raw.zarr.zip').write_bytes(b'archive')
    (target/'complete.json').write_text(json.dumps({'observations':8,'source':'goes','product':'ABI-L2-CMIPF','band':13}));(scratch/'progress.json').unlink()
    task_progress(run,scratch.parent)
    with index.connect(db,read_only=True) as con:
        assert con.execute('SELECT selected,checkpointed,completed,status FROM tasks').fetchone()==(8,8,8,'complete')


def test_band_profiles_route_readers_without_changing_writer_count(tmp_path,monkeypatch):
    from ecore_weather import monthly,monthly_stream
    from ecore_weather.common import Selection,Asset
    seen=[]
    def writer(selection,assets,target,band,*args):
        seen.append((band,args))
        return {'path':str(target),'status':'reused','observations':len(assets),'stored_bytes':10,'read_bytes':0}
    monkeypatch.setattr(monthly_stream,'_write_one',writer)
    assets=[Asset('noaa-goes16',f'OR_ABI-L2-CMIPF-M6C{band:02}_G16.nc',1,'etag','2021-01-01T00:00:00Z') for band in (1,2)]
    selection=Selection('goes','ABI-L2-CMIPF','2021-01-01','2021-02-01',(-70,14,-62,22),assets,satellite=16,bands=(1,2))
    profiles={'1':{'profile':{'read_mode':'async_pipeline','read_processes':2,'download_concurrency':16}},
              '2':{'profile':{'read_mode':'range','read_processes':4,'workers':1}}}
    result=monthly.fetch(selection,str(tmp_path),monthly_writers=1,index_results=False,report_dir=None,read_profiles=profiles)
    assert result['monthly_writers']==1
    assert seen[0][1][5]==2 and seen[0][1][7]=='async_pipeline' and seen[0][1][8]==16
    assert seen[1][1][0]==1 and seen[1][1][5]==4 and seen[1][1][7]=='range'


def test_existing_archive_is_not_completion_of_changed_selection(tmp_path):
    from ecore_weather.runlog import Run,task_progress
    db=tmp_path/'db.duckdb';run=Run({'source':'goes'},tmp_path/'runs',db)
    scratch=tmp_path/'scratch/task';scratch.mkdir(parents=True)
    target=tmp_path/'data/month';target.mkdir(parents=True)
    (target/'raw.zarr.zip').write_bytes(b'archive')
    (target/'complete.json').write_text(json.dumps({'observations':2,'asset_ids':['old','other']}))
    (scratch/'progress.json').write_text(json.dumps({'target':str(target),'asset_ids':['new','other'],'selected':2,'next_index':1}))
    task_progress(run,scratch.parent)
    with index.connect(db,read_only=True) as con:
        assert con.execute('SELECT completed,status FROM tasks').fetchone()==(0,'checkpointed')


def test_power_restart_replays_untrusted_tail_and_preserves_failure_evidence(tmp_path,monkeypatch):
    import zarr,numpy as np,hashlib
    from ecore_weather.checkpoint_recovery import recover
    monkeypatch.setenv('ECORE_RECOVERY_EVIDENCE',str(tmp_path/'evidence'))
    parent=tmp_path/'scratch';parent.mkdir()
    data=np.arange(16,dtype='int32').reshape(16,1,1)
    root=zarr.open_group(parent/'raw.zarr',mode='w');root.create_array('measurement',data=data,chunks=(1,1,1))
    rows=[{'asset_id':str(i)} for i in range(16)]
    batch={'rows':[{'index':i,'row':rows[i],'metadata':'{}','checksums':{'measurement':hashlib.sha256(data[i].tobytes()).hexdigest()}} for i in range(8)]}
    (parent/'batch-000000.json').write_text(json.dumps(batch));(parent/'batch-000008.json').write_text('')
    (parent/'progress.json').write_text(json.dumps({'next_index':16,'asset_ids':[r['asset_id'] for r in rows]}))
    recovered=recover(parent,rows)
    assert recovered['verified_prefix']==8 and recovered['replay_observations']==8
    assert Path(recovered['preserved_tail']).is_file() and not (parent/'batch-000008.json').exists()
    np.testing.assert_array_equal(root['measurement'][:],data)
    assert json.loads((parent/'progress.json').read_text())['next_index']==8
    assert recover(parent,rows) is None


def test_queue_counts_stac_and_maps_legacy_progress_to_durable_task(tmp_path):
    from ecore_weather.runlog import Run,queue_saved_selections,task_progress
    from ecore_weather.common import Asset,Selection
    from ecore_weather.catalog import save_selection
    assets=[Asset('noaa-goes16',f'OR_ABI-L2-CMIPF-M6C{b:02}_G16.nc',1,'e','2021-05-01T00:00:00Z') for b in (1,2)]
    selection=Selection('goes','ABI-L2-CMIPF','2021-05-01','2021-06-01',(-70,14,-62,22),assets,satellite=16,bands=(1,2))
    output=tmp_path/'output';save_selection(selection,output/'month/product',index_results=False)
    db=tmp_path/'index.duckdb';run=Run({'source':'goes'},tmp_path/'runs',db)
    queue_saved_selections(run,output,tmp_path/'data')
    folder=tmp_path/'scratch/legacy';folder.mkdir(parents=True)
    (folder/'progress.json').write_text(json.dumps({'asset_ids':[assets[0].id],'band':1,'product':selection.product,'next_index':1}))
    task_progress(run,folder.parent)
    with index.connect(db,read_only=True) as con:
        rows=con.execute('SELECT band,month,selected,checkpointed,status FROM tasks ORDER BY band').fetchall()
    assert rows==[(1,'2021-05',1,1,'checkpointed'),(2,'2021-05',1,0,'queued')]


def test_progress_inventory_resolves_destination_once_not_each_source(tmp_path,monkeypatch):
    from ecore_weather.runlog import Run,queue_saved_selections
    from ecore_weather.common import Asset,Selection
    from ecore_weather.catalog import save_selection
    assets=[Asset('noaa-goes16',f'OR_ABI-L2-CMIPF-M6C01_G16_{i}.nc',1,'e',f'2021-05-01T00:{i:02d}:00Z') for i in range(40)]
    selection=Selection('goes','ABI-L2-CMIPF','2021-05-01','2021-06-01',(-70,14,-62,22),assets,satellite=16,bands=(1,))
    output=tmp_path/'output';save_selection(selection,output/'month/product',index_results=False)
    run=Run({'source':'goes'},tmp_path/'runs',tmp_path/'index.duckdb')
    original=Path.resolve;resolved=[]
    def resolve(path,*a,**k):
        if 'data' in path.parts:resolved.append(path)
        return original(path,*a,**k)
    monkeypatch.setattr(Path,'resolve',resolve)
    queue_saved_selections(run,output,tmp_path/'data')
    assert resolved==[tmp_path/'data']


def test_failed_scratch_packaging_keeps_exact_bytes_and_current_native(tmp_path,monkeypatch):
    import tarfile
    from ecore_weather.jobs_cleanup import archive_failed_scratch
    monkeypatch.setattr('ecore_weather.jobs_policy.ensure_paused',lambda:None)
    old=tmp_path/'study-scratch/ecore-month-old';old.mkdir(parents=True)
    (old/'broken.json').write_bytes(b'');(old/'raw').write_bytes(b'failed source evidence')
    current=tmp_path/'study-scratch/goes-cmipf-current';current.mkdir();(current/'keep').write_bytes(b'raw')
    manifest={'deleted':[],'protected':[]};archive_failed_scratch(tmp_path,manifest)
    assert not old.exists() and (current/'keep').read_bytes()==b'raw'
    row=manifest['deleted'][0]
    with tarfile.open(row['evidence_archive']) as archive:
        assert archive.extractfile('raw').read()==b'failed source evidence'
        assert archive.extractfile('broken.json').read()==b''


def test_new_shared_attempt_does_not_keep_a_stale_verified_pause_receipt(tmp_path,monkeypatch):
    from ecore_weather import jobs_goes
    monkeypatch.chdir(tmp_path);monkeypatch.setenv('ECORE_SHARED_GOES','1')
    output=tmp_path/'run';output.mkdir();(output/'paused.json').write_text('{"status":"paused"}')
    def failed(arguments):raise ValueError('new attempt failed')
    monkeypatch.setattr(jobs_goes,'main',failed)
    with pytest.raises(ValueError,match='new attempt failed'):
        jobs.run_fetch('goes',['--output',str(output),'--scratch',str(tmp_path/'scratch')],str(tmp_path/'index.duckdb'))
    assert not (output/'paused.json').exists()
    assert json.loads((output/'previous-paused.json').read_text())['status']=='paused'
    assert json.loads(next((tmp_path/'results/runs').glob('*/run.json')).read_text())['status']=='failed'


def test_saved_optional_mcmipf_selection_does_not_enter_native_shared_pipeline(tmp_path,monkeypatch):
    from ecore_weather import cli,catalog,monthly
    from ecore_weather.common import Asset,Selection
    selection=Selection('goes','ABI-L2-MCMIPF','2021-05-01','2021-06-01',(-70,14,-62,22),
        [Asset('noaa-goes16','OR_ABI-L2-MCMIPF-M6_G16.nc',1,'e','2021-05-01T00:00:00Z')],satellite=16,bands=(13,))
    monkeypatch.setattr(catalog,'load_selection',lambda _:selection)
    monkeypatch.setattr(catalog,'save_selection',lambda *a,**k:None)
    configs=[]
    def fetch(*a,**k):configs.append(k['shared_config']);return {'records':[]}
    monkeypatch.setattr(monthly,'fetch',fetch)
    assert cli.main('goes',['--operation','fetch','--selection','saved.json','--output',str(tmp_path),'--skip-index'])==0
    assert configs==[None]


def test_shared_benchmark_global_configuration_is_indexed_without_object_logs(tmp_path):
    from ecore_weather.goes_shared import SharedConfig
    suite=tmp_path/'bench';suite.mkdir()
    config=SharedConfig().manifest()
    (suite/'suite.json').write_text(json.dumps({'rows':[{'stage':'final','satellite':16,
        'repeat':1,'wall_seconds':100,'returned_bytes':1234,'peak_rss_bytes':1024,
        'values_equal':True,'config':config}]}))
    database=tmp_path/'index.duckdb'
    index.import_operational_evidence(suite,tmp_path/'receipts',database)
    index.import_operational_evidence(suite,tmp_path/'receipts',database)
    with index.connect(database,read_only=True) as con:
        values=con.execute('SELECT config_json FROM benchmark_cases').fetchall()
    assert len(values)==1 and json.loads(values[0][0])==config


def test_index_writer_waits_for_brief_external_read_lock(tmp_path):
    import subprocess
    import sys
    import time
    from ecore_weather.index import connect
    path = tmp_path/'index.duckdb'
    with connect(path):
        pass
    child = subprocess.Popen([sys.executable, '-c',
        "import duckdb,sys,time; con=duckdb.connect(sys.argv[1],read_only=True); print('ready',flush=True); time.sleep(.7); con.close()", str(path)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        assert child.stdout.readline().strip() == 'ready'
        started = time.monotonic()
        with connect(path, lock_timeout=3) as con:
            con.execute("INSERT INTO jobs VALUES ('test','running',1,'test',0,0,'{}')")
        assert time.monotonic()-started >= .4
        assert child.wait(timeout=3) == 0
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()


def test_shared_resume_defers_future_archive_hashes_until_admission(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from ecore_weather import jobs_goes, jobs_mrms, goes_shared
    first = {'stage': 'h2-2021', 'start': '2021-10-01', 'end_excluded': '2021-11-01',
             'report_dir': str(tmp_path/'2021-10')}
    later = {'stage': 'h1-2026', 'start': '2026-01-01', 'end_excluded': '2026-02-01',
             'report_dir': str(tmp_path/'2026-01')}
    checkpoint = tmp_path/'checkpoints/h1-2026/2026-01-01.json'
    checkpoint.parent.mkdir(parents=True)
    checkpoint.write_text(json.dumps({'status': 'complete', 'start': later['start']}))
    monkeypatch.setattr(jobs_mrms, 'preflight', lambda _: None)
    checks = []
    monkeypatch.setattr(jobs_goes, 'checkpoint_matches', lambda row, product: checks.append(row) or True)
    def consume(requests, *args):
        iterator = iter(requests)
        assert next(iterator)[0] == '2021-10-0'
        assert checks == []  # Future DAS hashes must not delay first admission.
        # A pause leaves later months unvisited and their checkpoints intact.
        return {'paused': True, 'global_metrics': {}}
    monkeypatch.setattr(goes_shared, 'fetch_contexts', consume)
    args = SimpleNamespace(destination='unused', output=str(tmp_path), scratch=str(tmp_path/'scratch'),
        product='ABI-L2-CMIPF', begin_month='2021-10', stop_after_month=None, max_hours=1)
    assert jobs_goes.run_shared([first, later], args, goes_shared.SharedConfig()) == 75
    assert checks == [] and checkpoint.exists()
