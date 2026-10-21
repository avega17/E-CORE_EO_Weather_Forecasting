import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import threading
import time

import numpy as np
import pytest
import xarray as xr

from ecore_weather import jobs_mrms, mrms, mrms_profile, mrms_pipeline
from ecore_weather.common import Asset, Selection, iso


def _slow_fetch_for_deadline(*args):
    time.sleep(30)
    raise AssertionError('Hard deadline should terminate this worker')


def test_profiles_precedence_and_input_validation(tmp_path):
    path=tmp_path/'profile.json'
    path.write_text(json.dumps({'schema':1,'source':'mrms','resources':{'monthly_writers':8,'workers':8}}))
    args=argparse.Namespace(profile=str(path),workers=4)
    result=mrms_profile.resolve(args)
    assert args.monthly_writers==8 and args.workers==4 and args.decode_workers==1
    assert result['profile']['sha256']==hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ValueError,match='at most 16'):
        mrms_profile.resolve(argparse.Namespace(workers=17))
    path.write_text(json.dumps({'schema':1,'source':'goes','resources':{}}))
    with pytest.raises(ValueError):mrms_profile.resolve(argparse.Namespace(profile=str(path)))


def test_snapshot_identity_without_git(tmp_path,monkeypatch):
    monkeypatch.setenv('ECORE_RECORD_CODE_IDENTITY','1')
    (tmp_path/'snapshot.json').write_text(json.dumps({'git_revision':'commit','sha256':'content'}))
    assert mrms_profile.code_identity(tmp_path)==('commit','content')


def test_code_identity_is_not_inspected_by_default(tmp_path,monkeypatch):
    monkeypatch.delenv('ECORE_RECORD_CODE_IDENTITY',raising=False)
    def forbidden(*args,**kwargs):
        pytest.fail('No source-file inspection or Git command without developer opt-in')
    monkeypatch.setattr(Path,'exists',forbidden)
    import subprocess
    monkeypatch.setattr(subprocess,'check_output',forbidden)
    assert mrms_profile.code_identity(tmp_path)==(None,None)


def test_opted_in_code_metadata_failure_cannot_stop_job(tmp_path,monkeypatch):
    import subprocess
    monkeypatch.setenv('ECORE_RECORD_CODE_IDENTITY','1')
    monkeypatch.setattr(subprocess,'check_output',lambda *a,**kw:(_ for _ in ()).throw(FileNotFoundError('git')))
    assert mrms_profile.code_identity(tmp_path)==(None,None)
    (tmp_path/'snapshot.json').write_text('invalid optional metadata')
    assert mrms_profile.code_identity(tmp_path)==(None,None)


def test_ordered_reads_reserve_earliest_observation_and_bound_bytes(monkeypatch):
    monkeypatch.setattr(mrms_pipeline,'roi_reservation',lambda _:400000)
    started=[]
    active=peak=0
    lock=threading.Lock()
    def read(asset,*_):
        nonlocal active,peak
        with lock:
            started.append(asset.key);active+=1;peak=max(peak,active)
        time.sleep(.04 if asset.key=='0' else .001)
        with lock:active-=1
        return xr.Dataset({'measurement':(('y','x'),np.array([[int(asset.key)]],dtype='float64'))}),{}
    monkeypatch.setattr(mrms,'read',read)
    assets=[Asset('bucket',str(i),1,'etag','2022-01-01') for i in range(6)]
    selection=Selection('mrms','PrecipRate_00.00','2022-01-01','2022-02-01',(-67,18,-66,19),assets)
    metrics={}
    values=[]
    for row,(ds,_) in mrms_pipeline.ordered_reads([{'asset':a} for a in assets],selection,None,8,16,1,metrics):
        values.append(ds.measurement.item())
    assert values==list(range(6)) and peak<=2
    assert metrics['effective_max_inflight']==2
    assert metrics['peak_roi_reserved_bytes']<=1024**2


def test_roi_too_large_fails_before_read(monkeypatch):
    monkeypatch.setattr(mrms_pipeline,'roi_reservation',lambda _:2*1024**2)
    selection=argparse.Namespace(bbox=(-70,14,-62,22))
    with pytest.raises(ValueError,match='at least 2'):
        list(mrms_pipeline.ordered_reads([],selection,None,1,1,1,{}))


def test_two_products_fill_adjacent_months_and_reuse_receipts(tmp_path,monkeypatch):
    from ecore_weather import index
    monkeypatch.setattr(jobs_mrms,'ProcessPoolExecutor',lambda max_workers,**kw:ThreadPoolExecutor(max_workers=max_workers))
    monkeypatch.setattr(jobs_mrms,'preflight',lambda _:None)
    monkeypatch.setattr(mrms_profile,'cap_library_threads',lambda:None)
    monkeypatch.setattr(index,'record_fetch',lambda _:None)
    monkeypatch.delenv('ECORE_DURABLE_STATE',raising=False)
    active=peak=0
    lock=threading.Lock()
    calls=[]
    barrier=threading.Barrier(4)
    def discover(begin,end,bbox,product,**kw):
        asset=Asset('bucket',f'{product}/{begin:%Y-%m}',1,'etag',iso(begin))
        return Selection('mrms',product,iso(begin),iso(end),bbox,[asset],expected_times=(iso(begin),))
    def fetch(selection,destination,*_):
        nonlocal active,peak
        with lock:
            active+=1;peak=max(peak,active);calls.append((selection.product,selection.start))
        barrier.wait(timeout=10)
        target=Path(destination)/selection.product/selection.start[:7]
        target.mkdir(parents=True)
        archive=target/'raw.zarr.zip';archive.write_bytes(b'verified')
        (target/'complete.json').write_text(json.dumps({'archive_sha256':hashlib.sha256(b'verified').hexdigest()}))
        with lock:active-=1
        return {'monthly_archives':[{'path':str(archive),'status':'saved','observations':1}],
                'read_bytes':1,'stored_bytes':8}
    monkeypatch.setattr(mrms,'discover',discover)
    monkeypatch.setattr(jobs_mrms,'_fetch_selection',fetch)
    args=['--start','2022-01-01','--end','2022-03-01','--products','composite-reflectivity','base-reflectivity',
          '--destination',str(tmp_path/'data'),'--output',str(tmp_path/'run'),'--scratch',str(tmp_path/'scratch')]
    assert jobs_mrms.main(args)==0
    assert peak==4 and len(calls)==4
    monkeypatch.setattr(mrms,'discover',lambda *a,**kw:pytest.fail('Verified request must be reused'))
    assert jobs_mrms.main(args)==0 and len(calls)==4


def test_dry_run_counts_selected_products(capsys):
    jobs_mrms.main(['--dry-run','--start','2022-01-01','--end','2022-01-02',
                    '--products','base-reflectivity'])
    assert json.loads(capsys.readouterr().out)['nominal_slots']==144


def test_source_month_partition_keeps_boundary_slots_once(monkeypatch):
    from ecore_weather.common import utc
    slots=['2021-01-01T00:00:00Z','2021-01-01T00:10:00Z',
           '2021-01-01T00:20:00Z','2021-02-01T00:00:00Z','2021-02-01T00:10:00Z']
    times=['2020-12-31T23:58:00Z','2021-01-01T00:08:00Z',None,
           '2021-01-31T23:58:00Z','2021-02-01T00:08:00Z']
    def discover(begin,end,bbox,product,**kw):
        expected=tuple(t for t in slots if utc(begin)<=utc(t)<utc(end))
        assets=[Asset('bucket',t,1,'etag',stamp) for t,stamp in zip(slots,times) if t in expected and stamp]
        matches=tuple({'slot_time':a.key,'asset_id':a.id,'source_time':a.time,'offset_seconds':-120} for a in assets)
        return Selection('mrms',product,iso(begin),iso(end),bbox,assets,expected_times=expected,hourly_matches=matches)
    monkeypatch.setattr(mrms,'discover',discover)
    start,end=utc('2021-01-01'),utc('2021-03-01')
    selections=[jobs_mrms.discover_source_month(begin,stop,product,(-70,14,-62,22),start,end)
        for begin,stop,product in jobs_mrms.source_month_schedule(start,end,['MergedReflectivityQCComposite_00.50'])]
    assert sorted(t for s in selections for t in s.expected_times)==slots
    ids=[a.id for s in selections for a in s.assets]
    assert len(ids)==len(set(ids))==4
    assert [len(s.assets) for s in selections]==[1,2,1]


def test_mrms_failure_remains_failure_after_pause(tmp_path,monkeypatch):
    from ecore_weather import jobs
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('ECORE_RUN_ROOT',str(tmp_path/'runs'))
    def failed(arguments):
        (tmp_path/'run'/'pause.request').write_text('pause')
        return 1
    monkeypatch.setattr(jobs_mrms,'main',failed)
    assert jobs.run_fetch('mrms',['--output',str(tmp_path/'run'),'--scratch',str(tmp_path/'scratch')],str(tmp_path/'db.duckdb'))==1


def test_remote_progress_does_not_require_local_index(tmp_path,monkeypatch,capsys):
    from ecore_weather import job_status,jobs
    monkeypatch.setattr(jobs,'status',lambda _:pytest.fail('Remote monitor must not open compute-node index'))
    path=tmp_path/'progress.json'
    path.write_text(json.dumps({'snapshot_at':time.time(),'runs':[],'tasks':[]}))
    assert job_status.main(['--progress',str(path)],source='mrms')==0
    assert 'No matching' in capsys.readouterr().out


def test_tuning_prefers_lower_resources_within_five_percent():
    from ecore_weather.mrms_tuning import choose
    small=dict(mrms_profile.DEFAULTS)
    large={**small,'monthly_writers':8}
    rows=[dict(status='complete',equal=True,resources=r,wall_seconds=t) for r,t in [(small,100),(large,96)]]
    assert choose(rows)==small
    rows[-1]['wall_seconds']=80
    assert choose(rows)==large


def test_hard_deadline_terminates_unfinished_process(tmp_path,monkeypatch):
    import multiprocessing
    from concurrent.futures import ProcessPoolExecutor
    monkeypatch.setattr(jobs_mrms,'ProcessPoolExecutor',lambda max_workers,**kw:
        ProcessPoolExecutor(max_workers=max_workers,mp_context=multiprocessing.get_context('fork')))
    monkeypatch.setattr(jobs_mrms,'preflight',lambda _:None)
    monkeypatch.setattr(mrms_profile,'cap_library_threads',lambda:None)
    monkeypatch.setattr(jobs_mrms,'_fetch_selection',_slow_fetch_for_deadline)
    def discover(begin,end,bbox,product,**kw):
        return Selection('mrms',product,iso(begin),iso(end),bbox,
            [Asset('bucket','slow',1,'etag',iso(begin))],expected_times=(iso(begin),))
    monkeypatch.setattr(mrms,'discover',discover)
    started=time.monotonic()
    code=jobs_mrms.main(['--start','2022-09-18T12:00:00Z','--end','2022-09-18T12:10:00Z',
        '--products','base-reflectivity','--destination',str(tmp_path),'--output',str(tmp_path/'run'),
        '--scratch',str(tmp_path/'scratch'),'--max-hours','0.0005','--drain-minutes','0'])
    assert code==75 and time.monotonic()-started<8
    receipt=json.loads((tmp_path/'run/paused.json').read_text())
    assert receipt['hard_deadline_reached'] and receipt['unfinished_product_months']


def test_tuning_repeats_the_lower_resource_screening_candidate():
    from ecore_weather.mrms_tuning import finalists
    base = dict(monthly_writers=12, workers=4, decode_workers=1,
                max_inflight=16, roi_queue_mib=256, memory_gib=64)
    rows = [dict(status="complete", equal=True, resources={**base, "workers": n},
                 wall_seconds=t) for n, t in [(4, 76.4), (8, 75.8), (16, 75.7)]]
    chosen = finalists(rows)
    assert [r["workers"] for r in chosen] == [4, 16]
