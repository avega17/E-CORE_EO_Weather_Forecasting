"""Shared limits, C02 tail admission, and exact raw archive checks."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import hashlib
import queue
import threading
import time
import numpy as np
import pytest
import xarray as xr
from ecore_weather.common import Asset,Selection
from ecore_weather.goes_shared import MonthSlots,SharedConfig,SharedReads,ReaderClient


def test_tail_slots_are_bounded_and_release_normal_capacity():
    slots=MonthSlots(2,2)
    assert slots.admit('jan',[1,2,13]) and slots.admit('feb',[1,2,13])
    assert not slots.admit('mar',[1,2,13])
    slots.verified('jan',1);slots.verified('jan',13)
    assert slots.active['jan']['role']=='tail' and slots.admit('mar',[1,2,13])
    slots.verified('feb',1);slots.verified('feb',13)
    assert slots.admit('apr',[1,2,13])
    slots.verified('mar',1);slots.verified('mar',13)
    assert slots.active['mar']['role']=='normal' and not slots.admit('may',[1,2,13])
    slots.finish('jan')
    assert slots.active['mar']['role']=='tail' and slots.admit('may',[1,2,13])
    assert len(slots.active)==4


def test_first_probe_drains_replenishment_instead_of_starving_new_month():
    from collections import deque
    broker=object.__new__(SharedReads)
    broker.config=SharedConfig(range_readers=4,roi_budget_mib=1)
    broker.states={name:{'rows':[None]*1000,'band':2,'cursor':0,
                         'payload':payload,'held':{}}
                   for name,payload in [('established',16),('new_month',None)]}
    broker.order=deque(broker.states)
    broker.roi=16;broker.roi_peak=16
    broker.ranges=broker.locals=broker.probes=0
    broker.max_ranges=broker.max_locals=0
    broker._spawn=lambda coroutine:coroutine.close()
    broker._admit()
    assert broker.states['established']['cursor']==0
    assert broker.states['new_month']['cursor']==0
    # The already-admitted result is consumed; the new probe gets the credit.
    broker.roi=0
    broker._admit()
    assert broker.states['new_month']['cursor']==1
    assert broker.states['established']['cursor']==0
    assert broker.roi==1024**2


def fake_sources(monkeypatch,tmp_path):
    import obstore,obstore.store
    from ecore_weather import goes_staging,goes_shared
    ds=xr.Dataset({'CMI':(('y','x'),np.array([[4,5],[6,7]],dtype='int16')),
        'DQF':(('y','x'),np.array([[0,1],[2,3]],dtype='int8'))},coords={'x':[.1,.2],'y':[.3,.4]})
    ds.CMI.attrs.update(scale_factor=.1,add_offset=2,_FillValue=-1)
    fixture=tmp_path/'fixture.nc';ds.to_netcdf(fixture,engine='h5netcdf');content=fixture.read_bytes()
    activity={'downloads':0,'peak':0,'bands':set(),'mixed':False,'stores':0}
    def store(*a,**k):activity['stores']+=1;return object()
    monkeypatch.setattr(obstore.store,'S3Store',store)
    async def get(store,key,**kwargs):
        activity['downloads']+=1;activity['peak']=max(activity['peak'],activity['downloads'])
        activity['bands'].add(int(__import__('re').search(r'M6C(\d+)_',key)[1]))
        class Response:
            def stream(self,**kwargs):
                async def blocks():
                    # An early slow observation must not deadlock ordered consumption.
                    await asyncio.sleep(.05 if key.endswith('0.nc') else .005)
                    yield content
                    activity['downloads']-=1
                return blocks()
        return Response()
    monkeypatch.setattr(obstore,'get_async',get)
    monkeypatch.setattr(goes_staging.goes,'subset_dataset',lambda source,*a:source.load().copy(deep=True))
    def read(asset,bbox,band,block):
        activity['mixed'] |= activity['downloads']>0
        time.sleep(.01)
        copy=ds.copy(deep=True);copy.attrs.update(source_url=asset.url,source_etag=asset.etag,
            observation_time=asset.time,scan_end=asset.end_time,requested_bbox=list(bbox))
        return copy,{'read_bytes':asset.size,'range_requests':1,'source_retries':0,'failed_requests':0,
            'transfer_task_seconds':.01,'reader_seconds':.01,'read_decode_crop_s':.01,'reader_cpu_seconds':0}
    monkeypatch.setattr(goes_shared,'range_read',read)
    assets=[Asset('noaa-goes16',f'ABI-L2-CMIPF/OR_ABI-L2-CMIPF-M6C{b:02}_G16_{i}.nc',
        len(content),hashlib.md5(content).hexdigest(),f'2022-09-18T12:{i:02}:00Z')
        for b in (1,2,3,7,8,9,10,13) for i in range(8)]
    return ds,assets,activity


def test_all_bands_shared_downloads_order_and_checkpoint_retirement(tmp_path,monkeypatch):
    ds,assets,activity=fake_sources(monkeypatch,tmp_path)
    cfg=SharedConfig(download_concurrency=16,local_readers=4,range_readers=2,roi_budget_mib=1,staging_mib=8)
    selection=Selection('goes','ABI-L2-CMIPF',assets[0].time,'2022-09-18T13:00:00Z',(-70,14,-62,22),assets,bands=(1,2,3,7,8,9,10,13),satellite=16)
    with SharedReads(cfg,pool_class=ThreadPoolExecutor) as broker:
        req,reply=broker.attach('sep');client=ReaderClient('sep',req,reply)
        def consume(band):
            rows=[{'asset':a,'asset_id':a.id} for a in assets if f'C{band:02}_' in a.key]
            metrics={};folder=tmp_path/f'C{band:02}'
            for row,(actual,timing) in client.reads(f'sep:C{band:02}',rows,selection,band,folder,metrics):
                np.testing.assert_array_equal(actual.CMI,ds.CMI)
                assert actual.CMI.attrs==ds.CMI.attrs
                if band!=2 and row is rows[-1]:client.send(f'sep:C{band:02}','commit',ids={a['asset_id'] for a in rows})
            return metrics
        with ThreadPoolExecutor(8) as pool:results=list(pool.map(consume,selection.bands))
        deadline=time.time()+3
        while broker.disk and time.time()<deadline:time.sleep(.01)
        assert broker.disk==0 and broker.roi==0
        snapshot=broker.snapshot();client.close()
    assert activity['stores']==1 and activity['mixed']
    assert activity['bands']=={1,3,7,8,9,10,13}
    assert 7<=activity['peak']<=16
    assert snapshot['roi_queue_peak_bytes']<=cfg.roi_budget_mib*1024**2
    assert snapshot['peak_range_readers']<=2 and snapshot['peak_local_readers']<=4
    assert all(not list((tmp_path/f'C{b:02}').glob('*.nc')) for b in (1,3,7,8,9,10,13))


def test_shared_pipeline_round_trip_real_month_process(tmp_path,monkeypatch):
    ds,assets,activity=fake_sources(monkeypatch,tmp_path)
    from ecore_weather import goes_shared
    real=goes_shared.SharedReads
    monkeypatch.setattr(goes_shared,'SharedReads',lambda c,ctx:real(c,ctx,pool_class=ThreadPoolExecutor))
    selection=Selection('goes','ABI-L2-CMIPF',assets[0].time,'2022-09-18T13:00:00Z',(-70,14,-62,22),assets,bands=(1,2,3,7,8,9,10,13),satellite=16)
    config=SharedConfig(download_concurrency=16,roi_budget_mib=8,staging_mib=8)
    report=goes_shared.fetch(selection,tmp_path/'dest',config,tmp_path/'scratch',index_results=False)
    assert len(report['monthly_archives'])==8 and not report['interrupted']
    from ecore_weather.storage import open_raw
    for row in report['monthly_archives']:
        with open_raw(row['path']) as archived:
            np.testing.assert_array_equal(archived[f'CMI_C{row["band"]:02}'].values,np.stack([ds.CMI.values]*8))
            np.testing.assert_array_equal(archived[f'DQF_C{row["band"]:02}'].values,np.stack([ds.DQF.values]*8))
    reused=goes_shared.fetch(selection,tmp_path/'dest',config,tmp_path/'scratch',index_results=False)
    assert all(row['status']=='reused' for row in reused['monthly_archives'])


def test_failure_wakes_all_waiters_and_keeps_staged_evidence(tmp_path,monkeypatch):
    ds,assets,activity=fake_sources(monkeypatch,tmp_path)
    from ecore_weather import goes_staging
    def broken(*args):raise IOError('fixture corrupt HDF5')
    monkeypatch.setattr(goes_staging,'local_read',broken)
    with SharedReads(SharedConfig(roi_budget_mib=1,staging_mib=8),pool_class=ThreadPoolExecutor) as broker:
        req,reply=broker.attach('failure');client=ReaderClient('failure',req,reply)
        sel=Selection('goes','ABI-L2-CMIPF',assets[0].time,'2022-09-18T13:00:00Z',(-70,14,-62,22),assets,bands=(1,),satellite=16)
        rows=[{'asset':a,'asset_id':a.id} for a in assets if 'C01_' in a.key]
        with pytest.raises(OSError,match='corrupt HDF5'):
            list(client.reads('failure:C01',rows,sel,1,tmp_path/'failed',{}))
        client.close()
    assert list((tmp_path/'failed').glob('*.nc'))


def test_shared_cli_defaults_alias_conflict_and_legacy_profile_rejection(tmp_path):
    from ecore_weather.cli import parser
    from ecore_weather.goes_shared import config_from_args
    args=parser('goes').parse_args([])
    assert args.pipeline=='shared'
    config=config_from_args(args)
    assert config.month_writers==2 and config.tail_months==1 and config.local_readers==2 and config.range_readers==8
    assert config.download_concurrency==32 and config.staging_mib==16384
    with pytest.raises(SystemExit):parser('goes').parse_args(['--month-writers','2','--monthly-writers','4'])
    with pytest.warns(FutureWarning):
        assert config_from_args(parser('goes').parse_args(['--monthly-writers','4'])).month_writers==4
    path=tmp_path/'v1.json';path.write_text('{"2":{"profile":{"read_mode":"range"}}}')
    with pytest.raises(ValueError,match='schema 2'):config_from_args(parser('goes').parse_args(['--shared-profile',str(path)]))


def test_scheduler_choice_requires_both_satellites_and_all_repeats():
    from ecore_weather.goes_scheduler_benchmark import choose
    rows=[]
    for sat in (16,19):
        for repeat in (1,2,3):
            for method,seconds in [('legacy',100),('new',104)]:
                rows.append({'profile_id':method,'satellite':sat,'repeat':repeat,
                    'values_equal':True,'peak_rss_bytes':1024**3,'wall_seconds':seconds,
                    'config':SharedConfig().manifest()})
    decision=choose(rows)
    assert decision['pipeline']=='shared' and 'no demonstrated' in decision['reason']
    for row in rows:
        if row['profile_id']=='new' and row['satellite']==19:row['wall_seconds']=106
    assert choose(rows)['pipeline']=='legacy'
    with pytest.raises(ValueError):choose([r for r in rows if r['satellite']!=19])


def test_scheduler_resource_ties_use_relative_readiness_not_gain_points():
    from ecore_weather.goes_scheduler_benchmark import choose
    rows=[]
    for sat in (16,19):
        for repeat in (1,2,3):
            for name,seconds,owners in [('legacy',100,2),('fast',50,4),('cheap',54,1)]:
                rows.append({'profile_id':name,'satellite':sat,'repeat':repeat,
                    'values_equal':True,'peak_rss_bytes':1024**3,'wall_seconds':seconds,
                    'config':SharedConfig(month_writers=owners).manifest()})
    assert choose(rows)['global_config']['month_writers']==4


def test_launch_review_uses_portable_source_hashes_and_rejects_changed_code(tmp_path):
    import ast
    from ecore_weather.goes_rollout import validate_reviewed_source
    source=tmp_path/'reader.py';source.write_text('def reader():\n    return 1\n')
    fragment=ast.get_source_segment(source.read_text(),ast.parse(source.read_text()).body[0])
    receipt={'after_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),
        'read_write_source_sha256':{'reader':hashlib.sha256(fragment.encode()).hexdigest()},
        'read_write_components_unchanged':True}
    validate_reviewed_source(source,receipt)
    receipt.update(read_write_components_unchanged=False,verified_scheduler_revision=True,
                   paired_repeats_per_satellite=2)
    with pytest.raises(ValueError,match='revision differs'):validate_reviewed_source(source,receipt)
    receipt['paired_repeats_per_satellite']=3
    validate_reviewed_source(source,receipt)
    source.write_text('def reader():\n    return 2\n')
    with pytest.raises(ValueError,match='revision differs'):validate_reviewed_source(source,receipt)


def test_verified_batch_resume_uses_same_identity_with_new_topology(tmp_path,monkeypatch):
    """A failed pack must leave raw checkpoints reusable without redownloading."""
    ds,assets,activity=fake_sources(monkeypatch,tmp_path)
    from ecore_weather import goes_shared,monthly_stream
    real=goes_shared.SharedReads
    monkeypatch.setattr(goes_shared,'SharedReads',lambda c,ctx:real(c,ctx,pool_class=ThreadPoolExecutor))
    selection=Selection('goes','ABI-L2-CMIPF',assets[0].time,'2022-09-18T13:00:00Z',(-70,14,-62,22),
        [a for a in assets if 'C13_' in a.key],bands=(13,),satellite=16)
    finalizer=monthly_stream._finalize_archive
    def fail(*args):raise IOError('intentional finalizer interruption')
    monkeypatch.setattr(monthly_stream,'_finalize_archive',fail)
    with pytest.raises(OSError,match='interruption'):
        goes_shared.fetch(selection,tmp_path/'dest',SharedConfig(roi_budget_mib=8,staging_mib=8),tmp_path/'scratch',index_results=False)
    assert list((tmp_path/'scratch').glob('*/batch-*.json'))
    monkeypatch.setattr(monthly_stream,'_finalize_archive',finalizer)
    before=activity['peak']
    report=goes_shared.fetch(selection,tmp_path/'dest',SharedConfig(month_writers=1,roi_budget_mib=8,staging_mib=8),tmp_path/'scratch',index_results=False)
    assert report['monthly_archives'][0]['resumed_observations']==8
    assert report['read_bytes']==0 and report['monthly_archives'][0]['status']=='saved'


def test_pause_drains_admitted_month_and_does_not_load_next(tmp_path,monkeypatch):
    ds,assets,activity=fake_sources(monkeypatch,tmp_path)
    from ecore_weather import goes_shared
    real=goes_shared.SharedReads
    monkeypatch.setattr(goes_shared,'SharedReads',lambda c,ctx:real(c,ctx,pool_class=ThreadPoolExecutor))
    selection=Selection('goes','ABI-L2-CMIPF',assets[0].time,'2022-09-18T13:00:00Z',(-70,14,-62,22),
        [a for a in assets if 'C13_' in a.key],bands=(13,),satellite=16)
    stopped=[False];loaded=[]
    def load():loaded.append('next');return selection
    def finish(key,report):stopped[0]=True
    result=goes_shared.fetch_contexts([('first',selection,None),('next',load,None)],str(tmp_path/'dest'),
        SharedConfig(month_writers=1,roi_budget_mib=8,staging_mib=8),tmp_path/'scratch',False,
        lambda:stopped[0],finish)
    assert result['paused'] and not loaded and len(result['reports'])==1
    assert result['reports'][0]['monthly_archives'][0]['status']=='saved'


def test_launch_capacity_uses_native_crop_bytes_and_all_band_evidence(tmp_path,monkeypatch):
    from ecore_weather.goes_rollout import storage_preflight
    import shutil
    archives=[{'band':b,'observations':8,'stored_bytes':8000} for b in (1,2,3,7,8,9,10,13)]
    suite={'rows':[{'stage':'final','monthly_metrics':archives,'listed_source_bytes':10**15}]}
    choice={'pipeline':'shared','global_config':SharedConfig().manifest()}
    usage=shutil.disk_usage(tmp_path)
    monkeypatch.setattr(shutil,'disk_usage',lambda path:usage._replace(free=10**12))
    report=storage_preflight(suite,choice,tmp_path,tmp_path/'scratch')
    assert report['month_bytes_with_50pct_margin']==8000*31*24*6*1.5
    assert report['month_slots_including_tails']==4
    monkeypatch.setattr(shutil,'disk_usage',lambda path:usage._replace(free=1))
    with pytest.raises(OSError,match='scratch needs'):storage_preflight(suite,choice,tmp_path,tmp_path/'scratch')
    suite['rows'][0]['monthly_metrics']=archives[:-1]
    with pytest.raises(ValueError,match='all eight'):storage_preflight(suite,choice,tmp_path,tmp_path/'scratch')


def test_completion_summary_separates_c02_tail_and_production_measurement_scope():
    from ecore_weather.goes_rollout import completion_summary,production_review
    archives=[{'month':'2022-09','band':b,'attempt_started_at':100,
        'attempt_ended_at':120 if b==2 else 110,'write_seconds':20 if b==2 else 10,
        'observations':8,'stored_bytes':1000,'read_bytes':10000} for b in (1,2,3,7,8,9,10,13)]
    summary=completion_summary({'monthly_metrics':archives})
    assert summary['first_band_seconds_from_first_writer']==10
    assert summary['first_month_seconds_from_first_writer']==20
    assert summary['c02_only_remaining_seconds_by_month']=={'2022-09':10}
    month={'archives':archives,'wall_seconds':20,'read_bytes':80000,'stored_bytes':8000,
        'start':'2022-09-01','product':'ABI-L2-CMIPF','global_config':SharedConfig().manifest()}
    review=production_review(month,{'benchmark_reference':{'16':{'sample':'bounded'}}})
    assert review['attempt_average_mbps']==.032 and len(review['bands'])==8
    assert 'not whole-month forecasts' in review['comparison_scope']


def test_native_grouping_resolves_paths_once_per_band_month(tmp_path,monkeypatch):
    from pathlib import Path
    from ecore_weather.goes_shared import _groups
    assets=[Asset('noaa-goes16',f'ABI-L2-CMIPF/OR_ABI-L2-CMIPF-M6C{band:02}_G16_{i}.nc',1,'e',
        '2022-09-18T12:00:00Z') for band in (1,2,3,7,8,9,10,13) for i in range(128)]
    selection=Selection('goes','ABI-L2-CMIPF','2022-09-01','2022-10-01',(-70,14,-62,22),assets,
        bands=(1,2,3,7,8,9,10,13),satellite=16)
    calls=[];original=Path.resolve
    def resolve(self,*a,**k):calls.append(str(self));return original(self,*a,**k)
    monkeypatch.setattr(Path,'resolve',resolve)
    groups=_groups(selection,tmp_path/'dest')
    assert len(groups)==8 and len(calls)==9 and sum(len(rows) for _,rows in groups)==1024


def test_notebook_progress_is_cumulative_across_months_and_reuse(tmp_path,monkeypatch):
    from dataclasses import replace
    from ecore_weather import goes_shared
    ds,assets,activity=fake_sources(monkeypatch,tmp_path)
    real=goes_shared.SharedReads
    monkeypatch.setattr(goes_shared,'SharedReads',lambda c,ctx:real(c,ctx,pool_class=ThreadPoolExecutor))
    first=[a for a in assets if 'C13_' in a.key]
    second=[replace(a,key=a.key.replace('_G16_','_G16_oct_'),time=a.time.replace('2022-09','2022-10')) for a in first]
    selection=Selection('goes','ABI-L2-CMIPF','2022-09-01','2022-11-01',(-70,14,-62,22),first+second,bands=(13,),satellite=16)
    seen=[]
    report=goes_shared.fetch(selection,tmp_path/'dest',SharedConfig(roi_budget_mib=8,staging_mib=8),tmp_path/'scratch',
        index_results=False,progress=lambda done,total,row:seen.append((done,total)))
    assert not report['interrupted'] and seen[-1]==(16,16)
    assert [done for done,_ in seen]==sorted(done for done,_ in seen)
    seen.clear()
    partial=replace(selection,assets=first[:2],end='2022-10-01')
    goes_shared.fetch(partial,tmp_path/'dest',SharedConfig(roi_budget_mib=8,staging_mib=8),tmp_path/'scratch',
        index_results=False,progress=lambda done,total,row:seen.append((done,total)))
    assert seen[-1]==(2,2) and all(done<=2 for done,_ in seen)


def test_resumed_progress_does_not_report_previous_attempt_bytes(tmp_path, monkeypatch):
    import json
    import time
    from types import SimpleNamespace
    from ecore_weather.goes_shared import _progress, SharedConfig
    from ecore_weather.common import Asset
    from ecore_weather.index import connect
    monkeypatch.setenv('ECORE_INDEX_PATH', str(tmp_path/'index.duckdb'))
    asset = Asset('noaa-goes16', 'key', 10, 'etag', '2021-10-01T00:00:00Z')
    cfg = SharedConfig()
    broker = SimpleNamespace(config=cfg, snapshot=lambda: {'bands': {},
        'roi_reserved_bytes': 0, 'scratch_reserved_bytes': 0})
    target = str(tmp_path/'archive')
    active = {'month': {'groups': [((target, 1), [asset])], 'verified': set(),
        'task_progress': {1: {'attempt_read_bytes': 900, 'initial_read_bytes': 1000, 'next_index': 0}},
        'requested_indices': {1: [0]}, 'started_at': time.time()-2,
        'role': 'normal', 'selection': SimpleNamespace(product='ABI-L2-CMIPF')}}
    _progress(active, broker, True)
    with connect(read_only=True) as con:
        row = json.loads(con.execute('SELECT metrics_json FROM tasks').fetchone()[0])
    assert row['attempt_read_bytes'] == 0 and row['read_bytes'] == 1000
