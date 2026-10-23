"""Monitor measurements must have honest scopes and survive completion/locks."""
import json

import pytest

from ecore_weather.job_status import observe,filter_tasks,render,main,aggregate_bandwidth


def data(counter=100_000_000,state='checkpointed',run_id='run'):
    return {'runs':[{'run_id':run_id,'status':'running','process_alive':True,'pid':1,'heartbeat':100}],
        'tasks':[{'task_id':'/data/goes/ABI-L2-CMIPF/goes16/C02/roi-test/2021/05/raw.zarr.zip',
            'run_id':run_id,'source':'goes','product':'ABI-L2-CMIPF','month':'2021-05','band':2,
            'selected':100,'checkpointed':20,'completed':100 if state=='complete' else 0,
            'status':state,'updated_at':100,'metrics':{'read_bytes':counter,'transfer_task_seconds':99999}}]}


def test_watch_rate_uses_returned_byte_delta_over_wall_time_not_parallel_task_sum():
    cache={};first=observe(data(),cache,100)['tasks'][0]
    assert first['average_mbps'] is None
    task=observe(data(110_000_000),cache,110)['tasks'][0]
    assert task['average_mbps']==8 and task['elapsed_seconds']==10
    assert task['display_status']=='fetching' and task['timing_scope']=='watch'
    # A completion freezes the observed timing after scratch deletion.
    done=observe(data(120_000_000,'complete'),cache,120)['tasks'][0]
    later=observe(data(120_000_000,'complete'),cache,200)['tasks'][0]
    assert done['elapsed_seconds']==later['elapsed_seconds']==20
    assert later['average_mbps']==8


def test_completed_attempt_uses_only_its_matching_byte_and_time_scope():
    snapshot=data(999_000_000,'complete')
    snapshot['tasks'][0]['attempt_metrics']={'write_seconds':20,'read_bytes':40_000_000,'build_read_bytes':999_000_000}
    task=observe(snapshot,{},100)['tasks'][0]
    assert task['timing_scope']=='attempt' and task['average_mbps']==16 and task['returned_bytes']==40_000_000
    unknown=observe(data(999_000_000,'complete'),{},100)['tasks'][0]
    assert unknown['elapsed_seconds'] is None and unknown['timing_scope']=='unavailable'


def test_counter_reset_or_new_run_restarts_monitor_window():
    cache={};observe(data(),cache,100);observe(data(110_000_000),cache,110)
    reset=observe(data(105_000_000),cache,120)['tasks'][0]
    assert reset['elapsed_seconds']==0 and reset['average_mbps'] is None
    new=observe(data(200_000_000,run_id='new'),cache,140)['tasks'][0]
    assert new['elapsed_seconds']==0


def test_month_overlap_and_source_band_filters():
    tasks=data()['tasks']
    assert len(filter_tasks(tasks,'goes','2021-05-15','2021-06-01',band=2))==1
    assert filter_tasks(tasks,start='2021-06-01')==[]
    assert filter_tasks(tasks,end='2021-05-01')==[]
    assert filter_tasks(tasks,source='mrms')==[]
    with pytest.raises(ValueError):filter_tasks(tasks,start='2021-06-01',end='2021-05-01')


def test_watch_cli_table_json_and_cached_snapshot_during_index_lock(tmp_path,monkeypatch,capsys):
    from ecore_weather import jobs
    monkeypatch.setattr(jobs,'status',lambda _:data())
    monkeypatch.setattr('ecore_weather.job_status.time.time',lambda:100)
    cache=tmp_path/'monitor.json';index=tmp_path/'index.duckdb'
    assert main(['--start','2021-05-01','--end','2021-06-01','--cache',str(cache)],index_path=index)==0
    text=capsys.readouterr().out
    assert 'Verified/Selected' in text and 'C02' in text and 'Avg Mb/s' in text and 'watch' in text
    monkeypatch.setattr(jobs,'status',lambda _:{'status':'index_unavailable','reason':'coordinator lock'})
    monkeypatch.setattr('ecore_weather.job_status.time.time',lambda:110)
    assert main(['--json','--cache',str(cache)],index_path=index)==0
    locked=json.loads(capsys.readouterr().out)
    assert locked['cached_at']==100 and len(locked['tasks'])==1
    assert locked['tasks'][0]['average_mbps'] is None  # A stale snapshot never fabricates throughput.
    assert not index.exists()  # Monitoring never opens or creates a write connection.


def test_queued_tasks_have_no_fabricated_time_or_speed():
    task=observe(data(state='queued'),{},100)['tasks'][0]
    assert task['elapsed_seconds'] is None and task['average_mbps'] is None
    text=render({'runs':[],'tasks':[task]},100)
    assert 'queued' in text and 'unavailable' in text


def test_manifest_fallback_survives_deleted_scratch_and_deduplicates_archives(tmp_path):
    from ecore_weather.job_status import completed_manifest_status
    raw=tmp_path/'data/goes/ABI-L2-CMIPF/goes16/C02/roi-test/2021/05/raw.zarr.zip'
    raw.parent.mkdir(parents=True);raw.write_bytes(b'fixture');(raw.parent/'complete.json').write_text('{}')
    root=tmp_path/'run';receipt=root/'checkpoints/h1-2021/2021-05-01.json'
    receipt.parent.mkdir(parents=True)
    payload={'status':'complete','product':'ABI-L2-CMIPF','start':'2021-05-01',
        'archives':[{'path':str(raw),'band':2,'observations':4457}]}
    receipt.write_text(json.dumps(payload))
    second=root/'checkpoints/h1-2021/duplicate.json';second.write_text(json.dumps(payload))
    data=completed_manifest_status('goes',{'goes':[root]})
    assert len(data['tasks'])==1 and data['tasks'][0]['completed']==4457
    table=render(observe(data,{},100),100)
    assert 'durable completed month' in table and '4,457/4,457' in table
    assert 'unavailable' in table  # No invented historical timing.
    raw.unlink()
    assert not completed_manifest_status('goes',{'goes':[root]})['tasks']


def test_resumed_unadmitted_band_does_not_report_prior_attempt_as_active():
    snapshot=data()
    snapshot['runs'][0]['started_at']=200
    snapshot['tasks'][0]['metrics'].update(attempt_started_at=100,attempt_read_bytes=1000,attempt_elapsed_seconds=10)
    row=observe(snapshot,{},210)['tasks'][0]
    assert row['display_status']=='waiting'
    assert row['average_mbps'] is None and row['elapsed_seconds'] is None
    assert row['timing_scope']=='prior attempt'


def test_status_timestamps_replace_scope_column():
    snapshot=data();snapshot['tasks'][0]['metrics']['attempt_started_at']=90
    text=render(observe(snapshot,{},100),100)
    assert 'Started UTC' in text and 'Updated UTC' in text
    assert 'Scope' not in text


def test_old_failed_attempt_waits_when_new_run_owns_same_month():
    snapshot=data(state='failed',run_id='old')
    snapshot['runs'][0]['status']='failed'
    current=data(run_id='new')
    snapshot['runs']+=current['runs'];snapshot['tasks']+=current['tasks']
    assert observe(snapshot,{},100)['tasks'][0]['display_status']=='waiting'


def bandwidth_data(stamp,first,second,state='checkpointed'):
    from copy import deepcopy
    snapshot=data(first,state=state)
    snapshot.update(snapshot_at=stamp)
    snapshot['runs'][0]['started_at']=0
    snapshot['tasks'][0]['metrics'].update(attempt_started_at=1,attempt_read_bytes=first)
    other=deepcopy(snapshot['tasks'][0]);other.update(task_id='second',band=13)
    other['metrics'].update(read_bytes=second,attempt_read_bytes=second)
    snapshot['tasks'].append(other)
    return snapshot


def test_aggregate_uses_common_elapsed_time_and_median_of_fresh_intervals():
    cache={}
    first=bandwidth_data(100,100_000_000,100_000_000)
    aggregate_bandwidth(first,cache,100)
    assert first['aggregate_bandwidth']['run_average_mbps']==16
    assert first['aggregate_bandwidth']['recent_median_mbps'] is None
    next_=bandwidth_data(110,110_000_000,120_000_000)
    aggregate_bandwidth(next_,cache,110)
    assert next_['aggregate_bandwidth']['recent_average_mbps']==24
    assert next_['aggregate_bandwidth']['recent_median_mbps']==24
    # Refreshing the same durable snapshot is not an interval of zero transfer.
    aggregate_bandwidth(next_,cache,115)
    assert next_['aggregate_bandwidth']['intervals']==1
    final=bandwidth_data(120,130_000_000,140_000_000,state='complete')
    aggregate_bandwidth(final,cache,120)
    assert final['aggregate_bandwidth']['recent_median_mbps']==28
    assert final['aggregate_bandwidth']['recent_average_mbps']==28
    assert 'run average' in render(final,120)


def test_aggregate_resets_on_counter_reset_and_does_not_mix_resumed_attempts():
    cache={}
    for stamp,value in [(100,100_000_000),(110,110_000_000)]:
        snapshot=bandwidth_data(stamp,value,value);aggregate_bandwidth(snapshot,cache,stamp)
    reset=bandwidth_data(120,1,1);aggregate_bandwidth(reset,cache,120)
    assert reset['aggregate_bandwidth']['intervals']==0
    assert reset['aggregate_bandwidth']['recent_median_mbps'] is None
    previous=data();previous['tasks'][0]['metrics'].update(attempt_started_at=10,attempt_read_bytes=1_000_000)
    previous['runs'][0]['started_at']=200
    aggregate_bandwidth(previous,{},210)
    assert previous['aggregate_bandwidth']['run_average_mbps'] is None


def test_aggregate_deduplicates_physical_task_and_ignores_historical_attempt_metrics():
    from copy import deepcopy
    snapshot=bandwidth_data(100,100_000_000,100_000_000,'complete')
    snapshot['tasks'][0]['attempt_metrics']={'read_bytes':10**15,'write_seconds':1}
    snapshot['tasks'].append(deepcopy(snapshot['tasks'][0]))
    aggregate_bandwidth(snapshot,{},100)
    assert snapshot['aggregate_bandwidth']['run_average_mbps']==16


def test_remote_progress_cache_persists_between_watch_invocations(tmp_path,monkeypatch,capsys):
    progress=tmp_path/'progress.json';cache=tmp_path/'monitor.json'
    for stamp,value in [(100,100_000_000),(110,110_000_000)]:
        progress.write_text(json.dumps(bandwidth_data(stamp,value,value)))
        monkeypatch.setattr('ecore_weather.job_status.time.time',lambda:stamp)
        assert main(['--progress',str(progress),'--cache',str(cache),'--json'],
                    index_path=tmp_path/'index.duckdb',source='goes')==0
        result=json.loads(capsys.readouterr().out)
    assert cache.is_file() and not (tmp_path/'index.duckdb').exists()
    assert result['aggregate_bandwidth']['recent_median_mbps']==16


def test_aggregate_new_task_has_no_counter_jump_and_history_is_bounded():
    cache={}
    for stamp in range(100,180):
        snapshot=bandwidth_data(stamp,stamp*1_000_000,stamp*1_000_000)
        aggregate_bandwidth(snapshot,cache,stamp)
    assert snapshot['aggregate_bandwidth']['intervals']==60
    extra=data()['tasks'][0].copy();extra.update(task_id='unknown-start',metrics={'read_bytes':10**12})
    snapshot['tasks'].append(extra);snapshot['snapshot_at']=180
    aggregate_bandwidth(snapshot,cache,180)
    assert snapshot['aggregate_bandwidth']['intervals']==0
    assert snapshot['aggregate_bandwidth']['run_average_mbps'] is None
