"""Monitor measurements must have honest scopes and survive completion/locks."""
import json

import pytest

from ecore_weather.job_status import observe,filter_tasks,render,main


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
