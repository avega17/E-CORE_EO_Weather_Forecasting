"""Test large-memory policies with synthetic counters, never large allocations."""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from ecore_weather.goes_memory import GIB, MemoryGuard, MemoryPolicy, configure_study, sample_memory
from ecore_weather.goes_limits import IntentionalPause


@pytest.fixture(autouse=True)
def clean_memory_environment(monkeypatch):
    names=('ECORE_RSS_LIMIT_BYTES','ECORE_GOES_MEMORY_POLICY','ECORE_GOES_MEMORY_REPORT',
           'ECORE_SHARED_GOES','ECORE_GOES_PROGRESS','ECORE_GOES_CHECKPOINT_ROOT','ECORE_GOES_CHECKPOINT_FRACTION')
    for name in names:monkeypatch.delenv(name,raising=False)
    yield
    import os
    for name in names:os.environ.pop(name,None)


def snapshot(rss=64,available=400):
    return {'sampled_at':1000,'rss_bytes':int(rss*GIB),'node_available_bytes':int(available*GIB),
            'node_total_bytes':512*GIB,'process_count':1,'largest_processes':[{'pid':123,'rss_bytes':int(rss*GIB),'name':'python'}]}


def production(sampler,report=None):
    return MemoryGuard(384*GIB,MemoryPolicy('checkpoint_pause',256,32,60,5,8),report,sampler)


def test_original_failure_is_below_production_limit_but_strict_benchmark_still_fails():
    values=snapshot();values['rss_bytes']=68720574464  # Exact supplied job failure.
    production(lambda:values).check(now=0)
    with pytest.raises(MemoryError,match='benchmark memory limit'):
        MemoryGuard(64*GIB,sampler=lambda:values).check(now=0)


def test_warning_and_transient_spike_do_not_stop_fetch(capsys):
    values=[snapshot(300),snapshot(385),snapshot(380),snapshot(385),snapshot(300)]
    guard=production(lambda:values.pop(0))
    for now in (0,5,60,65,120):guard.check(now=now)
    assert guard.exceeded_since is None
    assert capsys.readouterr().out.count('memory warning')==1
    assert guard.peak==385*GIB


@pytest.mark.parametrize('rss,available,reason',[(385,100,'summed RSS'),(64,31,'available RAM')])
def test_sustained_pressure_pauses_and_publishes_evidence(tmp_path,rss,available,reason):
    report=tmp_path/'memory.json'
    guard=production(lambda:snapshot(rss,available),report)
    guard.check(now=0);guard.check(now=55)
    with pytest.raises(IntentionalPause,match=reason):guard.check(now=60)
    data=json.loads(report.read_text())
    assert data['latest']['pressure_seconds']==60 and data['policy']['mode']=='checkpoint_pause'
    assert data['latest']['node_total_bytes']==512*GIB


def test_sampling_and_evidence_are_bounded(tmp_path):
    calls=[]
    def sample():calls.append(1);return snapshot()
    guard=production(sample,tmp_path/'memory.json')
    guard.check(now=0,roles={123:'month 2021-08'})
    for now in (1,2,4.9):guard.check(now=now)
    assert len(calls)==1
    assert guard.latest['largest_processes'][0]['role']=='month 2021-08'
    for now in range(5,101,5):guard.check(now=now)
    guard.publish()
    data=json.loads((tmp_path/'memory.json').read_text())
    assert len(data['recent_samples'])==12 and len(calls)==21
    assert not (tmp_path/'memory.json.tmp').exists()


def test_critical_node_pressure_has_no_grace_delay():
    guard=production(lambda:snapshot(64,7))
    with pytest.raises(IntentionalPause,match='critical available RAM'):
        guard.check(now=0)


def test_exiting_child_does_not_fail_measurement(monkeypatch):
    import psutil
    class Process:
        pid=1
        def children(self,recursive):return [SimpleNamespace(pid=2,memory_info=self.vanished)]
        def vanished(self):raise psutil.NoSuchProcess(2)
        def memory_info(self):return SimpleNamespace(rss=10)
        def name(self):return 'python'
    monkeypatch.setattr(psutil,'Process',Process)
    monkeypatch.setattr(psutil,'virtual_memory',lambda:SimpleNamespace(total=512*GIB,available=450*GIB))
    actual=sample_memory()
    assert actual['rss_bytes']==10 and actual['process_count']==1


def test_profile_applies_to_direct_study_but_does_not_override_benchmark(tmp_path,monkeypatch):
    import os
    configure_study('jobs/goes_polaris.json',tmp_path/'memory.json')
    guard=MemoryGuard.from_environment()
    assert guard.limit==384*GIB and guard.policy.mode=='checkpoint_pause'
    monkeypatch.setenv('ECORE_RSS_LIMIT_BYTES',str(48*GIB))
    monkeypatch.setenv('ECORE_GOES_MEMORY_POLICY','{"mode":"strict"}')
    configure_study('jobs/goes_polaris.json',tmp_path/'another.json')
    guard=MemoryGuard.from_environment()
    assert guard.limit==48*GIB and guard.policy.mode=='strict'


def test_memory_pause_uses_exit75_and_compact_reason(tmp_path,monkeypatch):
    from ecore_weather import jobs_goes,jobs_mrms,goes_shared
    monkeypatch.setattr(jobs_mrms,'preflight',lambda _:None)
    def fetch(*args,**kwargs):
        guard=production(lambda:snapshot(385))
        guard.check(now=0);guard.check(now=60)
    monkeypatch.setattr(goes_shared,'fetch_contexts',fetch)
    code=jobs_goes.main(['--start','2021-08-01','--end','2021-09-01',
        '--destination',str(tmp_path),'--output',str(tmp_path/'state'),
        '--scratch',str(tmp_path/'scratch'),'--checkpoint-dir',str(tmp_path/'durable'),
        '--shared-profile','jobs/goes_polaris.json'])
    assert code==75
    paused=json.loads((tmp_path/'state/paused.json').read_text())
    assert 'sustained memory pressure' in paused['reason']
    assert not (tmp_path/'state/failure.json').exists()


def test_memory_report_in_durable_progress_and_readable_status(tmp_path,monkeypatch):
    from ecore_weather import jobs,runlog,job_status
    report=tmp_path/'memory.json'
    guard=production(lambda:snapshot(),report);guard.check(now=0)
    monkeypatch.setenv('ECORE_GOES_MEMORY_REPORT',str(report))
    monkeypatch.setattr(jobs,'status',lambda *a,**k:{'runs':[],'tasks':[]})
    destination=tmp_path/'progress.json'
    runlog.publish_progress('goes',destination)
    data=json.loads(destination.read_text())
    rendered=job_status.render(data,1005)
    assert 'summed RSS 64.0 GiB' in rendered and 'node available 400.0 GiB' in rendered


def test_polari_goes_wrapper_separates_study_and_pilot_policy():
    # The full shell tests also execute the wrapper with mocked PBS commands.
    script=Path('jobs/polaris_goes.pbs').read_text()
    assert 'pilot_memory_gib' in script and 'ECORE_GOES_MEMORY_POLICY' in script
    assert 'ECORE_GOES_MEMORY_REPORT="$state_root/memory.json"' in script
