import pytest
from ecore_weather.goes_tuning import choose_readers


def comparison():
    return {'status': 'complete', 'rows': [
        {'read_processes': p, 'source_threads': t, 'satellite': s, 'repeat': r,
         'wall_seconds': seconds, 'values_equal': True, 'peak_rss_bytes': 1024**3}
        for p,t,seconds in [(2,8,100), (4,1,70), (8,1,60)]
        for s in (16,19) for r in (1,2,3)]}


def test_choose_faster_independent_processes():
    result=choose_readers(comparison())
    assert result['read_processes']==8 and result['source_threads']==1
    report=comparison()
    report['rows'] += [{**r,'read_processes':64,'wall_seconds':1}
                      for r in report['rows'] if r['read_processes']==8]
    assert choose_readers(report)['read_processes']==8


def test_no_gain_on_one_satellite_does_not_qualify():
    report=comparison()
    for row in report['rows']:
        if row['satellite']==19 and row['read_processes']!=2:
            row['wall_seconds']=105
    assert choose_readers(report)['read_processes']==2


def test_memory_or_changed_values_excludes_candidate():
    report=comparison()
    for row in report['rows']:
        if row['read_processes']==8:
            row['peak_rss_bytes']=9*1024**3
        if row['read_processes']==4:
            row['values_equal']=False
    assert choose_readers(report)['read_processes']==2


def test_incomplete_evidence_cannot_select_production_counts():
    report=comparison();report['rows']=[r for r in report['rows'] if r['repeat']!=3]
    with pytest.raises(ValueError,match='Three verified'):
        choose_readers(report)
    with pytest.raises(ValueError,match='not complete'):
        choose_readers({'status':'running'})




def test_fingerprint_compares_object_metadata_and_pixels(monkeypatch):
    from contextlib import contextmanager
    import numpy as np
    import xarray as xr
    from ecore_weather import jobs_benchmarks as benchmark
    one=xr.Dataset({'CMI': (('y','x'),np.array([[3]],dtype='int16')),
        'metadata': ('time',np.array(['same calibration'],dtype=object))},coords={'x':[0],'y':[1]})
    two=one.copy(deep=True)
    @contextmanager
    def open_sample(path):
        yield one if path=='one' else two
    monkeypatch.setattr(benchmark,'open_raw',open_sample)
    a=[{'band':13,'path':'one'}];b=[{'band':13,'path':'two'}]
    assert benchmark.content_fingerprint(a)==benchmark.content_fingerprint(b)
    two.CMI.values[0,0]=4
    assert benchmark.content_fingerprint(a)!=benchmark.content_fingerprint(b)


def test_async_staging_can_win_and_requires_scratch_evidence():
    report=comparison()
    report['rows'] += [{**r,'read_mode':'async_full','read_processes':4,'source_threads':1,
        'wall_seconds':40,'download_concurrency':8,'staging_mib':4096,'scratch_peak_bytes':1024**3}
        for r in list(report['rows']) if r['read_processes']==2]
    winner=choose_readers(report)
    assert winner['read_mode']=='async_full'
    assert winner['download_concurrency']==8 and winner['staging_mib']==4096
    for row in report['rows']:
        if row.get('read_mode')=='async_full':row['scratch_peak_bytes']=9*1024**3
    assert choose_readers(report)['read_mode']=='range'
