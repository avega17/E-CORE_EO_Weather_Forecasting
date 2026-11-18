from __future__ import annotations

import pytest
from ecore_weather.jobs.jobs_policy import choose_readers


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
    from ecore_weather.reporting import jobs_benchmarks as benchmark
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


import h5py
import numpy as np
import xarray as xr

from ecore_weather.reporting.study_goes import write_uncompressed_netcdf
from ecore_weather.reporting.study_goes import SCENARIOS, UNCOMPRESSED_NETCDF_METHOD, summarize
from ecore_weather.reporting.report_benchmarks import roi_options


def test_uncompressed_crop_discards_source_compression_preserves_packed_values(tmp_path):
    values = np.full((100, 100), 42, dtype='int16')
    ds = xr.Dataset({'CMI': (('y','x'), values,
        {'scale_factor': .1, 'add_offset': 20., '_FillValue': np.int16(-1)})})
    ds.CMI.encoding = {'zlib': True, 'complevel': 9, 'chunksizes': (100, 100)}
    path = tmp_path/'crop.nc'
    write_uncompressed_netcdf(ds, path)
    with h5py.File(path) as source:
        assert source['CMI'].compression is None
        assert source['CMI'].dtype == values.dtype
        np.testing.assert_array_equal(source['CMI'][:], values)
        assert source['CMI'].attrs['scale_factor'] == .1
        assert source['CMI'].attrs['_FillValue'] == -1
    assert ds.CMI.encoding['zlib'] is True


def test_roi_expansions_retain_current_geographic_support():
    options = roi_options()
    w,s,e,n = options['current']
    for larger in options.values():
        assert larger[0] <= w and larger[1] <= s
        assert larger[2] >= e and larger[3] >= n
    assert options['east_to_60w'][2] == -60.


def test_old_compressed_crop_cache_cannot_supply_uncompressed_estimate():
    scenarios = {name: {'band_files': 1, 'listed_full_file_bytes': 200,
        'nominal_band_file_opportunities': 1, 'nominal_missing_band_files': 0,
        'distinct_scan_times': 1, 'by_satellite_band': {'16:C01': 1}}
        for name, _, _ in SCENARIOS}
    month = {'scenarios': scenarios, 'inventory_seconds': 1}
    sample = {'satellite': 16, 'band': 1, 'roi_transfer_bytes': 100,
        'uncompressed_crop_netcdf_bytes': 50, 'compressed_zarr_chunk_bytes': 20,
        'read_seconds': 1, 'write_seconds': 1}
    old = summarize([month], [sample])['scenarios']['eight_6ph']
    assert old['estimated_uncompressed_crop_netcdf_bytes']['central'] is None
    assert old['estimated_compressed_zarr_bytes']['central'] == 2068
    sample['netcdf_size_method'] = UNCOMPRESSED_NETCDF_METHOD
    fresh = summarize([month], [sample])['scenarios']['eight_6ph']
    assert fresh['estimated_uncompressed_crop_netcdf_bytes']['central'] == 50
