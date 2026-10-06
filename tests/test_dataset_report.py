import json
import numpy as np
import pytest
import xarray as xr
from ecore_weather import dataset_report as report
from ecore_weather.common import write_json
from ecore_weather.earth2_io import write_dataset


def test_quality_unsigned_fill_and_no_valid():
    ds=xr.Dataset({'CMI_C13':(('y','x'),np.array([[0,-2,-1,4]],dtype='int16')),
                   'DQF_C13':(('y','x'),np.array([[0,1,0,3]],dtype='int8'))})
    ds.CMI_C13.attrs.update(_Unsigned='true',_FillValue=-1,scale_factor=.1,add_offset=1,valid_range=[0,-2])
    s=report.pixel_statistics(ds,'CMI_C13')
    assert s['strict_good']==1 and s['good_plus_conditional']==2 and s['fill']==1
    assert s['conditional_stats']['maximum']==65534*.1+1
    unknown=report.pixel_statistics(ds.drop_vars('DQF_C13'),'CMI_C13')
    assert unknown['quality_unknown']==4 and unknown['strict_stats']['median'] is None
    empty=report.pixel_statistics(ds,'CMI_C13',np.zeros((1,4),dtype=bool))
    assert empty['pixels']==0 and empty['fill_percent'] is None


def test_radar_valid_negatives_bitmap_and_ambiguous_zero():
    ds=xr.Dataset({'measurement':(('latitude','longitude'),np.array([[-99,-999,-10,0,3]],dtype='float32')),
        'bitmap_valid':(('latitude','longitude'),[[1,1,1,1,0]])},attrs={'product':'MergedReflectivityQCComposite_00.50'})
    s=report.pixel_statistics(ds,'measurement')
    assert s['fill']==1 and s['no_coverage']==1 and s['bitmap_missing']==1 and s['valid_negative']==1
    ds.attrs['product']='MergedAzShear_0-2kmAGL_00.50'
    assert report.pixel_statistics(ds,'measurement')['ambiguous_shear_zero']==1


def test_audit_resume_and_inventory_physical_dedup(tmp_path,monkeypatch):
    folder=tmp_path/'data/mrms/MultiSensor_QPE_01H_Pass2_00.00/roi-test/2022/09';folder.mkdir(parents=True)
    ds=xr.Dataset({'measurement':(('time','latitude','longitude'),np.array([[[0.,-1.]],[[2.,3.]]],dtype='float32')),
                   'bitmap_valid':(('time','latitude','longitude'),np.ones((2,1,2),dtype='uint8'))},
        coords={'time':np.array(['2022-09-18T00:00','2022-09-18T01:00'],dtype='datetime64[ns]'),
                'latitude':[18.],'longitude':[-67.,-66.]},attrs={'source':'mrms','product':'MultiSensor_QPE_01H_Pass2_00.00'})
    # Local archive reader also accepts directory stores, but hashing acceptance uses ZIPs.
    import zipfile,shutil
    working=folder/'raw.zarr';write_dataset(ds,working)
    path=folder/'raw.zarr.zip'
    with zipfile.ZipFile(path,'w') as zipped:
        for p in working.rglob('*'):
            if p.is_file():zipped.write(p,p.relative_to(working))
    shutil.rmtree(working)
    assets=[{'asset_id':str(i),'time':str(t)+'Z','source_url':f'https://noaa-mrms-pds.s3.amazonaws.com/test{i}','source_bytes':10,'etag':'x'} for i,t in enumerate(ds.time.values)]
    write_json(folder/'complete.json',{'raw_path':path.name,'source':'mrms','product':ds.attrs['product'],'assets':assets,
        'stored_bytes':path.stat().st_size,'observations':2,'archive_sha256':report.file_hash(path)})
    summary,rows=report.inventory(tmp_path/'data',tmp_path/'out')
    assert summary['archives']==1 and summary['listed_compressed_source_bytes']==20
    first=report.diagnose(tmp_path/'data',tmp_path/'out',full=True,patches={})
    second=report.diagnose(tmp_path/'data',tmp_path/'out',full=True,patches={})
    assert first['processed']==2 and second['processed']==0 and second['reused']==2
    assert report.export_report(tmp_path/'out')['diagnostic_rows']==2
    for unit in ('days','weeks','months','years'):
        assert (tmp_path/'out'/f'diagnostic-{unit}.csv').is_file()
    fig,stats=report.coverage_map(tmp_path/'data',tmp_path/'out')
    assert stats['pixels']==2 and (tmp_path/'out'/'coverage-example.png').is_file()
    import matplotlib.pyplot as plt
    plt.close(fig)
    previous_provenance=report.provenance
    monkeypatch.setattr(report,'provenance',lambda:{**previous_provenance(),'code_hash':'changed-diagnostic-code'})
    refreshed=report.diagnose(tmp_path/'data',tmp_path/'out',full=True,patches={})
    assert refreshed['processed']==2 and refreshed['reused']==0
    assert report.export_report(tmp_path/'out')['diagnostic_rows']==2
    path.write_bytes(path.read_bytes()+b'corrupt')
    with pytest.raises(IOError,match='size differs|hash differs'):report.diagnose(tmp_path/'data',tmp_path/'out',full=True,patches={})


def test_aggregate_moments_are_pixel_weighted_and_html_embeds_plotly_once(tmp_path):
    from ecore_weather.report_views import diagnostic_table,export
    code=report.provenance()['code_hash']
    with report.connection(tmp_path) as con:
        con.execute('INSERT INTO audits VALUES (?,?,?,?,?,?)',['a','path','sha',code,'sample_complete','{}'])
        for hour,n,mean,std in [(0,2,1.,1.),(1,6,5.,2.)]:
            payload={'pixels':n,'strict_good':n,'strict_stats':{'mean':mean,'std':std,'minimum':0,'maximum':9,'q25':1,'median':2,'q75':3}}
            con.execute('INSERT INTO diagnostics VALUES (?,?,?,?,?,?,?)',['a','path','test',None,f'2021-01-01 {hour:02}:00:00','Whole ROI',json.dumps(payload)])
    row=diagnostic_table(tmp_path).iloc[0]
    assert row['mean']==4.0 and row['strict_good_percent']==100
    assert row['std']==pytest.approx((6.25)**.5)
    assert 'median' not in row.index
    export(tmp_path)
    html=(tmp_path/'report.html').read_text()
    assert html.count('plotly.js v')==1
    assert 'Per-observation distributions' in html


def test_mrms_reference_comparison_distinguishes_bitmap_and_coordinates():
    from ecore_weather.report_benchmarks import _mrms_compare_frames
    from ecore_weather.common import Asset
    asset = Asset('noaa-mrms-pds', 'test', 10, 'etag', '2024-09-01T00:00:00Z')
    values = np.array([[[[-99., -999., -2., np.nan]]]])
    ref = xr.DataArray(values, dims=('time', 'variable', 'lat', 'lon'),
        coords={'time': [np.datetime64('2024-09-01')], 'variable': ['refc'],
                'lat': [18.], 'lon': [292., 293., 294., 295.],
                'actual_time_refc': ('time', [np.datetime64('2024-09-01')])})
    frame = {'time': asset.time, 'values': np.array([[-99., -999., -2., 9999.]]),
             'bitmap': np.array([[1, 1, 1, 0]]), 'latitude': np.array([18.]),
             'longitude': np.array([292., 293., 294., 295.])}
    result = _mrms_compare_frames(ref, [frame], [asset], (-69., 17., -64., 19.))
    assert result['valid_pixels_equal'] and result['actual_times_equal']
    assert result['maximum_coordinate_difference_degrees'] == 0
    frame['longitude'] = frame['longitude'] + 9.2e-7
    assert _mrms_compare_frames(ref, [frame], [asset], (-69., 17., -64., 19.))['maximum_coordinate_difference_degrees'] > 0
    frame['longitude'] = frame['longitude'] + 1e-4
    with pytest.raises(AssertionError, match='longitude differs'):
        _mrms_compare_frames(ref, [frame], [asset], (-69., 17., -64., 19.))
    frame['longitude'] = np.array([292., 293., 294., 295.])
    frame['values'][0, 2] = 2  # Valid negative reflectivity cannot be masked.
    with pytest.raises(AssertionError):
        _mrms_compare_frames(ref, [frame], [asset], (-69., 17., -64., 19.))


def test_mrms_source_benchmark_requires_pause_before_artifacts(tmp_path, monkeypatch):
    from ecore_weather import jobs_policy
    from ecore_weather.report_benchmarks import mrms_source_main
    output = tmp_path/'should-not-exist.json'
    def active():
        raise RuntimeError('active coordinator')
    monkeypatch.setattr(jobs_policy, 'ensure_paused', active)
    with pytest.raises(RuntimeError, match='active coordinator'):
        mrms_source_main(['--output', str(output)])
    assert not output.exists()


def test_operational_cli_routes_mrms_benchmark(monkeypatch):
    from ecore_weather import jobs, report_benchmarks
    calls = []
    monkeypatch.setattr(report_benchmarks, 'mrms_source_main', lambda args: calls.append(args) or 0)
    assert jobs.main(['benchmark', 'mrms', '--frames-per-month', '4']) == 0
    assert calls == [['--frames-per-month', '4']]


def test_mrms_source_benchmark_offline_round_trip(tmp_path, monkeypatch):
    """Exercise the CLI/order/cache cleanup with fake transport, not live evidence."""
    import concurrent.futures
    import importlib
    import os
    from ecore_weather import jobs_policy, mrms
    from ecore_weather.common import PR_BBOX
    from ecore_weather.report_benchmarks import mrms_source_main
    module = importlib.import_module('earth2studio.data.mrms')
    monkeypatch.setattr(jobs_policy, 'ensure_paused', lambda: None)
    class Reference:
        MRMS_REGION = 'CONUS'
        def __init__(self, **kwargs):
            pass
        def __call__(self, times, variables):
            lat, lon = ([40., 39.], [260., 261.]) if self.MRMS_REGION == 'CONUS' else ([18.], [293., 294.])
            values = np.full((len(times), 1, len(lat), len(lon)), -2., dtype='float64')
            stamps = np.array(times, dtype='datetime64[s]')
            return xr.DataArray(values, dims=('time', 'variable', 'lat', 'lon'),
                coords={'time': stamps, 'variable': variables, 'lat': lat, 'lon': lon,
                        'actual_time_refc': ('time', stamps)})
    monkeypatch.setattr(module, 'MRMS', Reference)
    monkeypatch.setattr(concurrent.futures, 'ProcessPoolExecutor',
                        lambda max_workers, **kw: concurrent.futures.ThreadPoolExecutor(max_workers=max_workers))
    def read(asset, bbox, product, transport):
        ds = xr.Dataset({'measurement': (('latitude', 'longitude'), [[-2., -2.]]),
                         'bitmap_valid': (('latitude', 'longitude'), [[1, 1]])},
                        coords={'latitude': [18.], 'longitude': [293., 294.]})
        return ds, {'download_s': 0., 'decode_crop_s': 0.}
    monkeypatch.setattr(mrms, 'read', read)
    for month in ['2022-09', '2024-09', '2025-09', '2026-06']:
        year, mm = month.split('-')
        folder = tmp_path/'data/mrms/MergedReflectivityQCComposite_00.50/roi-test'/year/mm
        folder.mkdir(parents=True)
        assets = [{'time': f'{month}-01T00:{i*10:02d}:00Z', 'source_bytes': 1, 'etag': 'x',
                   'source_url': f'https://noaa-mrms-pds.s3.amazonaws.com/CARIB/MergedReflectivityQCComposite_00.50/{year}{mm}01/sample{i}.gz'} for i in range(2)]
        (folder/'complete.json').write_text(json.dumps({'region': list(PR_BBOX), 'assets': assets}))
    output = tmp_path/'summary.json'
    prior = os.environ.get('EARTH2STUDIO_DATA_CACHE')
    assert mrms_source_main(['--location', str(tmp_path/'data'), '--output', str(output),
                             '--frames-per-month', '2']) == 0
    data = json.loads(output.read_text())
    assert len(data['rows']) == 7 and data['equality']['valid_pixels_equal']
    assert set(data['median_seconds']) == {'project_CARIB', 'stock_CARIB'}
    assert os.environ.get('EARTH2STUDIO_DATA_CACHE') == prior
