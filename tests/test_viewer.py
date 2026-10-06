"""Viewer regressions for monthly stores and annual HF backup containers."""

from contextlib import redirect_stdout
import hashlib
import io
import json
from unittest.mock import patch
import zipfile
import pytest

from ecore_weather import view_backup, view_frames, view_index, view_storage, viewer


def test_widget_find_selects_mrms_and_goes_records():
    rows = [{'dataset': '/data/mrms/PrecipRate_00.00/roi-abc',
             'time': '2022-07-01T00:00:00Z', 'path': '/fake/raw.zarr.zip',
             'source': 'mrms', 'band': None}]
    with (patch.object(view_index, 'months_available', return_value=[]),
          patch.object(view_index, 'inventory', side_effect=lambda *args: rows),
          redirect_stdout(io.StringIO())):
        panel = viewer.controls()
        controls = panel._ecore_controls
        controls['search'].click()
        assert controls['dataset'].value == 'PrecipRate_00.00'
        assert len(controls['state']['selected']) == 1
        assert not controls['single'].disabled
        assert controls['band'].disabled
        controls['source'].value = 'goes'
        rows[:] = [{'dataset': f'/data/goes/ABI-L2-CMIPF/goes16/C{band:02d}/roi-{roi}',
                    'time': '2022-07-01T00:00:00Z', 'path': f'/fake/C{band:02d}.zarr.zip',
                    'source': 'goes', 'band': band}
                   for band, roi in [(8, 'abc'), (13, 'def')]]
        controls['search'].click()
        assert controls['dataset'].value == 'ABI-L2-CMIPF/goes16'
        assert {value for _, value in controls['band'].options} == {8, 13}
        assert not controls['band'].disabled
        assert not controls['single'].disabled
        assert len(controls['state']['selected']) == 1
        controls['band'].value = 8
        assert controls['state']['selected'][0]['band'] == 8
        # The preview resolves its inputs from the current dataset/band controls,
        # so an unchanged dropdown value cannot leave an empty stale selection.
        rows_for_view = viewer.resolve_dataset_rows(
            controls['state']['records'], controls['dataset'].value, 'goes', controls['band'].value)
        assert len(rows_for_view) == 1 and rows_for_view[0]['band'] == 8


def test_single_preview_re_resolves_current_dataset_after_stale_selection():
    rows = [{'dataset': '/data/mrms/PrecipRate_00.00/roi-abc',
             'time': '2022-07-01T00:00:00Z', 'path': '/fake/raw.zarr.zip',
             'source': 'mrms', 'band': None}]
    with (patch.object(view_index, 'months_available', return_value=[]),
          patch.object(view_index, 'inventory', return_value=rows),
          patch('IPython.display.display'),
          patch.object(view_frames, 'prepare', return_value=[{'time': '2022-07-01T00:00:00Z'}]) as prepare,
          patch.object(view_frames, 'portable_map', return_value='map'),
          redirect_stdout(io.StringIO())):
        panel = viewer.controls()
        controls = panel._ecore_controls
        controls['search'].click()
        controls['state']['selected'] = []  # Simulate a widget refresh racing the old cache.
        assert not controls['single'].disabled
        controls['single'].click()
        assert prepare.call_args.args[0] == rows
        assert not controls['export'].disabled


def test_all_preview_tabs_use_portable_renderer_without_leaflet():
    rows = [{'dataset': '/data/mrms/PrecipRate_00.00/roi-abc',
             'time': '2022-10-01T00:00:00Z', 'path': '/fake/raw.zarr.zip',
             'source': 'mrms', 'band': None}]
    with (patch.object(view_index, 'months_available', return_value=[]),
          patch.object(view_index, 'inventory', return_value=rows),
          patch('IPython.display.display'),
          patch.object(view_frames, 'prepare', return_value=[{'time': rows[0]['time']}]),
          patch.object(view_frames, 'portable_map', return_value='portable map') as portable,
          patch.object(view_frames, 'leaflet', side_effect=AssertionError('Leaflet should not load')),
          redirect_stdout(io.StringIO())):
        controls = viewer.controls()._ecore_controls
        controls['search'].click()
        controls['single'].click()
        controls['day'].click()
        controls['multi'].click()
        assert portable.call_count == 3
        assert not controls['export'].disabled


def test_empty_search_disables_visualization_controls():
    with (patch.object(view_index, 'months_available', return_value=[]),
          patch.object(view_index, 'inventory', return_value=[]),
          redirect_stdout(io.StringIO())):
        panel = viewer.controls()
        controls = panel._ecore_controls
        controls['search'].click()
        assert 'No matches' in controls['status'].value
        assert controls['single'].disabled
        assert controls['day'].disabled
        assert controls['multi'].disabled


def test_storage_summary_reads_markers_without_zarr_chunks(tmp_path):
    folder = tmp_path / 'mrms' / 'PrecipRate_00.00' / 'roi-abc' / '2021' / '01'
    folder.mkdir(parents=True)
    (folder / 'raw.zarr.zip').write_bytes(b'123456')
    (folder / 'complete.json').write_text(json.dumps({'source': 'mrms',
        'product': 'PrecipRate_00.00', 'raw_path': 'raw.zarr.zip', 'stored_bytes': 6,
        'observations': 2, 'assets': [{'source_bytes': 20}, {'source_bytes': 10}]}))
    rows = view_storage.local_archives(tmp_path, 'mrms')
    assert len(rows) == 1
    assert view_storage.totals(rows)['source_bytes'] == 30
    assert view_storage.totals(rows)['stored_bytes'] == 6
    assert '20.0%' in view_storage.ratio_text(view_storage.totals(rows))
    with patch.object(view_index, 'months_available', return_value=[]), redirect_stdout(io.StringIO()):
        panel = viewer.controls()
        controls = panel._ecore_controls
        controls['location'].value = str(tmp_path)
        controls['sizes'].click()
    assert controls['size_product'].value == 'All'
    assert 'PrecipRate_00.00' in controls['size_product'].options
    assert '20.0% of source' in controls['size_details'].value
    assert '20.0% of source' in controls['size_overview'].value
    assert 'NOAA source object and compressed Zarr storage sizes' in controls['size_chart'].value
    controls['size_product'].value = 'PrecipRate_00.00'
    assert '2021' in controls['size_year'].options


def test_storage_totals_report_partial_original_size_coverage():
    summary = view_storage.totals([
        {'stored_bytes': 20, 'source_bytes': 100, 'observations': 1},
        {'stored_bytes': 30, 'source_bytes': None, 'observations': 2},
    ])
    assert summary['source_bytes'] is None
    assert summary['known_source_bytes'] == 100
    assert summary['source_coverage'] == 0.5
    assert view_storage.ratio_text(summary) == '20.0% of source (80.0% smaller)'


def test_archive_source_sizes_are_not_reported_as_complete_when_assets_are_partial(tmp_path):
    folder = tmp_path / 'mrms' / 'product' / 'roi-abc' / '2022' / '01'
    folder.mkdir(parents=True)
    (folder / 'raw.zarr.zip').write_bytes(b'archive')
    (folder / 'complete.json').write_text(json.dumps({'source': 'mrms',
        'product': 'product', 'raw_path': 'raw.zarr.zip', 'stored_bytes': 7,
        'observations': 2, 'assets': [{'source_bytes': 30}, {'source_bytes': None}]}))
    row = view_storage.local_archives(tmp_path, 'mrms')[0]
    assert row['source_bytes'] is None
    assert row['known_source_bytes'] == 30
    assert row['source_size_coverage'] == 0.5
    summary = view_storage.totals([row])
    assert summary['source_bytes'] is None
    assert summary['source_coverage'] == 0
    assert summary['listed_asset_size_coverage'] == 0.5
    assert summary['known_asset_source_bytes'] == 30
    assert view_storage.ratio_text(summary) == 'unavailable'


def test_prepare_opens_each_monthly_store_once_and_preserves_requested_order():
    from contextlib import contextmanager
    import numpy as np
    import xarray as xr

    datasets = {
        'same-month.zip': xr.Dataset(coords={'time': np.array([
            '2022-01-01T00:00', '2022-01-01T00:10', '2022-01-02T00:00'], dtype='datetime64[m]')}),
        'next-month.zip': xr.Dataset(coords={'time': np.array([
            '2022-02-01T00:00'], dtype='datetime64[m]')}),
    }
    opens = []

    @contextmanager
    def fake_open(record, select_time=True):
        path = record['path']
        opens.append(path)
        assert not select_time
        yield datasets[path]

    records = [
        {'path': 'same-month.zip', 'time': '2022-01-02T00:00:00Z'},
        {'path': 'same-month.zip', 'time': '2022-01-01T00:00:00Z'},
        {'path': 'next-month.zip', 'time': '2022-02-01T00:00:00Z'},
    ]
    progress = []
    with (patch.object(view_frames, 'open_observation', side_effect=fake_open),
          patch.object(view_frames, '_frame_dataset', side_effect=lambda ds, row, path, *a:
                       {'path': path, 'time': row['time'], 'bbox': (1, 2, 3, 4)})):
        result = view_frames.prepare(records, progress=lambda done,total,info:
                                      progress.append((done,total,info['status'])))
    assert opens == ['same-month.zip', 'next-month.zip']
    assert [item['time'] for item in result] == [row['time'] for row in records]
    assert progress == [(1,3,'prepared'),(2,3,'prepared'),(3,3,'prepared')]


def test_prepare_reads_each_mrms_timestamp_from_the_full_monthly_store():
    import numpy as np
    import xarray as xr

    datasets = {
        'same-month.zip': xr.Dataset(
            {'measurement': (('time','y','x'), np.array([[[10]],[[20]],[[30]]],dtype='float32'))},
            coords={'time': np.array(['2022-01-01T00:00','2022-01-01T00:10',
                                      '2022-01-01T00:20'],dtype='datetime64[m]'),
                    'y':[18.0], 'x':[-66.0]}),
        'next-month.zip': xr.Dataset(
            {'measurement': (('time','y','x'), np.array([[[40]]],dtype='float32'))},
            coords={'time': np.array(['2022-02-01T00:00'],dtype='datetime64[m]'),
                    'y':[18.0], 'x':[-66.0]}),
    }
    opened = []

    def fake_open(path):
        opened.append(path)
        return datasets[path]

    records = [
        {'path':'same-month.zip','time':'2022-01-01T00:10:00Z'},
        {'path':'same-month.zip','time':'2022-01-01T00:00:00Z'},
        {'path':'next-month.zip','time':'2022-02-01T00:00:00Z'},
    ]
    with (patch.object(view_frames, 'open_raw', side_effect=fake_open),
          patch.object(view_frames, '_frame_dataset', side_effect=lambda ds,row,path,*a:
              {'path':path,'time':row['time'],'bbox':(1,2,3,4),
               'value':float(ds.measurement.values.flat[0])})):
        result = view_frames.prepare(records)
    assert opened == ['same-month.zip','next-month.zip']
    assert [frame['value'] for frame in result] == [20,10,40]


def test_leaflet_play_updates_preloaded_mrms_overlay(monkeypatch):
    """The Play control must send frame indices to the Python URL updater."""
    import ipywidgets as widgets
    from types import SimpleNamespace

    overlays = []

    class FakeMap(widgets.HTML):
        def __init__(self, **kwargs):
            super().__init__()
            self.layers = []
        def add(self, layer):
            self.layers.append(layer)
        def fit_bounds(self, bounds):
            pass

    class FakeOverlay:
        def __init__(self, **kwargs):
            self.url = kwargs['url']
            overlays.append(self)

    monkeypatch.setattr('ipyleaflet.Map', FakeMap)
    monkeypatch.setattr('ipyleaflet.ImageOverlay', FakeOverlay)
    monkeypatch.setattr('ipyleaflet.LayersControl', lambda **kwargs: widgets.HTML())
    monkeypatch.setattr('ipyleaflet.basemaps', SimpleNamespace(
        OpenStreetMap=SimpleNamespace(Mapnik=object())))
    import numpy as np
    frames = [
        {'values': values, 'bbox': (-68., 17., -65., 20.),
         'extent': (0., 1., 0., 1.), 'time': f'2023-08-01T00:0{i}:00Z',
         'label': 'measurement', 'units': 'dBZ'}
        for i, values in enumerate((np.array([[0., 1.]]), np.array([[0., 0.]]),
                                    np.array([[1., 1.]])))
    ]
    panel = view_frames.leaflet(frames)
    play, slider = panel.children[2].children
    assert len(overlays) == 1
    initial_url = overlays[0].url
    play.value = 1
    assert slider.value == 1
    assert overlays[0].url != initial_url
    assert '00:01:00Z' in panel.children[0].value
    second_url = overlays[0].url
    play.value = 2
    assert slider.value == 2
    assert overlays[0].url != second_url
    assert '00:02:00Z' in panel.children[0].value
    slider.value = 0
    assert play.value == 0
    assert overlays[0].url == initial_url


def test_portable_map_plays_without_leaflet_frontend(monkeypatch):
    import numpy as np
    from ecore_weather import maps

    monkeypatch.setattr(maps, '_land_polygons', lambda: [])
    frames = [
        {'values': np.array([[float(i), np.nan], [0., 1.]], dtype='float32'),
         'bbox': (-68., 17., -65., 20.), 'extent': (-7.6e6, -7.2e6, 1.9e6, 2.3e6),
         'time': f'2023-08-01T00:0{i}:00Z', 'label': 'measurement', 'units': 'dBZ'}
        for i in range(2)
    ]
    panel = view_frames.portable_map(frames)
    picture = panel.children[1]
    assert 'data:image/png;base64,' in picture.value
    assert 'jupyter-leaflet' not in picture.value
    initial = picture.value
    zoom, play, slider = panel.children[2].children
    play.value = 1
    assert slider.value == 1 and picture.value != initial
    assert '00:01:00Z' in panel.children[0].value
    zoom.value = 150
    assert 'width:3px' in picture.value


def test_earth2_monthly_view_selection_is_source_specific(tmp_path):
    """Current product/region/month archives remain selectable without legacy IDs."""
    import numpy as np
    import xarray as xr
    from ecore_weather.earth2_io import write_dataset

    mrms_folder = tmp_path / 'mrms' / 'PrecipRate_00.00' / 'roi-new' / '2022' / '10'
    mrms_folder.mkdir(parents=True)
    mrms = xr.Dataset({'measurement': (('time', 'y', 'x'), np.ones((2, 3, 4), 'float32'),
                          {'units': 'mm h-1'}),
                       'bitmap_valid': (('time', 'y', 'x'), np.ones((2, 3, 4), 'uint8'))},
                      coords={'time': np.array(['2022-10-01T00:00', '2022-10-01T00:10'], dtype='datetime64[m]'),
                              'y': [18., 18.1, 18.2], 'x': [-67., -66.9, -66.8, -66.7]})
    mrms.attrs['requested_bbox'] = [-67., 18., -66.8, 18.2]
    write_dataset(mrms, mrms_folder / 'raw.zarr')
    (mrms_folder / 'complete.json').write_text(json.dumps({'source': 'mrms',
        'product': 'PrecipRate_00.00', 'raw_path': 'raw.zarr', 'observations': 2,
        'assets': [{'asset_id': 'a', 'time': '2022-10-01T00:00:00Z'},
                   {'asset_id': 'b', 'time': '2022-10-01T00:10:00Z'}]}))

    rows = view_index.inventory(tmp_path, 'mrms', '2022-10-01', '2022-10-02')
    assert len(rows) == 2
    assert {viewer.logical_dataset(row) for row in rows} == {'PrecipRate_00.00'}
    assert viewer.resolve_dataset_rows(rows, 'PrecipRate_00.00', 'mrms') == rows
    # A same-named selection from the other source must never collide.
    assert viewer.resolve_dataset_rows(rows, 'PrecipRate_00.00', 'goes') == []


def test_local_find_uses_read_only_index_and_falls_back_during_write(tmp_path, monkeypatch):
    from ecore_weather import index

    folder = tmp_path / 'mrms' / 'PrecipRate_00.00' / 'roi-test' / '2022' / '10'
    folder.mkdir(parents=True)
    archive = folder / 'raw.zarr.zip'
    archive.write_bytes(b'archive')
    (folder / 'complete.json').write_text(json.dumps({
        'source': 'mrms', 'product': 'PrecipRate_00.00', 'raw_path': archive.name,
        'assets': [{'asset_id': 'scan-a', 'time': '2022-10-01T00:00:00Z'}],
    }))
    database = tmp_path / 'archive-index.duckdb'
    monkeypatch.setenv('ECORE_INDEX_PATH', str(database))
    with index.connect(database) as db:
        db.execute('INSERT INTO archives VALUES (?,?,?,?,?,?,?,?,?,?,?)', index._archive_row(archive))
        # DuckDB cannot open a read-only connection while this process holds a
        # read-write one. The viewer must still find the completed manifest.
        assert len(view_index.inventory(tmp_path, 'mrms', '2022-10-01', '2022-10-02')) == 1
    indexed = index.search(tmp_path, 'mrms', '2022-10-01', '2022-10-02')
    assert len(indexed) == 1 and indexed[0]['asset_id'] == 'scan-a'


def test_local_find_refreshes_after_a_fetch_completes():
    earlier = {'dataset': '/data/mrms/PrecipRate_00.00/roi-a',
               'time': '2022-10-01T00:00:00Z', 'path': '/data/first.zarr.zip',
               'source': 'mrms', 'band': None}
    later = {**earlier, 'time': '2022-10-01T00:10:00Z', 'path': '/data/second.zarr.zip'}
    calls = []

    def inventory(*args):
        calls.append(args)
        return [earlier] if len(calls) == 1 else [earlier, later]

    with (patch.object(view_index, 'months_available', return_value=[]),
          patch.object(view_index, 'inventory', side_effect=inventory),
          redirect_stdout(io.StringIO())):
        controls = viewer.controls()._ecore_controls
        controls['search'].click()
        assert len(controls['state']['records']) == 1
        controls['search'].click()
        assert len(controls['state']['records']) == 2
    assert len(calls) == 2


def test_storage_explorer_inspects_selected_hf_year_and_reports_ratio(monkeypatch):
    bundle = {'source': 'mrms', 'product': 'PrecipRate_00.00', 'year': 2022,
        'key': 'bundle-key', 'stored_bytes': 150}
    monthly = [{'product': 'PrecipRate_00.00', 'year': '2022', 'month': '10',
        'stored_bytes': 100, 'source_bytes': 400, 'observations': 6,
        'path': 'monthly member', 'kind': 'monthly member in annual HF backup'}]
    monkeypatch.setattr(view_backup, 'list_bundles', lambda *args: [bundle])
    monkeypatch.setattr(view_backup, 'inspect_bundle', lambda selected: [
        {'local_path': 'mrms/PrecipRate_00.00/roi-a/2022/10/raw.zarr.zip',
         'month': '2022-10', 'member': 'monthly member', 'observations': 6,
         'stored_bytes': 100, 'source_bytes': 400}])
    monkeypatch.setattr(view_storage, 'backup_archives', lambda selected, rows: monthly)
    with (patch.object(view_index, 'months_available', return_value=[]),
          redirect_stdout(io.StringIO())):
        panel = viewer.controls()
        controls = panel._ecore_controls
        controls['storage_kind'].value = 'Hugging Face'
        controls['sizes'].click()
        controls['size_product'].value = 'PrecipRate_00.00'
        controls['size_year'].value = '2022'
        controls['size_month'].value = '2022-10'
        assert '25.0% of source' in controls['size_details'].value
        assert 'compressed Zarr' in controls['size_overview'].value


def test_new_goes_monthly_earth2studio_store_renders(tmp_path):
    pytest.importorskip('earth2studio')
    import numpy as np
    import xarray as xr
    from ecore_weather.earth2_io import write_dataset
    from ecore_weather import view_frames

    folder = tmp_path / 'goes' / 'ABI-L2-CMIPF' / 'goes16' / 'C13' / 'roi-abc' / '2022' / '09'
    folder.mkdir(parents=True)
    ds = xr.Dataset({'CMI_C13': (('time', 'y', 'x'), np.full((1, 4, 4), 100, 'int16'),
                     {'scale_factor': 0.1, 'add_offset': 200.0, 'units': 'K', '_FillValue': -1}),
                     'DQF_C13': (('time', 'y', 'x'), np.zeros((1, 4, 4), 'int8'))},
                    coords={'time': [np.datetime64('2022-09-15T00:00:20')],
                            'x': np.linspace(0.01, 0.02, 4), 'y': np.linspace(0.04, 0.05, 4)})
    ds['goes_imager_projection'] = ((), 0, {'grid_mapping_name': 'geostationary',
        'semi_major_axis': 6378137.0, 'semi_minor_axis': 6356752.31414,
        'perspective_point_height': 35786023.0,
        'longitude_of_projection_origin': -75.0,
        'latitude_of_projection_origin': 0.0, 'sweep_angle_axis': 'x'})
    ds.attrs['requested_bbox'] = [-70.24, 14.36, -62.56, 22.04]
    directory = folder / 'build.zarr'
    write_dataset(ds, directory)
    archive = folder / 'raw.zarr.zip'
    with zipfile.ZipFile(archive, 'w', compression=zipfile.ZIP_STORED) as packed:
        for path in directory.rglob('*'):
            if path.is_file():
                packed.write(path, path.relative_to(directory).as_posix())
    (folder / 'complete.json').write_text(json.dumps({'source': 'goes',
        'product': 'ABI-L2-CMIPF', 'band': 13, 'raw_path': archive.name,
        'stored_bytes': archive.stat().st_size, 'observations': 1,
        'assets': [{'asset_id': 'scan', 'time': '2022-09-15T00:00:20Z',
                    'source_bytes': 10000}]}))
    rows = view_index.inventory(archive, 'goes', '2022-09-15', '2022-09-16')
    assert len(rows) == 1 and rows[0]['band'] == 13
    sizes = view_storage.local_archives(tmp_path, 'goes')
    assert len(sizes) == 1 and sizes[0]['source_bytes'] == 10000
    image = view_frames.frame(rows[0], 'CMI_C13', pixels=64)
    assert image['values'].shape == (64, 64)


class _FakeS3:
    def __init__(self, payload, key):
        self.payload, self.key = payload, key
        self.ranges = []

    def head_object(self, **kwargs):
        assert kwargs['Key'] == self.key
        return {'ContentLength': len(self.payload)}

    def get_object(self, **kwargs):
        if kwargs['Key'].endswith('complete.json'):
            raise AssertionError('The test uses an already listed completion marker')
        assert kwargs['Key'] == self.key
        span = kwargs['Range'].removeprefix('bytes=')
        lo, hi = map(int, span.split('-'))
        self.ranges.append((lo, hi))
        return {'Body': io.BytesIO(self.payload[lo:hi + 1])}


def test_hf_bundle_inspect_and_restore_one_month(tmp_path, monkeypatch):
    monthly = b'example monthly ZIP bytes'
    digest = hashlib.sha256(monthly).hexdigest()
    relative = 'mrms/PrecipRate_00.00/roi-abc/2021/01/raw.zarr.zip'
    member = 'data/' + relative
    marker_member = 'metadata/' + relative.removesuffix('raw.zarr.zip') + 'complete.json'
    manifest = {'source': 'mrms', 'product': 'PrecipRate_00.00', 'year': 2021,
        'archives': [{'local_path': relative, 'member': member,
            'marker_member': marker_member, 'stored_bytes': len(monthly),
            'sha256': digest, 'observations': 2}]}
    payload = io.BytesIO()
    with zipfile.ZipFile(payload, 'w') as z:
        z.writestr(member, monthly, compress_type=zipfile.ZIP_STORED)
        z.writestr(marker_member, json.dumps({'raw_path': 'raw.zarr.zip',
            'archive_sha256': digest, 'stored_bytes': len(monthly)}))
        z.writestr('coverage/2021-01/PrecipRate_00.00-checkpoint.json',
            json.dumps({'source_listed_bytes': 100}))
        z.writestr('year_manifest.json', json.dumps(manifest))
    key = 'noaa-subsets/yearly-v1/mrms/PrecipRate_00.00/2021/bundle.zip'
    client = _FakeS3(payload.getvalue(), key)
    class Writer:
        def __init__(self, root):
            self.client = client
            self.config = {'bucket': 'bucket'}
    monkeypatch.setattr(view_backup, 'BucketWriter', Writer)
    bundle = {'remote_root': 'hf://buckets/ns/bucket/noaa-subsets',
        'bucket': 'bucket', 'key': key, 'stored_bytes': len(payload.getvalue()),
        'source': 'mrms', 'product': 'PrecipRate_00.00', 'year': 2021}
    rows = view_backup.inspect_bundle(bundle)
    assert rows[0]['month'] == '2021-01'
    assert rows[0]['source_bytes'] == 100
    size_rows = view_storage.backup_archives(bundle, rows)
    assert size_rows[0]['stored_bytes'] == len(monthly)
    assert size_rows[0]['source_bytes'] == 100
    restored = view_backup.restore_month(bundle, rows[0], tmp_path)
    assert restored.read_bytes() == monthly
    assert json.loads((restored.parent / 'complete.json').read_text())['archive_sha256'] == digest
    assert view_backup.restore_month(bundle, rows[0], tmp_path) == restored
    assert client.ranges and all(hi < len(payload.getvalue()) for _, hi in client.ranges)
    restored.write_bytes(b'different data')
    with pytest.raises(FileExistsError, match='Choose another cache'):
        view_backup.restore_month(bundle, rows[0], tmp_path)
