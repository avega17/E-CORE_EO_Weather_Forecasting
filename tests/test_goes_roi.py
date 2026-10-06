import h5py
import numpy as np
import xarray as xr

from ecore_weather.study_goes import write_uncompressed_netcdf
from ecore_weather.study_goes import SCENARIOS, UNCOMPRESSED_NETCDF_METHOD, summarize
from ecore_weather.report_benchmarks import roi_options


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
