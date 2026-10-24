"""Only checksum-verified complete source defects may become coverage gaps."""
import hashlib
from contextlib import contextmanager
from types import SimpleNamespace
import numpy as np
import pytest
import zarr
from ecore_weather import goes, goes_monthly
from ecore_weather.common import Asset


def test_multipart_etag_requires_exact_part_digests(tmp_path):
    import pickle
    content=b'x'*(5*1024**2)+b'final part'
    parts=[hashlib.md5(content[i:i+5*1024**2]).digest() for i in range(0,len(content),5*1024**2)]
    etag=hashlib.md5(b''.join(parts)).hexdigest()+'-2'
    path=tmp_path/'source.nc';path.write_bytes(content)
    asset=Asset('noaa-goes16','ABI-L2-CMIPF/scan.nc',len(content),etag,'2023-10-13')
    proof=goes._verified_object_evidence(path,asset)
    assert proof['verification']=='full-object-size-and-multipart-etag-md5'
    assert proof['part_size']==5*1024**2 and proof['etag']==etag
    path.write_bytes(b'y'+content[1:])
    assert goes._verified_object_evidence(path,asset) is None
    error=pickle.loads(pickle.dumps(goes.ConfirmedCorruptSourceError('HDF checksum',proof)))
    assert error.evidence==proof and str(error)=='HDF checksum'


def test_unconfirmed_or_wrong_roi_cannot_exclude_a_source():
    from ecore_weather import catalog
    from ecore_weather.common import Selection
    asset=Asset('noaa-goes16','ABI-L2-CMIPF/scan.nc',10,'a'*32,'2023-10-13')
    selection=Selection('goes','ABI-L2-CMIPF','2023-10-01','2023-11-01',(-70,14,-62,22),[asset])
    with pytest.raises(ValueError,match='checksum-confirmed'):
        catalog.record_corrupt_source(selection,asset,OSError('range failure'))
    issue=catalog.record_corrupt_source(selection,asset,goes.ConfirmedCorruptSourceError('filter returned failure',
        {'verification':'full-object-size-and-single-part-etag-md5','md5':asset.etag,'download_bytes':asset.size}))
    issue['region']=[0,1,2,3]
    with pytest.raises(ValueError,match='does not match'):catalog.confirmed_source_issues(selection)


@pytest.mark.parametrize('failures',[0,1,2,3])
def test_range_hdf_retry_uses_fresh_handles_and_preserves_native_values(monkeypatch,failures):
    import xarray as xr
    from ecore_weather.common import Transport
    asset=Asset('noaa-goes16','ABI-L2-CMIPF/C02.nc',10,'etag','2023-09-02T08:30:20Z')
    ds=xr.Dataset({'CMI':(('y','x'),np.array([[0,4]],dtype='int16')),
                   'DQF':(('y','x'),np.array([[0,1]],dtype='uint8'))},coords={'x':[1.,2.],'y':[3.]})
    ds.CMI.attrs.update(scale_factor=.25,add_offset=5,_FillValue=-1)
    opened=[];closed=[];calls=[]
    @contextmanager
    def cache(*args,**kwargs):
        token=object();opened.append(token)
        try:yield token
        finally:closed.append(token)
    @contextmanager
    def source(*args,**kwargs):yield ds
    def subset(*args,**kwargs):
        calls.append(1)
        if len(calls)<=failures:raise OSError("Can't synchronously read data (filter returned failure during read)")
        return ds.copy(deep=True)
    monkeypatch.setattr(goes,'remote_file',cache)
    monkeypatch.setattr(goes.xr,'open_dataset',source)
    monkeypatch.setattr(goes,'subset_dataset',subset)
    monkeypatch.setattr(goes.time,'sleep',lambda _:None)
    transport=Transport('obstore')
    if failures==3:
        with pytest.raises(OSError,match='after 3 fresh HDF5 attempts') as error:
            goes.read(asset,transport=transport,bands=(2,))
        assert asset.url in str(error.value) and asset.time in str(error.value)
    else:
        actual,timing=goes.read(asset,transport=transport,bands=(2,))
        np.testing.assert_array_equal(actual.CMI,ds.CMI)
        np.testing.assert_array_equal(actual.DQF,ds.DQF)
        assert actual.CMI.attrs==ds.CMI.attrs
        assert timing['hdf_read_retries']==failures
    assert len(opened)==min(failures+1,3) and opened==closed
    assert transport.retries==min(failures,2)


def test_invalid_selection_is_not_retried_or_reclassified(monkeypatch):
    from ecore_weather.common import Transport
    asset=Asset('noaa-goes16','ABI-L2-CMIPF/scan.nc',10,'etag','2023-09-01')
    @contextmanager
    def cache(*args,**kwargs):yield object()
    def fail(*args,**kwargs):raise ValueError('ROI outside source grid')
    monkeypatch.setattr(goes,'remote_file',cache);monkeypatch.setattr(goes.xr,'open_dataset',fail)
    transport=Transport('obstore')
    with pytest.raises(ValueError,match='ROI outside'):goes.read(asset,transport=transport)
    assert transport.retries==0


def test_short_range_response_retries_and_counts_returned_bytes(monkeypatch):
    import obstore
    from ecore_weather.common import Transport
    asset=Asset('noaa-goes16','ABI-L2-CMIPF/scan.nc',10,'etag','2023-09-01')
    responses=iter([b'abc',b'abcd'])
    monkeypatch.setattr(obstore,'get',lambda *a,**k:SimpleNamespace(bytes=lambda:next(responses)))
    monkeypatch.setattr('ecore_weather.common.time.sleep',lambda _:None)
    transport=Transport('obstore');transport._stores[asset.bucket]=object()
    assert transport.read(asset,0,4)==b'abcd'
    assert (transport.bytes,transport.requests,transport.retries)==(7,2,1)


def test_persistent_short_range_never_reaches_hdf(monkeypatch):
    import obstore
    from ecore_weather.common import Transport
    asset=Asset('noaa-goes16','ABI-L2-CMIPF/scan.nc',10,'etag','2023-09-01')
    monkeypatch.setattr(obstore,'get',lambda *a,**k:SimpleNamespace(bytes=lambda:b'ab'))
    monkeypatch.setattr('ecore_weather.common.time.sleep',lambda _:None)
    transport=Transport('obstore');transport._stores[asset.bucket]=object()
    with pytest.raises(OSError,match='Incomplete NOAA range response'):transport.read(asset,0,4)
    assert (transport.bytes,transport.requests,transport.retries,transport.failed_requests)==(6,3,2,1)


@pytest.mark.parametrize('matching_etag', [True, False])
def test_full_object_integrity_requires_exact_content(monkeypatch, matching_etag):
    content=b'exact NOAA object fixture'
    etag=hashlib.md5(content).hexdigest() if matching_etag else 'different-content'
    asset=Asset('noaa-goes19','ABI-L2-MCMIPF/scan.nc',len(content),etag,'2026-05-21T20:50:21Z')
    class Transport:
        def read(self, asset):return content
    def broken(*args, **kwargs):raise RuntimeError('incorrect metadata checksum after all read attempts')
    monkeypatch.setattr(goes.xr,'open_dataset',broken)
    expected=goes.ConfirmedCorruptSourceError if matching_etag else RuntimeError
    with pytest.raises(expected) as failure:
        goes.read(asset,transport=Transport(),full_file=True)
    if matching_etag:
        assert failure.value.evidence['md5']==etag
        assert failure.value.evidence['download_bytes']==len(content)


def test_confirmed_corruption_is_explicit_coverage_without_pixels(tmp_path, monkeypatch):
    asset=Asset('noaa-goes19','ABI-L2-MCMIPF/scan.nc',100,'etag','2026-05-21T20:50:21Z')
    calls=[]
    def broken(*args,**kwargs):
        calls.append(kwargs.get('full_file',False))
        if calls[-1]:
            raise goes.ConfirmedCorruptSourceError('incorrect metadata checksum',{'md5':'etag'})
        raise RuntimeError('incorrect metadata checksum')
    monkeypatch.setattr(goes,'read',broken)
    row=goes_monthly._read_scan(asset,(-70,14,-62,22),(13,),1024**2)
    assert calls==[False,True]
    assert row['status']=='corrupt_source'
    root=zarr.open_group(str(tmp_path/'raw.zarr'),mode='w')
    rows=goes_monthly._write_batch(root,{'results':[row]},(13,))
    assert rows[0]['integrity_evidence']=={'md5':'etag'}
    assert rows[0]['available_bands']==[] and rows[0]['checksums']=={}
    assert list(root.groups())==[]
    goes_monthly._verify_stage(root,[{'rows':rows}])
