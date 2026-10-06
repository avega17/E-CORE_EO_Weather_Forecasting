"""Only checksum-verified complete source defects may become coverage gaps."""
import hashlib
import numpy as np
import pytest
import zarr
from ecore_weather import goes, goes_monthly
from ecore_weather.common import Asset


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
