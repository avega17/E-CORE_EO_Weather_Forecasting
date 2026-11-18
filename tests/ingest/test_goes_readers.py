from __future__ import annotations

"""Native-band restart and raw preservation contracts."""
import json
import numpy as np
import pytest
import xarray as xr
from ecore_weather.core.common import Asset, Selection
from ecore_weather.ingest import monthly_stream
from ecore_weather.storage.archives import open_raw


def test_native_checkpoint_resume_and_existing_store(tmp_path, monkeypatch):
    assets = [Asset('noaa-goes16', f'ABI-L2-CMIPF/test-M6C02_s{i}.nc',100,'etag',
                    f'2022-09-18T{12+i//6:02d}:{i%6*10:02d}:00Z') for i in range(10)]
    selection = Selection('goes','ABI-L2-CMIPF','2022-09-18','2022-09-19',(-70,14,-62,22),assets,
                          bands=(2,),satellite=16)
    called = []
    fail = [True]
    def reads(rows, sel, band, *args):
        for row in rows:
            i = assets.index(row['asset'])
            called.append(i)
            if i == 8 and fail[0]:
                raise OSError('interrupted')
            ds = xr.Dataset({'CMI':(('y','x'),np.full((3,4),i,dtype='int16')),
                             'DQF':(('y','x'),np.full((3,4),i%2,dtype='int8'))},
                            coords={'x':np.array([4,5,6,7],dtype='int16'),'y':[2,1,0]},
                            attrs={'product':'ABI-L2-CMIPF','observation_time':row['time']})
            ds.CMI.attrs.update(scale_factor=.01+i*.001,add_offset=1.,_FillValue=-1)
            ds.x.attrs.update(scale_factor=.000014,units='rad')
            yield row,(ds,{})
    monkeypatch.setattr('ecore_weather.ingest.goes_native.ordered_reads',reads)
    target=tmp_path/'month';scratch=tmp_path/'scratch'
    with pytest.raises(OSError, match='interrupted'):
        monthly_stream._write_one(selection,assets,target,2,scratch=scratch)
    assert list(scratch.glob('goes-cmipf-*/batch-*.json'))
    from ecore_weather.ingest import goes_monthly
    checkpoint=goes_monthly._checkpoint
    progress=[]
    def record_checkpoint(path,data):
        if path.name=='progress.json':progress.append(data['next_index'])
        return checkpoint(path,data)
    monkeypatch.setattr(goes_monthly,'_checkpoint',record_checkpoint)
    called.clear();fail[0]=False
    result=monthly_stream._write_one(selection,assets,target,2,scratch=scratch)
    assert called == [8,9]
    assert progress==[10]  # Revalidating saved frames never rewinds progress.
    with open_raw(result['path']) as ds:
        np.testing.assert_array_equal(ds.CMI_C02.values[:,0,0],np.arange(10))
        assert ds.x.attrs['scale_factor'] == .000014
        assert json.loads(ds.source_metadata_json.values[-1])['variables']['CMI']['attrs']['scale_factor'] == .01+9*.001
    assert not list(scratch.glob('goes-cmipf-*'))
    assert monthly_stream._write_one(selection,assets,target,2,scratch=scratch)['status']=='reused'


def test_native_defaults_and_order():
    from ecore_weather.sources.earth2_sources import GOESCaribbeanSource
    from ecore_weather.ui.fetch_controls import selection_controls
    assert GOESCaribbeanSource().product == 'ABI-L2-CMIPF'
    assert selection_controls('goes')['product'].value == 'ABI-L2-CMIPF'
    from importlib.util import spec_from_file_location,module_from_spec
    from ecore_weather.jobs import jobs_goes as m
    assert m.STAGES[0][0]=='h1-2021' and m.STAGES[-1][0]=='h1-2026'
    from pathlib import Path
    env=m.snapshot_environment()
    assert env['ECORE_REPO_ROOT']==str(Path('.').resolve())
    assert env['PYTHONPATH'].split(':')[0]==str(Path('src').resolve())


def test_reader_context_excludes_inventory():
    from ecore_weather.ingest.goes_native import _read_context
    from types import SimpleNamespace
    context=_read_context(SimpleNamespace(source='goes',bbox=(-70,14,-62,22),
        product='ABI-L2-CMIPF',bands=(2,),assets=['large inventory']*40000),2,1024**2)
    import pickle
    assert not hasattr(context,'assets') and len(pickle.dumps(context))<512
    assert context.band==2 and context.block_size==1024**2


@pytest.mark.parametrize('gap_index',[8,3])
def test_confirmed_source_gap_reuses_prefix_without_fabricated_frame(tmp_path,monkeypatch,gap_index):
    from ecore_weather.storage import catalog as catalog
    from ecore_weather.sources import goes
    assets=[Asset('noaa-goes16',f'ABI-L2-CMIPF/OR-M6C02_scan{i}.nc',100,'a'*32,
                  f'2023-10-01T00:{i*5:02d}:00Z') for i in range(10)]
    selection=Selection('goes','ABI-L2-CMIPF','2023-10-01','2023-11-01',(-70,14,-62,22),assets,bands=(2,),satellite=16)
    calls=[];fail=[True]
    def reader(rows,*args):
        for row in rows:
            i=assets.index(row['asset']);calls.append(i)
            if i==8 and fail[0]:raise OSError('source failed')
            ds=xr.Dataset({'CMI':(('y','x'),np.full((2,3),i,dtype='int16')),
                           'DQF':(('y','x'),np.full((2,3),i%2,dtype='int8'))},coords={'y':[2,1],'x':[1,2,3]})
            ds.CMI.attrs.update(scale_factor=.01+i*.001,add_offset=1.,_FillValue=-1)
            yield row,(ds,{})
    monkeypatch.setattr('ecore_weather.ingest.goes_native.ordered_reads',reader)
    target=tmp_path/'month';scratch=tmp_path/'scratch'
    with pytest.raises(OSError,match='source failed'):
        monthly_stream._write_one(selection,assets,target,2,scratch=scratch)
    parent=next(scratch.glob('goes-cmipf-*'))
    issue=catalog.record_corrupt_source(selection,assets[gap_index],goes.ConfirmedCorruptSourceError(
        'filter returned failure',{'verification':'full-object-size-and-single-part-etag-md5',
                                  'download_bytes':100,'md5':'a'*32}))
    path=catalog.save_selection(selection,tmp_path/'selection',index_results=False)
    selection=catalog.load_selection(path)
    assert len(selection.assets)==10 and catalog.confirmed_source_issues(selection)=={assets[gap_index].id:issue}
    calls.clear();fail[0]=False
    if gap_index==3:
        with pytest.raises(OSError,match='verified scratch prefix'):
            monthly_stream._write_one(selection,assets,target,2,scratch=scratch)
        assert not calls and json.loads((parent/'progress.json').read_text())['next_index']==8
        return
    result=monthly_stream._write_one(selection,assets,target,2,scratch=scratch)
    assert result['resumed_observations']==8 and calls==[9]
    with open_raw(result['path']) as ds:
        assert len(ds.time)==9
        np.testing.assert_array_equal(ds.CMI_C02[:,0,0],list(range(8))+[9])
        assert str(ds.time.values[-1]).startswith('2023-10-01T00:45:00')
        assert ds.attrs['source_issues']==[issue]
        assert json.loads(ds.source_metadata_json.values[-1])['variables']['CMI']['attrs']['scale_factor']==pytest.approx(.019)
    marker=json.loads((target/'complete.json').read_text())
    assert marker['source_issues']==[issue] and assets[8].id not in marker['asset_ids']
    assert marker['observations']==9 and not parent.exists()
    assert monthly_stream._write_one(selection,assets,target,2,scratch=scratch)['status']=='reused'


"""Only checksum-verified complete source defects may become coverage gaps."""
import hashlib
from contextlib import contextmanager
from types import SimpleNamespace
import numpy as np
import pytest
import zarr
from ecore_weather.sources import goes
from ecore_weather.ingest import goes_monthly
from ecore_weather.core.common import Asset


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
    from ecore_weather.storage import catalog as catalog
    from ecore_weather.core.common import Selection
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
    from ecore_weather.core.common import Transport
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
    from ecore_weather.core.common import Transport
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
    from ecore_weather.core.common import Transport
    asset=Asset('noaa-goes16','ABI-L2-CMIPF/scan.nc',10,'etag','2023-09-01')
    responses=iter([b'abc',b'abcd'])
    monkeypatch.setattr(obstore,'get',lambda *a,**k:SimpleNamespace(bytes=lambda:next(responses)))
    monkeypatch.setattr('ecore_weather.core.common.time.sleep',lambda _:None)
    transport=Transport('obstore');transport._stores[asset.bucket]=object()
    assert transport.read(asset,0,4)==b'abcd'
    assert (transport.bytes,transport.requests,transport.retries)==(7,2,1)


def test_persistent_short_range_never_reaches_hdf(monkeypatch):
    import obstore
    from ecore_weather.core.common import Transport
    asset=Asset('noaa-goes16','ABI-L2-CMIPF/scan.nc',10,'etag','2023-09-01')
    monkeypatch.setattr(obstore,'get',lambda *a,**k:SimpleNamespace(bytes=lambda:b'ab'))
    monkeypatch.setattr('ecore_weather.core.common.time.sleep',lambda _:None)
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


import asyncio
import hashlib
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import pytest
import xarray as xr
from ecore_weather.ingest import goes_staging
from ecore_weather.core.common import Asset, Selection


def test_production_streaming_async_download_bound(tmp_path,monkeypatch):
    import obstore
    import obstore.store
    active=peak=0
    class Response:
        def stream(self,min_chunk_size):
            async def blocks():
                nonlocal active
                await asyncio.sleep(.01)
                yield b'ab';yield b'cd'
                active-=1
            return blocks()
    async def get(*args,**kwargs):
        nonlocal active,peak
        active+=1;peak=max(peak,active)
        return Response()
    monkeypatch.setattr(obstore,'get_async',get)
    monkeypatch.setattr(obstore.store,'S3Store',lambda *a,**k:object())
    assets=[Asset('noaa-goes16',f'{i}.nc',4,'etag',f'2022-09-18T12:{i}0:00Z') for i in range(4)]
    items=asyncio.run(goes_staging.download_batch(assets,tmp_path,2))
    assert peak==2
    assert sum(row['read_bytes'] for row in items)==16
    assert all(open(row['path'],'rb').read()==b'abcd' for row in items)


def test_staged_native_reader_preserves_values_and_cleans_sources(tmp_path,monkeypatch):
    class Pool(ThreadPoolExecutor):
        def __init__(self,max_workers,mp_context=None):super().__init__(max_workers)
    monkeypatch.setattr(goes_staging,'ProcessPoolExecutor',Pool)
    ds=xr.Dataset({'CMI':(('y','x'),np.array([[4,5],[6,7]],dtype='int16')),
                   'DQF':(('y','x'),np.zeros((2,2),dtype='int8'))},coords={'x':[1,2],'y':[3,4]})
    ds.CMI.attrs.update(scale_factor=.1,add_offset=2,_FillValue=-1)
    fixture=tmp_path/'fixture.nc';ds.to_netcdf(fixture,engine='h5netcdf')
    content=fixture.read_bytes()
    assets=[Asset('noaa-goes16',f'ABI-L2-CMIPF/scan-{i}.nc',len(content),hashlib.md5(content).hexdigest(),
        f'2022-09-18T12:{i}0:00Z') for i in range(3)]
    selection=Selection('goes','ABI-L2-CMIPF','2022-09-18T12:00:00Z','2022-09-18T13:00:00Z',(-70,14,-62,22),assets,bands=(13,))
    async def download(assets,directory,concurrency):
        rows=[]
        for asset in assets:
            path=directory/(asset.id+'.nc');path.write_bytes(content)
            rows.append({'path':str(path),'read_bytes':asset.size,'range_requests':1,
                'source_retries':0,'failed_requests':0,'transfer_task_seconds':.01})
        return rows
    monkeypatch.setattr(goes_staging,'download_batch',download)
    monkeypatch.setattr(goes_staging.goes,'subset_dataset',lambda source,*a:source.load().copy(deep=True))
    metrics={};folder=tmp_path/'scratch'
    output=list(goes_staging.staged_reads([{'asset':a} for a in assets],selection,13,2,8,512,4096,folder,metrics))
    assert len(output)==3
    for row,(actual,timing) in output:
        np.testing.assert_array_equal(actual.CMI,ds.CMI)
        assert actual.CMI.attrs==ds.CMI.attrs
        assert actual.attrs['source_etag']==row['asset'].etag
        assert actual.attrs['observation_time']==row['asset'].time
    assert metrics['read_bytes']==3*len(content)
    assert not list(folder.iterdir())
    from ecore_weather.ingest.monthly_stream import _write_one
    from ecore_weather.storage.archives import open_raw
    archived=_write_one(selection,assets,tmp_path/'month',13,scratch=tmp_path/'monthly-scratch',
        read_mode='async_full',download_concurrency=8,staging_mib=4096)
    assert archived['status']=='saved' and archived['read_bytes']==3*len(content)
    with open_raw(archived['path']) as reopened:
        np.testing.assert_array_equal(reopened.CMI_C13.values,np.stack([ds.CMI.values]*3))
        np.testing.assert_array_equal(reopened.DQF_C13.values,np.stack([ds.DQF.values]*3))
    assert _write_one(selection,assets,tmp_path/'month',13,
        read_mode='async_full')['status']=='reused'
    with pytest.raises(MemoryError,match='exceeds'):
        oversized=Asset('noaa-goes16','large.nc',2*1024**2,'etag',assets[0].time)
        list(goes_staging.staged_reads([{'asset':oversized}],selection,13,2,8,512,1,folder,{}))


def test_pipeline_order_bounds_cache_resume_and_checkpoint_retirement(tmp_path, monkeypatch):
    import obstore
    import obstore.store
    class Pool(ThreadPoolExecutor):
        def __init__(self,max_workers,mp_context=None):super().__init__(max_workers)
    monkeypatch.setattr(goes_staging,'ProcessPoolExecutor',Pool)
    monkeypatch.setattr(obstore.store,'S3Store',lambda *a,**k:object())
    ds=xr.Dataset({'CMI':(('y','x'),np.arange(4,dtype='int16').reshape(2,2)),
                   'DQF':(('y','x'),np.zeros((2,2),dtype='int8'))},coords={'x':[1,2],'y':[3,4]})
    ds.CMI.attrs.update(scale_factor=.1,add_offset=2,_FillValue=-1)
    fixture=tmp_path/'fixture.nc';ds.to_netcdf(fixture,engine='h5netcdf');content=fixture.read_bytes()
    assets=[Asset('noaa-goes16',f'scan-{i}.nc',len(content),hashlib.md5(content).hexdigest(),
                 f'2022-09-18T12:{i:02}:00Z') for i in range(16)]
    selection=Selection('goes','ABI-L2-CMIPF',assets[0].time,'2022-09-18T13:00:00Z',(-70,14,-62,22),assets,bands=(13,))
    requests=[]
    class Response:
        def stream(self,min_chunk_size):
            async def blocks():
                await asyncio.sleep(.005)
                yield content
            return blocks()
    async def get(store,key,**kwargs):requests.append(key);return Response()
    monkeypatch.setattr(obstore,'get_async',get)
    monkeypatch.setattr(goes_staging.goes,'subset_dataset',lambda source,*a:source.load().copy(deep=True))
    folder=tmp_path/'sources';metrics={}
    iterator=goes_staging.pipeline_reads([{'asset':a} for a in assets],selection,13,2,4,1,1,folder,metrics)
    first=next(iterator);np.testing.assert_array_equal(first[1][0].CMI,ds.CMI)
    iterator.close()
    assert list(folder.glob('*.receipt.json'))  # Failed/uncommitted sources remain reusable.
    output=[]
    for row,result in goes_staging.pipeline_reads([{'asset':a} for a in assets],selection,13,2,4,1,1,folder,metrics):
        output.append((row,result))
        if len(output)%8==0:goes_staging.retire_committed(folder,[r['asset'].id for r,_ in output[-8:]])
    assert [r['asset'].id for r,_ in output]==[a.id for a in assets]
    assert metrics['cached_full_source_files']>=1
    assert metrics['peak_active_downloads']<=4
    assert metrics['scratch_peak_bytes']<=1024**2
    assert metrics['roi_queue_peak_bytes']<=1024**2
    assert not list(folder.glob('*.nc'))
    from ecore_weather.ingest.monthly_stream import _write_one
    from ecore_weather.storage.archives import open_raw
    archived=_write_one(selection,assets,tmp_path/'archive',13,scratch=tmp_path/'scratch',read_mode='async_pipeline',
                        read_processes=2,download_concurrency=4,prefetch_mib=1,staging_mib=1)
    with open_raw(archived['path']) as reopened:
        np.testing.assert_array_equal(reopened.CMI_C13,np.stack([ds.CMI.values]*16))
        np.testing.assert_array_equal(reopened.DQF_C13,np.stack([ds.DQF.values]*16))
    assert _write_one(selection,assets,tmp_path/'archive',13,read_mode='async_pipeline')['status']=='reused'


def test_pipeline_failure_propagates_without_deadlock(tmp_path,monkeypatch):
    import obstore
    import obstore.store
    monkeypatch.setattr(obstore.store,'S3Store',lambda *a,**k:object())
    async def fail(*a,**k):raise OSError('source failed')
    monkeypatch.setattr(obstore,'get_async',fail)
    asset=Asset('noaa-goes16','failed.nc',100,'etag','2022-09-18T12:00:00Z')
    selection=Selection('goes','ABI-L2-CMIPF',asset.time,'2022-09-18T13:00:00Z',(-70,14,-62,22),[asset],bands=(13,))
    with pytest.raises(OSError,match='source failed'):
        list(goes_staging.pipeline_reads([{'asset':asset}],selection,13,1,1,1,1,tmp_path/'sources',{}))
