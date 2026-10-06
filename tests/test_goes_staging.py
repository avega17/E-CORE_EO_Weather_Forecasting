import asyncio
import hashlib
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import pytest
import xarray as xr
from ecore_weather import goes_staging
from ecore_weather.common import Asset, Selection


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
    from ecore_weather.monthly_stream import _write_one
    from ecore_weather.storage import open_raw
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
    from ecore_weather.monthly_stream import _write_one
    from ecore_weather.storage import open_raw
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
