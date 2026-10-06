"""Native-band restart and raw preservation contracts."""
import json
import numpy as np
import pytest
import xarray as xr
from ecore_weather.common import Asset, Selection
from ecore_weather import monthly_stream
from ecore_weather.storage import open_raw


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
    monkeypatch.setattr('ecore_weather.goes_native.ordered_reads',reads)
    target=tmp_path/'month';scratch=tmp_path/'scratch'
    with pytest.raises(OSError, match='interrupted'):
        monthly_stream._write_one(selection,assets,target,2,scratch=scratch)
    assert list(scratch.glob('goes-cmipf-*/batch-*.json'))
    from ecore_weather import goes_monthly
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
    from ecore_weather.earth2_sources import GOESCaribbeanSource
    from ecore_weather.ui import selection_controls
    assert GOESCaribbeanSource().product == 'ABI-L2-CMIPF'
    assert selection_controls('goes')['product'].value == 'ABI-L2-CMIPF'
    from importlib.util import spec_from_file_location,module_from_spec
    from ecore_weather import jobs_goes as m
    assert m.STAGES[0][0]=='h1-2021' and m.STAGES[-1][0]=='h1-2026'
    from pathlib import Path
    env=m.snapshot_environment()
    assert env['ECORE_REPO_ROOT']==str(Path('.').resolve())
    assert env['PYTHONPATH'].split(':')[0]==str(Path('src').resolve())


def test_reader_context_excludes_inventory():
    from ecore_weather.goes_native import _read_context
    from types import SimpleNamespace
    context=_read_context(SimpleNamespace(source='goes',bbox=(-70,14,-62,22),
        product='ABI-L2-CMIPF',bands=(2,),assets=['large inventory']*40000),2,1024**2)
    import pickle
    assert not hasattr(context,'assets') and len(pickle.dumps(context))<512
    assert context.band==2 and context.block_size==1024**2
