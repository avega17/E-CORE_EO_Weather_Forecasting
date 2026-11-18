"""Scientific processing, capacity, snapshots and random-access contracts."""
from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
import xarray as xr



from ecore_weather.processing.transforms import (Recipe, atomic_json, target_grid, decode_observation,
    scan_dataset, geometry, regrid, align_observations, window_index)


def test_grid_full_bbox():
    for resolution,shape in [(1,(864,840)),(2,(432,420)),(3,(288,280))]:
        grid=target_grid(Recipe(resolution_km=resolution))
        assert grid.inside_bbox.shape==shape
        np.testing.assert_allclose(np.diff(grid.x),resolution*1000)
        assert not grid.inside_bbox.values.all()


def test_scan_unsigned_quality_and_invalid_calibration():
    ds=xr.Dataset({'CMI':(('y','x'),np.array([[-1,-2,1,2,3]],dtype='int16')),
                   'DQF':(('y','x'),np.array([[0,1,0,2,0]],dtype='uint8'))})
    ds.CMI.attrs.update(_Unsigned='true',_FillValue=65535,valid_range=[0,-2],scale_factor=.5,add_offset=2.)
    result=decode_observation(ds)
    assert result['classes'].tolist()==[[1,9,0,5,0]]
    assert np.isnan(result['values'][0,0]) and np.isnan(result['values'][0,1])
    assert result['values'][0,2]==2.5
    assert decode_observation(ds,quality='conditional')['values'][0,1]==65534*.5+2
    ds.CMI.attrs['scale_factor']=np.inf
    assert not decode_observation(ds)['valid'].any()


def test_mrms_sentinels_do_not_become_clear_air():
    ds=xr.Dataset({'measurement':(('latitude','longitude'),[[-20.,0.,-99.,-999.,7.]]),
        'bitmap_valid':(('latitude','longitude'),[[1,1,1,1,0]])},attrs={'product':'MergedReflectivityQCComposite_00.50'})
    result=decode_observation(ds)
    assert result['valid'].tolist()==[[True,True,False,False,False]]
    assert result['values'][0,0]==-20


def test_scan_metadata_is_not_first_scans_calibration():
    ds=xr.Dataset({'CMI':(('time','y','x'),[[[1]],[[1]]]),'planck_fk1':xr.DataArray(1.)},coords={'time':[0,1],
        'source_metadata_json':('time',[json.dumps({'variables':{'CMI':{'attrs':{'scale_factor':s}},
            'planck_fk1':{'dims':[],'values':s,'dtype':'<f4','attrs':{'units':'1'}}}}) for s in (2,7)])})
    assert decode_observation(scan_dataset(ds,0))['classes'][0,0]==8
    assert scan_dataset(ds,1).CMI.attrs['scale_factor']==7
    assert scan_dataset(ds,1).planck_fk1.item()==7
    assert scan_dataset(ds,1).planck_fk1.dtype==np.dtype('float32')
    assert ds.planck_fk1.item()==1
    assert ds.CMI.attrs=={}


def test_bilinear_requires_valid_contributors_and_categorical_quality():
    source=xr.Dataset({'measurement':(('latitude','longitude'),[[0,10],[20,30]])},
        coords={'latitude':[0.,1.],'longitude':[0.,1.]})
    grid=xr.Dataset({'latitude':(('y','x'),[[.5]]),'longitude':(('y','x'),[[.5]]),
                     'inside_bbox':(('y','x'),[[True]])},attrs={'test':'center'})
    mapped=geometry(source,grid,'bilinear')
    decoded={'values':np.array([[0,10],[20,30]],'float32'),'valid':np.ones((2,2),bool),
             'quality':np.array([[0,1],[2,3]],'uint8'),'classes':np.zeros((2,2),'uint8')}
    assert regrid(decoded,mapped)['values'][0,0]==15
    assert regrid(decoded,mapped)['quality'][0,0] in [0,1,2,3]
    decoded['valid'][0,0]=False
    assert np.isnan(regrid(decoded,mapped)['values'][0,0])


def test_alignment_causal_no_reuse_and_cross_month():
    records=[]
    for i,t in enumerate(pd.date_range('2026-01-31T23:00Z',periods=12,freq='10min')):
        for v in ['abi13c','refc']:
            start=t-pd.Timedelta(minutes=8) if v.startswith('abi') else t-pd.Timedelta(minutes=1)
            records.append({'variable':v,'asset_id':f'{v}{i}','time':start.isoformat(),
                'end_time':(t-pd.Timedelta(seconds=20)).isoformat() if v.startswith('abi') else start.isoformat(),
                'path':'a','row_index':i,'source':'goes' if v.startswith('abi') else 'mrms'})
    frame=align_observations(pd.DataFrame(records),'2026-01-31T23:50Z','2026-02-01T01:00Z',['abi13c','refc'])
    assert frame.asset_id.is_unique
    assert all(pd.Timestamp(r.end_time)<=pd.Timestamp(r.slot_time) for r in frame.itertuples())
    windows=window_index(frame,'2026-01-31T23:50Z','2026-02-01T01:00Z',['abi13c','refc'])
    assert windows.eligible.all()
    missing=frame[frame.asset_id!='refc6']
    assert not window_index(missing,'2026-02-01','2026-02-01T00:10Z',['abi13c','refc']).eligible.any()


def test_capacity_five_percent_and_mode():
    from ecore_weather.processing.process_store import capacity,choose_mode
    disk=capacity('/tmp',usage=SimpleNamespace(total=1000,free=500),reserve_percent=5)
    assert disk['usable_bytes']==450
    assert choose_mode('auto',449,disk)=='full'
    assert choose_mode('auto',451,disk)=='blocks'
    with pytest.raises(OSError,match='Use blocks'):choose_mode('full',451,disk)


def test_radar_only_and_empty_variable_selection():
    assert Recipe(bands=(),radar=('refc','refc_base')).variables==('refc','refc_base')
    with pytest.raises(ValueError,match='and/or'):Recipe(bands=(),radar=())


@pytest.fixture
def native_selection(tmp_path):
    from ecore_weather.storage.archives import pack_raw
    from ecore_weather.storage.earth2_io import write_dataset
    from ecore_weather.processing.process_catalog import build_index
    recipe=Recipe(bbox=(-66.45,18.15,-66.35,18.25),bands=(13,),radar=())
    grid=target_grid(recipe)
    # Radar-shaped test data also need real GOES projection for native decoding.
    from pyproj import CRS,Transformer
    projection={'grid_mapping_name':'geostationary','perspective_point_height':35786023.,
        'longitude_of_projection_origin':-75.,'sweep_angle_axis':'x','semi_major_axis':6378137.,
        'semi_minor_axis':6356752.31414,'latitude_of_projection_origin':0.}
    crs=CRS.from_cf(projection)
    x,y=Transformer.from_crs('EPSG:4326',crs,always_xy=True).transform(grid.longitude,grid.latitude)
    xs=np.linspace(x.min()-1000,x.max()+1000,10)/projection['perspective_point_height']
    ys=np.linspace(y.max()+1000,y.min()-1000,10)/projection['perspective_point_height']
    times=pd.date_range('2022-09-18T00:00Z',periods=12,freq='10min')
    attrs={'product':'ABI-L2-CMIPF'}
    ds=xr.Dataset({'CMI_C13':(('time','y','x'),np.arange(1200,dtype='int16').reshape(12,10,10)),
        'DQF_C13':(('time','y','x'),np.zeros((12,10,10),dtype='uint8')),
        'goes_imager_projection':xr.DataArray(0,attrs=projection)},
        coords={'time':times.tz_localize(None).values,'x':xs,'y':ys},attrs=attrs)
    ds.CMI_C13.attrs.update(scale_factor=.1,add_offset=200.,units='K',_FillValue=-1)
    root=tmp_path/'native';path=root/'goes/ABI-L2-CMIPF/goes16/C13/roi-test/2022/09'
    path.mkdir(parents=True)
    write_dataset(ds,path/'raw.zarr');pack_raw(path)
    assets=[{'asset_id':str(i),'time':t.isoformat(), 'end_time':(t+pd.Timedelta(minutes=9)).isoformat(),
        'source_url':'https://noaa-goes16.s3.amazonaws.com/test.nc','source_bytes':100} for i,t in enumerate(times)]
    atomic_json(path/'complete.json',{'source':'goes','product':'ABI-L2-CMIPF','band':13,'satellite':16,
        'raw_path':'raw.zarr.zip','stored_bytes':(path/'raw.zarr.zip').stat().st_size,'region':recipe.bbox,
        'assets':assets,'observations':12,'archive_sha256':'fixture-archive-identity'})
    info=build_index(root,tmp_path/'preparation','2022-09-18T00:10Z','2022-09-18T02:00Z',recipe,
                     tmp_path/'process.duckdb')
    return info,recipe


def test_materialize_random_source_reuse_and_verification(native_selection,tmp_path):
    from ecore_weather.processing.process_store import materialize,ProcessedSource,validate_store
    info,recipe=native_selection
    result=materialize(info['preparation_path'],tmp_path/'processed',workers=1)
    assert result['status']=='complete'
    unit=Path(result['units'][0]['path'])
    assert validate_store(unit)['status']=='verified'
    sample=ProcessedSource(unit)(['2022-09-18T01:00Z','2022-09-18T00:10Z'],['abi13c'])
    assert sample.dims==('time','variable','y','x') and np.isfinite(sample.values).any()
    assert sample.time.values[0]>sample.time.values[1]
    assert np.isfinite(sample.latitude.values).all()
    with ProcessedSource(unit) as source:
        first=source(['2022-09-18T01:00Z'],['abi13c']);handle=source._dataset
        second=source(['2022-09-18T01:00Z'],['abi13c'])
        assert source._dataset is handle
        np.testing.assert_array_equal(first.values,second.values)
    assert source._dataset is None
    assert materialize(info['preparation_path'],tmp_path/'processed',workers=1)['units'][0]['reused']


def test_uncompressed_capacity_estimate_accounts_for_padded_chunks(native_selection,tmp_path):
    from ecore_weather.processing.process_catalog import build_index
    from ecore_weather.processing.process_store import materialize
    info,recipe=native_selection
    changed=build_index(info['archive_root'],tmp_path/'prep-none',info['start'],info['end_excluded'],
        replace(recipe,compression='none',chunk_time=6),tmp_path/'none.duckdb')
    result=materialize(changed['preparation_path'],tmp_path/'processed-none',workers=1)
    assert result['estimate']['estimated_stored_bytes']>=result['units'][0]['stored_bytes']
    assert result['estimate']['time_padding_factor']>=1


def test_virtual_values_coordinates_and_metadata(native_selection):
    pytest.importorskip('icechunk')
    from ecore_weather.processing.process_catalog import build_catalog,open_virtual_archive
    from ecore_weather.storage.archives import open_raw
    info,_=native_selection;root=Path(info['preparation_path'])
    result=build_catalog(root)
    row=pd.read_parquet(root/'archives.parquet').iloc[0]
    with open_raw(row.path) as native,open_virtual_archive(root,row.archive_id) as virtual:
        np.testing.assert_array_equal(native.CMI_C13.values,virtual.CMI_C13.values)
        np.testing.assert_array_equal(native.time.values,virtual.time.values)
        assert virtual.CMI_C13.attrs==native.CMI_C13.attrs
    assert result['metadata_bytes']>0


def test_resume_does_not_recompute_verified_prefix(native_selection,tmp_path,monkeypatch):
    from ecore_weather.processing import process_store as store
    info,_=native_selection;counter=[0]
    def progress(*unused):counter[0]+=1
    first=store.materialize(info['preparation_path'],tmp_path/'processed',workers=1,
        stop=lambda:counter[0]>=1,progress=progress)
    assert first['status']=='paused' and first['units'][0]['verified_slots']==8
    calls=[];original=store.transform_records
    def track(records,*args):
        calls.extend(r['row_index'] for r in records if r)
        return original(records,*args)
    monkeypatch.setattr(store,'transform_records',track)
    result=store.materialize(info['preparation_path'],tmp_path/'processed',workers=1)
    # The estimator deliberately rereads three sampled frames; the actual resumed batch does not replay its prefix.
    assert result['status']=='complete'
    assert not any(i in calls[3:] for i in (0,1,2))


def test_corrupt_derived_checkpoint_is_rejected(native_selection,tmp_path):
    from ecore_weather.processing.process_store import materialize,validate_store
    info,_=native_selection
    result=materialize(info['preparation_path'],tmp_path/'processed',workers=1)
    path=Path(result['units'][0]['path']);saved=json.loads((path/'progress.json').read_text())
    saved['batches'][0]['sha256']='wrong'
    atomic_json(path/'progress.json',saved)
    with pytest.raises(IOError,match='verified values'):validate_store(path)


def test_partial_reader_keeps_its_validated_snapshot(native_selection,tmp_path):
    from ecore_weather.processing.process_store import materialize,ProcessedSource
    info,_=native_selection;done=[False]
    first=materialize(info['preparation_path'],tmp_path/'processed',workers=1,
        stop=lambda:done[0],progress=lambda *unused:done.__setitem__(0,True))
    path=first['units'][0]['path'];source=ProcessedSource(path,allow_partial=True)
    original=source.generation
    materialize(info['preparation_path'],tmp_path/'processed',workers=1)
    assert source.generation==original
    with pytest.raises(KeyError):source(['2022-09-18T01:50Z'],['abi13c'])
    assert ProcessedSource(path).generation!=original


def test_writer_ownership(tmp_path):
    from ecore_weather.processing.process_store import owner
    with owner(tmp_path):
        with pytest.raises(RuntimeError,match='already has a writer'):
            with owner(tmp_path):pass


def test_blocks_are_explicit_and_can_be_bounded(native_selection,tmp_path):
    from ecore_weather.processing.process_store import materialize
    info,_=native_selection
    result=materialize(info['preparation_path'],tmp_path/'processed',mode='blocks',block_hours=1,
        max_blocks=1,workers=1)
    assert result['mode']=='blocks' and result['status']=='paused'
    assert result['units'][0]['status']=='complete'
    assert not (Path(result['destination'])/'complete.json').exists()
    resumed=materialize(info['preparation_path'],tmp_path/'processed',mode='blocks',block_hours=1,
        max_blocks=1,workers=1)
    assert len(resumed['units'])==2 and resumed['units'][0]['reused']


def test_block_consumer_prefetch_and_explicit_retirement(native_selection,tmp_path):
    from ecore_weather.processing.process_store import iter_materialized_blocks
    info,_=native_selection
    blocks=iter_materialized_blocks(info['preparation_path'],tmp_path/'processed',block_hours=1,workers=1,release=True)
    first=next(blocks);path=first.path
    assert (path/'complete.json').exists()
    assert len(pd.read_parquet(path/'windows.parquet'))>0
    second=next(blocks)
    assert second.path!=path and not path.exists()
    blocks.close()
    assert Path(info['preparation_path']).is_dir()


def test_virtual_reuse_and_missing_source(native_selection):
    pytest.importorskip('icechunk')
    from ecore_weather.processing.process_catalog import build_catalog,open_virtual_archive
    info,_=native_selection;root=Path(info['preparation_path'])
    first=build_catalog(root);second=build_catalog(root)
    assert second['reused'] and first['snapshot_id']==second['snapshot_id']
    row=pd.read_parquet(root/'archives.parquet').iloc[0]
    Path(row.path).rename(Path(row.path).with_suffix('.moved'))
    with pytest.raises(IOError,match='missing or changed'):
        with open_virtual_archive(root,row.archive_id):pass


def test_parallax_cache_invalidation_and_bounded_eviction(native_selection,tmp_path,monkeypatch):
    from ecore_weather.processing import process_parallax as parallax
    from ecore_weather.processing.process_catalog import load_index
    from ecore_weather.processing.transforms import NativeReader
    info,recipe=native_selection;_,aligned,_=load_index(info['preparation_path'])
    record=aligned.iloc[0].to_dict();record['satellite']=16
    cache=tmp_path/'parallax';(cache/'heights').mkdir(parents=True)
    height=cache/'heights/height.nc';xr.Dataset({'HT':(('y','x'),[[1.]])}).to_netcdf(height)
    receipt=cache/'heights'/f'{record["asset_id"]}.json'
    evidence={'source_asset_id':record['asset_id'],'satellite':16,'path':str(height),'height_etag':'first'}
    atomic_json(receipt,evidence);calls=[]
    def calculate(*unused):calls.append(True);return {'indices':np.array([1],dtype='int64')}
    monkeypatch.setattr(parallax,'mapping_from_height',calculate)
    with NativeReader() as reader:
        dataset=reader.read(record);grid=target_grid(recipe)
        parallax.corrected_mapping(dataset,record,grid,cache)
        parallax.corrected_mapping(dataset,record,grid,cache)
        assert len(calls)==1
        evidence['height_etag']='changed';atomic_json(receipt,evidence)
        parallax.corrected_mapping(dataset,record,grid,cache)
        assert len(calls)==2
        evidence['height_etag']='uncached';atomic_json(receipt,evidence)
        parallax.corrected_mapping(dataset,record,grid,cache,budget_gib=1e-12)
        assert not list(cache.glob('*.npz')) and receipt.is_file() and height.is_file()


def test_config_explicit_arguments_override_json(tmp_path):
    from ecore_weather.processing.process_cli import parser
    config={'resolution_km':2,'workers':1}
    p=parser();p.set_defaults(**config)
    args=p.parse_args(['estimate','--resolution-km','3'])
    assert args.resolution_km==3 and args.workers==1


def test_controls_are_inert(monkeypatch):
    from ecore_weather.ui.process_ui import create
    from ecore_weather.processing import process_cli
    monkeypatch.setattr(process_cli,'run',lambda *args:pytest.fail('Construction must not process data'))
    assert len(create().children)>5


def test_process_dispatch_uses_shared_interface(monkeypatch):
    from ecore_weather.jobs import cli as jobs
    from ecore_weather.processing import process_cli
    seen=[]
    monkeypatch.setattr(process_cli,'main',lambda args:seen.append(args) or 75)
    assert jobs.main(['process','materialize','--mode','blocks'])==75
    assert seen==[['materialize','--mode','blocks']]


def test_regional_adapter_preserves_weights_and_rebuilds_geometry(monkeypatch):
    import sys,types,torch
    from ecore_weather.processing.process_model import caribbean_adapter
    base=torch.nn.Module();base._is_flash=True;base.amp=False
    for name in ('latitudes','longitudes','topo','valid_mask','conditioning_valid_mask'):
        base.register_buffer(name,torch.zeros((200,200)))
    base.register_parameter('test_weight',torch.nn.Parameter(torch.ones(1)))
    base.input_interp='old';base._flash_plan='old'
    module=types.ModuleType('earth2studio.models.nn.stormscope_flash');calls=[]
    def configure(model,*args):
        assert model.input_interp is None and model._flash_plan is None
        calls.append(model.latitudes[0,0].item())
    module.configure_region=configure
    monkeypatch.setitem(sys.modules,module.__name__,module)
    latitude=np.full((200,200),18.2);longitude=np.full((200,200),-66.4)
    support=np.ones((200,200),bool);support[0]=False
    result=caribbean_adapter(base,latitude,longitude,np.arange(200),np.arange(200),np.zeros((200,200)),support)
    assert result.test_weight is base.test_weight
    assert base.latitudes[0,0]==0 and base.input_interp=='old'
    assert not result.valid_mask[0].any() and result.valid_mask[1].all()
    assert calls==pytest.approx([18.2])


@pytest.mark.parametrize('exit_code',[0,75,1])
def test_pbs_process_wrapper_arguments_and_exit(tmp_path,exit_code):
    import os,subprocess,sys
    tools=tmp_path/'bin';tools.mkdir()
    for name,script in [('qstat','#!/bin/bash\necho "Resource_List.walltime = 00:20:00"\n'),
        ('mpiexec','#!/bin/bash\nprintf "%s\\n" "$@" > "$ECORE_MOCK_ARGS"\nexit "$ECORE_MOCK_EXIT"\n')]:
        path=tools/name;path.write_text(script);path.chmod(0o755)
    args=tmp_path/'arguments'
    env={**os.environ,'PATH':str(tools)+os.pathsep+os.environ['PATH'],'PBS_JOBID':'1234.pbs',
        'ECORE_PROJECT_ROOT':str(tmp_path/'project'),'ECORE_CODE_ROOT':str(Path.cwd()),
        'ECORE_PROCESS_PYTHON':sys.executable,'ECORE_NODE_SCRATCH':str(tmp_path/'scratch'),
        'ECORE_MOCK_ARGS':str(args),'ECORE_MOCK_EXIT':str(exit_code),'ECORE_REVISION':'not-a-job-gate'}
    result=subprocess.run(['bash','jobs/polaris_process.pbs'],env=env,text=True,capture_output=True)
    assert result.returncode==exit_code,result.stdout+result.stderr
    words=args.read_text().splitlines()
    assert words[words.index('--reserve-percent')+1]=='5'
    assert words[words.index('--destination')+1].startswith(str(tmp_path/'scratch'))
    assert words[words.index('--max-hours')+1]==str(300/3600)


def test_zero_height_satpy_mapping(native_selection):
    pytest.importorskip('satpy',reason='Parallax geometry runs in the separate processing environment')
    from ecore_weather.processing.process_catalog import load_index
    from ecore_weather.processing.transforms import NativeReader
    from ecore_weather.processing.process_parallax import mapping_from_height
    info,recipe=native_selection;_,aligned,_=load_index(info['preparation_path'])
    with NativeReader() as reader:
        ds=reader.read(aligned.iloc[0].to_dict())
        # Synthetic zero height is a geometry test, not a source-data replacement.
        height=ds.drop_vars([n for n in ds if n.startswith(('CMI','DQF'))]).copy(deep=True)
        height['HT']=(('y','x'),np.zeros((len(ds.y),len(ds.x)),dtype='float32'))
        height.HT.attrs['units']='m'
        mapped=mapping_from_height(ds,height,target_grid(recipe))
        decoded=decode_observation(ds)
        actual=regrid(decoded,mapped)
        expected=regrid(decoded,geometry(ds,target_grid(recipe)))
        common=actual['valid']&expected['valid']
        assert common.any()
        np.testing.assert_allclose(actual['values'][common],expected['values'][common],atol=.1)
        height.HT.values[:]=np.nan
        unknown=mapping_from_height(ds,height,target_grid(recipe))
        assert not unknown['support'].any()


def test_sharded_resume_and_timestamp_pixel_validity(native_selection,tmp_path):
    from ecore_weather.processing.process_catalog import build_index
    from ecore_weather.processing.process_store import materialize,validate_store
    info,recipe=native_selection
    changed=build_index(info['archive_root'],tmp_path/'sharded-prep',info['start'],info['end_excluded'],
        replace(recipe,shard_time=24),tmp_path/'sharded.duckdb')
    result=materialize(changed['preparation_path'],tmp_path/'out',workers=1)
    path=Path(result['units'][0]['path'])
    with xr.open_zarr(str(path/'data.zarr'),consolidated=False,mask_and_scale=False) as ds:
        assert ds.valid.dims==('time',) and ds.pixel_valid.dims==('time','variable','y','x')
        assert ds.valid.dtype==bool and not ds.valid.values.all()
        assert ds['values'].encoding['shards'][0]==24
    assert validate_store(path)['status']=='verified'
    assert materialize(changed['preparation_path'],tmp_path/'out',workers=1)['units'][0]['reused']


def test_training_stats_exclude_future_and_windows_crossing_splits(native_selection,tmp_path):
    from ecore_weather.processing.process_store import materialize,export_statistics,training_windows
    info,_=native_selection
    result=materialize(info['preparation_path'],tmp_path/'out',workers=1);unit=Path(result['units'][0]['path'])
    report=export_statistics(unit,'2022-09-18T00:10Z','2022-09-18T01:00Z',batch_frames=2)
    with xr.open_zarr(str(unit/'data.zarr'),consolidated=False,mask_and_scale=False) as ds:
        sub=ds.sel(time=slice('2022-09-18T00:10','2022-09-18T00:50'))
        values=sub['values'].where(sub.pixel_valid.astype(bool)).values
        stats=json.loads((unit/'stats.json').read_text())['values']['abi13c']
        assert stats['mean']==pytest.approx(np.nanmean(values),rel=1e-6)
        assert stats['std']==pytest.approx(np.nanstd(values),rel=1e-6)
    assert report['training_slots']==5
    windows=training_windows(info['preparation_path'],'2022-09-18T00:10Z','2022-09-18T02:00Z')
    assert not windows.iloc[0].eligible and not windows.iloc[-1].eligible


def test_water_vapor_never_in_cloud_top_recipe():
    with pytest.raises(ValueError,match='water-vapor'):Recipe(correction_bands=(10,))
    with pytest.raises(ValueError,match='24-frame'):Recipe(shard_time=12)


def test_archived_height_matching_is_causal_and_same_platform(tmp_path):
    from ecore_weather.processing.process_parallax import archived_height
    root=tmp_path/'goes/ABI-L2-ACHAF/goes16/roi-test/2022/09';root.mkdir(parents=True)
    (root/'raw.zarr.zip').write_bytes(b'test')
    atomic_json(root/'complete.json',{'satellite':16,'assets':[
        {'asset_id':'past','time':'2022-09-18T11:50Z','end_time':'2022-09-18T11:59Z'},
        {'asset_id':'future','time':'2022-09-18T12:00Z','end_time':'2022-09-18T12:09Z'}]})
    record={'path':str(tmp_path/'goes/ABI-L2-CMIPF/x.zip'),'satellite':16,'asset_id':'cmi','end_time':'2022-09-18T12:00Z'}
    assert archived_height(record)['height_asset_id']=='past'
    assert archived_height({**record,'satellite':19}) is None
    assert archived_height({**record,'end_time':'2022-09-18T13:00Z'}) is None


def test_retirement_deletes_only_rebuildable_inactive_legacy_assets(native_selection,tmp_path):
    import shutil
    from ecore_weather.processing.process_store import materialize
    from ecore_weather.processing.process_pilots import retire_icechunk
    info,_=native_selection
    result=materialize(info['preparation_path'],tmp_path/'new',workers=1)
    replacement=result['units'][0]['path'];root=Path(info['archive_root'])
    old=root/'processed/old/full/data.icechunk';old.mkdir(parents=True);(old/'obsolete').write_bytes(b'old')
    prep=root/'preparation/old';prep.mkdir(parents=True)
    shutil.copy(Path(info['preparation_path'])/'archives.parquet',prep/'archives.parquet')
    preview=retire_icechunk(root,replacement,tmp_path/'reports')
    assert old.is_dir() and preview['retired']
    applied=retire_icechunk(root,replacement,tmp_path/'reports',apply=True)
    assert not (root/'processed/old').exists() and applied['deleted_bytes']>0
    assert Path(replacement).is_dir()
    assert next(root.rglob('raw.zarr.zip')).is_file()


def test_height_product_preserves_native_arrays_and_has_no_band_selection(monkeypatch):
    from ecore_weather.sources import goes
    from ecore_weather.ingest.monthly_stream import _science
    monkeypatch.setattr(goes,'s3_client',lambda:None)
    obj={'Key':'ABI-L2-ACHAF/2022/261/16/OR_ABI-L2-ACHAF-M6_G16_s20222611600205_e20222611609513_c20222611612228.nc','Size':123,'ETag':'"abc"'}
    monkeypatch.setattr(goes,'list_objects',lambda *a:[obj])
    selection=goes.discover('2022-09-18T16:00Z','2022-09-18T16:10Z',product='ABI-L2-ACHAF',satellite=16)
    assert selection.bands==() and len(selection.assets)==1
    assert goes.acquisition_coverage(selection).expected_files.iloc[0]==6
    source=xr.Dataset({'HT':(('y','x'),np.array([[1,2]],'int16')),'DQF':(('y','x'),[[0,2]])})
    names=goes.selected_variables(source,())
    assert 'HT' in names and 'DQF' in names
    np.testing.assert_array_equal(_science(source,None).HT,source.HT)


def test_height_job_clips_partial_month_dates(tmp_path,capsys):
    from ecore_weather.jobs.jobs_goes import main
    assert main(['--product','ABI-L2-ACHAF','--start','2026-01-31T23:50Z',
        '--end','2026-02-01T00:10Z','--output',str(tmp_path),'--dry-run'])==0
    text=capsys.readouterr().out
    assert '2026-01-31T23:50:00' in text and '2026-02-01T00:10:00' in text


def test_missing_height_stays_unknown_instead_of_zero_or_uncorrected(native_selection,tmp_path):
    from ecore_weather.processing.process_catalog import load_index
    from ecore_weather.processing.process_store import transform_records
    info,recipe=native_selection;_,aligned,_=load_index(info['preparation_path'])
    arrays,metrics=transform_records([aligned.iloc[0].to_dict()],replace(recipe,parallax='cloud-height'),
        target_grid(recipe),tmp_path/'geometry',tmp_path/'heights')
    assert np.isnan(arrays['values']).all() and not arrays['valid'].any()
    assert not arrays['height_known'].any() and not arrays['parallax_applied'].any()
    assert metrics['missing_height_observations']==1


def test_reference_gpu_check_reports_blocked_without_downloads(tmp_path,monkeypatch):
    import torch
    from ecore_weather.processing.process_model import stock_flash_check
    monkeypatch.setattr(torch.cuda,'is_available',lambda:False)
    result=stock_flash_check(tmp_path)
    assert result['status']=='blocked' and 'CUDA GPU' in result['reason']
    assert (tmp_path/'model-reference.json').is_file()


def test_model_sample_freezes_committed_prefix_and_rejects_future_sources(tmp_path):
    from ecore_weather.processing.process_model import export_model_sample
    path=tmp_path/'active';path.mkdir();(path/'readers.lock').touch()
    times=np.datetime64('2021-01-15T15:00','ns')+np.arange(8)*np.timedelta64(10,'m')
    variables=[f'abi{b:02d}c' for b in (1,2,3,7,8,9,10,13)]
    shape=(8,8,200,200);geography=np.ones((200,200),'float32')
    ds=xr.Dataset({'values':(('time','variable','y','x'),np.ones(shape,'float32')),
        'pixel_valid':(('time','variable','y','x'),np.ones(shape,'uint8')),
        'source_time_ns':(('time','variable'),np.broadcast_to((times-np.timedelta64(3,'m')).astype('int64')[:,None],(8,8))),
        'source_end_ns':(('time','variable'),np.broadcast_to((times-np.timedelta64(30,'s')).astype('int64')[:,None],(8,8))),
        'latitude':(('y','x'),geography*18.2),'longitude':(('y','x'),geography*-66.4),
        'inside_bbox':(('y','x'),geography.astype(bool))},
        coords={'time':times,'variable':variables,'y':np.arange(200)*3000,'x':np.arange(200)*3000},
        attrs={'grid':{'resolution_m':3000},'recipe':{'parallax':'none'}})
    ds.to_zarr(path/'data.zarr',consolidated=False)
    atomic_json(path/'progress.json',{'verified_slots':6,'recipe_id':'scientific-fixture'})
    with pytest.raises(ValueError,match='allow-partial'):
        export_model_sample(path,tmp_path/'sample','2021-01-15T15:50Z',200)
    result=export_model_sample(path,tmp_path/'sample','2021-01-15T15:50Z',200,True)
    with np.load(result['path']) as sample:
        np.testing.assert_array_equal(sample['time_ns'],times[:6].astype('int64'))
        assert sample['support'].all() and sample['values'].shape==(6,8,200,200)
    with pytest.raises(ValueError,match='verified prefix'):
        export_model_sample(path,tmp_path/'future','2021-01-15T16:00Z',200,True)
    import zarr
    root=zarr.open_group(str(path/'data.zarr'),mode='a')
    root['source_end_ns'][2,0]=times[2].astype('int64')+1
    with pytest.raises(ValueError,match='future source'):
        export_model_sample(path,tmp_path/'bad','2021-01-15T15:50Z',200,True)
    from ecore_weather.processing.process_model import evaluate_caribbean_forecast
    with np.load(result['path']) as sample:
        prediction=tmp_path/'forecast.npz'
        np.savez_compressed(prediction,predictions=np.stack([sample['values'][-1]+.25]*2),
            last_observation=sample['values'][-1],latitude=sample['latitude'],longitude=sample['longitude'],
            variables=sample['variables'],support=sample['support'],lead_minutes=np.array([10,20]))
    with pytest.raises(KeyError):
        evaluate_caribbean_forecast(prediction,result['path'],tmp_path/'comparison')
    atomic_json(path/'progress.json',{'verified_slots':8,'recipe_id':'scientific-fixture'})
    comparison=evaluate_caribbean_forecast(prediction,result['path'],tmp_path/'comparison')
    assert len(comparison['comparisons'])==16
    assert comparison['future_observations_sent_to_model'] is False
    assert all(r['mae']==pytest.approx(.25) and r['persistence_mae']==0 for r in comparison['comparisons'])


def test_approximate_topography_requires_explicit_engineering_opt_in(tmp_path):
    import rasterio
    from rasterio.transform import from_origin
    from ecore_weather.processing.process_model import regional_topography
    path=tmp_path/'height.tif'
    with rasterio.open(path,'w',driver='GTiff',height=2,width=2,count=1,dtype='float32',
        crs='EPSG:4326',transform=from_origin(-67,19,1,1)) as dst:
        dst.write(np.array([[-1000,1000],[0,2000]],'float32'),1)
    atomic_json(path.with_suffix('.tif.json'),{'validation_level':'empirical-approximation',
        'scale_factor':.0001,'add_offset':.5,'ocean':'retain','checkpoint_convention_source':'explicit paired calibration'})
    lat=np.array([[18.5]]);lon=np.array([[-66.5]])
    with pytest.raises(ValueError,match='allow-approximate-topography'):regional_topography(path,lat,lon)
    np.testing.assert_allclose(regional_topography(path,lat,lon,allow_approximate=True),[[.4]])


def test_process_workers_match_serial_and_resume_committed_batches(native_selection,tmp_path):
    from ecore_weather.processing.process_store import materialize
    info,_=native_selection;prep=info['preparation_path'];committed=[]
    serial=materialize(prep,tmp_path/'serial',workers=1)
    paused=materialize(prep,tmp_path/'parallel',workers=2,prefetch_batches=2,
        stop=lambda:bool(committed),progress=lambda *args:committed.append(args[0]))
    assert paused['status']=='paused' and committed==[8]
    parallel=materialize(prep,tmp_path/'parallel',workers=2,prefetch_batches=2)
    a=json.loads((Path(serial['units'][0]['path'])/'progress.json').read_text())
    b=json.loads((Path(parallel['units'][0]['path'])/'progress.json').read_text())
    assert a['batches']==b['batches']
    assert b['execution']['executor']=='process' and b['execution']['workers']==2
