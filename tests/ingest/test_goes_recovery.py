from __future__ import annotations

"""Source defects are explicit gaps; unexplained reads cannot complete a store."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import hashlib
import json
import shutil

import numpy as np
import pytest

from ecore_weather.sources import goes
from ecore_weather.ingest import goes_shared
from ecore_weather.ingest import goes_staging
from ecore_weather.storage import catalog as catalog
from ecore_weather.ingest import monthly_stream
from ecore_weather.core.common import Selection
from ecore_weather.ingest.goes_recovery import SourceReadError, source_error
from ecore_weather.ingest.goes_shared import SharedConfig
from test_goes_shared import fake_sources


@pytest.mark.parametrize('kind',['verified_html','valid_fallback','unconfirmed_html'])
def test_individual_c02_failure_does_not_stop_other_bands(tmp_path,monkeypatch,kind):
    import obstore
    ds,assets,activity=fake_sources(monkeypatch,tmp_path)
    original_get=obstore.get_async;original_range=goes_shared.range_read
    chosen=[a for a in assets if 'C02_' in a.key or 'C13_' in a.key]
    bad=next(a for a in chosen if 'C02_' in a.key and a.key.endswith('3.nc'))
    content=b'<!DOCTYPE HTML><title>invalid published image</title>'
    if kind!='valid_fallback':
        replacement=replace(bad,size=len(content),etag=hashlib.md5(content).hexdigest()
                            if kind=='verified_html' else 'unsupported-non-md5-etag')
        chosen=[replacement if a==bad else a for a in chosen];bad=replacement
    calls=[]
    def range_read(asset,*args):
        if asset.id==bad.id:
            calls.append(asset.id)
            raise SourceReadError(asset,"not the signature of a valid netCDF4 file",metrics={'read_bytes':8})
        return original_range(asset,*args)
    async def get(store,key,**kwargs):
        if key==bad.key and kind!='valid_fallback':
            class Response:
                def stream(self,**kw):
                    async def blocks():yield content
                    return blocks()
            return Response()
        return await original_get(store,key,**kwargs)
    monkeypatch.setattr(goes_shared,'range_read',range_read)
    monkeypatch.setattr(obstore,'get_async',get)
    real=goes_shared.SharedReads
    monkeypatch.setattr(goes_shared,'SharedReads',lambda c,ctx:real(c,ctx,pool_class=ThreadPoolExecutor))
    selection=Selection('goes','ABI-L2-CMIPF','2022-09-18T12:00:00Z','2022-09-18T13:00:00Z',
                        (-70,14,-62,22),chosen,bands=(2,13),satellite=16)
    report=goes_shared.fetch(selection,tmp_path/'dest',SharedConfig(roi_budget_mib=8,staging_mib=8),
                             tmp_path/'scratch',index_results=False)
    rows={r['band']:r for r in report['monthly_archives']}
    assert rows[13]['status']=='saved'
    from ecore_weather.storage.archives import open_raw
    with open_raw(rows[13]['path']) as stored:
        np.testing.assert_array_equal(stored.CMI_C13,np.stack([ds.CMI]*8))
    if kind=='unconfirmed_html':
        assert rows[2]['status']=='deferred' and report['paused']
        assert not (tmp_path/'dest'/rows[2]['path']).parent.joinpath('complete.json').exists()
        assert not report['source_issues'] and len(calls)==2
    else:
        assert rows[2]['status']=='saved' and not report['paused']
        with open_raw(rows[2]['path']) as stored:
            expected=7 if kind=='verified_html' else 8
            np.testing.assert_array_equal(stored.CMI_C02,np.stack([ds.CMI]*expected))
            np.testing.assert_array_equal(stored.DQF_C02,np.stack([ds.DQF]*expected))
            assert stored.CMI_C02.attrs==ds.CMI.attrs
        if kind=='verified_html':
            assert len(report['source_issues'])==1
            assert report['source_issues'][0]['asset_id']==bad.id
            assert report['source_issues'][0]['integrity_evidence']['md5']==bad.etag
            marker=json.loads(__import__('pathlib').Path(rows[2]['path']).parent.joinpath('complete.json').read_text())
            assert marker['source_issues']==report['source_issues']
        else:assert report['source_issues']==[]


@pytest.mark.parametrize('old_schema',[False,True])
def test_new_gap_after_published_prefix_restores_without_refetching(interrupted,tmp_path,old_schema):
    selection,target,scratch,parent,dataset=interrupted
    folder=tmp_path/'eagle'/parent.name;receipt=folder/'checkpoint.json'
    if old_schema:
        state=json.loads(receipt.read_text());state['schema']=1
        for k in ('requested','requested_asset_ids_sha256','source_issues'):state.pop(k,None)
        receipt.write_text(json.dumps(state))
    old_parts={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in folder.glob('part-*.zip')}
    bad=selection.assets[20]
    catalog.record_corrupt_source(selection,bad,goes.ConfirmedCorruptSourceError('file signature not found',{
        'verification':'full-object-size-and-single-part-etag-md5','download_bytes':bad.size,'md5':bad.etag}))
    shutil.rmtree(scratch)
    read=[]
    def reader(rows,*args):
        for row in rows:
            index=selection.assets.index(row['asset']);read.append(index)
            assert index>=16 and index!=20
            yield row,(dataset(index),{})
    result=monthly_stream._write_one(selection,selection.assets,target,13,scratch=tmp_path/'ssd-two',reader_factory=reader)
    assert result['resumed_observations']==16 and read==[i for i in range(16,32) if i!=20]
    from ecore_weather.storage.archives import open_raw
    with open_raw(result['path']) as stored:
        np.testing.assert_array_equal(stored.CMI_C13,np.stack([dataset(i).CMI for i in range(32) if i!=20]))
    assert not folder.exists()  # Removed only after final destination verification.


def test_gap_inside_durable_prefix_cannot_change_verified_pixels(interrupted,tmp_path):
    selection,target,scratch,parent,dataset=interrupted
    bad=selection.assets[3]
    catalog.record_corrupt_source(selection,bad,goes.ConfirmedCorruptSourceError('file signature not found',{
        'verification':'full-object-size-and-single-part-etag-md5','download_bytes':bad.size,'md5':bad.etag}))
    receipt=tmp_path/'eagle'/parent.name/'checkpoint.json';before=receipt.read_bytes()
    shutil.rmtree(scratch)
    with pytest.raises(IOError,match='verified prefix'):
        monthly_stream._write_one(selection,selection.assets,target,13,scratch=tmp_path/'ssd-two',reader_factory=lambda *a:())
    assert receipt.read_bytes()==before and not (target/'complete.json').exists()


def test_programming_and_storage_errors_remain_fatal():
    import errno
    for exc in (ValueError('ROI outside source grid'),KeyError('CMI'),MemoryError(),
                OSError(errno.ENOSPC,'No space left'),OSError(errno.EACCES,'Permission denied')):
        assert not source_error(exc)
    assert source_error(ValueError("b'<!DOCTYP' is not the signature of a valid netCDF4 file"))


def test_evidence_write_failure_wakes_peers_as_fatal(tmp_path,monkeypatch):
    import queue,threading
    from ecore_weather.ingest import goes_recovery
    from ecore_weather.core.common import Asset
    asset=Asset('noaa-goes16','ABI-L2-CMIPF-M6C13_G16.nc',10,'a'*32,'2024-09-01')
    selection=Selection('goes','ABI-L2-CMIPF','2024-09-01','2024-10-01',(-70,14,-62,22),[asset],bands=(13,),satellite=16)
    class Client:
        error=None
        condition=threading.Condition()
        def __init__(self,*args):pass
        def close(self):pass
    monkeypatch.setattr(goes_shared,'ReaderClient',Client)
    def bad(*a,**k):
        raise SourceReadError(asset,'file signature not found',{
            'verification':'full-object-size-and-single-part-etag-md5','download_bytes':10,'md5':asset.etag})
    monkeypatch.setattr(monthly_stream,'_write_one',bad)
    monkeypatch.setattr(goes_recovery,'save_local_issues',lambda *a:(_ for _ in ()).throw(OSError('evidence disk unavailable')))
    events=queue.Queue()
    goes_shared.write_month('sep',selection,[((str(tmp_path/'data'),13),[asset])],SharedConfig(),None,None,events,str(tmp_path))
    event=events.get_nowait()
    assert event['op']=='failed' and 'evidence disk unavailable' in event['error']


def test_second_gap_after_new_partial_checkpoint_preserves_both_prefixes(interrupted,tmp_path):
    selection,target,scratch,parent,dataset=interrupted
    def add(index):
        bad=selection.assets[index]
        catalog.record_corrupt_source(selection,bad,goes.ConfirmedCorruptSourceError('file signature not found',{
            'verification':'full-object-size-and-single-part-etag-md5','download_bytes':bad.size,'md5':bad.etag}))
    add(20);shutil.rmtree(scratch)
    def interrupted_again(rows,*args):
        for row in rows:
            i=selection.assets.index(row['asset'])
            if i==25:raise IOError('fixture second allocation ended')
            yield row,(dataset(i),{})
    with pytest.raises(IOError,match='second allocation'):
        monthly_stream._write_one(selection,selection.assets,target,13,scratch=tmp_path/'ssd-two',reader_factory=interrupted_again)
    receipt=tmp_path/'eagle'/parent.name/'checkpoint.json'
    assert json.loads(receipt.read_text())['next_index']==24
    shutil.rmtree(tmp_path/'ssd-two');add(26);read=[]
    def final(rows,*args):
        for row in rows:
            i=selection.assets.index(row['asset']);read.append(i)
            yield row,(dataset(i),{})
    result=monthly_stream._write_one(selection,selection.assets,target,13,scratch=tmp_path/'ssd-three',reader_factory=final)
    assert result['resumed_observations']==24 and read==[25,27,28,29,30,31]
    from ecore_weather.storage.archives import open_raw
    with open_raw(result['path']) as stored:
        np.testing.assert_array_equal(stored.CMI_C13,np.stack([dataset(i).CMI for i in range(32) if i not in (20,26)]))


@pytest.mark.parametrize('field,value',[('ETag','"wrong"'),('ContentRange','bytes 1-4/10'),
                                      ('ContentRange','bytes 0-3/11')])
def test_metadata_mismatch_retries_without_exposing_bytes_to_decoder(monkeypatch,field,value):
    from types import SimpleNamespace
    import obstore
    from ecore_weather.core.common import Transport,Asset
    asset=Asset('noaa-goes16','image.nc',10,'etag','2024-09-01')
    metadata={'meta':{'e_tag':'etag','size':10},'range':(0,4)}
    if field=='ETag':metadata['meta']['e_tag']=value
    elif value.startswith('bytes 1'):metadata['range']=(1,5)
    else:metadata['meta']['size']=11
    response=SimpleNamespace(**metadata,bytes=lambda:b'data')
    monkeypatch.setattr(obstore,'get',lambda *a,**k:response)
    monkeypatch.setattr('ecore_weather.core.common.time.sleep',lambda _:None)
    transport=Transport('obstore');transport._stores[asset.bucket]=object()
    from ecore_weather.ingest.goes_recovery import InvalidResponseError
    with pytest.raises(InvalidResponseError):transport.read(asset,0,4)
    assert transport.requests==3 and transport.bytes==12 and transport.failed_requests==1


"""A fresh allocation reuses only verified, durably copied native pixels."""
import json
from pathlib import Path
import shutil
import zipfile

import numpy as np
import pytest
import xarray as xr

from ecore_weather.core.common import Asset, Selection
from ecore_weather.ingest import monthly_stream
from ecore_weather.ingest.durable_checkpoints import DurableBatches


@pytest.fixture
def interrupted(tmp_path, monkeypatch, request):
    monkeypatch.delenv('ECORE_GOES_LIMIT_LEDGER', raising=False)
    monkeypatch.setenv('ECORE_GOES_CHECKPOINT_ROOT', str(tmp_path / 'eagle'))
    monkeypatch.setenv('ECORE_GOES_CHECKPOINT_FRACTION', str(getattr(request,'param',.25)))
    assets = [Asset('noaa-goes16', f'ABI-L2-CMIPF-M6C13-{i}', 100, f'etag-{i}',
                    f'2021-01-01T{i//6:02}:{i%6*10:02}:00Z') for i in range(32)]
    selection = Selection('goes', 'ABI-L2-CMIPF', '2021-01-01', '2021-02-01',
                          (-70,14,-62,22), assets, bands=(13,), satellite=16)
    def dataset(index):
        ds = xr.Dataset({'CMI': (('y','x'), np.full((2,3), index, dtype='int16')),
                         'DQF': (('y','x'), np.full((2,3), index%4, dtype='int8'))},
                        coords={'x': [.1,.2,.3], 'y': [.4,.3]})
        ds.CMI.attrs.update(scale_factor=.1, add_offset=20, _FillValue=-1)
        ds.attrs.update(scan_index=index, calibration=f'scan-{index}')
        return ds
    def reader(rows, *args):
        for row in rows:
            i = assets.index(row['asset'])
            if i == 16:
                raise IOError('simulated node loss after half the scans')
            yield row, (dataset(i), {})
    target = tmp_path / 'dest/month'
    scratch = tmp_path / 'ssd-one'
    with pytest.raises(IOError, match='simulated node loss'):
        monthly_stream._write_one(selection, assets, target, 13, scratch=scratch,
                                  reader_factory=reader)
    parent = next(scratch.glob('goes-cmipf-*'))
    return selection, target, scratch, parent, dataset


def test_incremental_parts_and_resume_after_node_scratch_erased(interrupted, tmp_path):
    selection, target, scratch, parent, dataset = interrupted
    receipt = tmp_path / 'eagle' / parent.name / 'checkpoint.json'
    state = json.loads(receipt.read_text())
    assert state['next_index'] == 16
    assert [(p['begin'],p['end']) for p in state['parts']] == [(0,8),(8,16)]
    chunks = []
    for part in state['parts']:
        with zipfile.ZipFile(receipt.parent / part['file']) as archive:
            chunks.append({n for n in archive.namelist() if n.startswith('raw.zarr/CMI_C13/c/')})
            assert not any('source-cache' in n for n in archive.namelist())
    # Zarr may omit all-fill chunks (the synthetic first frame is all zero).
    assert 0 < len(chunks[0]) <= 8 and len(chunks[1]) == 8 and not chunks[0] & chunks[1]
    shutil.rmtree(scratch)  # A later PBS allocation starts with an empty SSD.
    read = []
    def reader(rows, *args):
        for row in rows:
            i = selection.assets.index(row['asset']);read.append(i)
            assert i >= 16
            yield row, (dataset(i), {})
    result = monthly_stream._write_one(selection, selection.assets, target, 13,
        scratch=tmp_path/'ssd-two', reader_factory=reader)
    assert result['resumed_observations'] == 16 and read == list(range(16,32))
    assert (target / 'complete.json').is_file() and not receipt.parent.exists()
    from ecore_weather.storage.archives import open_raw
    with open_raw(result['path']) as archived:
        np.testing.assert_array_equal(archived.CMI_C13, np.stack([dataset(i).CMI for i in range(32)]))
        np.testing.assert_array_equal(archived.DQF_C13, np.stack([dataset(i).DQF for i in range(32)]))
        assert len(archived.source_metadata_json) == 32
        for i, meta in enumerate(archived.source_metadata_json.values):
            assert json.loads(meta)['dataset']['scan_index'] == i


def test_corrupt_durable_part_fails_without_new_source_reads(interrupted, tmp_path):
    selection, target, scratch, parent, _ = interrupted
    folder = tmp_path/'eagle'/parent.name
    part = next(folder.glob('part-*.zip'));part.write_bytes(b'corrupt')
    shutil.rmtree(scratch)
    def reader(*args):
        pytest.fail('No NOAA reads before restoring and verifying durable parts')
    with pytest.raises(IOError, match='hash mismatch'):
        monthly_stream._write_one(selection, selection.assets, target, 13,
            scratch=scratch, reader_factory=reader)
    assert not (target/'complete.json').exists() and part.exists()


def test_failed_copy_keeps_previous_receipt_and_can_retry(interrupted, tmp_path, monkeypatch):
    selection, _, _, parent, _ = interrupted
    rows = monthly_stream._archive_rows(selection, selection.assets, {})
    durable = DurableBatches(tmp_path/'other-eagle', parent, rows)
    assert durable.publish(8)
    before = (durable.folder/'checkpoint.json').read_bytes()
    original = shutil.copyfile
    monkeypatch.setattr(shutil, 'copyfile', lambda *args: (_ for _ in ()).throw(IOError('copy interrupted')))
    with pytest.raises(IOError, match='copy interrupted'):
        durable.publish(16)
    assert (durable.folder/'checkpoint.json').read_bytes() == before
    monkeypatch.setattr(shutil, 'copyfile', original)
    assert durable.publish(16) and durable.count == 16


def test_changed_scientific_identity_is_rejected(interrupted, tmp_path):
    selection, _, _, parent, _ = interrupted
    rows = monthly_stream._archive_rows(selection, selection.assets, {})
    rows[0]['asset_id'] = 'different-source'
    with pytest.raises(IOError, match='selection differs'):
        DurableBatches(tmp_path/'eagle', parent, rows)


@pytest.mark.parametrize('interrupted',[1.0],indirect=True)
def test_controlled_failure_flushes_verified_prefix_before_interval(interrupted,tmp_path):
    _, _, _, parent, _=interrupted
    state=json.loads((tmp_path/'eagle'/parent.name/'checkpoint.json').read_text())
    assert state['next_index']==16 and len(state['parts'])==1
    assert (state['parts'][0]['begin'],state['parts'][0]['end'])==(0,16)


def test_checkpoint_bytes_are_counted_by_storage_cap(tmp_path, monkeypatch):
    from ecore_weather.ingest import goes_limits
    monkeypatch.delenv('ECORE_GOES_LIMIT_LEDGER', raising=False)
    monkeypatch.delenv('ECORE_GOES_ACCOUNTING_ROOTS', raising=False)
    checkpoints = tmp_path/'separate-checkpoints';checkpoints.mkdir()
    (checkpoints/'part.zip').write_bytes(b'x'*1024)
    try:
        ledger = goes_limits.configure(tmp_path/'dest', tmp_path/'state', tmp_path/'ssd', 1,
                                      extra_roots=[checkpoints])
        data = json.loads(ledger.read_text())
        assert goes_limits.physical_bytes(data['roots']) == 1024
    finally:
        monkeypatch.delenv('ECORE_GOES_LIMIT_LEDGER', raising=False)


def test_durable_files_cannot_share_the_working_directory(tmp_path):
    with pytest.raises(ValueError,match='separate from working scratch'):
        DurableBatches(tmp_path,tmp_path/'band',[])
