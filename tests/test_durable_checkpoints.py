"""A fresh allocation reuses only verified, durably copied native pixels."""
import json
from pathlib import Path
import shutil
import zipfile

import numpy as np
import pytest
import xarray as xr

from ecore_weather.common import Asset, Selection
from ecore_weather import monthly_stream
from ecore_weather.durable_checkpoints import DurableBatches


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
    from ecore_weather.storage import open_raw
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
    from ecore_weather import goes_limits
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
