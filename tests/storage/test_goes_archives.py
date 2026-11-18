from __future__ import annotations

"""Grouped MCMIPF archive contracts using tiny packed arrays."""

import gzip
import json
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pytest
import zarr

from ecore_weather.ingest.goes_monthly import _append, _band_group, _pack, _verify_stage
from ecore_weather.ingest import goes_monthly
from ecore_weather.storage.archives import open_raw
from ecore_weather.storage.discovery import inventory
from ecore_weather.storage.catalog import load_selection, save_selection
from ecore_weather.core.common import Asset, Selection
from ecore_weather.jobs.index import record_fetch, search


@pytest.mark.parametrize("archive_product", ("ABI-L2-MCMIPF", "ABI-L2-CMI-2KM-HYBRID"))
def test_grouped_month_preserves_missing_channel_and_viewer_rows(tmp_path, archive_product):
    folder = tmp_path / "goes" / archive_product / "goes16" / "roi-example" / "2022" / "09"
    folder.mkdir(parents=True)
    working = folder / "working.zarr"
    root = zarr.open_group(str(working), mode="a")
    root.attrs.update({"source": "goes", "product": archive_product,
                       "requested_bbox": [-70.24, 14.36, -62.56, 22.04]})
    x, y = np.array([1., 2.]), np.array([3., 4.])
    scans = []
    batches = []
    for i, bands in enumerate(((1, 13), (13,))):
        stamp = f"2022-09-18T12:{i}0:00Z"
        images = {band: {"cmi": np.full((2, 2), i + band, dtype="int16"),
                         "dqf": np.zeros((2, 2), dtype="int8"),
                         "cmi_attrs": {"scale_factor": 0.1}, "dqf_attrs": {},
                         "origin": {"product": "ABI-L2-CMIPF" if
                                    archive_product == "ABI-L2-CMI-2KM-HYBRID" and band == 13
                                    else "ABI-L2-MCMIPF"}}
                  for band in bands}
        scan = {"x": x, "y": y, "x_attrs": {}, "y_attrs": {},
                "time": stamp, "images": images}
        checksums = {}
        for band in bands:
            group = _band_group(root, band, scan)
            index, hashes = _append(group, band, scan)
            checksums[str(band)] = {"index": index, **hashes}
        asset_id = f"scan-{i}"
        scans.append({"asset_id": asset_id, "time": stamp,
                      "available_bands": list(bands), "source_bytes": 100})
        batches.append({"rows": [{"asset_id": asset_id, "checksums": checksums}]})
    _verify_stage(root, batches)
    with gzip.open(working / "ecore_metadata.json.gz", "wt") as output:
        json.dump({"dataset_attrs": dict(root.attrs),
                   "groups": {"C01": {"asset_ids": ["scan-0"]},
                              "C13": {"asset_ids": ["scan-0", "scan-1"]}},
                   "source_metadata": {row["asset_id"]: json.dumps({"variables": {}})
                                       for row in scans}}, output)
    archive = folder / "raw.zarr.zip"
    _pack(working, archive)
    (folder / "complete.json").write_text(json.dumps({"source": "goes",
        "product": archive_product, "raw_path": archive.name,
        "assets": scans, "stored_bytes": archive.stat().st_size}))
    with open_raw(archive, group="C01") as band1:
        assert band1.sizes["time"] == 1
        np.testing.assert_array_equal(band1.CMI_C01.values[0], np.full((2, 2), 1))
    with open_raw(archive, group="C13") as band13:
        assert band13.sizes["time"] == 2
        np.testing.assert_array_equal(band13.CMI_C13.values[1], np.full((2, 2), 14))
    found = inventory(tmp_path, source="goes")
    assert [(row["asset_id"], row["band"]) for row in found] == [
        ("scan-0", 1), ("scan-0", 13), ("scan-1", 13)]


def test_goes_stac_keeps_discovery_facts_without_changing_selection_id(tmp_path):
    asset = Asset("noaa-goes16", "ABI-L2-MCMIPF/example.nc", 100, "etag",
                  "2022-09-18T12:00:00Z")
    selection = Selection("goes", "ABI-L2-MCMIPF", "2022-09-18T12:00:00Z",
        "2022-09-18T13:00:00Z", (-70.24, 14.36, -62.56, 22.04), [asset],
        bands=(1, 13), satellite=16)
    before = selection.id
    selection.discovery_facts = {"duplicate_processing_versions": [],
        "hours_without_files": [], "undersubscribed_hours": [],
        "scan_mode_changes": []}
    save_selection(selection, tmp_path, index_results=False)
    loaded = load_selection(tmp_path / "items.json.gz")
    assert loaded.id == before
    assert loaded.discovery_facts == selection.discovery_facts


def test_hybrid_and_pure_band_records_coexist_in_local_index(tmp_path, monkeypatch):
    monkeypatch.setenv("ECORE_INDEX_PATH", str(tmp_path / "index.duckdb"))
    for product, suffix in (("ABI-L2-MCMIPF", ""),
                            ("ABI-L2-CMI-2KM-HYBRID", "-HYBRID")):
        folder=tmp_path/product
        folder.mkdir();archive=folder/'raw.zarr.zip';archive.write_bytes(b'archive')
        from ecore_weather.core.common import write_json
        write_json(folder/'complete.json',{'source':'goes','product':product,'raw_path':archive.name,
            'observations':1,'assets':[{'asset_id':'same-scan','time':'2026-01-01T00:00:00Z','available_bands':[13]}]})
        record_fetch({"source": "goes", "monthly_archives":[{'path':str(archive),'observations':1}], "selection_id": "same-source-selection",
            "archive_product": product, "run_id": product,
            "selection_summary": {"product": "ABI-L2-MCMIPF"},
            "root": str(tmp_path), "wall_s": 1,
            "records": [{"asset_id": "same-scan", "subset_id": "same-roi-C13" + suffix,
                         "time": "2026-01-01T00:00:00Z", "band": 13,
                         "source_url": "https://noaa-goes19.s3.amazonaws.com/example.nc",
                         "source_bytes": 100, "url": str(tmp_path / product / "raw.zarr.zip"),
                         "status": "archived"}]})
    found = search(tmp_path, source="goes", band=13)
    assert {row["product"] for row in found} == {
        "ABI-L2-MCMIPF", "ABI-L2-CMI-2KM-HYBRID"}


def test_failed_scan_batch_is_retried_on_resume(tmp_path, monkeypatch):
    """A transient source error must not become a completed scratch checkpoint."""
    class LocalPool(ThreadPoolExecutor):
        def __init__(self, max_workers, mp_context=None, initializer=None):
            super().__init__(max_workers=max_workers)

    monkeypatch.setattr(goes_monthly, "ProcessPoolExecutor", LocalPool)
    assets = [Asset("noaa-goes16", f"ABI-L2-MCMIPF/scan-{i}.nc", 100, "etag",
                    f"2022-09-18T12:{i}0:00Z") for i in range(2)]
    selection = Selection("goes", "ABI-L2-MCMIPF", "2022-09-18T12:00:00Z",
        "2022-09-18T13:00:00Z", (-70.24, 14.36, -62.56, 22.04), assets,
        bands=(13,), satellite=16)
    attempts = {assets[1].id: 0}

    def read_batch(portion, *_args):
        rows = []
        for asset in portion:
            if asset.id in attempts:
                attempts[asset.id] += 1
                if attempts[asset.id] == 1:
                    rows.append({"status": "source_error", "asset_id": asset.id,
                        "time": asset.time, "source_url": asset.url, "etag": asset.etag,
                        "source_bytes": asset.size, "bucket": asset.bucket,
                        "key": asset.key, "error": "temporary read failure"})
                    continue
            rows.append({"status": "ok", "asset_id": asset.id, "time": asset.time,
                "source_url": asset.url, "etag": asset.etag, "source_bytes": asset.size,
                "bucket": asset.bucket, "key": asset.key, "end_time": None,
                "metadata": json.dumps({"variables": {}}),
                "x": np.array([1., 2.]), "y": np.array([3., 4.]),
                "x_attrs": {}, "y_attrs": {}, "read_decode_crop_s": 0.01,
                "images": {13: {"cmi": np.full((2, 2), 13, dtype="int16"),
                    "dqf": np.zeros((2, 2), dtype="int8"), "cmi_attrs": {},
                    "dqf_attrs": {}, "origin": {"product": "ABI-L2-MCMIPF",
                    "asset_id": asset.id, "source_url": asset.url, "etag": asset.etag}}}})
        return {"results": rows, "payload_bytes": 40, "read_bytes": 100,
                "range_requests": 1, "transfer_task_seconds": 0.01,
                "source_retries": 0, "failed_requests": 0,
                "batch_wall_seconds": 0.01, "reader_cpu_seconds": 0.01}

    monkeypatch.setattr(goes_monthly, "_read_batch", read_batch)
    target, scratch = tmp_path / "month", tmp_path / "scratch"
    kwargs = dict(reader_processes=1, source_threads=1, prefetch_mib=16,
                  scratch=scratch)
    with pytest.raises(OSError, match="restart will retry"):
        goes_monthly._write_one(selection, assets, target, **kwargs)
    stage = goes_monthly._stage_path(scratch, selection, assets, selection.product)
    progress = json.loads((stage / "progress.json").read_text())
    assert progress["next_index"] == 1
    result = goes_monthly._write_one(selection, assets, target, **kwargs)
    assert result["status"] == "saved" and result["observations"] == 2
    assert attempts[assets[1].id] == 2
    assert not stage.exists()


def test_batch_writer_keeps_per_band_times_and_missing_channel(tmp_path):
    root = zarr.open_group(str(tmp_path / "batch.zarr"), mode="w")
    scans = []
    for i, available in enumerate(((1, 13), (13,), (1, 13))):
        scans.append({"status": "ok", "asset_id": f"scan-{i}",
            "time": f"2022-09-18T12:{i}0:00Z", "source_url": f"noaa/scan-{i}",
            "etag": "etag", "source_bytes": 100, "bucket": "noaa-goes16",
            "key": f"scan-{i}.nc", "end_time": None, "metadata": "{}",
            "read_decode_crop_s": 0.01, "x": np.array([1., 2.]),
            "y": np.array([3., 4.]), "x_attrs": {}, "y_attrs": {},
            "images": {band: {"cmi": np.full((2, 2), i + band, dtype="int16"),
                "dqf": np.zeros((2, 2), dtype="int8"), "cmi_attrs": {},
                "dqf_attrs": {}, "origin": {"product": "ABI-L2-MCMIPF"}}
                for band in available}})
    rows = goes_monthly._write_batch(root, {"results": scans}, (1, 13))
    goes_monthly._verify_stage(root, [{"rows": rows}])
    assert [row["available_bands"] for row in rows] == [[1, 13], [13], [1, 13]]
    assert root["C01/time"].shape == (2,)
    assert root["C13/time"].shape == (3,)
    np.testing.assert_array_equal(root["C01/CMI_C01"][:, 0, 0], [1, 3])


"""Annual backup gates and recovery use tiny files, never live NOAA/HF writes."""
from contextlib import nullcontext
import hashlib
import io
import json
from pathlib import Path
import zipfile

import pytest

from ecore_weather.storage import goes_backup as backup
from ecore_weather.storage import jobs_backup
from ecore_weather.core.common import PR_BBOX, write_json


def make_year(root, study, year):
    for month in jobs_backup._year_months(year):
        entries=[]
        for satellite in ((16,19) if (year,month)==(2025,4) else (19,) if year>=2026 else (16,)):
            for band in backup.BANDS:
                stamp=f'{year}-{month:02d}-'+('08' if satellite==19 and year==2025 else '01')+'T00:00:20Z'
                name=f'goes/{backup.PRODUCT}/goes{satellite}/C{band:02d}/roi-native/{year}/{month:02d}/raw.zarr.zip'
                archive=root/name;archive.parent.mkdir(parents=True,exist_ok=True)
                archive.write_bytes(f'{year}-{month}-{satellite}-{band}'.encode())
                asset=f'{year}-{month}-{satellite}-{band}'
                write_json(archive.parent/'complete.json',{
                    'source':'goes','product':backup.PRODUCT,'band':band,'satellite':satellite,
                    'region':PR_BBOX,'raw_path':archive.name,'observations':1,
                    'stored_bytes':archive.stat().st_size,'archive_sha256':jobs_backup._sha256(archive),
                    'asset_ids':[asset],'assets':[{'asset_id':asset,'time':stamp,
                        'source_url':f'https://noaa-goes{satellite}.s3.amazonaws.com/{backup.PRODUCT}/{year}/scan.nc'}]})
                entries.append({'path':str(archive)})
        stage=f'h{1 if month<=6 else 2}-{year}'
        checkpoint=study/'checkpoints'/stage/f'{year}-{month:02d}-01.json'
        write_json(checkpoint,{'status':'complete','product':backup.PRODUCT,'bands':backup.BANDS,
            'start':f'{year}-{month:02d}-01',
            'end_excluded':f'{year+1}-01-01' if month==12 else f'{year}-{month+1:02d}-01',
            'selected_assets':len(entries),'archives':entries,
            'available_bands':backup.BANDS,'missing_bands':[]})


def test_annual_goes_bundle_preserves_native_band_and_satellite_members(tmp_path):
    root,study=tmp_path/'data',tmp_path/'study';make_year(root,study,2025)
    entries,coverage,digest=backup.collect_year(2025,root,study)
    bundle=jobs_backup._build_bundle(2025,backup.PRODUCT,entries,[],tmp_path/'work',root,
        source='goes',region_id=backup.REGION,coverage=coverage,selection_sha256=digest)
    with zipfile.ZipFile(bundle['path']) as z:
        manifest=json.loads(z.read('year_manifest.json'))
        assert manifest['source']=='goes' and 'local_source_root' not in manifest
        assert len(manifest['archives'])==104 and len(manifest['coverage'])==12
        assert {r['satellite'] for r in manifest['archives']}=={16,19}
        assert all('asset_ids' not in row for row in manifest['archives'])
        for row in manifest['archives']:
            assert z.read(row['member'])==(root/row['local_path']).read_bytes()
            assert z.getinfo(row['member']).compress_type==zipfile.ZIP_STORED
    assert bundle['sha256']==jobs_backup._sha256(bundle['path'])


def test_h1_2026_and_deduplicated_physical_members(tmp_path):
    root,study=tmp_path/'data',tmp_path/'study';make_year(root,study,2026)
    entries,coverage,_=backup.collect_year(2026,root,study)
    assert len(entries)==48 and len(coverage)==6
    assert coverage[-1]['end_excluded'].startswith('2026-07-01')
    path=backup._month_checkpoint(study,2026,1);row=json.loads(path.read_text())
    row['archives'].append(row['archives'][0]);write_json(path,row)
    with pytest.raises(ValueError,match='Duplicate physical'):backup.collect_year(2026,root,study)


@pytest.mark.parametrize('change',['missing_band','wrong_product','missing_month','bad_count'])
def test_incomplete_or_relabelled_selection_blocks_backup(tmp_path,change):
    root,study=tmp_path/'data',tmp_path/'study';make_year(root,study,2026)
    path=backup._month_checkpoint(study,2026,1);row=json.loads(path.read_text())
    if change=='missing_month':path.unlink()
    else:
        if change=='missing_band':row['archives'].pop()
        if change=='wrong_product':row['product']='ABI-L2-MCMIPF'
        if change=='bad_count':row['selected_assets']+=1
        write_json(path,row)
    with pytest.raises(ValueError):backup.collect_year(2026,root,study)


def test_full_study_gate_runs_before_credentials_or_uploads(tmp_path,monkeypatch):
    monkeypatch.setattr('ecore_weather.storage.archives.configured_bucket',lambda:pytest.fail('HF must not be contacted'))
    with pytest.raises(ValueError,match='checkpoint'):
        backup.backup_study(tmp_path,tmp_path/'study',tmp_path/'work')


def test_annual_coverage_requires_matching_confirmed_source_gap(tmp_path):
    root,study=tmp_path/'data',tmp_path/'study';make_year(root,study,2026)
    path=backup._month_checkpoint(study,2026,1);saved=json.loads(path.read_text())
    archive=Path(saved['archives'][0]['path']);marker_path=archive.parent/'complete.json'
    marker=json.loads(marker_path.read_text())
    issue={'asset_id':'bad-source','status':'confirmed_corrupt_source','product':backup.PRODUCT,
        'region':list(PR_BBOX),'source_url':f'https://noaa-goes19.s3.amazonaws.com/{backup.PRODUCT}/bad.nc',
        'time':'2026-01-01T01:00:20Z','source_bytes':100,'etag':'a'*32,
        'integrity_evidence':{'verification':'full-object-size-and-single-part-etag-md5',
                              'download_bytes':100,'md5':'a'*32}}
    marker['source_issues']=[issue];write_json(marker_path,marker)
    saved['selected_assets']+=1;saved['source_issues']=[issue];write_json(path,saved)
    _,coverage,_=backup.collect_year(2026,root,study)
    assert coverage[0]['unavailable_observations']==1
    saved['source_issues']=[];write_json(path,saved)
    with pytest.raises(ValueError,match='observation count'):backup.collect_year(2026,root,study)


def test_preflight_requires_persistent_scratch_and_checks_space(tmp_path,monkeypatch):
    import subprocess
    from types import SimpleNamespace
    root=tmp_path/'data';root.mkdir()
    args=['--destination',str(root),'--scratch',str(tmp_path/'scratch')]
    monkeypatch.setattr(subprocess,'check_output',lambda *a,**k:'tmpfs\n')
    with pytest.raises(OSError,match='persistent Linux'):backup.local_preflight(args)
    monkeypatch.setattr(subprocess,'check_output',lambda *a,**k:'ext4\n')
    monkeypatch.setattr(backup.shutil,'disk_usage',lambda *a:SimpleNamespace(free=1024**3))
    with pytest.raises(OSError,match='DAS free space'):backup.local_preflight(args)


def test_annual_publisher_has_one_owner(tmp_path):
    with backup._backup_owner(tmp_path):
        with pytest.raises(OSError,match='Another GOES annual publisher'):
            with backup._backup_owner(tmp_path):pass


def test_corrupt_monthly_bytes_cannot_be_packaged(tmp_path):
    root,study=tmp_path/'data',tmp_path/'study';make_year(root,study,2026)
    entries,coverage,digest=backup.collect_year(2026,root,study)
    entry=next(iter(entries.values()));original=entry['archive'].read_bytes()
    entry['archive'].write_bytes(b'x'*len(original))
    with pytest.raises(IOError,match='Monthly ZIP differs'):
        jobs_backup._build_bundle(2026,backup.PRODUCT,entries,[],tmp_path/'work',root,
            source='goes',region_id=backup.REGION,coverage=coverage,selection_sha256=digest)
    assert entry['archive'].exists()


class Body(io.BytesIO):
    def iter_chunks(self,chunk_size):
        while data:=self.read(chunk_size):yield data


class Writer:
    objects={}
    corrupt=False
    uploads=0
    def __init__(self,root):self.client=self;self.config={'bucket':'bucket'};self.bucket_id='ns/bucket'
    def key(self,key):return key
    def marker(self,prefix):
        data=self.objects.get(prefix+'/complete.json')
        return json.loads(data) if data else None
    def upload_file(self,path,bucket,key,**kwargs):
        type(self).uploads+=1;self.objects[key]=Path(path).read_bytes()
        if kwargs.get('Callback'):kwargs['Callback'](len(self.objects[key]))
    def head_object(self,Bucket,Key):return {'ContentLength':len(self.objects[Key])}
    def get_object(self,Bucket,Key):
        data=self.objects[Key]
        return {'Body':Body(b'x'*len(data) if self.corrupt and Key.endswith('.zip') else data)}
    def put_object(self,Bucket,Key,Body,**kwargs):self.objects[Key]=Body


@pytest.fixture
def fake_hf(monkeypatch):
    Writer.objects={};Writer.corrupt=False;Writer.uploads=0
    monkeypatch.setattr(jobs_backup,'BucketWriter',Writer)
    monkeypatch.setattr(jobs_backup,'bucket_writer',lambda *a,**k:nullcontext())
    return Writer


def test_upload_readback_gate_receipt_reuse_and_cleanup(tmp_path,monkeypatch,fake_hf):
    root,study=tmp_path/'data',tmp_path/'study';make_year(root,study,2026)
    monkeypatch.setattr(backup,'validate_study',lambda *a:[{'year':2026}])
    work=tmp_path/'work'
    assert backup.backup_study(root,study,work,'hf://buckets/ns/bucket')==0
    assert not list(work.glob('*.zip')) and len(list(root.rglob('raw.zarr.zip')))==48
    receipt=json.loads((study/'backups/2026.json').read_text())
    assert receipt['status']=='complete' and fake_hf.uploads==1
    assert any(k.endswith('/year_manifest.json') for k in fake_hf.objects)
    assert backup.backup_study(root,study,work,'hf://buckets/ns/bucket')==0
    assert fake_hf.uploads==1  # Rechecks the remote hash without rebuilding a ZIP.


def test_failed_readback_preserves_work_and_publishes_no_completion(tmp_path,monkeypatch,fake_hf):
    root,study=tmp_path/'data',tmp_path/'study';make_year(root,study,2026)
    monkeypatch.setattr(backup,'validate_study',lambda *a:[{'year':2026}])
    fake_hf.corrupt=True
    with pytest.raises(IOError,match='read-back hash'):
        backup.backup_study(root,study,tmp_path/'work','hf://buckets/ns/bucket')
    assert list((tmp_path/'work').glob('*.zip'))
    assert not any(k.endswith('/complete.json') for k in fake_hf.objects)
    assert json.loads((study/'backups/progress.json').read_text())['status']=='failed'
    fake_hf.corrupt=False
    assert backup.backup_study(root,study,tmp_path/'work','hf://buckets/ns/bucket')==0
    assert not list((tmp_path/'work').glob('*.zip'))


def test_pause_never_starts_upload(tmp_path,monkeypatch,fake_hf):
    root,study=tmp_path/'data',tmp_path/'study';make_year(root,study,2026)
    monkeypatch.setattr(backup,'validate_study',lambda *a:[{'year':2026}])
    pause=study/'pause.request';pause.touch()
    assert backup.backup_study(root,study,tmp_path/'work','hf://buckets/ns/bucket',pause_path=pause)==75
    assert not fake_hf.objects


def test_dispatch_and_full_study_argument_guard(monkeypatch):
    from ecore_weather.jobs import cli as jobs
    calls=[]
    monkeypatch.setattr(backup,'main',lambda args:calls.append(args) or 0)
    assert jobs.main(['backup','goes','--year','2026'])==0
    assert calls==[['--year','2026']]
    monkeypatch.setattr(jobs,'run_fetch',lambda *a,**kw:calls.append(kw) or 0)
    assert jobs.main(['resume','goes','--backup-after-complete','--max-hours','0'])==0
    assert calls[-1]['backup_options']=={'work_dir':None,'remote_root':None}
    with pytest.raises(SystemExit):jobs.main(['resume','goes','--backup-after-complete','--end','2023-01-01'])


@pytest.mark.parametrize('code',[0,1,75])
def test_fetch_exit_code_gates_backup(tmp_path,monkeypatch,code):
    from ecore_weather.jobs import cli as jobs
    from ecore_weather.jobs import jobs_goes
    calls=[]
    monkeypatch.setenv('ECORE_RUN_ROOT',str(tmp_path/'runs'))
    monkeypatch.setattr(jobs_goes,'main',lambda args:code)
    monkeypatch.setattr(backup,'after_fetch',lambda *args:calls.append(args) or 0)
    monkeypatch.setattr(backup,'local_preflight',lambda *args:{})
    assert jobs.run_fetch('goes',['--output',str(tmp_path/'state'),'--scratch',str(tmp_path/'scratch')],
        index_path=tmp_path/'index.duckdb',backup_options={'work_dir':None})==code
    assert bool(calls)==(code==0)


def test_backup_exception_marks_run_failed(tmp_path,monkeypatch):
    from ecore_weather.jobs import cli as jobs
    from ecore_weather.jobs import jobs_goes
    monkeypatch.setenv('ECORE_RUN_ROOT',str(tmp_path/'runs'))
    monkeypatch.setattr(jobs_goes,'main',lambda args:0)
    def fail(*args):raise OSError('HF unavailable')
    monkeypatch.setattr(backup,'after_fetch',fail)
    monkeypatch.setattr(backup,'local_preflight',lambda *args:{})
    with pytest.raises(OSError,match='HF unavailable'):
        jobs.run_fetch('goes',['--output',str(tmp_path/'state'),'--scratch',str(tmp_path/'scratch')],
            index_path=tmp_path/'index.duckdb',backup_options={"work_dir":None})
    run=json.loads(next((tmp_path/'runs').glob('*/run.json')).read_text())
    assert run['status']=='failed' and run['exit_code']==1
