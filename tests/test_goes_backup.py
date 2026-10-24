"""Annual backup gates and recovery use tiny files, never live NOAA/HF writes."""
from contextlib import nullcontext
import hashlib
import io
import json
from pathlib import Path
import zipfile

import pytest

from ecore_weather import goes_backup as backup, jobs_backup
from ecore_weather.common import PR_BBOX, write_json


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
    monkeypatch.setattr('ecore_weather.storage.configured_bucket',lambda:pytest.fail('HF must not be contacted'))
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
    from ecore_weather import jobs
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
    from ecore_weather import jobs,jobs_goes
    calls=[]
    monkeypatch.setenv('ECORE_RUN_ROOT',str(tmp_path/'runs'))
    monkeypatch.setattr(jobs_goes,'main',lambda args:code)
    monkeypatch.setattr(backup,'after_fetch',lambda *args:calls.append(args) or 0)
    monkeypatch.setattr(backup,'local_preflight',lambda *args:{})
    assert jobs.run_fetch('goes',['--output',str(tmp_path/'state'),'--scratch',str(tmp_path/'scratch')],
        index_path=tmp_path/'index.duckdb',backup_options={'work_dir':None})==code
    assert bool(calls)==(code==0)


def test_backup_exception_marks_run_failed(tmp_path,monkeypatch):
    from ecore_weather import jobs,jobs_goes
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
