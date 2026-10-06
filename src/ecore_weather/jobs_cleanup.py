"""Evidence-first cleanup; no research archives or resumable native stores deleted."""
from __future__ import annotations
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import shutil
from .runlog import compact,preserve_evidence,save


def yearly_working_copy(path,source_root,client,bucket):
    """Return durable-copy evidence or raise, leaving unresolved ZIPs intact."""
    from .dataset_report import file_hash
    local_path=path.with_name(path.name.rsplit('-',1)[0]+'-local-bundle.json')
    receipt_path=local_path.with_name(local_path.name.replace('-local-bundle','-remote-bundle'))
    local=json.loads(local_path.read_text());receipt=json.loads(receipt_path.read_text())
    if receipt.get('status') not in ('saved','reused') or not receipt.get('readback_seconds'):
        raise ValueError('No completed remote read-back receipt')
    if receipt['archive_sha256']!=local['sha256'] or path.stat().st_size!=receipt['stored_bytes'] or file_hash(path)!=local['sha256']:
        raise ValueError('Working copy differs from verified receipt')
    head=client.head_object(Bucket=bucket,Key=receipt['remote_key'])
    if head['ContentLength']!=receipt['stored_bytes']:raise ValueError('Remote backup object size changed')
    response=client.get_object(Bucket=bucket,Key=receipt['remote_key'].rsplit('/',1)[0]+'/complete.json')
    try:remote=json.loads(response['Body'].read())
    finally:response['Body'].close()
    if remote['archive_sha256']!=receipt['archive_sha256'] or remote['manifest_sha256']!=local['manifest_sha256']:
        raise ValueError('Remote backup completion identity changed')
    root=Path(source_root).resolve()
    for archive in local['archives']:
        original=(root/archive['local_path']).resolve()
        if not original.is_relative_to(root) or not original.is_file():raise ValueError('Monthly original is absent')
        marker=json.loads((original.parent/'complete.json').read_text())
        if original.stat().st_size!=archive['stored_bytes'] or marker['archive_sha256']!=archive['sha256']:
            raise ValueError('Monthly original identity changed')
    return {'receipt':str(receipt_path),'remote_key':receipt['remote_key'],'remote_etag':head.get('ETag'),
            'sha256':local['sha256'],'monthly_originals':len(local['archives'])}


def execute(root='results',source_root='/mnt/p/ecore_eo_datasets',apply=False):
    root=Path(root).resolve()
    if root.name!='results':raise ValueError('Cleanup must target the project results directory')
    evidence=preserve_evidence(root/'evidence',roots=[str(p) for p in root.iterdir() if p.is_dir() and p.name not in ('study-scratch','study-code-snapshots','benchmarks','runs','evidence')])
    deleted=[];protected=[];compressed=[]
    # Provenance inventories are compressed losslessly in place; tiny collection links remain.
    for path in sorted(root.rglob('items.json')):
        if 'study-code-snapshots' in path.parts or 'study-scratch' in path.parts:continue
        content=path.read_bytes();destination=path.with_suffix('.json.gz')
        packed=gzip.compress(content,compresslevel=6,mtime=0)
        if gzip.decompress(packed)!=content:raise IOError('STAC compression read-back differs')
        compressed.append({'path':str(path),'original_bytes':len(content),'compressed_bytes':len(packed)})
        if not apply:continue
        destination.write_bytes(packed)
        collection=path.parent/'collection.json'
        if collection.is_file():
            data=json.loads(collection.read_text())
            for link in data.get('links',[]):
                if Path(link.get('href','')).name=='items.json':link['href']='./items.json.gz'
            collection.write_text(json.dumps(data,separators=(',',':'))+'\n')
        path.unlink()
    client=bucket=None
    for path in sorted((root/'mirror-mrms-yearly').glob('*/*.zip')):
        try:
            if client is None:
                from .hf_storage import s3_client,s3_configuration
                from .storage import configured_bucket
                bucket_id=configured_bucket();client=s3_client(bucket_id);bucket=s3_configuration(bucket_id)['bucket']
            proof=yearly_working_copy(path,source_root,client,bucket)
            deleted.append({'path':str(path),'bytes':path.stat().st_size,'reason':'Verified remote annual backup and retained monthly originals','proof':proof})
            if apply:path.unlink()
        except Exception as exc:protected.append({'path':str(path),'reason':str(exc)})
    # Compact only operational reports. Selections and completion manifests are never stripped.
    for path in sorted(root.rglob('*.json')):
        if path.name in ('complete.json','collection.json','progress.json','items.json','year_manifest.json') or any(p in path.parts for p in ('study-code-snapshots','study-scratch','evidence','benchmarks')):continue
        try:
            original=path.read_bytes();data=json.loads(original)
            packed=(json.dumps(compact(data),separators=(',',':'),default=str)+'\n').encode()
            if len(packed)>=len(original):continue
            if apply:path.write_bytes(packed)
            compressed.append({'path':str(path),'original_bytes':len(original),'compressed_bytes':len(packed)})
        except (OSError,ValueError):continue
    protected.extend({'path':str(p),'reason':'Native resumable checkpoints or unresolved failed scratch'} for p in (root/'study-scratch').glob('*'))
    manifest={'applied':apply,'evidence':evidence,'deleted':deleted,'protected':protected,'compacted':compressed,
              'deleted_bytes':sum(r['bytes'] for r in deleted),'compact_savings_bytes':sum(r['original_bytes']-r['compressed_bytes'] for r in compressed)}
    if apply:
        retire_obsolete_folders(root,manifest)
        archive_failed_scratch(root,manifest)
        previous=root/'cleanup.json'
        if previous.is_file():
            old=json.loads(previous.read_text())
            for key in ('deleted','protected'):
                manifest[key]=list({r['path']:r for r in [*old.get(key,[]),*manifest[key]]}.values())
            accumulated={r['path']:r for r in old.get('compacted',[])}
            for row in manifest['compacted']:
                if row['path'] in accumulated:row['original_bytes']=accumulated[row['path']]['original_bytes']
                accumulated[row['path']]=row
            manifest['compacted']=list(accumulated.values())
        manifest['deleted_bytes']=sum(r['bytes'] for r in manifest['deleted'])
        manifest['compact_savings_bytes']=sum(r['original_bytes']-r['compressed_bytes'] for r in manifest['compacted'])
        save(root/'cleanup.json',manifest)
    return manifest


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',default='results');p.add_argument('--source-root',default='/mnt/p/ecore_eo_datasets')
    p.add_argument('--current-study-only',action='store_true',help='Remove obsolete results; retain active scratch, current studies, index and backup receipts')
    p.add_argument('--apply',action='store_true');args=p.parse_args(argv)
    result=prune_current_studies(args.root,args.source_root,args.apply) if args.current_study_only else execute(args.root,args.source_root,args.apply)
    print({k:result[k] for k in ('applied','deleted_bytes','compact_savings_bytes')});print('Protected:',len(result['protected']))
    return 0


def retire_obsolete_folders(root,manifest):
    import gzip,json,hashlib,shutil
    from .catalog import load_selection
    from .index import _writer_lock,connect
    root=Path(root);obsolete=['2022Q4-goes-3sph','2024-mrms','2025-mrms','all-2022-mrms','comparison-2023','dataset-report-sample','duckdb-hf-probe','goes','goes-roi-study','h2-2022','mirror-mrms-2021','mrms','mrms-roi','sept-2022-goes-3sph','sept-2022-goes-3sph-8band','throughput']
    preserve_evidence(roots=[str(root/n) for n in obsolete])
    mapped={};deleted=[]
    for name in obsolete:
     folder=root/name
     if not folder.is_dir():continue
     # Never remove datasets or incomplete source/array stores during report cleanup.
     protected=[p for p in folder.rglob('*') if p.is_file() and (p.suffix in ('.zip','.nc','.gz') and p.name!='items.json.gz' or p.name in ('complete.json','progress.json','zarr.json'))]
     if protected:
      manifest['protected'].append({'path':str(folder),'reason':'Dataset/test payload needs separate verified cleanup'});continue
     for collection in folder.rglob('collection.json'):
      selection=load_selection(collection)
      item=collection.with_name('items.json.gz')
      if not item.exists():raise RuntimeError(f'Selection missing: {collection}')
      raw=gzip.decompress(item.read_bytes());digest=hashlib.sha256(raw).hexdigest()
      target=root/'selections'/f'{selection.id}-{digest[:12]}'
      target.mkdir(parents=True,exist_ok=True)
      packed=target/'items.json.gz'
      if packed.exists() and gzip.decompress(packed.read_bytes())!=raw:raise RuntimeError('Canonical selection collision')
      shutil.copyfile(item,packed);shutil.copyfile(collection,target/'collection.json')
      for old in (collection,item,collection.with_name('items.json')):
       mapped[str(old)]=str((target/'collection.json').resolve());mapped[str(old.resolve())]=str((target/'collection.json').resolve())
     size=sum(p.stat().st_size for p in folder.rglob('*') if p.is_file())
     shutil.rmtree(folder);deleted.append({'path':str(folder),'bytes':size,'reason':'Compact evidence retained; compressed selections moved to canonical selection registry'})
    with _writer_lock(),connect() as db:
     for old,new in mapped.items():db.execute('UPDATE selections SET catalog_path=? WHERE catalog_path=?',[new,old])
    manifest['deleted'].extend(deleted);manifest['deleted_bytes']+=sum(r['bytes'] for r in deleted)
    return manifest


def archive_failed_scratch(root,manifest):
    """Keep exact failed legacy stores in verified TARs instead of thousands of files."""
    import tarfile
    from .jobs_policy import ensure_paused
    candidates=sorted((Path(root)/'study-scratch').glob('ecore-month-*'))
    try:ensure_paused()
    except RuntimeError:
        manifest['protected'].extend({'path':str(p),'reason':'Active fetch; defer legacy scratch packaging'} for p in candidates)
        return
    evidence=Path(root)/'evidence'/'failed-scratch';evidence.mkdir(parents=True,exist_ok=True)
    for folder in candidates:
        paths=sorted(p for p in folder.rglob('*') if p.is_file())
        if any(p.is_symlink() for p in folder.rglob('*')) or any(p.name=='complete.json' for p in paths):
            manifest['protected'].append({'path':str(folder),'reason':'Link or completion marker; needs separate review'});continue
        target=evidence/(folder.name+'.tar.gz')
        if target.exists():raise FileExistsError(f'Unresolved prior evidence archive: {target}')
        digests={};size=0
        with tarfile.open(target,'w:gz') as archive:
            for file in paths:
                relative=file.relative_to(folder).as_posix();content=file.read_bytes()
                digests[relative]=hashlib.sha256(content).hexdigest();size+=len(content)
                archive.add(file,arcname=relative,recursive=False)
        verified=set()
        with tarfile.open(target,'r:gz') as archive:
            for member in archive:
                stream=archive.extractfile(member)
                if stream is None:raise IOError('Unexpected non-file evidence member')
                with stream:actual=hashlib.sha256(stream.read()).hexdigest()
                if actual!=digests.get(member.name):raise IOError('Failed scratch evidence differs on read-back')
                verified.add(member.name)
        if verified!=set(digests):raise IOError('Failed scratch evidence is incomplete')
        evidence_hash=hashlib.sha256(target.read_bytes()).hexdigest()
        row={'path':str(folder.resolve()),'bytes':size,'replacement_bytes':target.stat().st_size,'files':len(paths),
            'reason':'Exact failed legacy scratch preserved in value-verified forensic TAR',
            'evidence_archive':str(target),'sha256':evidence_hash}
        save(evidence/(folder.name+'.json'),row)
        shutil.rmtree(folder)
        manifest['deleted'].append(row)
        manifest['protected']=[r for r in manifest['protected'] if Path(r['path']).resolve()!=folder.resolve()]


def prune_current_studies(root='results',source_root='/mnt/p/ecore_eo_datasets',apply=False):
    """Explicit user-authorized result retention; never touches research storage."""
    import os,time
    root=Path(root).resolve();research=Path(source_root).resolve()
    if root!=Path('results').resolve() or root==research or research.is_relative_to(root):
        raise ValueError('Cleanup must target results, separately from research archives')
    def load(path):
        try:return json.loads(path.read_text())
        except (OSError,ValueError):return {}
    def size(path):
        if path.is_symlink():return 0
        if path.is_file():return path.stat().st_size
        return sum(p.stat().st_size for p in path.rglob('*') if p.is_file() and not p.is_symlink())
    deleted=[];protected=[]
    def remove(path,reason):
        if not path.exists() and not path.is_symlink():return
        deleted.append({'path':str(path.relative_to(root)),'bytes':size(path),'reason':reason})
        if apply:
            if path.is_dir() and not path.is_symlink():shutil.rmtree(path)
            else:path.unlink()
    launch=load(root/'study-goes-native/launch.json')
    snapshots={Path(launch['snapshot']).name} if launch.get('snapshot') else set()
    replacement=load(root/'study-goes-native/handoff/replacement.json')
    if replacement.get('snapshot'):snapshots.add(Path(replacement['snapshot']).name)
    active_runs=set()
    for folder in (root/'runs').glob('*'):
        state=load(folder/'run.json') or load(folder/'config.json')
        if state.get('status')=='running':
            import psutil
            try:
                proc=psutil.Process(state['pid'])
                if 'dataset_jobs.py' in ' '.join(proc.cmdline()):active_runs.add(folder.name)
            except (psutil.Error,KeyError):pass
    # Backup receipts remain with the MRMS study; large annual copies are redundant
    # only if every packaged month still has a matching verified local original.
    backup=root/'mirror-mrms-yearly'
    unresolved=False;remote_client=None;remote_bucket=None
    for path in backup.glob('*/*.zip'):
        local=load(path.with_name(path.stem.rsplit('-',1)[0]+'-local-bundle.json'))
        originals=local.get('archives',[])
        valid=bool(originals) and path.stat().st_size==local.get('size')
        for archive in originals:
            original=research/archive['local_path'];marker=load(original.parent/'complete.json')
            valid=valid and original.is_relative_to(research) and original.is_file() and original.stat().st_size==archive['stored_bytes'] and marker.get('archive_sha256')==archive['sha256']
        if not valid:
            receipt=load(path.with_name(path.stem.rsplit('-',1)[0]+'-remote-bundle.json'))
            try:
                if receipt.get('status') not in ('saved','reused') or not receipt.get('readback_seconds') or receipt.get('archive_sha256')!=local.get('sha256'):
                    raise ValueError('Missing verified remote receipt')
                if remote_client is None:
                    from .hf_storage import s3_client,s3_configuration
                    from .storage import configured_bucket
                    identity=configured_bucket();remote_client=s3_client(identity);remote_bucket=s3_configuration(identity)['bucket']
                head=remote_client.head_object(Bucket=remote_bucket,Key=receipt['remote_key'])
                response=remote_client.get_object(Bucket=remote_bucket,Key=receipt['remote_key'].rsplit('/',1)[0]+'/complete.json')
                try:marker=json.loads(response['Body'].read())
                finally:response['Body'].close()
                valid=(head['ContentLength']==path.stat().st_size==receipt['stored_bytes']
                    and marker['archive_sha256']==receipt['archive_sha256']
                    and marker['manifest_sha256']==local['manifest_sha256'])
            except Exception as exc:
                protected.append({'path':str(path.relative_to(root)),'reason':str(exc)})
        if valid:remove(path,'Annual working ZIP has retained originals or verified remote identity and prior read-back receipt')
        else:protected.append({'path':str(path.relative_to(root)),'reason':'Annual copy has unresolved monthly originals'});unresolved=True
    receipts=[p for p in backup.rglob('*.json') if p.name.endswith(('-remote-bundle.json','-local-bundle.json'))]
    if apply:
        for p in receipts:
            target=root/'study-mrms/backups'/p.relative_to(backup);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,target)
    # Completed MRMS selections are redundant with canonical source manifests.
    for selection in (root/'study-mrms/months').glob('*/*-selection'):
        checkpoint=selection.with_name(selection.name.removesuffix('-selection')+'.json')
        row=load(checkpoint);locations=row.get('archives',[])
        if row.get('status')=='unavailable' or (row.get('status')=='complete' and locations and all(Path(v).is_file() and load(Path(v).parent/'complete.json').get('archive_sha256') for v in locations)):
            remove(selection,'Completed selection; verified archives, provenance and compact checkpoint retained')
    current_goes=root/'study-goes-native'
    for checkpoint in (current_goes/'checkpoints').glob('*/*.json'):
        row=load(checkpoint)
        if row.get('status') not in ('complete','unavailable'):continue
        locations=[a.get('path') for a in row.get('archives',[])]
        if row.get('status')=='complete' and not all(v and Path(v).is_file() and load(Path(v).parent/'complete.json').get('archive_sha256') for v in locations):continue
        month=checkpoint.stem[:7];folder=current_goes/checkpoint.parent.name/month
        if folder.exists():remove(folder,'Completed native month; compact checkpoint and canonical archives retained')
    if apply:
        # A recent one-month reuse test overwrote run_config; retain the full-study
        # definition separately without fabricating its historic launch time.
        configs=[load(p) for p in (root/'study-mrms/run-config-history').glob('*.json')]
        configs=[c for c in configs if c.get('end_excluded','')>='2026-07-01' and len(c.get('products',[]))==4]
        if configs:save(root/'study-mrms/study-definition.json',{'start':'2021-01-01T00:00:00Z','end_excluded':'2026-07-01T00:00:00Z','latest_full_range_configuration':configs[-1],'note':'Definition and retained completed checkpoints; run_config may describe a later small reuse check'})
    keep={'study-goes-native','study-mrms','study-scratch','study-code-snapshots','runs','archive_index.duckdb','archive_index.duckdb.wal','cleanup.json'}
    for path in root.iterdir():
        if path.name not in keep and not (path==backup and unresolved):remove(path,'Outside retained MRMS and native GOES studies')
    for folder in (root/'study-code-snapshots').glob('*'):
        if folder.name not in snapshots:remove(folder,'Superseded code snapshot; current pinned snapshot retained')
    for folder in (root/'runs').glob('*'):
        if folder.name not in active_runs:remove(folder,'Superseded operational run; typed index and study checkpoints retained')
    protected.append({'path':'study-scratch','reason':'All live four-month checkpoint stores and staged data retained'})
    # Retain scientific selections only while needed for unfinished work. Durable
    # archive metadata and verified month receipts describe finished selections.
    for study in ('study-goes-native','study-mrms'):
        folder=root/study
        for path in list(folder.rglob('*')):
            if not path.is_file() or 'backups' in path.parts or 'checkpoints' in path.parts or 'handoff' in path.parts:continue
            if path.name.endswith('.log') or any(t in path.name.lower() for t in ('smoke','guard','audit','probe','inspection','restart','live-monitor','source-integrity','reader-choice','handoff-ready','failure-history','legacy-cleanup')):
                remove(path,'Superseded test or operational detail')
        for history in ('run-config-history','failure-history'):
            if (folder/history).exists():remove(folder/history,'Superseded run history; current configuration and month receipts retained')
    manifest={'applied':apply,'created_at':time.time(),'policy':'Retain latest MRMS and active native GOES studies only',
        'deleted':deleted,'protected':protected,'deleted_bytes':sum(x['bytes'] for x in deleted),'compact_savings_bytes':0}
    if apply:save(root/'cleanup.json',manifest)
    return manifest
