"""Evidence-first cleanup; no research archives or resumable native stores deleted."""
from __future__ import annotations
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import shutil
from ecore_weather.jobs.runlog import compact, save


def yearly_working_copy(path,source_root,client,bucket):
    """Return durable-copy evidence or raise, leaving unresolved ZIPs intact."""
    from ecore_weather.reporting.dataset_report import file_hash
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
    if apply:
        from ecore_weather.jobs.jobs_policy import ensure_paused
        ensure_paused()
    if root.name!='results':raise ValueError('Cleanup must target the project results directory')
    evidence = {'status': 'not copied; durable measurements remain in the index'}
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
        if not apply:
            protected.append({'path':str(path),'reason':'Remote verification is deferred; preview never accesses credentials or the network'})
            continue
        try:
            if client is None:
                from ecore_weather.storage.hf_storage import s3_client, s3_configuration
                from ecore_weather.storage.archives import configured_bucket
                bucket_id=configured_bucket();client=s3_client(bucket_id);bucket=s3_configuration(bucket_id)['bucket']
            proof=yearly_working_copy(path,source_root,client,bucket)
            deleted.append({'path':str(path),'bytes':path.stat().st_size,'reason':'Verified remote annual backup and retained monthly originals','proof':proof})
            if apply:path.unlink()
        except Exception as exc:protected.append({'path':str(path),'reason':str(exc)})
    # Compact only operational reports. Selections and completion manifests are never stripped.
    for path in sorted(root.rglob('*.json')):
        if path.name != 'run.json' or path.name in ('complete.json','collection.json','progress.json','items.json','year_manifest.json') or any(p in path.parts for p in ('study-code-snapshots','study-scratch','evidence','benchmarks')):continue
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
        manifest['deleted_bytes']=sum(r['bytes'] for r in manifest['deleted'])
        manifest['compact_savings_bytes']=sum(r['original_bytes']-r['compressed_bytes'] for r in manifest['compacted'])
        save(root/'cleanup.json',manifest)
    return manifest


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',default='results');p.add_argument('--source-root',default='/mnt/p/ecore_eo_datasets')
    p.add_argument('--current-study-only',action='store_true',help='Remove obsolete results; retain active scratch, current studies, index and backup receipts')
    p.add_argument('--consolidate-receipts',action='store_true',help='Move completed task receipts into one portable journal per study')
    p.add_argument('--apply',action='store_true');args=p.parse_args(argv)
    if args.consolidate_receipts:
        print(consolidate_receipts(args.root,args.apply));return 0
    result=prune_current_studies(args.root,args.source_root,args.apply) if args.current_study_only else execute(args.root,args.source_root,args.apply)
    print({k:result[k] for k in ('applied','deleted_bytes','compact_savings_bytes')});print('Protected:',len(result['protected']))
    return 0






def prune_current_studies(root='results',source_root='/mnt/p/ecore_eo_datasets',apply=False):
    """Explicit user-authorized result retention; never touches research storage."""
    import os,time
    root=Path(root).resolve();research=Path(source_root).resolve()
    if apply:
        from ecore_weather.jobs.jobs_policy import ensure_paused
        ensure_paused()
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
    active_runs=set();retained_runs={};latest_runs={}
    for folder in (root/'runs').glob('*'):
        state=load(folder/'run.json') or load(folder/'config.json')
        source=state.get('config',{}).get('source',state.get('source'))
        if not source or state.get('status') in ('failed','paused','interrupted','unknown'):
            # Unresolved failures and unclassified owners need an explicit decision.
            retained_runs[folder.name]='Unresolved or unclassified run'
        if source:
            stamp=state.get('started_at',0)
            if source not in latest_runs or stamp>latest_runs[source][0]:
                latest_runs[source]=(stamp,folder.name)
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
        if not valid and not apply:
            protected.append({'path':str(path.relative_to(root)),'reason':'Remote verification deferred; preview performs no network requests'})
            unresolved=True
            continue
        if not valid:
            receipt=load(path.with_name(path.stem.rsplit('-',1)[0]+'-remote-bundle.json'))
            try:
                if receipt.get('status') not in ('saved','reused') or not receipt.get('readback_seconds') or receipt.get('archive_sha256')!=local.get('sha256'):
                    raise ValueError('Missing verified remote receipt')
                if remote_client is None:
                    from ecore_weather.storage.hf_storage import s3_client, s3_configuration
                    from ecore_weather.storage.archives import configured_bucket
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
        if path.name not in keep and not (path==backup and unresolved):protected.append({'path':str(path),'reason':'Unclassified output; retained until its owner establishes completion'})
    for folder in (root/'study-code-snapshots').glob('*'):
        if folder.name not in snapshots:remove(folder,'Superseded code snapshot; current pinned snapshot retained')
    for folder in (root/'runs').glob('*'):
        if folder.name in retained_runs or folder.name in {v[1] for v in latest_runs.values()}:
            protected.append({'path':str(folder.relative_to(root)),'reason':retained_runs.get(folder.name,'Latest source run')})
        elif folder.name not in active_runs:
            remove(folder,'Superseded completed run; typed index and study checkpoints retained')
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


def consolidate_receipts(root='results', apply=False):
    """Consolidate only completed receipts, preserving pending tasks and arrays."""
    from ecore_weather.core.paths import ReceiptPath, store_receipt
    if apply:
        from ecore_weather.jobs.jobs_policy import ensure_paused
        ensure_paused()
    count=0
    for study in Path(root).glob('study-*'):
        for folder in ('months','checkpoints'):
            for path in sorted((study/folder).rglob('*.json')):
                row=json.loads(path.read_text())
                if row.get('status') not in ('complete','unavailable'):
                    continue
                count+=1
                if apply:
                    store_receipt(path,row)
                    if json.loads(ReceiptPath(path).read_text())!=row:
                        raise IOError('Portable task receipt changed during consolidation')
    return {'applied':apply,'completed_receipts':count}
