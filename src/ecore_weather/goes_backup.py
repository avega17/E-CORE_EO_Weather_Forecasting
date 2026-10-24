"""Annual native GOES backups, after verified completion of the local study.

Monthly compressed ZIPs remain untouched. Operational receipts contain aggregate
counts; source provenance stays in the canonical monthly completion records.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import time

from .common import PR_BBOX, utc, write_json
from .jobs_backup import _build_bundle, _upload_one, _year_months, _sha256

PRODUCT = 'ABI-L2-CMIPF'
BANDS = (1, 2, 3, 7, 8, 9, 10, 13)
START, END = '2021-01-01', '2026-07-01'
YEARS = tuple(range(2021, 2027))
REGION = 'roi-70.24W-14.36N-62.56W-22.04N'


def _option(arguments, key, default=None):
    return arguments[arguments.index(key)+1] if key in arguments else default


def _save(path, value):
    """Readers see the old receipt or the new receipt, never a partial JSON."""
    path=Path(path)
    pending=path.with_name(path.name+'.tmp')
    write_json(pending,value)
    with pending.open('rb') as stream:os.fsync(stream.fileno())
    os.replace(pending,path)


def validate_fetch_arguments(arguments):
    """Do not attach the full-study backup to a clipped or historical fetch."""
    if (os.getenv('PBS_JOBID') or _option(arguments, '--phase', 'all') != 'all' or '--stop-after-month' in arguments
            or _option(arguments, '--product', PRODUCT) != PRODUCT
            or '--reuse-cmipf-2km-from' in arguments
            or utc(_option(arguments, '--start', START)) != utc(START)
            or utc(_option(arguments, '--end', END)) != utc(END)):
        raise ValueError('--backup-after-complete requires the full 2021-01-01 to 2026-07-01 native CMIPF study, without a stop-after cutoff')


def local_preflight(arguments):
    """Capacity estimates use observed native archives, never NOAA file sizes."""
    import subprocess
    import psutil
    from .goes_shared import default_config
    destination=Path(_option(arguments,'--destination','/mnt/p/ecore_eo_datasets')).resolve()
    scratch=Path(_option(arguments,'--scratch')).resolve()
    scratch.mkdir(parents=True,exist_ok=True)
    filesystem=subprocess.check_output(['findmnt','-n','-o','FSTYPE','-T',str(scratch)],text=True).strip()
    if filesystem in ('tmpfs','ramfs','9p','drvfs'):
        raise OSError('GOES scratch must be persistent Linux disk storage, not RAM or the Windows DAS')
    defaults=default_config()
    profile=_option(arguments,'--shared-profile')
    config=json.loads(Path(profile).read_text())['global_config'] if profile else defaults.manifest()
    staging=int(_option(arguments,'--staging-mib',config['staging_mib']))*1024**2
    owners=int(_option(arguments,'--month-writers',config['month_writers']))+int(_option(arguments,'--tail-months',config['tail_months']))
    months={};seen=set()
    for marker_path in (destination/'goes'/PRODUCT).glob('goes*/C*/roi-*/*/*/complete.json'):
        marker=json.loads(marker_path.read_text())
        if tuple(marker.get('region',[]))!=PR_BBOX:continue
        archive=marker_path.parent/marker.get('raw_path','raw.zarr.zip')
        if not archive.is_file() or archive.stat().st_size!=marker.get('stored_bytes'):
            raise OSError(f'Existing native GOES archive does not match its completion record: {archive}')
        if str(archive) in seen:continue
        seen.add(str(archive))
        stamp=f'{marker_path.parent.parent.name}-{marker_path.parent.name}'
        row=months.setdefault(stamp,{'bytes':0,'bands':set()})
        row['bytes']+=archive.stat().st_size;row['bands'].add(marker.get('band'))
    complete=[r['bytes'] for m,r in months.items() if set(BANDS)<=r['bands'] and '2021-01'<=m<='2026-06']
    largest=max(complete,default=16*1024**3)
    remaining=max(0,66-len(complete))
    destination_required=int(remaining*largest*1.25)+5*1024**3
    scratch_required=staging+int(2*owners*largest*1.25)
    if shutil.disk_usage(destination).free<destination_required:
        raise OSError(f'DAS free space is below projected remaining GOES output plus margin: {destination_required} bytes')
    if shutil.disk_usage(scratch).free<scratch_required:
        raise OSError(f'Linux scratch free space is below staging plus store/packing reservation: {scratch_required} bytes')
    for process in psutil.process_iter(['pid','cmdline']):
        if process.pid==os.getpid():continue
        args=process.info['cmdline'] or []
        if '--dry-run' not in args and any(Path(a).name=='dataset_jobs.py' for a in args) and any(
                args[i:i+2] in (['fetch','goes'],['resume','goes']) for i in range(len(args)-1)):
            raise OSError(f'Another local GOES coordinator is present (PID {process.pid}); do not share stores')
    return {'completed_native_months':len(complete),'native_zip_bytes':sum(r['bytes'] for r in months.values()),
        'projected_remaining_bytes_with_margin':destination_required,'scratch_required_bytes':scratch_required,
        'scratch_filesystem':filesystem,'estimate_basis':'largest observed complete native month; 25% margin; not an HF forecast'}


def _month_checkpoint(study, year, month):
    stage = f'h{1 if month <= 6 else 2}-{year}'
    paths = sorted((Path(study)/'checkpoints'/stage).glob(f'{year}-{month:02d}-01*.json'))
    if len(paths) != 1:
        raise ValueError(f'Expected one completed GOES checkpoint for {year}-{month:02d}; found {len(paths)}')
    return paths[0]


def collect_year(year, source_root, study_output):
    """Validate complete monthly coverage and deduplicate physical native ZIPs.

The copy into the annual bundle checks every monthly SHA-256 again. This pass
checks coverage and file identities without loading arrays or source inventories.
"""
    root = Path(source_root).resolve()
    entries, coverage = {}, []
    for month in _year_months(year):
        path = _month_checkpoint(study_output, year, month)
        saved = json.loads(path.read_text())
        begin = utc(f'{year}-{month:02d}-01')
        stop = utc(f'{year+1}-01-01' if month == 12 else f'{year}-{month+1:02d}-01')
        requested = set(saved.get('bands', saved.get('available_bands', []) + saved.get('missing_bands', [])))
        if (saved.get('product') not in (None, PRODUCT) or requested != set(BANDS)
                or utc(saved.get('start', '1900-01-01')) != begin
                or utc(saved.get('end_excluded', '1900-01-01')) != stop
                or saved.get('status') not in ('complete', 'unavailable')):
            raise ValueError(f'Incomplete or incompatible GOES month: {path}')
        evidence = saved.get('coverage', [])
        if saved['status'] == 'unavailable':
            if (saved.get('product') != PRODUCT or saved.get('selected_assets') != 0
                    or saved.get('archives') or not evidence
                    or any(row.get('files') != 0 for row in evidence)):
                raise ValueError(f'Unavailable GOES month lacks explicit empty-selection evidence: {path}')
        month_bands, observations, source_issues = set(), 0, {}
        for item in saved.get('archives', []):
            archive = Path(item['path']).resolve()
            if not archive.is_relative_to(root) or not archive.is_file():
                raise FileNotFoundError(f'Missing monthly archive or path outside DAS: {archive}')
            marker_path = archive.parent/'complete.json'
            marker = json.loads(marker_path.read_text())
            band = marker.get('band')
            assets, ids = marker.get('assets', []), marker.get('asset_ids', [])
            if (marker.get('source') != 'goes' or marker.get('product') != PRODUCT
                    or tuple(marker.get('region', [])) != PR_BBOX or band not in BANDS
                    or marker.get('satellite') not in (16, 19)
                    or marker.get('raw_path') != archive.name
                    or marker.get('stored_bytes') != archive.stat().st_size
                    or not marker.get('archive_sha256')
                    or marker.get('observations') != len(ids) or len(set(ids)) != len(ids)
                    or {a.get('asset_id') for a in assets} != set(ids)
                    or any(f'/{PRODUCT}/' not in a.get('source_url', '') or
                           not begin <= utc(a['time']) < stop for a in assets)):
                raise ValueError(f'Native CMIPF archive identity/coverage differs: {marker_path}')
            relative = archive.relative_to(root).as_posix()
            if relative in entries:
                raise ValueError(f'Duplicate physical GOES archive in monthly checkpoints: {archive}')
            entries[relative] = {'archive': archive, 'marker_path': marker_path,
                'marker_sha256':hashlib.sha256(marker_path.read_bytes()).hexdigest(),
                'marker': marker, 'member': f'data/{relative}',
                'marker_member': f'metadata/{archive.parent.relative_to(root).as_posix()}/complete.json',
                'selected_by': [f'{year}-{month:02d}']}
            month_bands.add(band)
            observations += marker['observations']
            for issue in marker.get('source_issues', []):
                proof=issue.get('integrity_evidence',{})
                proof_etag=proof.get('etag') if proof.get('verification') == 'full-object-size-and-multipart-etag-md5' else proof.get('md5')
                if (issue.get('status') != 'confirmed_corrupt_source'
                        or issue.get('asset_id') in ids or issue.get('asset_id') in source_issues
                        or issue.get('product') != PRODUCT or issue.get('region') != list(PR_BBOX)
                        or f'/{PRODUCT}/' not in issue.get('source_url','')
                        or not begin <= utc(issue.get('time','1900-01-01')) < stop
                        or proof.get('download_bytes') != issue.get('source_bytes')
                        or not issue.get('etag') or proof_etag != issue['etag'].strip('"').lower()
                        or proof.get('verification') not in ('full-object-size-and-single-part-etag-md5',
                                                            'full-object-size-and-multipart-etag-md5')):
                    raise ValueError(f'Invalid GOES source-gap evidence: {marker_path}')
                source_issues[issue['asset_id']]=issue
        missing = set(saved.get('missing_bands', []))
        if month_bands & missing or month_bands | missing != set(BANDS):
            raise ValueError(f'GOES month does not account for all eight bands: {path}')
        if (observations+len(source_issues) != saved.get('selected_assets')
                or source_issues != {issue['asset_id']:issue for issue in saved.get('source_issues',[])}):
            raise ValueError(f'Selected GOES observation count differs from archived observations: {path}')
        coverage.append({'month': f'{year}-{month:02d}', 'status': saved['status'],
            'start': begin.isoformat(), 'end_excluded': stop.isoformat(),
            'selected_observations': observations, 'available_bands': sorted(month_bands),
            'missing_bands': sorted(missing), 'source_coverage': evidence,
            'unavailable_observations':len(source_issues)})
    identity = {'source': 'goes', 'product': PRODUCT, 'region': PR_BBOX, 'bands': BANDS,
                'year': year, 'coverage': coverage,
                'archives': [(p, e['marker']['archive_sha256'], e['marker']['stored_bytes'],e['marker_sha256'])
                             for p, e in sorted(entries.items())]}
    digest = hashlib.sha256(json.dumps(identity,sort_keys=True).encode()).hexdigest()
    return entries, coverage, digest


def validate_study(source_root, study_output):
    """Require all 66 month checkpoints before any annual upload is admitted."""
    years = []
    for year in YEARS:
        entries, coverage, digest = collect_year(year, source_root, study_output)
        years.append({'year': year, 'archive_count': len(entries),
                      'stored_bytes': sum(e['marker']['stored_bytes'] for e in entries.values()),
                      'selection_sha256': digest})
    return years


@contextmanager
def _backup_owner(study_output):
    directory=Path(study_output)/'backups';directory.mkdir(parents=True,exist_ok=True)
    with (directory/'.owner.lock').open('a') as stream:
        try:fcntl.flock(stream,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError as exc:raise OSError('Another GOES annual publisher owns this study') from exc
        try:yield
        finally:fcntl.flock(stream,fcntl.LOCK_UN)


def backup_study(source_root, study_output, work_dir=None, remote_root=None,
                 year=None, pause_path=None, update=None, dry_run=False):
    if dry_run:return _backup_study(source_root,study_output,work_dir,remote_root,year,pause_path,update,True)
    with _backup_owner(study_output):
        return _backup_study(source_root,study_output,work_dir,remote_root,year,pause_path,update)


def _backup_study(source_root, study_output, work_dir=None, remote_root=None,
                 year=None, pause_path=None, update=None, dry_run=False):
    """One annual work file and one publisher; failures preserve retry evidence."""
    from .storage import configured_bucket
    output = Path(study_output)/'backups'
    work = Path(work_dir or Path.home()/'.cache/ecore-weather/goes-yearly')
    # Check all years before resolving credentials or contacting HF.
    plan = validate_study(source_root, study_output)
    if dry_run:
        print(json.dumps({'status': 'planned', 'years': plan, 'work_dir': str(work),
                          'remote_root': remote_root or 'configured HF bucket/yearly-v1'},indent=2))
        return 0
    remote_root = remote_root or f'hf://buckets/{configured_bucket()}/noaa-subsets/yearly-v1'
    output.mkdir(parents=True,exist_ok=True)
    work.mkdir(parents=True,exist_ok=True)
    started = time.time()
    def progress(**values):
        if pause_path and Path(pause_path).exists():
            from .goes_limits import IntentionalPause
            raise IntentionalPause('pause requested during annual GOES backup')
        row = {'source': 'goes', 'started_at': started, 'updated_at': time.time(), **values}
        _save(output/'progress.json',row)
        if update:update(phase=row.get('phase','backup'),backup=row)
    try:
        for selected in plan:
            y = selected['year']
            if year is not None and year != y:continue
            entries, coverage, digest = collect_year(y, source_root, study_output)
            if not entries:
                _save(output/f'{y}.json', {'year': y, 'status': 'unavailable', 'coverage': coverage})
                continue
            receipt_path = output/f'{y}.json'
            prior = json.loads(receipt_path.read_text()) if receipt_path.exists() else {}
            bundle = prior.get('local_bundle', {})
            matching = bundle.get('selection_sha256') == digest
            # A verified receipt can be rechecked remotely without rebuilding its ZIP.
            reusable_remote = matching and prior.get('status') == 'complete' and prior.get('remote_root')==remote_root
            reusable_local = (matching and Path(bundle.get('path', '')).is_file()
                              and _sha256(bundle['path']) == bundle.get('sha256'))
            if not (reusable_remote or reusable_local):
                required = sum(e['marker']['stored_bytes'] + e['marker_path'].stat().st_size for e in entries.values())
                if shutil.disk_usage(work).free < required*1.1 + 1024**3:
                    raise OSError(f'Insufficient Linux disk space for one {y} annual GOES bundle ({required} bytes plus margin)')
                progress(phase='backup-pack',year=y,total_bytes=required)
                bundle = _build_bundle(y,PRODUCT,entries,[],work,source_root,source='goes',
                    region_id=REGION,coverage=coverage,selection_sha256=digest)
                # Avoid duplicating per-source provenance in operational receipts.
                bundle.pop('archives',None)
                _save(receipt_path,{'status':'packed','year':y,'local_bundle':bundle})
            progress(phase='backup-verify-remote' if reusable_remote else 'backup-upload',year=y)
            result = _upload_one(bundle,remote_root,progress=progress)
            _save(receipt_path,{'status':'complete','year':y,'remote_root':remote_root,
                                    'local_bundle':bundle,'remote':result})
            local = Path(bundle['path'])
            # Only remove our annual work file; never remove a monthly research ZIP.
            if local.parent.resolve() == work.resolve() and local.name==f'{PRODUCT}-{y}.zip':
                local.unlink(missing_ok=True)
            print(f'GOES {y} backup verified: {result["stored_bytes"]} bytes; annual work ZIP removed',flush=True)
        progress(phase='backup-complete',status='complete')
        return 0
    except Exception as exc:
        from .goes_limits import IntentionalPause
        paused = isinstance(exc,IntentionalPause)
        _save(output/'progress.json',{'status':'paused' if paused else 'failed',
            'phase':'backup','updated_at':time.time(),'reason':str(exc)})
        if paused:return 75
        raise


def after_fetch(arguments, options, pause, update):
    validate_fetch_arguments(arguments)
    study = _option(arguments,'--output','results/study-goes-native')
    summary = json.loads((Path(study)/'summary.json').read_text())
    if summary.get('status') != 'complete':
        raise ValueError('GOES fetch did not publish a complete study summary; backups were not started')
    return backup_study(_option(arguments,'--destination','/mnt/p/ecore_eo_datasets'),study,
        options.get('work_dir'),options.get('remote_root'),pause_path=pause,update=update)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--year',type=int,choices=YEARS,help='Retry one year; full-study completion is still required')
    parser.add_argument('--source-root',default='/mnt/p/ecore_eo_datasets')
    parser.add_argument('--study-output',default='results/study-goes-native')
    parser.add_argument('--work-dir',help='Linux disk for one temporary annual bundle')
    parser.add_argument('--remote-root',help='HF bucket yearly-backup prefix')
    parser.add_argument('--dry-run',action='store_true')
    args=parser.parse_args(argv)
    return backup_study(args.source_root,args.study_output,args.work_dir,args.remote_root,
                        args.year,dry_run=args.dry_run)
