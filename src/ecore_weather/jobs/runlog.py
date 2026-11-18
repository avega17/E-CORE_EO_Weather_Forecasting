"""Compact operational evidence. Scientific object provenance stays in archives."""
from __future__ import annotations
from ecore_weather.core.paths import result_path
import hashlib
import json
import os
from pathlib import Path
import socket
import threading
import time
import uuid

from ecore_weather.core.common import write_json

_PROGRESS_LOCK = threading.Lock()

OMIT = {'records', 'assets', 'features', 'hourly_matches', 'expected_times',
        'shifted_matches', 'missing_times', 'source_metadata_json', 'asset_ids'}


def compact(value):
    """Remove repeated object inventories, including those inside month results."""
    if isinstance(value, dict):
        result = {k: compact(v) for k, v in value.items() if k not in OMIT}
        for key in ('records', 'assets', 'features', 'missing_times'):
            if isinstance(value.get(key), list):
                result[key + '_count'] = len(value[key])
        return result
    if isinstance(value, (list, tuple)):
        return [compact(v) for v in value]
    return value


def save(path, value):
    from ecore_weather.core.paths import store_receipt
    value = compact(value)
    if isinstance(value, dict) and store_receipt(path, value):
        return
    write_json(path, value)




class Run:
    """One run manifest plus typed coordinator-owned database rows."""
    def __init__(self, config, output=None, index_path=None):
        self.id = uuid.uuid4().hex
        self.output = Path(output or result_path('runs'))/self.id
        self.index_path = index_path
        self.started = time.time()
        self.state = {'run_id': self.id, 'status': 'running', 'pid': os.getpid(),
            'host': socket.gethostname(), 'started_at': self.started,
            'heartbeat': self.started, 'config': compact(config)}
        self.update()

    def update(self, **values):
        from ecore_weather.jobs.index import _writer_lock, connect
        self.state.update(values, heartbeat=time.time())
        save(self.output/'run.json', self.state)
        with _writer_lock(self.index_path), connect(self.index_path) as con:
            con.execute('INSERT OR REPLACE INTO jobs VALUES (?,?,?,?,?,?,?)',
                [self.id, self.state['status'], self.state['pid'], self.state['host'],
                 self.started, self.state['heartbeat'], json.dumps(compact(self.state))])

    def finish(self, code=0, **values):
        self.update(status='complete' if code == 0 else 'paused' if code == 75 else 'failed',
            ended_at=time.time(), exit_code=code, **values)


def scratch_root():
    return Path(os.getenv('ECORE_SCRATCH', str(Path.home()/'.cache/ecore-weather/scratch')))


def task_progress(run,scratch):
    """Persist aggregate task counters; completed rows survive scratch removal."""
    from ecore_weather.jobs.index import _writer_lock, connect
    updates=[];superseded=[]
    for path in Path(scratch).glob('*/progress.json'):
        try:row=json.loads(path.read_text())
        except (OSError,ValueError):continue
        if row.get('asset_ids'):row['asset_ids_digest']=hashlib.sha256(json.dumps(sorted(row['asset_ids'])).encode()).hexdigest()
        identity=(row.get('product'),row.get('band'),row.get('asset_ids_digest'))
        if not row.get('target'):row.update(getattr(run,'_task_locations',{}).get(identity,{}))
        if row.get('target'):superseded.append(str(path.parent.resolve()))
        total=row.get('selected',len(row.get('asset_ids',[])))
        updates.append([str((Path(row['target'])/'raw.zarr.zip').resolve()) if row.get('target') else str(path.parent.resolve()),run.id,row.get('source','goes'),row.get('product'),
            row.get('month'),row.get('band'),total,row.get('next_index',0),0,
            'checkpointed',path.stat().st_mtime,json.dumps(compact(row))])
    with _writer_lock(run.index_path),connect(run.index_path) as con:
        if superseded:con.executemany('DELETE FROM tasks WHERE task_id=?',[[v] for v in superseded])
        if updates:con.executemany('INSERT OR REPLACE INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',updates)
        # Mark previously observed tasks complete only when their durable marker proves it.
        rows=con.execute("SELECT task_id,metrics_json FROM tasks WHERE run_id=? AND status!='complete'",[run.id]).fetchall()
        for task_id,payload in rows:
            data=json.loads(payload);target=data.get('target')
            if target and (Path(target)/'complete.json').is_file():
                marker=json.loads((Path(target)/'complete.json').read_text())
                ids=marker.get('asset_ids',[a.get('asset_id') for a in marker.get('assets',[])])
                if data.get('asset_ids_digest') and hashlib.sha256(json.dumps(sorted(ids)).encode()).hexdigest()!=data['asset_ids_digest']:continue
                from ecore_weather.jobs.index import _archive_row
                archive=Path(target)/marker.get('raw_path','raw.zarr.zip')
                if not archive.is_file() or archive.stat().st_size!=marker.get('stored_bytes',archive.stat().st_size):continue
                con.execute('INSERT OR REPLACE INTO archives VALUES (?,?,?,?,?,?,?,?,?,?,?)',_archive_row(archive))
                con.execute("UPDATE tasks SET completed=?,status='complete',updated_at=? WHERE task_id=?",
                    [marker.get('observations',0),time.time(),task_id])


def closed_snapshot(destination,index_path=None):
    """Publish only a closed database copy; never copy a live WAL."""
    import shutil
    from ecore_weather.jobs.index import _writer_lock, connect, database_path
    target=Path(destination);target.parent.mkdir(parents=True,exist_ok=True)
    with _writer_lock(index_path):
        with connect(index_path) as con:con.execute('CHECKPOINT')
        temporary=target.with_suffix(target.suffix+'.partial')
        shutil.copyfile(database_path(index_path),temporary);os.replace(temporary,target)


def publish_progress(source, destination, index_path=None):
    """Atomic shared snapshot; never exposes the node-local live database."""
    from ecore_weather.jobs.cli import status
    with _PROGRESS_LOCK:
        data = compact(status(index_path, coordinator=True))
        if data.get('status') == 'index_unavailable':
            return
        data['tasks'] = [task for task in data.get('tasks',[]) if task.get('source')==source]
        relevant = {task.get('run_id') for task in data['tasks']}
        relevant.add(os.getenv('ECORE_RUN_ID'))
        runs=data.get('runs',[])
        data['runs'] = [run for run in runs if run.get('run_id') in relevant] or runs[:1]
        data.update(snapshot_at=time.time(), source=source, pbs_job_id=os.getenv('PBS_JOBID'))
        memory_path=os.getenv('ECORE_GOES_MEMORY_REPORT') if source=='goes' else None
        if memory_path:
            try:data['memory']=json.loads(Path(memory_path).read_text())
            except (OSError,ValueError):pass
        write_json(destination, data)


class BoundedStream:
    """Mirror output into two rotating 4 MiB logs, retaining normal terminal output."""
    def __init__(self,original,path,handler=None):
        import logging
        from logging.handlers import RotatingFileHandler
        self.original=original
        self.handler=handler or RotatingFileHandler(path,maxBytes=4*1024**2,backupCount=1,encoding='utf-8')
        self.handler.terminator='';self.handler.setFormatter(logging.Formatter('%(message)s'))
    def write(self,text):
        import logging
        self.original.write(text)
        if text:self.handler.handle(logging.LogRecord('ecore',logging.INFO,'',0,text,(),None))
        return len(text)
    def flush(self):self.original.flush();self.handler.flush()
    def isatty(self):return False
    def fileno(self):return self.original.fileno()
    def close(self):self.handler.close()


def queue_saved_selections(run,output,destination):
    """Coordinator records band-month counts once, without per-asset SQL rows."""
    from ecore_weather.storage.catalog import load_selection
    from ecore_weather.ingest.monthly import _band, _identity
    from ecore_weather.jobs.index import _writer_lock, connect
    seen=getattr(run,'_catalogs_seen',{});locations=getattr(run,'_task_locations',{})
    updates=[]
    destination=Path(destination).resolve()
    for file in Path(output).rglob('collection.json'):
        if 'history' in file.parts:continue
        modified=file.stat().st_mtime_ns
        if seen.get(str(file))==modified:continue
        selection=load_selection(file)
        if selection.source!='goes' or selection.product!='ABI-L2-CMIPF':continue
        groups={}
        selection_id=selection.id
        for asset in selection.assets:
            groups.setdefault((_band(asset),asset.time[:7]),[]).append(asset)
        for (band,month),assets in groups.items():
            base=destination/'goes'/selection.product/f'goes{selection.satellite}'/f'C{band:02d}'
            target=str(base/f'roi-{_identity(selection,band)}'/month[:4]/month[5:7])
            digest=hashlib.sha256(json.dumps(sorted(a.id for a in assets)).encode()).hexdigest()
            metrics={'target':target,'month':month,'source':'goes','catalog':str(file),'selection_id':selection_id,'asset_ids_digest':digest}
            locations[(selection.product,band,digest)]=metrics
            updates.append([str(Path(target)/'raw.zarr.zip'),run.id,'goes',selection.product,month,band,
                len(assets),0,0,'queued',time.time(),json.dumps(metrics)])
        seen[str(file)]=modified
    if updates:
        with _writer_lock(run.index_path),connect(run.index_path) as con:
            con.executemany('INSERT OR IGNORE INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',updates)
    run._catalogs_seen=seen;run._task_locations=locations
