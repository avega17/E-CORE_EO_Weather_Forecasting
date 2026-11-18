"""Archive-level discovery and compact coordinator-owned run records.

The database is an index. Arrays, source provenance and compressed STAC remain
portable, and live writers never depend on individual source-object log rows.
"""
from __future__ import annotations
from ecore_weather.core.paths import result_path
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
from datetime import datetime, timezone
from ecore_weather.core.common import utc
from ecore_weather.jobs.runlog import compact


def database_path(path=None):
    value = str(path or os.getenv('ECORE_INDEX_PATH', str(result_path('archive_index.duckdb'))))
    if value.startswith(('hf://','/mnt/')):
        raise ValueError('The DuckDB index must be stored on a local Linux filesystem.')
    return Path(value).expanduser().resolve()


@contextlib.contextmanager
def _writer_lock(path=None):
    key = hashlib.sha256(str(database_path(path)).encode()).hexdigest()[:20]
    with (Path('/tmp')/f'ecore-duckdb-{key}.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try: yield
        finally: fcntl.flock(lock, fcntl.LOCK_UN)


def _connect_with_retry(filename, read_only, lock_timeout):
    """Wait for brief external readers; read-only callers still fall back quickly."""
    import duckdb
    import time
    deadline = time.monotonic() + lock_timeout
    while True:
        try:
            return duckdb.connect(str(filename), read_only=read_only)
        except duckdb.IOException as exc:
            locked = 'Could not set lock' in str(exc) or 'Conflicting lock' in str(exc)
            if read_only or not locked or time.monotonic() >= deadline:
                raise
            time.sleep(min(.25, max(0, deadline-time.monotonic())))


def connect(path=None, *, read_only=False, lock_timeout=30):
    import duckdb
    filename = database_path(path)
    if not read_only: filename.parent.mkdir(parents=True,exist_ok=True)
    con = _connect_with_retry(filename, read_only, lock_timeout)
    if read_only: return con
    con.execute('''CREATE TABLE IF NOT EXISTS selections (
        selection_id VARCHAR PRIMARY KEY, source VARCHAR, product VARCHAR,
        start_utc TIMESTAMP, end_utc TIMESTAMP, request_json JSON,
        catalog_path VARCHAR, indexed_at TIMESTAMP DEFAULT current_timestamp)''')
    con.execute('''CREATE TABLE IF NOT EXISTS fetch_runs (
        run_id VARCHAR PRIMARY KEY, source VARCHAR, product VARCHAR,
        selection_id VARCHAR, destination VARCHAR, started_at TIMESTAMP,
        wall_seconds DOUBLE, source_bytes UBIGINT, stored_bytes UBIGINT,
        peak_rss_bytes UBIGINT, status VARCHAR, report_json JSON)''')
    con.execute('''CREATE TABLE IF NOT EXISTS archives (
        path VARCHAR PRIMARY KEY, source VARCHAR, product VARCHAR, band INTEGER,
        start_utc TIMESTAMP, end_utc TIMESTAMP, observations UBIGINT,
        stored_bytes UBIGINT, listed_source_bytes UBIGINT, sha256 VARCHAR,
        marker_path VARCHAR)''')
    con.execute('''CREATE TABLE IF NOT EXISTS jobs (
        run_id VARCHAR PRIMARY KEY, status VARCHAR, pid BIGINT, host VARCHAR,
        started_at DOUBLE, heartbeat DOUBLE, config_json JSON)''')
    con.execute('''CREATE TABLE IF NOT EXISTS tasks (
        task_id VARCHAR PRIMARY KEY, run_id VARCHAR, source VARCHAR,
        product VARCHAR, month VARCHAR, band INTEGER, selected BIGINT,
        checkpointed BIGINT, completed BIGINT, status VARCHAR,
        updated_at DOUBLE, metrics_json JSON)''')
    con.execute('''CREATE TABLE IF NOT EXISTS benchmark_cases (
        case_id VARCHAR PRIMARY KEY, suite_id VARCHAR, scope VARCHAR,
        satellite INTEGER, repeat INTEGER, wall_seconds DOUBLE,
        returned_bytes UBIGINT, peak_rss_bytes UBIGINT, verified BOOLEAN,
        config_json JSON, metrics_json JSON)''')
    con.execute('''CREATE TABLE IF NOT EXISTS backup_receipts (
        key VARCHAR PRIMARY KEY, sha256 VARCHAR, stored_bytes UBIGINT,
        status VARCHAR, receipt_json JSON)''')
    return con


def record_selection(selection, catalog_path, path=None):
    with _writer_lock(path), connect(path) as con:
        con.execute('INSERT OR REPLACE INTO selections VALUES (?,?,?,?,?,?,?,current_timestamp)',
            [selection.id,selection.source,selection.product,
             utc(selection.start).replace(tzinfo=None),utc(selection.end).replace(tzinfo=None),
             json.dumps(compact(selection.summary())),str(catalog_path)])


def _archive_row(path):
    path=Path(path).resolve(); marker_path=path.parent/'complete.json'
    marker=json.loads(marker_path.read_text())
    assets=marker.get('assets',[])
    times=[utc(a['time']).replace(tzinfo=None) for a in assets if a.get('time')]
    objects={a.get('asset_id') or a.get('source_url'):a.get('source_bytes',0) for a in assets}
    return [str(path),marker.get('source'),marker.get('product'),marker.get('band'),
        min(times) if times else None,max(times) if times else None,
        marker.get('observations',len(assets)),path.stat().st_size,
        sum(v or 0 for v in objects.values()),marker.get('archive_sha256'),str(marker_path)]


def record_fetch(report, path=None):
    if not report:return
    selection=report.get('selection_summary',{})
    source=report.get('source',selection.get('source','unknown'))
    run_id=report.get('run_id') or os.getenv('ECORE_RUN_ID',report.get('selection_id',''))+':'+report.get('selection_id','')+':'+str(report.get('started_at',''))
    started=report.get('started_at') or datetime.now(timezone.utc).isoformat()
    with _writer_lock(path),connect(path) as con:
        for row in report.get('monthly_archives',[]):
            archive=Path(row.get('path',''))
            if archive.is_file() and (archive.parent/'complete.json').is_file():
                con.execute('INSERT OR REPLACE INTO archives VALUES (?,?,?,?,?,?,?,?,?,?,?)',_archive_row(archive))
                identity=_archive_row(archive)
                con.execute('INSERT OR REPLACE INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                    [str(archive),os.getenv('ECORE_RUN_ID',run_id),source,identity[2],
                     str(identity[4])[:7] if identity[4] else None,identity[3],identity[6],identity[6],identity[6],
                     'complete',__import__('time').time(),json.dumps(compact(row),default=str)])
        con.execute('INSERT OR REPLACE INTO fetch_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
            [run_id,source,report.get('archive_product',selection.get('product','unknown')),
             report.get('selection_id',''),report.get('root',''),utc(started).replace(tzinfo=None),
             report.get('wall_s',0),report.get('read_bytes',0),report.get('stored_bytes',0),
             report.get('peak_rss_bytes',0),'pending_sources' if report.get('pending_sources') else 'interrupted' if report.get('interrupted') else 'complete',
             json.dumps(compact(report),default=str)])

    if os.getenv('ECORE_DURABLE_STATE'):
        from ecore_weather.jobs.runlog import closed_snapshot
        closed_snapshot(Path(os.environ['ECORE_DURABLE_STATE'])/'archive_index.duckdb',path)


def search(location, source=None, start=None, end=None, band=None, path=None):
    """Locate archive candidates, then read only their portable metadata."""
    from ecore_weather.storage.discovery import observation
    if not database_path(path).is_file():return []
    clauses=['starts_with(path, ?)']; params=[str(Path(location).expanduser().resolve()).rstrip('/')+'/']
    if source:clauses.append('source=?');params.append(source)
    if start:clauses.append('(end_utc IS NULL OR end_utc>=?)');params.append(utc(start).replace(tzinfo=None))
    if end:clauses.append('(start_utc IS NULL OR start_utc<?)');params.append(utc(end).replace(tzinfo=None))
    if band is not None:clauses.append('(band=? OR band IS NULL)');params.append(int(band))
    with connect(path,read_only=True) as con:
        candidates=con.execute('SELECT marker_path FROM archives WHERE '+' AND '.join(clauses),params).fetchall()
    result=[]
    for (filename,) in candidates:
        marker=Path(filename)
        if not marker.is_file():continue
        rows=observation(str(marker.parent),json.loads(marker.read_text()))
        for row in rows if isinstance(rows,list) else [rows]:
            stamp=utc(row['time'])
            if (not start or stamp>=utc(start)) and (not end or stamp<utc(end)) and (band is None or row.get('band') in (None,band)):
                result.append(row)
    return sorted(result,key=lambda r:(r['time'],r.get('band') or 0))


def rebuild(locations, path=None):
    """Build a new compact database, preserving measured run summaries atomically."""
    import duckdb
    from ecore_weather.storage.discovery import local_archives
    filename=database_path(path); temporary=filename.with_name(filename.stem+'.rebuilding.duckdb')
    with _writer_lock(filename):
        if temporary.exists():raise FileExistsError(f'Unfinished index rebuild: {temporary}')
        try:
            with connect(temporary) as target:
                seen=set()
                for location in locations:
                    for source in ('mrms','goes'):
                        for row in local_archives(location,source,strict=True):
                            archive=Path(row['path']).resolve()
                            if str(archive) in seen:continue
                            target.execute('INSERT INTO archives VALUES (?,?,?,?,?,?,?,?,?,?,?)',_archive_row(archive))
                            seen.add(str(archive))
                if filename.is_file():
                    with duckdb.connect(str(filename),read_only=True) as old:
                        tables={r[0] for r in old.execute('SHOW TABLES').fetchall()}
                        for table in ('selections','fetch_runs','jobs','tasks','benchmark_cases','backup_receipts'):
                            if table not in tables:continue
                            cursor=old.execute('SELECT * FROM '+table)
                            columns=[c[0] for c in cursor.description]
                            while batch:=cursor.fetchmany(8):
                                for record in batch:
                                    record=list(record)
                                    for i,column in enumerate(columns):
                                        if column.endswith('_json') and record[i]:record[i]=json.dumps(compact(json.loads(record[i])),default=str)
                                    target.execute(f'INSERT OR REPLACE INTO {table} ({",".join(columns)}) VALUES ({",".join("?" for _ in record)})',record)
                target.execute("""INSERT INTO tasks
                    SELECT path,'manifest-rebuild',source,product,strftime(start_utc,'%Y-%m'),band,
                      observations,observations,observations,'complete',epoch(current_timestamp),'{}'
                    FROM archives WHERE path NOT IN (SELECT task_id FROM tasks)""")
                count=target.execute('SELECT count(*) FROM archives').fetchone()[0]
                if count!=len(seen):raise IOError('Archive inventory rebuild mismatch')
                target.execute('CHECKPOINT')
            with connect(temporary,read_only=True) as test:
                if test.execute('SELECT count(*) FROM archives').fetchone()[0]!=len(seen):raise IOError('Index read-back mismatch')
            os.replace(temporary,filename)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    return filename


def import_operational_evidence(suite_directory='results/benchmarks/goes-final', receipt_directory='results/mirror-mrms-yearly', path=None):
    """Typed compact benchmark cases and durable backup receipts; no asset rows."""
    suite=Path(suite_directory);count=receipts=0
    with _writer_lock(path),connect(path) as con:
        for filename in ('suite.json','stock.json','matched.json'):
            file=suite/filename
            if not file.is_file():continue
            data=json.loads(file.read_text())
            for row in data.get('rows',[]):
                payload=compact(row)
                identity=hashlib.sha256(json.dumps([str(suite.resolve()),filename,payload],sort_keys=True,default=str).encode()).hexdigest()
                con.execute('INSERT OR REPLACE INTO benchmark_cases VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                    [identity,str(suite.resolve()),row.get('stage',filename[:-5]),row.get('satellite'),row.get('repeat'),
                     row.get('wall_seconds',row.get('readiness_seconds')),row.get('returned_bytes'),row.get('peak_rss_bytes'),
                     row.get('values_equal',row.get('source_hashes_equal',False)),json.dumps(row.get('config') or row.get('profile',{})),json.dumps(payload,default=str)])
                count+=1
        for file in Path(receipt_directory).glob('*/*-remote-bundle.json'):
            receipt=json.loads(file.read_text())
            if receipt.get('status') not in ('saved','reused') or not receipt.get('readback_seconds'):continue
            con.execute('INSERT OR REPLACE INTO backup_receipts VALUES (?,?,?,?,?)',
                [receipt['remote_key'],receipt['archive_sha256'],receipt['stored_bytes'],receipt['status'],json.dumps(compact(receipt))])
            receipts+=1
    return {'benchmark_cases':count,'backup_receipts':receipts}



import fcntl
import json
import os
from pathlib import Path
import shutil
import tempfile
import time

from ecore_weather.jobs import index

TABLE_KEYS = {
    'selections': 'selection_id', 'fetch_runs': 'run_id', 'archives': 'path',
    'jobs': 'run_id', 'tasks': 'task_id', 'benchmark_cases': 'case_id',
    'backup_receipts': 'key',
}


def _payload(row, name):
    return json.loads(row.get(name) or '{}')


def _choose(table, old, new):
    if old == new:
        return old
    if table in ('archives', 'backup_receipts'):
        for column in ('sha256', 'stored_bytes'):
            if old.get(column) is not None and new.get(column) is not None and old[column] != new[column]:
                raise ValueError(f'Conflicting {table} evidence for {old[TABLE_KEYS[table]]}')
        return new
    if table == 'benchmark_cases':
        raise ValueError(f'Conflicting benchmark case {old["case_id"]}')
    if table == 'tasks':
        # Only marker-backed completions receive preference during reconciliation.
        return max((old, new), key=lambda r: r.get('updated_at') or 0)
    if table == 'jobs':
        return max((old, new), key=lambda r: (r.get('heartbeat') or 0, r.get('status') != 'running'))
    if table == 'selections':
        return max((old, new), key=lambda r: (Path(r.get('catalog_path') or '').is_file(),
                                              str(r.get('indexed_at') or '')))
    # fetch_runs are immutable measurements once complete, not counters to sum.
    if table == 'fetch_runs':
        if old.get('status') == 'complete' and new.get('status') == 'complete':
            raise ValueError(f'Conflicting completed fetch run {old["run_id"]}')
        return max((old, new), key=lambda r: (r.get('status') == 'complete',
                                              r.get('wall_seconds') or 0))
    raise ValueError(table)


def merge_snapshots(inputs, destination, scratch, *, outcomes=None):
    """Read closed inputs, validate a fresh DB, then atomically publish it.

    Inputs and any old destination remain untouched on failure. Destination is
    never a live writable index. The caller must stop all snapshot publishers.
    Existing destination history is included automatically. Array contents were
    checked on original publication; here marker identity and physical size are
    reconciled, without rereading every scientific array or ZIP byte.
    """
    import duckdb

    target = Path(destination).resolve()
    sources = list(dict.fromkeys(Path(p).resolve() for p in inputs))
    if target.is_file() and target not in sources:
        sources.insert(0, target)
    if not sources:
        raise ValueError('At least one closed snapshot is required')
    for source in sources:
        if not source.is_file():
            raise FileNotFoundError(f'Missing closed index snapshot: {source}')
        if Path(str(source) + '.wal').exists():
            raise ValueError(f'Snapshot has a WAL; stop its writer before merging: {source}')
    scratch = index.database_path(Path(scratch) / 'merge-work.duckdb').parent
    scratch.mkdir(parents=True, exist_ok=True)
    target.parent.mkdir(parents=True, exist_ok=True)
    with (target.parent / (target.name + '.merge.lock')).open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError('Another operational index merge is active') from exc
        with tempfile.TemporaryDirectory(prefix='ecore-index-merge-', dir=scratch) as work:
            fresh = Path(work) / 'merged.duckdb'
            counts = {}
            with index.connect(fresh) as con:
                columns = {t: [r[0] for r in con.execute(f'SELECT * FROM {t} LIMIT 0').description]
                           for t in TABLE_KEYS}
                for number, source in enumerate(sources):
                    alias = f'snapshot_{number}'
                    quoted = str(source).replace("'", "''")
                    con.execute(f"ATTACH '{quoted}' AS {alias} (READ_ONLY)")
                    tables = {r[0] for r in con.execute(
                        'SELECT table_name FROM information_schema.tables WHERE table_catalog=?', [alias]).fetchall()}
                    for table, key in TABLE_KEYS.items():
                        if table not in tables:
                            continue
                        reader = con.cursor()
                        reader.execute(f'SELECT * FROM {alias}.main.{table}')
                        fields = [r[0] for r in reader.description]
                        if set(fields) != set(columns[table]):
                            raise ValueError(f'Unsupported {table} snapshot schema: {source}')
                        while batch := reader.fetchmany(128):
                            for record in batch:
                                row = dict(zip(fields, record))
                                if table == 'archives':
                                    for name in ('path', 'marker_path'):
                                        row[name] = str(Path(row[name]).resolve())
                                elif table == 'tasks' and str(row['task_id']).startswith('/'):
                                    row['task_id'] = str(Path(row['task_id']).resolve())
                                previous = con.execute(f'SELECT * FROM {table} WHERE {key}=?', [row[key]]).fetchone()
                                if previous:
                                    row = _choose(table, dict(zip(columns[table], previous)), row)
                                values = [row[name] for name in columns[table]]
                                con.execute(f'INSERT OR REPLACE INTO {table} VALUES ({",".join("?" for _ in values)})', values)
                        reader.close()
                    con.execute(f'DETACH {alias}')
                # Confirm canonical archive identity. No raw data are rewritten.
                archives = con.execute('SELECT * FROM archives').fetchall()
                verified = {}
                for values in archives:
                    row = dict(zip(columns['archives'], values))
                    path = Path(row['path'])
                    marker = Path(row['marker_path'])
                    if not path.is_file() or not marker.is_file():
                        raise FileNotFoundError(f'Archive or completion marker missing: {path}')
                    fact = json.loads(marker.read_text())
                    if (not fact.get('archive_sha256') or fact['archive_sha256'] != row['sha256']
                            or path.stat().st_size != row['stored_bytes']
                            or fact.get('stored_bytes', row['stored_bytes']) != row['stored_bytes']):
                        raise ValueError(f'Archive completion evidence conflicts: {path}')
                    verified[row['path']] = row
                # A verified archive always overrides older partial/failure task state.
                for path, row in verified.items():
                    con.execute('''UPDATE tasks SET status='complete', completed=?, checkpointed=?,
                        selected=greatest(coalesce(selected,0),?), metrics_json=metrics_json WHERE task_id=?''',
                        [row['observations'], row['observations'], row['observations'], path])
                    if not con.execute('SELECT 1 FROM tasks WHERE task_id=?', [path]).fetchone():
                        con.execute('INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                            [path, 'manifest-merge', row['source'], row['product'],
                             str(row['start_utc'])[:7] if row['start_utc'] else None, row['band'],
                             row['observations'], row['observations'], row['observations'], 'complete', time.time(), '{}'])
                con.execute("UPDATE tasks SET status='unknown' WHERE status='complete' AND task_id NOT IN (SELECT path FROM archives)")
                con.execute("UPDATE tasks SET status='interrupted' WHERE status IN ('running','fetching','queued','checkpointed','finalizing') AND task_id NOT IN (SELECT path FROM archives)")
                for run_id, in con.execute("SELECT run_id FROM jobs WHERE status='running'").fetchall():
                    state = (outcomes or {}).get(run_id, 'interrupted')
                    con.execute('UPDATE jobs SET status=? WHERE run_id=?', [state, run_id])
                for table in TABLE_KEYS:
                    counts[table] = con.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
                con.execute('CHECKPOINT')
            with duckdb.connect(str(fresh), read_only=True) as check:
                for table, expected in counts.items():
                    if check.execute(f'SELECT count(*) FROM {table}').fetchone()[0] != expected:
                        raise IOError(f'Merged {table} read-back mismatch')
            staging = target.with_name(target.name + '.publishing')
            try:
                shutil.copyfile(fresh, staging)
                with duckdb.connect(str(staging), read_only=True) as check:
                    for table, expected in counts.items():
                        if check.execute(f'SELECT count(*) FROM {table}').fetchone()[0] != expected:
                            raise IOError(f'Published {table} read-back mismatch')
                os.replace(staging, target)
            finally:
                staging.unlink(missing_ok=True)
    return {'status': 'complete', 'destination': str(target), 'inputs': [str(p) for p in sources],
            'rows': counts, 'archive_bytes': sum(row['stored_bytes'] for row in verified.values())}
