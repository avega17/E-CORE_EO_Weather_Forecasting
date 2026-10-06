"""Archive-level discovery and compact coordinator-owned run records.

The database is an index. Arrays, source provenance and compressed STAC remain
portable, and live writers never depend on individual source-object log rows.
"""
from __future__ import annotations
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
from datetime import datetime, timezone
from .common import utc
from .runlog import compact


def database_path(path=None):
    value = str(path or os.getenv('ECORE_INDEX_PATH','results/archive_index.duckdb'))
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
             report.get('peak_rss_bytes',0),'interrupted' if report.get('interrupted') else 'complete',
             json.dumps(compact(report),default=str)])

    if os.getenv('ECORE_DURABLE_STATE'):
        from .runlog import closed_snapshot
        closed_snapshot(Path(os.environ['ECORE_DURABLE_STATE'])/'archive_index.duckdb',path)


def search(location, source=None, start=None, end=None, band=None, path=None):
    """Locate archive candidates, then read only their portable metadata."""
    from .view_index import observation
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
    from .view_storage import local_archives
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
