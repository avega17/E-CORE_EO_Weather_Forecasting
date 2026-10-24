"""Consolidate closed operational snapshots without changing research archives."""
from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import shutil
import tempfile
import time

from . import index

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
