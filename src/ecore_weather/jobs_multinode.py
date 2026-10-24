"""Independent Polaris GOES workers and consolidation of their closed indices.

Run with ``python -m ecore_weather.jobs_multinode --help``. This orchestration
does not change native readers, archive layout, or workstation fetch settings.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import fcntl
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid

from .pbs import interval


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.partial')
    temporary.write_text(json.dumps(value, indent=2, default=str) + '\n')
    os.replace(temporary, path)


@dataclass(frozen=True)
class Partition:
    label: str
    start: str
    end: str
    pilot_start: str
    pilot_end: str


def partitions(path):
    """One owner per physical month, with stable labels across PBS allocations."""
    data = json.loads(Path(path).read_text())
    if (data.get('schema') != 1 or data.get('product') != 'ABI-L2-CMIPF'
            or data.get('bands') != [1, 2, 3, 7, 8, 9, 10, 13]
            or data.get('bbox') != [-70.24, 14.36, -62.56, 22.04]):
        raise ValueError('Partitions must use the native CMIPF study product, bands and ROI')
    result = []
    for row in data['partitions']:
        label = row['label']
        if not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,39}', label):
            raise ValueError('Partition labels need simple lowercase letters, numbers, hyphens or underscores')
        begin, end = interval(row['start'], row['end'])
        if any((d.day, d.hour, d.minute, d.second, d.microsecond) != (1, 0, 0, 0, 0) for d in (begin, end)):
            raise ValueError('Study partitions must start/end on UTC month boundaries')
        if begin.year < 2021 or end > datetime(2026, 7, 1, tzinfo=timezone.utc):
            raise ValueError('Partition outside the supported study period')
        a, b = interval(row['pilot_start'], row['pilot_end'])
        if (b-a).total_seconds() > 86400:
            raise ValueError('Each pilot window is limited to one day')
        result.append(Partition(**{k: row[k] for k in Partition.__dataclass_fields__}))
    result.sort(key=lambda p: interval(p.start, p.end)[0])
    if len(result) != 3 or len({p.label for p in result}) != 3:
        raise ValueError('This launcher requires three uniquely labelled partitions')
    for previous, following in zip(result, result[1:]):
        if interval(previous.start, previous.end)[1] != interval(following.start, following.end)[0]:
            raise ValueError('Partitions must be contiguous, with no overlapping physical months or gaps')
    return result


def hosts(path):
    # PBS may repeat hostnames for slots. mpiexec gets exactly one unique node.
    result = list(dict.fromkeys(line.strip() for line in Path(path).read_text().splitlines() if line.strip()))
    if len(result) != 3 or any(not re.fullmatch(r'[a-zA-Z0-9_.-]+', h) for h in result):
        raise ValueError('PBS_NODEFILE must contain exactly three distinct valid node names')
    return result


@contextmanager
def ownership(state):
    """Exclude overlapping ensemble launches using this shared state root."""
    state = Path(state)
    state.mkdir(parents=True, exist_ok=True)
    with (state / 'ensemble.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError('Another multi-node launcher owns this GOES state root') from exc
        yield


def prepare_worker(state, legacy, start, end, *, issue_file=None):
    """Reuse portable selections; optionally attach proven source-gap evidence."""
    begin, stop = interval(start, end)
    target = Path(state) / 'fetch'
    original = Path(legacy) / 'fetch'
    copied = 0
    current = begin.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while current < stop:
        stage = f'h{1 if current.month <= 6 else 2}-{current.year}'
        relative = Path(stage) / current.strftime('%Y-%m') / 'ABI-L2-CMIPF'
        source_dir, destination = original / relative, target / relative
        # Only selections, not another coordinator's mutable summaries/DB files.
        if source_dir.is_dir() and not (destination / 'collection.json').exists():
            for name in ('items.json.gz', 'items.json', 'collection.json'):
                source = source_dir / name
                if source.is_file():
                    destination.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(source, destination / name)
                    copied += 1
        current = current.replace(year=current.year+1, month=1) if current.month == 12 else current.replace(month=current.month+1)
    imported = unmatched = 0
    if issue_file:
        from . import catalog, goes
        facts = json.loads(Path(issue_file).read_text())['source_issues']
        for fact in facts:
            observed, _ = interval(fact['time'], '2026-07-01')
            if not begin <= observed < stop:
                continue
            month = observed.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            following = month.replace(year=month.year+1, month=1) if month.month == 12 else month.replace(month=month.month+1)
            stage = f'h{1 if month.month <= 6 else 2}-{month.year}'
            folder = target / stage / month.strftime('%Y-%m') / 'ABI-L2-CMIPF'
            if (folder / 'collection.json').is_file():
                selection = catalog.load_selection(folder / 'collection.json')
            else:
                selection = goes.discover(month.isoformat(), following.isoformat(), satellite='auto')
            asset = next((a for a in selection.assets if a.id == fact['asset_id']), None)
            if asset is None:
                # A replacement NOAA version must be read normally, never skipped.
                unmatched += 1
                print(f'Source-gap fact not applicable to current listing: {fact["time"]}', flush=True)
                continue
            selection.discovery_facts = dict(getattr(selection, 'discovery_facts', {}) or {})
            issues = list(selection.discovery_facts.get('source_issues', []))
            issues = [v for v in issues if v.get('asset_id') != fact['asset_id']]
            selection.discovery_facts['source_issues'] = [*issues, fact]
            catalog.confirmed_source_issues(selection)
            catalog.save_selection(selection, folder, index_results=False)
            imported += 1
    return {'copied_selection_files': copied, 'imported_source_issues': imported,
            'unmatched_source_issues': unmatched, 'host': socket.gethostname()}


def aggregate_status(state, *, partition=None, start=None, end=None):
    """Read durable progress; use the existing readable table renderer per node."""
    from .job_status import main as monitor
    plan = json.loads((Path(state) / 'ensemble.json').read_text())
    if partition is not None and partition not in {row['label'] for row in plan['partitions']}:
        raise ValueError(f'Unknown partition {partition}')
    code = 0
    for row in plan['partitions']:
        if partition is not None and row['label'] != partition:
            continue
        print(f'\n{row["label"]} | {row["start"]} → {row["end"]} (end excluded) | node {row.get("host", "unassigned")}', flush=True)
        progress = Path(state) / 'partitions' / row['label'] / 'progress.json'
        if progress.is_file():
            code = max(code, monitor(['--progress', str(progress), '--start', start or row['start'],
                                     '--end', end or row['end']], source='goes'))
        else:
            print('No durable worker progress yet; consult PBS output.')
    return code


def _exit_code(codes):
    failed = [value for value in codes if value not in (0, 75)]
    return 1 if failed else 75 if 75 in codes else 0


def _merge(state, scratch, canonical, *, require_all=True):
    from .index_merge import merge_snapshots
    state, canonical = Path(state), Path(canonical)
    plan = json.loads((state / 'ensemble.json').read_text())
    snapshots = [state / 'partitions' / row['label'] / 'archive_index.duckdb' for row in plan['partitions']]
    if not any(path.exists() for path in snapshots) and (state / 'merge.json').is_file() and canonical.is_file():
        previous = json.loads((state / 'merge.json').read_text())
        if previous.get('ensemble_run_id') == plan.get('ensemble_run_id'):
            return previous
    if require_all and any(not path.is_file() for path in snapshots):
        raise FileNotFoundError('A worker snapshot is missing; retain all inputs and retry after inspection')
    inputs = [path for path in snapshots if path.is_file()]
    # Include prior single-node radar and satellite history, if available.
    project_state = state.parents[1] if state.name in ('study', 'pilot') else state.parent
    for candidate in (state / 'archive_index.duckdb', project_state / 'study' / 'archive_index.duckdb'):
        if candidate.is_file():
            inputs.append(candidate)
    result = merge_snapshots(inputs, canonical, scratch)
    result['ensemble_run_id'] = plan.get('ensemble_run_id')
    save(state / 'merge.json', result)
    # These are only closed copies. Verified monthly arrays are never removed.
    for snapshot in snapshots:
        if snapshot.resolve() != canonical.resolve():
            snapshot.unlink(missing_ok=True)
    return result


def launch(config, nodefile, state, code_root, python, seconds, *, mode='study', dry_run=False):
    selected, nodes = partitions(config), hosts(nodefile)
    if mode not in ('study', 'pilot') or seconds <= 600:
        raise ValueError('Use study/pilot mode and more than ten minutes walltime')
    state, code_root = Path(state).resolve(), Path(code_root).resolve()
    profile = Path(os.getenv('ECORE_PROFILE', str(code_root / 'jobs/goes_polaris.json')))
    if os.getenv('ECORE_GOES_STORAGE_CAP_GB') or json.loads(profile.read_text()).get('storage_cap_gb'):
        raise ValueError('Multi-node jobs use real quota/free-space checks; a per-node dataset cap is not a shared reservation')
    plan = {'schema': 1, 'status': 'planned', 'ensemble_run_id': uuid.uuid4().hex,
            'pbs_job_id': os.getenv('PBS_JOBID'),
            'host': socket.gethostname(), 'pid': os.getpid(), 'heartbeat': time.time(),
            'mode': mode, 'partitions': [{**p.__dict__, 'host': node} for p, node in zip(selected, nodes)],
            'state': str(state), 'profile': str(profile),
            'canonical_index': str(state.parents[1] / 'archive_index.duckdb')}
    if dry_run:
        print(json.dumps(plan, indent=2))
        return 0
    with ownership(state):
        save(state / 'ensemble.json', {**plan, 'status': 'running', 'started_at': time.time()})
        runtime = Path(os.getenv('ECORE_NODE_SCRATCH', '/local/scratch')) / f'ecore-ensemble-{os.getenv("USER", "user")}-{os.getenv("PBS_JOBID", "local").split(".")[0]}'
        runtime.mkdir(parents=True, exist_ok=True)
        canonical = Path(plan['canonical_index'])
        # Seed files are closed, immutable while workers run, and optional.
        processes, logs = [], []
        started = time.monotonic()
        stopping = False
        previous = {}

        def pause(signum=None, frame=None):
            nonlocal stopping
            stopping = True
            for p in selected:
                path = state / 'partitions' / p.label / 'fetch' / 'pause.request'
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('Allocation requested pause; preserve verified partial checkpoints.\n')

        for sig in (signal.SIGINT, signal.SIGTERM):
            previous[sig] = signal.signal(sig, pause)
        codes = {}
        launch_error = None
        try:
            for p, host in zip(selected, nodes):
                hostfile = runtime / f'{p.label}.hosts'
                hostfile.write_text(host + '\n')
                worker_state = state / 'partitions' / p.label
                worker_state.mkdir(parents=True, exist_ok=True)
                begin, end = (p.pilot_start, p.pilot_end) if mode == 'pilot' else (p.start, p.end)
                env = dict(os.environ, ECORE_GOES_PARTITION=p.label,
                           ECORE_PYTHON=str(python), ECORE_CODE_ROOT=str(code_root),
                           ECORE_GOES_NODE_ONLY='1', ECORE_PBS_MODE=mode,
                           ECORE_GOES_STATE_ROOT=str(worker_state),
                           PBS_GOES_START=begin, PBS_GOES_END=end,
                           ECORE_GOES_CHECKPOINT_DIR=str(state / 'partial-archives'),
                           ECORE_GOES_LEGACY_STATE=str(state),
                           ECORE_GOES_SEED_INDEX=str(canonical),
                           ECORE_GOES_SOURCE_ISSUES=str(code_root / 'jobs/goes_source_issues.json'),
                           ECORE_GOES_ALLOCATION_DEADLINE=str(time.time() + max(0, seconds-180-(time.monotonic()-started))),
                           ECORE_GOES_JOB_SECONDS=str(seconds - 180))
                if mode == 'pilot':
                    env['ECORE_DESTINATION'] = str(Path(os.environ['ECORE_PROJECT_ROOT']) / 'ecore-weather' / 'goes-pilot-archives' / p.label)
                handler = RotatingFileHandler(worker_state / 'worker.log', maxBytes=4*1024**2, backupCount=1)
                handler.setFormatter(logging.Formatter('%(message)s'))
                # One bounded log per partition. Drain PIPE continuously so MPI
                # output cannot block a reader or grow an unbounded PBS file.
                def drain(stream, handler=handler):
                    try:
                        for line in stream:
                            handler.emit(logging.LogRecord('worker', logging.INFO, '', 0, line.rstrip('\n'), (), None))
                    finally:
                        stream.close()
                        handler.close()
                command = ['mpiexec', '-n', '1', '--ppn', '1', '--depth=64', '--cpu-bind', 'none',
                           '--hostfile', str(hostfile), '/bin/bash', str(code_root / 'jobs/polaris_goes.pbs')]
                print(f'Launching {p.label} on {host} | {begin} to {end} | {worker_state}/worker.log', flush=True)
                try:
                    proc = subprocess.Popen(command, env=env, stdout=subprocess.PIPE,
                                            stderr=subprocess.STDOUT, text=True, bufsize=1)
                    processes.append((p.label, proc))
                    log_thread = threading.Thread(target=drain, args=(proc.stdout,), daemon=True)
                    log_thread.start()
                    logs.append(log_thread)
                except OSError as exc:
                    handler.close()
                    codes[p.label] = 1
                    print(f'{p.label} failed to launch: {exc}; other workers continue', flush=True)
            if not processes:
                raise RuntimeError('No node worker launched; see launch errors above')
            total_workers = len(processes) + sum(p.label in codes for p in selected)
            heartbeat = time.monotonic()
            while len(codes) < total_workers:
                for label, proc in processes:
                    code = proc.poll()
                    if code is not None and label not in codes:
                        codes[label] = code
                        save(state / 'worker-exits.json', {'status': 'collecting', 'exit_codes': codes})
                        print(f'{label} exited {code}; other node workers continue independently', flush=True)
                elapsed = time.monotonic() - started
                if elapsed >= seconds - 360 and not stopping:
                    pause()
                if elapsed >= seconds - 150:
                    for label, proc in processes:
                        if proc.poll() is None:
                            proc.terminate()
                    break
                if time.monotonic() - heartbeat >= 30:
                    save(state / 'ensemble.json', {**plan, 'status': 'running', 'heartbeat': time.time(), 'exit_codes': codes})
                    heartbeat = time.monotonic()
                if len(codes) < total_workers:
                    time.sleep(1)
        except Exception as exc:
            launch_error = f'{type(exc).__name__}: {exc}'
            pause()
        finally:
            for label, proc in processes:
                if launch_error and proc.poll() is None:
                    proc.terminate()
                if proc.poll() is None:
                    try:
                        codes[label] = proc.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        codes[label] = proc.wait()
                else:
                    codes[label] = proc.returncode
            for log_thread in logs:
                log_thread.join(10)
            for sig, handler in previous.items():
                signal.signal(sig, handler)
            save(state / 'worker-exits.json', {'status': 'closed', 'exit_codes': codes})
        code = 1 if launch_error else _exit_code(list(codes.values()))
        summary = {**plan, 'status': 'complete' if code == 0 else 'paused' if code == 75 else 'failed',
                   'exit_codes': codes, 'wall_seconds': time.monotonic()-started,
                   'heartbeat': time.time(), 'merge': 'pending'}
        if launch_error:
            summary['error'] = launch_error
        if mode == 'study' and time.monotonic()-started < seconds-60:
            try:
                summary['merge'] = _merge(state, runtime, canonical)
            except Exception as exc:
                summary['merge_error'] = f'{type(exc).__name__}: {exc}'
                print(f'Index merge pending; node snapshots preserved: {exc}', flush=True)
        elif mode == 'pilot':
            summary['merge'] = 'not applicable: successful pilot removes temporary archives'
        save(state / 'ensemble.json', summary)
        print(json.dumps({k: summary[k] for k in ('status', 'exit_codes', 'merge')}, default=str), flush=True)
        return code


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='operation', required=True)
    run = commands.add_parser('launch')
    run.add_argument('--config', required=True)
    run.add_argument('--nodefile', required=True)
    run.add_argument('--state', required=True)
    run.add_argument('--code-root', required=True)
    run.add_argument('--python', default=sys.executable)
    run.add_argument('--seconds', type=int, required=True)
    run.add_argument('--mode', choices=('study', 'pilot'), default='study')
    run.add_argument('--dry-run', action='store_true')
    prep = commands.add_parser('prepare')
    prep.add_argument('--state', required=True)
    prep.add_argument('--legacy', required=True)
    prep.add_argument('--start', required=True)
    prep.add_argument('--end', required=True)
    prep.add_argument('--source-issues')
    for name in ('status', 'merge'):
        child = commands.add_parser(name)
        child.add_argument('--state', required=True)
        if name == 'status':
            child.add_argument('--partition', help='Show only one stable partition label')
            child.add_argument('--start', help='Filter month rows by UTC start')
            child.add_argument('--end', help='Filter month rows by exclusive UTC end')
        if name == 'merge':
            child.add_argument('--scratch', required=True)
            child.add_argument('--destination', required=True)
    args = parser.parse_args(argv)
    try:
        if args.operation == 'launch':
            return launch(args.config, args.nodefile, args.state, args.code_root, args.python,
                          args.seconds, mode=args.mode, dry_run=args.dry_run)
        if args.operation == 'prepare':
            print(json.dumps(prepare_worker(args.state, args.legacy, args.start, args.end,
                                            issue_file=args.source_issues)))
            return 0
        if args.operation == 'status':
            return aggregate_status(args.state, partition=args.partition, start=args.start, end=args.end)
        with ownership(args.state):
            print(json.dumps(_merge(args.state, args.scratch, args.destination), indent=2))
        return 0
    except Exception as exc:
        parser.exit(1, f'Multi-node {args.operation} failed: {type(exc).__name__}: {exc}\n')


if __name__ == '__main__':
    raise SystemExit(main())
