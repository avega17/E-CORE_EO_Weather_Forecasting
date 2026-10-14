"""Coordinate a verified month-end pause for an already running GOES job."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time


def _write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f'.tmp-{os.getpid()}')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    os.replace(temporary, path)


def _read(path):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return None


def _run_path(run_id):
    return Path('results/runs') / run_id / 'run.json'


def _live_run(run_id, expected_pid):
    """Require the named run record and the same live coordinator process."""
    run = _read(_run_path(run_id))
    # The coordinator replaces this small JSON record atomically. Tolerate the
    # brief interval where it is absent or incomplete while the PID is alive.
    if run is None:
        try:
            import psutil
            process = psutil.Process(expected_pid)
            command = ' '.join(process.cmdline())
            if process.is_running() and 'dataset_jobs.py' in command:
                return True, None
        except (ImportError, OSError, ValueError):
            pass
        return False, run
    if not run or run.get('run_id') != run_id or run.get('status') != 'running':
        return False, run
    if int(run.get('pid', -1)) != int(expected_pid):
        return False, run
    try:
        import psutil
        process = psutil.Process(expected_pid)
        command = ' '.join(process.cmdline())
        if (not process.is_running() or 'dataset_jobs.py' not in command
                or 'resume' not in command or 'goes' not in command):
            return False, run
        if process.create_time() > float(run.get('started_at', 0)) + 2:
            return False, run
    except (ImportError, OSError, ValueError):
        return False, run
    return True, run


def _verified_month(path, month):
    """Trust the coordinator's completed row and check its eight closed archives."""
    from .jobs_goes import BANDS

    row = _read(path)
    if (not row or row.get('status') != 'complete'
            or row.get('product') != 'ABI-L2-CMIPF'
            or row.get('start') != f'{month}-01'
            or row.get('missing_bands') != []
            or set(row.get('available_bands', [])) != set(BANDS)):
        return False
    archives = row.get('archives', [])
    if len(archives) != len(BANDS) or {int(a.get('band', -1)) for a in archives} != set(BANDS):
        return False
    for archive in archives:
        location = Path(archive.get('path', ''))
        marker_path = location.parent / 'complete.json'
        marker = _read(marker_path)
        if (archive.get('status') not in ('saved', 'reused')
                or not location.is_file() or not marker
                or marker.get('product') != 'ABI-L2-CMIPF'
                or marker.get('source') != 'goes'
                or not marker.get('archive_sha256')
                or f"C{int(archive.get('band')):02d}" not in location.parts
                or marker.get('observations') != archive.get('observations')
                or marker.get('stored_bytes') != location.stat().st_size
                or archive.get('stored_bytes') != location.stat().st_size):
            return False
    return True


def _stage_for_month(month):
    from .jobs_goes import STAGES, _months
    for stage, start, end in STAGES:
        for first, _ in _months(start, end):
            if first.strftime('%Y-%m') == month:
                return stage
    raise ValueError(f'{month} is outside the GOES study')


def arm_stop_after(argv=None):
    """Start one small supervisor that requests the normal pause after a month verifies."""
    parser = argparse.ArgumentParser(description=arm_stop_after.__doc__)
    parser.add_argument('--month', required=True, help='Verified study month, YYYY-MM')
    parser.add_argument('--run-id', required=True, help='Exact active GOES run identifier')
    parser.add_argument('--output', default='results/study-goes-native')
    args = parser.parse_args(argv)
    try:
        time.strptime(args.month, '%Y-%m')
    except ValueError:
        parser.error('--month must use YYYY-MM')
    stage = _stage_for_month(args.month)
    output = Path(args.output).resolve()
    state_path = output / f'stop-after-{args.month}.json'
    run_path = _run_path(args.run_id)
    run = _read(run_path)
    if not run or run.get('source') not in ('goes', None):
        # The durable run configuration stores source under its compact config.
        config = (run or {}).get('config', {})
        if config.get('source') != 'goes':
            parser.error(f'Run {args.run_id} is not an identifiable GOES run')
    pid = int(run.get('pid', 0))
    if not _live_run(args.run_id, pid)[0]:
        parser.error(f'GOES run {args.run_id} is not active')
    if (output / 'pause.request').exists():
        parser.error('A pause is already requested for this run')
    prior = _read(state_path)
    if prior and prior.get('status') in ('armed', 'pause_requested', 'paused'):
        parser.error(f'A stop-after controller already exists: {state_path}')

    state = {'schema': 1, 'status': 'armed', 'run_id': args.run_id,
        'coordinator_pid': pid, 'month': args.month, 'stage': stage,
        'output': str(output), 'checkpoint': str(output / 'checkpoints' / stage / f'{args.month}-01.json'),
        'armed_at': time.time(), 'heartbeat': time.time(),
        'behavior': 'request normal pause after all eight band archives are verified; already admitted writers drain'}
    _write(state_path, state)
    repo = Path(__file__).resolve().parents[2]
    pythonpath = repo / 'src'
    code = (f"import sys; sys.path.insert(0, {str(pythonpath)!r}); "
            f"from ecore_weather.jobs_control import watch_stop_after; "
            f"watch_stop_after({str(state_path)!r})")
    session = f"ecore_goes_stop_after_{args.month.replace('-', '_')}"
    if subprocess.run(['tmux', 'has-session', '-t', session], capture_output=True).returncode == 0:
        parser.error(f'Controller session already exists: {session}')
    command = [sys.executable, '-c', code]
    try:
        subprocess.run(['tmux', 'new-session', '-d', '-s', session, '-c', str(repo),
                        shlex.join(command)], check=True)
    except (OSError, subprocess.CalledProcessError) as exc:
        state.update(status='controller_start_failed', ended_at=time.time(), error=str(exc))
        _write(state_path, state)
        parser.error(f'Could not start stop-after controller: {exc}')
    state['controller'] = session
    state['controller_started_at'] = time.time()
    _write(state_path, state)
    print(json.dumps({'status': 'armed', 'month': args.month, 'run_id': args.run_id,
        'controller': session, 'state': str(state_path),
        'checkpoint': state['checkpoint']}, indent=2))
    return 0


def watch_stop_after(state_file, poll_seconds=30):
    """Wait for the target's verified checkpoint, then request a drain-safe pause."""
    state_file = Path(state_file)
    state = _read(state_file)
    if not state or state.get('status') != 'armed':
        return 2
    checkpoint = Path(state['checkpoint'])
    pause_request = Path(state['output']) / 'pause.request'
    while True:
        alive, run = _live_run(state['run_id'], state['coordinator_pid'])
        if not alive:
            final = _read(Path(state['output']) / 'paused.json')
            if final and final.get('status') == 'paused':
                state.update(status='paused_before_target', ended_at=time.time(),
                    note='Coordinator paused before the requested month completed.')
            else:
                state.update(status='coordinator_exited_before_target', ended_at=time.time(),
                    coordinator_state=(run or {}).get('status'))
            _write(state_file, state)
            return 1

        if _verified_month(checkpoint, state['month']):
            if pause_request.exists():
                state.update(status='pause_request_conflict', ended_at=time.time(),
                    error='A different pause request appeared before the target month verified.')
                _write(state_file, state)
                return 1
            pause_request.parent.mkdir(parents=True, exist_ok=True)
            temporary = pause_request.with_name(pause_request.name + f'.tmp-{os.getpid()}')
            temporary.write_text(f"Pause after verified GOES month {state['month']}\n")
            os.replace(temporary, pause_request)
            from .jobs_goes import BANDS
            state.update(status='pause_requested', pause_requested_at=time.time(),
                verified_checkpoint_mtime=checkpoint.stat().st_mtime,
                verified_bands=list(BANDS))
            _write(state_file, state)
            break

        state['heartbeat'] = time.time()
        _write(state_file, state)
        time.sleep(poll_seconds)

    while True:
        state_now = _read(_run_path(state['run_id']))
        paused = _read(Path(state['output']) / 'paused.json')
        if state_now and state_now.get('status') != 'running':
            if paused and paused.get('status') == 'paused' and _verified_month(checkpoint, state['month']):
                state.update(status='paused', ended_at=time.time(),
                    run_exit_code=state_now.get('exit_code'),
                    note='The coordinator completed its drain-safe pause after the requested verified month.')
            else:
                state.update(status='coordinator_stopped_unexpectedly', ended_at=time.time(),
                    coordinator_state=state_now.get('status'))
            _write(state_file, state)
            return 0 if state['status'] == 'paused' else 1
        state['heartbeat'] = time.time()
        _write(state_file, state)
        time.sleep(poll_seconds)
