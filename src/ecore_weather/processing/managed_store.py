"""Manage the workstation's primary processed dataset."""
from contextlib import ExitStack
import fcntl
import json
from pathlib import Path
import shutil

from ecore_weather.processing.transforms import atomic_json


def primary_store(destination):
    root = Path(destination).expanduser().resolve()
    pointer = root / '.managed-selection.json'
    if pointer.is_file():
        name = json.loads(pointer.read_text())['selection_id']
        path = root / name
        if path.parent == root and path.is_dir():
            return path
    # Adopt the largest existing full build as the primary. Other research
    # outputs stay separate until the user explicitly removes them.
    candidates = []
    for progress in root.glob('*/full/progress.json'):
        state = json.loads(progress.read_text())
        candidates.append((state['verified_slots'], progress.parent.parent))
    return max(candidates)[1] if candidates else None


def writer_active(unit):
    path = Path(unit) / 'owner.lock'
    if not path.is_file():
        return False
    with path.open('rb') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
    return False


def lock_file(stack, path, message):
    lock = stack.enter_context(path.open('a'))
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        raise RuntimeError(message) from exc


def build(preparation, destination, size, selection_id, replacement, execute):
    """Serialize managed builds and require an exact, explicit retirement target."""
    root = Path(destination).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    target = root / selection_id
    if target.parent != root or target.is_symlink():
        raise ValueError('Invalid processing selection identity')
    with ExitStack() as stack:
        lock_file(stack, root / '.managed-build.lock', 'A managed dataset build is already running')
        previous = primary_store(root)
        if previous and previous != target:
            if not replacement or Path(replacement).expanduser().resolve() != previous:
                raise ValueError(f'Explicitly confirm replacement of {previous}')
            if previous.parent != root or previous.is_symlink():
                raise ValueError('Refusing to replace a store outside its managed destination')
            if preparation and Path(preparation).resolve().is_relative_to(previous):
                raise ValueError('The preparation index must live outside the store being replaced')
            # The running study predates the managed lock and owns the unit lock.
            units = sorted(p for p in previous.iterdir()
                           if p.is_dir() and (p.name == 'full' or p.name.startswith('block-')))
            if not units or any(p.is_symlink() for p in units):
                raise ValueError('Could not establish ownership of the replacement target')
            for unit in units:
                lock_file(stack, unit / 'owner.lock', f'A writer is active in {unit}')
                lock_file(stack, unit / 'readers.lock', f'A reader is active in {unit}')
            free = shutil.disk_usage(root).free
            for file in previous.rglob('*'):
                if file.is_file() and not file.is_symlink():
                    info = file.stat()
                    free += info.st_blocks * 512
            if size['required_bytes'] > free - size['disk']['reserve_bytes']:
                raise OSError('Replacement will not fit after the required free-space reserve')
            shutil.rmtree(previous)
        elif replacement and (not previous or previous != target):
            raise ValueError('The confirmed replacement target has changed. Review the build again.')
        atomic_json(root / '.managed-selection.json', {'selection_id': selection_id})
        return execute()
