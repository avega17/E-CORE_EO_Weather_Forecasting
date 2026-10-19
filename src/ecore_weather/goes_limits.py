"""Local admission/publication reservations for a bounded Eagle GOES job.

The ledger is node-local and disposable. Research archives and the shared
progress snapshot are the durable record, not this resource ledger.
"""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import threading

_LOCK = threading.Lock()
STATE_ALLOWANCE = 512 * 1024**2
PER_FRAME = {1: 226487, 2: 1572387, 3: 254879, 7: 153110,
             8: 147092, 9: 127256, 10: 137221, 13: 153428}


class IntentionalPause(RuntimeError):
    def __init__(self, reason):
        super().__init__('GOES intentional pause: ' + reason)


def physical_bytes(roots):
    """Count each physical file once, including uncommitted publication copies."""
    seen = set()
    total = 0
    for root in roots:
        for path in Path(root).rglob('*'):
            try:
                if path.is_symlink() or not path.is_file():
                    continue
                stat = path.stat()
            except FileNotFoundError:
                continue
            identity = (stat.st_dev, stat.st_ino)
            if identity not in seen:
                seen.add(identity)
                total += stat.st_size
    return total


def configure(destination, state, scratch, cap_gb):
    if cap_gb is None:
        os.environ.pop('ECORE_GOES_LIMIT_LEDGER', None)
        return None
    if cap_gb <= 0:
        raise ValueError('GOES storage cap must be positive')
    path = Path(scratch) / 'goes-storage-reservations.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    roots = [str(Path(destination).resolve() / 'goes'), str(Path(state).resolve())]
    roots.extend(json.loads(os.getenv('ECORE_GOES_ACCOUNTING_ROOTS', '[]')))
    path.write_text(json.dumps({'limit': int(cap_gb * 1e9), 'roots': roots, 'reservations': {}}))
    os.environ['ECORE_GOES_LIMIT_LEDGER'] = str(path.resolve())
    with locked() as data:
        check(data)
    return path


@contextmanager
def locked():
    path = os.getenv('ECORE_GOES_LIMIT_LEDGER')
    if not path:
        yield None
        return
    with _LOCK, open(path, 'r+') as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        data = json.load(stream)
        try:
            yield data
        finally:
            stream.seek(0)
            json.dump(data, stream)
            stream.truncate()
            stream.flush()
            fcntl.flock(stream, fcntl.LOCK_UN)


def check(data):
    if data is None:
        return
    occupied = physical_bytes(data['roots'])
    reserved = sum(data['reservations'].values())
    if occupied + reserved + STATE_ALLOWANCE > data['limit']:
        raise IntentionalPause(f'storage cap; occupied={occupied}, reserved={reserved}, '
                               f'state allowance={STATE_ALLOWANCE}, limit={data["limit"]} bytes')


def reserve_groups(groups):
    """Reserve estimated remaining output before a month starts reading NOAA."""
    with locked() as data:
        if data is None:
            return
        before = dict(data['reservations'])
        for (target, band), assets in groups:
            marker = Path(target) / 'complete.json'
            saved = json.loads(marker.read_text()) if marker.exists() else {}
            ids = set(saved.get('asset_ids', [a['asset_id'] for a in saved.get('assets', [])]))
            new = sum(a.id not in ids for a in assets)
            if new:
                # Packed ZIP overhead and changing day/night compression need margin.
                data['reservations'][str(Path(target).resolve())] = int(saved.get('stored_bytes', 0)) + int(new * PER_FRAME[band] * 1.2) + 1024**2
        try:
            check(data)
        except BaseException:
            data['reservations'] = before
            raise


@contextmanager
def publication(target, size):
    """Replace the estimate with actual candidate bytes before copying to Eagle."""
    key = str(Path(target).resolve())
    with locked() as data:
        if data is not None:
            previous = data['reservations'].get(key, 0)
            data['reservations'][key] = size
            try:
                check(data)
            except BaseException:
                data['reservations'][key] = previous
                raise
    yield
    # Once publication succeeds the physical archive is counted instead.
    with locked() as data:
        if data is not None:
            data['reservations'].pop(key, None)
