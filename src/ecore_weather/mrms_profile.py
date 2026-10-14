"""Explicit machine settings for MRMS; scientific selections stay independent."""
from __future__ import annotations

import hashlib
import json
import os
import contextlib
from pathlib import Path

DEFAULTS = dict(monthly_writers=4, workers=8, decode_workers=1,
                max_inflight=8, roi_queue_mib=128, memory_gib=32)


def add_arguments(parser, *, writers=True):
    parser.add_argument('--profile', help='Versioned MRMS machine JSON; explicit arguments override it')
    options = dict(DEFAULTS)
    if not writers:
        options.pop('monthly_writers')
    help_text = {
        'monthly_writers': 'Independent product-month processes, across adjacent months',
        'workers': 'Download threads per product-month (1–16)',
        'decode_workers': 'Independent GRIB decode slots per product-month',
        'max_inflight': 'Maximum admitted observations per writer, including active reads',
        'roi_queue_mib': 'Reserved ROI payload capacity per writer, in MiB',
        'memory_gib': 'Total job-process RSS ceiling in GiB; also covers decoder buffers',
    }
    for name in options:
        parser.add_argument('--'+name.replace('_', '-'), type=int, help=help_text[name])


def resolve(args):
    values = dict(DEFAULTS)
    identity = None
    if getattr(args, 'profile', None):
        path = Path(args.profile).expanduser().resolve()
        payload = path.read_bytes()
        data = json.loads(payload)
        if data.get('schema') != 1 or data.get('source') != 'mrms':
            raise ValueError('Expected a schema-1 MRMS profile')
        settings = data.get('resources', {})
        unknown = set(settings)-set(DEFAULTS)
        if unknown:
            raise ValueError(f'Unknown MRMS profile resources: {sorted(unknown)}')
        values.update(settings)
        identity = dict(path=str(path), sha256=hashlib.sha256(payload).hexdigest(), name=data.get('name'))
    for key in DEFAULTS:
        explicit = getattr(args, key, None)
        if explicit is not None:
            values[key] = explicit
    if any(type(v) is not int or v < 1 for v in values.values()):
        raise ValueError('MRMS resource limits must be positive integers')
    if values['workers'] > 16:
        raise ValueError('MRMS workers must be at most 16; increase independent writers instead')
    if values['decode_workers'] > values['workers']:
        raise ValueError('Decode slots cannot exceed download workers')
    if values['max_inflight'] < values['decode_workers']:
        raise ValueError('Admitted observations cannot be fewer than decode slots')
    for key, value in values.items():
        setattr(args, key, value)
    return {'schema': 1, 'source': 'mrms', 'profile': identity, 'resources': values}


def hardware():
    import psutil
    return {'logical_cpus': psutil.cpu_count(), 'physical_cores': psutil.cpu_count(logical=False),
            'cpu_affinity': sorted(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else None,
            'memory_bytes': psutil.virtual_memory().total}


def code_identity(root=None):
    import subprocess
    root = Path(root or Path(__file__).resolve().parents[2])
    snapshot = root/'snapshot.json'
    if snapshot.exists():
        record = json.loads(snapshot.read_text())
        return record.get('git_revision'), record['sha256']
    revision = subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD'], text=True).strip()
    return revision, None


def cap_library_threads():
    for name in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS', 'NUMEXPR_NUM_THREADS'):
        os.environ[name] = '1'
    import torch
    torch.set_num_threads(1)


@contextlib.contextmanager
def job_memory_limit(memory_gib):
    keys=('ECORE_MEMORY_ROOT_PID','ECORE_MEMORY_LIMIT_BYTES')
    prior={key:os.environ.get(key) for key in keys}
    os.environ[keys[0]]=str(os.getpid())
    os.environ[keys[1]]=str(int(memory_gib*1024**3))
    try:
        yield
    finally:
        for key,value in prior.items():
            if value is None:os.environ.pop(key,None)
            else:os.environ[key]=value


def check_memory_limit():
    if not os.getenv('ECORE_MEMORY_LIMIT_BYTES'):
        return
    import psutil
    root=psutil.Process(int(os.environ['ECORE_MEMORY_ROOT_PID']))
    total=0
    for proc in [root,*root.children(recursive=True)]:
        try:total+=proc.memory_info().rss
        except psutil.Error:pass
    if total>int(os.environ['ECORE_MEMORY_LIMIT_BYTES']):
        raise MemoryError(f'Job-process RSS {total} bytes exceeds configured ceiling')
