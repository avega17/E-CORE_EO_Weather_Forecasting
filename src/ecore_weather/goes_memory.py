"""Compact process-memory measurements and distinct study/benchmark policies."""
from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import time

GIB = 1024**3


def sample_memory():
    """Sum resident pages, not unique physical RAM; tolerate exiting children."""
    import psutil
    root = psutil.Process()
    processes = [root]
    try:
        processes += root.children(recursive=True)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass
    rows = []
    for process in processes:
        try:
            info = process.memory_info()
            rows.append({'pid': process.pid, 'name': process.name(), 'rss_bytes': info.rss})
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    node = psutil.virtual_memory()
    return {'sampled_at': time.time(), 'rss_bytes': sum(r['rss_bytes'] for r in rows),
            'node_total_bytes': node.total, 'node_available_bytes': node.available,
            'process_count': len(rows), 'largest_processes': sorted(rows,
                key=lambda r: r['rss_bytes'], reverse=True)[:12],
            'measurement': 'Sum of coordinator and descendant RSS; shared pages may be counted more than once. Node available includes reclaimable memory.'}


@dataclass(frozen=True)
class MemoryPolicy:
    mode: str = 'strict'
    warning_gib: float = 0
    minimum_available_gib: float = 0
    grace_seconds: float = 0
    sample_seconds: float = .1
    emergency_available_gib: float = 0

    def validate(self):
        if self.mode not in ('strict', 'checkpoint_pause'):
            raise ValueError('GOES memory mode must be strict or checkpoint_pause')
        if min(self.warning_gib, self.minimum_available_gib, self.grace_seconds,self.emergency_available_gib) < 0 or self.sample_seconds <= 0:
            raise ValueError('Memory thresholds/grace must be nonnegative; sampling interval must be positive')
        return self


class MemoryGuard:
    """A strict benchmark fails; sustained study pressure saves checkpoints."""
    def __init__(self, limit=0, policy=None, report=None, sampler=sample_memory):
        self.limit = int(limit)
        if self.limit < 0:
            raise ValueError('Memory limit must be nonnegative')
        self.policy = (policy or MemoryPolicy()).validate()
        self.report, self.sampler = report, sampler
        self.latest = None
        self.history = deque(maxlen=12)
        self.peak = 0
        self.last_poll = self.last_report = None
        self.exceeded_since = None
        self.warned = False

    @classmethod
    def from_environment(cls):
        raw = json.loads(os.getenv('ECORE_GOES_MEMORY_POLICY', '{}'))
        limit=os.getenv('ECORE_RSS_LIMIT_BYTES','0')
        if not raw and not int(limit):raw={'sample_seconds':5}
        return cls(limit, MemoryPolicy(**raw),
                   os.getenv('ECORE_GOES_MEMORY_REPORT'))

    def summary(self):
        return {'policy': asdict(self.policy), 'rss_limit_bytes': self.limit,
                'sampled_peak_rss_bytes': self.peak, 'latest': self.latest,
                'recent_samples': list(self.history)}

    def publish(self):
        if self.report:
            from .runlog import save
            # Monitoring failures cannot replace the actual fetch outcome.
            try:
                target=Path(self.report)
                pending=target.with_name(target.name+'.tmp')
                save(pending, self.summary())
                os.replace(pending,target)
            except OSError as exc:
                print(f'GOES memory report unavailable: {exc}', flush=True)

    def check(self, now=None, roles=None):
        now = time.monotonic() if now is None else now
        if self.last_poll is not None and now-self.last_poll < self.policy.sample_seconds:
            return
        self.last_poll = now
        self.latest = self.sampler()
        if roles:
            for row in self.latest['largest_processes']:
                row['role'] = roles.get(row['pid'], 'reader/finalizer/helper')
        rss = self.latest['rss_bytes']
        available = self.latest['node_available_bytes']
        self.peak = max(self.peak, rss)
        self.history.append({k: self.latest[k] for k in
            ('sampled_at', 'rss_bytes', 'node_available_bytes', 'process_count')})
        reasons = []
        if self.limit and rss > self.limit:
            reasons.append(f'summed RSS {rss/GIB:.2f} GiB exceeds configured {self.limit/GIB:.2f} GiB')
        if self.policy.minimum_available_gib and available < self.policy.minimum_available_gib*GIB:
            reasons.append(f'node available RAM {available/GIB:.2f} GiB is below {self.policy.minimum_available_gib:g} GiB')
        warning = bool(reasons) or bool(self.policy.warning_gib and rss >= self.policy.warning_gib*GIB)
        if warning and not self.warned:
            print(f'GOES memory warning: summed RSS {rss/GIB:.2f} GiB; node available {available/GIB:.2f} GiB; '
                  f'policy {self.policy.mode}, grace {self.policy.grace_seconds:g}s. Shared pages can inflate summed RSS.', flush=True)
        self.warned = warning
        if reasons:
            if self.exceeded_since is None:
                self.exceeded_since = now
        else:
            self.exceeded_since = None
        duration = 0 if self.exceeded_since is None else now-self.exceeded_since
        self.latest.update(pressure_reasons=reasons, pressure_seconds=duration, warning=warning)
        emergency=bool(self.policy.emergency_available_gib and available < self.policy.emergency_available_gib*GIB)
        if emergency:
            reasons.append(f'critical available RAM below {self.policy.emergency_available_gib:g} GiB; no grace delay')
        stop = emergency or bool(reasons) and (self.policy.mode == 'strict' or duration >= self.policy.grace_seconds)
        if stop or warning or self.last_report is None or now-self.last_report >= 30:
            self.publish()
            self.last_report = now
        if stop:
            reason = '; '.join(reasons)
            if self.policy.mode == 'strict':
                raise MemoryError(f'GOES benchmark memory limit: {reason}')
            from .goes_limits import IntentionalPause
            raise IntentionalPause(f'sustained memory pressure for {duration:g}s: {reason}; saving verified partial checkpoints')


def configure_study(profile, report):
    """Use the profile unless a wrapper/benchmark supplied an explicit policy."""
    if profile and 'ECORE_RSS_LIMIT_BYTES' not in os.environ and 'ECORE_GOES_MEMORY_POLICY' not in os.environ:
        data=json.loads(Path(profile).read_text())
        if 'job_memory_gib' in data:
            os.environ['ECORE_RSS_LIMIT_BYTES']=str(int(data['job_memory_gib']*GIB))
            os.environ['ECORE_GOES_MEMORY_POLICY']=json.dumps(data.get('memory_policy',{}))
    os.environ.setdefault('ECORE_GOES_MEMORY_REPORT',str(report))
