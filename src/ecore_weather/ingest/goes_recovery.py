"""Exceptional source failures, separate from archive and configuration errors."""
from __future__ import annotations

from ecore_weather.core.common import InvalidResponseError, validate_response, format_error
import errno
import hashlib
import json
from pathlib import Path


class SourceReadError(OSError):
    def __init__(self, asset, message, evidence=None, metrics=None):
        super().__init__(message)
        self.asset, self.evidence, self.metrics = asset, evidence, metrics or {}

    def __reduce__(self):
        return type(self), (self.asset, str(self), self.evidence, self.metrics)








def source_error(exc):
    from ecore_weather.sources.goes import hdf_integrity_error, ConfirmedCorruptSourceError
    from ecore_weather.core.transfer_budget import BudgetExceeded
    if isinstance(exc, (MemoryError, BudgetExceeded)):
        return False
    if isinstance(exc, OSError) and exc.errno in (errno.ENOSPC, errno.EROFS, errno.EACCES, errno.ENOMEM):
        return False
    if isinstance(exc, (SourceReadError, InvalidResponseError, ConfirmedCorruptSourceError, TimeoutError, ConnectionError)):
        return True
    if format_error(exc) or hdf_integrity_error(exc):
        return True
    if type(exc).__module__.startswith(('obstore', 'requests', 'botocore', 'aiohttp', 'urllib3')):
        return True
    return any(text in str(exc).lower() for text in (
        'incomplete noaa', 'connection reset', 'connection timed out',
        'timed out', 'unexpected eof', 'remote end closed', 'http 503', 'http 502', 'http 429'))


def restore_issues(selection, groups, scratch, durable_root=None):
    """Import exact scientific evidence before constructing a resumed row map."""
    from ecore_weather.storage.catalog import confirmed_source_issues
    from ecore_weather.ingest.monthly_stream import _archive_rows
    assets = {a.id:a for a in selection.assets}
    for (target, band), chosen in groups:
        marker = Path(target)/'complete.json'
        prior = json.loads(marker.read_text()) if marker.is_file() else {}
        rows = _archive_rows(selection, chosen, prior, include_unavailable=True)
        identity = hashlib.sha256(json.dumps({'target':str(Path(target).resolve()),
            'ids':[r['asset_id'] for r in rows]},sort_keys=True).encode()).hexdigest()[:24]
        name = 'goes-cmipf-'+identity
        candidates = [Path(scratch)/name/'source-issues.json']
        if durable_root:
            candidates.append(Path(durable_root)/name/'checkpoint.json')
        for path in candidates:
            if not path.is_file():continue
            facts = json.loads(path.read_text()).get('source_issues', [])
            for fact in facts:
                if fact['asset_id'] not in assets:
                    raise ValueError('Restored source evidence has a different selection')
                # Validate the entire fact, including URL, time, ETag and ROI,
                # rather than rebuilding it from a potentially unrelated asset.
                old = getattr(selection,'discovery_facts',{})
                selection.discovery_facts = {**old,'source_issues':[
                    *[v for v in old.get('source_issues',[]) if v['asset_id'] != fact['asset_id']], fact]}
                confirmed_source_issues(selection)


def save_local_issues(parent, selection):
    from ecore_weather.storage.catalog import confirmed_source_issues
    from ecore_weather.ingest.goes_monthly import _checkpoint
    _checkpoint(Path(parent)/'source-issues.json', {'source_issues':list(confirmed_source_issues(selection).values())})
