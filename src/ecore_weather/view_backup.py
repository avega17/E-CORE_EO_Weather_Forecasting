"""Read the verified HF year bundles and restore one monthly Zarr ZIP on demand.

The annual ZIP is a backup container, not a Zarr store.  Its monthly members
are ZIP_STORED, so S3 range reads can inspect metadata or copy one member
without downloading the other months.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import time
from pathlib import Path
import zipfile

from .hf_storage import BucketWriter


class S3RangeFile(io.RawIOBase):
    """Small seekable read-ahead stream for a single immutable S3 object."""

    def __init__(self, client, bucket, key, block_size=4 * 1024 * 1024, metrics=None):
        self.client, self.bucket, self.key = client, bucket, key
        self.size = client.head_object(Bucket=bucket, Key=key)['ContentLength']
        self.block_size = block_size
        self.position = 0
        self.metrics = metrics if metrics is not None else {}
        self.cached_start = -1
        self.cached = b''

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.position

    def seek(self, offset, whence=os.SEEK_SET):
        position = offset + (self.position if whence == os.SEEK_CUR else self.size if whence == os.SEEK_END else 0)
        if position < 0:
            raise ValueError('Negative seek in HF backup')
        self.position = position
        return position

    def read(self, size=-1):
        if size is None or size < 0:
            size = self.size - self.position
        remaining = min(size, max(0, self.size - self.position))
        chunks = []
        while remaining:
            if not self.cached_start <= self.position < self.cached_start + len(self.cached):
                self.cached_start = (self.position // self.block_size) * self.block_size
                end = min(self.size, self.cached_start + self.block_size) - 1
                started = time.perf_counter()
                response = self.client.get_object(Bucket=self.bucket, Key=self.key,
                    Range=f'bytes={self.cached_start}-{end}')
                try:
                    expected = end - self.cached_start + 1
                    self.cached = response['Body'].read(expected + 1)
                finally:
                    response['Body'].close()
                self.metrics['returned_bytes'] = self.metrics.get('returned_bytes',0)+len(self.cached)
                self.metrics['get_requests'] = self.metrics.get('get_requests',0)+1
                self.metrics['transfer_seconds'] = self.metrics.get('transfer_seconds',0)+time.perf_counter()-started
                if len(self.cached) != expected:
                    raise IOError('HF did not honor the requested byte range')
            offset = self.position - self.cached_start
            part = self.cached[offset:offset + remaining]
            chunks.append(part)
            self.position += len(part)
            remaining -= len(part)
        return b''.join(chunks)


def list_bundles(remote_root, source='mrms'):
    """List verified annual backup markers; no Zarr chunks or ZIP data are read."""
    writer = BucketWriter(remote_root)
    prefix = writer.key(f'yearly-v1/{source}/').rstrip('/') + '/'
    pages = writer.client.get_paginator('list_objects_v2').paginate(
        Bucket=writer.config['bucket'], Prefix=prefix)
    bundles = []
    for page in pages:
        for item in page.get('Contents', []):
            key = item['Key']
            if not key.endswith('/complete.json'):
                continue
            response = writer.client.get_object(Bucket=writer.config['bucket'], Key=key)
            try:
                marker = json.loads(response['Body'].read())
            finally:
                response['Body'].close()
            if marker.get('source') != source or not marker.get('archive_sha256'):
                continue
            archive_key = key.removesuffix('complete.json') + marker['raw_path']
            bundles.append({**marker, 'key': archive_key, 'marker_key': key,
                'bucket': writer.config['bucket'], 'remote_root': remote_root})
    return sorted(bundles, key=lambda r: (r['source'], r['product'], r['year']))


def inspect_bundle(bundle):
    """Read annual manifest and compact monthly checkpoints by S3 byte ranges."""
    writer = BucketWriter(bundle['remote_root'])
    stream = S3RangeFile(writer.client, bundle['bucket'], bundle['key'])
    if stream.size != bundle['stored_bytes']:
        raise IOError('HF bundle length differs from completion marker')
    with zipfile.ZipFile(stream) as archive:
        manifest = json.loads(archive.read('year_manifest.json'))
        if (manifest.get('source') != bundle['source'] or
                manifest.get('product') != bundle['product'] or
                manifest.get('year') != bundle['year']):
            raise IOError('HF bundle manifest differs from completion marker')
        # Older verified packages predate the compact coverage checkpoints.
        # Their STAC Items still carry the listed NOAA object sizes, so use
        # the exact asset IDs included in each monthly member.
        source_sizes = {}
        for name in archive.namelist():
            if name.startswith('selections/') and name.endswith('-items.json'):
                selection = json.loads(archive.read(name))
                for feature in selection.get('features', []):
                    size = feature.get('properties', {}).get('ecore:source', {}).get('size')
                    if size is not None:
                        source_sizes[feature['id']] = int(size)
        rows = []
        for row in manifest['archives']:
            relative = Path(row['local_path'])
            month = f'{relative.parts[-3]}-{relative.parts[-2]}'
            asset_ids = row.get('asset_ids', [])
            source_bytes = (sum(source_sizes[asset_id] for asset_id in asset_ids)
                if asset_ids and all(asset_id in source_sizes for asset_id in asset_ids) else None)
            if source_bytes is None:
                marker = json.loads(archive.read(row['marker_member']))
                assets = marker.get('assets', [])
                if assets and all(asset.get('source_bytes') is not None for asset in assets):
                    source_bytes = sum(int(asset['source_bytes']) for asset in assets)
            if source_bytes is None:
                checkpoint = f"coverage/{month}/{bundle['product']}-checkpoint.json"
                if checkpoint in archive.namelist():
                    source_bytes = json.loads(archive.read(checkpoint)).get('source_listed_bytes')
            rows.append({**row, 'month': month, 'source_bytes': source_bytes})
    return rows


def restore_month(bundle, archive_row, cache_root, progress=None, metrics=None):
    """Extract and SHA-check one monthly member into a local analysis cache."""
    relative = Path(archive_row['local_path'])
    if relative.is_absolute() or '..' in relative.parts or relative.parts[0] != bundle['source']:
        raise ValueError('Unsafe monthly path in HF backup manifest')
    root = Path(cache_root).expanduser().resolve()
    target = (root / relative).resolve()
    if not target.is_relative_to(root):
        raise ValueError('Monthly path escapes the selected cache')
    marker_path = target.parent / 'complete.json'
    if target.is_file() and marker_path.is_file():
        marker = json.loads(marker_path.read_text())
        if marker.get('archive_sha256') == archive_row['sha256'] and _sha256(target) == archive_row['sha256']:
            return target
    if target.exists() or marker_path.exists():
        raise FileExistsError(f'Cache path already holds different data: {target}. Choose another cache directory.')
    writer = BucketWriter(bundle['remote_root'])
    stream = S3RangeFile(writer.client, bundle['bucket'], bundle['key'], metrics=metrics)
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = target.with_name('.' + target.name + '.part')
    try:
        with zipfile.ZipFile(stream) as archive:
            info = archive.getinfo(archive_row['member'])
            if info.compress_type != zipfile.ZIP_STORED or info.file_size != archive_row['stored_bytes']:
                raise IOError('Monthly archive member differs from yearly manifest')
            digest = hashlib.sha256()
            count = 0
            with archive.open(info) as source, staging.open('wb') as output:
                while block := source.read(4 * 1024 * 1024):
                    started = time.perf_counter()
                    output.write(block)
                    if metrics is not None: metrics['write_seconds'] = metrics.get('write_seconds',0)+time.perf_counter()-started
                    started = time.perf_counter()
                    digest.update(block)
                    if metrics is not None: metrics['hash_seconds'] = metrics.get('hash_seconds',0)+time.perf_counter()-started
                    count += len(block)
                    if progress:
                        progress(count, info.file_size)
            if count != archive_row['stored_bytes'] or digest.hexdigest() != archive_row['sha256']:
                raise IOError('Restored monthly archive failed SHA-256 verification')
            marker = json.loads(archive.read(archive_row['marker_member']))
            if (marker.get('archive_sha256') != digest.hexdigest() or
                    marker.get('stored_bytes') != count or marker.get('raw_path') != target.name):
                raise IOError('Restored monthly marker differs from archive')
        os.replace(staging, target)
        temporary_marker = marker_path.with_name('.complete.json.part')
        temporary_marker.write_text(json.dumps(marker, sort_keys=True) + '\n')
        os.replace(temporary_marker, marker_path)
        return target
    finally:
        staging.unlink(missing_ok=True)


def _sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()
