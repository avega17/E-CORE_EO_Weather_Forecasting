"""Incremental, verified native GOES scan checkpoints on durable storage.

Each immutable ZIP contains only new time chunks, their batch checksums, and
the small metadata needed to reconstruct the node-local working store. This
does not publish a partial store as a completed research archive.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import zipfile


def _sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(8 * 1024**2), b''):
            digest.update(block)
    return digest.hexdigest()


class DurableBatches:
    """Single-owner incremental checkpoints; restore is verified before use."""

    def __init__(self, root, parent, rows, fraction=.25):
        if not 0 < fraction <= 1:
            raise ValueError('Checkpoint fraction must be greater than zero and at most one')
        self.parent = Path(parent)
        self.folder = Path(root) / self.parent.name
        if (self.folder.resolve().is_relative_to(self.parent.resolve())
                or self.parent.resolve().is_relative_to(self.folder.resolve())):
            raise ValueError('Durable checkpoint directory must be separate from working scratch')
        self.rows = rows
        self.ids_digest = hashlib.sha256('\n'.join(r['asset_id'] for r in rows).encode()).hexdigest()
        # Publication occurs only at already verified eight-scan boundaries.
        self.interval = max(8, ((int(len(rows) * fraction) + 7) // 8) * 8)
        self.state = self._load()

    def _load(self):
        receipt = self.folder / 'checkpoint.json'
        state = json.loads(receipt.read_text()) if receipt.is_file() else {
            'schema': 1, 'identity': self.parent.name, 'selected': len(self.rows),
            'ordered_asset_ids_sha256': self.ids_digest, 'parts': [], 'next_index': 0}
        if (state.get('schema') != 1 or state.get('identity') != self.parent.name
                or state.get('ordered_asset_ids_sha256') != self.ids_digest
                or state.get('selected') != len(self.rows)):
            raise IOError('Durable checkpoint scientific selection differs')
        end = 0
        for part in state['parts']:
            if part['begin'] != end or not end < part['end'] <= len(self.rows):
                raise IOError('Non-contiguous durable checkpoint parts')
            if Path(part['file']).name != part['file']:
                raise IOError('Invalid durable checkpoint filename')
            end = part['end']
        if end != state['next_index']:
            raise IOError('Durable checkpoint observation count differs')
        return state

    @property
    def count(self):
        return self.state['next_index']

    def restore(self):
        """Restore committed parts when the local prefix is shorter or absent."""
        # A power loss during extraction may have copied progress.json before
        # all batches. Only actual, contiguous batch records establish a prefix.
        count = 0
        for batch in sorted(self.parent.glob('batch-*.json')):
            for item in json.loads(batch.read_text())['rows']:
                if item['index'] != count or item['row']['asset_id'] != self.rows[count]['asset_id']:
                    raise IOError('Local checkpoint selection is not contiguous')
                count += 1
        if count >= self.count:
            return 0
        # Validate every part and member before changing local recovery files.
        for part in self.state['parts']:
            path = self.folder / part['file']
            if _sha(path) != part['sha256']:
                raise IOError(f'Durable checkpoint hash mismatch: {path}')
            with zipfile.ZipFile(path) as archive:
                for name in archive.namelist():
                    relative = PurePosixPath(name)
                    allowed = (relative.parts and relative.parts[0] == 'raw.zarr'
                               or len(relative.parts) == 1 and (name == 'progress.json'
                               or name.startswith('batch-') and name.endswith('.json')))
                    if relative.is_absolute() or '..' in relative.parts or not allowed:
                        raise IOError('Invalid durable checkpoint ZIP member')
                if archive.testzip() is not None:
                    raise IOError('Durable checkpoint ZIP checksum mismatch')
        self.parent.mkdir(parents=True, exist_ok=True)
        # All-fill chunks may legitimately be absent from a ZIP. Remove an
        # untrusted local prefix first so stale chunks cannot replace that fill.
        for meta in (self.parent/'raw.zarr').glob('*/zarr.json'):
            dims=json.loads(meta.read_text()).get('dimension_names',[])
            if len(dims)>1 and dims[0]=='time':
                for chunk in (meta.parent/'c').glob('*'):
                    if chunk.name.isdigit() and int(chunk.name)<self.count:
                        if chunk.is_dir():shutil.rmtree(chunk)
                        else:chunk.unlink()
        for part in self.state['parts']:
            with zipfile.ZipFile(self.folder / part['file']) as archive:
                archive.extractall(self.parent)
        return self.count

    def publish(self, count, force=False):
        """Copy new compressed chunks once; leave old receipt intact on failure."""
        if not 0 <= count <= len(self.rows):
            raise ValueError('Durable checkpoint count is outside the selection')
        begin = self.count
        if count <= begin or not force and count - begin < self.interval and count != len(self.rows):
            return False
        if count != len(self.rows) and count % 8:
            raise ValueError('Durable checkpoint must end at a verified scan batch')
        files = [self.parent / 'progress.json']
        files += [p for p in self.parent.glob('batch-*.json')
                  if begin <= int(p.stem.split('-')[1]) < count]
        # Current native layout has one complete ROI per time chunk. Refuse a
        # changed layout rather than copying incomplete shards as checkpoints.
        store = self.parent / 'raw.zarr'
        files.append(store / 'zarr.json')
        for meta in sorted(store.glob('*/zarr.json')):
            description = json.loads(meta.read_text())
            dimensions = description.get('dimension_names', [])
            is_measurement = len(dimensions) > 1 and dimensions[0] == 'time'
            if is_measurement and (description['chunk_grid']['configuration']['chunk_shape'][0] != 1
                    or any(c.get('name') == 'sharding_indexed' for c in description.get('codecs', []))):
                raise ValueError('Durable checkpoints require unsharded single-frame chunks')
            files.append(meta)
            for file in sorted((meta.parent / 'c').rglob('*')):
                if not file.is_file():
                    continue
                if is_measurement:
                    index = int(file.relative_to(meta.parent / 'c').parts[0])
                    if not begin <= index < count:
                        continue
                files.append(file)
        name = f'part-{begin:06d}-{count:06d}.zip'
        packed = self.parent / ('durable-' + name)
        try:
            with zipfile.ZipFile(packed, 'w', compression=zipfile.ZIP_STORED, allowZip64=True) as archive:
                for file in files:
                    archive.write(file, file.relative_to(self.parent).as_posix())
            digest = _sha(packed)
            from .goes_limits import publication
            with publication(self.folder, packed.stat().st_size):
                self.folder.mkdir(parents=True, exist_ok=True)
                pending = self.folder / ('.' + name + '.tmp')
                shutil.copyfile(packed, pending)
                if _sha(pending) != digest:
                    raise IOError('Durable checkpoint copy failed verification')
                with pending.open('rb') as stream:
                    os.fsync(stream.fileno())
                os.replace(pending, self.folder / name)
                updated = {**self.state, 'next_index': count,
                    'parts': [*self.state['parts'], {'file': name, 'begin': begin,
                        'end': count, 'bytes': packed.stat().st_size, 'sha256': digest}]}
                from .goes_monthly import _checkpoint
                _checkpoint(self.folder / 'checkpoint.json', updated)
                self.state = updated
        finally:
            packed.unlink(missing_ok=True)
        return True

    def discard_after_completion(self):
        """Called only after final monthly archive publication and read-back."""
        if self.folder.is_dir():
            shutil.rmtree(self.folder)
