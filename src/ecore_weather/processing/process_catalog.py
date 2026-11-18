"""Rebuildable scientific alignment and virtual native-ZIP catalogs."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
import json
from pathlib import Path
import time

import numpy as np
import pandas as pd
import xarray as xr

from ecore_weather.processing.transforms import Recipe, align_observations, atomic_json, observation_index, scientific_id, window_index


def build_index(location, preparation, start, end, recipe, index_path='results/dataset-processing/process.duckdb'):
    import duckdb
    started = time.perf_counter()
    observations, archives = observation_index(location, start, end, recipe)
    if observations.empty:
        raise ValueError('No completed native observations match this selection')
    identity = scientific_id({'recipe': asdict(recipe), 'start': str(start), 'end': str(end),
        'archives': sorted(zip(archives.archive_id, archives.sha256.fillna('')))})
    root = Path(preparation).expanduser().resolve() / identity
    root.mkdir(parents=True, exist_ok=True)
    aligned = align_observations(observations, start, end, recipe.variables)
    if aligned.empty:
        raise ValueError('No causal ten-minute observations could be aligned')
    windows = window_index(aligned, start, end, recipe.variables)
    # Parquet is portable scientific indexing; DuckDB is a disposable local query index.
    for name, frame in [('archives', archives), ('observations', observations),
                         ('aligned', aligned), ('windows', windows)]:
        temporary = root / f'{name}.tmp.parquet'
        frame.to_parquet(temporary, index=False)
        temporary.replace(root / f'{name}.parquet')
    if str(Path(index_path).resolve()).startswith('/mnt/'):
        raise ValueError('Live processing DuckDB must remain Linux-local')
    Path(index_path).parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(index_path)) as con:
        con.execute('CREATE TABLE IF NOT EXISTS processing_selections (id VARCHAR PRIMARY KEY, path VARCHAR, observations BIGINT, windows BIGINT, eligible BIGINT)')
        con.execute('INSERT OR REPLACE INTO processing_selections VALUES (?,?,?,?,?)',
                    [identity, str(root), len(observations), len(windows), int(windows.eligible.sum())])
    missing = {v: int(len(windows)) if v not in set(aligned.variable) else 0 for v in recipe.variables}
    summary = {'schema': 1, 'selection_id': identity, 'archive_root': str(Path(location).resolve()),
        'preparation_path': str(root), 'start': str(start), 'end_excluded': str(end),
        'recipe': asdict(recipe), 'observations': len(observations), 'archives': len(archives),
        'slots': int(aligned.slot_time.nunique()), 'windows': len(windows),
        'eligible_windows': int(windows.eligible.sum()), 'absent_variables': [v for v,n in missing.items() if n],
        'index_seconds': time.perf_counter()-started,
        'causality': 'GOES scan-end <= slot, age <= 10 min; MRMS <= slot, age <= 5 min; no reuse'}
    atomic_json(root / 'selection.json', summary)
    return summary


def load_index(preparation):
    root = Path(preparation)
    return (json.loads((root / 'selection.json').read_text()),
            pd.read_parquet(root / 'aligned.parquet'), pd.read_parquet(root / 'windows.parquet'))


def recipe_from_dict(value):
    value = dict(value)
    for key in ('bands', 'radar', 'bbox'):
        if key in value: value[key] = tuple(value[key])
    return Recipe(**value)


def virtual_dataset(path):
    from virtualizarr import open_virtual_dataset
    from virtualizarr.parsers import ZippedZarrParser
    from obspec_utils.registry import ObjectStoreRegistry
    from obstore.store import LocalStore
    registry = ObjectStoreRegistry({'file:///': LocalStore(prefix='/')})
    return open_virtual_dataset(Path(path).resolve().as_uri(), registry=registry,
        parser=ZippedZarrParser(), loadable_variables=['time', 'x', 'y', 'latitude', 'longitude'],
        decode_times=False)


def build_catalog(preparation):
    """Publish one native group per immutable archive; no implicit band alignment."""
    from ecore_weather.processing.process_store import owner
    with owner(Path(preparation)/'catalog-owner'):
        return _build_catalog(preparation)


def _build_catalog(preparation):
    import icechunk as ic
    root = Path(preparation)
    manifest = root / 'virtual.json'
    archives = pd.read_parquet(root / 'archives.parquet')
    if manifest.exists():
        saved = json.loads(manifest.read_text())
        current = {(r['archive_id'], r['path'], r['stored_bytes'], r['sha256'])
                   for r in archives.to_dict('records')}
        previous = {(r['archive_id'], r['path'], r['stored_bytes'], r['sha256'])
                    for r in saved['entries']}
        if current == previous and Path(saved['repository']).is_dir():
            for row in saved['entries']:
                if not Path(row['path']).is_file() or Path(row['path']).stat().st_size != row['stored_bytes']:
                    raise IOError(f'Virtual source missing/changed: {row["path"]}')
            return {**{k:v for k,v in saved.items() if k!='entries'}, 'reused': True}
    repository = root / 'native.icechunk'
    config = ic.RepositoryConfig()
    # Existing chunks live inside external immutable local ZIPs.
    source_root = json.loads((root/'selection.json').read_text())['archive_root']
    prefix = Path(source_root).as_uri().rstrip('/')+'/'
    config.set_virtual_chunk_container(ic.VirtualChunkContainer(prefix, ic.local_filesystem_store(source_root), name='native'))
    from icechunk.credentials import LocalFileSystemAccess
    repo = ic.Repository.open_or_create(ic.local_filesystem_storage(str(repository)), config=config,
        authorize_virtual_chunk_access={prefix: LocalFileSystemAccess})
    session = repo.writable_session('main')
    entries = []
    started = time.perf_counter()
    for row in archives.to_dict('records'):
        path = Path(row['path'])
        if not path.is_file() or path.stat().st_size != row['stored_bytes']:
            raise IOError(f'Native archive missing/changed: {path}')
        group = 'archives/' + row['archive_id']
        dataset = virtual_dataset(path)
        from ecore_weather.storage.archives import open_raw
        with open_raw(path) as native:
            # Coordinate decoding belongs to our native archive contract. Generic
            # CF encoding must not apply a packed-coordinate scale a second time.
            for name in native.coords:
                if name not in dataset:
                    continue  # Large provenance stays in its canonical sidecar.
                coordinate = native[name].load().copy(deep=True)
                coordinate.encoding = {}
                dataset = dataset.assign_coords({name: coordinate})
        # Generic parser omits our provenance sidecar. The manifest and reader below
        # retain its canonical archive location rather than duplicating large JSONs.
        dataset.vz.to_icechunk(session.store, group=group, mode='w')
        entries.append({**row, 'group': group, 'provenance': 'canonical ecore_metadata sidecar'})
    snapshot = session.commit('Reference verified native NOAA ZIP chunks')
    result = {'schema': 1, 'repository': str(repository), 'snapshot_id': snapshot,
        'entries': entries, 'seconds': time.perf_counter()-started,
        'metadata_bytes': sum(p.stat().st_size for p in repository.rglob('*') if p.is_file()),
        'source_dependency': 'Original ZIP paths must remain available; rebuild after relocation'}
    atomic_json(manifest, result)
    return {k:v for k,v in result.items() if k != 'entries'}


@contextmanager
def open_virtual_archive(preparation, archive_id):
    """Expose virtual pixels with the exact original calibration/provenance metadata."""
    import icechunk as ic
    from ecore_weather.storage.archives import open_raw
    record = json.loads((Path(preparation) / 'virtual.json').read_text())
    entry = next(r for r in record['entries'] if r['archive_id'] == archive_id)
    path = Path(entry['path'])
    if not path.is_file() or path.stat().st_size != entry['stored_bytes']:
        raise IOError(f'Virtual source is missing or changed: {path}')
    from icechunk.credentials import LocalFileSystemAccess
    source_root = json.loads((Path(preparation)/'selection.json').read_text())['archive_root']
    repo = ic.Repository.open(ic.local_filesystem_storage(record['repository']),
        authorize_virtual_chunk_access={Path(source_root).as_uri().rstrip('/')+'/': LocalFileSystemAccess})
    session = repo.readonly_session(snapshot_id=record['snapshot_id'])
    virtual = xr.open_zarr(session.store, group=entry['group'], consolidated=False,
                           decode_cf=False, mask_and_scale=False, chunks=None)
    try:
        with open_raw(path) as native:
            virtual.attrs = native.attrs.copy()
            for name in native.coords:
                virtual = virtual.assign_coords({name: native[name].load().copy(deep=True)})
            for name in native.variables:
                if name in virtual: virtual[name].attrs = native[name].attrs.copy()
                elif native[name].ndim == 0:
                    virtual[name] = native[name].load().copy(deep=True)
        yield virtual
    finally:
        virtual.close()
