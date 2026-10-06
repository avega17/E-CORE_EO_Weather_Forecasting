"""Verify and move earlier dated stores to shared paths, without downloading NOAA data."""
import argparse
import time
from pathlib import Path
from ecore_weather import catalog, storage
from ecore_weather.common import write_json


def merge(selection, destination, progress=None):
    destination = str(Path(destination).resolve())
    older = storage._older_local_stores(destination, selection)
    identity = storage.subset_identity(selection)
    root = Path(destination)/storage.product_path(selection)/f'roi-{identity}'
    root.mkdir(parents=True, exist_ok=True)
    write_json(root/'subset.json', {**storage.subset_specification(selection), 'subset_id':identity})
    counts = {'moved':0, 'already_shared':0, 'not_present':0, 'duplicates_removed':0}
    changes = []
    complete = False
    try:
        for index, asset in enumerate(selection.assets):
            stamp = asset.time[:19].replace('-','').replace(':','')
            target = root/stamp[:4]/stamp[4:6]/stamp[6:8]/f'{stamp[9:]}-{asset.id}'
            candidates = older.get(asset.id, ())
            with storage.object_writer(str(root)+'/'+asset.id):
                if not target.exists():
                    moved = storage._adopt_local(candidates, str(target), selection, asset, identity)
                    counts['moved' if moved else 'not_present'] += 1
                else:
                    counts['already_shared'] += 1
                if not target.exists():
                    continue
                marker = storage._read_marker(str(target))
                if not marker or marker.get('subset_id') != identity or marker.get('asset_id') != asset.id:
                    raise ValueError(f'Target identity conflict: {target}')
                with storage.open_raw(target/marker.get('raw_path','raw.zarr')) as ds:
                    if storage.fingerprint(ds) != marker['array_sha256'] or storage.metadata_fingerprint(ds) != marker['metadata_sha256']:
                        raise IOError(f'Target verification failed: {target}')
                    native = ds.copy(deep=False)
                    native.attrs = {k:v for k,v in ds.attrs.items() if k not in ('hourly_slot','hourly_slot_offset_seconds')}
                    native_metadata = storage.metadata_fingerprint(native)
                if marker.get("previous_location"):
                    changes.append({"old": marker["previous_location"], "new": str(target)})
                for candidate in candidates:
                    if not candidate.exists():
                        changes.append({'old':str(candidate), 'new':str(target)})
                        continue
                    with storage.object_writer(candidate):
                        previous = storage._read_marker(str(candidate))
                        if not previous or previous.get('asset_id') != asset.id:
                            continue
                        raw_name = previous.get('raw_path','raw.zarr')
                        if raw_name not in ('raw.zarr','raw.zarr.zip'):
                            raise ValueError('Invalid raw container')
                        with storage.open_raw(candidate/raw_name) as ds:
                            if tuple(ds.attrs.get('requested_bbox', ())) != tuple(selection.bbox):
                                continue
                            if selection.source == 'goes' and 'MCMIP' in selection.product:
                                from ecore_weather.goes import science_variables
                                if set(science_variables(ds)) != {f'CMI_C{b:02d}' for b in selection.bands}:
                                    continue
                            if storage.fingerprint(ds) != previous['array_sha256'] or storage.metadata_fingerprint(ds) != previous['metadata_sha256']:
                                raise IOError(f'Existing duplicate failed verification: {candidate}')
                            native = ds.copy(deep=False)
                            native.attrs = {k:v for k,v in ds.attrs.items() if k not in ('hourly_slot','hourly_slot_offset_seconds')}
                            identical = previous['array_sha256'] == marker['array_sha256'] and storage.metadata_fingerprint(native) == native_metadata
                        if not identical:
                            raise ValueError(f'Different data at {candidate}; retained for review')
                        # Remove only verified duplicate raw and completion metadata; keep unrelated files.
                        import shutil
                        raw = candidate/raw_name
                        if raw.is_dir(): shutil.rmtree(raw)
                        else: raw.unlink()
                        (candidate/'complete.json').unlink()
                        if not any(candidate.iterdir()): candidate.rmdir()
                        counts['duplicates_removed'] += 1
                        changes.append({'old':str(candidate), 'new':str(target)})
            if index % 100 == 0:
                print(counts, flush=True)
                if progress: progress({'root':str(root), 'counts':counts, 'relocations':changes, 'complete':False})
        # Remove only empty ancestors of the stores this run moved/de-duplicated.
        base = Path(destination)/storage.product_path(selection)
        for change in changes:
            path = Path(change['old'])
            if not path.exists(): path = path.parent
            while path != base and path.is_relative_to(base) and path.exists():
                if any(path.iterdir()): break
                path.rmdir()
                path = path.parent
        complete = True
    finally:
        if progress: progress({"root":str(root), "counts":counts, "relocations":changes, "complete":complete})
    return {'root':str(root), 'counts':counts, 'relocations':changes, 'complete':complete}

