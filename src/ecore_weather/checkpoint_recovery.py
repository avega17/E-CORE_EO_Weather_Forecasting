"""Recheck scientific scan batches after interruption; replay only untrusted tails."""
from pathlib import Path
import hashlib
import json
import tarfile
import time
import os
import numpy as np
import zarr


def recover(parent, rows):
    parent=Path(parent);files=sorted(parent.glob('batch-*.json'))
    if not files:return None
    root=zarr.open_group(parent/'raw.zarr',mode='r')
    count=0;problem=None
    for file in files:
        begin=count
        try:
            batch=json.loads(file.read_text())
            if not batch.get('rows'):raise ValueError('Empty scan batch')
            for item in batch['rows']:
                if item['index']!=count or item['row']['asset_id']!=rows[count]['asset_id']:
                    raise ValueError('Non-contiguous or changed scan identity')
                for name,expected in item['checksums'].items():
                    actual=hashlib.sha256(np.ascontiguousarray(root[name][count]).tobytes()).hexdigest()
                    if actual!=expected:raise ValueError(f'Raw {name} checksum differs at frame {count}')
                count+=1
        except (ValueError,KeyError,IndexError,OSError,RuntimeError) as exc:
            count=begin;problem=f'{file.name}: {type(exc).__name__}: {exc}';break
    if problem is None:return None
    # Preserve the old metadata and untrusted chunks before replaying them.
    evidence=Path(os.getenv('ECORE_RECOVERY_EVIDENCE','results/evidence/checkpoint-recovery'));evidence.mkdir(parents=True,exist_ok=True)
    destination=evidence/f'{parent.name}-tail-{count:06d}-{time.time_ns()}.tar.gz'
    tail=[p for p in files if int(p.stem.split('-')[1])>=count]
    original=parent/'progress.json'
    with tarfile.open(destination,'w:gz') as backup:
        for file in [*tail,original]:
            if file.is_file():backup.add(file,arcname=str(file.relative_to(parent)))
        if (parent/'raw.zarr'/'zarr.json').is_file():backup.add(parent/'raw.zarr'/'zarr.json',arcname='raw.zarr/zarr.json')
        for name in root.array_keys():
            metadata=parent/'raw.zarr'/name/'zarr.json'
            if metadata.is_file():backup.add(metadata,arcname=str(metadata.relative_to(parent)))
            chunks=parent/'raw.zarr'/name/'c'
            if not chunks.is_dir():continue
            if name in ('x','y','time','latitude','longitude','variable'):
                backup.add(chunks,arcname=str(chunks.relative_to(parent)))
                continue
            for path in chunks.iterdir():
                if path.name.isdigit() and int(path.name)>=count and name.startswith(('CMI_','DQF_','measurement','bitmap_')):
                    backup.add(path,arcname=str(path.relative_to(parent)))
    for file in tail:file.unlink()
    state=json.loads(original.read_text()) if original.is_file() else {}
    before=state.get('next_index');state['next_index']=count
    report={'previous_checkpointed':before,'verified_prefix':count,'replay_observations':max(0,(before or count)-count),
        'reason':problem,'preserved_tail':str(destination)}
    from .goes_monthly import _checkpoint
    _checkpoint(original,{**state,'checkpoint_recovery':report})
    _checkpoint(evidence/f'{parent.name}.json',report)
    return report
