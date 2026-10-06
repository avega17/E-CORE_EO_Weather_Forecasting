"""Metadata-only size summaries for monthly Zarr stores and annual HF backups."""

from __future__ import annotations

import json
from pathlib import Path

from .storage import valid_raw_name


def local_archives(location, source, strict=False):
    """Find completed monthly stores by their shallow layout, never walking chunks."""
    root = Path(location).expanduser()
    if root.name.endswith(('.zarr', '.zarr.zip')):
        candidates = [root.parent / 'complete.json']
    else:
        base = root / source if (root / source).is_dir() else root
        patterns = (
            'roi-*/????/??/complete.json',
            '*/roi-*/????/??/complete.json',
            '*/*/roi-*/????/??/complete.json',
            '*/*/*/roi-*/????/??/complete.json',
        )
        candidates = [path for pattern in patterns for path in base.glob(pattern)]
    rows = []
    for path in sorted(set(candidates)):
        marker = json.loads(path.read_text())
        if marker.get('source') != source or not valid_raw_name(marker.get('raw_path')):
            continue
        archive = path.parent / marker['raw_path']
        if not archive.is_file() and not archive.is_dir():
            if strict: raise IOError(f"Verified archive is missing: {archive}")
            continue
        if archive.is_file():
            stored = archive.stat().st_size
        else:
            stored = marker.get('stored_bytes')
        if marker.get('stored_bytes') is not None and stored != marker['stored_bytes']:
            if strict: raise IOError(f"Verified archive size differs from marker: {archive}")
            continue
        assets = marker.get('assets', [])
        listed_sizes = [asset.get('source_bytes') for asset in assets]
        known_source_bytes = sum(int(value) for value in listed_sizes if value is not None)
        # Do not present a partial sum as though it were the complete original
        # source size for this archive. Totals can still report coverage.
        source_bytes = (known_source_bytes if assets and all(value is not None
                        for value in listed_sizes) else None)
        rows.append({'source': source, 'product': marker.get('product') or path.parent.name,
            'band': marker.get('band'), 'year': path.parent.parent.name,
            'month': path.parent.name, 'stored_bytes': stored,
            'source_bytes': source_bytes, 'observations': marker.get('observations') or len(assets),
            'known_source_bytes': known_source_bytes if assets else None,
            'source_size_coverage': (sum(value is not None for value in listed_sizes) / len(assets)
                                     if assets else 0.0),
            'path': str(archive), 'kind': 'monthly Zarr'})
    return rows


def backup_archives(bundle, archive_rows):
    """Convert one inspected HF bundle to the same size-summary records."""
    rows = []
    for item in archive_rows:
        rows.append({'source': bundle['source'], 'product': bundle['product'],
            'band': None, 'year': item['month'][:4], 'month': item['month'][-2:],
            'stored_bytes': item['stored_bytes'], 'source_bytes': item.get('source_bytes'),
            'known_source_bytes': item.get('source_bytes'),
            'source_size_coverage': 1.0 if item.get('source_bytes') is not None else 0.0,
            'observations': item['observations'], 'path': item['member'],
            'kind': 'monthly member in annual HF backup'})
    return rows


def totals(rows):
    stored = sum(int(row['stored_bytes']) for row in rows if row.get('stored_bytes') is not None)
    known = [row for row in rows if row.get('source_bytes') is not None]
    source = sum(int(row['source_bytes']) for row in known)
    partial = sum(int(row.get('known_source_bytes') or 0) for row in rows)
    return {'archives': len(rows), 'observations': sum(row.get('observations') or 0 for row in rows),
        'stored_bytes': stored, 'source_bytes': source if len(known) == len(rows) and rows else None,
        'known_source_bytes': source,
        'known_asset_source_bytes': partial,
        'known_stored_bytes': sum(int(row['stored_bytes']) for row in known if row.get('stored_bytes') is not None),
        'source_coverage': len(known) / len(rows) if rows else 0.0,
        'listed_asset_size_coverage': (sum(row.get('source_size_coverage', 1.0 if row.get('source_bytes') is not None else 0.0)
            for row in rows) / len(rows) if rows else 0.0)}


def format_bytes(value):
    if value is None:
        return 'unavailable'
    for unit in ('B', 'KiB', 'MiB', 'GiB', 'TiB'):
        if abs(value) < 1024 or unit == 'TiB':
            return f'{value:,.1f} {unit}' if unit != 'B' else f'{value:,} B'
        value /= 1024


def ratio_text(row):
    source, stored = row.get('source_bytes'), row.get('stored_bytes')
    if source is None:
        source = row.get('known_source_bytes')
        stored = row.get('known_stored_bytes', stored)
    if not source:
        return 'unavailable'
    ratio = stored / source
    comparison = ('smaller' if ratio < 1 else 'larger' if ratio > 1 else 'the same size')
    detail = f'{100 * abs(ratio - 1):.1f}% {comparison}' if ratio != 1 else comparison
    return f'{100 * ratio:.1f}% of source ({detail})'
