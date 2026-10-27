"""Save a selection as two STAC JSON files, independent of source-file count."""

from dataclasses import asdict
import json
import gzip
import os
from pathlib import Path

import pystac

from .common import Asset, Selection, utc, write_json


def record_corrupt_source(selection, asset, error):
    """Attach exceptional scientific coverage evidence; keep the STAC object.

    A range failure, missing file or arbitrary error is insufficient. The caller
    must have decoded a complete object whose MD5 ETag was independently matched.
    """
    from .goes import ConfirmedCorruptSourceError
    if not isinstance(error, ConfirmedCorruptSourceError) or asset not in selection.assets:
        raise ValueError('A selected, checksum-confirmed corrupt source is required')
    fact = {'status':'confirmed_corrupt_source', 'asset_id':asset.id,
        'source_url':asset.url, 'time':asset.time, 'etag':asset.etag,
        'source_bytes':asset.size, 'product':selection.product,
        'region':list(selection.bbox), 'error':str(error),
        'integrity_evidence':error.evidence}
    selection.discovery_facts = dict(getattr(selection, 'discovery_facts', {}))
    facts = selection.discovery_facts.setdefault('source_issues', [])
    facts[:] = [row for row in facts if row.get('asset_id') != asset.id]
    facts.append(fact)
    confirmed_source_issues(selection)  # Reject incomplete or unscoped evidence.
    return fact


def confirmed_source_issues(selection):
    """Return only exact-source, exact-ROI confirmed corruption records."""
    assets = {asset.id:asset for asset in selection.assets}
    issues = {}
    for issue in getattr(selection, 'discovery_facts', {}).get('source_issues', []):
        if issue.get('status') != 'confirmed_corrupt_source':continue
        asset = assets.get(issue.get('asset_id'));evidence=issue.get('integrity_evidence', {})
        proof_etag=evidence.get('etag') if evidence.get('verification') == 'full-object-size-and-multipart-etag-md5' else evidence.get('md5')
        if (asset is None or issue.get('source_url') != asset.url or issue.get('etag') != asset.etag
                or issue.get('source_bytes') != asset.size or issue.get('time') != asset.time
                or issue.get('product') != selection.product or issue.get('region') != list(selection.bbox)
                or evidence.get('download_bytes') != asset.size or proof_etag != asset.etag.strip('"').lower()
                or evidence.get('verification') not in (
                    'full-object-size-and-single-part-etag-md5', 'full-object-size-and-multipart-etag-md5')):
            raise ValueError('Corrupt-source evidence does not match the selected source and ROI')
        issues[asset.id] = issue
    return issues


def save_selection(selection: Selection, directory, index_results=True, compressed=True):
    directory = Path(directory)
    request = asdict(selection)
    request.pop("assets")
    # A tolerated observation may precede the requested nominal start. STAC's
    # extent must contain the actual observations, while ecore:request retains
    # the user's half-open slot interval.
    earliest = min([utc(selection.start), *(utc(a.time) for a in selection.assets)])
    latest = max([utc(selection.end), *(utc(a.end_time or a.time) for a in selection.assets)])
    extent = pystac.Extent(pystac.SpatialExtent([[-180, -90, 180, 90]]),
                          pystac.TemporalExtent([[earliest, latest]]))
    discovery_facts = getattr(selection, "discovery_facts", None)
    collection = pystac.Collection(selection.id,
        f"Selected NOAA {selection.source.upper()} files; requested crop and times are explicit metadata.",
        extent=extent, license="other", extra_fields={"ecore:request": request,
            **({"ecore:discovery_facts": discovery_facts} if discovery_facts is not None else {})})
    collection.add_link(pystac.Link("license", "https://registry.opendata.aws/noaa-mrms-pds/"
                                   if selection.source == "mrms" else "https://registry.opendata.aws/noaa-goes/"))
    filename = 'items.json.gz' if compressed else 'items.json'
    collection.add_link(pystac.Link("items", './'+filename, media_type="application/geo+json"))
    collection.add_link(pystac.Link("via", './'+filename, media_type="application/geo+json",
                                   title="Executable selection manifest"))
    items = []
    for asset in selection.assets:
        item = pystac.Item(asset.id, geometry=None, bbox=None, datetime=utc(asset.time),
                          properties={"ecore:source": asdict(asset)})
        item.add_asset("source", pystac.Asset(asset.url,
            media_type="application/gzip" if asset.key.endswith(".gz") else "application/x-netcdf",
            roles=["data"], extra_fields={"file:size": asset.size, "ecore:etag": asset.etag}))
        items.append(item)
    manifest = pystac.ItemCollection(items, extra_fields={"ecore:request": request,
        **({"ecore:discovery_facts": discovery_facts} if discovery_facts is not None else {})})
    directory.mkdir(parents=True, exist_ok=True)
    if compressed:
        temporary = directory/(filename+'.partial')
        with gzip.open(temporary, 'wt', encoding='utf-8') as stream:
            json.dump(manifest.to_dict(), stream, separators=(',', ':'))
        os.replace(temporary, directory/filename)
    else:
        write_json(directory / filename, manifest.to_dict())
    pending=directory/'collection.json.tmp'
    write_json(pending, collection.to_dict())
    with pending.open('rb') as stream:os.fsync(stream.fileno())
    os.replace(pending,directory/'collection.json')
    collection_path = directory / "collection.json"
    if index_results:
        try:
            from .index import record_selection
            record_selection(selection, collection_path)
        except ImportError:
            # Catalog creation remains usable in lightweight environments.
            pass
    return collection_path


def _selection(request, assets):
    request = dict(request)
    for field in ("bbox", "bands", "expected_times", "hourly_matches"):
        if field in request:
            request[field] = tuple(request[field])
    return Selection(**request, assets=assets)


def load_selection(path):
    path = Path(path)
    with (gzip.open(path, 'rt', encoding='utf-8') if path.suffix == '.gz' else path.open()) as stream:
        document = json.load(stream)
    if document.get("type") == "FeatureCollection":
        selection = _selection(document["ecore:request"],
            [Asset(**item["properties"]["ecore:source"]) for item in document["features"]])
        if "ecore:discovery_facts" in document:
            selection.discovery_facts = document["ecore:discovery_facts"]
        return selection
    if document.get("type") != "Collection":
        request = dict(document)
        return _selection({k: v for k, v in request.items() if k != "assets"},
                          [Asset(**a) for a in request["assets"]])
    for link in document.get("links", []):
        if link["rel"] == "items":
            return load_selection(path.parent / link["href"])
    # Read earlier per-item static catalogs, but do not generate them anymore.
    collection = pystac.Collection.from_file(str(path))
    return _selection(collection.extra_fields["ecore:request"],
                      [Asset(**item.properties["ecore:source"]) for item in collection.get_items()])
