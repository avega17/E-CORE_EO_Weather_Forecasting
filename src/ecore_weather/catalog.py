"""Save a selection as two STAC JSON files, independent of source-file count."""

from dataclasses import asdict
import json
import gzip
import os
from pathlib import Path

import pystac

from .common import Asset, Selection, utc, write_json


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
    write_json(directory / "collection.json", collection.to_dict())
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
