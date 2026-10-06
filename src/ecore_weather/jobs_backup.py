"""Back up a verified MRMS year as one compressed-container object per product."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import json
import re
from pathlib import Path
import sys
import time
import zipfile



from ecore_weather import mrms
from ecore_weather.common import write_json
from ecore_weather.hf_storage import BucketWriter, bucket_writer
from ecore_weather.storage import configured_bucket

BLOCK_BYTES = 8 * 1024 * 1024


def _sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(BLOCK_BYTES), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_stored_member(archive, path, member_name):
    """Copy a member without recompressing monthly ZIPs and hash it in one pass."""
    path = Path(path)
    info = zipfile.ZipInfo(member_name)
    info.compress_type = zipfile.ZIP_STORED if path.suffix == ".zip" else zipfile.ZIP_DEFLATED
    info.external_attr = 0o100644 << 16
    info.create_system = 3
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as source, archive.open(info, "w", force_zip64=True) as target:
        while True:
            block = source.read(BLOCK_BYTES)
            if not block:
                break
            target.write(block)
            digest.update(block)
            size += len(block)
    return {"path": member_name, "size": size, "sha256": digest.hexdigest()}


def _collect_year(year, source_root, study_output):
    """Read study checkpoints and verify each local archive's manifest."""
    source_root = Path(source_root).resolve()
    study_output = Path(study_output)
    grouped = {product: {} for product in mrms.DEFAULT_PRODUCTS}
    selections = {product: [] for product in mrms.DEFAULT_PRODUCTS}
    # The study ends at 2026-07-01, so its final year bundle covers H1 only.
    month_count = 6 if year == 2026 else 12
    for month in range(1, month_count + 1):
        month_key = f"{year}-{month:02d}"
        for product in mrms.DEFAULT_PRODUCTS:
            checkpoint = study_output / "months" / month_key / f"{product}.json"
            if not checkpoint.is_file():
                raise FileNotFoundError(f"Missing local product-month checkpoint: {checkpoint}")
            row = json.loads(checkpoint.read_text())
            if row.get("month", month_key) != month_key or row.get("product", product) != product:
                raise ValueError(f"Checkpoint period does not match its path: {checkpoint}")
            if row.get("status") == "unavailable":
                continue
            if row.get("status") != "complete" or not row.get("archives"):
                raise ValueError(f"Local product-month is incomplete: {checkpoint}")
            selection_path = checkpoint.parent / f"{product}-selection" / "items.json.gz"
            if not selection_path.is_file():selection_path=selection_path.with_suffix("")
            if not selection_path.is_file():
                raise FileNotFoundError(f"Saved source selection is missing: {selection_path}")
            selected=json.loads(__import__("gzip").open(selection_path,"rt").read()) if selection_path.suffix==".gz" else json.loads(selection_path.read_text())
            features = {item["id"]: item for item in selected.get("features", [])}
            selected_ids = set(features)
            invalid_sources = row.get("invalid_source_files", [])
            invalid_ids = {item["asset_id"] for item in invalid_sources}
            if len(selected_ids) != row.get("listed_slots", row.get("matched_slots")):
                raise ValueError(f"STAC listing count differs from checkpoint: {selection_path}")
            if len(invalid_ids) != len(invalid_sources) or not invalid_ids.issubset(selected_ids):
                raise ValueError(f"Invalid-source list differs from STAC selection: {checkpoint}")
            for invalid in invalid_sources:
                source = features[invalid["asset_id"]].get("properties", {}).get("ecore:source", {})
                if (invalid.get("reason") != "zero_byte_noaa_object" or invalid.get("size") != 0 or
                    source.get("size") != 0 or source.get("key") != invalid.get("key") or
                    source.get("time") != invalid.get("observation_time") or
                    source.get("etag") != invalid.get("etag")):
                    raise ValueError(f"Invalid-source checkpoint does not match STAC: {checkpoint}")
            if len(selected_ids) - len(invalid_ids) != row.get("matched_slots"):
                raise ValueError(f"Valid source count differs from checkpoint: {checkpoint}")
            selections[product].append({"study_month": month_key,
                "member": f"selections/{month_key}/{product}-items.json"+(".gz" if selection_path.suffix==".gz" else ""),
                "path": selection_path})
            # The STAC catalog retains every NOAA listing; include the matching
            # checkpoint so the yearly backup also explains any unarchived empty
            # source objects.
            selections[product].append({"study_month": month_key,
                "member": f"coverage/{month_key}/{product}-checkpoint.json",
                "path": checkpoint})
            selected_archives = []
            for archive_name in row["archives"]:
                archive = Path(archive_name).resolve()
                if not archive.is_relative_to(source_root) or not archive.is_file():
                    raise FileNotFoundError(f"Selected local archive is missing/outside source root: {archive}")
                marker_path = archive.parent / "complete.json"
                if not marker_path.is_file():
                    raise FileNotFoundError(f"Archive completion marker is missing: {marker_path}")
                marker = json.loads(marker_path.read_text())
                if (marker.get("source") != "mrms" or marker.get("product") != product or
                        marker.get("raw_path") != archive.name or
                        marker.get("observations") != len(marker.get("asset_ids", [])) or
                        archive.stat().st_size != marker.get("stored_bytes") or
                        not marker.get("archive_sha256")):
                    raise ValueError(f"Local archive marker does not describe the ZIP: {marker_path}")
                if _sha256(archive) != marker["archive_sha256"]:
                    raise ValueError(f"Local archive bytes differ from the completion marker: {archive}")
                relative = archive.relative_to(source_root).as_posix()
                member = f"data/{relative}"
                marker_member = f"metadata/{relative.removesuffix(archive.name)}complete.json"
                entry = grouped[product].setdefault(relative, {
                    "archive": archive, "marker_path": marker_path, "marker": marker,
                    "member": member, "marker_member": marker_member,
                    "selected_by": []})
                if entry["marker"].get("asset_ids") != marker.get("asset_ids"):
                    raise ValueError(f"Local archive manifest changed within selection: {archive}")
                entry["selected_by"].append(month_key)
                selected_archives.append(set(marker["asset_ids"]))
            archived_ids = set().union(*selected_archives) if selected_archives else set()
            if not (selected_ids - invalid_ids).issubset(archived_ids):
                raise ValueError(f"A selected source item is absent from local archives: {checkpoint}")
            if archived_ids.intersection(invalid_ids):
                raise ValueError(f"An unavailable empty source object appears in an archive: {checkpoint}")
    return grouped, selections


def _build_bundle(year, product, entries, selections, output_dir, source_root):
    if not entries:
        raise ValueError(f"No local archives were selected for {product} in {year}")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    temporary = output_dir / f".{product}-{year}.building.zip"
    final = output_dir / f"{product}-{year}.zip"
    members = []
    archive_rows = []
    try:
        with zipfile.ZipFile(temporary, mode="w", allowZip64=True) as bundle:
            for relative, entry in sorted(entries.items()):
                archive_info = _write_stored_member(bundle, entry["archive"], entry["member"])
                if (archive_info["size"] != entry["marker"]["stored_bytes"] or
                        archive_info["sha256"] != entry["marker"]["archive_sha256"]):
                    raise IOError(f"Monthly ZIP differs from its verified marker: {entry['archive']}")
                marker_info = _write_stored_member(bundle, entry["marker_path"], entry["marker_member"])
                members.extend((archive_info, marker_info))
                archive_rows.append({"local_path": relative, "member": entry["member"],
                    "marker_member": entry["marker_member"], "selected_by": entry["selected_by"],
                    "observations": entry["marker"]["observations"],
                    "asset_ids": entry["marker"]["asset_ids"],
                    "stored_bytes": archive_info["size"], "sha256": archive_info["sha256"]})
            for selection in selections:
                info = _write_stored_member(bundle, selection["path"], selection["member"])
                members.append(info)
            period_end = f"{year}-07-01T00:00:00Z" if year == 2026 else f"{year+1}-01-01T00:00:00Z"
            manifest = {"schema": "ecore-mrms-year-bundle-v1", "source": "mrms",
                "year": year, "product": product,
                "requested_study_period": [f"{year}-01-01T00:00:00Z", period_end],
                "local_source_root": str(Path(source_root).resolve()),
                "archive_count": len(archive_rows), "archives": archive_rows,
                "members": members}
            manifest_bytes = (json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n").encode()
            bundle.writestr("year_manifest.json", manifest_bytes,
                            compress_type=zipfile.ZIP_DEFLATED)
        with zipfile.ZipFile(temporary) as check:
            names = set(check.namelist())
            if "year_manifest.json" not in names or len(names) != len(members) + 1:
                raise IOError(f"Year package structure is incomplete: {temporary}")
            read_manifest = json.loads(check.read("year_manifest.json"))
            if read_manifest.get("archive_count") != len(archive_rows):
                raise IOError("Year package manifest count differs")
        temporary.replace(final)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    digest = _sha256(final)
    return {"source": "mrms", "year": year, "product": product,
        "path": str(final.resolve()), "size": final.stat().st_size,
        "sha256": digest, "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "archive_count": len(archive_rows), "observation_count": sum(x["observations"] for x in archive_rows),
        "archives": archive_rows, "member_count": len(members) + 1,
        "container_compression": "ZIP_STORED for already-compressed monthly ZIPs"}


def _upload_one(bundle, remote_root):
    from boto3.s3.transfer import TransferConfig

    writer = BucketWriter(remote_root)
    product = bundle["product"]
    year = bundle["year"]
    relative = f"mrms/{product}/{year}/{product}-{year}-{bundle['sha256'][:16]}.zip"
    marker_relative = f"mrms/{product}/{year}/complete.json"
    marker = {"schema": "ecore-mrms-year-backup-v1", "source": "mrms",
        "product": product, "year": bundle["year"], "raw_path": relative.rsplit("/", 1)[-1],
        "archive_count": bundle["archive_count"], "observation_count": bundle["observation_count"],
        "stored_bytes": bundle["size"], "archive_sha256": bundle["sha256"],
        "manifest_sha256": bundle["manifest_sha256"], "updated_at": datetime.now(timezone.utc).isoformat()}
    existing = writer.marker(marker_relative.rsplit("/", 1)[0])
    if existing and existing.get("archive_sha256") == bundle["sha256"]:
        head = writer.client.head_object(Bucket=writer.config["bucket"], Key=writer.key(relative))
        if head.get("ContentLength") != bundle["size"]:
            raise IOError(f"Existing HF package size differs from its completion marker: {product}")
        readback_started = time.perf_counter()
        response = writer.client.get_object(Bucket=writer.config["bucket"], Key=writer.key(relative))
        digest, readback_size = hashlib.sha256(), 0
        body = response["Body"]
        try:
            for block in body.iter_chunks(chunk_size=BLOCK_BYTES):
                if block:
                    digest.update(block)
                    readback_size += len(block)
        finally:
            body.close()
        if readback_size != bundle["size"] or digest.hexdigest() != bundle["sha256"]:
            raise IOError(f"Existing HF yearly package read-back hash differs: {product}")
        return {**marker, "status": "reused", "remote_key": writer.key(relative),
            "upload_seconds": 0.0,
            "readback_seconds": time.perf_counter() - readback_started}
    transfer = TransferConfig(multipart_threshold=2 * 1024**3,
        multipart_chunksize=2 * 1024**3, max_concurrency=1, use_threads=True)
    started = time.perf_counter()
    with bucket_writer(writer.bucket_id, scope=marker_relative):
        writer.client.upload_file(bundle["path"], writer.config["bucket"],
            writer.key(relative), Config=transfer)
        upload_seconds = time.perf_counter() - started
        head = writer.client.head_object(Bucket=writer.config["bucket"], Key=writer.key(relative))
        if head.get("ContentLength") != bundle["size"]:
            raise IOError(f"HF package size differs after upload: {product}")
        readback_started = time.perf_counter()
        response = writer.client.get_object(Bucket=writer.config["bucket"], Key=writer.key(relative))
        digest = hashlib.sha256()
        readback_size = 0
        body = response["Body"]
        try:
            for block in body.iter_chunks(chunk_size=BLOCK_BYTES):
                if block:
                    digest.update(block)
                    readback_size += len(block)
        finally:
            body.close()
        if readback_size != bundle["size"] or digest.hexdigest() != bundle["sha256"]:
            raise IOError(f"HF yearly package read-back hash differs: {product}")
        writer.client.put_object(Bucket=writer.config["bucket"],
            Key=writer.key(marker_relative),
            Body=json.dumps(marker, sort_keys=True).encode(), ContentType="application/json")
    return {**marker, "status": "saved", "remote_key": writer.key(relative),
        "upload_seconds": upload_seconds,
        "readback_seconds": time.perf_counter() - readback_started}


def _list_prefix(writer, prefix):
    token = None
    while True:
        args = {"Bucket": writer.config["bucket"], "Prefix": writer.key(prefix.rstrip("/") + "/")}
        if token:
            args["ContinuationToken"] = token
        page = writer.client.list_objects_v2(**args)
        yield from page.get("Contents", [])
        token = page.get("NextContinuationToken")
        if not token:
            return


def _remove_replaced_months(bundles, source_root, monthly_root):
    """Remove only the exact, manifest-matched monthly prefixes after year read-back."""
    writer = BucketWriter(monthly_root)
    removed, preserved = [], []
    for bundle in bundles:
        for archive in bundle["archives"]:
            local_marker = json.loads((Path(source_root) / archive["local_path"]).parent.joinpath("complete.json").read_text())
            prefix = archive["local_path"].rsplit("/", 1)[0]
            objects = list(_list_prefix(writer, prefix))
            if not objects:
                continue
            remote_marker = writer.marker(prefix)
            if remote_marker:
                if (remote_marker.get("source") != "mrms" or
                        remote_marker.get("product") != bundle["product"] or
                        remote_marker.get("asset_ids") != local_marker.get("asset_ids")):
                    preserved.append({"prefix": prefix, "reason": "remote marker differs from local manifest"})
                    continue
            else:
                # A killed month writer may leave uncommitted raw-<uuid>.zarr objects.
                allowed = re.compile(r"^raw-[0-9a-f]{32}\.zarr/")
                prefix_key = writer.key(prefix.rstrip("/") + "/") + "/"
                names = [item["Key"].removeprefix(prefix_key).lstrip("/") for item in objects]
                if not names or not all(allowed.match(name) for name in names):
                    preserved.append({"prefix": prefix, "reason": "unmarked prefix contains unknown objects"})
                    continue
            with bucket_writer(writer.bucket_id, scope=prefix):
                keys = [{"Key": item["Key"]} for item in objects]
                for start in range(0, len(keys), 1000):
                    writer.client.delete_objects(Bucket=writer.config["bucket"],
                        Delete={"Objects": keys[start:start+1000], "Quiet": True})
            removed.append({"prefix": prefix, "objects": len(objects)})
    return removed, preserved


def _save_cleanup_history(output, year, removed, preserved):
    path = Path(output) / "cleanup-history.json"
    current = json.loads(path.read_text()) if path.is_file() else {}
    by_prefix = {item["prefix"]: item for item in current.get("removed_month_prefixes", [])}
    by_prefix.update({item["prefix"]: item for item in removed})
    current.update({"year": year, "verified_year_bundle_count": len(mrms.DEFAULT_PRODUCTS),
        "removed_month_prefixes": sorted(by_prefix.values(), key=lambda item: item["prefix"]),
        "preserved_month_prefixes": preserved,
        "local_archives_modified": False})
    write_json(path, current)
    return current


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=2021)
    parser.add_argument("--source-root", default="/mnt/p/ecore_eo_datasets")
    parser.add_argument("--study-output", default="results/study-mrms")
    parser.add_argument("--output")
    parser.add_argument("--remote-root", help="HF yearly-backup prefix")
    parser.add_argument("--monthly-root", help="Existing HF month-level mirror prefix")
    parser.add_argument("--writers", type=int, default=4,
        help="Independent MRMS product-year package uploads (default 4)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--cleanup-only", action="store_true",
        help="After checking all saved yearly completion markers, remove only replaced month prefixes")
    args = parser.parse_args(argv)
    if args.writers < 1:
        parser.error("--writers must be positive")
    source_root = Path(args.source_root).resolve()
    if not source_root.is_dir():
        parser.error(f"Local MRMS archive root is missing: {source_root}")
    base_root = f"hf://buckets/{configured_bucket()}/noaa-subsets"
    remote_root = args.remote_root or base_root + "/yearly-v1"
    monthly_root = args.monthly_root or base_root
    if args.output is None:
        args.output = str(Path(args.study_output)/"backups"/str(args.year))
    output = Path(args.output)
    if args.cleanup_only:
        saved = json.loads((output / "summary.json").read_text())
        if saved.get("status") != "complete" or saved.get("year") != args.year:
            parser.error("--cleanup-only requires a completed summary for this year")
        writer = BucketWriter(remote_root)
        if {row.get("product") for row in saved.get("bundles", [])} != set(mrms.DEFAULT_PRODUCTS):
            parser.error("Saved summary does not contain every default product bundle")
        cleanup_bundles = []
        for row in saved["bundles"]:
            marker = writer.marker(f"mrms/{row['product']}/{args.year}")
            if (not marker or marker.get("source") != "mrms" or
                    marker.get("product") != row["product"] or marker.get("year") != args.year or
                    marker.get("archive_sha256") != row.get("archive_sha256") or
                    marker.get("stored_bytes") != row.get("stored_bytes")):
                parser.error(f"Remote yearly marker does not match saved report: {row['product']}")
            head = writer.client.head_object(Bucket=writer.config["bucket"],
                Key=row["remote_key"])
            if head.get("ContentLength") != row["stored_bytes"]:
                parser.error(f"Remote yearly object size differs from saved report: {row['product']}")
            local_bundle = json.loads((output / f"{row['product']}-local-bundle.json").read_text())
            if (local_bundle.get("sha256") != row.get("archive_sha256") or
                    local_bundle.get("size") != row.get("stored_bytes")):
                parser.error(f"Local bundle report differs from the uploaded object: {row['product']}")
            cleanup_bundles.append(local_bundle)
        removed, preserved = _remove_replaced_months(cleanup_bundles, source_root, monthly_root)
        history = _save_cleanup_history(output, args.year, removed, preserved)
        saved["replaced_month_prefixes"] = history["removed_month_prefixes"]
        saved["preserved_month_prefixes"] = preserved
        write_json(output / "summary.json", saved)
        print(json.dumps({"status": "complete", "year": args.year,
            "replaced_month_prefix_count": len(removed),
            "preserved_month_prefix_count": len(preserved)}, indent=2))
        return 0
    groups, selections = _collect_year(args.year, source_root, args.study_output)
    plans = []
    for product in mrms.DEFAULT_PRODUCTS:
        files = sum(Path(entry["archive"]).stat().st_size for entry in groups[product].values())
        plans.append({"product": product, "month_archive_count": len(groups[product]),
            "local_month_zip_bytes": files,
            "remote_key": f"mrms/{product}/{args.year}/{product}-{args.year}-<sha16>.zip"})
    if args.dry_run:
        print(json.dumps({"year": args.year, "source_root": str(source_root),
            "remote_root": remote_root, "monthly_root": monthly_root,
            "writers": args.writers, "products": plans,
            "replace_monthly_after_all_year_bundles_verify": True}, indent=2))
        return 0
    output.mkdir(parents=True, exist_ok=True)
    bundles = []
    for product in mrms.DEFAULT_PRODUCTS:
        row = _build_bundle(args.year, product, groups[product], selections[product],
                            output, source_root)
        write_json(output / f"{product}-local-bundle.json", row)
        bundles.append(row)
        print(f"Packed {product}: {row['archive_count']} month stores, "
              f"{row['size']} bytes", flush=True)
    uploaded, errors = [], []
    with ThreadPoolExecutor(max_workers=args.writers, thread_name_prefix="hf-year") as pool:
        futures = {pool.submit(_upload_one, bundle, remote_root): bundle["product"]
                   for bundle in bundles}
        for future in as_completed(futures):
            product = futures[future]
            try:
                row = future.result()
            except Exception as error:
                errors.append({"product": product, "error": f"{type(error).__name__}: {error}"})
                continue
            uploaded.append(row)
            write_json(output / f"{product}-remote-bundle.json", row)
            print(f"Uploaded and read-back verified {product}: "
                  f"{row['stored_bytes']} bytes in {row['upload_seconds']:.1f}s upload + "
                  f"{row['readback_seconds']:.1f}s read-back", flush=True)
    if errors:
        write_json(output / "failure.json", {"errors": errors, "uploaded": uploaded,
            "monthly_prefixes_preserved": True})
        return 1
    removed, preserved = _remove_replaced_months(bundles, source_root, monthly_root)
    history = _save_cleanup_history(output, args.year, removed, preserved)
    summary = {"status": "complete", "source": "mrms", "year": args.year,
        "writers": args.writers, "remote_root": remote_root,
        "bundles": uploaded, "replaced_month_prefixes": history["removed_month_prefixes"],
        "preserved_month_prefixes": preserved,
        "note": "Local monthly DAS archives were not modified."}
    write_json(output / "summary.json", summary)
    print(json.dumps({"status": summary["status"], "year": args.year,
        "bundle_count": len(uploaded), "replaced_month_prefix_count": len(removed),
        "preserved_month_prefix_count": len(preserved)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


def _year_months(year):
    return range(1,7 if year==2026 else 13)
