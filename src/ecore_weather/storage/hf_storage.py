"""Coordinated HF batch transfers; keep filesystem compatibility out of writes."""

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
import time

_LOCKS = {}
_LOCKS_GUARD = threading.Lock()


def s3_credentials():
    """Return gateway credentials without ever logging secret values."""
    from dotenv import load_dotenv
    load_dotenv()
    access = os.getenv("HF_S3_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID")
    secret = os.getenv("HF_S3_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
    if not access or not secret:
        raise ValueError("HF bucket writes require S3 gateway access and secret keys in the environment.")
    return access, secret


def s3_configuration(bucket_id):
    """Build the HF S3 endpoint and bare bucket name expected by S3 clients."""
    from dotenv import load_dotenv
    load_dotenv()
    parts = bucket_id.strip("/").split("/", 1)
    if len(parts) != 2 or not all(parts):
        raise ValueError("HF_BUCKET_NAME must be namespace/bucket.")
    namespace, bucket = parts
    endpoint = os.getenv("HF_S3_ENDPOINT", f"https://s3.hf.co/{namespace}").rstrip("/")
    access, secret = s3_credentials()
    return {"namespace": namespace, "bucket": bucket, "endpoint_url": endpoint,
            "region_name": "us-east-1", "aws_access_key_id": access,
            "aws_secret_access_key": secret}


def s3_client(bucket_id):
    """Create a boto3 client with HF's path-style and checksum requirements."""
    from botocore.config import Config
    import boto3
    config = s3_configuration(bucket_id)
    credentials = {k: config[k] for k in ("aws_access_key_id", "aws_secret_access_key")}
    return boto3.client("s3", endpoint_url=config["endpoint_url"],
        region_name=config["region_name"], **credentials,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path",
            "multipart_threshold": 2 * 1024**3, "multipart_chunksize": 2 * 1024**3},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
            retries={"max_attempts": 6, "mode": "adaptive"},
            max_pool_connections=4))


class BucketWriter:
    """Single-coordinator writer for a Hugging Face Storage Bucket via S3."""

    def __init__(self, root, client=None):
        self.root = root.removeprefix("hf://buckets/").strip("/")
        parts = self.root.split("/", 2)
        if len(parts) < 2:
            raise ValueError("Expected hf://buckets/namespace/bucket/path.")
        self.bucket_id = "/".join(parts[:2])
        self.prefix = parts[2] if len(parts) == 3 else ""
        self.config = s3_configuration(self.bucket_id)
        self.client = client or s3_client(self.bucket_id)
        self.upload_bytes = 0
        self.upload_seconds = 0.0
        self.readback_bytes = 0

    def key(self, relative=""):
        return "/".join(p.strip("/") for p in (self.prefix, relative) if p.strip("/"))

    def check(self):
        self.client.head_bucket(Bucket=self.config["bucket"])

    def marker(self, relative=""):
        key = self.key(f"{relative.rstrip('/')}/complete.json" if relative else "complete.json")
        try:
            value = self.client.get_object(Bucket=self.config["bucket"], Key=key)
            return json.loads(value["Body"].read())
        except Exception as error:
            code = getattr(error, "response", {}).get("Error", {}).get("Code")
            if code in {"NoSuchKey", "404", "NotFound"}:
                return None
            raise

    def resume(self, relative, asset_id, schema_version, subset_id=None):
        marker = self.marker(relative)
        if not marker or marker.get("asset_id") != asset_id or marker.get("raw_schema_version") != schema_version:
            return None
        if subset_id is not None and marker.get("subset_id") != subset_id:
            return None
        try:
            response = self.client.head_object(Bucket=self.config["bucket"],
                Key=self.key(f"{relative.rstrip('/')}/{marker['raw_path']}"))
        except Exception:
            return None
        if response.get("ContentLength") != marker.get("stored_bytes"):
            return None
        return marker.get("raw_path")

    def publish(self, staging, relative, marker):
        """Upload payload, read it back for validation, then write completion."""
        from ecore_weather.storage.archives import fingerprint_streaming, metadata_fingerprint, open_raw

        staging = Path(staging)
        raw_path = marker.get("raw_path", "raw.zarr.zip")
        source = staging / raw_path
        if not source.is_file():
            raise FileNotFoundError(source)
        key = self.key(f"{relative.rstrip('/')}/{raw_path}")
        with bucket_writer(self.bucket_id):
            before = time.perf_counter()
            self.client.upload_file(str(source), self.config["bucket"], key)
            marker["stored_bytes"] = source.stat().st_size
            self.upload_bytes += marker["stored_bytes"]
            self.upload_seconds += time.perf_counter() - before

            with tempfile.TemporaryDirectory(prefix="ecore-hf-readback-") as temp:
                downloaded = Path(temp) / raw_path
                self.client.download_file(self.config["bucket"], key, str(downloaded))
                self.readback_bytes += downloaded.stat().st_size
                with open_raw(downloaded) as actual:
                    if (fingerprint_streaming(actual) != marker["array_sha256"] or
                            metadata_fingerprint(actual) != marker["metadata_sha256"]):
                        raise IOError("HF S3 read-back differs from the source arrays or metadata.")
            marker_key = self.key(f"{relative.rstrip('/')}/complete.json")
            self.client.put_object(Bucket=self.config["bucket"], Key=marker_key,
                                   Body=json.dumps(marker, sort_keys=True).encode(),
                                   ContentType="application/json")

    def put_json(self, relative, value):
        self.client.put_object(Bucket=self.config["bucket"], Key=self.key(relative),
                               Body=json.dumps(value, sort_keys=True).encode(),
                               ContentType="application/json")

    def publish_month(self, archive_path, relative, marker):
        """Publish one monthly ZIP, verify a complete read-back, then commit its marker."""
        from ecore_weather.storage.archives import fingerprint_streaming, metadata_fingerprint, open_raw
        archive_path = Path(archive_path)
        key = self.key(relative)
        with bucket_writer(self.bucket_id):
            start = time.perf_counter()
            self.client.upload_file(str(archive_path), self.config["bucket"], key)
            upload_seconds = time.perf_counter() - start
            self.upload_seconds += upload_seconds
            self.upload_bytes += archive_path.stat().st_size
            with tempfile.TemporaryDirectory(prefix="ecore-hf-month-readback-") as temp:
                downloaded = Path(temp) / archive_path.name
                self.client.download_file(self.config["bucket"], key, str(downloaded))
                self.readback_bytes += downloaded.stat().st_size
                if marker.get("archive_sha256"):
                    digest = hashlib.sha256()
                    with downloaded.open("rb") as stream:
                        for block in iter(lambda: stream.read(8*1024*1024), b""):
                            digest.update(block)
                    if digest.hexdigest() != marker["archive_sha256"]:
                        raise IOError("HF month read-back differs from the verified local ZIP.")
                    with open_raw(downloaded) as actual:
                        if actual.sizes.get("time") != marker["observations"]:
                            raise IOError("HF month read-back has the wrong observation count.")
                else:
                    with open_raw(downloaded) as actual:
                        if (fingerprint_streaming(actual) != marker["array_sha256"] or
                                metadata_fingerprint(actual) != marker["metadata_sha256"]):
                            raise IOError("HF month read-back differs from the local archive.")
            marker["stored_bytes"] = archive_path.stat().st_size
            marker["hf_upload_seconds"] = upload_seconds
            self.client.put_object(Bucket=self.config["bucket"],
                Key=self.key(relative.rsplit("/", 1)[0] + "/complete.json"),
                Body=json.dumps(marker, sort_keys=True).encode(), ContentType="application/json")

    def delete(self, relative):
        """Remove one known object after its replacement has been verified."""
        with bucket_writer(self.bucket_id):
            self.client.delete_object(Bucket=self.config["bucket"], Key=self.key(relative))


@contextmanager
def bucket_writer(bucket, scope=None):
    """Lock one HF bucket, or one disjoint archive path within that bucket.

    The default is one publisher per workstation bucket. A caller may supply a
    stable ``scope`` only when all its object writes are confined to that
    independent prefix; this permits bounded concurrency across monthly
    archives while still serializing retries or duplicate requests for the
    same archive. Cluster jobs must use one publishing coordinator, or put
    ECORE_HF_LOCK_DIR on a shared filesystem with working POSIX locks. This is
    not a distributed lock.
    """
    import fcntl
    folder = Path(os.getenv("ECORE_HF_LOCK_DIR", tempfile.gettempdir())) / "ecore-hf-locks"
    folder.mkdir(parents=True, exist_ok=True)
    bucket_path = folder / (hashlib.sha256(bucket.encode()).hexdigest()[:20] + ".lock")

    def thread_lock(path):
        with _LOCKS_GUARD:
            return _LOCKS.setdefault(str(path), threading.RLock())

    if scope is None:
        with thread_lock(bucket_path), bucket_path.open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)
        return

    scope_identity = f"{bucket}\0{scope}"
    scope_path = folder / (hashlib.sha256(scope_identity.encode()).hexdigest()[:20] + ".lock")
    # Scoped writers share a read lock on the bucket-wide file, excluding any
    # legacy/global publisher, and hold an exclusive lock for their own month.
    # Thus different month prefixes can overlap while identical prefixes cannot.
    with thread_lock(scope_path), bucket_path.open("a") as bucket_lock, scope_path.open("a") as month_lock:
        fcntl.flock(bucket_lock, fcntl.LOCK_SH)
        try:
            fcntl.flock(month_lock, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(month_lock, fcntl.LOCK_UN)
        finally:
            fcntl.flock(bucket_lock, fcntl.LOCK_UN)


class Publisher:
    def __init__(self, root, api=None):
        from dotenv import load_dotenv
        load_dotenv()
        from huggingface_hub import HfApi
        from huggingface_hub.utils import disable_progress_bars
        disable_progress_bars("huggingface_hub")
        parts = root.removeprefix("hf://buckets/").split("/")
        self.bucket = "/".join(parts[:2])
        self.api = api or HfApi(token=os.getenv("HF_TOKEN") or os.getenv("HF_API_KEY"))
        self.batch_calls = self.readback_bytes = 0

    def prefix(self, root):
        return root.removeprefix(f"hf://buckets/{self.bucket}/").rstrip("/")

    def check(self):
        with bucket_writer(self.bucket):
            self.api.bucket_info(self.bucket)

    def _download(self, paths, directory):
        files = []
        for remote, relative in paths:
            local = Path(directory) / relative
            local.parent.mkdir(parents=True, exist_ok=True)
            files.append((remote, local))
        self.api.download_bucket_files(self.bucket, files, raise_on_missing_files=True)
        self.batch_calls += 1
        self.readback_bytes += sum(p.stat().st_size for _, p in files)

    def resume(self, root, selection_id, asset_id, schema, subset_id=None):
        from ecore_weather.storage.archives import fingerprint, metadata_fingerprint, open_raw
        prefix = self.prefix(root)
        with bucket_writer(self.bucket), tempfile.TemporaryDirectory(prefix="ecore-hf-resume-") as temp:
            infos = list(self.api.get_bucket_paths_info(self.bucket, [prefix + "/complete.json"]))
            self.batch_calls += 1
            if not infos:
                return False
            self._download([(infos[0], "complete.json")], temp)
            marker = json.loads((Path(temp)/"complete.json").read_text())
            identity_matches = marker.get("subset_id") == subset_id if subset_id else marker.get("selection_id") == selection_id
            if not identity_matches or (marker.get("asset_id"), marker.get("raw_schema_version")) != (asset_id, schema):
                return False
            raw_path = marker.get("raw_path", "raw.zarr")
            if raw_path not in {"raw.zarr", "raw.zarr.zip"}:
                raise ValueError("Invalid raw container in completion marker.")
            paths = marker.get("files")
            if paths is None:
                # Earlier stores have no file inventory. Discover just this owned store.
                paths = [x.path[len(prefix)+1:] for x in self.api.list_bucket_tree(
                    self.bucket, prefix=prefix+"/raw.zarr", recursive=True) if hasattr(x, "size")]
            if not paths or any(not (p == raw_path or p.startswith(raw_path+"/")) or ".." in Path(p).parts for p in paths):
                raise ValueError("Invalid raw-store inventory in completion marker.")
            self._download([(prefix+"/"+p, p) for p in paths], temp)
            with open_raw(Path(temp)/marker.get("raw_path", "raw.zarr")) as ds:
                return raw_path if fingerprint(ds) == marker["array_sha256"] and metadata_fingerprint(ds) == marker["metadata_sha256"] else False

    def publish(self, staging, root, marker):
        from ecore_weather.storage.archives import fingerprint, metadata_fingerprint, open_raw
        prefix = self.prefix(root)
        files = sorted(p for p in Path(staging).rglob("*") if p.is_file() and p.relative_to(staging).as_posix() != "complete.json")
        paths = [p.relative_to(staging).as_posix() for p in files]
        with bucket_writer(self.bucket):
            self.api.batch_bucket_files(self.bucket, add=[(p, prefix+"/"+rel) for p, rel in zip(files, paths)])
            self.batch_calls += 1
            with tempfile.TemporaryDirectory(prefix="ecore-hf-verify-", dir=staging) as temp:
                self._download([(prefix+"/"+p, p) for p in paths], temp)
                with open_raw(Path(temp)/marker.get("raw_path", "raw.zarr")) as ds:
                    if fingerprint(ds) != marker["array_sha256"] or metadata_fingerprint(ds) != marker["metadata_sha256"]:
                        raise IOError("Published arrays or metadata failed the batch read-back check.")
            # HF batches are not transactional: publish completion only after verification.
            payload = json.dumps({**marker, "files": paths}).encode()
            self.api.batch_bucket_files(self.bucket, add=[(payload, prefix+"/complete.json")])
            self.batch_calls += 1
