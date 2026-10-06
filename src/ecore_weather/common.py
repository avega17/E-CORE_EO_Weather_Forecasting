"""Small shared helpers; all public NOAA reads are anonymous."""

from __future__ import annotations

import hashlib
import json
import threading
import time
import multiprocessing
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

PR_BBOX = (-70.24, 14.36, -62.56, 22.04)  # west, south, east, north
BENCHMARK_PERIODS = {
    "September–November 2022": ("2022-09-01", "2022-12-01"),
    "September–November 2025": ("2025-09-01", "2025-12-01"),
}


def default_workers():
    return max(1, multiprocessing.cpu_count() // 2)
PATCHES = {
    "Puerto Rico": (-67.5, 17.8, -65.2, 18.6),
    "Mona Passage": (-68.8, 17.8, -67.6, 18.8),
    "Virgin Islands": (-65.2, 17.5, -64.3, 18.7),
    "Offshore south": (-67.0, 15.0, -65.0, 16.0),
}


def utc(value) -> datetime:
    result = datetime.fromisoformat(str(value).replace("Z", "+00:00")) if not isinstance(value, datetime) else value
    return result.replace(tzinfo=timezone.utc) if result.tzinfo is None else result.astimezone(timezone.utc)


def iso(value) -> str:
    return utc(value).isoformat().replace("+00:00", "Z")


def validate_request(start, end, bbox):
    start, end = utc(start), utc(end)
    if start >= end:
        raise ValueError("The end must be later than the start (the end is excluded).")
    west, south, east, north = map(float, bbox)
    if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
        raise ValueError("Use west, south, east, north; crossing the date line is not supported yet.")
    return start, end, (west, south, east, north)


def hours(start, end):
    current = utc(start).replace(minute=0, second=0, microsecond=0)
    while current < utc(end):
        yield current
        current += timedelta(hours=1)


def time_slots(start, end, cadence_minutes):
    """Yield UTC slots on a cadence-aligned clock, with the end excluded."""
    if cadence_minutes < 1 or 60 % cadence_minutes:
        raise ValueError("Cadence must be a positive divisor of 60 minutes.")
    current = utc(start).replace(second=0, microsecond=0)
    minute = (current.minute // cadence_minutes) * cadence_minutes
    current = current.replace(minute=minute)
    while current < utc(end):
        if current >= utc(start):
            yield current
        current += timedelta(minutes=cadence_minutes)


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()[:20]


@dataclass(frozen=True)
class Asset:
    bucket: str
    key: str
    size: int
    etag: str
    time: str
    end_time: str | None = None

    @property
    def url(self):
        return f"https://{self.bucket}.s3.amazonaws.com/{self.key}"

    @property
    def id(self):
        return digest(asdict(self))


@dataclass
class Selection:
    source: str
    product: str
    start: str
    end: str
    bbox: tuple[float, float, float, float]
    assets: list[Asset]
    bands: tuple[int, ...] = ()
    expected_times: tuple[str, ...] = ()
    satellite: int | None = None
    hourly_matches: tuple[dict, ...] = ()
    time_tolerance_minutes: float = 0
    time_match: str = "exact"
    scans_per_hour: int | None = None  # GOES decimation; None keeps every scan
    cadence_minutes: int | None = None  # requested slots; actual source timestamps remain in assets

    @property
    def id(self):
        return digest(asdict(self))

    def summary(self):
        observed = ({m["slot_time"] for m in self.hourly_matches} if self.hourly_matches
                    else {a.time for a in self.assets})
        return {
            "source": self.source, "product": self.product,
            "start_utc": self.start, "end_utc_excluded": self.end,
            "files": len(self.assets), "source_MB": sum(a.size for a in self.assets) / 1e6,
            "missing_times": [t for t in self.expected_times if t not in observed],
            "bbox_west_south_east_north": self.bbox, "bands": self.bands,
            "satellite": self.satellite, "scans_per_hour": self.scans_per_hour,
            "cadence_minutes": self.cadence_minutes,
            "expected_hourly_slots": len(self.expected_times),
            "availability_note": "See acquisition_coverage for satellite scan counts." if self.source == "goes"
                                 else "Missing times refer to requested hourly slots, separately from pixel coverage.",
            "time_match": self.time_match, "time_tolerance_minutes": self.time_tolerance_minutes,
            "shifted_matches": [m for m in self.hourly_matches if m["offset_seconds"] != 0],
        }


def s3_client():
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config
    return boto3.client("s3", region_name="us-east-1", config=Config(
        signature_version=UNSIGNED, retries={"max_attempts": 4, "mode": "standard"},
        max_pool_connections=16, connect_timeout=15, read_timeout=60,
    ))


def list_objects(bucket, prefix, client=None):
    client = client or s3_client()
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        yield from page.get("Contents", [])


class Transport:
    """Count returned object bytes and read calls, including repeated range reads.

    Counters exclude TLS/HTTP headers and archive-listing requests. Read seconds
    are summed task time, not elapsed wall time when requests run concurrently.
    """

    def __init__(self, backend="s3fs", decode_workers=1):
        if backend not in {"s3fs", "obstore"}:
            raise ValueError("Choose s3fs or obstore.")
        self.backend = backend
        self.bytes = self.requests = self.retries = self.failed_requests = 0
        self.seconds = 0.0
        self._lock = threading.Lock()
        if decode_workers < 1:
            raise ValueError("decode_workers must be positive.")
        self.decode_slots = threading.BoundedSemaphore(decode_workers)
        self._stores = {}
        if backend == "s3fs":
            import s3fs
            from fsspec.asyn import get_loop
            self.io_loop = get_loop()
            self.fs = s3fs.S3FileSystem(anon=True, asynchronous=True, skip_instance_cache=True,
                                      client_kwargs={"region_name": "us-east-1"})

    def read(self, asset: Asset, start=None, end=None):
        before = time.perf_counter()
        from .transfer_budget import active, BudgetExceeded
        budget=active()
        for attempt in range(3):
            reservation=budget.reserve(end-start if start is not None else asset.size) if budget else None
            try:
                if self.backend == "s3fs":
                    from fsspec.asyn import sync
                    options = {"Bucket": asset.bucket, "Key": asset.key, "IfMatch": f'"{asset.etag}"'}
                    if start is not None:
                        options["Range"] = f"bytes={start}-{end-1}"
                    response = sync(self.io_loop, self.fs._call_s3, "get_object", **options)
                    try:
                        data = sync(self.io_loop, response["Body"].read)
                    finally:
                        response["Body"].close()
                else:
                    import obstore
                    from obstore.store import S3Store
                    with self._lock:
                        if asset.bucket not in self._stores:
                            self._stores[asset.bucket] = S3Store(asset.bucket, region="us-east-1", skip_signature=True)
                        store = self._stores[asset.bucket]
                    options = {"if_match": f'"{asset.etag}"'}
                    if start is not None:
                        options["range"] = (start, end)
                    data = bytes(obstore.get(store, asset.key, options=options).bytes())
                if budget:budget.settle(reservation,len(data))
                reservation=None
                with self._lock:
                    self.bytes += len(data)
                    self.requests += 1
                    self.seconds += time.perf_counter() - before
                return data
            except Exception:
                if budget and reservation is not None:budget.settle(reservation)
                with self._lock:
                    if attempt == 2:
                        self.failed_requests += 1
                    else:
                        self.retries += 1
                if attempt == 2:
                    raise
                time.sleep(0.5 * 2**attempt)

    def __enter__(self):
        return self

    def close(self):
        if self.backend == "s3fs" and not getattr(self, "_closed", False):
            creator = getattr(self.fs, "_s3creator", None)
            if creator is not None:
                from fsspec.asyn import sync
                sync(self.io_loop, creator.__aexit__, None, None, None)
            self._closed = True

    def __exit__(self, *args):
        self.close()


def remote_file(asset, transport, block_size=256 * 1024):
    """A bounded read cache for HDF5's seek/read calls."""
    from fsspec.spec import AbstractBufferedFile

    class RemoteFile(AbstractBufferedFile):
        def _fetch_range(self, start, end):
            return transport.read(asset, start, min(end, asset.size))

    return RemoteFile(fs=None, path=asset.key, mode="rb", size=asset.size,
                      block_size=block_size, cache_type="blockcache",
                      cache_options={"maxblocks": 64})


class PeakMemory:
    """Sample this process and its children; report bytes of resident memory."""
    def __enter__(self):
        import psutil
        self.process = psutil.Process()
        self.peak = 0
        self._stop = threading.Event()

        def sample():
            while not self._stop.is_set():
                try:
                    processes = [self.process] + self.process.children(recursive=True)
                    size = sum(p.memory_info().rss for p in processes if p.is_running())
                    self.peak = max(self.peak, size)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
                self._stop.wait(0.05)

        self._thread = threading.Thread(target=sample, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *args):
        self._stop.set()
        self._thread.join()


def jsonable(value):
    """Keep source metadata JSON-compatible without dropping array attributes."""
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, np.ndarray)):
        return [jsonable(v) for v in value]
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="backslashreplace")
    return value


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Unavailable report statistics use JSON null; raw Zarr attributes retain
    # their original fill encodings through jsonable(), independently.
    clean = json.loads(json.dumps(jsonable(value), default=str), parse_constant=lambda _: None)
    path.write_text(json.dumps(clean, indent=2, allow_nan=False) + "\n")
