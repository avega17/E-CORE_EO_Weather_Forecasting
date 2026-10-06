"""Earth2Studio-compatible adapters for native Caribbean NOAA observations.

The adapters use Earth2Studio's synchronous ``DataSource`` call contract while
leaving the source readers in :mod:`mrms` and :mod:`goes` responsible for file
decoding and native-grid cropping. Raw archival access is also exposed as an
xarray Dataset so packed GOES values and MRMS bitmap metadata are not lost.
"""

from __future__ import annotations

from datetime import timedelta

import numpy as np
import xarray as xr

from . import goes, mrms
from .common import PR_BBOX, Transport, iso, utc

try:  # Keep the existing research readers importable before the extra is installed.
    from earth2studio.data.base import DataSource
    from earth2studio.data.utils import prep_data_inputs
except ImportError:  # pragma: no cover - exercised in the minimal reader environment
    class DataSource:  # type: ignore[no-redef]
        """Typing fallback; install the notebooks extra for Earth2Studio helpers."""

    def prep_data_inputs(time, variable):
        if not isinstance(time, (list, tuple, np.ndarray)):
            time = [time]
        if not isinstance(variable, (list, tuple, np.ndarray)):
            variable = [variable]
        return [utc(t).replace(tzinfo=None) for t in time], [str(v) for v in variable]


MRMS_VARIABLES = {
    "precip_rate": "PrecipRate_00.00",
    "refc": "MergedReflectivityQCComposite_00.50",
    "base_reflectivity": "MergedBaseReflectivityQC_00.50",
    "qpe_pass1_1h": "MultiSensor_QPE_01H_Pass1_00.00",
    "qpe_1h": mrms.DEFAULT_PRODUCT,
    "radar_qpe_1h": "RadarOnly_QPE_01H_00.00",
    "az_shear_0to2km": "MergedAzShear_0-2kmAGL_00.50",
    "az_shear_3to6km": "MergedAzShear_3-6kmAGL_00.50",
}


def _inputs(time, variable):
    """Accept notebook/CLI UTC strings before Earth2Studio's datetime helper."""
    times = time if isinstance(time, (list, tuple, np.ndarray)) else [time]
    variables = variable if isinstance(variable, (list, tuple, np.ndarray)) else [variable]
    normalized = [utc(value).replace(tzinfo=None) for value in times]
    return prep_data_inputs(normalized, [str(value) for value in variables])


def _native_array(dataset: xr.Dataset, variable: str, stamp: str) -> xr.DataArray:
    value = dataset["measurement"]
    return value.expand_dims(time=[np.datetime64(utc(stamp).replace(tzinfo=None))], variable=[variable])


class MRMSCaribbeanSource(DataSource):
    """Read one CARIB MRMS product through the Earth2Studio DataSource API.

    Parameters
    ----------
    product : str
        NOAA product key in :data:`ecore_weather.mrms.PRODUCTS`.
    bbox : tuple[float, float, float, float]
        Native-grid crop in west, south, east, north order.
    time_tolerance_minutes : float
        Maximum look-back from each requested time. Files after the request
        time are never selected.
    transport : str
        NOAA read implementation, ``s3fs`` or ``obstore``.
    """

    def __init__(self, product=mrms.DEFAULT_PRODUCT, bbox=PR_BBOX,
                 time_tolerance_minutes=5, transport="obstore"):
        if product not in mrms.PRODUCTS:
            raise ValueError(f"Unsupported MRMS product: {product}")
        self.product = product
        self.bbox = tuple(map(float, bbox))
        self.time_tolerance_minutes = float(time_tolerance_minutes)
        self.transport = transport

    @staticmethod
    def read_asset(asset, bbox, product, transport):
        """Decode a selected NOAA object for the shared STAC fetch pipeline."""
        return mrms.read(asset, bbox, product, transport)

    def read_dataset(self, stamp):
        """Return a raw xarray Dataset and its actual source timestamp."""
        target = utc(stamp)
        selection = mrms.discover(target - timedelta(minutes=self.time_tolerance_minutes),
                                  target + timedelta(seconds=1), bbox=self.bbox,
                                  product=self.product,
                                  tolerance_minutes=self.time_tolerance_minutes,
                                  time_match="previous")
        if not selection.assets:
            raise FileNotFoundError(f"No {self.product} observation within tolerance of {iso(target)}")
        asset = max(selection.assets, key=lambda a: utc(a.time))
        with Transport(self.transport) as io:
            dataset, _ = mrms.read(asset, self.bbox, self.product, io)
        dataset.attrs.update(source_url=asset.url, source_etag=asset.etag,
                             requested_time=iso(target), observation_time=asset.time,
                             time_offset_seconds=(utc(asset.time)-target).total_seconds())
        return dataset

    def __call__(self, time, variable):
        times, variables = _inputs(time, variable)
        product_to_variable = {v: k for k, v in MRMS_VARIABLES.items()}
        supported = product_to_variable.get(self.product)
        if not supported or any(v != supported for v in variables):
            raise ValueError(f"{self.product} exposes Earth2Studio variable {supported!r} only.")
        slices = []
        for stamp in times:
            with self.read_dataset(stamp) as dataset:
                slices.append(_native_array(dataset, supported, dataset.attrs["observation_time"]))
        return xr.concat(slices, dim="time").transpose("time", "variable", "latitude", "longitude")


class GOESCaribbeanSource(DataSource):
    """Read one ABI band through the Earth2Studio DataSource API.

    MCMIPF serves all bands on NOAA's shared 2 km grid. CMIPF, when selected,
    retains a separate native grid for each band. The DataSource call serves
    one band; ``read_dataset`` can return multiple raw bands from MCMIPF.
    """

    def __init__(self, satellite="auto", band=13, bbox=PR_BBOX,
                 product="ABI-L2-CMIPF", transport="obstore"):
        self.satellite = satellite
        self.band = int(band)
        if self.band not in range(1, 17):
            raise ValueError("ABI band must be from 1 to 16.")
        if product not in goes.PRODUCTS:
            raise ValueError(f"Unsupported GOES CMI product: {product}")
        self.bbox = tuple(map(float, bbox))
        self.product = product
        self.transport = transport

    @staticmethod
    def read_asset(asset, bbox, bands, transport, block_size=1024*1024):
        """Read selected bands and the native geospatial window from a scan."""
        return goes.read(asset, bbox, bands, transport, block_size=block_size)

    def read_dataset(self, stamp):
        """Return raw packed pixels, flags, coordinates, and calibration metadata."""
        target = utc(stamp)
        satellite = (goes.east_satellite(target, target + timedelta(minutes=1))
                     if self.satellite == "auto" else int(self.satellite))
        selection = goes.discover(target - timedelta(minutes=5), target + timedelta(minutes=1),
            bbox=self.bbox, bands=(self.band,), satellite=satellite, product=self.product,
            scans_per_hour=None)
        candidates = [asset for asset in selection.assets if utc(asset.time) <= target]
        if not candidates:
            raise FileNotFoundError(f"No GOES-{satellite} C{self.band:02d} scan at/before {iso(target)} within five minutes")
        asset = max(candidates, key=lambda item: utc(item.time))
        with Transport(self.transport) as io:
            dataset, _ = goes.read(asset, self.bbox, (self.band,), io)
        dataset.attrs.update(source_url=asset.url, source_etag=asset.etag,
            requested_time=iso(target), observation_time=asset.time,
            time_offset_seconds=(utc(asset.time)-target).total_seconds())
        return dataset

    def __call__(self, time, variable):
        times, variables = _inputs(time, variable)
        aliases = {f"C{self.band:02d}", f"abi{self.band:02d}c", "CMI"}
        if any(value not in aliases for value in variables):
            raise ValueError(f"This source exposes ABI band C{self.band:02d} only.")
        slices = []
        for stamp in times:
            with self.read_dataset(stamp) as dataset:
                name = "CMI" if "CMI" in dataset else f"CMI_C{self.band:02d}"
                values = dataset[name].expand_dims(
                    time=[np.datetime64(utc(dataset.attrs["observation_time"]).replace(tzinfo=None))],
                    variable=[f"abi{self.band:02d}c"])
                slices.append(values)
        return xr.concat(slices, dim="time").transpose("time", "variable", "y", "x")


class MonthlyZarrSource(DataSource):
    """Expose a saved monthly native archive through Earth2Studio's DataSource API.

    A GOES archive represents one native band grid, while an MRMS archive holds
    one product. Source values and quality arrays remain untouched in storage;
    this adapter returns only the requested science variable to Earth2Studio.
    """

    def __init__(self, path, source, product=None, band=None,
                 time_tolerance_minutes=5):
        self.path = str(path)
        self.source = source
        self.product = product
        self.band = int(band) if band is not None else None
        self.time_tolerance_minutes = float(time_tolerance_minutes)

    def __call__(self, time, variable):
        times, variables = _inputs(time, variable)
        if self.source == "mrms":
            native = "measurement"
            exposed = {product: variable for variable, product in MRMS_VARIABLES.items()}.get(self.product)
            spatial = ("latitude", "longitude")
        elif self.source == "goes" and self.band is not None:
            native = f"CMI_C{self.band:02d}"
            exposed = f"abi{self.band:02d}c"
            spatial = ("y", "x")
        else:
            raise ValueError("MonthlyZarrSource requires MRMS product or a GOES band.")
        if not exposed or any(v not in {exposed, native, "CMI"} for v in variables):
            raise ValueError(f"This archive exposes {exposed!r} only.")

        from .storage import open_raw
        slices = []
        group = (f"C{self.band:02d}" if self.source == "goes" and
                 self.product in {"ABI-L2-MCMIPF", "ABI-L2-CMI-2KM-HYBRID"}
                 and self.band is not None else None)
        with (open_raw(self.path, group=group) if group else open_raw(self.path)) as dataset:
            if native not in dataset:
                raise KeyError(f"Archive {self.path} does not contain {native!r}.")
            source_times = np.asarray(dataset.time.values).astype("datetime64[ns]")
            for requested in times:
                target = np.datetime64(utc(requested).replace(tzinfo=None), "ns")
                offsets = (source_times - target).astype("timedelta64[ns]").astype("int64")
                eligible = np.flatnonzero((offsets <= 0) &
                    (offsets >= -self.time_tolerance_minutes * 60 * 1e9))
                if not len(eligible):
                    raise FileNotFoundError(f"No archived observation at or before {target} within tolerance.")
                index = int(eligible[np.argmax(source_times[eligible])])
                values = dataset[native].isel(time=index, drop=True)
                stamp = source_times[index].astype("datetime64[us]")
                slices.append(values.expand_dims(
                    time=[stamp], variable=[exposed]))
        return xr.concat(slices, dim="time").transpose("time", "variable", *spatial)


def read_selected_asset(asset, source, bbox, product, bands, transport,
                        block_size=1024*1024):
    """Use the same source-specific reader in notebooks and DataSource calls."""
    if source == "mrms":
        return MRMSCaribbeanSource.read_asset(asset, bbox, product, transport)
    return GOESCaribbeanSource.read_asset(asset, bbox, bands, transport, block_size)
