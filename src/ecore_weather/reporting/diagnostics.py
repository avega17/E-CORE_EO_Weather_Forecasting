"""Describe source values and missingness before changing any measurements."""

import numpy as np
import pandas as pd
import xarray as xr

from ecore_weather.core.common import PATCHES


def classify(ds, variable):
    """Return exclusive classes plus a decoded view for descriptive statistics.

    0 valid, 1 numeric fill/missing, 2 no coverage, 3 bitmap missing,
    4 NaN/Inf, 5 failed quality flag, 6 outside documented valid range.
    The order below defines which class wins when flags overlap.
    """
    raw = ds[variable]
    codes = xr.zeros_like(raw, dtype="uint8")
    if variable == "measurement":
        from ecore_weather.sources.mrms import PRODUCTS
        info = PRODUCTS[ds.attrs["product"]]
        physical = raw
        if info.get("zero_ambiguous"):
            codes = xr.where(raw == 0, 7, codes)
        else:
            codes = xr.where(raw == info["missing"], 1, codes)
            codes = xr.where(raw == info["no_coverage"], 2, codes)
        codes = xr.where(~np.isfinite(raw), 4, codes)
        codes = xr.where(ds.bitmap_valid == 0, 3, codes)
    else:
        from ecore_weather.sources.goes import decode
        physical = decode(ds)[variable]
        if "valid_range" in raw.attrs:
            low, high = raw.attrs["valid_range"]
            codes = xr.where((raw < low) | (raw > high), 6, codes)
        flag = variable.replace("CMI", "DQF")
        if flag in ds:
            codes = xr.where(ds[flag] != 0, 5, codes)
        fill = raw.attrs.get("_FillValue")
        if fill is not None:
            codes = xr.where(raw == fill, 1, codes)
        codes = xr.where(~np.isfinite(raw), 4, codes)
    codes.attrs["classes"] = "0 valid; 1 missing/fill; 2 no coverage; 3 bitmap missing; 4 NaN/Inf; 5 quality flag; 6 outside valid range; 7 ambiguous shear zero"
    return codes, physical


CLASS_NAMES = {0: "valid", 1: "missing_fill", 2: "no_coverage", 3: "bitmap_missing",
               4: "nonfinite", 5: "quality_flag", 6: "outside_valid_range",
               7: "ambiguous_shear_zero"}


def _patch_mask(ds, bbox):
    west, south, east, north = bbox
    if "latitude" in ds.dims:
        lon = (ds.longitude + 180) % 360 - 180
        return ((ds.latitude >= south) & (ds.latitude < north)) & ((lon >= west) & (lon < east))
    from ecore_weather.sources.goes import lonlat
    lon, lat = lonlat(ds)
    return xr.DataArray((lon >= west) & (lon < east) & (lat >= south) & (lat < north),
                        dims=("y", "x"), coords={"y": ds.y, "x": ds.x})


def describe(ds, patches=None):
    """One row per patch and variable. Missing whole files are reported elsewhere."""
    from ecore_weather.sources.goes import science_variables
    patches = PATCHES if patches is None else patches
    variables = ["measurement"] if "measurement" in ds else science_variables(ds)
    rows = []
    for variable in variables:
        codes, physical = classify(ds, variable)
        for patch, bbox in patches.items():
            mask = _patch_mask(ds, bbox).broadcast_like(codes)
            selected_codes = codes.values[mask.values]
            total = selected_codes.size
            valid = ((codes == 0) & mask)
            values = physical.values[valid.values]
            row = {"time": ds.attrs.get("observation_time"),
                   "slot_time": ds.attrs.get("hourly_slot", ds.attrs.get("observation_time")), "patch": patch, "variable": variable,
                   "units": physical.attrs.get("units"), "pixels": total, "valid_pixels": values.size}
            for code, name in CLASS_NAMES.items():
                count = int(np.count_nonzero(selected_codes == code))
                row[f"{name}_count"] = count
                row[f"{name}_pct"] = 100 * count / total if total else np.nan
            row["valid_zero_count"] = int(np.count_nonzero(values == 0))
            row["valid_zero_pct"] = 100 * row["valid_zero_count"] / total if total else np.nan
            row["valid_negative_count"] = int(np.count_nonzero(values < 0))
            row["note"] = "outside selected pixels" if total == 0 else "no valid measurements" if not values.size else ""
            row.update({name: np.nan for name in ["min", "max", "mean", "std", "q25", "median", "q75"]})
            if values.size:
                row.update(min=float(values.min()), max=float(values.max()), mean=float(values.mean()),
                           std=float(values.std()), q25=float(np.quantile(values, .25)),
                           median=float(np.median(values)), q75=float(np.quantile(values, .75)))
            # DQF frequencies are separate from the exclusive validity classes.
            flag = variable.replace("CMI", "DQF")
            if variable.startswith("CMI") and flag in ds:
                flags, counts = np.unique(ds[flag].values[mask.values], return_counts=True)
                for code, count in zip(flags, counts):
                    row[f"DQF_{int(code)}_count"] = int(count)
            rows.append(row)
    return pd.DataFrame(rows)


def describe_run(report, patches=None):
    from ecore_weather.storage.archives import open_raw
    frames = []
    for record in report["records"]:
        if record["status"] in ("saved", "reused"):
            with open_raw(record["url"]) as ds:
                frames.append(describe(ds, patches))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def plot_maps(ds, variable=None):
    import matplotlib.pyplot as plt
    from ecore_weather.sources.goes import science_variables
    variable = variable or ("measurement" if "measurement" in ds else science_variables(ds)[0])
    codes, physical = classify(ds, variable)
    fig, axes = plt.subplots(1, 3, figsize=(15, 4), constrained_layout=True)
    plot_coords = {dim: physical.coords[dim] for dim in physical.dims if dim in physical.coords}
    radar = variable == "measurement"
    if radar:
        plot_coords["longitude"] = (physical.longitude + 180) % 360 - 180
    raw_label = f"Raw value ({physical.attrs.get('units', '')})" if radar else "Packed pixel value"
    ds[variable].assign_coords(plot_coords).plot(ax=axes[0], cbar_kwargs={"label": raw_label})
    codes.assign_coords(plot_coords).plot(ax=axes[1], levels=np.arange(-.5, 8.5), cmap="tab10",
                                          cbar_kwargs={"label": "Pixel class", "ticks": range(8)})
    # Packed GOES coordinates are decoded along with the measurements. Transfer
    # the pixel mask positionally rather than aligning packed to physical axes.
    mask = xr.DataArray(codes.values == 0, dims=physical.dims, coords=physical.coords)
    physical.where(mask).assign_coords(plot_coords).plot(ax=axes[2],
        cbar_kwargs={"label": f"Valid value ({physical.attrs.get('units', '')})"})
    for ax in axes:
        ax.set(xlabel="Longitude (degrees)" if radar else "Scan x (radians)",
               ylabel="Latitude (degrees)" if radar else "Scan y (radians)")
    if radar:
        from ecore_weather.ui.maps import add_context
        bbox = ds.attrs.get("requested_bbox", (float(plot_coords["longitude"].min()), float(physical.latitude.min()),
                                              float(plot_coords["longitude"].max()), float(physical.latitude.max())))
        for ax in axes:
            add_context(ax, bbox)
    axes[0].set_title("Raw values, including sentinel codes")
    axes[1].set_title("Missingness and quality classes")
    axes[2].set_title("Valid measurements only")
    fig.supxlabel(codes.attrs["classes"], fontsize=8)
    return fig


def plot_coverage(table, expected_times=None):
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(10, 4), constrained_layout=True)
    for (patch, variable), group in table.groupby(["patch", "variable"]):
        series = pd.Series(group.valid_pct.values, index=pd.to_datetime(group["slot_time"] if "slot_time" in group else group.time, utc=True)).sort_index()
        if expected_times:
            series = series.reindex(series.index.union(pd.to_datetime(expected_times, utc=True)))
        ax.plot(series.index, series.values, label=f"{patch} · {variable}")
    ax.set(ylabel="Valid pixels (%)", xlabel="Time (UTC)", ylim=(0, 100))
    ax.legend(fontsize=8, loc="best")
    return fig


def outage_lengths(table):
    """Consecutive observed frames with no valid pixels; not elapsed missing-file time."""
    rows = []
    for (patch, variable), group in table.groupby(["patch", "variable"]):
        group = group.sort_values("time")
        run = longest = 0
        for n in group.valid_pixels:
            run = run + 1 if n == 0 else 0
            longest = max(run, longest)
        rows.append({"patch": patch, "variable": variable, "longest_invalid_observed_frames": longest})
    return pd.DataFrame(rows)
