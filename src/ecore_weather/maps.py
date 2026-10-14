"""Geographic context for small research plots; raw arrays remain unchanged."""
import warnings
import numpy as np


def add_context(ax, bbox, labels=True):
    """Cached Natural Earth 1:50m land polygons, also suitable for ordinary axes."""
    from matplotlib.collections import PolyCollection, LineCollection
    west, south, east, north = bbox
    try:
        polygons = _land_polygons()
    except (OSError, RuntimeError) as error:
        warnings.warn(f"Basemap unavailable; displaying geographic axes only: {error}", stacklevel=2)
        polygons = []
    selected = [p for p in polygons if p[:,0].max() >= west and p[:,0].min() <= east
                and p[:,1].max() >= south and p[:,1].min() <= north]
    ax.set_facecolor('#e5f1f7')
    ax.add_collection(PolyCollection(selected, facecolors='#e8e3d6', edgecolors='none', zorder=0))
    ax.add_collection(LineCollection(selected, colors='#404c52', linewidths=.6, zorder=5))
    ax.set(xlim=(west,east), ylim=(south,north), xlabel='Longitude (degrees)', ylabel='Latitude (degrees)')
    ax.set_aspect(1/max(.1, np.cos(np.deg2rad((south+north)/2))))
    ax.grid(alpha=.2)
    for label, lon, lat in ([('Puerto Rico',-66.45,18.23), ('Hispaniola',-70.1,19), ('Virgin Is.',-64.7,18.4)] if labels else []):
        if west < lon < east and south < lat < north:
            ax.text(np.clip(lon, west+.08*(east-west), east-.08*(east-west)), lat, label, fontsize=7, zorder=6, ha='center', color='#202020',
                    bbox={'facecolor':'white','alpha':.65,'edgecolor':'none','pad':1})


from functools import lru_cache

@lru_cache(maxsize=1)
def _land_polygons():
    from cartopy.io import shapereader
    reader = shapereader.Reader(shapereader.natural_earth('50m', 'physical', 'land'))
    try:
        return [np.asarray(poly.exterior.coords) for geom in reader.geometries()
                for poly in (list(geom.geoms) if geom.geom_type == 'MultiPolygon' else [geom])]
    finally:
        reader.close()


def plot_geographic(ds, variable=None, quality=True, hide_zero=False):
    """Decode and mask a display copy; GOES pixels stay on their native grid."""
    import matplotlib.pyplot as plt
    from . import diagnostics, goes
    variable = variable or ('measurement' if 'measurement' in ds else goes.science_variables(ds)[0])
    codes, physical = diagnostics.classify(ds, variable)
    values = np.array(physical.values, dtype=float, copy=True)
    if quality:
        values[codes.values != 0] = np.nan
    if hide_zero:
        values[values == 0] = np.nan
    if variable == 'measurement':
        lon, lat = np.meshgrid((ds.longitude.values+180)%360-180, ds.latitude.values)
    else:
        lon, lat = goes.lonlat(ds)
    finite = np.isfinite(lon) & np.isfinite(lat)
    if not finite.all():
        raise ValueError('This view crosses the satellite limb; choose a region within the visible Earth.')
    bbox = ds.attrs.get('requested_bbox', (float(lon.min()),float(lat.min()),float(lon.max()),float(lat.max())))
    fig, ax = plt.subplots(figsize=(9,6), constrained_layout=True)
    add_context(ax, bbox)
    mesh = ax.pcolormesh(lon, lat, values, shading='auto', cmap='viridis', alpha=.85, zorder=1)
    fig.colorbar(mesh, ax=ax, label=f"{variable} ({physical.attrs.get('units','')})")
    label = variable
    if variable == "CMI" and "band_id" in ds:
        label = f"CMI C{int(ds.band_id.values.item()):02d}"
    ax.set_title(f"{label} · {ds.attrs.get('observation_time', ds.attrs.get('time_coverage_start',''))}")
    fig.supxlabel('Natural Earth · display decoding/masking only; saved raw values are unchanged', fontsize=8)
    return fig
