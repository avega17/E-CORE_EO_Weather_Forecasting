"""Bounded display frames for Leaflet and animations; never modify stored rasters."""
import base64
import io
from collections import defaultdict
from pathlib import Path

import numpy as np

from .storage import open_raw


def _scan_attrs(dataset):
    """Restore scan-specific calibration on a selected multiband observation."""
    if 'source_metadata_json' not in dataset.coords or dataset.source_metadata_json.size != 1:
        return dataset
    import json
    value = np.asarray(dataset.source_metadata_json.values)
    metadata = json.loads(str(value.item()))
    dataset = dataset.copy(deep=False)
    for name, info in metadata.get('variables', {}).items():
        targets = [name] if name in dataset else [n for n in dataset.data_vars
            if name in ('CMI','DQF') and n.startswith(name+'_C')]
        for target in targets:
            dataset[target].attrs = info.get('attrs', {})
    return dataset


def open_observation(record, select_time=True):
    """Open a legacy single-file store or one timestamp from a monthly archive."""
    from contextlib import contextmanager

    @contextmanager
    def opened():
        path = (record.get("path") or record.get("url")) if isinstance(record, dict) else str(record)
        if not path:
            raise ValueError("Observation record has no archive path or URL")
        band = record.get('band') if isinstance(record, dict) else None
        grouped = isinstance(record, dict) and record.get('product') in {
            'ABI-L2-MCMIPF', 'ABI-L2-CMI-2KM-HYBRID'} and band is not None
        dataset = open_raw(path, group=f'C{band:02d}') if grouped else open_raw(path)
        try:
            if select_time and isinstance(record, dict) and "time" in dataset.dims:
                stamp = np.datetime64(record["time"].replace("Z", ""))
                yield _scan_attrs(dataset.sel(time=stamp, drop=False))
            else:
                yield dataset
        finally:
            dataset.close()
    return opened()


def _frame_dataset(ds, record=None, path=None, variable=None, quality=True,
                   hide_zero=False, pixels=384):
    import rioxarray
    from rasterio.enums import Resampling
    from rasterio.transform import from_bounds
    from pyproj import Transformer
    from . import diagnostics, goes
    variable=variable or ('measurement' if 'measurement' in ds else goes.science_variables(ds)[0])
    if variable not in ds:
        raise ValueError(f'{variable} is not saved in this subset. Choose another band or dataset.')
    codes,physical=diagnostics.classify(ds,variable)
    arr=physical.astype('float32').copy(deep=True)
    if quality: arr.values[codes.values!=0]=np.nan
    if hide_zero: arr.values[arr.values==0]=np.nan
    bbox=tuple(ds.attrs.get('requested_bbox',()))
    if variable=='measurement':
        arr=arr.assign_coords(longitude=(arr.longitude+180)%360-180).rename(longitude='x',latitude='y')
        arr=arr.rio.write_crs('EPSG:4326')
        if not bbox:bbox=(float(arr.x.min()),float(arr.y.min()),float(arr.x.max()),float(arr.y.max()))
    else:
        crs,height=goes.projection(ds)
        arr=arr.assign_coords(x=goes._physical_coordinate(ds.x)*height,y=goes._physical_coordinate(ds.y)*height).rio.write_crs(crs)
        if not bbox:
            lon,lat=goes.lonlat(ds);bbox=(float(np.nanmin(lon)),float(np.nanmin(lat)),float(np.nanmax(lon)),float(np.nanmax(lat)))
    west,south,east,north=bbox
    if not (-85<south<north<85):raise ValueError('The interactive map supports regions within Web Mercator latitude limits.')
    transform=Transformer.from_crs('EPSG:4326','EPSG:3857',always_xy=True)
    left,bottom=transform.transform(west,south);right,top=transform.transform(east,north)
    projected=arr.rio.write_nodata(np.nan).rio.reproject('EPSG:3857',shape=(pixels,pixels),
                transform=from_bounds(left,bottom,right,top,pixels,pixels),resampling=Resampling.nearest)
    label=variable if variable!='CMI' else f'CMI C{int(ds.band_id.values.item()):02d}'
    return {'values':projected.values,'bbox':bbox,'extent':(left,right,bottom,top),
            'time':record.get('time') if record else ds.attrs.get('observation_time',ds.attrs.get('time_coverage_start','')),
            'label':label,'units':physical.attrs.get('units',''),
            'path':str((record.get('path') or record.get('url')) if record else path)}


def frame(path, variable=None, quality=True, hide_zero=False, pixels=384):
    record = path if isinstance(path, dict) else None
    with open_observation(path) as ds:
        return _frame_dataset(ds, record, path, variable, quality, hide_zero, pixels)


def limits(frames):
    lows=[];highs=[]
    for f in frames:
        valid=f['values'][np.isfinite(f['values'])]
        if valid.size:lows.append(float(valid.min()));highs.append(float(valid.max()))
    low=min(lows) if lows else 0.;high=max(highs) if highs else 1.
    return low, high if high>low else low+1


def png(frame, scale):
    import matplotlib
    from matplotlib.colors import Normalize
    from PIL import Image
    rgba=matplotlib.colormaps['viridis'](Normalize(*scale,clip=True)(frame['values']),bytes=True)
    rgba[~np.isfinite(frame['values']),3]=0
    stream=io.BytesIO();Image.fromarray(rgba).save(stream,format='PNG')
    return 'data:image/png;base64,'+base64.b64encode(stream.getvalue()).decode()


def prepare(records, variable=None, quality=True, hide_zero=False, pixels=384, progress=None, max_frames=1500):
    if not records:raise ValueError('No observations match this view.')
    if len(records)>max_frames:raise ValueError(f'{len(records)} frames exceed the {max_frames}-frame display limit. Shorten the period or reduce images per day.')
    # A multi-day sequence often revisits the same handful of monthly ZIP
    # stores. Open each store once, then select all its requested time slices;
    # reopening a ZipStore for every frame repeats metadata and file setup.
    grouped=defaultdict(list)
    for index,row in enumerate(records):
        path=(row.get('path') or row.get('url')) if isinstance(row,dict) else str(row)
        group_band=(row.get('band') if isinstance(row,dict) and row.get('product') in
                    {'ABI-L2-MCMIPF','ABI-L2-CMI-2KM-HYBRID'} else None)
        grouped[(path,group_band)].append((index,row))
    frames=[None]*len(records)
    completed=0
    for (path,_),items in grouped.items():
        # The record-aware helper narrows to that record's timestamp. Here we
        # need the whole monthly store so each requested observation can be
        # selected independently below.
        with open_observation(items[0][1],select_time=False) as ds:
            for index,row in items:
                if isinstance(row,dict) and 'time' in row and 'time' in ds.dims:
                    stamp=np.datetime64(row['time'].replace('Z',''))
                    selected=_scan_attrs(ds.sel(time=stamp,drop=False))
                else:
                    selected=ds
                f=_frame_dataset(selected,row,path,variable,quality,hide_zero,pixels)
                f['time']=row['time'] if isinstance(row,dict) and 'time' in row else f['time']
                frames[index]=f
                completed+=1
                if progress:progress(completed,len(records),{'status':'prepared'})
    if len({tuple(f['bbox']) for f in frames})!=1:raise ValueError('Choose one dataset/region for an animation.')
    return frames


def leaflet(frames):
    """Preload PNGs once; Play changes the overlay without reading Zarr again.

    Python observers on both controls update the Leaflet URL directly. This
    keeps playback working in notebook frontends where linked controls do not
    reliably trigger the image layer's URL update.
    """
    import html
    import ipywidgets as w
    from ipyleaflet import Map, ImageOverlay, LayersControl, basemaps
    scale=limits(frames);urls=[png(f,scale) for f in frames]
    west,south,east,north=frames[0]['bbox']
    bounds=((south,west),(north,east))
    m=Map(center=((south+north)/2,(west+east)/2),zoom=7,scroll_wheel_zoom=True,
          basemap=basemaps.OpenStreetMap.Mapnik,layout=w.Layout(height='500px'))
    overlay=ImageOverlay(url=urls[0],bounds=bounds,name=frames[0]['label'],opacity=.8)
    m.add(overlay);m.add(LayersControl(position='topright'));m.fit_bounds(bounds)
    title=w.HTML();slider=w.IntSlider(min=0,max=len(frames)-1,value=0,description='Frame',continuous_update=True)
    play=w.Play(min=0,max=len(frames)-1,value=0,interval=500,disabled=len(frames)==1)
    # Keep the frame renderer directly subscribed to Play as well as the
    # slider. A kernel-side widget link alone can update the slider while the
    # Leaflet ImageOverlay remains on its first URL in some notebook frontends.
    last_index={'value':None}
    def update_index(i):
        i=int(i)
        if i==last_index['value']:
            return
        last_index['value']=i
        overlay.url=urls[i]
        title.value=f"<b>{html.escape(frames[i]['label'])}</b> · {html.escape(frames[i]['time'])} UTC · {i+1}/{len(frames)}"
    def update_from_play(change):
        i=change['new']
        if slider.value!=i:
            slider.value=i
        update_index(i)
    def update_from_slider(change):
        i=change['new']
        if play.value!=i:
            play.value=i
        update_index(i)
    play.observe(update_from_play,names='value')
    slider.observe(update_from_slider,names='value')
    update_index(0)
    legend=w.HTML(f"<small>Fixed scale for all frames: {scale[0]:.3g}–{scale[1]:.3g} {html.escape(str(frames[0]['units']))}. "
                  "Transparent pixels are hidden for display. Basemap © OpenStreetMap contributors.</small>"
                  "<div style='width:240px;height:10px;background:linear-gradient(to right,#440154,#3b528b,#21918c,#5ec962,#fde725)'></div>")
    panel=w.VBox([title,m,w.HBox([play,slider]),legend])
    # Keep the controls and preloaded frames reachable while this panel is displayed.
    panel._ecore_play=play
    panel._ecore_slider=slider
    panel._ecore_frames=frames
    return panel


def portable_map(frames):
    """Pan, zoom, and play frames using core widgets and embedded PNGs.

    VS Code's notebook webview may not register the jupyter-leaflet JavaScript
    module even when ipyleaflet imports in Python. This renderer has no custom
    frontend widget dependency and never rereads Zarr during playback.
    """
    import html
    from functools import lru_cache
    import ipywidgets as w
    from PIL import Image, ImageDraw
    from pyproj import Transformer

    scale=limits(frames)
    urls=[png(f,scale) for f in frames]
    first=frames[0]
    height,width=first['values'].shape
    left,right,bottom,top=first['extent']

    @lru_cache(maxsize=1)
    def background():
        from .maps import _land_polygons
        west,south,east,north=first['bbox']
        canvas=Image.new('RGB',(width,height),'#cfe7ee')
        draw=ImageDraw.Draw(canvas)
        projector=Transformer.from_crs('EPSG:4326','EPSG:3857',always_xy=True)
        try:
            polygons=_land_polygons()
        except (OSError,RuntimeError):
            polygons=[]
        for poly in polygons:
            if (poly[:,0].max()<west or poly[:,0].min()>east or
                    poly[:,1].max()<south or poly[:,1].min()>north):
                continue
            x,y=projector.transform(poly[:,0],poly[:,1])
            points=[((px-left)/(right-left)*width,(top-py)/(top-bottom)*height)
                    for px,py in zip(x,y)]
            if len(points)>=3:
                draw.polygon(points,fill='#e9e2d0',outline='#748381')
        for label,lon,lat in [('Puerto Rico',-66.45,18.23),
                              ('Hispaniola',-70.1,19.0),('Virgin Islands',-64.7,18.4)]:
            if west<lon<east and south<lat<north:
                x,y=projector.transform(lon,lat)
                draw.text(((x-left)/(right-left)*width,(top-y)/(top-bottom)*height),
                          label,fill='#27393a',stroke_width=1,stroke_fill='white')
        output=io.BytesIO()
        canvas.save(output,format='PNG')
        return 'data:image/png;base64,'+base64.b64encode(output.getvalue()).decode()

    base=background()
    title=w.HTML()
    picture=w.HTML()
    zoom=w.IntSlider(value=100,min=50,max=300,step=25,description='Zoom %',
                     tooltip='Enlarge the image, then scroll within the viewport to pan.')
    slider=w.IntSlider(min=0,max=len(frames)-1,value=0,description='Frame',continuous_update=True)
    play=w.Play(min=0,max=len(frames)-1,value=0,interval=500,disabled=len(frames)==1)
    last={'frame':None,'zoom':None}
    def update(i=None):
        i=slider.value if i is None else int(i)
        if (i,zoom.value)==(last['frame'],last['zoom']):
            return
        last.update(frame=i,zoom=zoom.value)
        display_width=round(width*zoom.value/100)
        display_height=round(height*zoom.value/100)
        picture.value=(
            '<div style="height:500px;max-width:100%;overflow:auto;background:#cfe7ee">'
            f'<div style="position:relative;width:{display_width}px;height:{display_height}px">'
            f'<img alt="geographic basemap" src="{base}" '
            'style="position:absolute;inset:0;width:100%;height:100%">'
            f'<img alt="{html.escape(frames[i]["label"])}" src="{urls[i]}" '
            'style="position:absolute;inset:0;width:100%;height:100%"></div></div>')
        title.value=(f'<b>{html.escape(frames[i]["label"])}</b> · '
                     f'{html.escape(str(frames[i]["time"]))} UTC · {i+1}/{len(frames)}')
    def from_play(change):
        if slider.value!=change['new']:
            slider.value=change['new']
        update(change['new'])
    def from_slider(change):
        if play.value!=change['new']:
            play.value=change['new']
        update(change['new'])
    play.observe(from_play,names='value')
    slider.observe(from_slider,names='value')
    zoom.observe(lambda change:update(),names='value')
    update(0)
    legend=w.HTML(f'<small>Fixed scale: {scale[0]:.3g}–{scale[1]:.3g} '
                  f'{html.escape(str(first["units"]))}. Transparent pixels are hidden '
                  'for display. Geographic context: Natural Earth.</small>')
    panel=w.VBox([title,picture,w.HBox([zoom,play,slider]),legend])
    panel._ecore_play=play
    panel._ecore_slider=slider
    panel._ecore_frames=frames
    return panel


def ffmpeg_available():
    import shutil
    return shutil.which('ffmpeg') is not None


ANIMATION_FORMATS = ('html', 'gif', 'mp4')


def save_animation(frames, path, interval=500):
    """Export prepared frames; the writer is chosen from the file extension.

    .html writes a self-contained jshtml page (no tile service), .gif uses the
    Pillow writer, and .mp4 uses ffmpeg when a binary is available. Colors use
    the same fixed scale as the interactive view.
    """
    import matplotlib.pyplot as plt
    from matplotlib.animation import FuncAnimation, PillowWriter, FFMpegWriter
    scale=limits(frames)
    fig,ax=plt.subplots(figsize=(7,6),constrained_layout=True)
    artist=ax.imshow(frames[0]['values'],extent=frames[0]['extent'],vmin=scale[0],vmax=scale[1],cmap='viridis')
    ax.set(xlabel='Web Mercator easting (m)',ylabel='Web Mercator northing (m)')
    fig.colorbar(artist,ax=ax,label=f"{frames[0]['label']} ({frames[0]['units']})")
    def update(i):artist.set_data(frames[i]['values']);ax.set_title(frames[i]['time']);return [artist]
    animation=FuncAnimation(fig,update,frames=len(frames),interval=interval,blit=False)
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    suffix=path.suffix.lower().lstrip('.')
    try:
        import matplotlib as mpl
        if suffix in ('html','htm'):
            with mpl.rc_context({'animation.embed_limit':100}):path.write_text(animation.to_jshtml())
        elif suffix=='gif':
            animation.save(path,writer=PillowWriter(fps=max(1,round(1000/interval))))
        elif suffix=='mp4':
            if not ffmpeg_available():raise RuntimeError('ffmpeg is not installed; choose .html or .gif.')
            animation.save(path,writer=FFMpegWriter(fps=max(1,round(1000/interval))))
        else:
            raise ValueError('Choose a .html, .gif, or .mp4 export path.')
    finally:plt.close(fig)
    return str(path)
