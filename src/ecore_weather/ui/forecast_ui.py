"""Map previews shared by the NAM viewer and derived-data notebook."""
from datetime import date, time, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np

from ecore_weather.core.common import PR_BBOX, iso, utc
from ecore_weather.processing.transforms import (
    NativeReader, Recipe, align_observations, decode_observation, geometry,
    observation_index, regrid, target_grid,
)


def select_goes(location, when, band, resolution=3):
    stamp = utc(when)
    recipe = Recipe(resolution_km=resolution, bands=(band,), radar=())
    begin = iso(stamp-timedelta(minutes=10))
    end = iso(stamp+timedelta(minutes=1))
    observations, _ = observation_index(location, begin, end, recipe)
    if observations.empty:
        raise ValueError('No verified GOES observations at this time')
    aligned = align_observations(observations, begin, end, recipe.variables)
    if aligned.empty:
        raise ValueError('No causal GOES observation for this slot')
    rows = aligned[aligned.slot_time.map(utc)==stamp]
    if rows.empty:
        raise ValueError('No causal GOES observation for this slot')
    record = rows.iloc[0].to_dict()
    with NativeReader(1) as reader:
        dataset = reader.read(record).load()
    return record, dataset, target_grid(recipe)


def map_field(axis, longitude, latitude, values, **options):
    mesh = axis.pcolormesh(longitude, latitude, values, shading='auto', **options)
    west,south,east,north = PR_BBOX
    axis.set(xlim=(west,east), ylim=(south,north), xlabel='Longitude', ylabel='Latitude')
    axis.set_aspect(1/np.cos(np.deg2rad((south+north)/2)))
    axis.grid(alpha=.2)
    return mesh


def missing_panel(axis, message):
    axis.text(.5,.5,message,ha='center',va='center',wrap=True,transform=axis.transAxes)
    axis.set_axis_off()


def parallax_comparison(location, when, band, resolution):
    from matplotlib import pyplot as plt
    from ecore_weather.processing.process_parallax import archived_height, mapping_from_height, open_height
    if band not in (1,2,3,7,13):
        raise ValueError('Choose C01, C02, C03, C07 or C13 for cloud-height correction')
    record, dataset, grid = select_goes(location, when, band, resolution)
    evidence = archived_height(record)
    if evidence is None:
        raise ValueError('No verified same-satellite height within 20 minutes of this scan')
    decoded = decode_observation(dataset)
    baseline = regrid(decoded, geometry(dataset,grid,'bilinear'))
    with open_height(evidence) as height:
        mapping = mapping_from_height(dataset,height,grid)
    corrected = regrid(decoded,mapping)
    support = baseline['valid'] & corrected['valid']
    values = [np.where(support, r['values'], np.nan) for r in (baseline,corrected)]
    if not support.any():
        raise ValueError('No common supported pixels in this selection')
    delta = values[1]-values[0]
    lo,hi = np.nanpercentile(np.concatenate(values),[2,98])
    limit = max(float(np.nanmax(np.abs(delta))),1e-6)
    panels = [*values,delta,np.where(support,mapping['displacement_m'].reshape(support.shape)/1000,np.nan)]
    figure,axes = plt.subplots(2,2,figsize=(11,8),layout='constrained')
    for axis,values,title,kwargs in zip(axes.flat,panels,
        ('Uncorrected','Cloud-height corrected','Corrected minus uncorrected','Displacement (km)'),
        ({'vmin':lo,'vmax':hi},{'vmin':lo,'vmax':hi},
         {'cmap':'RdBu_r','vmin':-limit,'vmax':limit},{'cmap':'magma'})):
        figure.colorbar(map_field(axis,grid.longitude,grid.latitude,values,**kwargs),ax=axis)
        axis.set_title(title)
    age = evidence['height_age_seconds']/60
    common = support.sum()/np.asarray(grid.inside_bbox).sum()
    figure.suptitle(f"GOES-{record['satellite']} C{band:02} · {resolution} km · {when} UTC\n"
                    f"Height age {age:.1f} min · {common:.1%} of the region valid in both views")
    return figure


def create_nam_viewer():
    import ipywidgets as w
    from IPython.display import display
    from matplotlib import pyplot as plt
    from ecore_weather.core.transfer_budget import Budget
    from ecore_weather.processing.nwp import FIELDS, ForecastArchive

    location=w.Text(value='/mnt/p/ecore_eo_datasets',description='Native data',layout=w.Layout(width='95%'))
    day=w.DatePicker(value=date(2022,9,18),description='UTC date')
    hour=w.TimePicker(value=time(16),description='UTC time',tooltip='Choose a whole hour')
    product=w.Dropdown(options=[('NAM Puerto Rico nest','priconest')],description='Product')
    field=w.Dropdown(options=[(r[0],k) for k,r in FIELDS.items()],description='NAM field')
    band=w.Dropdown(options=[(f'C{i:02}',i) for i in (1,2,3,7,8,9,10,13)],value=13,description='GOES band')
    button=w.Button(description='Load pair',tooltip='Read the selected forecast hour and GOES frame')
    output=w.Output()

    def load(_):
        button.disabled=True
        try:
            with output:
                output.clear_output(wait=True)
                if not day.value or not hour.value:
                    print('Choose a UTC date and time.')
                    return
                stamp=utc(f'{day.value}T{hour.value}Z')
                with TemporaryDirectory(prefix='ecore-nam-viewer-') as tmp:
                    result=ForecastArchive(Budget(Path(tmp)/'transfer.json',256*1024**2)).select_nam(stamp,field.value,read=True)
                fig,axes=plt.subplots(1,2,figsize=(12,5),layout='constrained')
                if result['status']=='available':
                    sample=result['sample']
                    plot=map_field(axes[0],sample['longitude'],sample['latitude'],sample['values'])
                    fig.colorbar(plot,ax=axes[0],label=sample['units'])
                    axes[0].set_title(f"NAM {field.value} · lead {result['lead_hours']} h\nRun {result['initialization']} · valid {result['valid_time']}")
                    print(f"GRIB step {sample['step_range']} ({sample['step_type']}). Archive availability {result['available_in_s3']}.")
                else:
                    missing_panel(axes[0],'No eligible NAM forecast')
                    print(result['skipped'])
                try:
                    record,dataset,grid=select_goes(location.value,stamp,band.value)
                    frame=regrid(decode_observation(dataset),geometry(dataset,grid,'bilinear'))
                    plot=map_field(axes[1],grid.longitude,grid.latitude,frame['values'])
                    fig.colorbar(plot,ax=axes[1],label='K' if band.value>=7 else 'Reflectance factor')
                    axes[1].set_title(f"GOES C{band.value:02} · scan end {record['end_time']}")
                except (ValueError,OSError,KeyError) as exc:
                    missing_panel(axes[1],str(exc))
                display(fig)
                plt.close(fig)
        except (ValueError,OSError) as exc:
            with output:
                print(exc)
        finally:
            button.disabled=False
    button.on_click(load)
    return w.VBox([location,w.HBox([day,hour,product]),w.HBox([field,band,button]),output])
