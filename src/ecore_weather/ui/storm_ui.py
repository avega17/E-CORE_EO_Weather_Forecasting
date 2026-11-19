"""Lazy single-block downloads and GPU execution for notebook 08."""
import json
from pathlib import Path

from ecore_weather.processing.storm_demo import (
    load_flash, forecast_rows, read_verified, render_rows, sha256,
)


def create_storm_demo(repo_id='', local='', model_python=None):
    import ipywidgets as w
    from IPython.display import display
    from matplotlib import pyplot as plt
    repo=w.Text(value=repo_id,description='HF dataset',layout=w.Layout(width='96%'))
    disk=w.Text(value=local,description='Local copy',layout=w.Layout(width='96%'))
    load=w.Button(description='Load catalog')
    storm=w.Dropdown(description='Storm')
    hour=w.Dropdown(description='Hour UTC')
    band=w.Dropdown(options=[(f'C{b:02}',b) for b in (1,2,3,7,8,9,10,13)],value=13,description='GOES band')
    run=w.Button(description='Run comparison',disabled=True,layout=w.Layout(min_width='155px'))
    user_accepts=w.Checkbox(description='Use the experimental Caribbean adapter and approximate topography',
                           indent=False,layout=w.Layout(width='96%'))
    out=w.Output()
    state={}

    def fetch(name):
        if state['local']:
            return Path(state['local'])/name
        from huggingface_hub import hf_hub_download
        return Path(hf_hub_download(state['repo'],name,repo_type='dataset',
                                   revision=state.get('revision'),token=False))

    def choose_storm(_=None):
        selected=state.get('catalog',{}).get('storms',{}).get(storm.value,{})
        hour.options=[t for block in selected.get('blocks',[]) for t in block['hours']]
        run.disabled=not bool(hour.options)
        if selected:
            with out:
                out.clear_output(wait=True)
                print(f"{len(hour.options)} hours published. {len(selected['unavailable_hours'])} hours awaiting verified observations.")

    def catalog(_):
        with out:
            out.clear_output(wait=True)
            try:
                state.update(local=disk.value.strip(),repo=repo.value.strip(),revision=None)
                if not state['local'] and not state['repo']:
                    raise ValueError('Enter the public dataset ID or a local demo directory')
                path=fetch('catalog.json')
                if not state['local'] and path.parent.parent.name=='snapshots':
                    state['revision']=path.parent.name
                state['catalog']=json.loads(path.read_text())
                storm.options=[(row['name'],key) for key,row in state['catalog']['storms'].items()]
                choose_storm()
            except (ValueError,OSError,RuntimeError) as exc:
                print(exc)

    def draw(_):
        if not user_accepts.value:
            with out:print('Confirm the experimental adapter before starting the GPU run.')
            return
        run.disabled=True
        load.disabled=True
        try:
            with out:
                out.clear_output(wait=True)
                selected=state['catalog']['storms'][storm.value]
                block=next(b for b in selected['blocks'] if hour.value in b['hours'])
                payload=fetch(block['file'])
                arrays=read_verified(payload,block['sha256'])
                for filename,digest in state['catalog']['elevation'].items():
                    path=fetch(filename)
                    if sha256(path)!=digest:
                        raise IOError(f'Elevation checksum differs for {filename}')
                identity=(block['sha256'],hour.value)
                if state.get('last_forecast')!=identity:
                    elevation=fetch('caribbean-elevation.tif')
                    if model_python:
                        import os
                        import subprocess
                        from tempfile import TemporaryDirectory
                        import numpy as np
                        from ecore_weather.core.paths import repository_root
                        source=str(repository_root()/'src')
                        with TemporaryDirectory(prefix='ecore-flash-result-') as folder:
                            result=Path(folder)/'forecast.npz'
                            subprocess.run([model_python,'-m','ecore_weather.processing.storm_demo',
                                str(payload),block['sha256'],hour.value,str(elevation),str(result)],check=True,
                                env={**os.environ,'PYTHONPATH':source,'MPLBACKEND':'Agg'})
                            with np.load(result,allow_pickle=False) as saved:
                                state['rows']=[]
                                for i in range(3):
                                    row={key:saved[f'{i}_{key}'].item() for key in ('valid_time','initialized')}
                                    row.update(truth=saved[f'{i}_truth'],forecast=saved.get(f'{i}_forecast'))
                                    state['rows'].append(row)
                    else:
                        if 'model' not in state:
                            state['model']=load_flash()
                        state['rows']=list(forecast_rows(state['model'],arrays,hour.value,elevation))
                    state['last_forecast']=identity
                rows=state['rows']
                figure=render_rows(rows,arrays,block,hour.value,band.value)
                display(figure)
                plt.close(figure)
                import numpy as np
                from ecore_weather.processing.transforms import BANDS
                channel=BANDS.index(band.value)
                for row in rows:
                    if row['forecast'] is None:
                        continue
                    forecast,truth=row['forecast'][channel],row['truth'][channel]
                    valid=np.isfinite(forecast)&np.isfinite(truth)
                    if valid.any():
                        unit='K' if band.value>=7 else 'reflectance factor'
                        mae=np.abs(forecast[valid]-truth[valid]).mean()
                        print(f"{row['valid_time']} · GOES MAE {mae:.3f} {unit} across {valid.sum():,} pixels")
        except (ValueError,RuntimeError,OSError) as exc:
            with out:print(exc)
        finally:
            run.disabled=not bool(hour.options)
            load.disabled=False
    load.on_click(catalog)
    storm.observe(choose_storm,names='value')
    run.on_click(draw)
    return w.VBox([repo,disk,load,w.HBox([storm,hour,band]),user_accepts,run,out])
