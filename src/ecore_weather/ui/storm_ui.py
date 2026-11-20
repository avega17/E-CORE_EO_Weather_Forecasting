"""Select and compare observed and forecast storm frames."""
import json
import os
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory

import numpy as np

from ecore_weather.processing.storm_demo import forecast_rows, load_flash, sha256
from ecore_weather.processing.storm_cube import NAM_LABELS, open_hub_cube, read_window
from ecore_weather.processing.transforms import BANDS


class ForecastWorker:
    """A notebook owns one GPU child and closes it when its controls are replaced."""

    def __init__(self, python):
        from ecore_weather.core.paths import repository_root
        self.process = subprocess.Popen(
            [python, '-u', '-m', 'ecore_weather.processing.storm_demo'],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
            env={**os.environ, 'PYTHONPATH': str(repository_root() / 'src'), 'MPLBACKEND': 'Agg'},
        )

    def forecast(self, arrays, hour, elevation):
        with TemporaryDirectory(prefix='ecore-flash-result-') as folder:
            inputs = Path(folder) / 'history.npz'
            output = Path(folder) / 'prediction.npz'
            np.savez(inputs, **arrays)
            self.process.stdin.write(json.dumps(dict(input=str(inputs), output=str(output),
                                                    hour=hour, elevation=str(elevation))) + '\n')
            self.process.stdin.flush()
            reply = self.process.stdout.readline()
            if not reply:
                raise RuntimeError('The Flash GPU worker exited. Run the forecast again to restart it.')
            response = json.loads(reply)
            if response['status'] != 'complete':
                raise RuntimeError(response['reason'])
            with np.load(output, allow_pickle=False) as saved:
                return saved['forecasts'], response['times']

    def close(self):
        if self.process.poll() is None:
            self.process.stdin.close()
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.process.terminate()
                self.process.wait(timeout=10)


def render_comparison(root, arrays, forecasts, times, nam_rows, band, nam_key, nam_index):
    from matplotlib import pyplot as plt
    from ecore_weather.ui.forecast_ui import map_field, missing_panel

    index = BANDS.index(band)
    truth = np.where(arrays['pixel_valid'][6:, index], arrays['values'][6:, index], np.nan)
    predicted = forecasts[:, index]
    common = np.concatenate([data[np.isfinite(data)] for data in (predicted, truth)])
    low, high = np.percentile(common, [2, 98]) if common.size else (0, 1)
    fig, axes = plt.subplots(6, 3, figsize=(13, 20), layout='constrained')
    nam_images = []
    nam_label = NAM_LABELS[nam_key]
    if nam_rows:
        ds = root['nam']
        coordinates = {k: ds[k][:] for k in ('latitude', 'longitude')}
        nam_images = [coordinates | {'values': values} for values in ds[nam_key][nam_index]]
    pixels = np.concatenate([a['values'][np.isfinite(a['values'])] for a in nam_images]) if nam_images else np.array([])
    nlow, nhigh = np.percentile(pixels, [2, 98]) if pixels.size else (0, 1)
    for i, (row, valid) in enumerate(zip(axes, times)):
        for axis, label, values in zip(row[:2], (f'Flash +{10*(i+1)} min', 'GOES observation'),
                                       (predicted[i], truth[i])):
            if np.isfinite(values).any():
                mesh = map_field(axis, arrays['longitude'], arrays['latitude'], values, vmin=low, vmax=high)
                fig.colorbar(mesh, ax=axis, label='K' if band >= 7 else 'Reflectance factor')
            else:
                missing_panel(axis, 'No valid pixels')
            axis.set_title(f'{label} · {valid[11:16]} UTC')
        if nam_images:
            j = int(i == 5)
            meta, values = nam_rows[j], nam_images[j]
            mesh = map_field(row[2], values['longitude'], values['latitude'], values['values'],
                             cmap='turbo', vmin=nlow, vmax=nhigh)
            fig.colorbar(mesh, ax=row[2], label=meta['units'])
            interval = f" · {meta['step_range']} h accumulation" if meta.get('step_type') == 'accum' else ''
            row[2].set_title(f"NAM {nam_label} · valid {meta['valid_time'][11:16]} UTC\n"
                             f"{meta['initialization'][11:16]} UTC run · +{meta['lead_hours']} h{interval}")
        else:
            missing_panel(row[2], 'No eligible NAM forecast pair for this field')
    fig.suptitle('Experimental Caribbean adaptation with approximate regional topography')
    return fig


def create_storm_demo(repo_id='', local='', model_python=None, catalog_revision=None):
    import atexit
    import ipywidgets as w
    from IPython.display import display
    from matplotlib import pyplot as plt

    repo = w.Text(value=repo_id, description='HF dataset')
    disk = w.Text(value=local, description='Local copy')
    load = w.Button(description='Load catalog')
    storm = w.Dropdown(description='Storm')
    hour = w.Dropdown(description='Hour UTC')
    band = w.Dropdown(options=[(f'C{b:02}', b) for b in BANDS], value=13, description='GOES band')
    nam = w.Dropdown(options=[(label, key) for key, label in NAM_LABELS.items()],
                     value='composite_reflectivity', description='NAM field')
    corrected = w.Checkbox(description='Parallax corrected', indent=False)
    consent = w.Checkbox(description='Use experimental Caribbean adapter and approximate topography',
                         indent=False, layout=w.Layout(width='auto'))
    run = w.Button(description='Run forecast', disabled=True)
    out = w.Output()
    state = {}

    def fetch(name, revision=None):
        if disk.value.strip():
            return Path(disk.value.strip()) / name
        from huggingface_hub import hf_hub_download
        return Path(hf_hub_download(repo.value.strip(), name, repo_type='dataset',
                                    revision=revision, token=False))

    def key():
        return storm.value, hour.value, corrected.value

    def render(_=None):
        if state.get('key') != key():
            return
        with out:
            out.clear_output(wait=True)
            try:
                entry = state['catalog']['storms'][storm.value]['hours'][hour.value]
                figure = render_comparison(state['cube'], state['arrays'], state['prediction'], state['times'],
                                           entry['nam'].get(nam.value, ()), band.value, nam.value, entry['nam_index'])
                display(figure)
                plt.close(figure)
                arrays = state['arrays']
                observed = arrays['pixel_valid'][6:, BANDS.index(band.value)].any(axis=(-2,-1))
                if not observed.all():
                    print(f'Observed comparison frames available  {observed.sum()}/6')
                if corrected.value:
                    applied = arrays['parallax_applied'][:6, BANDS.index(band.value)]
                    print(f'Input pixels corrected for this band  {applied.mean():.1%}')
                    print('Cloud-height correction applies to C01/C02/C03/C07/C13 where supported.')
                if problem := entry.get('nam_read_issues', {}).get(nam.value):
                    print(f'NAM read issue  {problem}')
            except Exception as exc:
                print(f'{type(exc).__name__}: {exc}')

    def choose(_=None):
        run.disabled = hour.value is None
        if state.get('key') == key():
            render()
        elif key() in state.get('cache', {}):
            arrays, prediction, times = state['cache'][key()]
            state.update(key=key(), arrays=arrays, prediction=prediction, times=times)
            render()
        else:
            with out:
                out.clear_output(wait=True)
                if hour.value:
                    print('Run forecast to compare the next six ten-minute steps.')

    def choose_storm(_=None):
        if 'catalog' in state and storm.value:
            hour.options = list(state['catalog']['storms'][storm.value]['hours'])
        choose()

    def catalog(_):
        with out:
            out.clear_output(wait=True)
            try:
                path = fetch('catalog.json', catalog_revision)
                data = json.loads(path.read_text())
                if data.get('schema') != 2:
                    raise ValueError('Choose a published v2 storm Zarr catalog')
                if disk.value.strip():
                    import zarr
                    cube = zarr.open_group(str(path.parent / data['store']), mode='r', use_consolidated=False)
                else:
                    cube = open_hub_cube(repo.value.strip(), path, data['store'])
                state.update(catalog=data, cube=cube, revision=None if disk.value.strip() else path.parent.name,
                             key=None, cache={})
                corrected.disabled = 'corrected' not in data['variants']
                storm.options = [(item['name'], slug) for slug, item in data['storms'].items()]
                choose_storm()
            except Exception as exc:
                print(f'{type(exc).__name__}: {exc}')

    def draw(_):
        if not consent.value:
            with out:
                print('Confirm the experimental adapter before starting the GPU run.')
            return
        run.disabled = load.disabled = True
        try:
            with out:
                out.clear_output(wait=True)
                identity = key()
                if identity in state['cache']:
                    arrays, prediction, times = state['cache'][identity]
                else:
                    entry = state['catalog']['storms'][storm.value]['hours'][hour.value]
                    arrays = read_window(state['cube'], entry, hour.value, corrected.value)
                    paths = {}
                    for name, digest in state['catalog']['elevation'].items():
                        path = fetch(name, state['revision'])
                        if sha256(path) != digest:
                            raise ValueError(f'Demo elevation checksum differs for {name}')
                        paths[name] = path
                    elevation = paths['caribbean-elevation.tif']
                    if model_python:
                        worker = state.get('worker')
                        if worker is None or worker.process.poll() is not None:
                            worker = state['worker'] = ForecastWorker(model_python)
                            atexit.register(worker.close)
                        prediction, times = worker.forecast(arrays, hour.value, elevation)
                    else:
                        if 'model' not in state:
                            state['model'] = load_flash()
                        rows = list(forecast_rows(state['model'], arrays, hour.value, elevation))
                        prediction = np.stack([row['forecast'] for row in rows])
                        times = [row['valid_time'] for row in rows]
                    if len(state['cache']) == 3:
                        del state['cache'][next(iter(state['cache']))]
                    state['cache'][identity] = arrays, prediction, times
                state.update(key=identity, arrays=arrays, prediction=prediction, times=times)
            render()
        except Exception as exc:
            with out:
                print(f'{type(exc).__name__}: {exc}')
        finally:
            run.disabled = hour.value is None
            load.disabled = False

    load.on_click(catalog)
    storm.observe(choose_storm, names='value')
    hour.observe(choose, names='value')
    corrected.observe(choose, names='value')
    for control in (band, nam):
        control.observe(render, names='value')
    run.on_click(draw)
    return w.VBox([repo, disk, load, w.HBox([storm, hour]), w.HBox([band, nam]), corrected, consent, run, out])
