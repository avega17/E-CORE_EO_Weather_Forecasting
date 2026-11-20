"""Controls for the single managed processing workflow."""
from __future__ import annotations

import json
from pathlib import Path


def processing_sections():
    import datetime
    import subprocess
    import sys

    import ipywidgets as w
    from IPython.display import display
    from ecore_weather.core.paths import repository_root, result_path
    from ecore_weather.processing.managed_store import primary_store, writer_active
    from ecore_weather.processing.process_cli import parser, run

    base = Path('/mnt/p/ecore_eo_datasets')
    location = w.Text(value=str(base), description='Native data', layout=w.Layout(width='95%'))
    destination = w.Text(value=str(base / 'processed'), description='Output root', layout=w.Layout(width='95%'))
    start = w.DatePicker(value=datetime.date(2022, 9, 18), description='Start UTC')
    end = w.DatePicker(value=datetime.date(2022, 9, 19), description='End UTC', tooltip='End excluded')
    resolution = w.Dropdown(options=[(f'{n} km', n) for n in (1,2,3)], value=3, description='Grid')
    correction = w.Checkbox(description='Apply cloud-height correction', indent=False)
    quality = w.Dropdown(options=[('Good pixels', 'good'), ('Good and conditional', 'conditional')], description='Quality')
    bands = w.SelectMultiple(options=[(f'C{n:02}',n) for n in (1,2,3,7,8,9,10,13)],
                            value=(1,2,3,7,8,9,10,13), description='GOES bands', rows=4)
    radar = w.SelectMultiple(options=[('Composite reflectivity','refc'), ('Base reflectivity','refc_base')],
                            value=('refc','refc_base'), description='Radar', rows=2)
    workers = w.BoundedIntText(value=8, min=0, max=1024, description='Processes')
    memory = w.BoundedIntText(value=32768, min=1024, max=1048576, description='Memory MiB')
    layout = w.Dropdown(options=[('Single-frame chunks', (1,0)), ('Six-frame chunks', (6,0)),
                                 ('24-frame shards', (1,24))], description='Layout')
    advanced = w.Accordion(children=[w.VBox([workers,memory,layout])], selected_index=None)
    advanced.set_title(0, 'Advanced processing settings')
    refresh = w.Button(description='Refresh status')
    status_out = w.Output()
    build_out = w.Output()
    preview_out = w.Output()
    check_out = w.Output()
    review = w.Button(description='Review build')
    launch = w.Button(description='Build processed dataset', disabled=True, button_style='primary',
                      layout=w.Layout(width='215px'))
    replace = w.Checkbox(description='Delete the named store and build this selection', indent=False,
                         layout=w.Layout(width='auto'), disabled=True)
    unit = w.Text(description='Store path', layout=w.Layout(width='95%'))
    variable = w.Dropdown(description='Variable', options=[f'abi{n:02}c' for n in (1,2,3,7,8,9,10,13)]
                                                           + ['refc','refc_base'], value='abi13c')
    day = w.DatePicker(value=datetime.date(2022,9,18), description='Date UTC')
    clock = w.TimePicker(value=datetime.time(16,10), description='Time UTC')
    save = w.Checkbox(description='Save figure', indent=False)
    view = w.Button(description='Preview processed')
    parallax = w.Button(description='Compare parallax')
    validate = w.Button(description='Validate store')
    report = w.Button(description='Export report')
    state = {}
    result_root = result_path('dataset-processing/notebook-build')

    def current(_=None):
        with status_out:
            status_out.clear_output(wait=True)
            path = primary_store(destination.value)
            if path:
                selected = path / 'full'
                unit.value = str(selected)
                print(f'Managed store  {selected}')
                status = selected / 'progress.json'
                if status.is_file():
                    print(f"Verified slots  {json.loads(status.read_text())['verified_slots']:,}")
                if writer_active(selected):
                    print('A writer is active')
                else:
                    print('Complete' if (selected / 'complete.json').is_file() else 'Incomplete and resumable')
            else:
                print('No managed processed store found')
            heights = result_path('dataset-processing/heights/height-summary.json')
            if heights.is_file():
                status = json.loads(heights.read_text())
                print(f"Height fetch  {status['status']}. Latest verified  {status.get('latest', 'unavailable')}")
            job = state.get('job')
            if job:
                code = job.poll()
                print('Notebook build is running' if code is None else f'Notebook build exit code  {code}')
            log = result_root / 'build.log'
            if log.is_file():
                with log.open('rb') as stream:
                    stream.seek(max(0, log.stat().st_size - 1200))
                    recent = stream.read().decode(errors='replace').splitlines()
                print('\n'.join(recent[-3:]))

    def args(operation):
        if not start.value or not end.value or start.value >= end.value:
            raise ValueError('Choose an increasing date range')
        return [operation, '--location', location.value, '--destination', destination.value,
            '--preparation-root', str(Path(location.value) / 'preparation'),
            '--start', start.value.isoformat(), '--end', end.value.isoformat(),
            '--resolution-km', str(resolution.value), '--quality', quality.value,
            '--correction', 'cloud-height' if correction.value else 'none',
            '--workers', str(workers.value), '--memory-mib', str(memory.value),
            '--chunk-time', str(layout.value[0]), '--shard-time', str(layout.value[1]),
            '--bands', *map(str,bands.value), '--radar', *radar.value,
            '--output', str(result_root)]

    def invalidate(_=None):
        state.pop('review', None)
        launch.disabled = replace.disabled = True
        replace.value = False

    def review_build(_):
        invalidate()
        with build_out:
            build_out.clear_output(wait=True)
            try:
                existing = primary_store(destination.value)
                if existing and writer_active(existing / 'full'):
                    print('The current store has a writer. Review again after it exits.')
                    return
                configuration = args('estimate')
                parsed = parser().parse_args(configuration)
                index = run(parser().parse_args(args('index')))
                selection = ['--selection', index['preparation_path']]
                estimate = run(parser().parse_args(configuration + selection))
                target = Path(destination.value).resolve() / index['selection_id']
                previous = existing if existing and existing != target else None
                print(f'New selection  {parsed.start} to {parsed.end} UTC (end excluded), '
                      f'{resolution.value} km, {parsed.correction}')
                print(f"Estimated storage  {estimate['estimated_stored_bytes']/1e9:.2f} GB")
                print(f'Target  {target}')
                if previous:
                    import zarr
                    old = zarr.open_group(str(previous/'full/data.zarr'), mode='r', use_consolidated=False)
                    times = old['time']
                    marker = previous/'full/complete.json'
                    storage = json.loads(marker.read_text()).get('stored_bytes') if marker.is_file() else None
                    if storage is None:
                        storage = sum(p.stat().st_size for p in previous.rglob('*') if p.is_file())
                    print(f"Will delete  {previous}\nCurrent store  {times[0]} to {times[-1]}, "
                          f"{old.attrs['recipe']['resolution_km']} km, {old.attrs['recipe']['parallax']}, {storage/1e9:.2f} GB")
                    print('An interrupted replacement can resume from its own verified slots.')
                elif existing:
                    print('Resume or reuse the existing selection.')
                if existing and writer_active(existing / 'full'):
                    print('The current store has a writer. Review again after it exits.')
                    return
                state['review'] = (args('materialize') + selection + ['--managed-store'], previous)
                replace.disabled = previous is None
                launch.disabled = previous is not None
            except Exception as exc:
                print(f'{type(exc).__name__}: {exc}')

    def start_build(_):
        with build_out:
            if 'review' not in state:
                return
            configuration, previous = state['review']
            if previous and not replace.value:
                return
            job = state.get('job')
            if job and job.poll() is None:
                print('A notebook build is already running')
                return
            result_root.mkdir(parents=True, exist_ok=True)
            if previous:
                configuration += ['--replace-store', str(previous)]
            with (result_root / 'build.log').open('w') as log:
                state['job'] = subprocess.Popen([sys.executable, str(repository_root()/'scripts/dataset_jobs.py'),
                    'process', *configuration], stdout=log, stderr=subprocess.STDOUT)
            print('Build submitted. Use Refresh status to follow verified slots.')
            invalidate()

    def preview(corrected):
        def click(_):
            with preview_out:
                preview_out.clear_output(wait=True)
                try:
                    from matplotlib import pyplot as plt
                    stamp = f'{day.value}T{clock.value}Z'
                    if corrected:
                        from ecore_weather.ui.forecast_ui import parallax_comparison
                        if variable.value not in ('abi01c','abi02c','abi03c','abi07c','abi13c'):
                            raise ValueError('Choose C01/C02/C03/C07/C13 for the correction preview')
                        figure = parallax_comparison(location.value, stamp, int(variable.value[3:5]), resolution.value)
                    else:
                        from ecore_weather.processing.process_store import ProcessedSource
                        from ecore_weather.processing.storm_demo import as_time
                        with ProcessedSource(Path(unit.value), allow_partial=True) as source:
                            frame = source([as_time(stamp)], [variable.value]).isel(time=0, variable=0)
                        figure, axis = plt.subplots(figsize=(8,6), layout='constrained')
                        frame.plot(ax=axis)
                    if save.value:
                        folder = repository_root()/'figures/dataset-processing'
                        folder.mkdir(parents=True,exist_ok=True)
                        figure.savefig(folder/f'{variable.value}-{day.value}-{"parallax" if corrected else "preview"}.png', dpi=150)
                    display(figure)
                    plt.close(figure)
                except Exception as exc:
                    print(f'{type(exc).__name__}: {exc}')
        return click

    def check(action):
        def click(_):
            with check_out:
                check_out.clear_output(wait=True)
                try:
                    if action == 'validate':
                        from ecore_weather.processing.process_store import reader_lease, validate_store
                        with reader_lease(unit.value):
                            print(validate_store(unit.value))
                    else:
                        print(export_report(result_path('dataset-processing'))['path'])
                except Exception as exc:
                    print(f'{type(exc).__name__}: {exc}')
        return click

    for control in (location,destination,start,end,resolution,correction,quality,bands,radar,workers,memory,layout):
        control.observe(invalidate, names='value')
    replace.observe(lambda change: setattr(launch, 'disabled', not change['new']), names='value')
    refresh.on_click(current)
    review.on_click(review_build)
    launch.on_click(start_build)
    view.on_click(preview(False))
    parallax.on_click(preview(True))
    validate.on_click(check('validate'))
    report.on_click(check('report'))
    return (
        w.VBox([refresh,status_out]),
        w.VBox([location,destination,w.HBox([start,end]),w.HBox([resolution,quality]),correction,
                w.HBox([bands,radar]),advanced]),
        w.VBox([review,replace,launch,build_out]),
        w.VBox([unit,variable,w.HBox([day,clock]),save,w.HBox([view,parallax]),preview_out]),
        w.VBox([w.HBox([validate,report]),check_out]),
    )
def export_report(output):
    """One readable Markdown report from compact bounded-experiment receipts."""
    root=Path(output);root.mkdir(parents=True,exist_ok=True)
    sections=['# Dataset processing results','Generated from bounded scientific processing receipts.']
    for name in ['storage-benchmark','worker-benchmark','parallax-pilot','loader-check','model-reference','model-check','caribbean-comparison','nam-availability','retired-stores']:
        path=root/(name+'.json')
        if not path.exists():continue
        data=json.loads(path.read_text());sections+=['',f'## {name.replace("-"," ").title()}','']
        import pandas as pd
        if isinstance(data.get('cases'),list):
            frame=pd.DataFrame(data['cases'])
            nested=[column for column in frame if frame[column].map(lambda v:isinstance(v,(dict,list))).any()]
            sections.append(frame.drop(columns=nested).to_markdown(index=False))
        elif name=='caribbean-comparison':
            frame=pd.DataFrame(data['comparisons'])
            sections.append(data['scope'])
            sections.append(frame[['variable','units','lead_minutes','mae','persistence_mae','rmse','persistence_rmse']].to_markdown(index=False,floatfmt='.4f'))
            if data.get('figure'):
                import os
                figure=os.path.relpath(Path(data['figure']).resolve(),root.resolve())
                sections.append(f'[Observed and forecast images]({figure})')
        elif name=='storage-benchmark':
            for key in ('disk','linux_disk'):
                if key in data:
                    sections+=['',f'### {key.replace("_"," ").title()}',
                        pd.DataFrame(data[key]['cases']).groupby('operation').agg(
                            median_GB_s=('GB_per_second','median')).to_markdown(),
                        data[key]['cache_scope']]
            for key in ('windows','linux_windows'):
                if key in data:
                    frame=pd.DataFrame(data[key]['cases'])
                    sections+=['',f'### {key.replace("_"," ").title()} — disposable Zarr layout trials']
                    groups=[c for c in ('compression','chunk_time','shard_time','spatial_chunk','frames','readers') if c in frame]
                    sections.append(frame.groupby(groups).agg(
                        median_windows_s=('windows_per_second','median'),median_latency_s=('median_window_seconds','median')).to_markdown())
            if 'current_snapshot_reads' in data:
                frame=pd.DataFrame(data['current_snapshot_reads']['cases'])
                groups=(['cache_metadata'] if 'cache_metadata' in frame else [])+['frames','readers']
                sections+=['','### Actual plain-Zarr store and model-facing reader',
                    frame.groupby(groups).agg(
                        median_windows_s=('windows_per_second','median'),median_latency_s=('median_window_seconds','median')).to_markdown()]
            sections.append(data.get('model_consumption','Model consumption was not measured.'))
        else:sections.extend(f'- {k}: {v}' for k,v in data.items() if not isinstance(v,(dict,list)))
    target=root/'report.md';target.write_text('\n'.join(sections)+'\n')
    return {'path':str(target),'status':'complete'}
