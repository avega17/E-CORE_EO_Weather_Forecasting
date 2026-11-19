"""Readable processing controls; construction never reads imagery or starts work."""
from __future__ import annotations

import json
from pathlib import Path


def create():
    import ipywidgets as w
    from IPython.display import display,Markdown
    from ecore_weather.processing.process_cli import parser, run
    location=w.Text(value='/mnt/p/ecore_eo_datasets',description='Raw data',layout=w.Layout(width='95%'),
        tooltip='Root containing verified native goes/ and mrms/ monthly archives')
    destination=w.Text(value='/mnt/p/ecore_eo_datasets/processed',description='Derived',layout=w.Layout(width='95%'),
        tooltip='Separate DAS directory on workstation; job-local scratch on Polaris')
    preparation=w.Text(value='/mnt/p/ecore_eo_datasets/preparation',description='Prep path',layout=w.Layout(width='95%'),
        tooltip='Virtual catalog, temporal index and reusable geometry; no full transformed observations')
    import datetime
    start=w.DatePicker(value=datetime.date(2022,9,18),description='Start UTC')
    end=w.DatePicker(value=datetime.date(2022,9,19),description='End UTC',tooltip='Exclusive ending date')
    resolution=w.Dropdown(options=[('1 km',1),('2 km',2),('3 km',3)],value=1,description='Grid')
    quality=w.Dropdown(options=[('Good pixels','good'),('Good + conditional','conditional')],description='Quality')
    mode=w.Dropdown(options=['auto','full','blocks'],description='Storage',tooltip='Auto uses the full selection when it fits with a 5% free-space reserve')
    layout=w.Dropdown(options=[('Frame chunks',(1,0)),('Six-frame chunks',(6,0)),('24-frame shards',(1,24))],description='Layout',tooltip='Measured layout choices; complete shards are written before checkpointing')
    correction=w.Dropdown(options=[('Uncorrected','none'),('Cloud heights','cloud-height')],description='Parallax',tooltip='C01/C02/C03/C07/C13 only; requires matched height data')
    compression=w.Dropdown(options=['zstd','none'],description='Codec',tooltip='Lossless Zstd by default; uncompressed only when measured and affordable')
    workers=w.BoundedIntText(value=0,min=0,max=1024,description='Processes',tooltip='0 uses all CPUs allowed by this machine/job; each process opens independent native archives')
    memory=w.BoundedIntText(value=32768,min=1024,max=1048576,description='Memory MiB',tooltip='Budget for process, decoded-source and prefetched output buffers; leave RAM for the operating system')
    bands=w.SelectMultiple(options=[(f'C{n:02d}',n) for n in [1,2,3,7,8,9,10,13]],
        value=(1,2,3,7,8,9,10,13),description='GOES bands',rows=4)
    radar=w.SelectMultiple(options=[('Composite reflectivity','refc'),('Base reflectivity','refc_base')],
        value=('refc','refc_base'),description='Radar',rows=2,
        tooltip='Use Ctrl/Cmd to select products; clear the selection for GOES only')
    selection=w.Text(description='Selection',layout=w.Layout(width='95%'),tooltip='Filled after indexing; paste an existing preparation selection path to reuse it')
    unit=w.Text(description='Store',layout=w.Layout(width='95%'),tooltip='Completed full/block directory, not a raw monthly archive')
    def changed_selection(change):
        selection.value='';unit.value=''
    for control in (location,start,end,resolution,quality,compression,bands,radar,layout,correction):
        control.observe(changed_selection,names='value')
    save=w.Checkbox(value=False,description='Save preview to figures/',indent=False)
    output=w.Output();buttons=[]
    instructions=w.HTML('<p>Choose completed native data and a separate output path. <b>Index</b> finds observations; '
        '<b>Estimate</b> reads a small sample. <b>Materialize</b> writes the chosen derived variant. '
        'Changing scientific choices clears the saved selection: click <b>Index</b> again. '
        'A virtual catalog alone does not interpolate data. Dates are UTC and the ending date is excluded.</p>')
    def arguments(operation):
        values=[operation,'--location',location.value,'--destination',destination.value,'--preparation-root',preparation.value,
            '--start',start.value.isoformat(),'--end',end.value.isoformat(),'--resolution-km',str(resolution.value),
            '--quality',quality.value,'--mode',mode.value,'--compression',compression.value,
            '--chunk-time',str(layout.value[0]),'--shard-time',str(layout.value[1]),'--correction',correction.value,
            '--workers',str(workers.value),'--memory-mib',str(memory.value)]
        values+=['--bands',*[str(n) for n in bands.value],'--radar',*radar.value]
        if selection.value and operation not in ('index','inventory'):values+=['--selection',selection.value]
        if unit.value:values+=['--unit',unit.value]
        return parser().parse_args(values)
    def action(operation):
        def clicked(_):
            for button in buttons:button.disabled=True
            with output:
                output.clear_output(wait=True)
                try:
                    result=run(arguments(operation))
                    if result.get('preparation_path'):selection.value=result['preparation_path']
                    if result.get('units'):
                        unit.value=result['units'][0]['path']
                        variable.options=result['units'][0].get('variables',variable.options)
                    import pandas as pd
                    display(pd.DataFrame([{k:v for k,v in result.items() if not isinstance(v,(dict,list))}]).T.rename(columns={0:'Result'}))
                    if result.get('absent_variables'):display(Markdown('Missing products: '+', '.join(result['absent_variables'])))
                    if result.get('cases'):display(pd.DataFrame(result['cases']))
                except Exception as exc:display(Markdown(f'**{type(exc).__name__}:** {exc}'))
                finally:
                    for button in buttons:button.disabled=False
        return clicked
    actions=[]
    for label,operation,tip in [('Index','index','Build causal observation and model-window indices'),
        ('Virtual catalog','catalog','Reference native ZIP chunks without copying pixels'),
        ('Estimate','estimate','Read representative ROI samples and estimate complete-variant storage'),
        ('Materialize','materialize','Write physical arrays and masks to the Derived path'),
        ('Validate','validate','Reopen and check all committed processing batches'),
        ('Statistics','stats','Export training-only moments and geographic invariants'),
        ('Benchmark','benchmark','Run bounded storage tests; requires the supported fetch pause'),
        ('Parallax pilot','parallax','Validate C01/C02/C03/C07/C13 with bounded cloud-height examples'),
        ('Model check','model-check','Report model/data requirements; CLI opts in to experimental geography')]:
        button=w.Button(description=label,tooltip=tip,layout=w.Layout(min_width='145px',width='auto'))
        button.on_click(action(operation));buttons.append(button);actions.append(button)
    preview=w.Button(description='Preview',tooltip='Read one completed derived sample',layout=w.Layout(min_width='145px'))
    variable=w.Dropdown(options=[f'abi{n:02d}c' for n in [1,2,3,7,8,9,10,13]]+['refc','refc_base'],description='Variable')
    def draw(_):
        with output:
            output.clear_output(wait=True)
            try:
                import numpy as np,matplotlib.pyplot as plt
                from ecore_weather.processing.process_store import ProcessedSource
                path=Path(unit.value);state=json.loads((path/'progress.json').read_text())
                import zarr
                raw=zarr.open_group(str(path/'data.zarr'),mode='r')
                choices=raw['variable'][:].tolist()
                if variable.value not in choices:variable.options=choices
                times=raw['time'][:]
                with ProcessedSource(path) as source:
                    sample=source([times[len(times)//2]],[variable.value]).isel(time=0,variable=0)
                figure,axis=plt.subplots(figsize=(8,6),layout='constrained')
                sample.plot(ax=axis);axis.set_title(f'{variable.value} · {str(sample.time.values)}')
                if save.value:
                    target=Path('figures/dataset-processing');target.mkdir(parents=True,exist_ok=True)
                    figure.savefig(target/f'{variable.value}-preview.png',dpi=150)
                display(figure);plt.close(figure)
            except Exception as exc:display(Markdown(f'**{type(exc).__name__}:** {exc}'))
    correction_day=w.DatePicker(value=datetime.date(2022,9,18),description='Preview date')
    correction_time=w.TimePicker(value=datetime.time(16,10),description='Time UTC')
    correction_band=w.Dropdown(options=[(f'C{b:02}',b) for b in (1,2,3,7,13)],value=13,description='Cloud band')
    compare=w.Button(description='Compare parallax',layout=w.Layout(min_width='150px'),
                     tooltip='Read one verified native GOES and ACHAF pair; use the selected grid')
    def compare_frame(_):
        compare.disabled=True
        try:
            with output:
                output.clear_output(wait=True)
                from matplotlib import pyplot as plt
                from ecore_weather.ui.forecast_ui import parallax_comparison
                from ecore_weather.core.paths import repository_root
                stamp=f'{correction_day.value}T{correction_time.value}Z'
                fig=parallax_comparison(location.value,stamp,correction_band.value,resolution.value)
                if save.value:
                    target=repository_root()/'figures/dataset-processing/parallax'
                    target.mkdir(parents=True,exist_ok=True)
                    fig.savefig(target/f'parallax-preview-C{correction_band.value:02d}-{correction_day.value}T{correction_time.value:%H%M}Z-{resolution.value}km.png',dpi=150)
                display(fig)
                plt.close(fig)
        except (ValueError,OSError,KeyError) as exc:
            with output: print(exc)
        finally:
            compare.disabled=False
    compare.on_click(compare_frame)
    preview.on_click(draw);buttons.append(preview)
    return w.VBox([instructions,location,preparation,destination,w.HBox([start,end]),
        w.HBox([resolution,quality]),w.Box([mode,compression,layout,correction],layout=w.Layout(display='flex',flex_flow='row wrap')),w.HBox([bands,radar]),selection,unit,
        w.Box(actions,layout=w.Layout(display='flex',flex_flow='row wrap',gap='8px')),
        w.HBox([workers,memory]),w.HBox([variable,preview,save]),
        w.HBox([correction_day,correction_time,correction_band,compare]),output])


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
