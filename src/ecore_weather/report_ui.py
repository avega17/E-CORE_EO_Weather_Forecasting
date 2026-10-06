"""Readable notebook controls. Construction does not fetch or audit arrays."""
from pathlib import Path
import ipywidgets as w
import pandas as pd
from IPython.display import display,Markdown
from . import dataset_report as report,report_benchmarks as bench
from .common import PATCHES


def summary_view(summary):
    keys=('archives','observations','selected_observations','listed_compressed_source_bytes','physical_archive_bytes','physical_zip_bytes','processed','reused')
    display(pd.DataFrame([{'Metric':key.replace('_',' ').capitalize(),'Value':summary[key]} for key in keys if key in summary]))


def cached_view(output,section,**filters):
    from .report_views import figures,audit_scope,distribution_table,measurement_table,benchmark_summary
    charts,quality,storage,performance=figures(output,**filters)
    if section=='Diagnostics':
        if quality.empty:display(Markdown('No cached audits for this code revision. Run a sample audit to populate the view.'));return
        display(pd.DataFrame([audit_scope(output,**filters)]))
        display(quality[['product','band','patch','period','observations','strict_good_percent','good_plus_conditional_percent','fill_percent','bitmap_missing_percent','quality_unknown_percent','valid_zero_percent','valid_negative_percent','mean','std']].head(100))
        for figure in charts[:4]:display(figure)
        distributions=distribution_table(output,**filters)
        display(Markdown('Each distribution belongs to one observation; these are not pooled pixel quartiles.'))
        if not distributions.empty:
            import plotly.express as px
            display(px.line(distributions[distributions.patch=='Whole ROI'],x='time',y=['q25','median','q75'],facet_row='product',title='Per-observation valid-value quartiles'))
        facts=Path(output)/'coverage-facts.json'
        if facts.exists():
            import json
            display(pd.DataFrame(json.loads(facts.read_text())).drop(columns=['coverage_facts'],errors='ignore').head(100))
    else:
        display(storage.head(100));display(performance.head(100));display(benchmark_summary(output).head(100))
        for figure in charts[4:] if not quality.empty else charts:display(figure)


def create():
    source=w.ToggleButtons(options=[('Radar','mrms'),('Satellite','goes')],description='Source')
    location=w.Text(value='/mnt/p/ecore_eo_datasets',description='Local root',layout=w.Layout(width='700px'),
        tooltip='Local dataset root or restored monthly ZIP. HF backup paths require restoring a verified month first.')
    product=w.Dropdown(options=[('All products',None)],description='Product',layout=w.Layout(width='650px'))
    band=w.Dropdown(options=[('All bands',None)],description='ABI band',disabled=True)
    start=w.DatePicker(description='Start UTC');end=w.DatePicker(description='End excluded')
    patch=w.Dropdown(options=['Whole ROI',*PATCHES],description='Patch')
    output=w.Text(value=report.DEFAULT_OUTPUT,description='Report folder',layout=w.Layout(width='700px'))
    full=w.Checkbox(value=False,description='Audit every completed observation')
    sample=w.Button(description='Audit');find=w.Button(description='Find')
    export=w.Button(description='Export');read_test=w.Button(description='Local reads')
    backend_test=w.Button(description='Backends')
    repeats=w.BoundedIntText(value=3,min=1,max=10,description='Repeats')
    remote=w.Text(description='HF root',placeholder='hf://buckets/namespace/bucket/noaa-subsets',layout=w.Layout(width='700px'))
    hf_test=w.Button(description='HF restore')
    diagnostic_output=w.Output();performance_output=w.Output();selection_output=w.Output()
    hf_find=w.Button(description='Find HF backups')
    hf_year=w.Dropdown(options=[('Choose backups first',None)],description='Year')
    hf_product=w.Dropdown(options=[('Choose year first',None)],description='Backup product',layout=w.Layout(width='650px'))
    hf_month=w.Dropdown(options=[('Choose product first',None)],description='Month')
    cache=w.Text(value='results/dataset-report/hf-cache',description='Restore cache',layout=w.Layout(width='700px'))
    hf_restore=w.Button(description='Restore month')
    hf_output=w.Output();backups={'bundles':[],'members':[],'bundle':None}
    def update_members(_=None):
        from ecore_weather.view_backup import inspect_bundle
        candidates=[b for b in backups['bundles'] if b['year']==hf_year.value and b['product']==hf_product.value]
        if not candidates:return
        if backups['bundle']==candidates[0] and backups['members']:return
        with hf_output:
            try:
                backups['bundle']=candidates[0];backups['members']=inspect_bundle(candidates[0])
                hf_month.options=[(m['month'],i) for i,m in enumerate(backups['members'])]
            except Exception as exc:print(f'{type(exc).__name__}: {exc}')
    def update_products(_=None):
        options=sorted({b['product'] for b in backups['bundles'] if b['year']==hf_year.value})
        hf_product.options=[(p,p) for p in options] or [('No products',None)]
        update_members()
    def find_backups(_):
        from ecore_weather.view_backup import list_bundles
        from ecore_weather.storage import destination_root
        with hf_output:
            hf_output.clear_output(wait=True)
            try:
                if not remote.value:remote.value=destination_root('hf')
                backups['bundles']=list_bundles(remote.value,source.value)
                years=sorted({b['year'] for b in backups['bundles']})
                hf_year.options=[(str(y),y) for y in years] or [('No verified backups',None)]
                update_products()
                print('Choose a year, product and month. Restore verifies the monthly SHA-256.')
            except Exception as exc:print(f'{type(exc).__name__}: {exc}')
    def restore_backup(_):
        from ecore_weather.view_backup import restore_month
        with hf_output:
            try:
                if backups['bundle'] is None or hf_month.value is None:raise ValueError('Find backups and choose a month first.')
                path=restore_month(backups['bundle'],backups['members'][hf_month.value],cache.value)
                location.value=str(Path(cache.value).resolve())
                print('Verified monthly archive ready:',path)
            except Exception as exc:print(f'{type(exc).__name__}: {exc}')
    hf_year.observe(update_products,names='value');hf_product.observe(update_members,names='value')
    hf_find.on_click(find_backups);hf_restore.on_click(restore_backup)
    backup_panel=w.Accordion(children=[w.VBox([remote,hf_find,hf_year,hf_product,hf_month,cache,hf_restore,hf_output])],selected_index=None)
    backup_panel.set_title(0,'Restore an HF backup for diagnostics')
    controls=w.VBox([source,location,backup_panel,w.HBox([start,end]),product,band,patch,output,find,selection_output])
    def filters():
        return dict(source=source.value,product=product.value,band=band.value if source.value=='goes' else None,
            start=str(start.value) if start.value else None,end=str(end.value) if end.value else None)
    def find_action(_):
        with selection_output:
            selection_output.clear_output(wait=True)
            try:
                summary,rows=report.inventory(location.value,output.value,**filters())
                options=sorted({r['product'] for r in rows})
                previous=product.value
                product.options=[('All products',None),*[(p,p) for p in options]]
                if previous in options:product.value=previous
                summary_view(summary)
            except Exception as exc:print(f'{type(exc).__name__}: {exc}')
    find.on_click(find_action)
    def change_source(_):
        product.options=[('All products',None)]
        band.disabled=source.value!='goes'
        backups.update(bundles=[],members=[],bundle=None)
        hf_year.options=[('Choose backups first',None)]
        hf_month.options=[]
    source.observe(change_source,names='value')
    wavelengths=(.47,.64,.86,1.37,1.6,2.2,3.9,6.2,6.9,7.3,8.4,9.6,10.3,11.2,12.3,13.3)
    labels=('Blue visible','Red visible','Veggie near infrared','Cirrus','Snow/ice','Cloud phase','Shortwave infrared',
        'Upper water vapor','Mid water vapor','Lower water vapor','Cloud-top phase','Ozone','Clean longwave window',
        'Longwave window','Dirty longwave window','CO₂ longwave')
    band.options=[('All bands',None),*[(f'{labels[b-1]} (C{b:02d}, {wavelengths[b-1]:g} µm)',b) for b in range(1,17)]]
    def run_diagnostics(_):
        with diagnostic_output:
            diagnostic_output.clear_output(wait=True)
            try:
                scopes={} if patch.value=='Whole ROI' else {patch.value:PATCHES[patch.value]}
                result=report.diagnose(location.value,output.value,full=full.value,patches=scopes,**filters())
                summary_view(result)
                cached_view(output.value,"Diagnostics",**filters())
                report.export_report(output.value)

            except Exception as exc:print(f'{type(exc).__name__}: {exc}')
    sample.on_click(run_diagnostics)
    map_button=w.Button(description='Coverage map',tooltip='Diagnostic classes on one native grid; no interpolation or archived values are changed.')
    def show_map(_):
        with diagnostic_output:
            diagnostic_output.clear_output(wait=True)
            try:
                fig,stats=report.coverage_frequency(location.value,output.value,**filters())
                display(fig);summary_view(stats)
                import matplotlib.pyplot as plt
                plt.close(fig)
            except Exception as exc:print(f'{type(exc).__name__}: {exc}')
    map_button.on_click(show_map)
    def run_benchmark(kind):
        with performance_output:
            performance_output.clear_output(wait=True)
            try:
                function=bench.backends if kind=='backend' else bench.local_reads
                display(pd.DataFrame(function(location.value,output.value,repeats.value,**filters())['rows']))
            except Exception as exc:print(f'{type(exc).__name__}: {exc}')
    read_test.on_click(lambda _:run_benchmark('local'));backend_test.on_click(lambda _:run_benchmark('backend'))
    def run_hf(_):
        with performance_output:
            performance_output.clear_output(wait=True)
            try:display(pd.DataFrame(bench.hf_restore(remote.value,output.value)['rows']))
            except Exception as exc:print(f'{type(exc).__name__}: {exc}')
    hf_test.on_click(run_hf)
    def save_report(_):
        with selection_output:
            try:
                result=report.export_report(output.value)
                display(Markdown(f"Saved interactive HTML, aggregate CSVs and Markdown to `{output.value}`."))
                summary_view(result)
            except Exception as exc:print(f'{type(exc).__name__}: {exc}')
    export.on_click(save_report)
    diagnostics_tab=w.VBox([w.HTML('Pixel counts and per-observation statistics. No raw arrays are changed.'),full,w.HBox([sample,map_button]),diagnostic_output])
    performance_tab=w.VBox([w.HTML('Local reads exclude rendering. Backend tests write identical cached tensors. HF tests restore verified backups.'),
        repeats,read_test,backend_test,hf_test,performance_output])
    model_tab=w.VBox([w.HTML('Future work: decode and inspect → cloud-top parallax geometry → 1 km training grid → causal model windows. No inference or regridding runs here.'),
        w.HTML('<p>See docs/StormScope-paper-notes.md and docs/CorrDiff-paper-notes.md. Cloud height, quality and geographic support are required.</p>')])
    tabs=w.Tab(children=[diagnostics_tab,performance_tab,model_tab])
    for i,title in enumerate(['Diagnostics','Performance','Model preparation']):tabs.set_title(i,title)
    cached=w.Button(description='Cached results',tooltip='Read saved summaries without opening raw arrays.')
    cached.on_click(lambda _: (cached_view(output.value,'Diagnostics',**filters()),cached_view(output.value,'Performance',**filters())))
    buttons=[sample,find,export,read_test,backend_test,hf_test,hf_find,hf_restore,map_button,cached]
    for button in buttons:
        button.layout=w.Layout(min_width='145px',width='auto')
        if not button.tooltip:button.tooltip=button.description+' using the selected dates, product and local dataset root.'
        callbacks=list(button._click_handlers.callbacks)
        button._click_handlers.callbacks=[]
        def guarded(_,callbacks=callbacks):
            for control in buttons:control.disabled=True
            try:
                for callback in callbacks:callback(_)
            finally:
                for control in buttons:control.disabled=False
        button.on_click(guarded)
    for box in (controls,diagnostics_tab,performance_tab):box.layout=w.Layout(width='100%',overflow='visible')
    controls.children=(*controls.children,w.HTML('Dates are UTC; the end date is excluded. Audits are sampled unless explicitly enabled.'))
    container=w.VBox([controls,tabs,w.HBox([cached,export],layout=w.Layout(flex_flow='row wrap'))])
    return container

