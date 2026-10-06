"""Browse a local archive, HF prefix, run report, or individual raw Zarr subset."""
import argparse
import json
import re
from pathlib import Path

from .storage import open_raw, valid_raw_name
from .maps import plot_geographic

MAX_VIEW_FRAMES = 300


def logical_dataset(row):
    """One menu entry for a product/satellite/region across months and ABI bands."""
    source = row.get('source')
    name = str(row.get('dataset') or '').split('/' + str(source) + '/', 1)[-1]
    # Region/grid identifiers are hashes today, but their representation is an
    # archive detail. Treat the complete path component as the identifier.
    name = re.sub(r'/roi-[^/]+(?:/.*)?$', '', name)
    if source == 'goes':
        name = re.sub(r'/C\d{2}$', '', name)
    return name


def resolve_dataset_rows(records, dataset, source, band=None):
    """Resolve a widget selection from the current inventory, not stale UI state."""
    if not dataset:
        return []
    rows = [row for row in records if row.get('source') == source
            and logical_dataset(row) == dataset]
    if source == 'goes' and band is not None and any(row.get('band') is not None for row in rows):
        rows = [row for row in rows if row.get('band') == band]
    return sorted(rows, key=lambda row: (row['time'], row.get('path', '')))


def stores(location, limit=200):
    """List bounded results; choose a product/date prefix for a large archive."""
    location = str(location).rstrip('/')
    if location.endswith(('.zarr', '.zarr.zip')):
        return [location]
    if location.startswith('hf://buckets/'):
        from .hf_storage import Publisher
        publisher = Publisher(location)
        import tempfile
        markers = []
        for obj in publisher.api.list_bucket_tree(publisher.bucket, prefix=publisher.prefix(location), recursive=True):
            if obj.path.endswith('/complete.json') and '/yearly-v1/' not in '/'+obj.path.strip('/')+'/':
                markers.append(obj)
                if len(markers) >= limit:
                    break
        paths = []
        if markers:
            # One batch for completion metadata rather than per-chunk filesystem calls.
            with tempfile.TemporaryDirectory(prefix='ecore-view-list-') as temp:
                publisher._download([(obj, f'{i}.json') for i,obj in enumerate(markers)], temp)
                for i,obj in enumerate(markers):
                    marker = json.loads((Path(temp)/f'{i}.json').read_text())
                    name = marker.get('raw_path','raw.zarr')
                    if not valid_raw_name(name):
                        raise ValueError('Invalid raw container in completion marker.')
                    paths.append(f'hf://buckets/{publisher.bucket}/{obj.path.removesuffix("complete.json")}{name}')
        return sorted(paths)
    root = Path(location).expanduser()
    if root.is_file() and root.suffix == '.json':
        report = json.loads(root.read_text())
        return [r['url'] for r in report.get('records', []) if r.get('url') and r.get('status') in ('saved','reused')][:limit]
    if not root.is_dir():
        raise FileNotFoundError(location)
    # Walk stops before entering chunk directories; no recursive chunk inventory.
    import os
    paths = []
    for directory, folders, files in os.walk(root):
        folders[:] = sorted(f for f in folders if not f.endswith('.zarr'))
        if 'complete.json' in files:
            marker = json.loads((Path(directory)/'complete.json').read_text())
            name = marker.get('raw_path', 'raw.zarr')
            if not valid_raw_name(name):
                raise ValueError('Invalid raw container in completion marker.')
            paths.append(str(Path(directory)/name))
            if len(paths) >= limit:
                break
    return sorted(paths)


def draw(path, variable=None, quality=True, hide_zero=False, output=None, display=True):
    import matplotlib.pyplot as plt
    from .view_frames import open_observation
    with open_observation(path) as ds:
        fig = plot_geographic(ds, variable, quality=quality, hide_zero=hide_zero)
        try:
            if output:
                Path(output).parent.mkdir(parents=True, exist_ok=True)
                fig.savefig(output, dpi=150)
            if display:
                from IPython.display import display as show
                show(fig)
        finally:
            plt.close(fig)
    return str(output) if output else None


def controls():
    import html
    from datetime import date, datetime, time, timedelta, timezone
    import ipywidgets as w
    from IPython.display import display
    from . import view_index, view_frames, ui
    from .storage import destination_root
    source=w.ToggleButtons(options=[('MRMS radar','mrms'),('GOES satellite','goes')],description='Source')
    storage_kind=w.ToggleButtons(options=['Local','Hugging Face'],description='Storage')
    location=w.Text(value='/mnt/p/ecore_eo_datasets',description='Location',layout=w.Layout(width='95%'),
                    tooltip='Local: archive root, product folder, or raw Zarr. HF annual backup browser: hf://buckets/namespace/bucket/noaa-subsets. An hf-mount path works as Local.')
    help_text=w.HTML('Local example: <code>/mnt/p/ecore_eo_datasets</code>. HF yearly ZIPs are backups; open the '
                     '<b>HF yearly backup</b> section to restore one month to a local cache before viewing it. '
                     'Choose dates in UTC; the ending instant is excluded.')
    # This local sample month exists in both the saved GOES example and MRMS
    # archive, so the first search can produce a view for either source.
    start_day=w.DatePicker(value=date(2022,10,1),description='Start UTC',tooltip='First included date. Combine with the adjacent UTC time.')
    end_day=w.DatePicker(value=date(2022,10,2),description='End UTC',tooltip='Ending date/time is excluded; next-day midnight includes a whole day.')
    start_time=w.TimePicker(value=time(0),description='Time',tooltip='Start time in UTC, included.')
    end_time=w.TimePicker(value=time(0),description='Time',tooltip='End time in UTC, excluded.')
    month=w.Select(options=[],description='Month',rows=4,
                   tooltip='Months that have stored observations in this local archive. Selecting one sets Start and End to that month; editing a date clears it. Choose a manual range for more than one month.')
    month_note=w.HTML('Local archives list available months before you search.')
    find=w.Button(description='Find',icon='search',button_style='primary',tooltip='Find observations in the selected archive and period.')
    find_bar=w.IntProgress(value=0,min=0,max=1,bar_style='info',layout=w.Layout(width='180px',visibility='hidden'))
    dataset=w.Dropdown(options=[],description='Dataset',layout=w.Layout(width='95%'),
                      tooltip='Choose one product/satellite/region. Observation times use the slider below, not this menu.')
    band=w.Dropdown(options=[],description='ABI band',disabled=True,
                    tooltip='Satellite band for the view. Switching bands re-reads the same observations without a new search. Only stored bands are listed.')
    status=w.HTML();errors=w.Output()
    state={'records':[],'selected':[],'all_selected':[],'frames':None,'name':None,'mode':None,'inv_key':None,'inventory':{},'bands':{},'months':{}}
    quality=w.Checkbox(value=True,description='Hide invalid pixels',tooltip='Apply documented quality masks to display copies only.')
    zero=w.Checkbox(value=True,description='Hide zero values',tooltip='Make valid zero rainfall transparent for display. Stored zeros remain unchanged.')
    renderer=w.Dropdown(options=[('Portable map (VS Code)','portable'),
                                 ('Leaflet map (JupyterLab)','leaflet')],
                        value='portable',description='Map display',
                        tooltip='Portable uses core widgets and embedded images. Leaflet needs the jupyter-leaflet frontend module.')
    renderer_note=w.HTML('The portable map works in VS Code without a separate Leaflet widget. '
        'Use Zoom and scroll within the map to pan; Play and the frame slider update animations.')
    index=w.IntSlider(min=0,max=0,value=0,description='Observation',continuous_update=False)
    stamp=w.HTML('Find observations to select an image.')
    export_path=w.Text(value='figures/dataset-view.png',description='Export path',layout=w.Layout(width='70%'),
                       tooltip='Single image: .png. Animations: .html, .gif, or .mp4 (mp4 needs ffmpeg).')
    export_button=w.Button(description='Export',icon='download',disabled=True,tooltip='Export the displayed view to the chosen path.')
    export_row=w.HBox([export_path,export_button]);export_row.layout.display='none'
    single_button=w.Button(description='Show map',icon='map',disabled=True,tooltip='Display the selected observation on an interactive map.');single_output=w.Output()
    day=w.DatePicker(value=start_day.value,description='Day UTC',tooltip='One day within the search bounds; every available observation is included.')
    daily_button=w.Button(description='Day',icon='play',disabled=True,tooltip='Prepare every available observation on the chosen day as an animation.');daily_output=w.Output()
    per_day=w.IntSlider(value=4,min=1,max=24,description='Frames/day',continuous_update=False,
                       tooltip='Evenly select up to 24 available observations per day; no temporal interpolation or gap filling. More frames take longer to prepare.')
    multi_button=w.Button(description='Multi-day',icon='film',disabled=True,tooltip='Prepare the selected number of observations per day across the period.');multi_output=w.Output()
    tabs=w.Tab(children=[w.VBox([index,stamp,single_button,single_output]),
                         w.VBox([day,w.HTML('All available samples on this day within the search bounds. Prepare once, then use Play or drag the frame slider.'),daily_button,daily_output]),
                         w.VBox([per_day,w.HTML('Use the start/end bounds above. Each day contributes available samples; missing days remain absent. Colors stay fixed across the sequence.'),multi_button,multi_output])])
    for i,label in enumerate(['Single image','Within one day','Across days']):tabs.set_title(i,label)

    backup_product=w.Dropdown(options=[],description='Product',layout=w.Layout(width='430px'))
    backup_year=w.Dropdown(options=[],description='Year',layout=w.Layout(width='160px'))
    backup_month=w.Dropdown(options=[],description='Month',layout=w.Layout(width='95%'))
    backup_list=w.Button(description='List backups',icon='search',tooltip='List verified annual HF backup markers; no image data are downloaded.')
    backup_inspect=w.Button(description='Inspect year',icon='list',tooltip='Read the ZIP manifest and small coverage reports using S3 byte ranges.')
    backup_restore=w.Button(description='Prepare month',icon='download',tooltip='Copy and verify only the selected monthly Zarr ZIP in the local cache.')
    backup_cache=w.Text(value='results/view-cache',description='Local cache',layout=w.Layout(width='95%'),
        tooltip='Writable local directory for restored monthly archives. This cache is separate from the canonical DAS stores.')
    backup_note=w.HTML('HF annual ZIPs contain monthly Zarr archives. List, inspect, then prepare one month to view. '
        'Only the chosen member is transferred; check local free space before preparing a large month.')
    backup_progress=w.IntProgress(value=0,min=0,max=1,layout=w.Layout(width='95%',visibility='hidden'))
    backup_panel=w.Accordion(children=[w.VBox([backup_note,backup_list,
        w.HBox([backup_product,backup_year],layout=w.Layout(flex_flow='row wrap')),
        backup_inspect,backup_month,backup_cache,backup_restore,backup_progress])])
    backup_panel.set_title(0,'HF yearly backup')
    backup_panel.selected_index=None
    backup_panel.layout.display='none'

    size_load=w.Button(description='Explore storage',icon='database',button_style='info',
        tooltip='Read completion markers and source-size metadata. Does not read image arrays.')
    size_product=w.Dropdown(options=[],description='Product',layout=w.Layout(width='430px'))
    size_year=w.Dropdown(options=[],description='Year',layout=w.Layout(width='160px'))
    size_month=w.Dropdown(options=[],description='Month',layout=w.Layout(width='175px'))
    size_details=w.HTML('Choose a source and location, then click Explore storage.')
    size_overview=w.HTML('')
    size_chart=w.HTML()
    size_sections=w.Accordion(children=[
        w.VBox([size_overview]),
        w.VBox([size_details]),
        w.VBox([size_chart])])
    for i,label in enumerate(['At a glance','Archive details','Compare sizes']):size_sections.set_title(i,label)
    size_sections.selected_index=0
    size_panel=w.Accordion(children=[w.VBox([w.HTML('Compare compressed archives with the listed NOAA source-object sizes. '
        'Source objects may already be compressed, so this is not a raw-array compression ratio. '
        'Choose a product, year, then month to move from totals to individual archives. '
        'The percentage compares stored monthly Zarr bytes with the listed sizes of the original NOAA objects; '
        'it does not estimate the smaller crop as if NOAA had served a cropped file.'),
        size_load,w.HBox([size_product,size_year,size_month],layout=w.Layout(flex_flow='row wrap')),
        size_sections])])
    size_panel.set_title(0,'Storage explorer')
    size_panel.selected_index=None
    backup_state={'bundles':[],'rows':[]}
    size_state={'rows':[],'bundles':[],'inspected':{}}

    def remote_changed():
        backup_panel.layout.display=None if storage_kind.value=='Hugging Face' else 'none'

    def list_backups(_):
        from . import view_backup
        with errors:
            errors.clear_output(wait=True)
            backup_list.disabled=True
            try:
                backup_note.value='Listing verified HF annual backups…'
                backup_state['bundles']=view_backup.list_bundles(location.value,source.value)
                products=sorted({r['product'] for r in backup_state['bundles']})
                backup_product.options=products
                backup_product.value=products[0] if products else None
                update_backup_years()
                backup_note.value=(f'{len(backup_state["bundles"])} verified annual package(s). '
                    'Choose a product and year, then inspect its monthly members.' if products else
                    'No verified annual packages for this source and path.')
            except Exception as error:
                print(f'{type(error).__name__}: {error}')
            finally:
                backup_list.disabled=False

    def update_backup_years(change=None):
        years=sorted({r['year'] for r in backup_state['bundles']
            if r['product']==backup_product.value})
        backup_year.options=years
        backup_year.value=years[0] if years else None
        backup_month.options=[]
        backup_state['rows']=[]

    def selected_bundle():
        return next((r for r in backup_state['bundles'] if r['product']==backup_product.value
            and r['year']==backup_year.value),None)

    def inspect_backup(_):
        from . import view_backup
        with errors:
            errors.clear_output(wait=True)
            backup_inspect.disabled=True
            try:
                bundle=selected_bundle()
                if not bundle:
                    raise ValueError('List HF backups and choose a product and year first.')
                backup_note.value='Reading annual manifest and monthly coverage…'
                rows=view_backup.inspect_bundle(bundle)
                backup_state['rows']=rows
                backup_month.options=[(f"{r['month']} · {Path(r['local_path']).parts[2]} · "
                    f"{r['observations']:,} observations",r['member']) for r in rows]
                backup_month.value=rows[0]['member'] if rows else None
                backup_note.value=f'{len(rows)} monthly archive(s) in this verified {bundle["year"]} package. Choose one to prepare.'
            except Exception as error:
                print(f'{type(error).__name__}: {error}')
            finally:
                backup_inspect.disabled=False

    def prepare_backup(_):
        from . import view_backup
        with errors:
            errors.clear_output(wait=True)
            backup_restore.disabled=True
            try:
                bundle=selected_bundle()
                row=next((r for r in backup_state['rows'] if r['member']==backup_month.value),None)
                if not bundle or not row:
                    raise ValueError('Inspect a yearly backup and choose a month first.')
                backup_progress.layout.visibility='visible'
                def progress(done,total):
                    backup_progress.max=max(1,(total+1024**2-1)//1024**2)
                    backup_progress.value=min(backup_progress.max,done//1024**2)
                restored=view_backup.restore_month(bundle,row,backup_cache.value,progress)
                backup_progress.value=backup_progress.max
                backup_note.value=f'Verified local month: <code>{html.escape(str(restored))}</code>. Now showing its observations.'
                storage_kind.value='Local'
                location.value=str(restored)
                year,mon=map(int,row['month'].split('-'))
                start_day.value=date(year,mon,1)
                end_day.value=date(year+mon//12,mon%12+1,1)
                start_time.value=time(0);end_time.value=time(0)
                search(None)
            except Exception as error:
                print(f'{type(error).__name__}: {error}')
            finally:
                backup_restore.disabled=False
                backup_progress.layout.visibility='hidden'

    backup_list.on_click(list_backups)
    backup_product.observe(update_backup_years,names='value')
    backup_inspect.on_click(inspect_backup)
    backup_restore.on_click(prepare_backup)

    def update_size_products(change=None):
        size_product.options=['All']+sorted({r['product'] for r in size_state['rows']})
        if size_state['bundles'] and not size_state['rows']:
            size_product.options=['All']+sorted({r['product'] for r in size_state['bundles']})
        size_product.value='All'

    def update_size_years(change=None):
        selected=size_product.value
        options=size_state['rows'] or size_state['bundles']
        size_year.options=['All']+sorted({str(r['year']) for r in options
            if selected in (None,'All',r['product'])})
        size_year.value='All'
        update_size_months()

    def update_size_months(change=None):
        rows=size_state['rows']
        if (size_state['bundles'] and size_product.value not in (None,'All')
                and size_year.value not in (None,'All')):
            from . import view_backup, view_storage
            bundle=next((b for b in size_state['bundles'] if b['product']==size_product.value
                and str(b['year'])==size_year.value),None)
            if bundle:
                key=bundle['key']
                if key not in size_state['inspected']:
                    size_state['inspected'][key]=view_storage.backup_archives(bundle,view_backup.inspect_bundle(bundle))
                rows=size_state['inspected'][key]
        options=['All']+sorted({f"{r['year']}-{r['month']}" for r in rows
            if size_product.value in (None,'All',r['product'])
            and (size_state['bundles'] or size_year.value in (None,'All',str(r['year'])))})
        if list(size_month.options)!=options:
            size_month.options=options
        size_month.value='All'
        render_sizes()

    def render_sizes(change=None):
        from . import view_storage
        rows=[r for r in size_state['rows'] if size_product.value in (None,'All',r['product'])
            and size_year.value in (None,'All',str(r['year']))
            and size_month.value in (None,'All',f"{r['year']}-{r['month']}")]
        if size_state['bundles'] and size_product.value not in (None,'All') and size_year.value not in (None,'All'):
            bundle=next((b for b in size_state['bundles'] if b['product']==size_product.value
                and str(b['year'])==size_year.value),None)
            if bundle:
                rows=[r for r in size_state['inspected'].get(bundle['key'],[])
                    if size_month.value in (None,'All',f"{r['year']}-{r['month']}")]
        if not rows:
            size_overview.value=''
            size_details.value=(f'{len(size_state["bundles"])} verified annual backup(s), '
                f'{view_storage.format_bytes(sum(b["stored_bytes"] for b in size_state["bundles"]))} stored. '
                'Choose a product and year for monthly NOAA size comparison.' if size_state['bundles']
                else 'No completed monthly archives match these filters.')
            size_chart.value=''
            return
        total=view_storage.totals(rows)
        known_source=total.get('known_source_bytes',total.get('source_bytes'))
        known_asset_source=total.get('known_asset_source_bytes',known_source)
        source_coverage=total.get('source_coverage',1.0)
        source_label=view_storage.format_bytes(total.get('source_bytes'))
        if total.get('source_bytes') is None and known_source:
            source_label=f'{view_storage.format_bytes(known_source)} in complete rows'
        elif total.get('source_bytes') is None and known_asset_source:
            source_label=f'{view_storage.format_bytes(known_asset_source)} partial only'
        size_overview.value=(
            '<div style="display:flex;flex-wrap:wrap;gap:10px;margin:8px 0">'
            f'<div style="padding:10px;border:1px solid #bbb;border-radius:6px"><b>{total["archives"]:,}</b><br>monthly archives</div>'
            f'<div style="padding:10px;border:1px solid #bbb;border-radius:6px"><b>{view_storage.format_bytes(total["stored_bytes"])}</b><br>compressed Zarr</div>'
            f'<div style="padding:10px;border:1px solid #bbb;border-radius:6px"><b>{source_label}</b><br>listed NOAA source files</div>'
            f'<div style="padding:10px;border:1px solid #bbb;border-radius:6px"><b>{view_storage.ratio_text(total)}</b><br>archive bytes / listed source bytes</div>'
            '</div>'
            f'<small>Complete listed sizes are available for {source_coverage:.1%} of selected archives '
            f'({total.get("listed_asset_size_coverage", 0.0):.1%} of source files). The percentage is the ratio of byte totals, '
            'not an average of archive percentages. These are full NOAA object sizes; '
            'the ROI crop is smaller and has no corresponding NOAA-served crop size.</small>')
        note=''
        if size_state['bundles']:
            bundle=next((b for b in size_state['bundles'] if b['product']==size_product.value
                and str(b['year'])==size_year.value),None)
            if bundle:
                note=(f'<p>Whole annual backup object: {view_storage.format_bytes(bundle["stored_bytes"])}. '
                    'Its size includes completion and selection metadata as well as the monthly members.</p>')
        detail=''.join(f'<tr><td>{html.escape(r["product"])}'
            f'{" C"+str(r["band"]).zfill(2) if r.get("band") else ""}</td>'
            f'<td>{r["year"]}-{r["month"]}</td><td>{view_storage.format_bytes(r.get("source_bytes"))}</td>'
            f'<td>{view_storage.format_bytes(r.get("stored_bytes"))}</td>'
            f'<td>{view_storage.ratio_text(r)}</td></tr>' for r in rows[:24])
        size_details.value=(f'<p>{total["archives"]} archive(s), {total["observations"]:,} observations. '
            f'Listed NOAA source objects: <b>{source_label}</b>; '
            f'compressed monthly Zarr: <b>{view_storage.format_bytes(total["stored_bytes"])}</b>; '
            f'<b>{view_storage.ratio_text(total)}</b>.</p>{note}'
            '<table><tr><th>Dataset</th><th>Month</th><th>NOAA objects</th><th>Zarr</th><th>Zarr/source</th></tr>'
            +detail+'</table>'+('<p>Showing first 24 rows; narrow the selectors for more.</p>' if len(rows)>24 else ''))
        groups={}
        for row in rows:
            label=(row['product'] if size_product.value in (None,'All') else
                str(row['year']) if size_year.value in (None,'All') else
                f"{row['year']}-{row['month']}")
            entry=groups.setdefault(label,{'stored':0,'source':0,'known':True})
            entry['stored']+=row['stored_bytes'] or 0
            entry['source']+=row['source_bytes'] or 0
            entry['known'] &= row['source_bytes'] is not None
        maximum=max((max(group['stored'],group['source']) for group in groups.values()),default=1) or 1
        bars=[]
        for label,group in sorted(groups.items())[:18]:
            bars.append(f'<div style="margin:8px 0"><b>{html.escape(label)}</b>')
            for caption,value,color in [('NOAA',group['source'] if group['known'] else None,'#407cb1'),
                                        ('Zarr',group['stored'],'#34886b')]:
                width=max(1,round(100*value/maximum)) if value is not None else 0
                bars.append('<div style="display:flex;align-items:center;gap:8px">'
                    f'<span style="min-width:3.5em">{caption}</span>'
                    f'<span style="display:inline-block;height:12px;width:{width}%;background:{color}"></span>'
                    f'<span>{view_storage.format_bytes(value)}</span></div>')
            bars.append('</div>')
        size_chart.value=('<div role="img" aria-label="NOAA source object and compressed Zarr storage sizes">'
            +''.join(bars)+'</div>'
            +('<p>Showing the first 18 groups; narrow the selectors for more.</p>' if len(groups)>18 else ''))

    def load_sizes(_):
        from . import view_backup, view_storage
        with errors:
            errors.clear_output(wait=True)
            size_load.disabled=True
            try:
                size_details.value='Reading completion metadata…'
                size_state['inspected']={}
                if storage_kind.value=='Hugging Face':
                    size_state['bundles']=view_backup.list_bundles(location.value,source.value)
                    size_state['rows']=[]
                else:
                    size_state['bundles']=[]
                    size_state['rows']=view_storage.local_archives(location.value,source.value)
                update_size_products()
                update_size_years()
            except Exception as error:
                print(f'{type(error).__name__}: {error}')
            finally:
                size_load.disabled=False
    size_load.on_click(load_sizes)
    size_product.observe(update_size_years,names='value')
    size_year.observe(update_size_months,names='value')
    size_month.observe(render_sizes,names='value')

    def clear_explorers(change=None):
        backup_state['bundles']=[]
        backup_state['rows']=[]
        backup_product.options=[]
        backup_year.options=[]
        backup_month.options=[]
        size_state['rows']=[]
        size_state['bundles']=[]
        size_state['inspected']={}
        size_product.options=[]
        size_year.options=[]
        size_month.options=[]
        size_details.value='Choose a source and location, then click Explore storage.'
        size_chart.value=''
        backup_note.value=('HF annual ZIPs contain monthly Zarr archives. List, inspect, then prepare one month '
            'to view. Only the chosen member is transferred.')
    source.observe(clear_explorers,names='value')
    location.observe(clear_explorers,names='value')

    def bounds():
        if not start_day.value or not end_day.value:raise ValueError('Choose start and end dates.')
        return (datetime.combine(start_day.value,start_time.value or time(0),tzinfo=timezone.utc),
                datetime.combine(end_day.value,end_time.value or time(0),tzinfo=timezone.utc))

    def refresh_months(change=None):
        key=(location.value,source.value)
        if location.value.startswith('hf://'):
            month.options=[];month_note.value='Remote archives: available months appear in the search result.'
            return
        # Compute availability only for the currently selected source, once per
        # location; toggling the source computes that source on demand.
        if key not in state['months']:
            month_note.value='Scanning local months…'
            state['months'][key]=view_index.months_available(location.value,source.value)
        months=state['months'][key]
        month.options=months
        month_note.value=(f'{len(months)} month(s) with stored observations in this archive.' if months
                          else 'No stored observations found for this source/location.')
    def change_storage(change):
        try:location.value=destination_root('hf') if change['new']=='Hugging Face' else '/mnt/p/ecore_eo_datasets'
        except ValueError as error:status.value=html.escape(str(error))
        remote_changed()
        refresh_months()
    storage_kind.observe(change_storage,names='value')
    def clear_search(change=None):
        state['records']=[];state['selected']=[];state['frames']=None;state['mode']=None;dataset.options=[];index.max=0
        single_button.disabled=True;daily_button.disabled=True;multi_button.disabled=True
        stamp.value='Parameters changed: click Find again.'
        export_row.layout.display='none';export_button.disabled=True
        band.options=[];band.disabled=source.value=='mrms'
        for out in (single_output,daily_output,multi_output):out.clear_output()
    _applying_month={'flag':False}
    def clear_month(change):
        if _applying_month['flag']:return
        if month.value is not None:month.value=None
    for control in (start_day,end_day,start_time,end_time):control.observe(clear_search,names='value')
    for control in (start_day,end_day,start_time,end_time):control.observe(clear_month,names='value')
    source.observe(refresh_months,names='value');location.observe(refresh_months,names='value')
    source.observe(clear_search,names='value');location.observe(clear_search,names='value')
    def apply_month(change):
        if change['name']!='value' or change['new'] is None:return
        year,mon=map(int,change['new'].split('-'))
        _applying_month['flag']=True
        try:
            start_day.value=date(year,mon,1)
            end_day.value=date(year+mon//12,mon%12+1,1)
            start_time.value=time(0);end_time.value=time(0)
        finally:_applying_month['flag']=False
    month.observe(apply_month,names='value')
    refresh_months()

    def update_stamp(change=None):
        rows=state['selected']
        stamp.value=html.escape(f"{rows[index.value]['time']} · {index.value+1}/{len(rows)}") if rows else 'No observations selected.'
    def update_view_buttons(rows):
        available=bool(rows)
        single_button.disabled=not available
        daily_button.disabled=not available
        multi_button.disabled=not available
    index.observe(update_stamp,names='value')
    def choose_dataset(change):
        base=change['new']
        state['frames']=None;state['mode']=None
        export_row.layout.display='none';export_button.disabled=True
        for out in (single_output,daily_output,multi_output):out.clear_output()
        rows=[r for r in state['records'] if r.get('source')==source.value
              and logical_dataset(r)==base]
        state['all_selected']=rows
        state['selected']=list(rows)
        refresh_bands()
    dataset.observe(choose_dataset,names='value')

    def refresh_bands():
        if source.value=='mrms':
            band.options=[];band.disabled=True
            state['selected']=list(state['all_selected'])
            update_view_buttons(state['selected'])
            index.max=max(0,len(state['selected'])-1);index.value=0;update_stamp()
            return
        group=dataset.value
        if group not in state['bands']:
            bands=sorted({r['band'] for r in state['all_selected'] if r['band']})
            if not bands:
                from . import goes
                collected=set()
                for r in state['all_selected'][:1]:
                    with open_raw(r['path'],group=f"C{r['band']:02d}" if r.get('product') in {'ABI-L2-MCMIPF','ABI-L2-CMI-2KM-HYBRID'} and r.get('band') else None) as ds:
                        collected |= {int(v.rsplit('_C',1)[-1]) for v in goes.science_variables(ds)}
                    if collected and len(collected) >= 16:
                        break
                bands=sorted(collected)
            state['bands'][group]=bands
        bands=state['bands'][group]
        wavelengths=(0.47,0.64,0.86,1.37,1.6,2.2,3.9,6.2,6.9,7.3,8.4,9.6,10.3,11.2,12.3,13.3)
        names=('Blue visible','Red visible','Veggie near-IR','Cirrus','Snow/ice','Cloud phase',
               'Shortwave IR','Upper-level water vapor','Mid-level water vapor',
               'Lower-level water vapor','Cloud-top phase','Ozone','Clean longwave window',
               'Longwave window','Dirty longwave window','CO₂ longwave')
        band.options=[(f'{names[b-1]} (C{b:02d}, {wavelengths[b-1]:g} µm)',b) for b in bands]
        band.disabled=not bands
        if bands and band.value not in bands:
            band.value=13 if 13 in bands else bands[-1]
        if bands and any(row['band'] is not None for row in state['all_selected']):
            state['selected']=[row for row in state['all_selected'] if row['band']==band.value]
        else:
            state['selected']=list(state['all_selected'])
        update_view_buttons(state['selected'])
        index.max=max(0,len(state['selected'])-1);index.value=0;update_stamp()

    def search(_):
        import time as _time
        with errors:
            errors.clear_output(wait=True);find.disabled=True
            find_bar.layout.visibility='visible';find_bar.bar_style='info'
            started=_time.perf_counter();status.value='Searching the archive…'
            try:
                start,end=bounds()
                key=(location.value,source.value,start.isoformat(),end.isoformat())
                # A running study fetch can publish a new completed month after
                # the previous Find. Refresh local searches on each click;
                # remote bucket listings remain cached until controls change.
                if storage_kind.value=='Local' or key not in state['inventory']:
                    state['inventory'][key]=view_index.inventory(location.value,source.value,start,end,None)
                rows=state['inventory'][key]
                state['records']=rows;state['bands']={}
                # Consolidate one logical dataset per product/satellite: a period
                # split across several subset folders (roi-...) is one view entry.
                groups=sorted({logical_dataset(r) for r in rows})
                old_dataset=dataset.value
                dataset.options=groups
                dataset.value=old_dataset if old_dataset in groups else (groups[0] if groups else None)
                # A widget observer may not run when its selected value is
                # unchanged while options are refreshed. Resolve explicitly so
                # visualization buttons always match the current inventory.
                choose_dataset({'new':dataset.value})
                day.value=start_day.value
                days=len({r['time'][:10] for r in rows})
                elapsed=_time.perf_counter()-started
                status.value=(f'{len(rows):,} observations across {days} day(s) in {len(groups)} dataset(s), '
                              f'found in {elapsed:.1f}s. Choose a dataset, then a view.')
                if not rows:
                    status.value+=(' No directly readable monthly archives here. Open HF yearly backup above and prepare a month.'
                        if storage_kind.value=='Hugging Face' else
                        ' No matches: check the dates, source, and location.')
            except Exception as error:
                state['records']=[];state['selected']=[];state['all_selected']=[]
                dataset.options=[];index.max=0
                update_view_buttons([]);update_stamp()
                status.value=f'Search failed: {html.escape(type(error).__name__)}: {html.escape(str(error))}'
                print(f'{type(error).__name__}: {error}')
            finally:find.disabled=False;find_bar.layout.visibility='hidden'
    find.on_click(search)

    def variable():return 'measurement' if source.value=='mrms' else None
    def current_selection():
        rows=resolve_dataset_rows(state['records'],dataset.value,source.value,
            band.value if source.value=='goes' and not band.disabled else None)
        state['all_selected']=[r for r in state['records'] if r.get('source')==source.value
                               and logical_dataset(r)==dataset.value]
        state['selected']=rows
        return rows
    def prepare(rows,pixels):
        if not rows:
            if not state['records']:
                raise ValueError('Click Find and choose a dataset before opening a visualization.')
            raise ValueError('The selected dataset or ABI band has no observations in this search. Choose another dataset or band, then click Find again.')
        # CMIP uses CMI; multiband stores use CMI_Cnn.
        name=variable()
        if source.value=='goes':
            with open_raw(rows[0]['path'],group=f"C{band.value:02d}" if rows[0].get('product') in {'ABI-L2-MCMIPF','ABI-L2-CMI-2KM-HYBRID'} else None) as ds:name='CMI' if 'CMI' in ds else f'CMI_C{band.value:02d}'
        with ui.FetchProgress(len(rows),'Display frames') as progress:
            return view_frames.prepare(rows,name,quality.value,zero.value,pixels,progress=progress,
                max_frames=MAX_VIEW_FRAMES),name
    def show_export(frames,name,mode):
        state['frames'],state['name'],state['mode']=frames,name,mode
        export_button.disabled=False;export_row.layout.display=None
        default={'single':'figures/dataset-view.png','day':'figures/dataset-day.html','multi':'figures/dataset-multi.html'}[mode]
        if export_path.value in ('figures/dataset-view.png','figures/dataset-day.html','figures/dataset-multi.html') or not export_path.value:
            export_path.value=default
    def render_single(_):
        with single_output:
            single_output.clear_output(wait=True);single_button.disabled=True
            try:
                rows=current_selection();chosen=[rows[index.value]] if rows else []
                frames,name=prepare(chosen,768)
                display(view_frames.leaflet(frames) if renderer.value=='leaflet'
                        else view_frames.portable_map(frames));show_export(frames,name,'single')
            except Exception as error:print(f'{type(error).__name__}: {error}')
            finally:single_button.disabled=False
    single_button.on_click(render_single)
    def animate(mode):
        out=daily_output if mode=='day' else multi_output
        button=daily_button if mode=='day' else multi_button
        with out:
            out.clear_output(wait=True);button.disabled=True
            try:
                rows=current_selection()
                if mode=='day':
                    if day.value is None:raise ValueError('Choose a day.')
                    rows=[r for r in rows if r['time'][:10]==day.value.isoformat()]
                else:rows=view_index.daily_sample(rows,per_day.value)
                frames,name=prepare(rows,384)
                display(view_frames.leaflet(frames) if renderer.value=='leaflet'
                        else view_frames.portable_map(frames))
                show_export(frames,name,mode)
                print(f'{len(frames)} frames prepared. Playback reads no additional Zarr data. Display limit: {MAX_VIEW_FRAMES} frames.')
            except Exception as error:print(f'{type(error).__name__}: {error}')
            finally:button.disabled=False
    daily_button.on_click(lambda _:animate('day'));multi_button.on_click(lambda _:animate('multi'))
    def do_export(_):
        with errors:
            errors.clear_output(wait=True);export_button.disabled=True
            try:
                frames,name,mode=state['frames'],state['name'],state['mode']
                if frames is None:raise ValueError('Show an image or prepare an animation first.')
                if mode=='single':
                    print(draw(frames[0],name,quality.value,zero.value,export_path.value,display=False))
                else:
                    print(view_frames.save_animation(frames,export_path.value))
            except Exception as error:print(f'{type(error).__name__}: {error}')
            finally:export_button.disabled=False
    export_button.on_click(do_export)
    def band_changed(change):
        if change['name']!='value' or change.get('old')==change.get('new'):return
        if source.value=='mrms':return
        if state['all_selected'] and any(row['band'] is not None for row in state['all_selected']):
            state['selected']=[row for row in state['all_selected'] if row['band']==change['new']]
            index.max=max(0,len(state['selected'])-1);index.value=0;update_stamp()
        if state['frames'] is None:return
        # Re-read the same observations for the new band; no new search.
        if state['mode']=='single':render_single(None)
        elif state['mode'] in ('day','multi'):animate(state['mode'])
    band.observe(band_changed,names='value')
    panel=w.VBox([source,storage_kind,location,help_text,backup_panel,
                  w.HBox([start_day,start_time]),w.HBox([end_day,end_time]),
                  month,month_note,w.HBox([find,find_bar]),status,dataset,band,
                  w.HBox([quality,zero,renderer]),renderer_note,errors,tabs,export_row,size_panel])
    panel._ecore_controls={'source':source,'location':location,'start':start_day,'end':end_day,'start_time':start_time,
                          'end_time':end_time,'tabs':tabs,'search':find,'dataset':dataset,'band':band,
                          'renderer':renderer,'single':single_button,
                          'day':daily_button,'multi':multi_button,'state':state,'index':index,'export':export_button,
                          'export_row':export_row,'export_path':export_path,'month':month,
                          'backup_panel':backup_panel,'backup_list':backup_list,'backup_product':backup_product,
                          'backup_year':backup_year,'backup_month':backup_month,'backup_inspect':backup_inspect,
                          'backup_restore':backup_restore,
                          'backup_cache':backup_cache,'storage_kind':storage_kind,'sizes':size_load,
                          'size_product':size_product,'size_year':size_year,'size_month':size_month,
                          'size_overview':size_overview,'size_details':size_details,'size_chart':size_chart,
                          'size_sections':size_sections,'status':status}
    display(panel)
    return panel


def main(argv=None):
    from . import view_index,view_frames,ui
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('location',help='Local/HF archive, one dataset folder, report JSON, or raw Zarr')
    parser.add_argument('--source',choices=['mrms','goes'],help='Filter to one sensor family')
    parser.add_argument('--start',help='Included UTC date/time')
    parser.add_argument('--end',help='Excluded UTC date/time')
    parser.add_argument('--band',type=int,default=13,help='GOES channel number')
    parser.add_argument('--index',type=int,default=0,help='Single-image index after filtering')
    parser.add_argument('--variable',help='Override measurement/CMI/CMI_Cnn')
    parser.add_argument('--mode',choices=['single','day','multi-day'],default='single')
    parser.add_argument('--day',help='UTC day for intra-day playback (YYYY-MM-DD)')
    parser.add_argument('--frames-per-day',type=int,choices=range(4,9),default=4)
    parser.add_argument('--show-invalid',action='store_true')
    parser.add_argument('--hide-zero',action='store_true')
    parser.add_argument('--output',help='PNG for single image, HTML for animation')
    args=parser.parse_args(argv)
    rows=view_index.inventory(args.location,args.source,args.start,args.end,None)
    # The band selects single-band CMIP product rows, or the displayed variable
    # in multiband stores; it never filters a multiband inventory out of the search.
    if args.source=='goes' and rows and all(r['band'] is not None for r in rows):
        rows=[r for r in rows if r['band']==args.band]
    if not rows:parser.error('No observations match the requested source and period.')
    if len({r['dataset'] for r in rows})>1:parser.error('Choose one product/region folder so this view does not mix datasets.')
    name=args.variable
    if name is None:
        with open_raw(rows[0]['path']) as ds:
            name='measurement' if 'measurement' in ds else 'CMI' if 'CMI' in ds else f'CMI_C{args.band:02d}'
    if args.mode=='single':
        if not 0<=args.index<len(rows):parser.error(f'Index must select one of {len(rows)} observations.')
        output=args.output or 'figures/dataset-view.png'
        print(draw(rows[args.index],name,not args.show_invalid,args.hide_zero,output,display=False))
    else:
        if args.mode=='day':
            day=args.day or rows[0]['time'][:10];rows=[r for r in rows if r['time'][:10]==day]
        else:rows=view_index.daily_sample(rows,args.frames_per_day)
        with ui.FetchProgress(len(rows),'Display frames') as progress:
            frames=view_frames.prepare(rows,name,not args.show_invalid,args.hide_zero,
                progress=progress,max_frames=MAX_VIEW_FRAMES)
        print(view_frames.save_animation(frames,args.output or 'figures/dataset-animation.html'))
    return 0
