"""Cached, pixel-weighted diagnostic summaries and shareable interactive charts."""
from __future__ import annotations
import html
import json
from pathlib import Path
import numpy as np
import pandas as pd

COUNTS=('pixels','numeric_valid','strict_good','good_plus_conditional','valid_zero','valid_negative',
        'fill','no_coverage','bitmap_missing','nonfinite','outside_range','quality_unknown','ambiguous_shear_zero','calibration_nonfinite')


def diagnostic_table(output,unit='month',**filters):
    from . import dataset_report as report
    if unit not in ('day','week','month','year'):raise ValueError('Choose day, week, month or year')
    fields=','.join(f"sum(coalesce(try_cast(json_extract(d.payload,'$.{key}') AS BIGINT),0)) AS {key}" for key in COUNTS)
    clauses=['a.code_hash=?'];parameters=[report.provenance()['code_hash']]
    if filters.get('source'):clauses.append("product LIKE 'ABI-%'" if filters['source']=='goes' else "product NOT LIKE 'ABI-%'")
    for name in ('product','band','patch'):
        if filters.get(name) is not None:clauses.append(name+'=?');parameters.append(filters[name])
    from .common import utc
    for name,op in [('start','>='),('end','<')]:
        if filters.get(name):clauses.append('time'+op+'?');parameters.append(utc(filters[name]).replace(tzinfo=None))
    where=' AND '.join(clauses)
    with report.connection(output) as con:
        frame=con.execute(f'''SELECT product,band,patch,date_trunc('{unit}',time) AS period,
            count(*) AS observations,{fields},
            sum(try_cast(json_extract(d.payload,'$.strict_stats.mean') AS DOUBLE)*try_cast(json_extract(d.payload,'$.strict_good') AS BIGINT)) AS value_sum,
            sum((pow(try_cast(json_extract(d.payload,'$.strict_stats.std') AS DOUBLE),2)+pow(try_cast(json_extract(d.payload,'$.strict_stats.mean') AS DOUBLE),2))*try_cast(json_extract(d.payload,'$.strict_good') AS BIGINT)) AS square_sum,
            min(try_cast(json_extract(d.payload,'$.strict_stats.minimum') AS DOUBLE)) AS minimum,
            max(try_cast(json_extract(d.payload,'$.strict_stats.maximum') AS DOUBLE)) AS maximum
            FROM diagnostics d JOIN audits a USING(audit_key) WHERE {where}
            GROUP BY ALL ORDER BY product,band,patch,period''',parameters).fetchdf()
    if frame.empty:return frame
    count=frame.strict_good.replace(0,np.nan)
    frame['mean']=frame.value_sum/count
    frame['std']=np.sqrt((frame.square_sum/count-frame['mean']**2).clip(lower=0))
    for key in COUNTS[1:]:frame[key+'_percent']=100*frame[key]/frame.pixels.replace(0,np.nan)
    frame['series']=frame['product']+frame.band.apply(lambda b:f' C{int(b):02d}' if pd.notna(b) else '')
    return frame.drop(columns=['value_sum','square_sum'])


def figures(output,**filters):
    import plotly.express as px
    from . import dataset_report as report
    charts=[];frame=diagnostic_table(output,'day',**filters)
    if len(frame):
        roi=frame[frame.patch=='Whole ROI']
        charts.append(px.line(roi,x='period',y='strict_good_percent',color='series',markers=True,
            labels={'period':'UTC date','strict_good_percent':'Strict-good pixels (%)','series':'Product / band'},title='Pixel-weighted quality over time'))
        if len(roi):
            pivot=roi.pivot_table(index='series',columns='period',values='strict_good_percent',aggfunc='first')
            charts.append(px.imshow(pivot,zmin=0,zmax=100,aspect='auto',labels={'color':'Good pixels (%)','x':'UTC date','y':'Product / band'},title='Quality by date and product'))
        patches=frame.groupby(['series','patch'],as_index=False)[['strict_good','pixels']].sum()
        patches['good_percent']=100*patches.strict_good/patches.pixels.replace(0,np.nan)
        charts.append(px.bar(patches,x='patch',y='good_percent',color='series',barmode='group',
            labels={'good_percent':'Strict-good pixels (%)'},title='Caribbean patch coverage'))
        charts.append(px.line(roi,x='period',y='mean',color='series',markers=True,title='Valid measurement means (product units; compare within a series)'))
    with report.connection(output) as con:
        tables={r[0] for r in con.execute('SHOW TABLES').fetchall()}
        rows=con.execute('SELECT payload FROM archive_inventory').fetchall() if 'archive_inventory' in tables else []
        metrics=con.execute('SELECT kind,payload FROM measurements').fetchall()
    accounting=pd.DataFrame([json.loads(r[0]) for r in rows])
    if len(accounting):
        available=[c for c in ('stored_bytes','source_bytes','logical_roi_measurement_bytes','logical_roi_quality_bytes','logical_coordinate_bytes') if c in accounting]
        if available:
            totals=accounting.groupby('product',as_index=False)[available].sum().melt('product',var_name='representation',value_name='bytes')
            totals['GiB']=totals['bytes']/1024**3
            charts.append(px.bar(totals,x='product',y='GiB',color='representation',barmode='group',title='Storage representations (physical ZIPs counted once)'))
    performance=[]
    for kind,payload in metrics:
        row=json.loads(payload)
        if kind=='fetch_index':
            seconds=row.get('wall_seconds',0);bytes_=row.get('source_bytes',0)
            performance.append({'product':row.get('product'),'started_at':row.get('started_at'),
                'wall_seconds':seconds,'returned_bytes':bytes_,'throughput_mbps':8*bytes_/seconds/1e6 if seconds else None,
                'scope':'Concurrent run wall time; no invented daily or per-band cost'})
    tasks=pd.DataFrame(performance)
    if len(tasks):
        charts.append(px.scatter(tasks,x='started_at',y='throughput_mbps',color='product',size='wall_seconds',
            labels={'throughput_mbps':'Returned payload / run wall time (Mbps)'},title='Measured run throughput'))
    return charts,frame,accounting,tasks


def export(output):
    out=Path(output);out.mkdir(parents=True,exist_ok=True)
    inventory=out/'inventory.json'
    filters=json.loads(inventory.read_text()).get('summary',{}).get('filters',{}) if inventory.exists() else {}
    charts,diagnostics,storage,performance=figures(out,**filters)
    diagnostics.to_csv(out/'quality-summary.csv',index=False)
    storage.to_csv(out/'storage-summary.csv',index=False)
    performance.to_csv(out/'performance-summary.csv',index=False)
    for unit in ('week','month','year'):diagnostic_table(out,unit,**filters).to_csv(out/f'quality-{unit}.csv',index=False)
    scope=audit_scope(out,**filters)
    text=['<!doctype html><html><head><meta charset="utf-8"><title>Dataset report</title></head><body>',
        '<h1>Dataset quality and performance</h1><p>Cached audit results. Percentages use pixel counts. Missing observations are separate from missing pixels. Image quartiles are not pooled quartiles.</p>']
    text.append('<h2>Audit scope</h2>'+pd.DataFrame([scope]).to_html(index=False))
    text.append('<h2>Per-observation distributions</h2>'+distribution_table(out,**filters).head(100).to_html(index=False))
    measured=benchmark_summary(out)
    measured.to_csv(out/'benchmark-summary.csv',index=False)
    text.append('<h2>Measured task and benchmark comparisons</h2>'+measured.head(100).to_html(index=False))
    for i,fig in enumerate(charts):text.append(fig.to_html(full_html=False,include_plotlyjs=True if i==0 else False))
    for title,frame in [('Quality summary',diagnostics),('Storage',storage),('Measured runs',performance)]:
        text.append(f'<h2>{html.escape(title)}</h2>'+frame.head(100).to_html(index=False))
    text.append('</body></html>');(out/'report.html').write_text('\n'.join(text))
    note=f'''# Dataset report

Audited summary rows: {len(diagnostics)}. Physical archive records: {len(storage)}.
Measured fetch runs: {len(performance)}.

Open `report.html` for interactive charts. The accompanying quality, storage and
performance CSVs contain exportable aggregates. Percentages are pixel-weighted;
combined means and standard deviations use valid pixel counts. Quartiles remain
per-observation summaries in `diagnostic-preview.csv`.

Network bytes, listed compressed NOAA bytes, logical arrays and physical ZIPs
are distinct representations. Concurrent run wall time is not summed task time.
Daily throughput is not inferred from monthly logs. Model preparation, including
parallax correction and interpolation, does not run in this report.
'''
    (out/'report.md').write_text(note)
    return {'html':str(out/'report.html'),'markdown':str(out/'report.md'),'charts':len(charts)}


def audit_scope(output,**filters):
    from . import dataset_report as report
    with report.connection(output) as con:
        tables={r[0] for r in con.execute('SHOW TABLES').fetchall()}
        available=con.execute("SELECT coalesce(sum(try_cast(json_extract(payload,'$.selected_observations') AS BIGINT)),0) FROM archive_inventory").fetchone()[0] if 'archive_inventory' in tables else 0
        audited=con.execute("SELECT count(*) FROM diagnostics d JOIN audits a USING(audit_key) WHERE patch='Whole ROI' AND a.code_hash=?",[report.provenance()['code_hash']]).fetchone()[0]
        states=con.execute('SELECT status,count(*) FROM audits WHERE code_hash=? GROUP BY status',[report.provenance()['code_hash']]).fetchall()
        details=con.execute('SELECT details FROM audits WHERE code_hash=?',[report.provenance()['code_hash']]).fetchall()
    selected=diagnostic_table(output,'day',**filters)
    audited=int(selected[selected.patch=='Whole ROI'].observations.sum()) if not selected.empty else 0
    full=bool(details) and all(json.loads(r[0]).get('scope')=='full selected observations' for r in details)
    return {'available_observations':available,'audited_observations':audited,
        'audit_scope':'full requested audit' if full else 'representative sample / incomplete audit',
        'failed_archive_audits':sum(n for state,n in states if state=='failed'),
        'missing_observations':'see coverage facts; not inferred from missing pixels'}


def distribution_table(output,**filters):
    from . import dataset_report as report
    clauses=['a.code_hash=?'];parameters=[report.provenance()['code_hash']]
    if filters.get('source'):clauses.append("product LIKE 'ABI-%'" if filters['source']=='goes' else "product NOT LIKE 'ABI-%'")
    for name in ('product','band','patch'):
        if filters.get(name) is not None:clauses.append(name+'=?');parameters.append(filters[name])
    from .common import utc
    for name,op in [('start','>='),('end','<')]:
        if filters.get(name):clauses.append('time'+op+'?');parameters.append(utc(filters[name]).replace(tzinfo=None))
    where=' AND '.join(clauses)
    with report.connection(output) as con:
        return con.execute(f"""SELECT product,band,time,patch,
          try_cast(json_extract(d.payload,'$.strict_stats.minimum') AS DOUBLE) AS minimum,
          try_cast(json_extract(d.payload,'$.strict_stats.q25') AS DOUBLE) AS q25,
          try_cast(json_extract(d.payload,'$.strict_stats.median') AS DOUBLE) AS median,
          try_cast(json_extract(d.payload,'$.strict_stats.q75') AS DOUBLE) AS q75,
          try_cast(json_extract(d.payload,'$.strict_stats.maximum') AS DOUBLE) AS maximum
          FROM diagnostics d JOIN audits a USING(audit_key) WHERE {where}
          ORDER BY time,product,band,patch LIMIT 1000""",parameters).fetchdf()


def measurement_table(output):
    from . import dataset_report as report
    rows=[]
    with report.connection(output) as con:
        payloads=con.execute('SELECT kind,payload FROM measurements').fetchall()
    fields=('satellite','repeat','wall_seconds','wall_s','read_bytes','returned_bytes','stored_bytes','write_seconds','download_seconds','local_decode_crop_seconds','reader_seconds','reader_wait_seconds','source_retries','peak_rss_bytes')
    def visit(value,scope):
        if not isinstance(value,dict):return
        if any(k in value for k in ('wall_seconds','wall_s','write_seconds','download_seconds')):
            rows.append({'scope':scope+':'+str(value.get('stage',value.get('scope',''))),'method':value.get('method',value.get('profile',{}).get('read_mode')),
                'product':value.get('product'),'band':value.get('band'),**{k:value[k] for k in fields if k in value}})
        for key in ('rows','monthly_archives','monthly_metrics','evidence'):
            for child in value.get(key,[]) if isinstance(value.get(key),list) else []:
                visit(child.get('summary',child) if isinstance(child,dict) else child,scope)
    for kind,payload in payloads:visit(json.loads(payload),kind)
    return pd.DataFrame(rows)


def benchmark_summary(output):
    frame=measurement_table(output)
    if frame.empty or 'method' not in frame:return frame
    frame=frame[frame.method.notna()]
    if frame.empty:return frame
    groups=[c for c in ('scope','method','satellite','band') if c in frame]
    numbers=[c for c in ('wall_seconds','returned_bytes','peak_rss_bytes','download_seconds','local_decode_crop_seconds') if c in frame]
    return frame.groupby(groups,dropna=False)[numbers].median().reset_index()
