"""Read-only archive diagnostics and auditable accounting, separate from ingestion."""
from __future__ import annotations
from collections import defaultdict
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import re
import subprocess
import time

import duckdb
import numpy as np
import pandas as pd

from .common import PATCHES, jsonable, utc, write_json
from .storage import open_raw
from .view_frames import _scan_attrs
from .view_storage import local_archives

DEFAULT_OUTPUT = 'results/dataset-report'


def file_hash(path):
    digest=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(8*1024*1024), b''):
            digest.update(block)
    return digest.hexdigest()


def provenance():
    root=Path(__file__).resolve().parents[2]
    modules=[Path(__file__),Path(__file__).with_name('diagnostics.py'),
             Path(__file__).with_name('goes.py'),Path(__file__).with_name('mrms.py'),
             Path(__file__).with_name('view_frames.py')]
    hashes={p.name:file_hash(p) for p in modules}
    versions={}
    for name in ('earth2studio','numpy','xarray','zarr','duckdb','obstore','torch'):
        try:versions[name]=importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:versions[name]=None
    import psutil
    return {'code_hash':hashlib.sha256(json.dumps(hashes,sort_keys=True).encode()).hexdigest(),
        'content_hashes':hashes,'git_revision':subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),
        'versions':versions,'platform':platform.platform(),'cpu_logical':psutil.cpu_count(),
        'ram_bytes':psutil.virtual_memory().total}


def selected_archives(location, source=None, product=None, band=None, start=None, end=None):
    """Enumerate completed manifests; never need a write connection to the fetch index."""
    if str(location).startswith('hf://'):
        raise ValueError('HF yearly ZIPs are backups. Restore a verified monthly member into a local cache first.')
    kinds=[source] if source else ['mrms','goes']
    rows=[];candidates=[]
    try:
        from .index import connect,database_path
        if database_path().is_file():
            clauses=['starts_with(path,?)'];parameters=[str(Path(location).expanduser().resolve()).rstrip('/')+'/']
            for name,value in [('source',source),('product',product)]:
                if value:clauses.append(name+'=?');parameters.append(value)
            if band:clauses.append('(band=? OR band IS NULL)');parameters.append(band)
            if start:clauses.append('end_utc>=?');parameters.append(utc(start).replace(tzinfo=None))
            if end:clauses.append('start_utc<?');parameters.append(utc(end).replace(tzinfo=None))
            with connect(read_only=True) as index:
                candidates=[r[0] for r in index.execute('SELECT path FROM archives WHERE '+' AND '.join(clauses),parameters).fetchall()]
    except (duckdb.Error,OSError):
        candidates=[]  # Completed manifests remain available during writer locks.
    if candidates:
        rows=[r for path in candidates for kind in kinds for r in local_archives(path,kind,strict=True)]
    else:
        rows=[r for kind in kinds for r in local_archives(location,kind,strict=True)]
    result=[];seen=set()
    for row in rows:
        path=Path(row['path'])
        if str(path.resolve()) in seen:continue
        if product and row['product']!=product:continue
        marker=json.loads((path.parent/'complete.json').read_text())
        available=list(marker.get('band_counts',{}))
        if band and row.get('band')!=band and f'C{band:02d}' not in available:continue
        assets=[a for a in marker.get('assets',[]) if
                (not start or utc(a['time'])>=utc(start)) and (not end or utc(a['time'])<utc(end))]
        if not assets:continue
        row={**row,'assets':assets,'marker':marker,
            'archive_sha256':marker.get('archive_sha256'),
            'selection_scope':'selected observation dates; physical ZIP covers entire month',
            'selected_observations':len(assets)}
        seen.add(str(path.resolve()));result.append(row)
    return result


def archive_accounting(path):
    """Zarr logical array sizes from metadata; compressed payload from ZIP headers."""
    import zipfile
    logical_measurement=logical_quality=logical_coordinates=0
    source_full=None
    with zipfile.ZipFile(path) as archive:
        entries=archive.infolist()
        payload=sum(i.compress_size for i in entries)
        metadata_bytes=sum(i.compress_size for i in entries if i.filename.endswith(('zarr.json','.json','.json.gz')))
        for item in entries:
            if not item.filename.endswith('/zarr.json'):continue
            meta=json.loads(archive.read(item))
            if meta.get('node_type')!='array':continue
            dtype=meta.get('data_type')
            if isinstance(dtype,dict) and dtype.get('name','').startswith('numpy.datetime64'):dtype='datetime64[ns]'
            try:size=int(np.prod(meta['shape']))*np.dtype(dtype).itemsize
            except (TypeError,ValueError):continue
            name=item.filename.split('/')[-2]
            if name=='measurement' or name.startswith('CMI'):logical_measurement+=size
            elif name=='bitmap_valid' or name.startswith('DQF'):logical_quality+=size
            elif name in ('time','latitude','longitude','x','y'):logical_coordinates+=size
    return {'logical_roi_measurement_bytes':logical_measurement,'logical_roi_quality_bytes':logical_quality,
        'logical_coordinate_bytes':logical_coordinates,'compressed_member_bytes':payload,
        'metadata_member_bytes':metadata_bytes,'zip_container_overhead_bytes':Path(path).stat().st_size-payload,
        'logical_full_source_array_bytes':source_full,
        'full_source_scope':'not reconstructed from compressed object length; requires original source array shapes'}


def inventory(location, output=DEFAULT_OUTPUT, **filters):
    rows=selected_archives(location,**filters)
    objects={};unknown=set()
    for row in rows:
        for a in row['assets']:
            key=(a.get('bucket') or str(a.get('source_url','')).split('.s3.')[0],
                 a.get('key') or a.get('source_url') or a['asset_id'],a.get('etag'))
            if a.get('source_bytes') is None:unknown.add(key)
            else:objects[key]=int(a['source_bytes'])
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    cache_path=output/'accounting-cache.json'
    cache=json.loads(cache_path.read_text()) if cache_path.is_file() else {}
    for i,row in enumerate(rows):
        stat=Path(row['path']).stat()
        key=json.dumps([2,row['path'],stat.st_size,stat.st_mtime_ns,row['archive_sha256']])
        if key not in cache:cache[key]=archive_accounting(row['path'])
        row.update(cache[key])
        if i%10==0:write_json(cache_path,cache)
    write_json(cache_path,cache)
    summary={'archives':len(rows),'selected_observations':sum(r['selected_observations'] for r in rows),
        'listed_compressed_source_bytes':sum(objects.values()),'source_size_unknown_objects':len(unknown-set(objects)),
        'filters':filters,'source_objects':len(set(objects)|unknown),'physical_zip_bytes':sum(r['stored_bytes'] for r in rows),
        'ratio_scope':'whole-month ZIP bytes versus selected unique compressed NOAA objects; partial-month ratios are not compression ratios',
        'measurement':'metadata inventory; no NOAA downloads','provenance':provenance(),
        'logical_whole_archive_roi_measurement_bytes':sum(r['logical_roi_measurement_bytes'] for r in rows),
        'logical_whole_archive_quality_bytes':sum(r['logical_roi_quality_bytes'] for r in rows),
        'logical_coordinate_bytes':sum(r['logical_coordinate_bytes'] for r in rows)}
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    write_json(output/'inventory.json',{'summary':summary,'archives':[{k:v for k,v in r.items() if k not in ('assets','marker')} for r in rows]})
    pd.DataFrame([{k:v for k,v in r.items() if k not in ('assets','marker')} for r in rows]).to_csv(output/'archives.csv',index=False)
    coverage=[]
    for row in rows:
        times=sorted(utc(a['time']) for a in row['assets'])
        intervals=np.diff([t.timestamp() for t in times])
        expected=600 if row['source']=='goes' else None
        modes=defaultdict(int)
        for asset in row['assets']:
            match=re.search(r'-M(\d)',asset.get('key') or asset.get('source_url',''))
            if match:modes[match[1]]+=1
        coverage.append({'path':row['path'],'product':row['product'],'band':row.get('band'),
            'observations':len(times),'duplicate_times':len(times)-len(set(times)),
            'first_time':str(times[0]),'last_time':str(times[-1]),
            'gaps_over_15_minutes':int((intervals>900).sum()) if expected else None,
            'gap_scope':'observed timestamp gaps, not proof of absent NOAA objects',
            'scan_modes':dict(modes),
            'nonpositive_scan_intervals':sum(utc(a['end_time'])<=utc(a['time']) for a in row['assets'] if a.get('end_time')),
            'coverage_facts':row['marker'].get('coverage',row['marker'].get('missing_bands'))})
    write_json(output/'coverage-facts.json',coverage)
    with connection(output) as con:
        con.execute('CREATE TABLE IF NOT EXISTS archive_inventory (path VARCHAR PRIMARY KEY, payload JSON)')
        con.execute('DELETE FROM archive_inventory')
        con.executemany('INSERT OR REPLACE INTO archive_inventory VALUES (?,?)',
            [[r['path'],json.dumps(jsonable({k:v for k,v in r.items() if k not in ('assets','marker')}))] for r in rows]) if rows else None
    return summary,rows


def _unsigned(values, attrs):
    if str(attrs.get('_Unsigned','')).lower()=='true' and values.dtype.kind=='i':
        return values.view(np.dtype(values.dtype.str.replace('i','u')))
    return values


def pixel_statistics(ds, variable, mask=None, return_classes=False):
    """Pixel counts may overlap; exclusive missing classes and quality are separate."""
    raw=np.asarray(ds[variable].values);attrs=ds[variable].attrs
    inside=np.ones(raw.shape,dtype=bool) if mask is None else np.broadcast_to(mask,raw.shape)
    finite=np.isfinite(raw)
    fill=np.zeros(raw.shape,dtype=bool);no_coverage=fill.copy();bitmap=fill.copy();ambiguous=fill.copy()
    quality=None;outside=fill.copy()
    if variable=='measurement':
        from .mrms import PRODUCTS
        info=PRODUCTS[ds.attrs['product']]
        if info.get('zero_ambiguous'):ambiguous=raw==0
        else:
            fill=raw==info['missing'];no_coverage=raw==info['no_coverage']
        if 'bitmap_valid' in ds:bitmap=np.asarray(ds.bitmap_valid.values)==0
        physical=raw.astype('float64')
    else:
        if '_FillValue' in attrs:fill=raw==attrs['_FillValue']
        encoded=_unsigned(raw,attrs)
        if 'valid_range' in attrs:
            limits=np.asarray(attrs['valid_range'],dtype=raw.dtype)
            limits=_unsigned(limits,attrs)
            outside=(encoded<limits[0])|(encoded>limits[1])
        physical=encoded.astype('float64')*attrs.get('scale_factor',1)+attrs.get('add_offset',0)
        flag=variable.replace('CMI','DQF')
        if flag in ds:quality=np.asarray(ds[flag].values)
    calibration_bad=~np.isfinite(physical)&finite
    numeric=inside&finite&~calibration_bad&~fill&~no_coverage&~bitmap&~outside&~ambiguous
    strict=numeric if variable=='measurement' else numeric&(quality==0) if quality is not None else np.zeros_like(numeric)
    conditional=strict if variable=='measurement' else numeric&((quality==0)|(quality==1)) if quality is not None else np.zeros_like(numeric)
    classes=np.zeros(raw.shape,dtype='uint8')
    if variable!='measurement':
        classes[numeric]=8 if quality is None else 0
        if quality is not None:
            classes[numeric&(quality==1)]=9
            classes[numeric&~np.isin(quality,[0,1])]=5
    classes[ambiguous]=7;classes[outside]=6;classes[fill]=1;classes[no_coverage]=2
    classes[~finite]=4;classes[calibration_bad]=10;classes[bitmap]=3
    category,category_counts=np.unique(classes[inside],return_counts=True)
    count=int(inside.sum())
    def number(condition):return int(np.count_nonzero(condition&inside))
    result={'pixels':count,'numeric_valid':number(numeric),'fill':number(fill),'no_coverage':number(no_coverage),
        'bitmap_missing':number(bitmap),'nonfinite':number(~finite),'outside_range':number(outside),
        'ambiguous_shear_zero':number(ambiguous),'strict_good':number(strict),'good_plus_conditional':number(conditional),
        'quality_unknown':count if variable!='measurement' and quality is None else 0,
        'raw_zero':number(raw==0),'valid_zero':number(strict&(physical==0)),
        'valid_negative':number(strict&(physical<0)),'units':attrs.get('units'),
        'packed_dtype':str(raw.dtype),'exclusive_classes':{str(int(c)):int(n) for c,n in zip(category,category_counts)},
        'class_legend':'0 valid/good;1 fill;2 no coverage;3 bitmap;4 nonfinite;5 bad/other quality;6 range;7 ambiguous zero;8 unknown quality;9 conditional quality;10 invalid calibration',
        'calibration_nonfinite':number(calibration_bad),'calibration':jsonable({k:attrs[k] for k in
            ('_Unsigned','_FillValue','scale_factor','add_offset','valid_range') if k in attrs})}
    if quality is not None:
        flags,counts=np.unique(quality[inside],return_counts=True)
        result['dqf_counts']={str(int(f)):int(c) for f,c in zip(flags,counts)}
    for key in ('fill','no_coverage','bitmap_missing','nonfinite','outside_range','ambiguous_shear_zero','strict_good','good_plus_conditional','quality_unknown','valid_zero'):
        result[key+'_percent']=100*result[key]/count if count else None
    for label,valid in [('strict',strict),('conditional',conditional),('numeric',numeric)]:
        values=physical[valid]
        result[label+'_stats']={name:None for name in ('minimum','maximum','mean','std','q25','median','q75')}
        if values.size:
            q=np.quantile(values,[.25,.5,.75])
            result[label+'_stats']=dict(minimum=float(values.min()),maximum=float(values.max()),
                mean=float(values.mean()),std=float(values.std()),q25=float(q[0]),median=float(q[1]),q75=float(q[2]))
    return (result,classes) if return_classes else result


def coverage_map(location, output=DEFAULT_OUTPUT, **filters):
    """Show exclusive diagnostic classes for one selected native observation."""
    import matplotlib.pyplot as plt
    from matplotlib.colors import BoundaryNorm
    rows=selected_archives(location,**filters)
    if not rows:raise ValueError('No completed observations match the selection')
    row=rows[0];band=filters.get('band') or row.get('band')
    if band is None and row['source']=='goes':band=int(next(iter(row['marker']['band_counts']))[1:])
    grouped=row['product'] in ('ABI-L2-MCMIPF','ABI-L2-CMI-2KM-HYBRID')
    with open_raw(row['path'],group=f'C{band:02d}' if grouped else None) as ds:
        stamp=np.datetime64(utc(row['assets'][0]['time']).replace(tzinfo=None),'ns')
        one=_scan_attrs(ds.sel(time=stamp,drop=False).load())
        variable='measurement' if row['source']=='mrms' else f'CMI_C{band:02d}'
        stats,classes=pixel_statistics(one,variable,return_classes=True)
        fig,ax=plt.subplots(figsize=(9,5));cmap=plt.get_cmap('tab20',11)
        image=classes[0] if classes.ndim==3 and classes.shape[0]==1 else classes
        im=ax.imshow(image,cmap=cmap,norm=BoundaryNorm(np.arange(-.5,11.5),11),interpolation='nearest')
        bar=fig.colorbar(im,ax=ax,ticks=np.arange(11))
        bar.ax.set_yticklabels(['Good','Fill','No coverage','Bitmap gap','Nonfinite','Other/bad DQF',
            'Outside range','Ambiguous shear zero','Unknown quality','Conditional DQF','Invalid calibration'])
        ax.set_title(f"{row['product']} {stamp} UTC");ax.set_xlabel('Native column');ax.set_ylabel('Native row')
        fig.tight_layout();Path(output).mkdir(parents=True,exist_ok=True)
        fig.savefig(Path(output)/'coverage-example.png',dpi=120)
    return fig,stats


def connection(output=DEFAULT_OUTPUT):
    out=Path(output);out.mkdir(parents=True,exist_ok=True)
    con=duckdb.connect(str(out/'report.duckdb'))
    con.execute('CREATE TABLE IF NOT EXISTS diagnostics (audit_key VARCHAR, path VARCHAR, product VARCHAR, band INTEGER, time TIMESTAMP, patch VARCHAR, payload JSON, PRIMARY KEY(audit_key,time,patch))')
    con.execute('CREATE TABLE IF NOT EXISTS audits (audit_key VARCHAR PRIMARY KEY, path VARCHAR, archive_hash VARCHAR, code_hash VARCHAR, status VARCHAR, details JSON)')
    con.execute('CREATE TABLE IF NOT EXISTS measurements (measurement_id VARCHAR PRIMARY KEY, kind VARCHAR, payload JSON)')
    return con


def _diagnose(location, output=DEFAULT_OUTPUT, full=False, batch_frames=8, memory_mib=512,
             patches=None, progress=None, **filters):
    """One archive opener per band, one worker; resume immutable observation rows."""
    if min(batch_frames,memory_mib)<1:raise ValueError('Batch and memory limits must be positive')
    rows=selected_archives(location,**filters);prov=provenance();scopes={'Whole ROI':None,**(PATCHES if patches is None else patches)}
    processed=reused=0;outcomes=[]
    with connection(output) as con:
        for archive in rows:
            path=archive['path'];actual_hash=file_hash(path)
            if archive['archive_sha256'] and actual_hash!=archive['archive_sha256']:
                raise IOError(f'Archive hash differs from verified marker: {path}')
            marker=archive['marker']
            bands=([int(filters['band'])] if filters.get('band') else
                [int(name[1:]) for name in marker.get('band_counts',{})] if marker.get('band_counts') else [archive.get('band')])
            for band in bands:
                key=hashlib.sha256(json.dumps([actual_hash,prov['code_hash'],band,scopes],sort_keys=True).encode()).hexdigest()
                con.execute('INSERT OR REPLACE INTO audits VALUES (?,?,?,?,?,?)',[key,path,actual_hash,prov['code_hash'],'running',json.dumps(prov)])
                grouped=marker.get('product') in ('ABI-L2-MCMIPF','ABI-L2-CMI-2KM-HYBRID')
                with open_raw(path,group=f'C{band:02d}' if grouped else None) as ds:
                    variable='measurement' if archive['source']=='mrms' else f'CMI_C{band:02d}'
                    allowed={np.datetime64(utc(a['time']).replace(tzinfo=None),'ns') for a in archive['assets']}
                    indices=[i for i,t in enumerate(ds.time.values) if np.datetime64(t,'ns') in allowed]
                    if not full:
                        # First and nearest-noon observations on first day of each quarter represented.
                        groups=defaultdict(list)
                        for i in indices:
                            t=pd.Timestamp(ds.time.values[i]);groups[(t.year,(t.month-1)//3)].append(i)
                        indices=sorted({i for portion in groups.values() for i in
                            (portion[0],min(portion,key=lambda j:abs((pd.Timestamp(ds.time.values[j])-pd.Timestamp(ds.time.values[portion[0]]).normalize()).total_seconds()-43200)))})
                    per_frame=sum(v.size//max(1,ds.sizes.get('time',1))*v.dtype.itemsize for v in ds.data_vars.values())
                    # float64 decoding and pixel masks need substantially more space than packed bytes.
                    effective=max(1,min(batch_frames,int(memory_mib*1024**2//max(1,per_frame*12))))
                    for offset in range(0,len(indices),effective):
                        portion=indices[offset:offset+effective]
                        pending=[]
                        for i in portion:
                            stamp=pd.Timestamp(ds.time.values[i]).to_pydatetime()
                            found=con.execute('SELECT count(*) FROM diagnostics WHERE audit_key=? AND time=?',[key,stamp]).fetchone()[0]
                            if found==len(scopes):reused+=1
                            else:pending.append(i)
                        if not pending:continue
                        batch=ds.isel(time=pending).load()
                        for j,i in enumerate(pending):
                            one=_scan_attrs(batch.isel(time=j,drop=False))
                            stamp=pd.Timestamp(ds.time.values[i]).to_pydatetime()
                            from .diagnostics import _patch_mask
                            records=[]
                            for label,bbox in scopes.items():
                                mask=None if bbox is None else _patch_mask(one,bbox).values
                                stats=pixel_statistics(one,variable,mask)
                                records.append([key,path,archive['product'],band,stamp,label,json.dumps(stats)])
                            with con.cursor() as cur:
                                cur.execute('BEGIN')
                                try:
                                    cur.executemany('INSERT OR REPLACE INTO diagnostics VALUES (?,?,?,?,?,?,?)',records)
                                    cur.execute('COMMIT')
                                except Exception:cur.execute('ROLLBACK');raise
                            processed+=1
                            if progress:progress(processed,reused,path,stamp)
                        batch.close()
                    con.execute('UPDATE audits SET status=?,details=? WHERE audit_key=?',
                        ['complete' if full else 'sample_complete',json.dumps({**prov,'scope':'full selected observations' if full else 'quarter sample','observations':len(indices),'effective_batch':effective,'filters':filters,'patches':scopes}),key])
                outcomes.append({'path':path,'band':band,'audit_key':key,'observations':len(indices)})
    result={'processed':processed,'reused':reused,'full':full,'audits':outcomes,'provenance':prov}
    write_json(Path(output)/'audit-summary.json',result)
    return result


def diagnose(location, output=DEFAULT_OUTPUT, **kwargs):
    """Retain completed rows and mark a failed audit so it can be resumed."""
    try:
        return _diagnose(location,output,**kwargs)
    except Exception as exc:
        with connection(output) as con:
            con.execute("UPDATE audits SET status='failed' WHERE status='running' AND code_hash=?",
                        [provenance()['code_hash']])
        write_json(Path(output)/'audit-failure.json',{'error':f'{type(exc).__name__}: {exc}',
            'completed_rows_retained':True,'provenance':provenance()})
        raise


def import_measurements(paths, output=DEFAULT_OUTPUT, kind='historical'):
    """Import only measured counters and labels, never duplicate source provenance."""
    labels={'source','product','band','month','year','method','stage','scope','measurement_scope',
        'phase_scope','satellite','repeat','status','kind','run_id','started_at','values_equal',
        'source_hashes_equal','profile','read_mode','download_concurrency','read_processes',
        'prefetch_mib','staging_mib','workers','monthly_writers','records_count','observations'}
    def measured(value):
        if not isinstance(value,dict):return value
        return {key:([measured(v) for v in val] if isinstance(val,list) else measured(val))
            for key,val in value.items() if key in labels or key.endswith(('_bytes','_seconds'))
            or key in ('wall_s','wall_seconds','rows','monthly_archives','monthly_metrics')}
    records=[]
    with connection(output) as con:
        for root in paths:
            root=Path(root)
            files=[root] if root.is_file() else list(root.rglob('*-monthly.json'))+list(root.rglob('*-remote-bundle.json'))+[p for name in ('suite.json','stock.json','matched.json','historical.json','hf-restore.json') for p in root.rglob(name)]
            for path in files:
                data=json.loads(path.read_text());file_digest=file_hash(path)
                entries=data.get('evidence',[{'path':str(path),'summary':data}])
                for entry in entries:
                    payload=measured(entry.get('summary',{}))
                    if not payload or not any(k in payload for k in ('rows','monthly_archives','wall_s','wall_seconds','write_seconds','read_bytes','stored_bytes')):continue
                    payload['evidence_path']=entry.get('path',str(path))
                    payload['evidence_sha256']=entry.get('sha256',file_digest)
                    payload['measurement_scope']=f'{kind} persisted log; no invented daily or band timings'
                    identity=hashlib.sha256(json.dumps(payload,sort_keys=True,default=str).encode()).hexdigest()
                    con.execute('INSERT OR REPLACE INTO measurements VALUES (?,?,?)',[identity,kind,json.dumps(payload)])
                    records.append(payload)
    return records


def export_report(output=DEFAULT_OUTPUT):
    """Stream aggregate exports and keep the diagnostic preview bounded."""
    out=Path(output);prov=provenance()
    with connection(out) as con:
        count=con.execute('SELECT count(*) FROM diagnostics d JOIN audits a USING(audit_key) WHERE a.code_hash=?',[prov['code_hash']]).fetchone()[0]
        audits=con.execute('SELECT * FROM audits WHERE code_hash=?',[prov['code_hash']]).fetchdf()
        measurements=con.execute('SELECT * FROM measurements').fetchdf()
        preview=con.execute('SELECT d.* FROM diagnostics d JOIN audits a USING(audit_key) WHERE a.code_hash=? ORDER BY product,band,time,patch LIMIT 1000',[prov['code_hash']]).fetchdf()
        drift=con.execute("""SELECT d.path,product,band,count(*) AS observations,
            count(DISTINCT json_extract(payload,'$.calibration')) AS observed_calibration_encodings,
            count(DISTINCT json_extract(payload,'$.packed_dtype')) AS observed_packed_dtypes
            FROM diagnostics d JOIN audits a USING(audit_key) WHERE a.code_hash=? AND patch='Whole ROI'
            GROUP BY ALL ORDER BY product,band,path""",[prov['code_hash']]).fetchdf()
        drift.to_csv(out/'metadata-drift.csv',index=False)
        # Counts can be pooled, image quartiles cannot. Different products/bands remain distinct.
        aggregate=con.execute("""SELECT product,band,patch,date_trunc('month',time) AS month,
            count(*) AS observations,sum(CAST(json_extract(payload,'$.pixels') AS BIGINT)) AS pixels,
            sum(CAST(json_extract(payload,'$.strict_good') AS BIGINT)) AS strict_good,
            sum(CAST(json_extract(payload,'$.good_plus_conditional') AS BIGINT)) AS good_plus_conditional,
            sum(CAST(json_extract(payload,'$.bitmap_missing') AS BIGINT)) AS bitmap_missing,
            sum(CAST(json_extract(payload,'$.quality_unknown') AS BIGINT)) AS quality_unknown
            FROM diagnostics d JOIN audits a USING(audit_key) WHERE a.code_hash=?
            GROUP BY ALL ORDER BY product,band,patch,month""",[prov['code_hash']]).fetchdf()
        for unit in ('day','week','year'):
            frame=con.execute(f"""SELECT product,band,patch,date_trunc('{unit}',time) AS period,
                count(*) AS observations,sum(CAST(json_extract(payload,'$.pixels') AS BIGINT)) AS pixels,
                sum(CAST(json_extract(payload,'$.strict_good') AS BIGINT)) AS strict_good,
                sum(CAST(json_extract(payload,'$.good_plus_conditional') AS BIGINT)) AS good_plus_conditional
                FROM diagnostics d JOIN audits a USING(audit_key) WHERE a.code_hash=?
                GROUP BY ALL ORDER BY product,band,patch,period""",[prov['code_hash']]).fetchdf()
            frame.to_csv(out/f'diagnostic-{unit}s.csv',index=False)
    for name,frame in [('diagnostic-preview',preview),('diagnostic-months',aggregate),('audits',audits),('measurements',measurements)]:
        frame.to_csv(out/f'{name}.csv',index=False)
    summary={'diagnostic_rows':count,'audits':len(audits),'measurements':len(measurements),
        'provenance':prov,'quartiles':'per observation/patch; no pooled quartile claim',
        'preview_limit':1000,'source_values':'read-only raw archives; masking and decoding are report-only'}
    write_json(out/'summary.json',summary)
    (out/'report.md').write_text('# Dataset report\n\n'+json.dumps(summary,indent=2)+'\n\nSee diagnostic-months.csv, diagnostic-preview.csv, archives.csv and measurements.csv.\n')
    if len(aggregate):
        import matplotlib.pyplot as plt
        fig,ax=plt.subplots(figsize=(10,4))
        for (product,band),group in aggregate[aggregate.patch=='Whole ROI'].groupby(['product','band'],dropna=False):
            ax.plot(pd.to_datetime(group.month),group.strict_good/group.pixels.replace(0,np.nan)*100,label=f'{product}'+(f' C{int(band):02d}' if pd.notna(band) else ''))
        ax.set_ylabel('Strict-good pixels (%)');ax.set_xlabel('Month UTC');ax.legend(fontsize=7);fig.tight_layout()
        fig.savefig(out/'coverage-timeline.png',dpi=120);plt.close(fig)
    from .report_views import export
    summary.update(export(out))
    return summary


def index_measurements(output=DEFAULT_OUTPUT, index_path=None):
    """Read fetch measurements without taking a write lock; manifests are fallback."""
    import os
    path=Path(index_path or os.getenv('ECORE_INDEX_PATH','results/archive_index.duckdb'))
    if not path.is_file():return {'status':'index_missing','fallback':'completed manifests'}
    try:
        with duckdb.connect(str(path),read_only=True) as source:
            rows=source.execute('SELECT run_id,source,product,destination,started_at,wall_seconds,source_bytes,stored_bytes,peak_rss_bytes,status FROM fetch_runs').fetchdf().to_dict('records')
    except duckdb.Error as exc:
        return {'status':'index_unavailable','fallback':'completed manifests','reason':str(exc).splitlines()[0]}
    with connection(output) as con:
        for row in rows:
            identity='index:'+row['run_id']
            con.execute('INSERT OR REPLACE INTO measurements VALUES (?,?,?)',[identity,'fetch_index',json.dumps(jsonable(row),default=str)])
    return {'status':'read_only','runs':len(rows)}


def coverage_frequency(location,output=DEFAULT_OUTPUT,maximum_frames=24,**filters):
    """Bounded frequency map across a selected archive, sampled native display pixels."""
    import matplotlib.pyplot as plt
    rows=selected_archives(location,**filters)
    if not rows:raise ValueError('No completed observations match the selection')
    row=rows[0];band=filters.get('band') or row.get('band')
    if band is None and row['source']=='goes':band=int(next(iter(row['marker']['band_counts']))[1:])
    grouped=row['product'] in ('ABI-L2-MCMIPF','ABI-L2-CMI-2KM-HYBRID')
    assets=row['assets'];indices=np.unique(np.linspace(0,len(assets)-1,min(len(assets),maximum_frames),dtype=int))
    frequency=None;totals=0
    with open_raw(row['path'],group=f'C{band:02d}' if grouped else None) as ds:
        for index in indices:
            stamp=np.datetime64(utc(assets[index]['time']).replace(tzinfo=None),'ns')
            one=_scan_attrs(ds.sel(time=stamp,drop=False).load())
            variable='measurement' if row['source']=='mrms' else f'CMI_C{band:02d}'
            stats,classes=pixel_statistics(one,variable,return_classes=True)
            image=np.squeeze(classes);stride=max(1,int(np.ceil(max(image.shape)/512)))
            valid=(image[::stride,::stride]==0).astype('float64')
            if frequency is None:frequency=np.zeros(valid.shape,dtype='float64')
            frequency+=valid;totals+=1
    fig,ax=plt.subplots(figsize=(9,5));im=ax.imshow(frequency/totals*100,vmin=0,vmax=100,cmap='viridis')
    fig.colorbar(im,ax=ax,label='Strict-good observations (%)')
    ax.set(title=f"{row['product']}: {totals} sampled observations from one archive",xlabel='Sampled native columns',ylabel='Sampled native rows')
    fig.tight_layout();Path(output).mkdir(parents=True,exist_ok=True);fig.savefig(Path(output)/'coverage-frequency.png',dpi=120)
    return fig,{'observations':totals,'selected_observations':len(assets),'scope':'bounded one-archive temporal sample; native display pixels sampled, no reprojection'}
