import json
from types import SimpleNamespace
from ecore_weather.common import Asset
from ecore_weather.fetch_progress import collect_progress,register_progress


def test_progress_survives_deleted_scratch_and_index_lock(tmp_path,monkeypatch):
    from ecore_weather import catalog,index
    assets=[Asset('noaa-goes19',f'ABI-L2-MCMIPF/{i}.nc',100,'etag',f'2026-05-01T00:{i}0:00Z') for i in range(2)]
    selection=SimpleNamespace(source='goes',product='ABI-L2-MCMIPF',satellite=19,assets=assets)
    results=tmp_path/'results';stac=results/'study-test/h1-2026/2026-05/ABI-L2-MCMIPF/items.json'
    stac.parent.mkdir(parents=True);stac.write_text('{}')
    monkeypatch.setattr(catalog,'load_selection',lambda p:selection)
    def locked(*a,**k):raise OSError('database locked')
    monkeypatch.setattr(index,'connect',locked)
    progress=results/'study-scratch/arbitrary-hash/progress.json'
    progress.parent.mkdir(parents=True);progress.write_text(json.dumps({'asset_ids':[a.id for a in assets],'next_index':1,'product':selection.product}))
    destination=tmp_path/'archives'
    kwargs=dict(results=results,destination=destination,product=selection.product)
    frame,state=collect_progress(**kwargs)
    assert frame.iloc[0].checkpointed_objects==1 and frame.iloc[0].remaining_objects==1
    marker=destination/'goes/ABI-L2-MCMIPF/goes19/roi-x/2026/05/complete.json'
    marker.parent.mkdir(parents=True);marker.write_text(json.dumps({'source':'goes','product':selection.product,'satellite':19,
        'assets':[{'asset_id':a.id,'time':a.time,'status':'ok' if i==0 else 'corrupt_source'} for i,a in enumerate(assets)]}))
    progress.unlink()
    frame,state=collect_progress(**kwargs)
    row=frame.iloc[0]
    assert row.state=='complete' and row.remaining_objects==0
    assert row.archived_objects==1 and row.corrupt_source_objects==1
    import duckdb
    with duckdb.connect() as db:
        register_progress(db,**kwargs)
        assert db.execute('SELECT sum(remaining_objects) FROM fetch_progress').fetchone()[0]==0
