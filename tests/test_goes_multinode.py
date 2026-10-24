import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from ecore_weather import index, jobs_multinode as multi
from ecore_weather.index_merge import merge_snapshots


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))
    return path


def config(tmp_path):
    data = json.loads(Path('jobs/goes_polaris_partitions.json').read_text())
    return write_json(tmp_path/'partitions.json', data)


def test_three_partitions_cover_remaining_study_once(tmp_path):
    rows = multi.partitions(config(tmp_path))
    assert len(rows) == 3
    assert rows[0].start == '2022-01-01' and rows[-1].end == '2026-07-01'
    assert all(a.end == b.start for a, b in zip(rows, rows[1:]))


@pytest.mark.parametrize('change,match', [
    (lambda d: d['partitions'][1].update(start='2023-12-01'), 'contiguous'),
    (lambda d: d['partitions'][1].update(start='2024-02-01'), 'contiguous'),
    (lambda d: d['partitions'][0].update(start='2022-01-02'), 'month boundaries'),
    (lambda d: d['partitions'][0].update(label='../unsafe'), 'labels'),
    (lambda d: d['partitions'][1].update(label='part-0'), 'uniquely'),
    (lambda d: d.update(product='ABI-L2-MCMIPF'), 'native CMIPF'),
    (lambda d: d.update(bands=[13]), 'native CMIPF'),
])
def test_invalid_partition_rejected_before_launch(tmp_path, change, match):
    path = config(tmp_path)
    data = json.loads(path.read_text()); change(data); write_json(path, data)
    with pytest.raises(ValueError, match=match): multi.partitions(path)


def test_host_slots_deduplicate_and_require_three_nodes(tmp_path):
    path = tmp_path/'nodes'; path.write_text('node-a\nnode-a\nnode-b\nnode-c\n')
    assert multi.hosts(path) == ['node-a', 'node-b', 'node-c']
    path.write_text('node-a\nnode-b\n')
    with pytest.raises(ValueError, match='three distinct'): multi.hosts(path)


def test_ensemble_lease_prevents_duplicate_owners(tmp_path):
    with multi.ownership(tmp_path):
        with pytest.raises(RuntimeError, match='Another multi-node'):
            with multi.ownership(tmp_path): pass


def archive(tmp_path, source='goes'):
    folder = tmp_path/source/'CMIPF'/'goes16'/'roi-test'/'C13'/'2022'/'01'
    folder.mkdir(parents=True)
    path = folder/'raw.zarr.zip'; path.write_bytes(b'scientific archive')
    write_json(folder/'complete.json', {'archive_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
               'stored_bytes':path.stat().st_size, 'source':source, 'product':'CMIPF', 'band':13,
               'observations':8, 'assets':[{'asset_id':'one', 'time':'2022-01-01T00:00:00Z', 'source_bytes':123}]})
    return path


def seed(path, raw=None, *, state='running', updated=1, task_state='fetching'):
    with index.connect(path) as con:
        con.execute('INSERT INTO jobs VALUES (?,?,?,?,?,?,?)', ['run',state,123,'node',0,updated,'{}'])
        if raw:
            con.execute('INSERT INTO archives VALUES (?,?,?,?,?,?,?,?,?,?,?)', index._archive_row(raw))
            con.execute('INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                        [str(raw),'run','goes','CMIPF','2022-01',13,8,4,0,task_state,updated,'{}'])
        con.execute('CHECKPOINT')


def test_merge_preserves_history_deduplicates_and_prefers_verified_archive(tmp_path):
    raw = archive(tmp_path)
    one, two, canonical = [tmp_path/name for name in ('one.duckdb','two.duckdb','canonical.duckdb')]
    seed(one, raw, state='complete', updated=1, task_state='complete')
    seed(two, raw, state='running', updated=2, task_state='failed')
    with index.connect(canonical) as con:
        con.execute("INSERT INTO jobs VALUES ('mrms-run','complete',1,'node',0,1,'{}')")
    result = merge_snapshots([one, two, one], canonical, tmp_path/'scratch')
    assert result['rows']['archives'] == 1 and result['rows']['jobs'] == 2
    assert result['archive_bytes'] == raw.stat().st_size
    with index.connect(canonical, read_only=True) as con:
        assert con.execute('SELECT status,completed FROM tasks').fetchone() == ('complete',8)
        assert con.execute("SELECT status FROM jobs WHERE run_id='run'").fetchone()[0] == 'interrupted'
    assert one.exists() and two.exists() and raw.exists()


def test_merge_normalizes_alias_paths_without_double_counting(tmp_path):
    raw = archive(tmp_path)
    alias = tmp_path/'alias'; alias.symlink_to(raw.parent, target_is_directory=True)
    one, two = tmp_path/'one.duckdb', tmp_path/'two.duckdb'
    seed(one, raw)
    with index.connect(two) as con:
        row = index._archive_row(raw); row[0] = str(alias/raw.name); row[-1] = str(alias/'complete.json')
        con.execute('INSERT INTO archives VALUES (?,?,?,?,?,?,?,?,?,?,?)', row)
    assert merge_snapshots([one,two],tmp_path/'out.duckdb',tmp_path/'work')['rows']['archives'] == 1


@pytest.mark.parametrize('failure', ['conflict','missing','wal','schema'])
def test_failed_merge_preserves_canonical_and_inputs(tmp_path, failure):
    raw = archive(tmp_path)
    one, canonical = tmp_path/'one.duckdb', tmp_path/'canonical.duckdb'
    seed(one, raw); seed(canonical, raw, state='complete')
    before = canonical.read_bytes()
    if failure == 'conflict':
        with index.connect(one) as con: con.execute("UPDATE archives SET sha256='conflicting'")
    elif failure == 'missing': raw.unlink()
    elif failure == 'wal': Path(str(one)+'.wal').write_bytes(b'unclosed writer')
    else:
        with index.connect(one) as con: con.execute('ALTER TABLE tasks ADD COLUMN unsupported INTEGER')
    with pytest.raises((ValueError, FileNotFoundError)):
        merge_snapshots([one], canonical, tmp_path/'scratch')
    assert canonical.read_bytes() == before and one.is_file()
    assert not canonical.with_name('canonical.duckdb.publishing').exists()


def test_merge_missing_snapshot_does_not_remove_other_snapshots(tmp_path):
    state = tmp_path/'state/goes/study'
    write_json(state/'ensemble.json', {'partitions':[{'label':f'part-{i}'} for i in range(3)]})
    first = state/'partitions/part-0/archive_index.duckdb'; seed(first)
    with pytest.raises(FileNotFoundError):
        multi._merge(state,tmp_path/'scratch',tmp_path/'canonical.duckdb')
    assert first.is_file()


def test_successful_merge_keeps_one_operational_database(tmp_path):
    state = tmp_path/'state/goes/study'
    write_json(state/'ensemble.json', {'partitions':[{'label':f'part-{i}'} for i in range(3)]})
    snapshots = [state/f'partitions/part-{i}/archive_index.duckdb' for i in range(3)]
    for snapshot in snapshots: seed(snapshot)
    target = tmp_path/'state/archive_index.duckdb'
    result = multi._merge(state,tmp_path/'scratch',target)
    assert target.is_file() and result['rows']['jobs'] == 1
    assert all(not p.exists() for p in snapshots)
    assert multi._merge(state,tmp_path/'scratch',target) == result


def test_merge_interruption_before_replacement_keeps_old_database(tmp_path,monkeypatch):
    from ecore_weather import index_merge
    one, canonical = tmp_path/'one.duckdb', tmp_path/'canonical.duckdb'
    seed(one); seed(canonical,state='complete')
    before=canonical.read_bytes()
    monkeypatch.setattr(index_merge.os,'replace',lambda *a,**k:(_ for _ in ()).throw(OSError('publication interrupted')))
    with pytest.raises(OSError,match='publication interrupted'):
        merge_snapshots([one],canonical,tmp_path/'work')
    assert canonical.read_bytes()==before and one.is_file()


def test_status_filters_partition_and_reads_only_durable_progress(tmp_path,monkeypatch):
    from ecore_weather import job_status
    state=tmp_path/'state'
    write_json(state/'ensemble.json',{'partitions':[
        {'label':f'part-{i}','start':'2022-01-01','end':'2023-01-01'} for i in range(3)]})
    write_json(state/'partitions/part-1/progress.json',{'runs':[],'tasks':[]})
    calls=[]
    monkeypatch.setattr(job_status,'main',lambda args,**kw:calls.append(args) or 0)
    assert multi.aggregate_status(state,partition='part-1',start='2022-05-01',end='2022-07-01')==0
    assert len(calls)==1 and '2022-05-01' in calls[0]
    assert not list(state.rglob('*.duckdb'))
    with pytest.raises(ValueError,match='Unknown partition'):multi.aggregate_status(state,partition='typo')


def test_prepare_copies_only_owned_saved_selections(tmp_path):
    state, legacy = tmp_path/'part', tmp_path/'legacy'
    for month in ('2022-01','2024-01'):
        folder = legacy/f'fetch/h1-{month[:4]}/{month}/ABI-L2-CMIPF'
        write_json(folder/'collection.json',{'copied':month})
        (folder/'items.json.gz').write_bytes(b'compressed selection')
        (folder/'summary.json').write_text('not copied')
    result = multi.prepare_worker(state,legacy,'2022-01-01','2024-01-01')
    assert result['copied_selection_files'] == 2
    assert (state/'fetch/h1-2022/2022-01/ABI-L2-CMIPF/items.json.gz').exists()
    assert not list(state.rglob('summary.json'))
    assert not (state/'fetch/h1-2024').exists()


def test_portable_gap_evidence_matches_source_and_is_not_a_blanket_skip(tmp_path,monkeypatch):
    from ecore_weather import catalog, goes
    from ecore_weather.common import Asset, Selection
    fact = json.loads(Path('jobs/goes_source_issues.json').read_text())['source_issues'][0]
    key = fact['source_url'].split('.amazonaws.com/')[1]
    asset = Asset('noaa-goes16',key,fact['source_bytes'],fact['etag'],fact['time'],
                  '2023-10-13T14:29:51.800000Z')
    selected = Selection('goes','ABI-L2-CMIPF','2023-10-01','2023-11-01',tuple(fact['region']),
                         [asset],satellite=16,bands=(1,2,3,7,8,9,10,13))
    monkeypatch.setattr(goes,'discover',lambda *a,**k:selected)
    state = tmp_path/'part'
    result = multi.prepare_worker(state,tmp_path/'old','2022-01-01','2024-01-01',issue_file='jobs/goes_source_issues.json')
    assert result['imported_source_issues'] == 1
    saved = catalog.load_selection(state/'fetch/h2-2023/2023-10/ABI-L2-CMIPF/collection.json')
    assert list(catalog.confirmed_source_issues(saved)) == [asset.id]
    assert saved.assets == [asset]  # Retain the object and explicit evidence.


@pytest.mark.parametrize('codes,expected',[([0,0,0],0),([0,75,0],75),([1,75,0],1)])
def test_aggregate_exit_preserves_pauses_and_failures(codes,expected):
    assert multi._exit_code(codes) == expected


def test_launch_has_distinct_state_hostfiles_and_no_nested_mpi(tmp_path,monkeypatch):
    selected = config(tmp_path)
    nodefile = tmp_path/'nodes'; nodefile.write_text('a\nb\nc\n')
    state = tmp_path/'state/goes/study'
    monkeypatch.setenv('ECORE_PROJECT_ROOT',str(tmp_path))
    monkeypatch.setenv('ECORE_NODE_SCRATCH',str(tmp_path/'scratch'))
    monkeypatch.delenv('ECORE_GOES_STORAGE_CAP_GB',raising=False)
    monkeypatch.delenv('ECORE_PROFILE',raising=False)
    commands = []
    class Process:
        def __init__(self,args,**kwargs):
            import io
            commands.append((args,kwargs['env']))
            self.returncode = 75 if len(commands) == 2 else 0
            self.stdout = io.StringIO('mock worker output\n')
        def poll(self): return self.returncode
    monkeypatch.setattr(multi.subprocess,'Popen',Process)
    # Missing snapshots must leave merge pending, never discard evidence.
    code = multi.launch(selected,nodefile,state,Path.cwd(),sys.executable,3600)
    assert code == 75 and len(commands) == 3
    assert len({env['ECORE_GOES_STATE_ROOT'] for _,env in commands}) == 3
    assert len({env['PBS_GOES_START'] for _,env in commands}) == 3
    assert all(env['ECORE_GOES_NODE_ONLY'] == '1' for _,env in commands)
    assert all(args.count('mpiexec') == 1 for args,_ in commands)
    for args,env in commands:
        assert Path(args[args.index('--hostfile')+1]).read_text().strip() in ('a','b','c')
        assert env['ECORE_GOES_CHECKPOINT_DIR'] == str(state/'partial-archives')
    result = json.loads((state/'ensemble.json').read_text())
    assert result['status'] == 'paused' and result['merge'] == 'pending'


def test_multinode_pbs_shell_syntax_and_allocation():
    subprocess.run(['bash','-n','jobs/polaris_goes_multinode.pbs'],check=True)
    script = Path('jobs/polaris_goes_multinode.pbs').read_text()
    assert 'select=3:system=polaris:ncpus=64' in script
    assert 'walltime=24:00:00' in script
    assert 'git ' not in script and 'ECORE_REVISION' not in script


def test_real_independent_processes_publish_then_merge(tmp_path,monkeypatch):
    config_file = config(tmp_path)
    nodefile = tmp_path/'nodes'; nodefile.write_text('a\nb\nc\n')
    executable = tmp_path/'bin/mpiexec'; executable.parent.mkdir()
    executable.write_text('#!'+sys.executable+'\n'+'''import json,os,sys
from pathlib import Path
from ecore_weather import index
state=Path(os.environ['ECORE_GOES_STATE_ROOT'])
label=os.environ['ECORE_GOES_PARTITION']
with index.connect(state/'archive_index.duckdb') as con:
    con.execute("INSERT INTO jobs VALUES (?, ?, 1, 'node',0,1,'{}')",[label,'paused' if label=='part-1' else 'complete'])
    con.execute('CHECKPOINT')
(state/'placement.json').write_text(json.dumps({'host':Path(sys.argv[sys.argv.index('--hostfile')+1]).read_text().strip(),
    'start':os.environ['PBS_GOES_START'],'end':os.environ['PBS_GOES_END']}))
print('Worker closed its snapshot')
sys.exit(75 if label=='part-1' else 0)
''')
    executable.chmod(0o755)
    monkeypatch.setenv('PATH',str(executable.parent)+os.pathsep+os.environ['PATH'])
    monkeypatch.setenv('PYTHONPATH',str(Path('src').resolve()))
    monkeypatch.setenv('ECORE_PROJECT_ROOT',str(tmp_path))
    monkeypatch.setenv('ECORE_NODE_SCRATCH',str(tmp_path/'scratch'))
    monkeypatch.delenv('ECORE_GOES_STORAGE_CAP_GB',raising=False)
    monkeypatch.delenv('ECORE_PROFILE',raising=False)
    state = tmp_path/'state/goes/study'
    assert multi.launch(config_file,nodefile,state,Path.cwd(),sys.executable,3600) == 75
    result=json.loads((state/'ensemble.json').read_text())
    assert result['merge']['status'] == 'complete'
    assert result['exit_codes'] == {'part-0':0,'part-1':75,'part-2':0}
    assert len({json.loads(p.read_text())['host'] for p in state.glob('partitions/*/placement.json')}) == 3
    assert not list(state.glob('partitions/*/archive_index.duckdb'))
    with index.connect(tmp_path/'state/archive_index.duckdb',read_only=True) as con:
        assert con.execute('SELECT count(*) FROM jobs').fetchone()[0] == 3
