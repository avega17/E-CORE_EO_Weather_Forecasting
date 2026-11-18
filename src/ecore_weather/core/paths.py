"""Resolve project outputs consistently in scripts, notebooks and worker copies."""
from pathlib import Path
import os


def repository_root():
    explicit = os.getenv('ECORE_REPO_ROOT') or os.getenv('ECORE_CODE_ROOT')
    if explicit:
        return Path(explicit).expanduser().resolve()
    for origin in (Path(__file__).resolve().parent, Path.cwd()):
        for candidate in (origin, *origin.parents):
            if (candidate / 'src/ecore_weather').is_dir():
                return candidate
    return Path.cwd()


def result_path(relative):
    """Resolve a built-in default; explicit CLI paths remain caller-relative."""
    return repository_root() / 'results' / relative


def _receipt_key(path):
    path = Path(path)
    for parent in path.parents:
        if parent.name in ('months', 'checkpoints'):
            return parent.parent/'study.json', path.relative_to(parent.parent).as_posix()
    return None, None


class ReceiptPath(type(Path())):
    """Read older individual receipts or a coordinator's portable study journal."""
    def is_file(self):
        if super().is_file():
            return True
        journal, key = _receipt_key(self)
        if journal is None or not journal.is_file():
            return False
        import json
        return key in json.loads(journal.read_text()).get('tasks', {})

    exists = is_file

    def read_text(self, *args, **kwargs):
        if super().is_file():
            return super().read_text(*args, **kwargs)
        import json
        journal, key = _receipt_key(self)
        if journal is None:
            return super().read_text(*args, **kwargs)
        return json.dumps(json.loads(journal.read_text())['tasks'][key])


def store_receipt(path, value):
    """Atomically replace the compact journal; each study has one coordinator."""
    journal, key = _receipt_key(path)
    if journal is None or value.get('status') not in ('complete', 'unavailable'):
        return False
    import json
    from ecore_weather.core.common import write_json
    state = json.loads(journal.read_text()) if journal.exists() else {'schema': 1, 'tasks': {}}
    state['tasks'][key] = value
    write_json(journal, state)
    # A journal commit supersedes only this exact old receipt, never arrays.
    Path(path).unlink(missing_ok=True)
    return True
