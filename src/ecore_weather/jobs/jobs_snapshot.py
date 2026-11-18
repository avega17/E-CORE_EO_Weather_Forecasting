"""Create one execution copy without Git inspection or code-hash gates."""
from pathlib import Path
import argparse
import json
import shutil
from ecore_weather.core.paths import repository_root


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-root', default='.cache/runtime')
    args = parser.parse_args(argv)
    root = repository_root()
    target = Path(args.output_root).resolve() / 'current'
    if target.exists():
        raise FileExistsError(f'Runtime copy already exists; retire it after its job stops: {target}')
    for name in ('src', 'scripts', 'notebooks', 'jobs', 'PR_rain_512_crop'):
        if (root/name).is_dir():
            shutil.copytree(root/name, target/name,
                ignore=shutil.ignore_patterns('__pycache__', '*.ipynb', 'results', '*.log', '.env'))
    for name in ('environment.yml', 'pyproject.toml'):
        shutil.copyfile(root/name, target/name)
    print(json.dumps({'snapshot': str(target), 'sha256': None,
                      'purpose': 'execution isolation; no code identity checks'}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
