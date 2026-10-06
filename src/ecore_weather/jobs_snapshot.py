"""Freeze the exact Python code used by a long study job under ignored results/."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess


def main(argv=None):
    parser = argparse.ArgumentParser(description="Create a self-contained study code snapshot")
    parser.add_argument("--output-root", default="results/study-code-snapshots")
    args = parser.parse_args(argv)
    repository = Path(__file__).resolve().parents[2]
    # The staged GOES runner invokes notebooks/02_goes.py as its CLI entrypoint.
    # Keep it in the snapshot so later notebook edits cannot change an active job.
    paths = sorted(path for folder in ("src", "scripts", "notebooks", "PR_rain_512_crop")
                   for path in (repository / folder).rglob("*.py")
                   if "__pycache__" not in path.parts)
    paths += [repository/'environment.yml',repository/'pyproject.toml',*sorted((repository/'jobs').glob('*'))]
    sha = hashlib.sha256()
    entries = []
    for path in paths:
        relative = path.relative_to(repository).as_posix()
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        sha.update(relative.encode() + b"\0" + digest.encode() + b"\n")
        entries.append({"path": relative, "sha256": digest})
    digest = sha.hexdigest()
    destination = Path(args.output_root).resolve() / digest[:16]
    if not destination.exists():
        for entry in entries:
            source = repository / entry["path"]
            target = destination / entry["path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
        manifest = {"sha256": digest,
            "git_revision": subprocess.check_output(["git", "rev-parse", "HEAD"],
                cwd=repository, text=True).strip(),
            "files": entries}
        (destination / "snapshot.json").write_text(json.dumps(manifest, indent=2) + "\n")
    for entry in entries:
        target = destination / entry["path"]
        if not target.is_file() or hashlib.sha256(target.read_bytes()).hexdigest() != entry["sha256"]:
            raise IOError(f"Existing study snapshot differs: {target}")
    print(json.dumps({"snapshot": str(destination), "sha256": digest,
                      "files": len(entries)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
