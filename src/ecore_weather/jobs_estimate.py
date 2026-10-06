"""Resumable metadata-only GOES study inventory with bounded ROI size samples.

Run with the ecore-weather Conda interpreter. No full-study imagery is fetched.
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys



from ecore_weather.common import PR_BBOX, utc, write_json
from ecore_weather.study_goes import END, START, run


def main(argv=None):
    parser = argparse.ArgumentParser(description="Estimate GOES-East Puerto Rico crop and archive costs")
    parser.add_argument("--start", default=START.isoformat(), help="UTC start, inclusive")
    parser.add_argument("--end", default=END.isoformat(), help="UTC end, excluded")
    parser.add_argument("--bbox", nargs=4, type=float, default=PR_BBOX,
                        metavar=("WEST", "SOUTH", "EAST", "NORTH"))
    parser.add_argument("--output", default="results/study-goes-estimate")
    parser.add_argument("--scratch", help="Linux-local temporary crop sample directory")
    parser.add_argument("--inventory-only", action="store_true",
                        help="List all study metadata but skip bounded native-pixel samples")
    parser.add_argument("--dry-run", action="store_true", help="Print request without listing NOAA files")
    args = parser.parse_args(argv)
    start, end = utc(args.start), utc(args.end)
    if start >= end:
        parser.error("End must be after start")
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    config = {"start": start.isoformat(), "end_excluded": end.isoformat(),
              "bbox": args.bbox, "output": args.output, "sample": not args.inventory_only,
              "git_revision": revision, "python": sys.executable,
              "created_at": datetime.now(timezone.utc).isoformat()}
    if args.dry_run:
        print(json.dumps(config, indent=2))
        return 0
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    write_json(output / "run_config.json", config)
    try:
        result = run(output, bbox=args.bbox, sample=not args.inventory_only,
                     scratch=args.scratch, start=start, end=end)
        with (output / "scenarios.csv").open("w", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["scenario", "band_files", "listed_full_file_bytes",
                "roi_transfer_bytes_central", "uncompressed_crop_netcdf_bytes_central",
                "compressed_zarr_bytes_central", "serial_task_seconds_central",
                "assumed_wall_days_low", "assumed_wall_days_central", "assumed_wall_days_high"])
            for name, row in result["scenarios"].items():
                writer.writerow([name, row["band_files"], row["listed_full_file_bytes"],
                    row["estimated_roi_transfer_bytes"]["central"],
                    row["estimated_uncompressed_crop_netcdf_bytes"]["central"],
                    row["estimated_compressed_zarr_bytes"]["central"],
                    row.get("estimated_total_local_seconds", {}).get("central"),
                    *[(row.get("estimated_local_wall_seconds_assumed", {}).get(label) or 0) / 86400
                      if row.get("estimated_local_wall_seconds_assumed", {}).get(label) is not None else None
                      for label in ("low", "central", "high")]])
        print(f"Completed GOES estimate: {output / 'summary.json'}")
        return 0 if args.inventory_only or result["sample_complete"] else 2
    except Exception as exc:
        write_json(output / "failure.json", {"error": f"{type(exc).__name__}: {exc}",
                                             "config": config})
        print(f"GOES estimate stopped: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
