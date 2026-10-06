"""The same research functions through argparse, suitable for later batch jobs."""

import argparse
import json
from dataclasses import replace
from pathlib import Path

from .common import BENCHMARK_PERIODS, PR_BBOX, default_workers, write_json


def parser(source):
    p = argparse.ArgumentParser(description=f"NOAA {source.upper()} research notebook as a script")
    p.add_argument("--operation", choices=["inspect", "fetch", "benchmark", "validate"] +
                   (["estimate"] if source == "goes" else []), default="inspect",
                   help="GOES estimate uses the fixed 8-band 6/3/1 and 16-band 6-per-hour study scenarios")
    p.add_argument("--period", choices=["2022", "2025"], default="2022")
    p.add_argument("--start", help="UTC start; overrides the selected example period")
    p.add_argument("--end", help="UTC ending date/time, excluded")
    p.add_argument("--bbox", nargs=4, type=float, default=PR_BBOX, metavar=("WEST", "SOUTH", "EAST", "NORTH"))
    if source == "mrms":
        from .mrms import parse_product_argument
        product_type = parse_product_argument
    else:
        product_type = None
    p.add_argument("--product", nargs="+" if source == "mrms" else None,
                   type=product_type, metavar="PRODUCT",
                   help=("MRMS name(s): precipitation-rate, composite-reflectivity, "
                         "base-reflectivity, radar-only-qpe-1h, low-level-azimuthal-shear, "
                         "mid-level-azimuthal-shear, multisensor-qpe-pass1, multisensor-qpe-pass2; "
                         "omit for the four default study fields" if source == "mrms"
                         else "GOES CMIPF or MCMIPF product; estimate always inventories CMIPF"))
    p.add_argument("--selection", help="Reuse a saved STAC collection or item manifest")
    p.add_argument("--skip-index", action="store_true",
                   help="Let a coordinating study runner record DuckDB rows after verification")
    p.add_argument("--workers", type=int, default=8 if source == "goes" else default_workers(),
                   help="GOES: source threads and scan batch limit per reader; HDF5 serializes calls within a process (use --read-processes for parallel HDF5). MRMS: source reads per monthly archive")
    p.add_argument("--decode-workers", type=int, default=1,
                   help="MRMS GRIB decode slots; accepted but unused for GOES")
    if source == "goes": p.add_argument("--read-profiles", help="Validated JSON settings by native band; writer count stays separate")
    p.add_argument("--read-processes", type=int, default=2 if source == "goes" else 0,
                   help="GOES: reader processes per active band-month; MRMS: optional separate file readers")
    if source == 'goes':
        from .goes_shared import add_arguments
        add_arguments(p)
    else:
        p.add_argument("--monthly-writers", type=int, default=2,
                       help="Independent product-month writers; HF publication stays at one")
    p.add_argument("--scratch", help="Fast local staging directory; cleaned after each subset")
    p.add_argument("--backend", choices=["s3fs", "obstore"], default="obstore" if source == "goes" else "s3fs")
    p.add_argument("--destination", default="/mnt/p/ecore_eo_datasets" if source == "goes" else "hf",
                   help="Local archive directory; HF remains available for supported layouts")
    p.add_argument("--output", default=f"results/{source}", help="Small selection and report output folder")
    p.add_argument("--max-files", type=int, help="Explicitly limit a sample; the report records the smaller selection")
    p.add_argument("--save-figures", metavar="DIRECTORY", help="Save PNG figures; off by default")
    p.add_argument("--plot-index", type=int, nargs="+", default=[0], help="Fetched image indices to export")
    p.add_argument("--recipe", choices=["compare", "legacy_exact", "quality_aware"] if source == "mrms" else ["decode", "quality", "reproject"], help="Session-only processing for the exported figure")
    p.add_argument("--center-crop", action="store_true", help="Show a centered 512-pixel MRMS view")
    p.add_argument("--repeats", type=int, default=1, help="Speed benchmark repetitions")
    p.add_argument("--skip-mentor", action="store_true", help="Skip the optional legacy calculation check during validation")
    if source == "mrms":
        p.add_argument("--tolerance-minutes", type=float, default=5)
        p.add_argument("--time-match", choices=["previous", "nearest", "exact"], default="previous")
    else:
        p.add_argument("--satellite", choices=["auto", "16", "17", "18", "19"], default="auto")
        p.add_argument("--bands", nargs="+", type=int, default=[1, 2, 3, 7, 8, 9, 10, 13])
        p.add_argument("--prefetch-mib", type=int, default=512,
                       help="Maximum queued GOES ROI data budget per band-month, in MiB")
        p.add_argument('--read-mode', choices=['range', 'async_full', 'async_pipeline'], default='range',
                       help='Legacy native CMIPF read mode; shared pipeline uses C02 ranges plus seven async bands')
        p.add_argument('--download-concurrency', type=int, default=32,
                       help='Global async whole-object downloads for the shared GOES pipeline')
        p.add_argument('--staging-mib', type=int, default=16384,
                       help='Global staged full-object capacity in MiB')
        p.add_argument("--block-size-kib", type=int, default=1024,
                       help="NOAA HDF5 range-read cache block size, in KiB")
        p.add_argument("--reuse-cmipf-2km-from", metavar="ARCHIVE_ROOT",
                       help="Opt in to a separately labeled 2 km hybrid archive: reuse exact-time C07/C08/C09/C10/C13 pixels from verified local CMIPF months; fetch missing bands from NOAA MCMIPF")
        p.add_argument("--scans-per-hour", type=int, default=0, metavar="N", help="Scans to keep per UTC hour (1-6); 0 keeps every available scan")
        p.add_argument("--inventory-only", action="store_true",
                       help="With estimate, list metadata without bounded crop samples")
    return p


def main(source, argv=None):
    p = parser(source)
    args = p.parse_args(argv)
    shared_config = None
    if source == 'goes':
        from .goes_shared import config_from_args
        try:
            config = config_from_args(args)
            args.monthly_writers = config.month_writers
            if args.pipeline == 'shared' and (args.product or 'ABI-L2-CMIPF') == 'ABI-L2-CMIPF' and not str(args.destination).startswith('hf'):
                if args.read_profiles:p.error('Use schema-2 global limits; per-band resource profiles require --pipeline legacy')
                shared_config = config
        except ValueError as exc:p.error(str(exc))
    if source == 'goes' and min(args.download_concurrency,args.staging_mib,args.prefetch_mib,args.read_processes)<1:
        p.error('GOES reader, download and staging bounds must be positive')
    if source == "goes" and args.reuse_cmipf_2km_from and args.product not in (None, "ABI-L2-MCMIPF"):
        p.error("--reuse-cmipf-2km-from requires MCMIPF source selection")
    if (args.workers < 1 or args.decode_workers < 1 or args.repeats < 1
            or args.monthly_writers < 1 or (args.max_files is not None and args.max_files < 1)):
        p.error("Workers, repeats, and max-files must be positive.")
    from . import benchmark, catalog, goes, mrms, storage, validation
    out = Path(args.output)
    try:
        if args.operation == "estimate":
            from .study_goes import run, START, END
            result = run(out, bbox=args.bbox, sample=not args.inventory_only,
                scratch=args.scratch, start=args.start or START, end=args.end or END)
            print(out / "summary.json", flush=True)
            return 0 if args.inventory_only or result["sample_complete"] else 2
        if args.selection:
            selections = [catalog.load_selection(args.selection)]
            if selections[0].source != source:
                raise ValueError("The saved selection belongs to the other notebook.")
        else:
            periods = list(BENCHMARK_PERIODS.values())
            start, end = periods[0 if args.period == "2022" else 1]
            request = dict(start=args.start or start, end=args.end or end, bbox=args.bbox)
            if source == "mrms":
                products = args.product or mrms.DEFAULT_PRODUCTS
                selections = [mrms.discover(**request, product=product,
                    tolerance_minutes=args.tolerance_minutes, time_match=args.time_match)
                    for product in products]
            else:
                scans = args.scans_per_hour or None
                selections = [goes.discover(**request, satellite=args.satellite, bands=args.bands,
                                             scans_per_hour=scans, product=args.product or "ABI-L2-CMIPF")]
        summaries = []
        failed = False
        for selection in selections:
            if source == "goes" and args.reuse_cmipf_2km_from and selection.product != "ABI-L2-MCMIPF":
                raise ValueError("CMIPF 2 km reuse requires an MCMIPF source selection")
            if args.max_files:
                assets = selection.assets[:args.max_files]
                ids = {a.id for a in assets}
                selection = replace(selection, assets=assets,
                    hourly_matches=tuple(m for m in selection.hourly_matches if m["asset_id"] in ids))
            from .runlog import compact
            print(compact(selection.summary()), flush=True)
            product_out = out / selection.product
            catalog.save_selection(selection, product_out, index_results=not args.skip_index)
            if args.operation == "inspect":
                if source == "goes":
                    print(goes.acquisition_coverage(selection).to_string(index=False))
                summaries.append(selection.summary())
                continue
            if args.operation == "validate":
                result = validation.validate(selection, args.workers, product_out/"validation.json",
                    compare_mentor=(not args.skip_mentor and selection.product == mrms.DEFAULT_PRODUCT),
                    save_figures=args.save_figures)
                print("Validation passed:", result["passed"], flush=True)
                failed |= not result["passed"]
                continue
            if args.operation == "benchmark":
                if source == "mrms":
                    if selection.product not in {mrms.DEFAULT_PRODUCT, "MultiSensor_QPE_01H_Pass1_00.00"}:
                        print(f"Skipping mentor comparison for {selection.product}: different variable or cadence.")
                        continue
                    _, records = benchmark.run_mrms(selection, report_dir=product_out, repeats=args.repeats,
                        workers=args.workers, read_processes=args.read_processes)
                    failed |= records.empty or not bool(records.matches_legacy.all())
                else:
                    benchmark.run_goes(selection, report_dir=product_out, repeats=args.repeats)
                continue
            from . import monthly
            report = monthly.fetch(selection, destination=args.destination, workers=args.workers,
                decode_workers=args.decode_workers, backend=args.backend, report_dir=product_out,
                scratch=args.scratch, read_processes=args.read_processes,
                monthly_writers=1 if args.destination == "hf" else args.monthly_writers,
                prefetch_mib=args.prefetch_mib if source == "goes" else 512,
                block_size=args.block_size_kib*1024 if source == "goes" else 1024*1024,
                index_results=not args.skip_index,
                read_mode=args.read_mode if source == 'goes' else 'range',
                download_concurrency=args.download_concurrency if source == 'goes' else 8,
                staging_mib=args.staging_mib if source == 'goes' else 4096,
                reuse_cmipf_root=args.reuse_cmipf_2km_from if source == "goes" else None,
                read_profiles=json.loads(Path(args.read_profiles).read_text()) if source == "goes" and args.read_profiles else None,
                shared_config=shared_config if selection.product=='ABI-L2-CMIPF' else None)
            failed |= bool(report.get("interrupted")) or any(
                r["status"] == "failed" for r in report["records"])
            if args.save_figures:
                from .visualization import show_or_save
                rows = [r for r in report["records"] if r["status"] in ("archived", "reused")]
                for index in args.plot_index:
                    if not 0 <= index < len(rows):
                        raise ValueError(f"Image index {index} is outside the {len(rows)} successful images.")
                    from .view_frames import open_observation
                    with open_observation(rows[index]) as ds:
                        print(show_or_save(ds, args.save_figures, display=False,
                            recipe=args.recipe, center_crop=args.center_crop))
        return 1 if failed else 0
    except Exception as exc:
        write_json(out/"failure.json", {"source": source, "operation": args.operation,
                                       "error": f"{type(exc).__name__}: {exc}"})
        print(f"{type(exc).__name__}: {exc}", flush=True)
        return 1
