# %% [markdown]
# # GOES: raw subsets, quality flags, and virtual references
# Start with full-disk StormScope bands C01, C02, C03, C07, C08, C09, C10, and C13.
# The default CMIPF source keeps each band on its native grid: C02 at nominal
# 0.5 km, C01/C03 at 1 km, and the selected infrared bands at 2 km. Choose a UTC
# period and satellite below; GOES-East uses GOES-16 before the April 2025
# operational handoff and GOES-19 afterwards. Raw storage keeps packed integers, coordinates, calibration, and
# quality flags. Decoding and interpolation happen only in memory.

# %%
if __name__ != "__mp_main__":  # Spawned readers must not construct notebook widgets.
    SOURCE = "goes"
# %%
if __name__ != "__mp_main__":  # Spawned readers must not construct notebook widgets.
    # The notebook and command-line entry use the same package functions.
    from IPython import get_ipython
    if __name__ == "__main__" and get_ipython() is None:
        import os
        import sys
        from pathlib import Path
        root = Path(__file__).resolve().parents[1]
        sys.path.insert(0, str(root / "src"))
        os.environ["ECORE_REPO_ROOT"] = str(root)
        os.environ["PYTHONPATH"] = str(root / "src") + (os.pathsep + os.environ["PYTHONPATH"] if os.environ.get("PYTHONPATH") else "")
        from ecore_weather.cli import main
        raise SystemExit(main(SOURCE))

# %% [markdown]
# ## Set up
# Locally, select the Conda kernel from `environment.yml`. Colab detects its
# runtime, clones the repository, and installs dependencies. Set `REVISION` to
# the pushed code you want to test. Never put a storage token in a saved cell.

# %%
if __name__ != "__mp_main__":  # Spawned readers must not construct notebook widgets.
    import os
    import subprocess
    import sys
    from pathlib import Path
    REPOSITORY = "https://github.com/avega17/E-CORE_EO_Radar_GFMs.git"
    REVISION = os.getenv("ECORE_REVISION", "main")
    IN_COLAB = "google.colab" in sys.modules or bool(os.getenv("COLAB_RELEASE_TAG"))
    if IN_COLAB:
        from google.colab import output
        output.enable_custom_widget_manager()
        root = Path("/content/E-CORE_EO_Radar_GFMs")
        if not root.exists():
            subprocess.run(["git", "clone", REPOSITORY, str(root)], check=True)
        subprocess.run(["git", "fetch", "origin", REVISION], cwd=root, check=True)
        subprocess.run(["git", "checkout", "--detach", "FETCH_HEAD"], cwd=root, check=True)
        os.chdir(root)
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", ".[notebooks]"], check=True)
    else:
        root = next((p for p in (Path.cwd(), *Path.cwd().parents)
                     if (p / "src/ecore_weather").exists()), Path.cwd())
        os.chdir(root)
    print("Repository:", root)
    print("Commit:", subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip())

# %%
if __name__ != "__mp_main__":  # Spawned readers must not construct notebook widgets.
    from dataclasses import replace
    from collections import Counter
    import pandas as pd
    import matplotlib.pyplot as plt
    import ipywidgets as widgets
    from IPython.display import display
    if get_ipython() is not None:
        get_ipython().run_line_magic("matplotlib", "inline")
    from ecore_weather import benchmark, catalog, diagnostics, storage, ui, validation, visualization, view_frames
    from ecore_weather.common import PATCHES
    SMOKE = os.getenv("ECORE_NOTEBOOK_SMOKE") == "1"

# %%
if __name__ != "__mp_main__":  # Spawned readers must not construct notebook widgets.
    from ecore_weather import goes
    READER = goes
# %% [markdown]
# ## Choose files and save the selection
# Dates, region, products, storage, and reader limits come from these controls.
# End dates are excluded. Creating controls does not start network work.
#
# Native-band monthly ZIPs are written to a local destination; the workstation default is
# the mounted P: drive. Choose a different local directory when needed.
# STAC describes the full selection in a small pair of JSON files. Archived
# samples are grouped by satellite, band, native ROI/grid and month.
# Different-resolution bands remain separate datasets.
#
# The product list offers full-disk imagery only. The CONUS sector (roughly
# 20°N–50°N, 125°W–65°W) does not cover Puerto Rico, and this project works in
# the Caribbean, so the CONUS products would fetch the wrong region here; they
# remain available from the command line for advanced use. A collapsed "Product
# and variable guide" below the controls describes both full-disk products and
# all sixteen ABI bands.
#
# **Scan frequency** keeps every available scan by default (ordinarily about
# one every ten minutes). Choose fewer scans only to shorten an exploratory run.
# For a long metadata-only size and time estimate, use this notebook as a script
# with `--operation estimate`; it inventories available scans and reads only a
# bounded set of native-grid crop samples.

# %%
if __name__ != "__mp_main__":  # Spawned readers must not construct notebook widgets.
    controls = ui.selection_controls(SOURCE)
    display(controls["panel"])
    def choose_data():
        selection = READER.discover(**ui.read_controls(controls))
        display(selection.summary())
        if selection.source == "goes":
            display(READER.acquisition_coverage(selection))
        print("Saved selection:", catalog.save_selection(selection, controls["output"].value))
        return selection
    chosen = ui.action("Find and save selection", choose_data)

# %% [markdown]
# ## Fetch raw subsets
# Source values, coordinates, and missing-value information stay unchanged.
# Lossless Zarr storage replaces temporary source containers. Completed subsets
# are verified and reused; failures are reported without changing destinations.
# Compact run reports track band-month counts, bytes and timings. Source IDs
# and metadata remain in the selection and archive. HF backup transfer time
# is measured separately from source reads and local archive writing.
#
# Each CMIPF object is cropped before loading its packed image and quality flags.
# One process owns each active month's separate band stores. Global download,
# local-reader and C02 range-reader pools serve every active month. Verified
# eight-scan scratch batches survive interruption.
# MCMIPF and hybrid remain separate historical products; their pixels are not
# reused in a CMIPF study. Future 1 km interpolation and cloud-top parallax
# correction belong to a separate training-preparation sprint.
# Shared CMIPF fetching downloads seven bands asynchronously and range-reads C02.
# Month writers own separate native-band stores. A C02-only tail can free
# normal month slots; global reader and byte limits do not multiply per band.
# Shared monthly scheduling is the selected default: two normal month owners,
# one extra C02 tail, 32 global downloads, two local and eight range readers.
# jobs/goes_workstation.json records the measured configuration and limitations.
# Change these controls for another machine; more workers are not always faster.
# Range-only experiments read requested file sections. The optional
# virtual bundle points back to NOAA files and remains dependent on their access.

# %%
if __name__ != "__mp_main__":  # Spawned readers must not construct notebook widgets.
    def fetch_data():
        from ecore_weather import monthly
        with ui.FetchProgress(len(chosen["value"].assets), "GOES") as progress:
            report = monthly.fetch(chosen["value"], destination=controls["destination"].value,
                workers=controls["workers"].value, report_dir=controls["output"].value,
                read_processes=controls["read_processes"].value,
                monthly_writers=controls["monthly_writers"].value,
                prefetch_mib=controls["prefetch_mib"].value,
                read_mode='range' if controls['read_mode'].value=='shared' else controls['read_mode'].value,
                read_profiles=__import__('json').loads(Path(controls['read_profiles'].value).read_text()) if controls['read_profiles'].value else None,
                download_concurrency=controls['download_concurrency'].value,
                staging_mib=controls['staging_mib'].value,
                shared_config=(__import__('ecore_weather.goes_shared',fromlist=['SharedConfig']).SharedConfig(
                    month_writers=controls['monthly_writers'].value,tail_months=controls['tail_months'].value,
                    download_concurrency=controls['download_concurrency'].value,local_readers=controls['local_readers'].value,
                    range_readers=controls['range_readers'].value,roi_budget_mib=controls['prefetch_mib'].value,
                    staging_mib=controls['staging_mib'].value,block_size=controls['block_size_kib'].value*1024)
                    if controls['read_mode'].value=='shared' and chosen['value'].product=='ABI-L2-CMIPF' and not str(controls['destination'].value).startswith('hf') else None),
                block_size=controls["block_size_kib"].value*1024,
                reuse_cmipf_root=(controls["reuse_cmipf_root"].value
                    if controls["reuse_cmipf"].value else None),
                backend="obstore",
                scratch=controls["scratch"].value or None, progress=progress)
        successful = [r for r in report["records"] if r["status"] in ("archived", "reused")]
        controls["image_index"].max = max(0, len(successful)-1)
        display(dict(Counter(r["status"] for r in report["records"])))
        display(pd.DataFrame([r for r in report["records"] if r["status"] == "failed"]))
        return report
    fetched = ui.action("Fetch raw subsets", fetch_data)

# %% [markdown]
# ## View a fetched image
# Range reads select bands and a native rectangular window before loading.
# The satellite grid is curved relative to longitude/latitude, so that window
# encloses the requested region. Compressed chunks may contain additional pixels.
# Packed pixel values are not temperatures until scale and offset are applied.
# The quality view keeps DQF=0 pixels. Raw quality flags remain available.
# Use the checkbox to save the displayed figures as PNGs; it is off by default.

# %%
if __name__ != "__mp_main__":  # Spawned readers must not construct notebook widgets.
    display(controls["view_panel"])
    recipe = widgets.Dropdown(options=[("Decoded values", "decode"), ("Quality mask", "quality"),
        ("Puerto Rico grid", "reproject"), ("Raw only", None)], value="quality", description="View")
    display(recipe)
    def show_image():
        rows = [r for r in fetched["value"]["records"] if r["status"] in ("archived", "reused")]
        row = rows[controls["image_index"].value]
        with view_frames.open_observation(row) as raw:
            before = storage.fingerprint(raw)
            variable = f"CMI_C{int(row['band']):02d}" if row.get("band") else "CMI"
            print("Source observation:", row["time"], "band:", row.get("band"))
            display(diagnostics.describe(raw))
            files = visualization.show_or_save(raw, controls["figure_dir"].value if controls["save_figures"].value else None,
                                               recipe=recipe.value, variable=variable)
            assert storage.fingerprint(raw) == before
            if files:
                print("Saved figures:", files)
        return files
    image_view = ui.action("Display selected image", show_image)

# %% [markdown]
# ## Try a virtual dataset
# VirtualiZarr records where array pieces live in NOAA files. Kerchunk references
# are bundled in one local JSON file; they are pointers rather than another image
# copy and still require NOAA access. Different grids, calibration, or compression
# stay in separate groups. Start with six scans across the chosen period:
# midnight and noon on the first, middle, and final included days.

# %%
if __name__ != "__mp_main__":  # Spawned readers must not construct notebook widgets.
    def make_references():
        selection = chosen["value"]
        path = goes.build_virtual(selection, Path(controls["output"].value)/"references",
                                   assets=goes.benchmark_assets(selection))
        print("Reference bundle:", path)
        with goes.open_virtual(path) as virtual:
            display(virtual)
        return path
    references = ui.action("Build sample references", make_references)

# %% [markdown]
# ## Validate representative scans or measure reading methods
# Discovery covers the full period. Validation uses the six actual scans above
# and compares full-file, range, and virtual reads, plus raw-Zarr round trips.
# It keeps one compact result and removes temporary successful-test artifacts.
# A normal speed experiment retains its small reports and reference bundle.
# Reference-building time is separate from reopening and reading. The full-file
# baseline temporarily downloads several gigabytes. Timing is sensitive to caches
# and network conditions; value equality and returned bytes are checked separately.

# %%
if __name__ != "__mp_main__":  # Spawned readers must not construct notebook widgets.
    def validate_data():
        return validation.validate(chosen["value"], workers=controls["workers"].value,
                                   report_path=Path(controls["output"].value)/"validation.json")
    checks = ui.action("Validate representative scans", validate_data)
    def compare_data(repeats=1):
        table = benchmark.run_goes(chosen["value"], report_dir=controls["output"].value, repeats=repeats)
        display(table)
        return table
    comparison = ui.action("Compare reading methods", compare_data)

# %%
if __name__ != "__mp_main__":  # Spawned readers must not construct notebook widgets.
    if SMOKE:
        import tempfile
        selection = goes.discover("2025-09-01", "2025-09-01T01:00:00")
        chosen["value"] = replace(selection, assets=selection.assets[:1])
        with tempfile.TemporaryDirectory(prefix="ecore-notebook-") as local:
            controls["destination"].value = local+"/data"
            controls["output"].value = local+"/run"
            controls["figure_dir"].value = local+"/figures"
            controls["save_figures"].value = True
            controls["workers"].value = 1
            catalog.save_selection(chosen["value"], controls["output"].value)
            fetched["value"] = fetch_data()
            assert all(r["status"] in ("archived", "reused") for r in fetched["value"]["records"])
            assert show_image()

# %% [markdown]
# [Notebook guide](../docs/notebooks.md) · [Developer guide](../docs/developer_guide.md)
# · [Validation results](../docs/storage_and_data_management.md)
