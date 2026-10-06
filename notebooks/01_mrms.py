# %% [markdown]
# # MRMS: raw radar observations and Caribbean coverage
# Choose a UTC period and native-grid Caribbean region below. The four default
# fields are precipitation rate, composite reflectivity, low-level azimuthal
# shear, and multisensor Pass2 one-hour QPE. Other MRMS products remain selectable.
# Six are sampled on ten-minute slots; the multisensor products remain hourly.
#
# Two-minute products use the latest available observation at or before each
# ten-minute slot, within five minutes. Hourly products use hourly slots. Actual
# source times and offsets stay alongside the request slots; gaps are not filled.
#
# As a script, `--product` accepts readable names such as `precipitation-rate`,
# `composite-reflectivity`, `low-level-azimuthal-shear`, and
# `multisensor-qpe-pass2`. The archive metadata still records NOAA's exact key.

# %%
if __name__ != "__mp_main__":  # Spawned readers must not construct notebook widgets.
    SOURCE = "mrms"
# %%
if __name__ != "__mp_main__":  # Spawned readers must not construct notebook widgets.
    # The notebook and command-line entry use the same package functions.
    from IPython import get_ipython
    if __name__ == "__main__" and get_ipython() is None:
        import os
        from pathlib import Path
        os.environ.setdefault("ECORE_REPO_ROOT", str(Path(__file__).resolve().parents[1]))
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
    from ecore_weather import mrms
    READER = mrms
# %% [markdown]
# ## Choose files and save the selection
# Dates, region, products, storage, and worker count come from these controls.
# End dates are excluded. Workers default to half the detected CPUs; decoding
# is bounded separately. Creating controls does not start network work.
#
# The default durable destination is the HF bucket configured in `.env` or session
# environment variables. Enter a local path to choose local storage explicitly.
# STAC describes each product selection. Monthly compressed Zarr archives use
# stable product, region/grid, and month paths, so overlapping requests reuse
# the same archive.
# A collapsed "Product and variable guide" explains the available CARIB fields,
# units, cadence, and missing-value codes. Azimuthal shear is a radar rotation
# proxy; a zero in those products is ambiguous without bitmap/coverage context.

# %%
if __name__ != "__mp_main__":  # Spawned readers must not construct notebook widgets.
    controls = ui.selection_controls(SOURCE)
    display(controls["panel"])
    def choose_data():
        request = ui.read_controls(controls)
        products = request.pop("products")
        selections = READER.discover_defaults(**request, products=products)
        for selection in selections:
            display(selection.summary())
            product_dir = Path(controls["output"].value) / selection.product
            print("Saved selection:", catalog.save_selection(selection, product_dir))
        return selections
    chosen = ui.action("Find and save selection", choose_data)

# %% [markdown]
# ## Fetch raw subsets
# Source values, coordinates, and missing-value information stay unchanged.
# Lossless Zarr storage replaces temporary source containers. Completed subsets
# are verified and reused; failures are reported without changing destinations.
# The run report keeps per-file outcomes and timings. Publishing includes the
# remote read-back check and is measured separately from source reads and writing.
#
# Why Zarr: the mentor's scripts re-downloaded and re-decoded each hour on every
# run, and their GeoTIFF output lost units and missing-value distinctions. MRMS
# gzip files must still be downloaded whole. Local fetching now streams bounded
# source batches to separate monthly Earth2Studio Zarr stores; each month is
# verified before its completion marker is written. HF publication has one
# writer to keep request rates bounded.

# %%
if __name__ != "__mp_main__":  # Spawned readers must not construct notebook widgets.
    def fetch_data():
        from ecore_weather import monthly
        reports = []
        for selection in chosen["value"]:
            product_dir = Path(controls["output"].value) / selection.product
            with ui.FetchProgress(len(selection.assets), selection.product) as progress:
                report = monthly.fetch(selection, destination=controls["destination"].value,
                    workers=controls["workers"].value, report_dir=product_dir,
                    read_processes=controls["read_processes"].value,
                    monthly_writers=controls["monthly_writers"].value,
                    scratch=controls["scratch"].value or None, progress=progress)
            reports.append(report)
        report = {"records": [r for result in reports for r in result["records"]], "reports": reports}
        successful = [r for r in report["records"] if r["status"] in ("archived", "reused")]
        controls["image_index"].max = max(0, len(successful)-1)
        display(dict(Counter(r["status"] for r in report["records"])))
        display(pd.DataFrame([r for r in report["records"] if r["status"] == "failed"]))
        return report
    fetched = ui.action("Fetch raw subsets", fetch_data)

# %% [markdown]
# ## Describe patches before processing
# QPE uses -1 for missing data and -3 for no coverage. Zero rain is valid.
# Azimuthal-shear zero is ambiguous and is reported separately. Other products
# have their own definitions; negative reflectivity can be valid.
# The GRIB bitmap separately marks whether a source cell contains a measurement.
# Statistics use valid pixels only. Missing hourly slots are reported by the
# selection, separately from missing pixels. Empty patches have no statistics.

# %%
if __name__ != "__mp_main__":  # Spawned readers must not construct notebook widgets.
    patches = widgets.SelectMultiple(options=list(PATCHES), value=tuple(PATCHES), description="Patches")
    display(patches)
    def inspect_patches():
        rows = fetched["value"]["records"]
        tables = []
        for row in rows:
            if "diagnostics" in row:
                tables.extend(row["diagnostics"])
            elif row["status"] in ("archived", "reused"):
                with view_frames.open_observation(row) as raw:
                    described = diagnostics.describe(raw).to_dict("records")
                    for record in described:
                        record["slot_time"] = row.get("slot_time", row["time"])
                    tables.extend(described)
        table = pd.DataFrame(tables)
        if not table.empty:
            table = table[table.patch.isin(patches.value)]
            display(table)
            slots = sorted({slot for selection in chosen["value"] for slot in selection.expected_times})
            figure = diagnostics.plot_coverage(table, slots)
            display(figure)
            if controls["save_figures"].value:
                folder = Path(controls["figure_dir"].value); folder.mkdir(parents=True, exist_ok=True)
                figure.savefig(folder / "mrms-coverage.png", dpi=150)
            plt.close(figure)
        return table
    patch_report = ui.action("Describe selected patches", inspect_patches)

# %% [markdown]
# ## View an image and compare processing
# Select a fetched image index. Raw maps display west longitudes as negative;
# storage retains NOAA's native 0–360 coordinates. Processing stays in memory.
# A separate rainfall map shows cached coastlines and land/ocean shading. It hides
# valid zeros only in this view so sparse rain stands out against the islands.
# The mentor interpolates and then removes negatives. The alternative masks
# documented missing values first, which changes results near coverage gaps.
# The exact comparison recipe restores the original grid in memory. Saved raw
# crops do not contain neighbouring pixels outside their edges; the automated
# mentor comparison reads a one-pixel margin for a fair comparison.
# A centered 512 × 512 crop is a view. Figure saving writes PNGs, not derived rasters.

# %%
if __name__ != "__mp_main__":  # Spawned readers must not construct notebook widgets.
    display(controls["view_panel"])
    recipe = widgets.Dropdown(options=[("Compare processing", "compare"), ("Raw only", None),
        ("Mentor's processing", "legacy_exact"), ("Mask missing values first", "quality_aware")], description="View")
    center = widgets.Checkbox(value=False, description="Centered 512 × 512")
    display(widgets.HBox([recipe, center]))
    def show_image():
        rows = [r for r in fetched["value"]["records"] if r["status"] in ("archived", "reused")]
        row = rows[controls["image_index"].value]
        with view_frames.open_observation(row) as raw:
            before = storage.fingerprint(raw)
            print("Source observation:", row["time"], "Hourly slot:", row.get("slot_time"))
            files = visualization.show_or_save(raw, controls["figure_dir"].value if controls["save_figures"].value else None,
                                               recipe=recipe.value, center_crop=center.value)
            assert storage.fingerprint(raw) == before
            if files:
                print("Saved figures:", files)
        return files
    image_view = ui.action("Display selected image", show_image)

# %% [markdown]
# ## Validate or measure the selected period
# Validation checks every selected raw subset and compares the mentor's unchanged
# calculations with the revised reader in isolated batches. Temporary data,
# catalogs, references, and detailed test reports are deleted after the check.
# Only a compact result remains. This parallel correctness check is separate from
# a speed benchmark. The speed button runs each method over the selected files;
# repeat promising comparisons three times before claiming an improvement.
# Use `benchmark.validate_mentor_week(selection)` for the original strict clock-hour
# downloader check. The matched comparison supplies its source selection while
# leaving the mentor's calculations unchanged.

# %%
if __name__ != "__mp_main__":  # Spawned readers must not construct notebook widgets.
    def validate_data():
        results = []
        for selection in chosen["value"]:
            if selection.product != mrms.DEFAULT_PRODUCT:
                continue
            results.append(validation.validate(selection, workers=controls["workers"].value,
                report_path=Path(controls["output"].value)/selection.product/"validation.json"))
        return results
    checks = ui.action("Validate selected period", validate_data)
    def compare_data(repeats=1):
        selection = next(s for s in chosen["value"] if s.product == mrms.DEFAULT_PRODUCT)
        table, details = benchmark.run_mrms(selection, report_dir=controls["output"].value,
                                            repeats=repeats, workers=controls["workers"].value, read_processes=controls["read_processes"].value)
        display(table)
        display(details.groupby(["variant", "status", "matches_legacy"]).size())
        return table
    comparison = ui.action("Run speed comparison", compare_data)
    # For repetitions: compare_data(repeats=3)

# %%
if __name__ != "__mp_main__":  # Spawned readers must not construct notebook widgets.
    if SMOKE:
        import tempfile
        chosen["value"] = [mrms.discover("2022-09-24T17:00:00", "2022-09-24T18:00:00")]
        with tempfile.TemporaryDirectory(prefix="ecore-notebook-") as local:
            controls["destination"].value = local+"/data"
            controls["output"].value = local+"/run"
            controls["figure_dir"].value = local+"/figures"
            controls["save_figures"].value = True
            controls["workers"].value = 1
            catalog.save_selection(chosen["value"][0], controls["output"].value)
            fetched["value"] = fetch_data()
            assert all(r["status"] in ("archived", "reused") for r in fetched["value"]["records"])
            inspect_patches()
            assert show_image()

# %% [markdown]
# [Notebook guide](../docs/notebooks.md) · [Developer guide](../docs/developer_guide.md)
# · [Validation results](../docs/storage_and_data_management.md)
