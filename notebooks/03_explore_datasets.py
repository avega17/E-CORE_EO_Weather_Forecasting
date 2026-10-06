# %% [markdown]
# # Explore radar and satellite imagery over time
# Choose **MRMS** or **GOES**, a local archive or the configured Hugging Face
# bucket, and UTC start/end dates and times. The ending instant is excluded.
# **Find observations** reads Earth2Studio monthly archive markers (or the local
# DuckDB index) and filters them by source and time. Then choose one
# product/region from the short dataset list. Observation times use a slider
# rather than a long dropdown. The **Month** list helps choose a stored period.
#
# Three tabs show a single image, every available observation within one day,
# or 1–24 selected images per day across the period. The default portable map
# uses a Zoom slider and scrolling to pan; Leaflet is optional when its frontend
# extension loads. Animations prepare images once, then use Play or the frame
# slider. Missing observations are not filled or interpolated in time.
#
# For display only, packed values are decoded, quality masks can be applied,
# and pixels are reprojected onto a small Web Mercator grid using nearest-neighbor
# sampling. Saved raw Zarr values and coordinates remain unchanged. Each animation
# is limited to 300 frames; shorten the period if necessary. Colors stay fixed
# throughout a sequence. The portable geographic background uses Natural Earth
# boundaries, which may be downloaded to a local cache on first use.

# %%
if __name__ != "__mp_main__":
    from IPython import get_ipython
    if __name__ == "__main__" and get_ipython() is None:
        from ecore_weather.viewer import main
        raise SystemExit(main())

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
    # This is a src-layout repository. Put this checkout ahead of any older
    # editable or site-installed copy so a fresh kernel loads the code being
    # viewed. Restart the kernel after changing src/ modules.
    source_path = str((root / "src").resolve())
    if source_path in sys.path:
        sys.path.remove(source_path)
    sys.path.insert(0, source_path)
    print("Repository:", root)
    print("Commit:", subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip())

# %% [markdown]
# ## Choose data, then a view
# Local example: `/mnt/p/ecore_eo_datasets`. You can enter a product folder or
# an individual monthly `raw.zarr.zip` path. The viewer opens the project's
# Earth2Studio-backed monthly Zarr arrays and reads the selected observation.
# An existing hf-mount directory also works through **Local**.
#
# Selecting **Hugging Face** fills in the configured bucket path. The yearly
# MRMS ZIP packages there are backups, not directly viewable Zarr stores. Open
# **HF yearly backup**, list the verified packages, choose a product and year,
# inspect its months, and click **Prepare month**. The viewer transfers and
# verifies only that monthly member into the local cache you choose, then finds
# its observations. The full yearly package is not downloaded. Direct remote
# monthly Zarr stores, if present, can still be searched. Credentials stay in
# the environment; no mount or tile server is required.
#
# For a local archive, the **Month** list quickly shows which months have stored
# observations for the selected source before you search (computed once, only for
# that source), so you can avoid a period with none. Picking a month sets Start
# and End to that month; editing a date clears the selection, so choose a manual
# range to span more than one month. Click **Find**; a small bar and the status
# line show the search is running, then report how many observations, days, and
# datasets matched and how long it took. One period that happens to be split
# across subset folders appears as a single dataset entry.
# Local **Find** opens the DuckDB index read-only and refreshes it each click,
# so a newly completed month appears without restarting the notebook. If the
# fetch coordinator has the database open for writing at that instant, Find
# reads completed archive manifests instead. An in-progress scratch batch is
# not yet a viewable archive; use the study-job progress query for its count.
# The satellite **ABI band** dropdown stays visible but disabled for MRMS. Only
# stored GOES bands are shown, and switching bands re-reads the observation.
# **Hide zero values** makes valid zero rain transparent in the display.
# Preparing an animation shows its own progress bar.
#
# After you **Show map** or prepare a **Day**/**Multi-day** animation, an export
# row appears below the tabs. Enter a path and click **Export**: the single image
# saves a PNG, and animations save a self-contained `.html` page, a `.gif`, or an
# `.mp4` (the `.mp4` option needs an ffmpeg binary). Colors keep the same fixed
# scale as the on-screen view.
#
# Open **Storage explorer** to compare completed archive sizes with the listed
# NOAA source-object bytes. Its nested sections show a summary, archive details,
# and a size comparison; choose a product, year, and month to narrow the view.
# The percentage is compressed Zarr bytes divided by the original listed NOAA
# object bytes for archives with known source sizes. NOAA objects may already be
# compressed, and these full-object sizes are not the size of the smaller ROI crop.
# If Find reports observations but a view still says to find observations,
# restart the kernel and run the setup cell again. The notebook puts this
# checkout's `src/` directory first on Python's import path; an already running
# kernel can still hold older imported modules in memory.

# %%
if __name__ != "__mp_main__":
    from ecore_weather import viewer
    print("Viewer module:", viewer.__file__)
    panel = viewer.controls()

# %% [markdown]
# ## Map display in VS Code
# The default portable map uses ordinary notebook widgets and embedded images,
# so it does not depend on the `jupyter-leaflet` JavaScript module. Use Zoom and
# scroll in the map to pan; Play and the frame slider work in both animation
# tabs. Choose **Leaflet map (JupyterLab)** only when that frontend loads its
# extension. A missing `LeafletMapModel` is a notebook frontend error, not a
# failure to read the saved Zarr archive.
#
# ## Run from a terminal
# `python notebooks/03_explore_datasets.py /path/to/raw.zarr.zip --hide-zero --output figures/rain.png`
#
# The [storage guide](../docs/storage_and_data_management.md) explains paths and remote access.
# The [Leafmap Zarr example](https://leafmap.org/notebooks/111_zarr/),
# [Cloud Native Geospatial guide](https://guide.cloudnativegeo.org/zarr/zarr-in-practice.html),
# and [Copernicus xarray example](https://help.marine.copernicus.eu/en/articles/8077952-how-to-open-and-visualize-zarr-format-data)
# describe larger-scale visualization options. A tile service is later work if
# our small subset viewer becomes insufficient.

# %% [markdown]
#
