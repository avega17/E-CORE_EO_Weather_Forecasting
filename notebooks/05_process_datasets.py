# %% [markdown]
# # Process native NOAA observations into regional datasets
# Start with completed monthly archives. This notebook builds time indices and
# reusable geometry, then creates physical-unit arrays on a declared 1, 2 or 3 km
# grid. Native archives stay unchanged. The same operations are available through
# `python scripts/dataset_jobs.py process --help` and this file's script interface.

# %%
if __name__ != '__mp_main__':
    from IPython import get_ipython
    from pathlib import Path
    import sys
    if __name__ == '__main__' and get_ipython() is None:
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
        from ecore_weather.processing.process_cli import main
        raise SystemExit(main())

# %% [markdown]
# ## Environment and storage
# Use the separate processing Conda environment described in
# [the processing guide](../docs/dataset_processing.md). GPU dependencies belong
# in the model environment. On the workstation both preparation artifacts and
# materialized variants use separate directories on P:; Linux SSD use is bounded.
# On Polaris keep reusable preparation on Eagle and variants on node scratch.
# Materialization uses independent processes. The Processes control defaults to
# 0, meaning all CPUs allowed by this machine or job. The workstation study
# configuration reserves 64 GiB for workers and prefetched output; one writer
# verifies and commits batches while later batches are being transformed.

# %%
if __name__ != '__mp_main__':
    import os, subprocess
    IN_COLAB='google.colab' in sys.modules or bool(os.getenv('COLAB_RELEASE_TAG'))
    if IN_COLAB:
        root=Path('/content/E-CORE_EO_Weather_Forecasting')
        if not root.exists():
            subprocess.run(['git','clone','https://github.com/avega17/E-CORE_EO_Weather_Forecasting.git',str(root)],check=True)
        os.chdir(root)
        subprocess.run([sys.executable,'-m','pip','install','-q','.[processing]'],check=True)
    else:
        root=next(p for p in (Path.cwd(),*Path.cwd().parents) if (p/'src/ecore_weather').is_dir())
    os.chdir(root)
    sys.path.insert(0,str(root/'src'))
    from IPython.display import display
    from ecore_weather.ui.process_ui import create

# %% [markdown]
# ## Choose a sample and inspect its preparation
# Indexing lists completed observations and identifies incomplete histories.
# Six ten-minute observations span 50 minutes between the first and last times.
# Preparation indexes point to unchanged native ZIPs. Materialization writes
# calibrated arrays to data.zarr; estimates read a small sample.

# %%
if __name__ != '__mp_main__':
    display(create())

# %% [markdown]
# ## What materialization produces
# Each variant contains physical values, validity, source support, categorical
# quality/missingness, actual source times and the common projected coordinates.
# A complete plain-Zarr store supports arbitrary eligible windows across months. Auto mode
# chooses it when the complete selection fits with a 5% filesystem reserve;
# otherwise temporal blocks provide a bounded working set. Both modes can resume.
# Finer output grids do not create extra measured detail in infrared bands.
#
# ## Correction and model experiments
# The parallax pilot uses separately matched cloud heights for C01/C02/C03/C07/C13 and retains
# uncertain-height boundaries. It does not silently correct water-vapor bands.
# The experimental Caribbean model adapter is an engineering forward-pass check,
# not a claim of regional forecast skill. Earth2Studio supplies solar-zenith
# conditioning internally. See the guide for explicit opt-in CLI commands and
# the additional requirements for a future coupled radar/GLM model.
