# %% [markdown]
# # Process native NOAA observations into regional datasets
# Choose verified NOAA observations and build a processed dataset on a 1, 2 or
# 3 km grid. Run all displays the controls. Review a selection before starting
# a build. The same operations are available through
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
# ## Setup
# Use the processing Conda environment on the workstation. Keep the derived
# store and preparation on the DAS. On Polaris set the output root to node
# scratch. See the [processing guide](../docs/dataset_processing.md).

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
    from ecore_weather.ui.process_ui import processing_sections
    status, choose, build, preview, validation = processing_sections()

# %% [markdown]
# ## Current status
# Refresh to see the managed store, verified slots, and latest cloud-height
# fetch record. You can preview frames that have already been verified.

# %%
if __name__ != '__mp_main__':
    display(status)

# %% [markdown]
# ## Choose a selection
# Dates are UTC and the ending date is excluded. Use 3 km for the Flash demo.
# Cloud-height correction applies to C01, C02, C03, C07, and C13 where matched
# height and source observations provide valid pixels.

# %%
if __name__ != '__mp_main__':
    display(choose)

# %% [markdown]
# ## Review and build
# Review builds a time index and measures a small sample for the storage
# estimate. **Build processed dataset** starts a background job that may run for
# hours. An identical selection resumes from its verified slots. A new selection
# requires confirmation to delete the named managed store first.

# %%
if __name__ != '__mp_main__':
    display(build)

# %% [markdown]
# ## Preview a frame
# Preview a committed processed frame or compare native GOES with and without
# cloud-height correction for a UTC time. These actions read only a sample.

# %%
if __name__ != '__mp_main__':
    display(preview)

# %% [markdown]
# ## Validate or export a report
# Validation rereads committed batches and can take time for a full study.
# Benchmark and model experiments use the documented CLI.

# %%
if __name__ != '__mp_main__':
    display(validation)
