# %% [markdown]
# # Dataset report: quality, storage and performance
# Inspect completed raw archives without changing them. Start with a quarter
# sample, then request a resumable full audit. Missing files and missing pixels
# mean different things. Network benchmarks should run with other fetches paused.
# Future 1 km interpolation and cloud-top parallax correction are planning only.

# %%
if __name__ != '__mp_main__':
    from IPython import get_ipython
    if __name__ == '__main__' and get_ipython() is None:
        import sys
        from pathlib import Path
        sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
        from dataset_report import main
        raise SystemExit(main())

# %% [markdown]
# ## Setup and dataset selection
# Use the ecore-weather kernel locally. In Colab, clone the requested revision
# and install the notebook dependencies. HF annual ZIPs are backup containers:
# restore a monthly member into a local cache before running pixel diagnostics.

# %%
if __name__ != '__mp_main__':
    import os, subprocess, sys
    from pathlib import Path
    IN_COLAB='google.colab' in sys.modules or bool(os.getenv('COLAB_RELEASE_TAG'))
    if IN_COLAB:
        root=Path('/content/E-CORE_EO_Radar_GFMs')
        if not root.exists(): subprocess.run(['git','clone','https://github.com/avega17/E-CORE_EO_Radar_GFMs.git',str(root)],check=True)
        revision=os.getenv('ECORE_REVISION','main')
        subprocess.run(['git','fetch','origin',revision],cwd=root,check=True)
        subprocess.run(['git','checkout','--detach','FETCH_HEAD'],cwd=root,check=True)
        os.chdir(root)
        subprocess.run([sys.executable,'-m','pip','install','-q','.[notebooks]'],check=True)
    else:
        root=next(p for p in (Path.cwd(),*Path.cwd().parents) if (p/'src/ecore_weather').is_dir())
    sys.path.insert(0,str(root/'src'))
    import ipywidgets as w
    import pandas as pd
    from IPython.display import display,Markdown
    from ecore_weather import dataset_report as report,report_benchmarks as bench
    from ecore_weather.common import PATCHES
    print('Repository commit:',subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip())

# %%
if __name__ != '__mp_main__':
    from ecore_weather.report_ui import create
    display(create())

# %% [markdown]
# ## Expected outputs
# The report folder holds a separate DuckDB database, inventory, compact
# summaries, a bounded diagnostic preview and benchmark evidence. Full audits
# resume verified observation rows; code or archive changes create a new audit.
# Quartiles describe individual observations and patches, not pooled pixels.
# Archive/source size ratios compare different representations; network bytes
# and compressed NOAA object sizes are reported separately. Use notebook 03
# to inspect individual images and animate completed local/restored archives.
