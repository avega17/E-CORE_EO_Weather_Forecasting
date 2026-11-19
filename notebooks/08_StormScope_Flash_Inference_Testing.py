# %% [markdown]
# # StormScope Flash on Caribbean storm windows
#
# Choose a storm, an exact UTC hour and a GOES band. Each row is an independent
# ten-minute forecast using six preceding GOES frames. The center row at H uses
# H−60 through H−10. The optional NAM field is valid at H and is repeated in the
# other rows for context.
#
# This runs a CONUS checkpoint with an experimental Caribbean adapter and
# approximate elevation scaling. The comparison shows how the model executes
# on our inputs. Regional forecast skill requires a separate evaluation.
#
# In Colab select an L4 GPU. Run the setup cell to prepare dependencies, then run
# the viewer cell. If the new notebook code is still in a local checkout, upload
# its source-only bundle as `/content/ecore-demo-source.zip` using Colab's Files
# panel before setup. Local runs use the separate model environment.

# %%
import os
from pathlib import Path
import subprocess
import sys

colab_runtime = bool(os.environ.get('COLAB_RELEASE_TAG')) or 'google.colab' in sys.modules
if colab_runtime:
    root = Path('/content/E-CORE_EO_Weather_Forecasting')
    bundle = Path('/content/ecore-demo-source.zip')
    if bundle.is_file():
        from zipfile import ZipFile
        with ZipFile(bundle) as archive:
            members = [name for name in archive.namelist()
                       if name.startswith('src/ecore_weather/') and name.endswith('.py')
                       and '..' not in Path(name).parts]
            archive.extractall(root, members)
    elif not (root / 'src').is_dir():
        subprocess.run(['git', 'clone', 'https://github.com/avega17/E-CORE_EO_Weather_Forecasting.git', str(root)], check=True)
    if not (root / 'src/ecore_weather/ui/storm_ui.py').is_file():
        raise FileNotFoundError('Use a checkout with notebook 08 support or upload ecore-demo-source.zip to /content.')
else:
    root = next(p for p in (Path.cwd(), *Path.cwd().parents) if (p / 'src/ecore_weather').is_dir())
sys.path.insert(0, str(root / 'src'))
model_python = None
if colab_runtime:
    from ecore_weather.processing.storm_environment import colab_model_python
    model_python = colab_model_python()

# %%
from IPython.display import display
from ecore_weather.ui.storm_ui import create_storm_demo

display(create_storm_demo(repo_id='asvnpr/ecore-stormscope-demo', model_python=model_python))
