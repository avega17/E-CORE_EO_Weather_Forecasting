# %% [markdown]
# # Explore NAM forecasts and GOES observations
# Pick an exact UTC hour, forecast field, and satellite band. The NAM panel uses
# the latest Puerto Rico nest run that has an exact-hour file available in the
# archive by ten minutes before the selected hour. It checks older runs if needed.
# The GOES panel uses a verified native observation from the local archives.
# The maps use the same geographic extent and separate physical units.
#
# Run with the processing environment. **Load pair** performs the on-demand reads.

# %%
from pathlib import Path
import sys

root = next(p for p in (Path.cwd(), *Path.cwd().parents) if (p / 'src/ecore_weather').is_dir())
sys.path.insert(0, str(root / 'src'))
from IPython.display import display
from ecore_weather.ui.forecast_ui import create_nam_viewer

display(create_nam_viewer())
