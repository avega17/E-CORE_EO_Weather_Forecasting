"""Simple notebook controls; creating them never fetches or uploads data."""

from datetime import date

import ipywidgets as widgets

from .common import BENCHMARK_PERIODS, PR_BBOX, default_workers

_MRMS_ROWS = [
    ("MultiSensor_QPE_01H_Pass2_00.00", "Radar blended with gauges and model data; one-hour accumulation, second pass with fuller gauge quality control", "mm", "−1 / −3", "hourly, ~2 h latency"),
    ("MultiSensor_QPE_01H_Pass1_00.00", "The same blend, earlier first pass", "mm", "−1 / −3", "hourly, ~1 h latency"),
    ("RadarOnly_QPE_01H_00.00", "Radar-only one-hour accumulation", "mm", "−1 / −3", "hourly"),
    ("PrecipRate_00.00", "Instantaneous radar precipitation rate, not an hourly total", "mm h−1", "−1 / −3", "~2 minutes"),
    ("MergedReflectivityQCComposite_00.50", "Quality-controlled composite (column-maximum) reflectivity", "dBZ", "−99 / −999", "~2 minutes"),
    ("MergedBaseReflectivityQC_00.50", "Quality-controlled lowest-tilt (base) reflectivity", "dBZ", "−99 / −999", "~2 minutes"),
    ("MergedAzShear_0-2kmAGL_00.50", "Low-level azimuthal shear, a radar rotation proxy", "0.001 s−1", "0 / 0; check bitmap", "~2 minutes"),
    ("MergedAzShear_3-6kmAGL_00.50", "Mid-level azimuthal shear, a radar rotation proxy", "0.001 s−1", "0 / 0; check bitmap", "~2 minutes"),
]
_MRMS_NOTE = ("An hourly accumulation is millimetres over the whole hour, not an instantaneous rate. "
              "−1 marks missing data and −3 marks no radar coverage; zero is a valid rain-free measurement, "
              "and the GRIB bitmap separately marks cells without a measurement. "
              "Sources: NOAA operational MRMS GRIB2 tables (v12.2) and the Project Pythia MRMS cookbook.")

_GOES_PRODUCT_ROWS = [
    ("ABI-L2-MCMIPF", "Cloud and moisture imagery: all 16 ABI bands in one file", "full disk, ~10 minutes"),
    ("ABI-L2-CMIPF", "The same imagery, one band per file; pick specific bands", "full disk, ~10 minutes"),
]
_GOES_PRODUCT_NOTE = ("F means full disk: the whole Earth view, which includes the Caribbean and Puerto Rico. "
                      "The CONUS sector (C, roughly 20°N–50°N, 125°W–65°W) does not cover Puerto Rico, so these "
                      "controls offer full-disk products only; CONUS remains available from the command line. "
                      "Sources: GOES Data Explorer product table; NOAA GOES-R ABI bands quick guide.")
_GOES_BAND_ROWS = [
    ("C01", "0.47", "Blue (visible)", "1 km", "daytime aerosols, smoke, dust, clouds"),
    ("C02", "0.64", "Red (visible)", "0.5 km", "sharpest daytime view: clouds, fog, weather systems"),
    ("C03", "0.86", "Veggie (near-IR)", "1 km", "vegetation and burn scars, daytime"),
    ("C04", "1.37", "Cirrus (near-IR)", "1 km", "thin cirrus clouds, daytime"),
    ("C05", "1.61", "Snow/Ice (near-IR)", "1 km", "tells snow and ice from water clouds, daytime"),
    ("C06", "2.24", "Cloud particle size (near-IR)", "1 km", "cloud droplet growth, daytime"),
    ("C07", "3.9", "Shortwave window (IR)", "2 km", "low clouds and fog at night, fires"),
    ("C08", "6.2", "Upper-level water vapor (IR)", "2 km", "upper-atmosphere moisture, jet stream"),
    ("C09", "6.9", "Mid-level water vapor (IR)", "2 km", "middle-atmosphere moisture"),
    ("C10", "7.3", "Lower-level water vapor (IR)", "2 km", "lower-atmosphere moisture"),
    ("C11", "8.4", "Cloud-top phase (IR)", "2 km", "ice versus water cloud tops"),
    ("C12", "9.6", "Ozone (IR)", "2 km", "ozone, turbulence, winds aloft"),
    ("C13", "10.3", "Clean longwave window (IR)", "2 km", "cloud and surface temperature, rainfall estimates"),
    ("C14", "11.2", "Longwave window (IR)", "2 km", "sea-surface temperature, convection"),
    ("C15", "12.3", "Dirty longwave window (IR)", "2 km", "volcanic ash, low-level moisture"),
    ("C16", "13.3", "CO2 longwave (IR)", "2 km", "cloud-top height, tropopause"),
]
_GOES_BAND_NOTE = ("Band 2 has the finest detail (0.5 km). Packed pixel values become physical values only after "
                   "the file's scale and offset are applied; DQF = 0 marks good-quality pixels. "
                   "Water-vapor bands C08–C10 and infrared windows C13–C16 are common for storm analysis. "
                   "Source: NOAA GOES-R ABI bands quick guide.")


def _table(headers, rows, note):
    cell = 'style="border: 1px solid #ccc; padding: 3px 8px; text-align: left;"'
    head = "".join(f"<th {cell}>{name}</th>" for name in headers)
    body = "".join("<tr>" + "".join(f"<td {cell}>{value}</td>" for value in row) + "</tr>" for row in rows)
    return ('<table style="border-collapse: collapse; font-size: 13px;">'
            f"<tr>{head}</tr>{body}</table>"
            f'<p style="font-size: 13px; max-width: 900px;">{note}</p>')


def product_cheatsheet(source):
    """A collapsed guide to the available products; building it does no network work."""
    if source == "goes":
        html = ("<b>Imagery products</b>" + _table(("Product", "What it is", "Coverage and cadence"), _GOES_PRODUCT_ROWS, _GOES_PRODUCT_NOTE)
                + "<b>ABI bands</b>" + _table(("Band", "Wavelength (µm)", "Name", "Resolution", "Common uses"), _GOES_BAND_ROWS, _GOES_BAND_NOTE))
    else:
        html = _table(("Product", "What it is", "Units", "Missing / no coverage", "Cadence"), _MRMS_ROWS, _MRMS_NOTE)
    accordion = widgets.Accordion(children=[widgets.HTML(html)], selected_index=None)
    accordion.set_title(0, "Product and variable guide")
    return accordion


def selection_controls(source):
    from . import goes, mrms
    start, end = next(iter(BENCHMARK_PERIODS.values()))
    controls = {
        "period": widgets.Dropdown(options=list(BENCHMARK_PERIODS), description="Example period"),
        "start": widgets.DatePicker(value=date.fromisoformat(start), description="Start (UTC)"),
        "end": widgets.DatePicker(value=date.fromisoformat(end), description="End, excluded"),
        "destination": widgets.Text(value="/mnt/p/ecore_eo_datasets" if source == "goes" else "hf", description="Save to", placeholder="hf or a local directory"),
        "workers": widgets.IntText(value=8 if source == "goes" else default_workers(), description="Read threads", tooltip="GOES: source threads and scan batch limit per reader; HDF5 calls serialize in that process. Increase Readers for parallel HDF5; 1 source thread gives one scan per task. MRMS: source reads per monthly archive. Hugging Face writes are controlled separately."),
        "read_processes": widgets.IntText(value=2 if source == "goes" else 0, description="Readers", tooltip=(
            "GOES: independent HDF5 reader processes per band-month; each uses the source-read threads. "
            "MRMS: 0 uses threads in the monthly writer, or choose separate file-reader processes.")),
        "monthly_writers": widgets.BoundedIntText(value=2, min=1, max=8, description="Monthly writers",
            tooltip="Number of separate local monthly Zarr archives to build at once. HF publishing stays at one writer to limit gateway requests."),
        "scratch": widgets.Text(value="", description="Local scratch", placeholder="Optional fast staging folder"),
        "save_figures": widgets.Checkbox(value=False, description="Save displayed figures"),
        "figure_dir": widgets.Text(value="figures", description="Figure folder"),
        "output": widgets.Text(value=f"results/{source}", description="Run folder"),
        "image_index": widgets.IntSlider(value=0, min=0, max=0, description="Image index"),
        # Widgets offer full-disk GOES only; CONUS products stay available through the CLI.
        "product": (widgets.SelectMultiple(options=[
                        ("Precipitation rate (~2 min; sample every 10 min)", "PrecipRate_00.00"),
                        ("Composite reflectivity (~2 min; sample every 10 min)", "MergedReflectivityQCComposite_00.50"),
                        ("Low-level azimuthal shear (~2 min; sample every 10 min)", "MergedAzShear_0-2kmAGL_00.50"),
                        ("Multisensor Pass2 one-hour QPE (hourly)", mrms.DEFAULT_PRODUCT)],
                        value=mrms.DEFAULT_PRODUCTS, description="Products", rows=4,
                        tooltip="Default research set: precipitation rate, composite reflectivity, low-level shear, and hourly multisensor Pass2 QPE. Other products can be named with --product in script runs.",
                        layout=widgets.Layout(width="600px")) if source == "mrms" else
                    widgets.Dropdown(options=[("MCMIPF, all bands at 2 km", "ABI-L2-MCMIPF"),
                        ("CMIPF, each band at natural resolution", "ABI-L2-CMIPF")],
                        value="ABI-L2-CMIPF", description="Product", layout=widgets.Layout(width="600px"))),
    }
    for name, value in zip(("west", "south", "east", "north"), PR_BBOX):
        controls[name] = widgets.FloatText(value=value, description=name.title(), layout=widgets.Layout(width="210px"))

    def change_period(change):
        start, end = BENCHMARK_PERIODS[change["new"]]
        controls["start"].value, controls["end"].value = date.fromisoformat(start), date.fromisoformat(end)

    controls["period"].observe(change_period, names="value")
    rows = [controls["period"], widgets.HBox([controls["start"], controls["end"]]),
            widgets.HBox([controls["west"], controls["south"]]),
            widgets.HBox([controls["east"], controls["north"]]), controls["product"]]
    if source == "goes":
        controls['read_mode'] = widgets.Dropdown(options=[('Regional range reads','range'),
            ('Shared bands (mixed)','shared'), ('Async full-file staging','async_full'), ('Overlapped staging','async_pipeline')],value='shared',description='Read mode',
            tooltip='Async mode downloads whole CMIPF objects concurrently before local HDF5 reads; uses extra network and scratch space.')
        controls['read_profiles'] = widgets.Text(description='Profiles JSON', tooltip='Optional validated per-band settings file. Overrides reader settings for each band, never the number of writers.', layout=widgets.Layout(width='650px'))
        controls['download_concurrency'] = widgets.BoundedIntText(value=32,min=1,max=128,
            description='Async GETs',tooltip='Global full-object downloads shared by all active months and seven async bands.')
        controls['staging_mib'] = widgets.BoundedIntText(value=16384,min=1,max=65536,
            description='Stage MiB',tooltip='Global full-object scratch capacity. An eight-scan batch reserves space before download.')
        controls["prefetch_mib"] = widgets.BoundedIntText(value=2048, min=8, max=8192,
            description="Queue MiB", tooltip="Global ROI and IPC memory reservations across all active months and bands.")
        controls["block_size_kib"] = widgets.Dropdown(options=[256,1024,4096], value=1024,
            description="Range KiB", tooltip="Size of one cached NOAA S3 range-read block.")
        from .goes_shared import default_config
        defaults=default_config()
        controls['monthly_writers'].value=defaults.month_writers
        controls['local_readers'] = widgets.BoundedIntText(value=defaults.local_readers,min=1,max=32,description='Local readers',tooltip='Global independent processes crop locally downloaded HDF5 files.')
        controls['range_readers'] = widgets.BoundedIntText(value=defaults.range_readers,min=1,max=32,description='C02 readers',tooltip='Global independent processes range-read C02 source files.')
        controls['tail_months'] = widgets.BoundedIntText(value=defaults.tail_months,min=0,max=8,description='C02 tails',tooltip='Additional C02-only month owners; share the same reader and memory limits.')
        controls['monthly_writers'].description='Month writers'
        controls['monthly_writers'].tooltip='Normal monthly store-owner processes. Each owns separate band archives; C02 tails are additional owners.'
        controls['workers'].layout.display='none'
        controls['read_processes'].layout.display='none'
        controls['read_profiles'].disabled=True
        controls["satellite"] = widgets.Dropdown(options=[("GOES-East for dates", "auto")]+[(f"GOES-{s}",s) for s in (16,17,18,19)], value="auto", description="GOES")
        wavelengths = (0.47, 0.64, 0.86, 1.37, 1.6, 2.2, 3.9, 6.2,
                       6.9, 7.3, 8.4, 9.6, 10.3, 11.2, 12.3, 13.3)
        names = ("Blue visible", "Red visible", "Veggie near-IR", "Cirrus", "Snow/ice",
                 "Cloud phase", "Shortwave IR", "Upper-level water vapor",
                 "Mid-level water vapor", "Lower-level water vapor", "Cloud-top phase",
                 "Ozone", "Clean longwave window", "Longwave window", "Dirty longwave window",
                 "CO₂ longwave")
        band_options = [(f"{names[b-1]} (C{b:02d}, {wavelengths[b-1]:g} µm)", b)
                        for b in range(1, 17)]
        controls["bands"] = widgets.SelectMultiple(options=band_options,
            value=goes.STORMSCOPE_BANDS, description="ABI bands", rows=6,
            tooltip="Choose native ABI channels. StormScope example defaults are C01, C02, C03, C07, C08, C09, C10, and C13.")
        controls["scans_per_hour"] = widgets.Dropdown(options=[("All available (~10-minute scans)", 0)]+[(f"{n} per hour", n) for n in range(1, 7)],
            value=0, description="Scan frequency", tooltip="Keep every available scan by default. Choose a smaller count only to make an exploratory selection shorter.")
        controls["reuse_cmipf"] = widgets.Checkbox(value=False,
            description="Reuse verified 2 km CMIPF bands (hybrid archive)",
            tooltip="Optional for existing CMIPF months. Reuse exact-time C07/C08/C09/C10/C13 pixels; fetch C01/C02/C03 and any missing band from NOAA MCMIPF. The archive is labeled hybrid with per-band origin.")
        controls["reuse_cmipf_root"] = widgets.Text(value="/mnt/p/ecore_eo_datasets",
            description="CMIPF root", disabled=True,
            tooltip="Local root containing verified CMIPF monthly ZIPs. Used only when hybrid reuse is checked.")
        def change_reuse(change):
            controls["reuse_cmipf_root"].disabled = not change["new"]
        controls["reuse_cmipf"].observe(change_reuse, names="value")
        def change_product(change):
            if change['new'] != 'ABI-L2-CMIPF':
                controls['read_mode'].value = 'range'
            controls['read_mode'].disabled = change['new'] != 'ABI-L2-CMIPF'
            if change["new"] != "ABI-L2-MCMIPF":
                controls["reuse_cmipf"].value = False
            controls["reuse_cmipf"].disabled = change["new"] != "ABI-L2-MCMIPF"
        def change_read_mode(change):
            shared=change['new']=='shared'
            controls['workers'].layout.display='none' if shared else ''
            controls['read_processes'].layout.display='none' if shared else ''
            controls['read_profiles'].disabled=shared
            for key in ('local_readers','range_readers','tail_months'):
                controls[key].layout.display='' if shared else 'none'
            controls['monthly_writers'].description='Month writers' if shared else 'Band writers'
            controls['download_concurrency'].tooltip=('Global whole-object downloads across all bands and months.' if shared else 'Whole-object downloads per active band-month in legacy mode.')
            controls['prefetch_mib'].tooltip=('Global ROI and IPC reservations across all months and bands.' if shared else 'ROI queue budget per active legacy band-month.')
        controls['read_mode'].observe(change_read_mode,names='value')
        change_read_mode({'new':controls['read_mode'].value})
        controls["product"].observe(change_product, names="value")
        change_product({"new": controls["product"].value})
        rows += [widgets.HBox([controls["satellite"], controls["bands"]]), controls["scans_per_hour"],
                 controls['read_mode'], widgets.HBox([controls['local_readers'],controls['range_readers'],controls['tail_months']]), controls['read_profiles'],widgets.HBox([controls['download_concurrency'],controls['staging_mib']]),
                 widgets.HBox([controls["prefetch_mib"], controls["block_size_kib"]]),
                 controls["reuse_cmipf"], controls["reuse_cmipf_root"]]
    else:
        controls["tolerance_minutes"] = widgets.BoundedFloatText(value=5, min=0, max=5, description="Margin (min)")
        controls["time_match"] = widgets.Dropdown(options=[("Latest at/before slot", "previous"), ("Nearest, either side", "nearest"), ("Exact clock hour", "exact")], description="Hour match")
        rows += [controls["tolerance_minutes"], controls["time_match"]]
    controls["cheatsheet"] = product_cheatsheet(source)
    rows += [controls["cheatsheet"], controls["destination"], controls["workers"], controls["read_processes"],
             controls["monthly_writers"], controls["scratch"], controls["output"]]
    for name, control in controls.items():
        if hasattr(control, "tooltip") and not control.tooltip:
            control.tooltip = {"start": "First requested UTC date, included.", "end": "Stopping UTC date, excluded.",
                "destination": "hf uses the configured bucket. Enter a local path to store locally.",
                "scratch": "Temporary local source files and Zarr staging; cleaned after each task.",
                "output": "Small selections, diagnostics and timing reports; separate from raw data.",
                "time_match": "Previous avoids selecting an observation from the future.",
                "tolerance_minutes": "Maximum difference between the hourly slot and actual observation time."}.get(name, getattr(control, "description", "Product and variable guide"))
    note=("GOES shared limits apply across every active month and band. C02 tails add store owners, not download or reader capacity. RAM also includes HDF5 caches and Python processes." if source=='goes' else "MRMS downloads and GRIB decoding have separate limits per product-month writer.")
    rows.insert(-4, widgets.HTML("<small>"+note+"</small>"))
    if source=='goes':
        for name in ('monthly_writers','local_readers','range_readers','tail_months','download_concurrency','staging_mib','prefetch_mib'):
            controls[name].style.description_width='110px'
            controls[name].layout.width='250px'
        for row in rows:
            if isinstance(row,widgets.HBox):row.layout.flex_flow='row wrap'
    controls["panel"] = widgets.VBox(rows)
    controls["view_panel"] = widgets.VBox([controls["image_index"], controls["save_figures"], controls["figure_dir"]])
    return controls


def read_controls(controls):
    if controls["start"].value is None or controls["end"].value is None:
        raise ValueError("Choose both dates.")
    request = {"start": str(controls["start"].value), "end": str(controls["end"].value),
               "bbox": tuple(controls[name].value for name in ("west", "south", "east", "north")),
               "product": controls["product"].value}
    if "satellite" in controls:
        request.update(satellite=controls["satellite"].value, bands=controls["bands"].value,
                       scans_per_hour=controls["scans_per_hour"].value or None)
    else:
        request.update(products=controls["product"].value,
                       tolerance_minutes=controls["tolerance_minutes"].value,
                       time_match=controls["time_match"].value)
    return request


def action(label, function):
    """Display one button and retain its return value for following cells."""
    from IPython.display import display
    button, output, result = widgets.Button(description=label, layout=widgets.Layout(width="250px")), widgets.Output(), {}

    def click(_):
        button.disabled = True
        result.pop("value", None)
        with output:
            output.clear_output()
            try:
                result["value"] = function()
                print("Finished. Continue with the next cell.")
            except Exception as e:
                print(f"{type(e).__name__}: {e}")
            finally:
                button.disabled = False

    button.on_click(click)
    display(widgets.VBox([button, output]))
    return result


class FetchProgress:
    """One progress bar for the known STAC item count, shared with scripts."""
    def __init__(self, total, description='Raw subsets'):
        from tqdm.auto import tqdm
        from collections import Counter
        self.counts = Counter()
        self.bar = tqdm(total=total, desc=description, unit='file')

    def __enter__(self):
        return self

    def __call__(self, done, total, row):
        if row.get('progress_kind')=='aggregate':
            self.bar.set_postfix({'checkpointed':done,'remaining':max(0,total-done)},refresh=False)
        else:
            self.counts[row['status']] += 1
            self.bar.set_postfix(dict(self.counts), refresh=False)
        self.bar.update(max(0, done-self.bar.n))

    def __exit__(self, *args):
        self.bar.close()
