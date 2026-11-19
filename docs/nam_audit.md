# NAM forecasts for the Puerto Rico study

The Puerto Rico NAM archive supports an hourly comparison from late September
2021 through June 2026. Its grid covers **91.0% of the acquisition region**
and **95.9% of the centered 256 × 256 model crop** on the 3 km project grid.
The audit downloaded **zero GRIB forecast bytes**. It used anonymous NOAA
object listings, text indexes and the earlier decoded audit for a check of
grid dimensions and compression. [NOAA AWS archive](https://registry.opendata.aws/noaa-nam/),
[NOAA NAM model documentation](https://emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/nam.php).

## Where the fields are available

The audited product is the high-resolution Puerto Rico nest, called
`priconest` in filenames. NOAA describes it as the **3 km NAM nest**. Its
published output grid is Mercator, with 544 columns, 310 rows, and reported
2.5 km spacing at its 20° N reference latitude. Those dimensions describe
NAM's output grid. Our 3 km model grid uses separate coordinates.

| Footprint | Longitude | Latitude |
|---|---|---|
| NAM native gridpoint bounds | 75.500–62.509° W | 15.000–22.005° N |
| Project acquisition box | 70.240–62.560° W | 14.360–22.040° N |

The missing area is mostly south of **15° N**, about 71 km of latitude
at the bottom of the acquisition box. There is also a narrow northern
strip. All acquisition longitudes lie within the published NAM bounds.
Of 76,604 project-grid cell centers inside the acquisition box, 69,736
lie within NAM's bounds. The centered model crop has 62,872 of 65,536
centers within the bounds. These are geographic coverage counts.
Variable-specific missing values and the interpolation stencil at the
grid edge could further reduce usable coverage. The earlier decoded
audit found a 310 × 321 native crop in all 133 samples.
[NOAA grid 194 specification](https://www.nco.ncep.noaa.gov/pmb/docs/on388/tableb.html).

NOAA also produced a **12 km Central America/Caribbean** grid, `afwaca`,
whose published bounds contain the whole acquisition box. A spot check
of the AWS archive found three-hourly files from September 2021 onward
on the sampled dates. The January 1 and August 10, 2021 examples were
absent. This broader product is a candidate for coarse regional context.
Its full historical availability and variables were outside the
Puerto Rico nest inventory. NOAA retired operational NAM in November
2026. RRFS is a separate model and would require its own coverage check.
[NOAA product inventory](https://www.nco.ncep.noaa.gov/pmb/products/nam/),
[NOAA grid specifications](https://www.nco.ncep.noaa.gov/pmb/docs/on388/tableb.html),
[NOAA retirement notice](https://www.weather.gov/media/notification/pdf_2026/SCN26-47_Retirement_of_NAM_SREF_HREF_HiresW_NAM_MOS_aad.pdf).

## When forecasts are available

A **cycle** is one model run initialized at 00, 06, 12 or 18 UTC. Each
cycle supplies an analysis at lead 0 and hourly forecasts through lead 60.
The **valid time** is the initialization plus the lead. Multiple cycles
can forecast the same valid hour, so the study counts each hour once.

The inventory checked all **8,028** scheduled cycles initialized from
January 1, 2021 through June 30, 2026, plus ten earlier cycles for
possible forecasts valid near the study start. It found **6,996** cycles
and **426,754** paired forecast objects and text indexes. The one
incomplete listed cycle was May 23, 2023 at 00 UTC, with leads 0 and 1
missing. Overlapping runs still covered those valid hours.

| Valid time period | Expected UTC hours | Any indexed forecast | Eligible by H−10 min |
|---|---:|---:|---:|
| 2021 | 8,760 | 2,568 | 2,378 |
| 2022 | 8,760 | 8,760 | 8,760 |
| 2023 | 8,760 | 8,760 | 8,760 |
| 2024 | 8,784 | 8,784 | 8,784 |
| 2025 | 8,760 | 8,760 | 8,760 |
| January–June 2026 | 4,344 | 4,344 | 4,344 |
| **Total** | **48,168** | **41,976 (87.1%)** | **41,786 (86.8%)** |

- **Archive presence.** The first indexed valid hour is September 16, 2021
  at 00 UTC. Each subsequent hour is represented through June 30, 2026
  at 23 UTC. There is no Puerto Rico nest coverage for the first 6,192
  study hours in this bucket.
- **Historical comparison.** For an hourly comparison at H, the notebook
  uses H−10 minutes as the cutoff and searches backward for the latest
  eligible initialization forecasting H. The audit used the GRIB object's
  S3 `LastModified` as evidence for this rule. Early files have archive
  timestamps several days later than their forecast time. That excludes
  another 190 hours, so the first eligible hour is **September 23, 2021
  at 22 UTC**, with continuous object coverage thereafter. The S3 timestamp
  cannot establish the original operational release time, particularly
  when data were copied into the archive later.
- **Storm windows.** All 180 requested hours across Earl, Fiona, Philippe,
  Ernesto and Erin have an eligible indexed object. All 52 Fred and Grace
  hours are earlier than the first archived cycle. The storm demo's earlier
  field-level check confirmed matching reflectivity at the same 180 hours.

## Variables for a precipitation pilot

This run read **3,480** text indexes, totaling **165.5 MB**. It sampled
initialization hours and forecast leads across every covered month. For the
pilot check, it selected one eligible forecast for each UTC hour of day
per month, spread across that month. All **1,392** of these indexes contain
a single match for every field below at the specified level and time statistic.
The full object census establishes archive availability. Variable presence
across the unsampled indexes and actual pixel validity remain unverified.
[NOAA analysis inventory](https://www.nco.ncep.noaa.gov/pmb/products/nam/nam.t00z.priconest.hiresf00.tm00.grib2.shtml),
[NOAA forecast inventory](https://www.nco.ncep.noaa.gov/pmb/products/nam/nam.t00z.priconest.hiresf06.tm00.grib2.shtml).

| Code and level | Plain-English meaning | Units |
|---|---|---|
| `PRES`, surface | Air pressure at the actual surface elevation. | Pa |
| `PRMSL`, mean sea level | Pressure adjusted to sea level for comparing weather systems across elevations. | Pa |
| `TMP`, 2 m | Near-surface air temperature. | K |
| `DPT`, 2 m | Near-surface dew point. Air cooled to this temperature would reach saturation. | K |
| `UGRD` and `VGRD`, 10 m | The two components of near-surface wind. Retain both for flow direction and speed, and check the wind-coordinate convention before rotating or regridding. | m/s |
| `PWAT`, whole column | Total water vapor above a point, expressed as the mass of liquid water it could yield. | kg/m² |
| `APCP`, surface | Total precipitation over the interval that ends at the valid hour. Applies to the water equivalent of rain and other precipitation. | kg/m² |
| `PRATE`, surface | Precipitation rate at the valid time. The index separately identifies a period maximum. | kg/m²/s |
| `REFC`, whole column | Strongest simulated radar return through the atmospheric column. It can be compared with observed composite reflectivity. | dBZ |

The **16-field** option adds six fields to describe the environment above
the surface. Keep their exact pressure levels with the data.

| Code and level | Plain-English meaning | Units |
|---|---|---|
| `HGT`, 500 hPa | Height of the 500 hPa pressure surface, useful for locating midlevel weather patterns. | geopotential m |
| `TMP`, 850 hPa | Lower-tropospheric air temperature. | K |
| `SPFH`, 850 hPa | Mass of water vapor per unit mass of moist air, at 850 hPa. | kg/kg |
| `UGRD` and `VGRD`, 850 hPa | Lower-tropospheric flow that helps describe moisture transport and storm motion. | m/s |
| `VVEL`, 700 hPa | Vertical motion expressed as a pressure change rate. Negative values typically indicate rising air. | Pa/s |

**Rainfall intervals matter.** The sampled pilot indexes contain 464 each
of 1-, 2- and 3-hour `APCP` intervals. For example, leads 4, 5 and 6 can
contain 3–4, 3–5 and 3–6 hour totals from the same run. Only the first of
those directly measures the preceding hour. Save the interval beginning,
interval end and initialization together. Deriving separate hourly totals
would require differences between compatible consecutive accumulations from
the same run and checks at each accumulation reset. Instantaneous `PRATE`
provides rate information, and integrating it would be a separate estimate.

## Storage and transfer estimates

The estimates store **one eligible forecast per valid hour** and one array
per selected field on the 310 × 321 native crop. A float32 array uses
398,040 logical bytes per field per hour. GB and TB below use decimal units.
The 30-day example assumes a fully covered month.

| Scope | Fields | Selected GRIB message transfer, low–median–high | Uncompressed float32 crop | Illustrative compressed crop |
|---|---:|---:|---:|---:|
| 30 days, 720 hours | 10 | 0.77–0.88–0.99 GB | 2.87 GB | 0.63–1.04 GB |
| 30 days, 720 hours | 16 | 1.32–1.50–1.67 GB | 4.59 GB | 1.16–1.92 GB |
| Entire eligible study, 41,786 hours | 10 | 44.5–51.3–57.3 GB | 166.3 GB | 36.8–60.2 GB |
| Entire eligible study, 41,786 hours | 16 | 76.8–87.1–96.7 GB | 266.1 GB | 67.4–111.5 GB |

The six core fields with matching previous measurements use their observed
10th–90th percentile compressed crop sizes. The pressure and dew-point
fields use planning allowances of 100–200 kB per image and `APCP` uses
10–120 kB. The upper-air estimate uses an earlier measured height crop
and allowances of 90–180 kB each for temperature and humidity, 170–250 kB
per wind component, and 100–240 kB for vertical motion. These are
assumptions for one-frame arrays compressed with Blosc/Zstd at level 3. The unmeasured fields,
weather mix and time chunking can change actual compression. Use the
uncompressed column as a baseline for capacity planning. A separate
uncompressed one-byte pixel mask per field would add up to 25%, plus
coordinates, provenance and filesystem overhead.

The transfer columns estimate the full-domain messages addressed by byte
ranges. Each range is counted once, including when the two wind components
share a GRIB message. The bounds scale the sampled 10th, 50th and 90th
percentiles of total bytes per hour. Index reads, HTTP overhead and retries
are additional. Whole-object reads for the same 41,786 hours would total
**3.29 TB** according to listed sizes. Keeping all runs and leads
initialized during the study would involve **37.27 TB** of source
objects before cropping or field selection.

## Reproduce the audit

```bash
python scripts/dataset_jobs.py process nam-audit --metadata-only \
  --start 2021-01-01 --end 2026-07-01 --output results/dataset-processing
```

Open `results/dataset-processing/nam-metadata-inventory.json` for monthly
coverage, lead counts, gaps, pilot byte summaries and the observed levels
and time descriptors for all variables. The command makes S3 LIST requests
and GET requests for `.idx` text files. It creates no forecast archives.

## Full field catalog

The 3,480 sampled indexes contain **133 distinct field codes**. Each code may occur at multiple heights or over different time intervals. Counts indicate indexes that contain the code at least once. Historical names such as CLMR/CLWMR and MDIV/MCONV have matching indexed levels in these samples. Verify their GRIB parameter identity and units before harmonizing them. [NOAA GRIB2 parameter documentation](https://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_doc/).

<details>
<summary>All observed codes in plain English</summary>

| Code | Meaning | Indexes |
|---|---|---:|
| `4LFTX` | Best four-layer lifted index. Tests buoyancy of parcels lifted from four near-surface layers. | 3,480 |
| `ABSV` | Absolute vorticity. Local atmospheric rotation combined with Earth's rotation. | 3,480 |
| `ALBDO` | Albedo. Fraction of incoming solar radiation reflected. | 3,480 |
| `APCP` | Total precipitation. Liquid-water equivalent accumulated over the reported interval. | 3,480 |
| `BGRUN` | Groundwater runoff. Water leaving through subsurface flow. | 3,480 |
| `BMIXL` | Blackadar mixing length. Length scale used to model turbulent mixing. | 3,480 |
| `BRTMP` | Brightness temperature. Blackbody temperature corresponding to modeled outgoing radiance. | 3,480 |
| `CAPE` | Convective available potential energy. Buoyant energy available to rising parcels. | 3,480 |
| `CCOND` | Canopy conductance. Ease of water-vapor exchange through vegetation. | 3,480 |
| `CD` | Drag coefficient. Factor relating flow to surface stress. | 3,480 |
| `CFRZR` | Freezing rain category. Flag for modeled freezing rain. | 3,480 |
| `CICE` | Cloud ice. Frozen water in cloud. | 3,480 |
| `CICEP` | Ice pellets category. Flag for modeled sleet or ice pellets. | 3,480 |
| `CIN` | Convective inhibition. Energy barrier to a parcel reaching free convection. | 3,480 |
| `CLMR` | Cloud mixing ratio. Cloud condensate relative to air mass under this historical index code. | 1,439 |
| `CLWMR` | Cloud mixing ratio. Cloud condensate relative to air mass under this index code. | 2,041 |
| `CNWAT` | Canopy water. Water held on the vegetation surface. | 3,480 |
| `CPOFP` | Frozen precipitation probability. Modeled likelihood of frozen precipitation. | 3,480 |
| `CRAIN` | Rain category. Flag for modeled rain. | 3,480 |
| `CSDSF` | Clear-sky downward solar flux. Incoming solar energy under a cloud-free calculation. | 3,480 |
| `CSNOW` | Snow category. Flag for modeled snow. | 3,480 |
| `DLWRF` | Downward thermal radiation. Infrared energy flux directed toward the surface. | 3,480 |
| `DPT` | Dew point. Temperature at which the air would reach saturation. | 3,480 |
| `DSWRF` | Downward shortwave radiation. Solar energy flux directed toward the surface. | 3,480 |
| `DZDT` | Geometric vertical velocity. Vertical motion expressed as height change per second. | 3,480 |
| `EVP` | Evaporation. Water lost to the atmosphere over the reported interval. | 3,480 |
| `FRICV` | Friction velocity. Velocity scale associated with turbulent surface stress. | 3,480 |
| `GFLUX` | Ground heat flux. Heat exchanged with the soil. | 3,480 |
| `GUST` | Wind gust. Modeled short-lived wind speed peak. | 3,480 |
| `HCDC` | High cloud cover. Percentage covered by high cloud. | 3,480 |
| `HGT` | Geopotential height. Height of a surface expressed relative to standard gravity. | 3,480 |
| `HINDEX` | Haines index. Combines dry air and instability indicators relevant to fire growth. | 3,480 |
| `HLCY` | Storm-relative helicity. Measures rotation potential from wind profiles relative to a moving storm. | 3,480 |
| `HPBL` | Boundary-layer height. Top of the lowest part of the atmosphere influenced by the surface. | 3,480 |
| `ICEC` | Ice cover. Fraction of the surface covered by ice. | 3,480 |
| `LAND` | Land mask. Distinguishes land from water. | 3,480 |
| `LCDC` | Low cloud cover. Percentage covered by low cloud. | 3,480 |
| `LFTX` | Surface lifted index. Parcel buoyancy indicator using a surface parcel. | 3,480 |
| `LHTFL` | Latent heat flux. Energy exchanged through water phase changes, such as evaporation. | 3,480 |
| `LRGHR` | Large-scale condensate heating. Temperature change from modeled condensation processes. | 3,480 |
| `LTNG` | Model lightning diagnostic. Indicates forecast lightning behavior from modeled conditions. | 3,480 |
| `LWHR` | Thermal radiation heating rate. Temperature tendency from longwave radiation. | 3,480 |
| `MAXDVV` | Peak downdraft speed. Largest downward vertical speed in the interval and low-level layer. | 3,248 |
| `MAXREF` | Peak simulated radar reflectivity. Interval maximum at the indicated height. | 3,248 |
| `MAXRH` | Peak relative humidity. Highest humidity relative to saturation in the interval. | 3,248 |
| `MAXUVV` | Peak updraft speed. Largest upward vertical speed in the interval and low-level layer. | 3,248 |
| `MAXUW` | First wind component at the hourly maximum 10 m wind speed. | 3,248 |
| `MAXVW` | Second wind component at the hourly maximum 10 m wind speed. | 3,248 |
| `MCDC` | Midlevel cloud cover. Percentage covered by middle cloud. | 3,480 |
| `MCONV` | Horizontal moisture divergence in the NCEP nest inventory. Indicates net lateral moisture transport. | 2,041 |
| `MDIV` | Horizontal moisture divergence. Historical index code for a lateral moisture transport diagnostic. | 1,439 |
| `MINRH` | Minimum relative humidity. Lowest humidity relative to saturation in the interval. | 3,248 |
| `MSLET` | Eta-method sea-level pressure. Pressure reduced to sea level using the Eta calculation. | 3,480 |
| `MSTAV` | Available soil moisture. Indicator of how much moisture is available in the ground. | 3,480 |
| `MXSALB` | Maximum snow albedo. Upper estimate of the reflectivity of snow-covered ground. | 3,480 |
| `MXUPHL` | Peak updraft helicity. An interval maximum of updraft rotation over a reported layer. | 3,248 |
| `NCPCP` | Nonconvective precipitation. Model precipitation from the large-scale processes. | 3,480 |
| `PEVAP` | Potential evaporation. Evaporation demand if moisture were available. | 3,480 |
| `PLI` | Parcel lifted index. Temperature difference used to assess buoyancy at 500 hPa. | 3,480 |
| `PLPL` | Parcel origin pressure. Pressure level from which the parcel was lifted. | 3,480 |
| `POROS` | Soil porosity. Fraction of soil volume that is pore space. | 3,480 |
| `POT` | Potential temperature. Temperature a parcel would have at a standard reference pressure. | 3,480 |
| `PRATE` | Precipitation rate. Mass of precipitation per unit area per second. | 3,480 |
| `PRES` | Air pressure. Atmospheric pressure at the indexed surface or level. | 3,480 |
| `PRMSL` | Mean-sea-level pressure. Surface pressure reduced to sea level. | 3,480 |
| `PWAT` | Precipitable water. Water vapor summed through the atmospheric column. | 3,480 |
| `RCQ` | Humidity control on canopy conductance. Describes how humidity limits vegetation exchange. | 3,480 |
| `RCS` | Solar control on canopy conductance. Describes how incoming sunlight affects vegetation exchange. | 3,480 |
| `RCSOL` | Soil-moisture control on canopy conductance. Describes how available soil water limits exchange. | 3,480 |
| `RCT` | Temperature control on canopy conductance. Describes the effect of temperature on vegetation exchange. | 3,480 |
| `REFC` | Composite reflectivity. Maximum simulated radar reflectivity in the vertical column. | 3,480 |
| `REFD` | Simulated radar reflectivity at the reported height or level. | 3,480 |
| `RETOP` | Radar echo top. Estimated altitude reached by detectable modeled echoes. | 3,480 |
| `RH` | Relative humidity. Water vapor as a percentage of saturation at the given temperature. | 3,480 |
| `RIME` | Rime factor. Indicator of supercooled droplets freezing onto ice particles. | 3,480 |
| `RLYRS` | Root-zone layers. Number of modeled soil layers occupied by roots. | 3,480 |
| `RSMIN` | Minimum stomatal resistance. Limit on how freely vapor can pass through leaf pores. | 3,480 |
| `RWMR` | Rainwater mixing ratio. Mass of liquid rain relative to air mass. | 3,480 |
| `SFCR` | Surface roughness length. Describes aerodynamic drag from surface texture. | 3,480 |
| `SFEXC` | Surface exchange coefficient. Controls transfer between the surface and overlying air. | 3,480 |
| `SHTFL` | Sensible heat flux. Heat exchanged directly between the surface and air. | 3,480 |
| `SMDRY` | Dry-soil evaporation threshold. Soil moisture below which direct evaporation ceases. | 3,480 |
| `SMREF` | Soil-moisture stress threshold. Soil wetness at which transpiration begins to be limited. | 3,480 |
| `SNFALB` | Snow-free albedo. Surface reflectivity in the absence of snow. | 3,480 |
| `SNMR` | Snow mixing ratio. Mass of airborne snow relative to air mass. | 3,480 |
| `SNOD` | Snow depth. Thickness of snow on the ground. | 3,480 |
| `SNOHF` | Snow phase-change heat flux. Energy exchanged as snow freezes or melts. | 3,480 |
| `SNOM` | Snowmelt. Meltwater produced over the reported interval. | 3,480 |
| `SNOWC` | Snow cover. Fraction of the ground covered by snow. | 3,480 |
| `SOILL` | Unfrozen soil water. Liquid water fraction of the soil. | 3,480 |
| `SOILM` | Soil water mass. Amount of water in the reported soil layer per unit ground area. | 3,480 |
| `SOILW` | Volumetric soil moisture. Soil volume fraction occupied by water. | 3,480 |
| `SOTYP` | Soil category. Integer class describing modeled soil type. | 3,480 |
| `SPFH` | Specific humidity. Water-vapor mass divided by moist-air mass. | 3,480 |
| `SRWEQ` | Snowfall water-equivalent rate. Rate of solid snowfall expressed as liquid-water mass. | 3,248 |
| `SSRUN` | Surface storm runoff. Water moving over the ground from precipitation. | 3,480 |
| `STRM` | Streamfunction. Flow quantity whose contours describe the nondivergent component of circulation. | 3,480 |
| `SWHR` | Solar heating rate. Temperature tendency due to shortwave radiation. | 3,480 |
| `TCDC` | Total cloud cover. Percentage covered by cloud across the column. | 3,480 |
| `TCLSW` | Column supercooled liquid water. Liquid below freezing summed through the column. | 3,480 |
| `TCOLC` | Column condensate. Condensed atmospheric water summed vertically. | 3,480 |
| `TCOLI` | Column cloud ice. Ice in clouds summed vertically. | 3,480 |
| `TCOLM` | Column melting ice. Melting frozen particles summed vertically. | 3,480 |
| `TCOLR` | Column rainwater. Falling liquid rain summed vertically. | 3,480 |
| `TCOLS` | Column snow. Airborne snow summed vertically. | 3,480 |
| `TCOLW` | Column cloud water. Liquid cloud droplets summed vertically. | 3,480 |
| `TCOND` | Total condensate mixing ratio. Condensed water mass relative to air at a reported level. | 3,480 |
| `TKE` | Turbulent kinetic energy. Energy per unit mass in turbulent motions. | 3,480 |
| `TMAX` | Maximum temperature during the indexed interval. | 3,248 |
| `TMIN` | Minimum temperature during the indexed interval. | 3,248 |
| `TMP` | Air or surface temperature at the indexed level. | 3,480 |
| `TSOIL` | Soil temperature at the reported depth. | 3,480 |
| `UFLX` | First component of turbulent momentum flux. | 3,480 |
| `UGRD` | First horizontal wind component. Together with VGRD it gives wind speed and direction. | 3,480 |
| `ULWRF` | Upward thermal radiation. Infrared energy flux directed upward. | 3,480 |
| `UPHL` | Updraft helicity. A measure of rotating ascent over a reported layer. | 3,480 |
| `USTM` | First horizontal component of modeled storm motion. | 3,480 |
| `USWRF` | Upward shortwave radiation. Reflected solar energy directed upward. | 3,480 |
| `VEG` | Vegetation fraction. Portion of ground covered by vegetation. | 3,480 |
| `VFLX` | Second component of turbulent momentum flux. | 3,480 |
| `VGRD` | Second horizontal wind component. Together with UGRD it gives wind speed and direction. | 3,480 |
| `VGTYP` | Vegetation category. Integer class describing land vegetation. | 3,480 |
| `VIL` | Vertically integrated liquid. Modeled radar diagnostic estimating liquid water in a column. | 3,480 |
| `VIS` | Visibility. Estimated horizontal viewing distance. | 3,480 |
| `VRATE` | Ventilation rate. Indicator of how effectively the boundary layer can disperse material. | 3,480 |
| `VSTM` | Second horizontal component of modeled storm motion. | 3,480 |
| `VUCSH` | Vertical shear of the first wind component. Its change with height. | 3,480 |
| `VVCSH` | Vertical shear of the second wind component. Its change with height. | 3,480 |
| `VVEL` | Pressure-coordinate vertical velocity. Negative values generally indicate ascent. | 3,480 |
| `VWSH` | Vertical wind-speed shear. Change in wind speed with height. | 3,480 |
| `WEASD` | Snow water equivalent. Liquid-water mass held in the accumulated snowpack. | 3,480 |
| `WILT` | Wilting point. Soil water content below which plants cannot extract enough water. | 3,480 |
| `WTMP` | Water temperature at the indexed surface or level. | 3,480 |

</details>
