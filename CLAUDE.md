# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains two Python modules for analyzing PACT (Photovoltaic Array Controller Test) outdoor module degradation data shared via Box.com flat files:

- **`pact_analysis/pact_analysis.py`** — `PACTAnalysis` class: loads flat files, computes daily efficiency, quality flags, T80, and summary statistics
- **`pact_plots/pact_plots.py`** — `PACTPlots` class: wraps `PACTAnalysis` to produce official PACT figures (blue-bar daily plots, bubble plots, streak plots)

The modules are not installed as packages; they are loaded directly via `sys.path.insert`. See `pact_analysis/examples/basic_example.ipynb` for usage.

## Usage Pattern

```python
import sys
sys.path.insert(0, '/path/to/pact_analysis')
import pact_analysis, pact_plots

flat_file_path = '/Users/jsstein/bin/Box Sync/PACT - Data/'

pa = pact_analysis.PACTAnalysis(flat_file_path)
pp = pact_plots.PACTPlots(flat_file_path)

# Core workflow for a single module
point_data   = pa.get_point_data(module)       # raw 30s MPPT data
daily_data   = pa.daily_performance(module)    # efficiency + quality flags
summary      = pa.summary_info(module)         # T80, peak efficiency, etc.
fig          = pp.daily_performance_plot(module)
fig          = pp.bubble_plot(modules)
```

## Architecture

### Flat File Directory Structure

`PACTAnalysis` auto-discovers modules by globbing for:
```
flat_file_path / P-XXXX-XX / {Outdoor_SNL,Outdoor_NREL,Outdoor_SNL_fixed-tilt} / data / point-data / *.csv
```
Reference modules live under `PACT_reference/` instead of `P-*-XX/`. IV-curve data lives alongside point-data in `data/iv-data/`.

Module IDs have the form `P-XXXX-XX` (e.g., `P-0138-01`). Batch folders use `P-XXXX-XX` (last two digits replaced with `XX`).

### Multijunction / Metamodule Handling

Modules with `module_type` containing `' J'` (e.g., `'GaInP/GaAs/Ge J1'`) are submodules of a multi-terminal metamodule. Submodules share a name up to the `-J` suffix (e.g., `P-0050-01-J1`, `P-0050-01-J2`). When all expected junctions are present, `pa.metamodules_available` contains the parent name. For metamodules, `get_point_data` sums `pmp` across junctions and averages environmental columns.

### Quality Flags in `daily_performance()`

`daily_performance()` joins five flag columns onto the daily efficiency table. A day's efficiency is set to `nan` if any flag is `False`:

| Flag | Meaning |
|------|---------|
| `flag_deployed` | Module was outdoors (not in `days_indoors` metadata) |
| `flag_uncensored` | Day not in `days_censored` metadata |
| `flag_snow_free` | Date not in `site_metadata['snow_days']` |
| `flag_min_up_fraction` | ≥80% of expected daytime data points present |
| `flag_min_insolation` | Daily POAI ≥ 4000 Wh/m² (informational; doesn't invalidate days) |

### T80 Metric

`summary_info()` (current method) determines T80 as the number of `days_deployed` when daily efficiency first drops below 80% of the rolling **peak** efficiency (5-day median) for 3 consecutive days. The older `summary_info_legacy()` uses **initial** efficiency instead of peak. Plot methods have matching `_legacy` variants.

### Caching

`get_point_data`, `get_module_metadata`, `get_site_metadata`, `daily_performance`, and `summary_info` are all decorated with `@lru_cache`. Call `pa.update_availability()` if flat files change during a session.

### `pact_plots/synoptic_exceptions.json`

Lists modules to `exclude` entirely from synoptic (summary) plots, and modules to `force-termination` (treat as having reached T80 even if the algorithm hasn't declared it).

### Ephemeris File

`pact_analysis` requires `de421.bsp` (Skyfield ephemeris) to compute sunrise/sunset for uptime fraction. A copy lives in `pact_analysis/` and `pact_analysis/examples/`. The `PACTAnalysis.__init__` calls `skyfield.api.load('de421.bsp')` so the file must be present in the working directory or on Skyfield's search path.

## Key Configuration Constants

Set on the `PACTAnalysis` instance; can be overridden after construction:

```python
pa.efficiency_minimum_irradiance = 100   # W/m², minimum POA for efficiency calc
pa.efficiency_minimum_insolation = 4000  # Wh/m², threshold for flag_min_insolation
pa.min_up_fraction = 0.8                 # minimum daytime uptime fraction
pa.site_tz = datetime.timezone(datetime.timedelta(hours=-7))  # MST (hardcoded)
```

## Data Pipeline (`NEW_PACT_Analysis.ipynb`)

A separate Jupyter notebook pipeline generates the flat files consumed by these modules. It queries a Sandia SQL Server database (`PVGrid` on `DB03SNLNT\PR`), processes MPPT and meteorological data, and uploads results to AWS S3 and Box. Run `NEW_PACT_Functions.ipynb` first to define helper functions.

## Dependencies

pandas, numpy, scipy, skyfield, matplotlib, json (stdlib) — for `pact_analysis`
`pact_analysis`, matplotlib, numpy, pandas, datetime, json — for `pact_plots`
