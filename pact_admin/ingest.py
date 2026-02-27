"""Database ingestion and monthly point-data update for a single module.

Replicates the workflow from NEW_PACT_Analysis.ipynb scoped to one module
and one calendar month:

  1. Query dbo.PACT_MPPTData for the module + month.
  2. Query the appropriate meteorological table (based on TestPad).
  3. Query air temperature from dbo.PACT_MET_PACT_MET_30s.
  4. Merge all series, apply bias corrections, drop pre-deployment rows.
  5. Write point-data_{pact_id}_{YYYY-MM}.csv to Box Sync.
  6. Regenerate the full daily-efficiency bar chart PNG.
  7. Optionally upload both files to S3.
"""

import calendar
import importlib.util
import os
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from .config import get_base_path
from . import registry


# ---------------------------------------------------------------------------
# TestPad → meteorological table configuration
# ---------------------------------------------------------------------------

_TESTPAD_MET = {
    4: {   # East tracker
        'table': 'dbo.PACT_MET_PACTTracker1',
        'select': 'TmStamp, E_Tracker1_Wm2_Avg, Trkr1Azimuth, Trkr1Altitude',
        'rename': {
            'E_Tracker1_Wm2_Avg': 'poa_global',
            'Trkr1Azimuth': 'surface_azimuth',
            'Trkr1Altitude': 'surface_tilt',
        },
        'fixed_tilt': False,
    },
    5: {   # West tracker
        'table': 'dbo.PACT_MET_PACTTracker2',
        'select': 'TmStamp, E_Tracker2_Wm2_Avg, Trkr2Azimuth, Trkr2Altitude',
        'rename': {
            'E_Tracker2_Wm2_Avg': 'poa_global',
            'Trkr2Azimuth': 'surface_azimuth',
            'Trkr2Altitude': 'surface_tilt',
        },
        'fixed_tilt': False,
    },
    6: {   # West fixed tilt
        'table': 'dbo.PACT_MET_PACTWestTilt_30s',
        'select': 'TmStamp, E_WestTiltPOA_Wm2_Avg',
        'rename': {'E_WestTiltPOA_Wm2_Avg': 'poa_global'},
        'fixed_tilt': True,
        'surface_tilt': 35,
        'surface_azimuth': 180,
    },
    11: {  # East fixed tilt (uses same POA table as west, per notebook)
        'table': 'dbo.PACT_MET_PACTWestTilt_30s',
        'select': 'TmStamp, E_WestTiltPOA_Wm2_Avg',
        'rename': {'E_WestTiltPOA_Wm2_Avg': 'poa_global'},
        'fixed_tilt': True,
        'surface_tilt': 35,
        'surface_azimuth': 180,
    },
}

# Bias corrections keyed by (pact_id, testpad) → {column: multiplicative_factor}
# Source: NEW_PACT_Analysis.ipynb (TestPad 5 load-switching bias)
_BIAS_CORRECTIONS = {
    ('P-0042-03', 5): {'vmp': 1.14},
    ('P-0042-04', 5): {'vmp': 1.14},
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _make_engine(cfg):
    """Create a SQLAlchemy engine from pact_config settings."""
    from sqlalchemy import create_engine
    server = cfg['db_server']
    database = cfg['db_name']
    username = cfg.get('db_username', 'PVGridUser')
    password = os.getenv('DB_PASSWORD_PRuser')
    if password is None:
        raise EnvironmentError(
            'DB_PASSWORD_PRuser environment variable is not set. '
            'Export it before running this command.'
        )
    conn_str = (
        f'mssql+pyodbc://{username}:{password}@{server}/{database}'
        f'?driver=ODBC+Driver+17+for+SQL+Server'
    )
    return create_engine(conn_str)


def _make_s3_bucket(cfg):
    """Return a boto3 Bucket object, applying proxy/SSL from config.

    Returns None if boto3 is not installed.
    """
    try:
        import boto3
    except ImportError:
        return None

    profile = cfg.get('aws_profile', 'default')
    os.environ['AWS_PROFILE'] = profile
    os.environ['AWS_DEFAULT_PROFILE'] = profile

    proxy = cfg.get('proxy')
    if proxy:
        os.environ.setdefault('HTTP_PROXY', proxy)
        os.environ.setdefault('HTTPS_PROXY', proxy)

    ssl_cert = cfg.get('ssl_cert')
    if ssl_cert:
        os.environ.setdefault('REQUESTS_CA_BUNDLE', ssl_cert)
        os.environ.setdefault('AWS_CA_BUNDLE', ssl_cert)

    s3 = boto3.resource('s3')
    return s3.Bucket(cfg['s3_bucket'])


def _query_mppt(engine, pact_id, start_dt, end_dt, tz):
    """Query MPPT data for a single module and return a cleaned DataFrame."""
    sql = (
        f"SELECT * FROM dbo.PACT_MPPTData "
        f"WHERE TmStamp BETWEEN '{start_dt}' AND '{end_dt}' "
        f"  AND ModuleID = '{pact_id}' "
        f"ORDER BY TmStamp ASC"
    )
    df = pd.read_sql(sql, engine, index_col='TmStamp')
    df.index = df.index.tz_localize(tz)
    df.drop(columns=['PACTMPPTDataID', 'Power', 'Filename'],
            inplace=True, errors='ignore')
    df = df.rename(columns={
        'Voltage': 'vmp',
        'Current': 'imp',
        'Temperature': 'temperature_module',
    })
    return df.rename_axis('date_time')


def _query_met(engine, pad_cfg, start_dt, end_dt, tz):
    """Query the test-pad meteorological table and return a cleaned DataFrame."""
    sql = (
        f"SELECT {pad_cfg['select']} FROM {pad_cfg['table']} "
        f"WHERE TmStamp BETWEEN '{start_dt}' AND '{end_dt}' "
        f"ORDER BY TmStamp ASC"
    )
    df = pd.read_sql(sql, engine, index_col='TmStamp')
    df = df.rename(columns=pad_cfg['rename']).rename_axis('date_time')
    df.index = df.index.tz_localize(tz)
    return df


def _query_air_temp(engine, start_dt, end_dt, tz):
    """Query ambient air temperature from dbo.PACT_MET_PACT_MET_30s."""
    sql = (
        f"SELECT TmStamp, AmbientTemp_C_Avg FROM dbo.PACT_MET_PACT_MET_30s "
        f"WHERE TmStamp BETWEEN '{start_dt}' AND '{end_dt}' "
        f"ORDER BY TmStamp ASC"
    )
    df = pd.read_sql(sql, engine, index_col='TmStamp')
    df = df.rename(columns={'AmbientTemp_C_Avg': 'temperature_air'}).rename_axis('date_time')
    df.index = df.index.tz_localize(tz)
    return df


def _merge_columns(dfmod, df_met, df_air, pad_cfg):
    """Concatenate MPPT, met, and air data into the canonical column order."""
    df_met = df_met[~df_met.index.duplicated()]
    df_air = df_air[~df_air.index.duplicated()]
    dfmod = dfmod[~dfmod.index.duplicated()]

    if pad_cfg['fixed_tilt']:
        # Assign constant tilt/azimuth onto the air-temp index (mirrors notebook)
        df_air = df_air.copy()
        df_air['surface_tilt'] = pad_cfg['surface_tilt']
        df_air['surface_azimuth'] = pad_cfg['surface_azimuth']
        return pd.concat([
            df_met['poa_global'],
            df_air['temperature_air'],
            dfmod['temperature_module'],
            dfmod['vmp'],
            dfmod['imp'],
            df_air['surface_tilt'],
            df_air['surface_azimuth'],
        ], axis=1)
    else:
        return pd.concat([
            df_met['poa_global'],
            df_air['temperature_air'],
            dfmod['temperature_module'],
            dfmod['vmp'],
            dfmod['imp'],
            df_met['surface_tilt'],
            df_met['surface_azimuth'],
        ], axis=1)


def _check_metadata_consistency(base_path):
    """Scan Box Sync for modules that have CSV files but no metadata entry.

    Returns a list of (module_id, csv_path) tuples for orphaned modules.
    Raises nothing — callers decide how to handle the results.
    """
    import glob
    import json as _json

    orphans = []
    for meta_path in sorted(Path(base_path).glob('P-*-XX/*/data/metadata/module-metadata.json')):
        with open(meta_path) as f:
            try:
                metadata = _json.load(f)
            except Exception:
                continue
        known_ids = {m['module_id'] for m in metadata}
        point_data_dir = meta_path.parent.parent / 'point-data'
        for csv in sorted(point_data_dir.glob('point-data_*.csv')):
            parts = csv.stem.split('_')   # point-data_P-XXXX-XX_YYYY-MM
            if len(parts) >= 2:
                mod_id = parts[1]
                if mod_id not in known_ids:
                    orphans.append((mod_id, str(csv)))
    return orphans


def _regenerate_plot(cfg, pact_id, batch, outdoor_dir, verbose):
    """Re-run PACTPlots and save the daily-efficiency PNG. Returns the PNG path."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    _pa, pact_analysis_dir, ephemeris_dir = _load_pact_analysis(cfg)

    pact_plots_dir = cfg.get('pact_plots_path')
    if not pact_plots_dir:
        raise KeyError('pact_plots_path must be set in pact_config.json')

    pact_plots_file = Path(pact_plots_dir) / 'pact_plots.py'
    spec = importlib.util.spec_from_file_location('pact_plots', pact_plots_file)
    _pp = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(_pp)

    flat_file_path = str(get_base_path(cfg))

    # Pre-flight: PACTAnalysis.__init__ will crash with a cryptic IndexError
    # if any CSV file belongs to a module not in module-metadata.json.
    orphans = _check_metadata_consistency(flat_file_path)
    if orphans:
        lines = '\n'.join(f'  {mid}  ({csv})' for mid, csv in orphans)
        raise ValueError(
            'The following modules have point-data CSV files but no entry in '
            'module-metadata.json. Add them to the setup CSV and run '
            '"python -m pact_admin sync-metadata", or remove the orphaned files:\n'
            + lines
        )

    from skyfield.iokit import Loader as _SkyLoader
    import skyfield.api as _skyfield_api
    _original_loader = _skyfield_api.load
    _skyfield_api.load = _SkyLoader(ephemeris_dir)
    try:
        pp = _pp.PACTPlots(flat_file_path)
    finally:
        _skyfield_api.load = _original_loader

    plots_dir = get_base_path(cfg) / f'{batch}-XX' / outdoor_dir / 'daily_plots'
    plots_dir.mkdir(parents=True, exist_ok=True)
    png_path = plots_dir / f'daily_efficiency_{pact_id}.png'

    if verbose:
        print('  Generating daily-efficiency bar chart...')

    fig = pp.daily_performance_plot(pact_id)
    plt.text(
        0, 0.01,
        'do not publish\nautomatically generated on '
        + datetime.today().strftime('%Y-%m-%d'),
    )
    fig.savefig(str(png_path), bbox_inches='tight')
    plt.close(fig)

    if verbose:
        print(f'  Wrote: {png_path}')
    return png_path


def _upload_to_s3(cfg, batch, outdoor_dir, csv_path, csv_name, png_path, pact_id, verbose):
    """Upload the CSV and PNG to S3. Silently skips if boto3 is missing."""
    bucket = _make_s3_bucket(cfg)
    if bucket is None:
        if verbose:
            print('  boto3 not available; skipping S3 upload.')
        return

    s3_csv_key = f'{batch}-XX/{outdoor_dir}/data/point-data/{csv_name}'
    try:
        bucket.upload_file(str(csv_path), s3_csv_key)
        if verbose:
            print(f'  S3: {s3_csv_key}')
    except Exception as exc:
        print(f'  S3 upload failed for CSV: {exc}')

    if png_path.exists():
        s3_png_key = f'{batch}-XX/{outdoor_dir}/daily_plots/daily_efficiency_{pact_id}.png'
        try:
            bucket.upload_file(str(png_path), s3_png_key)
            if verbose:
                print(f'  S3: {s3_png_key}')
        except Exception as exc:
            print(f'  S3 upload failed for plot: {exc}')


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def update_module_month(cfg, pact_id, year, month, upload_s3=True, verbose=True):
    """Fetch one month of data, write point-data CSV and bar chart.

    Parameters
    ----------
    cfg : dict
        Loaded pact_config.json (via pact_admin.config.load_config()).
    pact_id : str
        Module identifier, e.g. 'P-0042-04'.
    year : int
        Four-digit year.
    month : int
        Month number (1–12).
    upload_s3 : bool
        If True (default), upload the CSV and PNG to S3 after writing locally.
        Pass False when not on the SNL network or when S3 credentials are
        unavailable.
    verbose : bool
        Print progress messages.
    """
    yearmonth = f'{year}-{month:02d}'
    last_day = calendar.monthrange(year, month)[1]
    start_dt = f'{yearmonth}-01 00:00:00'
    end_dt = f'{yearmonth}-{last_day:02d} 23:59:59'

    if verbose:
        print(f'[{pact_id}] Updating {yearmonth}')
        print(f'  Date range: {start_dt}  to  {end_dt}')

    # --- deployment start date from setup CSV ---
    modules_df = registry.read_modules(cfg)
    rows = modules_df[modules_df['PACT_id'] == pact_id]
    if rows.empty:
        raise ValueError(f'{pact_id} not found in setup CSV')
    module_row = rows.iloc[0]
    tz = cfg.get('db_timezone', 'MST')
    deployment_start = pd.Timestamp(module_row['Start_date']).tz_localize(tz)
    site_key = module_row.get('Site', 'SNL') or 'SNL'
    outdoor_dir = cfg['sites'][site_key]['outdoor_directory']
    batch = pact_id[:6]

    # --- connect to SQL Server ---
    engine = _make_engine(cfg)

    # --- Step 1: MPPT data ---
    if verbose:
        print('  Querying MPPT data...')
    dfmod = _query_mppt(engine, pact_id, start_dt, end_dt, tz)

    if dfmod.empty:
        print(f'  {pact_id}: No MPPT data found for {yearmonth}. Nothing to do.')
        return

    testpad = int(dfmod['TestPad'].iloc[-1])
    if verbose:
        print(f'  TestPad: {testpad}  |  '
              f'Latest MPPT: {dfmod.index[-1].strftime("%Y-%m-%d %H:%M")}')

    if testpad not in _TESTPAD_MET:
        raise ValueError(
            f'TestPad {testpad} for {pact_id} is not recognised. '
            f'Known TestPads: {sorted(_TESTPAD_MET)}'
        )
    pad_cfg = _TESTPAD_MET[testpad]

    # --- Step 2: meteorological data ---
    if verbose:
        print(f'  Querying met from {pad_cfg["table"]}...')
    df_met = _query_met(engine, pad_cfg, start_dt, end_dt, tz)

    # --- Step 3: air temperature ---
    if verbose:
        print('  Querying air temperature...')
    df_air = _query_air_temp(engine, start_dt, end_dt, tz)

    # --- Step 4: merge ---
    df_all = _merge_columns(dfmod, df_met, df_air, pad_cfg)

    # --- bias corrections ---
    correction_key = (pact_id, testpad)
    if correction_key in _BIAS_CORRECTIONS:
        for col, factor in _BIAS_CORRECTIONS[correction_key].items():
            df_all[col] = df_all[col] * factor
            if verbose:
                print(f'  Bias correction applied: {col} × {factor}')

    # --- drop rows before deployment ---
    df_all = df_all[df_all.index >= deployment_start]

    # --- Step 5: write point-data CSV ---
    point_data_dir = (
        get_base_path(cfg) / f'{batch}-XX' / outdoor_dir / 'data' / 'point-data'
    )
    point_data_dir.mkdir(parents=True, exist_ok=True)
    csv_name = f'point-data_{pact_id}_{yearmonth}.csv'
    csv_path = point_data_dir / csv_name
    df_all.to_csv(csv_path, index=True)
    if verbose:
        print(f'  Wrote: {csv_path}')

    # --- Step 6: regenerate bar chart ---
    png_path = _regenerate_plot(cfg, pact_id, batch, outdoor_dir, verbose)

    # --- Step 7: S3 uploads ---
    if upload_s3:
        _upload_to_s3(cfg, batch, outdoor_dir, csv_path, csv_name,
                      png_path, pact_id, verbose)

    if verbose:
        print(f'[{pact_id}] Done.')


def _load_pact_analysis(cfg):
    """Load pact_analysis.py via importlib and return the module object.

    Applies the Skyfield loader redirect so de421.bsp is found locally.
    """
    pact_analysis_dir = cfg.get('pact_analysis_path')
    ephemeris_dir = cfg.get('ephemeris_path', pact_analysis_dir)
    if not pact_analysis_dir:
        raise KeyError('pact_analysis_path must be set in pact_config.json')

    if pact_analysis_dir not in sys.path:
        sys.path.insert(0, pact_analysis_dir)

    from skyfield.iokit import Loader as _SkyLoader
    import skyfield.api as _skyfield_api
    _original_loader = _skyfield_api.load
    _skyfield_api.load = _SkyLoader(ephemeris_dir)
    try:
        pa_file = Path(pact_analysis_dir) / 'pact_analysis.py'
        spec = importlib.util.spec_from_file_location('pact_analysis', pa_file)
        _pa = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(_pa)
    finally:
        _skyfield_api.load = _original_loader

    return _pa, pact_analysis_dir, ephemeris_dir


def plot_all_efficiency(cfg, output_path, active_only=False, batch=None,
                        use_plotly=False, pre_t80=False, verbose=True):
    """Generate a multi-line daily efficiency plot for all (or filtered) modules.

    Each module is drawn as its own line.  Days that fail quality flags are
    omitted (they appear as gaps in the line).

    Parameters
    ----------
    cfg : dict
        Loaded pact_config.json.
    output_path : str
        File path for the saved plot (PNG for matplotlib, HTML for plotly).
    active_only : bool
        If True, only plot modules listed as Active=Y in the setup CSV.
    batch : str or None
        If set, only plot modules whose PACT_id starts with this prefix
        (e.g. 'P-0042').
    use_plotly : bool
        If True, generate an interactive HTML plot using plotly instead of a
        static PNG.  Hovering over a line shows the module PACT-ID.
    pre_t80 : bool
        If True, truncate each module's data at its T80 date (the first day
        efficiency dropped below 80% of peak for 3 consecutive days).
        Modules that have not reached T80 are plotted in full.
    verbose : bool
    """
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    flat_file_path = str(get_base_path(cfg))

    # Pre-flight metadata consistency check
    orphans = _check_metadata_consistency(flat_file_path)
    if orphans:
        lines = '\n'.join(f'  {mid}  ({csv})' for mid, csv in orphans)
        raise ValueError(
            'The following modules have point-data CSV files but no entry in '
            'module-metadata.json. Add them to the setup CSV and run '
            '"python -m pact_admin sync-metadata", or remove the orphaned files:\n'
            + lines
        )

    _pa, pact_analysis_dir, ephemeris_dir = _load_pact_analysis(cfg)

    from skyfield.iokit import Loader as _SkyLoader
    import skyfield.api as _skyfield_api
    _original_loader = _skyfield_api.load
    _skyfield_api.load = _SkyLoader(ephemeris_dir)
    try:
        pa = _pa.PACTAnalysis(flat_file_path)
    finally:
        _skyfield_api.load = _original_loader

    # Determine which modules to include
    all_modules = sorted(pa.modules_available)

    if active_only or batch:
        modules_df = registry.read_modules(cfg)
        active_ids = set(modules_df[modules_df['Active'] == 'Y']['PACT_id'])
        if active_only:
            all_modules = [m for m in all_modules if m in active_ids]
        if batch:
            batch_prefix = batch[:6]
            all_modules = [m for m in all_modules if m.startswith(batch_prefix)]

    if not all_modules:
        print('No modules found matching the specified filters.')
        return

    if verbose:
        print(f'Plotting efficiency for {len(all_modules)} module(s)...')

    efficiency_series = {}
    t80_dates = {}          # always collected; written to sidecar JSON

    for module in all_modules:
        try:
            dp = pa.daily_performance(module)
            # Keep full series (including NaN) for the CSV; drop NaN for the plot
            eff_full = dp['efficiency'] * 100        # fraction → %

            # Collect T80 date (None if not yet reached)
            # str(t80_date) works for both datetime.date and datetime.datetime
            summary = pa.summary_info(module)
            t80_date = summary.get('t80_date')
            t80_ts = pd.Timestamp(str(t80_date)) if t80_date is not None else None
            t80_dates[module] = t80_ts.date().isoformat() if t80_ts is not None else None

            # Optionally truncate to pre-T80 data
            if pre_t80 and t80_ts is not None:
                if isinstance(eff_full.index, pd.DatetimeIndex):
                    if eff_full.index.tz is not None and t80_ts.tz is None:
                        t80_ts = t80_ts.tz_localize(eff_full.index.tz)
                    cutoff = t80_ts
                else:
                    cutoff = t80_ts.date()
                eff_full = eff_full.loc[:cutoff]

            eff_plot = eff_full.dropna()
            if eff_plot.empty:
                if verbose:
                    print(f'  {module}: no valid data, skipped')
                continue
            efficiency_series[module] = eff_full
            if verbose:
                t80_str = t80_dates[module] or 'not reached'
                print(f'  {module}: {len(eff_plot)} valid days  (T80: {t80_str})')
        except Exception as exc:
            if verbose:
                print(f'  {module}: skipped ({exc})')

    # Always save sidecar T80 JSON so standalone scripts can use it
    if t80_dates:
        import json
        t80_json_path = Path(output_path).with_name(
            Path(output_path).stem + '_t80.json'
        )
        with open(t80_json_path, 'w') as _f:
            json.dump(t80_dates, _f, indent=2)
        if verbose:
            print(f'Saved: {t80_json_path}')

    if use_plotly:
        try:
            import plotly.graph_objects as go
        except ImportError:
            raise ImportError(
                'plotly is required for --plotly. Install it with: pip install plotly'
            )

        fig_pl = go.Figure()
        for module, eff_full in efficiency_series.items():
            eff_plot = eff_full.dropna()
            fig_pl.add_trace(go.Scatter(
                x=eff_plot.index,
                y=eff_plot.values,
                mode='lines',
                name=module,
                line=dict(width=1, color='black'),
                hovertemplate='%{x|%Y-%m-%d}<br>Efficiency: %{y:.2f}%<br>Module: ' + module + '<extra></extra>',
            ))
        fig_pl.update_layout(
            title='PACT Daily Module Efficiency',
            xaxis_title='Date',
            yaxis_title='Daily Efficiency (%)',
            showlegend=False,
            hovermode='closest',
        )
        fig_pl.write_html(output_path)
        if verbose:
            print(f'Saved: {output_path}')

    else:
        fig, ax = plt.subplots(figsize=(14, 6))
        for module, eff_full in efficiency_series.items():
            eff_plot = eff_full.dropna()
            ax.plot(eff_plot.index, eff_plot.values, linewidth=0.8, color='black')

        ax.set_xlabel('Date')
        ax.set_ylabel('Daily Efficiency (%)')
        ax.set_title('PACT Daily Module Efficiency')
        ax.grid(True, alpha=0.3)

        fig.tight_layout()
        fig.savefig(output_path, bbox_inches='tight', dpi=150)
        plt.close(fig)
        if verbose:
            print(f'Saved: {output_path}')

    # Save CSV table: one row per date, one column per module
    if efficiency_series:
        csv_path = Path(output_path).with_suffix('.csv')
        table = pd.DataFrame(efficiency_series)
        table.index.name = 'date'
        table.to_csv(csv_path)
        if verbose:
            print(f'Saved: {csv_path}')


def update_batch_month(cfg, batch, year, month, upload_s3=True, verbose=True):
    """Run update_module_month for every active module in *batch*.

    Parameters
    ----------
    cfg : dict
        Loaded pact_config.json.
    batch : str
        Batch prefix, e.g. 'P-0042' or 'P-0042-XX' (the -XX suffix is ignored).
    year, month : int
    upload_s3 : bool
    verbose : bool
    """
    # Normalise 'P-0042-XX' → 'P-0042'
    batch_prefix = batch[:6]

    modules_df = registry.read_modules(cfg)
    active = modules_df[
        (modules_df['Active'] == 'Y') &
        (modules_df['PACT_id'].str.startswith(batch_prefix))
    ]

    if active.empty:
        print(f'No active modules found for batch {batch_prefix}.')
        return

    print(f'Batch {batch_prefix}: {len(active)} active module(s) — '
          f'{", ".join(active["PACT_id"].tolist())}')

    errors = []
    for _, row in active.iterrows():
        pact_id = row['PACT_id']
        try:
            update_module_month(cfg, pact_id, year, month,
                                upload_s3=upload_s3, verbose=verbose)
        except Exception as exc:
            print(f'[{pact_id}] ERROR: {exc}')
            errors.append((pact_id, exc))

    if errors:
        print(f'\n{len(errors)} module(s) failed:')
        for pid, exc in errors:
            print(f'  {pid}: {exc}')
    else:
        print(f'\nBatch {batch_prefix}: all modules completed successfully.')


def update_all_month(cfg, year, month, upload_s3=True, verbose=True):
    """Run update_module_month for every active module in the setup CSV.

    Parameters
    ----------
    cfg : dict
        Loaded pact_config.json.
    year, month : int
    upload_s3 : bool
    verbose : bool
    """
    modules_df = registry.read_modules(cfg)
    active = modules_df[modules_df['Active'] == 'Y']

    if active.empty:
        print('No active modules found in setup CSV.')
        return

    print(f'Updating {len(active)} active module(s) for '
          f'{year}-{month:02d}: {", ".join(active["PACT_id"].tolist())}')

    errors = []
    for _, row in active.iterrows():
        pact_id = row['PACT_id']
        try:
            update_module_month(cfg, pact_id, year, month,
                                upload_s3=upload_s3, verbose=verbose)
        except Exception as exc:
            print(f'[{pact_id}] ERROR: {exc}')
            errors.append((pact_id, exc))

    if errors:
        print(f'\n{len(errors)} module(s) failed:')
        for pid, exc in errors:
            print(f'  {pid}: {exc}')
    else:
        print(f'\nAll {len(active)} module(s) completed successfully.')
