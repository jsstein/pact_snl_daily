"""Module registry: reads/writes the setup CSV and JSON metadata files."""

import csv
import json
from pathlib import Path

import pandas as pd

from .config import (
    get_base_path,
    get_batch_dir,
    get_censored_days_csv_path,
    get_metadata_dir,
    get_module_metadata_path,
    get_setup_csv_path,
    get_site_cfg,
    get_site_metadata_path,
)


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def read_modules(cfg):
    """Read PACT_SNL_Outdoor_Modules_SETUP.csv and return a DataFrame.

    All columns are strings; missing cells are empty strings.
    """
    path = get_setup_csv_path(cfg)
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def write_modules(df, cfg):
    """Write the modules DataFrame back to PACT_SNL_Outdoor_Modules_SETUP.csv."""
    path = get_setup_csv_path(cfg)
    df.to_csv(path, index=False)


def read_censored_days_csv(cfg):
    """Read PACT_SNL_censored_days_SETUP.csv and return a DataFrame."""
    path = get_censored_days_csv_path(cfg)
    return pd.read_csv(path, dtype=str, keep_default_na=False)


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _parse_date(date_str):
    """Parse a date string and return a datetime.date."""
    return pd.to_datetime(date_str).date()


def _format_date_mdy(date_str):
    """Format a date as M/D/YY (no leading zeros, 2-digit year) for CSV storage."""
    d = _parse_date(date_str)
    return f'{d.month}/{d.day}/{d.strftime("%y")}'


def _format_date_iso(date_str):
    """Format a date as YYYY-MM-DD for JSON storage."""
    return _parse_date(date_str).isoformat()


# ---------------------------------------------------------------------------
# Site helpers
# ---------------------------------------------------------------------------

def _lookup_site_key(cfg, pact_id):
    """Return the site key for a given module by looking it up in the setup CSV."""
    df = read_modules(cfg)
    rows = df[df['PACT_id'] == pact_id]
    if rows.empty:
        raise ValueError(f'{pact_id} not found in setup CSV')
    site_key = rows.iloc[0].get('Site', 'SNL')
    return site_key if site_key else 'SNL'


# ---------------------------------------------------------------------------
# Module management
# ---------------------------------------------------------------------------

def add_module(cfg, pact_id, psel_id, area, module_type, start_date,
               site='SNL', notes=''):
    """Add a new module to the setup CSV, create directories, and update metadata.

    Parameters
    ----------
    pact_id : str, e.g. 'P-0150-01'
    psel_id : int
    area : float, module area in m²
    module_type : str, e.g. 'MHP'
    start_date : str, parseable date (YYYY-MM-DD or M/D/YY)
    site : str, key from pact_config.json sites dict (default 'SNL')
    notes : str, optional
    """
    # Validate site key before touching any files
    get_site_cfg(cfg, site)

    df = read_modules(cfg)
    if pact_id in df['PACT_id'].values:
        raise ValueError(f'{pact_id} already exists in the setup CSV')

    start_str = _format_date_mdy(start_date)
    new_row = {
        'Start_date': start_str,
        'End_date': '',
        'PACT_id': pact_id,
        'PSEL_id': str(psel_id),
        'Area': str(area),
        'Active': 'Y',
        'Type': module_type,
        'Site': site,
        'Notes': notes,
    }
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    write_modules(df, cfg)
    print(f'{pact_id}: Added to setup CSV (Start={start_str}, Site={site})')

    batch = pact_id[:6]
    create_directory_tree(cfg, batch, site)
    _add_module_to_metadata_json(cfg, pact_id, float(area), module_type, site)
    _ensure_site_metadata(cfg, batch, site)


def retire_module(cfg, pact_id, end_date):
    """Set Active=N and add End_date for an active module in the setup CSV."""
    df = read_modules(cfg)
    mask = (df['PACT_id'] == pact_id) & (df['Active'] == 'Y')
    if not mask.any():
        raise ValueError(f'No active module with ID {pact_id} found in setup CSV')

    end_str = _format_date_mdy(end_date)
    df.loc[mask, 'Active'] = 'N'
    df.loc[mask, 'End_date'] = end_str
    write_modules(df, cfg)
    print(f'{pact_id}: Retired (End_date={end_str})')


def list_modules(cfg, active_only=True):
    """Return a DataFrame of modules, optionally filtered to active only."""
    df = read_modules(cfg)
    if active_only:
        df = df[df['Active'] == 'Y'].copy()
    return df


def create_directory_tree(cfg, batch, site_key):
    """Create the standard directory tree for a batch under base_path."""
    batch_dir = get_batch_dir(cfg, batch, site_key)
    for subdir in ('data/metadata', 'data/point-data', 'data/iv-data'):
        target = batch_dir / subdir
        target.mkdir(parents=True, exist_ok=True)
        print(f'  Created: {target}')


# ---------------------------------------------------------------------------
# Censoring
# ---------------------------------------------------------------------------

def add_censor(cfg, pact_id, start, end, comment):
    """Add a censoring condition and record it in the censored days CSV.

    If pact_id is 'site', adds the condition to every module that was active
    during the censored period (start..end inclusive).
    """
    # Append to the censored days CSV first (source of truth)
    _append_censored_days_csv(cfg, pact_id, start, end, comment)

    censor = {'start': start, 'end': end, 'comment': comment}

    if pact_id == 'site':
        df = read_modules(cfg)
        c_start = pd.Timestamp(start)
        c_end = pd.Timestamp(end)
        count = 0
        for _, row in df.iterrows():
            m_start = pd.Timestamp(row['Start_date']) if row['Start_date'] else None
            m_end = pd.Timestamp(row['End_date']) if row['End_date'] else pd.Timestamp.max
            if m_start is None:
                continue
            if m_start <= c_end and m_end >= c_start:
                site_key = row.get('Site', 'SNL') or 'SNL'
                _add_censor_to_metadata(cfg, row['PACT_id'], censor, site_key)
                count += 1
        print(f'site: Applied censor to {count} modules active during {start}–{end}')
    else:
        site_key = _lookup_site_key(cfg, pact_id)
        _add_censor_to_metadata(cfg, pact_id, censor, site_key)


# ---------------------------------------------------------------------------
# Snow days
# ---------------------------------------------------------------------------

def add_snow_day(cfg, date):
    """Add a snow day (ISO date) to site-metadata.json for all existing batches.

    Iterates over all unique outdoor directories across configured sites.
    """
    date_iso = _format_date_iso(date)
    base = get_base_path(cfg)
    outdoor_dirs = {s['outdoor_directory'] for s in cfg['sites'].values()}
    updated = 0
    for outdoor_dir in sorted(outdoor_dirs):
        for batch_dir in sorted(base.glob('P-????-XX')):
            site_path = batch_dir / outdoor_dir / 'data' / 'metadata' / 'site-metadata.json'
            if not site_path.exists():
                continue
            with open(site_path) as f:
                data = json.load(f)
            if date_iso in data['snow_days']:
                continue
            data['snow_days'].append(date_iso)
            data['snow_days'].sort()
            with open(site_path, 'w') as f:
                json.dump(data, f, indent=4)
            print(f'{batch_dir.name}: Added snow day {date_iso}')
            updated += 1
    if updated == 0:
        print(f'No site-metadata.json files found (or {date_iso} already present in all)')


# ---------------------------------------------------------------------------
# Indoor periods
# ---------------------------------------------------------------------------

def add_indoor(cfg, pact_id, start, end, comment):
    """Add an indoor period to a module's days_indoors in module-metadata.json."""
    batch = pact_id[:6]
    site_key = _lookup_site_key(cfg, pact_id)
    meta_path = get_module_metadata_path(cfg, batch, site_key)
    if not meta_path.exists():
        raise FileNotFoundError(f'module-metadata.json not found: {meta_path}')

    with open(meta_path) as f:
        data = json.load(f)

    indoor = {'start': start, 'end': end, 'comment': comment}
    for mod in data:
        if mod['module_id'] == pact_id:
            if indoor in mod['days_indoors']:
                print(f'{pact_id}: Indoor period already exists')
                return
            mod['days_indoors'].append(indoor)
            with open(meta_path, 'w') as f:
                json.dump(data, f, indent=4)
            print(f'{pact_id}: Added indoor period {start} to {end}')
            return

    raise ValueError(f'{pact_id} not found in {meta_path}')


# ---------------------------------------------------------------------------
# Sync metadata
# ---------------------------------------------------------------------------

def sync_metadata(cfg):
    """Idempotently regenerate all JSON metadata files from the setup CSVs.

    For each batch group:
    - Rebuilds module-metadata.json from the setup CSV
    - Preserves existing days_indoors (not stored in any CSV)
    - Rebuilds days_censored entirely from the censored days CSV
    - Creates site-metadata.json if absent
    """
    df = read_modules(cfg)
    censored_df = read_censored_days_csv(cfg)

    # Group modules by batch (first 6 chars of PACT_id, e.g. 'P-0042')
    batches = {}
    for _, row in df.iterrows():
        batch = row['PACT_id'][:6]
        batches.setdefault(batch, []).append(row)

    for batch, rows in batches.items():
        # All modules in a batch share the same site; take it from the first row
        site_key = rows[0].get('Site', 'SNL') or 'SNL'
        meta_path = get_module_metadata_path(cfg, batch, site_key)

        # Preserve existing days_indoors (not tracked in any CSV)
        existing_indoors = {}
        if meta_path.exists():
            with open(meta_path) as f:
                for mod in json.load(f):
                    existing_indoors[mod['module_id']] = mod.get('days_indoors', [])

        # Build the module list and per-module active date ranges
        modules = []
        module_dates = {}
        for row in rows:
            pact_id = row['PACT_id']
            mod = {
                'module_id': pact_id,
                'module_area': float(row['Area']),
                'module_type': row['Type'],
                'days_indoors': existing_indoors.get(pact_id, []),
                'days_censored': [],
            }
            modules.append(mod)
            m_start = pd.Timestamp(row['Start_date']) if row['Start_date'] else pd.Timestamp.min
            m_end = pd.Timestamp(row['End_date']) if row['End_date'] else pd.Timestamp.max
            module_dates[pact_id] = (m_start, m_end)

        mod_by_id = {m['module_id']: m for m in modules}

        # Populate days_censored from the censored days CSV
        for _, crow in censored_df.iterrows():
            cpact_id = crow['pact_id']
            censor = {
                'start': crow['start'],
                'end': crow['end'],
                'comment': crow['comment'],
            }

            if cpact_id != 'site':
                if cpact_id in mod_by_id:
                    if censor not in mod_by_id[cpact_id]['days_censored']:
                        mod_by_id[cpact_id]['days_censored'].append(censor)
            else:
                # Site-wide: apply only to modules active during the censor window
                c_start = pd.Timestamp(crow['start'])
                c_end = pd.Timestamp(crow['end'])
                for mid, (m_start, m_end) in module_dates.items():
                    if m_start <= c_end and m_end >= c_start:
                        if censor not in mod_by_id[mid]['days_censored']:
                            mod_by_id[mid]['days_censored'].append(censor)

        # Write the metadata file
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        with open(meta_path, 'w') as f:
            json.dump(modules, f, indent=4)

        _ensure_site_metadata(cfg, batch, site_key)
        print(f'{batch} ({site_key}): synced {len(modules)} modules → {meta_path}')


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _add_module_to_metadata_json(cfg, pact_id, area, module_type, site_key):
    """Append a module entry to module-metadata.json if not already present."""
    batch = pact_id[:6]
    meta_path = get_module_metadata_path(cfg, batch, site_key)

    if meta_path.exists():
        with open(meta_path) as f:
            data = json.load(f)
    else:
        data = []

    for mod in data:
        if mod['module_id'] == pact_id:
            print(f'{pact_id}: Already in module-metadata.json')
            return

    data.append({
        'module_id': pact_id,
        'module_area': area,
        'module_type': module_type,
        'days_indoors': [],
        'days_censored': [],
    })
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    with open(meta_path, 'w') as f:
        json.dump(data, f, indent=4)
    print(f'{pact_id}: Added to module-metadata.json')


def _ensure_site_metadata(cfg, batch, site_key):
    """Create site-metadata.json for a batch if it does not already exist."""
    site = get_site_cfg(cfg, site_key)
    site_path = get_site_metadata_path(cfg, batch, site_key)
    if site_path.exists():
        return
    site_path.parent.mkdir(parents=True, exist_ok=True)
    # surface_tilt/azimuth: store as "null" string for tracker (matching existing
    # format), or as a number for fixed-tilt.
    tilt = 'null' if site['surface_tilt'] is None else site['surface_tilt']
    azimuth = 'null' if site['surface_azimuth'] is None else site['surface_azimuth']
    data = {
        'location': {
            'label': site['label'],
            'latitude': site['latitude'],
            'longitude': site['longitude'],
            'elevation': site['elevation'],
            'surface_tilt': tilt,
            'surface_azimuth': azimuth,
        },
        'snow_days': [],
    }
    with open(site_path, 'w') as f:
        json.dump(data, f, indent=4)
    print(f'{batch}: Created site-metadata.json (site={site_key})')


def _add_censor_to_metadata(cfg, pact_id, censor, site_key):
    """Add a censor dict to a module entry in module-metadata.json (no-op if duplicate)."""
    batch = pact_id[:6]
    meta_path = get_module_metadata_path(cfg, batch, site_key)
    if not meta_path.exists():
        print(f'{pact_id}: module-metadata.json not found at {meta_path}, skipping')
        return

    with open(meta_path) as f:
        data = json.load(f)

    found = False
    for mod in data:
        if mod['module_id'] == pact_id:
            found = True
            for existing in mod['days_censored']:
                if (existing['start'] == censor['start']
                        and existing['end'] == censor['end']
                        and existing['comment'] == censor['comment']):
                    print(f'{pact_id}: Censor condition already exists')
                    return
            mod['days_censored'].append(censor)
            break

    if not found:
        print(f'{pact_id}: Not found in module-metadata.json, skipping')
        return

    with open(meta_path, 'w') as f:
        json.dump(data, f, indent=4)
    print(f'{pact_id}: Censor condition added')


def _append_censored_days_csv(cfg, pact_id, start, end, comment):
    """Append one row to the censored days CSV."""
    path = get_censored_days_csv_path(cfg)
    with open(path, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([pact_id, start, end, comment])
    print(f'Appended to censored days CSV: {pact_id} {start}–{end}')
