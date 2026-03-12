"""Module registry: reads from the PVGrid database; writes to DB + CSV backup."""

import csv
import json
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

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
# DB helpers
# ---------------------------------------------------------------------------

def _make_engine(cfg, write=False):
    """Create a SQLAlchemy engine from pact_config settings.

    Parameters
    ----------
    write : bool
        If False (default), connect as PVGridUser (read-only).
        If True, connect as PVGridMaster (INSERT/UPDATE/DELETE).
    """
    if write:
        username = cfg.get('db_master_username', 'PVGridMaster')
        password = os.getenv('DB_PASSWORD_PRmaster')
        if password is None:
            raise EnvironmentError(
                'DB_PASSWORD_PRmaster environment variable is not set.'
            )
    else:
        username = cfg.get('db_username', 'PVGridUser')
        password = os.getenv('DB_PASSWORD_PRuser')
        if password is None:
            raise EnvironmentError(
                'DB_PASSWORD_PRuser environment variable is not set.'
            )
    server   = cfg['db_server']
    database = cfg['db_name']
    return create_engine(
        f'mssql+pyodbc://{username}:{password}@{server}/{database}'
        f'?driver=ODBC+Driver+17+for+SQL+Server'
    )


def _date_to_iso(val):
    """Convert a DB date value (datetime.date, Timestamp, NaT, None) to ISO string."""
    if val is None or not pd.notna(val):
        return ''
    return pd.Timestamp(val).date().isoformat()


# ---------------------------------------------------------------------------
# Read helpers  (DB is source of truth)
# ---------------------------------------------------------------------------

def read_modules(cfg):
    """Read pact_modules from the database and return a DataFrame.

    Column names match the original CSV for backward compatibility:
    PACT_id, PSEL_id, Area, Type, Site, Start_date, End_date, Active, Notes.
    Dates are ISO strings (YYYY-MM-DD); missing end dates are empty strings.
    """
    engine = _make_engine(cfg)
    df = pd.read_sql(
        text('SELECT pact_id, psel_id, area, module_type, site, '
             'start_date, end_date, active, notes '
             'FROM dbo.pact_modules ORDER BY module_id'),
        engine,
    )
    df = df.rename(columns={
        'pact_id':     'PACT_id',
        'psel_id':     'PSEL_id',
        'area':        'Area',
        'module_type': 'Type',
        'site':        'Site',
        'start_date':  'Start_date',
        'end_date':    'End_date',
        'active':      'Active',
        'notes':       'Notes',
    })
    df['Start_date'] = df['Start_date'].apply(_date_to_iso)
    df['End_date']   = df['End_date'].apply(_date_to_iso)
    df['PSEL_id']    = df['PSEL_id'].astype(str)
    df['Area']       = df['Area'].astype(str)
    df['Notes']      = df['Notes'].fillna('')
    return df


def read_censored_days(cfg):
    """Read pact_censored_days from the database and return a DataFrame.

    Columns: pact_id, start, end, comment  (match original CSV column names).
    Dates are ISO strings (YYYY-MM-DD).
    """
    engine = _make_engine(cfg)
    df = pd.read_sql(
        text('SELECT pact_id, start_date, end_date, comment '
             'FROM dbo.pact_censored_days ORDER BY censor_id'),
        engine,
    )
    df = df.rename(columns={'start_date': 'start', 'end_date': 'end'})
    df['start'] = df['start'].apply(_date_to_iso)
    df['end']   = df['end'].apply(_date_to_iso)
    return df


# Backward-compatible alias (external code that called read_censored_days_csv still works)
def read_censored_days_csv(cfg):
    return read_censored_days(cfg)


# ---------------------------------------------------------------------------
# CSV backup helpers
# ---------------------------------------------------------------------------

def write_modules(df, cfg):
    """Write the modules DataFrame to the CSV backup file."""
    path = get_setup_csv_path(cfg)
    df.to_csv(path, index=False)


def _append_censored_days_csv(cfg, pact_id, start, end, comment):
    """Append one row to the censored days CSV backup."""
    path = get_censored_days_csv_path(cfg)
    with open(path, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([pact_id, start, end, comment])
    print(f'CSV backup: appended {pact_id} {start}–{end}')


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _parse_date(date_str):
    """Parse a date string and return a datetime.date."""
    return pd.to_datetime(date_str).date()


def _format_date_iso(date_str):
    """Format a date as YYYY-MM-DD."""
    return _parse_date(date_str).isoformat()


# ---------------------------------------------------------------------------
# Site helpers
# ---------------------------------------------------------------------------

def _lookup_site_key(cfg, pact_id):
    """Return the site key for a given module from the database."""
    df = read_modules(cfg)
    rows = df[df['PACT_id'] == pact_id]
    if rows.empty:
        raise ValueError(f'{pact_id} not found in pact_modules')
    site_key = rows.iloc[0].get('Site', 'SNL')
    return site_key if site_key else 'SNL'


# ---------------------------------------------------------------------------
# Module management
# ---------------------------------------------------------------------------

_VALID_SITES = ('SNL', 'SNL_fixed-tilt')


def add_module(cfg, pact_id, psel_id, area, module_type, start_date,
               site, notes=''):
    """Add a new module to pact_modules (DB + CSV backup), create directories,
    and update metadata.

    Parameters
    ----------
    pact_id : str, e.g. 'P-0150-01'
    psel_id : int
    area : float, module area in m²
    module_type : str, e.g. 'MHP'
    start_date : str, parseable date (YYYY-MM-DD or M/D/YY)
    site : str, must be 'SNL' or 'SNL_fixed-tilt'
    notes : str, optional
    """
    if site not in _VALID_SITES:
        raise ValueError(
            f"Invalid site {site!r}. Must be one of: {', '.join(_VALID_SITES)}"
        )
    get_site_cfg(cfg, site)

    df = read_modules(cfg)
    if pact_id in df['PACT_id'].values:
        raise ValueError(f'{pact_id} already exists in pact_modules')

    start_iso = _format_date_iso(start_date)

    # --- DB insert ---
    engine = _make_engine(cfg, write=True)
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO dbo.pact_modules
                    (pact_id, psel_id, area, module_type, site,
                     start_date, active, notes)
                VALUES
                    (:pact_id, :psel_id, :area, :module_type, :site,
                     :start_date, 'Y', :notes)
            """),
            {
                'pact_id':     pact_id,
                'psel_id':     int(psel_id),
                'area':        float(area),
                'module_type': module_type,
                'site':        site,
                'start_date':  _parse_date(start_date),
                'notes':       notes or '',
            },
        )
    print(f'{pact_id}: Added to pact_modules (Start={start_iso}, Site={site})')

    # --- CSV backup ---
    new_row = {
        'Start_date': start_iso,
        'End_date':   '',
        'PACT_id':    pact_id,
        'PSEL_id':    str(psel_id),
        'Area':       str(area),
        'Active':     'Y',
        'Type':       module_type,
        'Site':       site,
        'Notes':      notes,
    }
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    write_modules(df, cfg)
    print(f'{pact_id}: Added to CSV backup')

    batch = pact_id[:6]
    create_directory_tree(cfg, batch, site)
    _add_module_to_metadata_json(cfg, pact_id, float(area), module_type, site)
    _ensure_site_metadata(cfg, batch, site)


def retire_module(cfg, pact_id, end_date):
    """Set active=N and end_date for an active module in pact_modules (DB + CSV backup)."""
    df = read_modules(cfg)
    mask = (df['PACT_id'] == pact_id) & (df['Active'] == 'Y')
    if not mask.any():
        raise ValueError(f'No active module with ID {pact_id} found in pact_modules')

    end_iso = _format_date_iso(end_date)

    # --- DB update ---
    engine = _make_engine(cfg, write=True)
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE dbo.pact_modules
                SET active = 'N', end_date = :end_date
                WHERE pact_id = :pact_id AND active = 'Y'
            """),
            {'pact_id': pact_id, 'end_date': _parse_date(end_date)},
        )
    print(f'{pact_id}: Retired in pact_modules (End_date={end_iso})')

    # --- CSV backup ---
    df.loc[mask, 'Active']   = 'N'
    df.loc[mask, 'End_date'] = end_iso
    write_modules(df, cfg)
    print(f'{pact_id}: Updated in CSV backup')


def delete_module(cfg, pact_id, purge=False):
    """Permanently remove a module from pact_modules (DB + CSV backup) and
    module-metadata.json.

    Parameters
    ----------
    pact_id : str, e.g. 'P-0150-01'
    purge : bool
        If True and no modules remain in the batch after deletion, remove the
        entire batch directory tree from Box Sync.  Default False.
    """
    import shutil

    df = read_modules(cfg)
    mask = df['PACT_id'] == pact_id
    if not mask.any():
        raise ValueError(f'{pact_id} not found in pact_modules')

    site_key = df.loc[mask, 'Site'].iloc[0] or 'SNL'
    batch = pact_id[:6]

    # --- DB delete ---
    engine = _make_engine(cfg, write=True)
    with engine.begin() as conn:
        conn.execute(
            text('DELETE FROM dbo.pact_modules WHERE pact_id = :pact_id'),
            {'pact_id': pact_id},
        )
    print(f'{pact_id}: Removed from pact_modules')

    # --- CSV backup ---
    write_modules(df[~mask].reset_index(drop=True), cfg)
    print(f'{pact_id}: Removed from CSV backup')

    # --- module-metadata.json ---
    meta_path = get_module_metadata_path(cfg, batch, site_key)
    if meta_path.exists():
        with open(meta_path) as f:
            data = json.load(f)
        remaining = [m for m in data if m['module_id'] != pact_id]
        if len(remaining) < len(data):
            with open(meta_path, 'w') as f:
                json.dump(remaining, f, indent=4)
            print(f'{pact_id}: Removed from {meta_path}')
        else:
            print(f'{pact_id}: Not found in {meta_path}')
    else:
        print(f'{pact_id}: module-metadata.json not found at {meta_path}')

    # --- optional directory purge ---
    if purge:
        batch_still_has_modules = df[~mask]['PACT_id'].str.startswith(batch).any()
        if batch_still_has_modules:
            print(f'{pact_id}: --purge skipped — other modules remain in batch {batch}')
        else:
            batch_dir = get_batch_dir(cfg, batch, site_key)
            batch_root = batch_dir.parent
            if batch_root.exists():
                shutil.rmtree(batch_root)
                print(f'{pact_id}: Purged directory tree {batch_root}')
            else:
                print(f'{pact_id}: Directory not found, nothing to purge ({batch_root})')


def list_modules(cfg, active_only=True):
    """Return a DataFrame of modules from the database, optionally active only."""
    df = read_modules(cfg)
    if active_only:
        df = df[df['Active'] == 'Y'].copy()
    return df


def add_modules_bulk(cfg, pact_id_start, pact_id_end, psel_id_start,
                     area, module_type, start_date, site, notes='',
                     add_to_db25=False, db25_env='DV',
                     module_model_id=None, source=None, module_owner=None,
                     date_received=None):
    """Add a consecutive range of modules that share all parameters except
    pact_id and psel_id.

    pact_id_start and pact_id_end must share the same batch prefix and differ
    only in their numeric suffix, e.g. 'P-0150-01' .. 'P-0150-10'.
    psel_id_start is assigned to the first module; each subsequent module
    gets psel_id_start + offset.

    Parameters
    ----------
    pact_id_start : str, e.g. 'P-0150-01'
    pact_id_end   : str, e.g. 'P-0150-10'
    psel_id_start : int
    area, module_type, start_date, site, notes : same as add_module
    add_to_db25   : bool — if True, also insert each module into db25_modules
    db25_env      : str — 'DV' or 'PR' (default 'DV')
    module_model_id : int, optional — FK to db25_module_models
    source        : str, optional
    module_owner  : str, optional
    date_received : str, optional — defaults to start_date
    """
    # Parse prefix and numeric range from pact_id_start / pact_id_end
    prefix_start, _, suffix_start = pact_id_start.rpartition('-')
    prefix_end,   _, suffix_end   = pact_id_end.rpartition('-')
    if prefix_start != prefix_end:
        raise ValueError(
            f'pact_id_start and pact_id_end must share the same prefix '
            f'({prefix_start!r} != {prefix_end!r})'
        )
    try:
        n_start = int(suffix_start)
        n_end   = int(suffix_end)
    except ValueError:
        raise ValueError(
            f'Module suffixes must be integers: {suffix_start!r}, {suffix_end!r}'
        )
    if n_end < n_start:
        raise ValueError(
            f'pact_id_end ({pact_id_end}) must be >= pact_id_start ({pact_id_start})'
        )

    # --- set up db25 once before the loop ---
    db25_ready = False
    if add_to_db25:
        try:
            from db25.cli import get_conn_str, resolve_paths, TABLES
            from db25.functions import (
                generate_sql_from_json,
                backup_csv_before_sync,
                insert_record_with_csv_sync,
            )
            csv_path, json_path = resolve_paths(None, None)
            json_file = os.path.join(json_path, 'modules.json')
            if not os.path.exists(json_file):
                print(f'WARNING: modules.json not found at {json_file} — skipping db25 inserts')
            else:
                _, type_dict = generate_sql_from_json(json_file)
                conn_str = get_conn_str(db25_env)
                backup_csv_before_sync(csv_path, 'modules')
                db25_ready = True
        except ImportError as exc:
            print(f'WARNING: Could not import db25 — skipping db25 inserts: {exc}')

    width = len(suffix_start)  # preserve zero-padding width
    results = []
    for offset, n in enumerate(range(n_start, n_end + 1)):
        pact_id = f'{prefix_start}-{n:0{width}d}'
        psel_id = psel_id_start + offset
        try:
            add_module(cfg, pact_id=pact_id, psel_id=psel_id, area=area,
                       module_type=module_type, start_date=start_date,
                       site=site, notes=notes)
            result_line = f'✓ {pact_id} (psel_id={psel_id})'

            if db25_ready:
                record = {'psel_id': psel_id, 'module_alt_id': pact_id}
                if module_model_id is not None:
                    record['module_model_id'] = module_model_id
                if source is not None:
                    record['source'] = source
                if module_owner is not None:
                    record['module_owner'] = module_owner
                record['date_received'] = date_received or start_date
                db25_result = insert_record_with_csv_sync(
                    table_name='modules',
                    record_dict=record,
                    conn_str=conn_str,
                    csv_path=csv_path,
                    type_dict=type_dict,
                    auto_increment_col=TABLES['modules'],
                )
                if db25_result['success']:
                    result_line += f' | db25 module_id={db25_result["new_id"]}'
                else:
                    result_line += f' | db25 ERROR: {db25_result["message"]}'

            results.append(result_line)
        except Exception as exc:
            results.append(f'✗ {pact_id}: {exc}')

    print('\n'.join(results))


def update_module(cfg, pact_id, area=None, module_type=None, psel_id=None,
                  site=None, start_date=None, notes=None):
    """Update one or more fields on an existing module in pact_modules (DB + CSV backup).

    Only the fields explicitly passed (not None) are changed.

    Parameters
    ----------
    pact_id : str, e.g. 'P-0150-01'
    area : float, optional
    module_type : str, optional
    psel_id : int, optional
    site : str, optional — must be 'SNL' or 'SNL_fixed-tilt'
    start_date : str, optional — parseable date (YYYY-MM-DD)
    notes : str, optional
    """
    updates = {}
    if area        is not None: updates['area']        = float(area)
    if module_type is not None: updates['module_type'] = module_type
    if psel_id     is not None: updates['psel_id']     = int(psel_id)
    if notes       is not None: updates['notes']       = notes
    if start_date  is not None: updates['start_date']  = _parse_date(start_date)
    if site        is not None:
        if site not in _VALID_SITES:
            raise ValueError(
                f"Invalid site {site!r}. Must be one of: {', '.join(_VALID_SITES)}"
            )
        updates['site'] = site

    if not updates:
        raise ValueError('No fields to update — supply at least one keyword argument.')

    # Verify the module exists
    df = read_modules(cfg)
    if pact_id not in df['PACT_id'].values:
        raise ValueError(f'{pact_id} not found in pact_modules')

    # --- DB update ---
    set_clause = ', '.join(f'{col} = :{col}' for col in updates)
    params = dict(updates, pact_id=pact_id)
    engine = _make_engine(cfg, write=True)
    with engine.begin() as conn:
        conn.execute(
            text(f'UPDATE dbo.pact_modules SET {set_clause} WHERE pact_id = :pact_id'),
            params,
        )
    changed = ', '.join(
        f'{k}={v}' for k, v in updates.items()
    )
    print(f'{pact_id}: Updated in pact_modules ({changed})')

    # --- CSV backup ---
    col_map = {
        'area': 'Area', 'module_type': 'Type', 'psel_id': 'PSEL_id',
        'notes': 'Notes', 'site': 'Site', 'start_date': 'Start_date',
    }
    mask = df['PACT_id'] == pact_id
    for db_col, value in updates.items():
        csv_col = col_map[db_col]
        df.loc[mask, csv_col] = (
            value.isoformat() if hasattr(value, 'isoformat') else str(value)
        )
    write_modules(df, cfg)
    print(f'{pact_id}: Updated in CSV backup')

    # --- sync area/type in module-metadata.json if changed ---
    if 'area' in updates or 'module_type' in updates:
        site_key = df.loc[mask, 'Site'].iloc[0] or 'SNL'
        batch = pact_id[:6]
        meta_path = get_module_metadata_path(cfg, batch, site_key)
        if meta_path.exists():
            with open(meta_path) as f:
                data = json.load(f)
            for mod in data:
                if mod['module_id'] == pact_id:
                    if 'area' in updates:
                        mod['module_area'] = updates['area']
                    if 'module_type' in updates:
                        mod['module_type'] = updates['module_type']
                    break
            with open(meta_path, 'w') as f:
                json.dump(data, f, indent=4)
            print(f'{pact_id}: Updated in module-metadata.json')


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
    """Add a censoring condition to pact_censored_days (DB + CSV backup).

    If pact_id is 'site', adds the condition to every module that was active
    during the censored period (start..end inclusive).
    """
    # --- DB insert ---
    engine = _make_engine(cfg, write=True)
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO dbo.pact_censored_days
                    (pact_id, start_date, end_date, comment)
                VALUES
                    (:pact_id, :start_date, :end_date, :comment)
            """),
            {
                'pact_id':    pact_id,
                'start_date': _parse_date(start),
                'end_date':   _parse_date(end),
                'comment':    comment,
            },
        )
    print(f'Added to pact_censored_days: {pact_id} {start}–{end}')

    # --- CSV backup ---
    _append_censored_days_csv(cfg, pact_id, start, end, comment)

    # --- update module-metadata.json ---
    censor = {'start': start, 'end': end, 'comment': comment}

    if pact_id == 'site':
        df = read_modules(cfg)
        c_start = pd.Timestamp(start)
        c_end   = pd.Timestamp(end)
        count = 0
        for _, row in df.iterrows():
            m_start = pd.Timestamp(row['Start_date']) if row['Start_date'] else None
            m_end   = pd.Timestamp(row['End_date']) if row['End_date'] else pd.Timestamp.max
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
    """Add a snow day (ISO date) to site-metadata.json for all existing batches."""
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
    batch    = pact_id[:6]
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
    """Idempotently regenerate all JSON metadata files from the database.

    For each batch group:
    - Rebuilds module-metadata.json from pact_modules
    - Preserves existing days_indoors (not yet in the DB)
    - Rebuilds days_censored entirely from pact_censored_days
    - Creates site-metadata.json if absent
    """
    df          = read_modules(cfg)
    censored_df = read_censored_days(cfg)

    batches = {}
    for _, row in df.iterrows():
        batch = row['PACT_id'][:6]
        batches.setdefault(batch, []).append(row)

    for batch, rows in batches.items():
        site_key  = rows[0].get('Site', 'SNL') or 'SNL'
        meta_path = get_module_metadata_path(cfg, batch, site_key)

        # Preserve existing days_indoors (not yet tracked in the DB)
        existing_indoors = {}
        if meta_path.exists():
            with open(meta_path) as f:
                for mod in json.load(f):
                    existing_indoors[mod['module_id']] = mod.get('days_indoors', [])

        modules      = []
        module_dates = {}
        for row in rows:
            pact_id = row['PACT_id']
            mod = {
                'module_id':   pact_id,
                'module_area': float(row['Area']),
                'module_type': row['Type'],
                'days_indoors':  existing_indoors.get(pact_id, []),
                'days_censored': [],
            }
            modules.append(mod)
            m_start = pd.Timestamp(row['Start_date']) if row['Start_date'] else pd.Timestamp.min
            m_end   = pd.Timestamp(row['End_date'])   if row['End_date']   else pd.Timestamp.max
            module_dates[pact_id] = (m_start, m_end)

        mod_by_id = {m['module_id']: m for m in modules}

        for _, crow in censored_df.iterrows():
            cpact_id = crow['pact_id']
            censor = {
                'start':   crow['start'],
                'end':     crow['end'],
                'comment': crow['comment'],
            }
            if cpact_id != 'site':
                if cpact_id in mod_by_id:
                    if censor not in mod_by_id[cpact_id]['days_censored']:
                        mod_by_id[cpact_id]['days_censored'].append(censor)
            else:
                c_start = pd.Timestamp(crow['start'])
                c_end   = pd.Timestamp(crow['end'])
                for mid, (m_start, m_end) in module_dates.items():
                    if m_start <= c_end and m_end >= c_start:
                        if censor not in mod_by_id[mid]['days_censored']:
                            mod_by_id[mid]['days_censored'].append(censor)

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
    batch     = pact_id[:6]
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
        'module_id':     pact_id,
        'module_area':   area,
        'module_type':   module_type,
        'days_indoors':  [],
        'days_censored': [],
    })
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    with open(meta_path, 'w') as f:
        json.dump(data, f, indent=4)
    print(f'{pact_id}: Added to module-metadata.json')


def _ensure_site_metadata(cfg, batch, site_key):
    """Create site-metadata.json for a batch if it does not already exist."""
    site      = get_site_cfg(cfg, site_key)
    site_path = get_site_metadata_path(cfg, batch, site_key)
    if site_path.exists():
        return
    site_path.parent.mkdir(parents=True, exist_ok=True)
    tilt    = 'null' if site['surface_tilt']    is None else site['surface_tilt']
    azimuth = 'null' if site['surface_azimuth'] is None else site['surface_azimuth']
    data = {
        'location': {
            'label':          site['label'],
            'latitude':       site['latitude'],
            'longitude':      site['longitude'],
            'elevation':      site['elevation'],
            'surface_tilt':   tilt,
            'surface_azimuth': azimuth,
        },
        'snow_days': [],
    }
    with open(site_path, 'w') as f:
        json.dump(data, f, indent=4)
    print(f'{batch}: Created site-metadata.json (site={site_key})')


def _add_censor_to_metadata(cfg, pact_id, censor, site_key):
    """Add a censor dict to a module entry in module-metadata.json (no-op if duplicate)."""
    batch     = pact_id[:6]
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
