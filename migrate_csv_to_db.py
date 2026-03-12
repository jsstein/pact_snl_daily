"""One-time migration: load the two setup CSVs into pact_modules and
pact_censored_days.

Usage
-----
    python3.10 migrate_csv_to_db.py

Prerequisites
-------------
- create_pact_tables.sql has already been run against PVGrid.
- DB_PASSWORD_PRuser environment variable is set.

The script is idempotent: if a pact_id already exists in pact_modules it
is skipped, so re-running is safe.
"""

import os
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

MODULES_CSV     = Path(__file__).parent / 'PACT_SNL_Outdoor_Modules_SETUP.csv'
CENSORED_CSV    = Path(__file__).parent / 'PACT_SNL_censored_days_SETUP.csv'

DB_SERVER   = r'DB03SNLNT\PR'
DB_NAME     = 'PVGrid'
DB_USERNAME = 'PVGridUser'


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_engine():
    password = os.getenv('DB_PASSWORD_PRuser')
    if not password:
        sys.exit('ERROR: DB_PASSWORD_PRuser environment variable is not set.')
    conn_str = (
        f'mssql+pyodbc://{DB_USERNAME}:{password}@{DB_SERVER}/{DB_NAME}'
        f'?driver=ODBC+Driver+17+for+SQL+Server'
    )
    return create_engine(conn_str)


def parse_date(date_str):
    """Return a datetime.date from M/D/YY, M/D/YYYY, or YYYY-MM-DD strings.
    Returns None for empty / missing values."""
    s = str(date_str).strip()
    if not s:
        return None
    return pd.to_datetime(s).date()


# ---------------------------------------------------------------------------
# Migrate modules
# ---------------------------------------------------------------------------

def migrate_modules(engine):
    print('\n--- pact_modules ---')
    df = pd.read_csv(MODULES_CSV, dtype=str, keep_default_na=False)

    # Fetch pact_ids already in the table so we can skip them on re-run
    with engine.connect() as conn:
        existing = {
            row[0] for row in conn.execute(
                text('SELECT pact_id FROM dbo.pact_modules')
            )
        }

    inserted = skipped = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            pact_id = row['PACT_id'].strip()
            if not pact_id:
                continue
            if pact_id in existing:
                print(f'  SKIP  {pact_id} (already exists)')
                skipped += 1
                continue

            start = parse_date(row['Start_date'])
            end   = parse_date(row['End_date'])

            conn.execute(
                text("""
                    INSERT INTO dbo.pact_modules
                        (pact_id, psel_id, area, module_type, site,
                         start_date, end_date, active, notes)
                    VALUES
                        (:pact_id, :psel_id, :area, :module_type, :site,
                         :start_date, :end_date, :active, :notes)
                """),
                {
                    'pact_id':      pact_id,
                    'psel_id':      int(row['PSEL_id']),
                    'area':         float(row['Area']),
                    'module_type':  row['Type'].strip(),
                    'site':         row['Site'].strip() or 'SNL',
                    'start_date':   start,
                    'end_date':     end,
                    'active':       row['Active'].strip() or 'N',
                    'notes':        row['Notes'].strip(),
                },
            )
            print(f'  INSERT {pact_id}')
            inserted += 1

    print(f'\npact_modules: {inserted} inserted, {skipped} skipped.')


# ---------------------------------------------------------------------------
# Migrate censored days
# ---------------------------------------------------------------------------

def migrate_censored_days(engine):
    print('\n--- pact_censored_days ---')
    df = pd.read_csv(CENSORED_CSV, dtype=str, keep_default_na=False)

    # Build the set of (pact_id, start_date, end_date) already present
    with engine.connect() as conn:
        existing = {
            (r[0], str(r[1]), str(r[2]))
            for r in conn.execute(
                text('SELECT pact_id, start_date, end_date FROM dbo.pact_censored_days')
            )
        }

    inserted = skipped = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            pact_id = row['pact_id'].strip()
            if not pact_id:
                continue

            start = parse_date(row['start'])
            end   = parse_date(row['end'])
            if start is None or end is None:
                print(f'  SKIP  {pact_id} (unparseable dates: {row["start"]!r}, {row["end"]!r})')
                skipped += 1
                continue

            key = (pact_id, str(start), str(end))
            if key in existing:
                print(f'  SKIP  {pact_id} {start}–{end} (already exists)')
                skipped += 1
                continue

            conn.execute(
                text("""
                    INSERT INTO dbo.pact_censored_days
                        (pact_id, start_date, end_date, comment)
                    VALUES
                        (:pact_id, :start_date, :end_date, :comment)
                """),
                {
                    'pact_id':    pact_id,
                    'start_date': start,
                    'end_date':   end,
                    'comment':    row['comment'].strip(),
                },
            )
            print(f'  INSERT {pact_id}  {start}–{end}')
            inserted += 1

    print(f'\npact_censored_days: {inserted} inserted, {skipped} skipped.')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    engine = make_engine()
    print(f'Connected to {DB_SERVER} / {DB_NAME}')
    migrate_modules(engine)
    migrate_censored_days(engine)
    print('\nMigration complete.')
