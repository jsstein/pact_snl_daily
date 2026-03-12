"""Create pact_modules and pact_censored_days tables in PVGrid.

Usage
-----
    python3.10 create_pact_tables.py

Prerequisites
-------------
- DB_PASSWORD_PRuser environment variable is set.
- The PVGrid login has CREATE TABLE permission in the dbo schema.
"""

import os
import sys

from sqlalchemy import create_engine, text


DB_SERVER   = r'DB03SNLNT\PR'
DB_NAME     = 'PVGrid'
DB_USERNAME = 'PVGridUser'


def make_engine():
    password = os.getenv('DB_PASSWORD_PRuser')
    if not password:
        sys.exit('ERROR: DB_PASSWORD_PRuser environment variable is not set.')
    conn_str = (
        f'mssql+pyodbc://{DB_USERNAME}:{password}@{DB_SERVER}/{DB_NAME}'
        f'?driver=ODBC+Driver+17+for+SQL+Server'
    )
    return create_engine(conn_str)


TABLES = {
    'dbo.pact_modules': """
        CREATE TABLE dbo.pact_modules (
            module_id   INT          IDENTITY(1,1) NOT NULL,
            pact_id     VARCHAR(30)  NOT NULL,
            psel_id     INT          NOT NULL,
            area        FLOAT        NOT NULL,
            module_type VARCHAR(50)  NOT NULL,
            site        VARCHAR(50)  NOT NULL  CONSTRAINT DF_pact_modules_site    DEFAULT 'SNL',
            start_date  DATE         NOT NULL,
            end_date    DATE             NULL,
            active      CHAR(1)      NOT NULL  CONSTRAINT DF_pact_modules_active  DEFAULT 'Y',
            notes       VARCHAR(500) NOT NULL  CONSTRAINT DF_pact_modules_notes   DEFAULT '',

            CONSTRAINT PK_pact_modules         PRIMARY KEY (module_id),
            CONSTRAINT UQ_pact_modules_pact_id UNIQUE      (pact_id),
            CONSTRAINT CK_pact_modules_active  CHECK       (active IN ('Y', 'N'))
        )
    """,
    'dbo.pact_censored_days': """
        CREATE TABLE dbo.pact_censored_days (
            censor_id  INT          IDENTITY(1,1) NOT NULL,
            pact_id    VARCHAR(30)  NOT NULL,
            start_date DATE         NOT NULL,
            end_date   DATE         NOT NULL,
            comment    VARCHAR(500) NOT NULL  CONSTRAINT DF_pact_censored_days_comment DEFAULT '',

            CONSTRAINT PK_pact_censored_days PRIMARY KEY (censor_id)
        )
    """,
}


def table_exists(conn, schema, table):
    result = conn.execute(
        text("""
            SELECT 1 FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table
        """),
        {'schema': schema, 'table': table},
    )
    return result.fetchone() is not None


if __name__ == '__main__':
    engine = make_engine()
    print(f'Connected to {DB_SERVER} / {DB_NAME}\n')

    with engine.begin() as conn:
        for qualified_name, ddl in TABLES.items():
            schema, table = qualified_name.split('.')
            if table_exists(conn, schema, table):
                print(f'  EXISTS   {qualified_name}')
            else:
                conn.execute(text(ddl))
                print(f'  CREATED  {qualified_name}')

    print('\nDone.')
