-- Create PACT module registry and censored-days tables in PVGrid.
--
-- Run this once against the PVGrid database (requires DDL permissions).
-- After verifying the tables, run migrate_csv_to_db.py to load existing data.

-- -------------------------------------------------------------------------
-- 1.  pact_modules  (replaces PACT_SNL_Outdoor_Modules_SETUP.csv)
-- -------------------------------------------------------------------------
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
);

-- -------------------------------------------------------------------------
-- 2.  pact_censored_days  (replaces PACT_SNL_censored_days_SETUP.csv)
--
--     pact_id may be a module ID (e.g. 'P-0042-04') or the literal string
--     'site' for site-wide censor periods.
-- -------------------------------------------------------------------------
CREATE TABLE dbo.pact_censored_days (
    censor_id  INT          IDENTITY(1,1) NOT NULL,
    pact_id    VARCHAR(30)  NOT NULL,
    start_date DATE         NOT NULL,
    end_date   DATE         NOT NULL,
    comment    VARCHAR(500) NOT NULL  CONSTRAINT DF_pact_censored_days_comment DEFAULT '',

    CONSTRAINT PK_pact_censored_days PRIMARY KEY (censor_id)
);
