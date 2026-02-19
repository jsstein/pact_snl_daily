"""Load and resolve pact_config.json."""

import json
from pathlib import Path

_CONFIG_PATH = Path(__file__).parent.parent / 'pact_config.json'


def load_config(path=None):
    """Load configuration from pact_config.json, expanding ~ in path fields."""
    if path is None:
        path = _CONFIG_PATH
    path = Path(path)
    with open(path) as f:
        cfg = json.load(f)

    # Expand ~ in path-like fields
    for key in ('base_path', 'ssl_cert'):
        if key in cfg and cfg[key]:
            cfg[key] = str(Path(cfg[key]).expanduser())

    # Resolve CSV paths relative to the config file's directory
    config_dir = path.parent
    for key in ('setup_csv', 'censored_days_csv'):
        if key in cfg:
            p = Path(cfg[key])
            if not p.is_absolute():
                cfg[key] = str(config_dir / p)

    return cfg


def get_site_cfg(cfg, site_key):
    """Return the site config dict for a given site key (e.g. 'SNL')."""
    try:
        return cfg['sites'][site_key]
    except KeyError:
        valid = list(cfg['sites'].keys())
        raise KeyError(f'Unknown site {site_key!r}. Valid sites: {valid}')


def get_base_path(cfg):
    return Path(cfg['base_path'])


def get_batch_dir(cfg, batch, site_key):
    """Return Path to the batch outdoor directory (e.g. P-0042-XX/Outdoor_SNL)."""
    outdoor = get_site_cfg(cfg, site_key)['outdoor_directory']
    return get_base_path(cfg) / f'{batch}-XX' / outdoor


def get_metadata_dir(cfg, batch, site_key):
    return get_batch_dir(cfg, batch, site_key) / 'data' / 'metadata'


def get_module_metadata_path(cfg, batch, site_key):
    return get_metadata_dir(cfg, batch, site_key) / 'module-metadata.json'


def get_site_metadata_path(cfg, batch, site_key):
    return get_metadata_dir(cfg, batch, site_key) / 'site-metadata.json'


def get_point_data_dir(cfg, batch, site_key):
    return get_batch_dir(cfg, batch, site_key) / 'data' / 'point-data'


def get_iv_data_dir(cfg, batch, site_key):
    return get_batch_dir(cfg, batch, site_key) / 'data' / 'iv-data'


def get_setup_csv_path(cfg):
    return Path(cfg['setup_csv'])


def get_censored_days_csv_path(cfg):
    return Path(cfg['censored_days_csv'])
