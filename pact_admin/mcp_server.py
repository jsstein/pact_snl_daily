"""MCP server exposing pact_admin CLI commands as tools.

Run as a standalone HTTP server (streamable-HTTP transport):

    python3.10 -m pact_admin.mcp_server

Then connect a client to http://127.0.0.1:8000/mcp
"""

import contextlib
import io
import sys

from mcp.server.fastmcp import FastMCP, Context

from . import config, ingest, registry

# ---------------------------------------------------------------------------
# Config loaded once at startup
# ---------------------------------------------------------------------------

cfg = config.load_config()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@contextlib.contextmanager
def _capture_stdout():
    """Redirect stdout to a StringIO buffer and return it."""
    buf = io.StringIO()
    old = sys.stdout
    sys.stdout = buf
    try:
        yield buf
    finally:
        sys.stdout = old


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

mcp = FastMCP("pact-admin", host="127.0.0.1", port=8000)


# ---- Registry tools --------------------------------------------------------

@mcp.tool()
def add_module(
    pact_id: str,
    psel_id: int,
    area: float,
    module_type: str,
    start_date: str,
    site: str,
    notes: str = "",
) -> str:
    """Add a new module to the database and metadata.

    site must be one of: 'SNL' or 'SNL_fixed-tilt'.

    Args:
        pact_id: PACT module ID, e.g. P-0150-01
        psel_id: PSEL (database) integer ID
        area: Module area in m²
        module_type: Module type string, e.g. MHP or OPV
        start_date: Outdoor deployment start date (YYYY-MM-DD)
        site: Site key — must be 'SNL' or 'SNL_fixed-tilt'
        notes: Optional notes
    """
    with _capture_stdout() as buf:
        registry.add_module(
            cfg,
            pact_id=pact_id,
            psel_id=psel_id,
            area=area,
            module_type=module_type,
            start_date=start_date,
            site=site,
            notes=notes,
        )
    return buf.getvalue()


@mcp.tool()
def add_modules_bulk(
    pact_id_start: str,
    pact_id_end: str,
    psel_id_start: int,
    area: float,
    module_type: str,
    start_date: str,
    site: str,
    notes: str = "",
    add_to_db25: bool = False,
    db25_env: str = "DV",
    module_model_id: int = None,
    source: str = None,
    module_owner: str = None,
    date_received: str = None,
) -> str:
    """Add a consecutive range of modules that share all parameters except pact_id and psel_id.

    site must be one of: 'SNL' or 'SNL_fixed-tilt'.

    Example: pact_id_start='P-0150-01', pact_id_end='P-0150-10', psel_id_start=3350
    adds P-0150-01 (psel 3350) through P-0150-10 (psel 3359).

    Args:
        pact_id_start: First module ID in the range, e.g. P-0150-01
        pact_id_end: Last module ID in the range, e.g. P-0150-10
        psel_id_start: PSEL ID for the first module; incremented by 1 for each subsequent module
        area: Module area in m²
        module_type: Module type string, e.g. MHP or OPV
        start_date: Outdoor deployment start date (YYYY-MM-DD)
        site: Site key — must be 'SNL' or 'SNL_fixed-tilt'
        notes: Optional notes applied to all modules
        add_to_db25: If True, also insert each module into db25_modules (default False)
        db25_env: db25 environment to insert into — 'DV' or 'PR' (default 'DV')
        module_model_id: Foreign key to db25_module_models (optional)
        source: Source/origin of the modules (optional)
        module_owner: Owner of the modules (optional)
        date_received: Date modules were received, YYYY-MM-DD (optional, defaults to start_date)
    """
    with _capture_stdout() as buf:
        registry.add_modules_bulk(
            cfg,
            pact_id_start=pact_id_start,
            pact_id_end=pact_id_end,
            psel_id_start=psel_id_start,
            area=area,
            module_type=module_type,
            start_date=start_date,
            site=site,
            notes=notes,
            add_to_db25=add_to_db25,
            db25_env=db25_env,
            module_model_id=module_model_id,
            source=source,
            module_owner=module_owner,
            date_received=date_received,
        )
    return buf.getvalue()


@mcp.tool()
def delete_module(pact_id: str, purge: bool = False, db25_env: str = None) -> str:
    """Permanently delete a module from pact_modules, pact_censored_days, and module-metadata.json.

    Use this only for modules added by mistake or with incorrect information.
    Orphaned censor records are always removed. Optionally also removes the
    matching row from db25_modules.

    Args:
        pact_id: PACT module ID to delete, e.g. P-0150-01
        purge: If True and no other modules remain in the batch, also delete
               the entire batch directory tree from Box Sync (including all
               data files). Default False.
        db25_env: If 'DV' or 'PR', also delete from db25_modules. Default None (skip).
    """
    with _capture_stdout() as buf:
        registry.delete_module(cfg, pact_id=pact_id, purge=purge, db25_env=db25_env)
    return buf.getvalue()


@mcp.tool()
def update_module(
    pact_id: str,
    area: float = None,
    module_type: str = None,
    psel_id: int = None,
    site: str = None,
    start_date: str = None,
    notes: str = None,
    active: str = None,
) -> str:
    """Update one or more fields on an existing module.

    Only the fields you supply are changed; omitted fields are left as-is.

    Args:
        pact_id: PACT module ID to update, e.g. P-0150-01
        area: New module area in m²
        module_type: New module type, e.g. MHP or OPV
        psel_id: New PSEL (database) integer ID
        site: New site — must be 'SNL' or 'SNL_fixed-tilt'
        start_date: New outdoor deployment start date (YYYY-MM-DD)
        notes: New notes
        active: Active flag — 'Y' to reactivate, 'N' to deactivate
    """
    with _capture_stdout() as buf:
        registry.update_module(
            cfg,
            pact_id=pact_id,
            area=area,
            module_type=module_type,
            psel_id=psel_id,
            site=site,
            start_date=start_date,
            notes=notes,
            active=active,
        )
    return buf.getvalue()


@mcp.tool()
def retire_module(pact_id: str, end_date: str) -> str:
    """Mark a module as inactive (Active=N) in the setup CSV.

    Args:
        pact_id: PACT module ID, e.g. P-0150-01
        end_date: Date the module was removed from outdoor deployment (YYYY-MM-DD)
    """
    with _capture_stdout() as buf:
        registry.retire_module(cfg, pact_id=pact_id, end_date=end_date)
    return buf.getvalue()


@mcp.tool()
def add_censor(pact_id: str, start: str, end: str, comment: str = "") -> str:
    """Add a censoring period for a module or all active modules.

    Args:
        pact_id: Module ID (e.g. P-0150-01) or 'site' to apply to all modules
                 active during the period
        start: Start date of censored period (YYYY-MM-DD)
        end: End date of censored period (YYYY-MM-DD)
        comment: Optional description of why the period is censored
    """
    with _capture_stdout() as buf:
        registry.add_censor(cfg, pact_id=pact_id, start=start, end=end, comment=comment)
    return buf.getvalue()


@mcp.tool()
def add_snow_day(date: str) -> str:
    """Add a snow day to all existing site-metadata.json files.

    Args:
        date: The snow day date (YYYY-MM-DD)
    """
    with _capture_stdout() as buf:
        registry.add_snow_day(cfg, date=date)
    return buf.getvalue()


@mcp.tool()
def add_indoor(pact_id: str, start: str, end: str, comment: str = "") -> str:
    """Record a period when a module was indoors (not deployed outdoors).

    Args:
        pact_id: PACT module ID, e.g. P-0150-01
        start: Start date of indoor period (YYYY-MM-DD)
        end: End date of indoor period (YYYY-MM-DD)
        comment: Optional description
    """
    with _capture_stdout() as buf:
        registry.add_indoor(cfg, pact_id=pact_id, start=start, end=end, comment=comment)
    return buf.getvalue()


@mcp.tool()
def sync_metadata() -> str:
    """Regenerate all module-metadata.json files from the setup CSVs (idempotent).

    Run this after editing the setup CSV directly, e.g. to remove a censor condition.
    """
    with _capture_stdout() as buf:
        registry.sync_metadata(cfg)
    return buf.getvalue()


@mcp.tool()
def list_modules(active_only: bool = True) -> str:
    """List modules from the setup CSV.

    Args:
        active_only: If True (default), only return modules with Active=Y
    """
    df = registry.list_modules(cfg, active_only=active_only)
    return df.to_string(index=False)


# ---- AWS tools -------------------------------------------------------------

@mcp.tool()
def aws_sso_login() -> str:
    """Authenticate with AWS SSO by opening a browser window.

    Runs `aws sso login` using the sso_session name from pact_config.json.
    Blocks until the user completes the browser-based login flow.
    """
    with _capture_stdout() as buf:
        ingest.aws_sso_login(cfg)
    return buf.getvalue()


# ---- Ingestion tools -------------------------------------------------------

@mcp.tool()
async def update_mpp(ctx: Context, target: str, year: int, month: int, upload_s3: bool = True) -> str:
    """Fetch one month of DB data and regenerate charts for one module, a batch, or all active modules.

    Use this tool when asked to update, run, process, fetch, refresh, or ingest monthly
    point-data or bar charts. Examples: "update P-0042-04 for March", "run batch P-0042",
    "process all modules for last month".

    Args:
        target: Module ID (e.g. P-0042-04), batch prefix (e.g. P-0042), or 'all'
        year: Four-digit year
        month: Month number (1-12)
        upload_s3: Whether to upload files to S3 (default True)
    """
    if target.count('-') >= 2:  # single module, e.g. P-0042-04
        with _capture_stdout() as buf:
            ingest.update_module_month(cfg, pact_id=target, year=year, month=month, upload_s3=upload_s3)
        return buf.getvalue()

    if target == 'all':
        modules_df = registry.read_modules(cfg)
        active = modules_df[modules_df['Active'] == 'Y']
        label = 'all active'
    else:  # batch prefix, e.g. P-0042
        batch_prefix = target[:6]
        modules_df = registry.read_modules(cfg)
        active = modules_df[
            (modules_df['Active'] == 'Y') &
            (modules_df['PACT_id'].str.startswith(batch_prefix))
        ]
        label = f'batch {batch_prefix}'

    if active.empty:
        return f'No active modules found for {label}.'

    total = len(active)
    results = []
    await ctx.info(f'Updating {total} module(s) for {label} — {year}-{month:02d}')

    for i, (_, row) in enumerate(active.iterrows()):
        pact_id = row['PACT_id']
        await ctx.info(f'[{i+1}/{total}] {pact_id} — starting...')
        with _capture_stdout() as buf:
            try:
                ingest.update_module_month(cfg, pact_id=pact_id, year=year,
                                           month=month, upload_s3=upload_s3)
                results.append(f'✓ {pact_id}')
            except Exception as exc:
                results.append(f'✗ {pact_id}: {exc}')
        out = buf.getvalue().strip()
        if out:
            await ctx.info(out)

    return '\n'.join(results)


@mcp.tool()
def efficiency_plot(
    output_path: str = "efficiency_plot.png",
    active_only: bool = False,
    batch: str = None,
    use_plotly: bool = False,
    pre_t80: bool = False,
) -> str:
    """Generate a daily efficiency vs. date plot for all (or filtered) modules.

    Also saves a companion CSV and a sidecar *_t80.json with T80 dates.

    Args:
        output_path: Output file path (.png for static, .html for plotly)
        active_only: Only include modules listed as Active=Y
        batch: Only include modules from this batch prefix, e.g. P-0042
        use_plotly: Generate an interactive HTML plot instead of a static PNG
        pre_t80: Truncate each module's data at its T80 date
    """
    with _capture_stdout() as buf:
        ingest.plot_all_efficiency(
            cfg,
            output_path=output_path,
            active_only=active_only,
            batch=batch,
            use_plotly=use_plotly,
            pre_t80=pre_t80,
        )
    return buf.getvalue()


@mcp.tool()
def module_summary(active_only: bool = False, output_path: str = None) -> str:
    """Generate a summary table for all modules.

    Returns a table with pact_id, start_date, end_date, days_to_t80,
    and max_efficiency_pct. Optionally saves to a CSV file.

    Args:
        active_only: Only include modules listed as Active=Y
        output_path: If provided, save the table as a CSV at this path
    """
    with _capture_stdout() as buf:
        ingest.generate_module_summary(
            cfg, output_path=output_path, active_only=active_only
        )
    return buf.getvalue()


@mcp.tool()
async def update_ivs(ctx: Context, target: str, year: int, month: int, upload_s3: bool = True) -> str:
    """Process IV curves for one module, a batch, or all active modules and write monthly CSVs.

    Use this tool when asked to process, run, fetch, ingest, or refresh IV curve data.
    Examples: "process IVs for P-0138-01", "run IV curves for batch P-0042",
    "ingest all IV data for January".

    Args:
        target: Module ID (e.g. P-0138-01), batch prefix (e.g. P-0042), or 'all'
        year: Four-digit year
        month: Month number (1-12)
        upload_s3: Upload each CSV to S3 (default True)
    """
    if target.count('-') >= 2:  # single module, e.g. P-0138-01
        with _capture_stdout() as buf:
            ingest.update_ivs(cfg, pact_id=target, year=year, month=month, upload_s3=upload_s3)
        return buf.getvalue()

    if target == 'all':
        modules_df = registry.read_modules(cfg)
        active = modules_df[modules_df['Active'] == 'Y']
        label = 'all active'
    else:  # batch prefix, e.g. P-0042
        batch_prefix = target[:6]
        modules_df = registry.read_modules(cfg)
        active = modules_df[
            (modules_df['Active'] == 'Y') &
            (modules_df['PACT_id'].str.startswith(batch_prefix))
        ]
        label = f'batch {batch_prefix}'

    if active.empty:
        return f'No active modules found for {label}.'

    total = len(active)
    results = []
    await ctx.info(f'Processing IV curves for {total} module(s) for {label} — {year}-{month:02d}')

    with _capture_stdout() as mount_buf:
        network_path = ingest._mount_iv_drive(cfg, verbose=True)
    if mount_buf.getvalue().strip():
        await ctx.info(mount_buf.getvalue().strip())

    try:
        for i, (_, row) in enumerate(active.iterrows()):
            pact_id = row['PACT_id']
            await ctx.info(f'[{i+1}/{total}] {pact_id} — starting...')
            with _capture_stdout() as buf:
                try:
                    ingest.update_ivs(cfg, pact_id=pact_id, year=year, month=month,
                                      upload_s3=upload_s3, _network_path=network_path)
                    results.append(f'✓ {pact_id}')
                except Exception as exc:
                    results.append(f'✗ {pact_id}: {exc}')
            out = buf.getvalue().strip()
            if out:
                await ctx.info(out)
    finally:
        with _capture_stdout() as unmount_buf:
            ingest._unmount_iv_drive(network_path, verbose=True)
        if unmount_buf.getvalue().strip():
            await ctx.info(unmount_buf.getvalue().strip())

    return '\n'.join(results)


@mcp.tool()
async def update_mpp_ivs(ctx: Context, target: str, year: int, month: int, upload_s3: bool = True) -> str:
    """Fetch MPP data and process IV curves for one module, a batch, or all active modules.

    Runs pact_update_mpp followed by pact_update_ivs for each module. Use this when
    asked to do a full monthly update or to process both point-data and IV curves together.

    Args:
        target: Module ID (e.g. P-0042-04), batch prefix (e.g. P-0042), or 'all'
        year: Four-digit year
        month: Month number (1-12)
        upload_s3: Whether to upload files to S3 (default True)
    """
    if target.count('-') >= 2:  # single module
        with _capture_stdout() as buf:
            ingest.update_module_month(cfg, pact_id=target, year=year, month=month, upload_s3=upload_s3)
        mpp_out = buf.getvalue().strip()
        with _capture_stdout() as buf:
            ingest.update_ivs(cfg, pact_id=target, year=year, month=month, upload_s3=upload_s3)
        iv_out = buf.getvalue().strip()
        return '\n'.join(filter(None, [mpp_out, iv_out]))

    if target == 'all':
        modules_df = registry.read_modules(cfg)
        active = modules_df[modules_df['Active'] == 'Y']
        label = 'all active'
    else:  # batch prefix
        batch_prefix = target[:6]
        modules_df = registry.read_modules(cfg)
        active = modules_df[
            (modules_df['Active'] == 'Y') &
            (modules_df['PACT_id'].str.startswith(batch_prefix))
        ]
        label = f'batch {batch_prefix}'

    if active.empty:
        return f'No active modules found for {label}.'

    total = len(active)
    results = []
    await ctx.info(f'Running MPP + IVs for {total} module(s) for {label} — {year}-{month:02d}')

    with _capture_stdout() as mount_buf:
        network_path = ingest._mount_iv_drive(cfg, verbose=True)
    if mount_buf.getvalue().strip():
        await ctx.info(mount_buf.getvalue().strip())

    try:
        for i, (_, row) in enumerate(active.iterrows()):
            pact_id = row['PACT_id']
            await ctx.info(f'[{i+1}/{total}] {pact_id} — MPP...')
            with _capture_stdout() as buf:
                try:
                    ingest.update_module_month(cfg, pact_id=pact_id, year=year,
                                               month=month, upload_s3=upload_s3)
                    mpp_status = '✓ MPP'
                except Exception as exc:
                    mpp_status = f'✗ MPP: {exc}'
            out = buf.getvalue().strip()
            if out:
                await ctx.info(out)

            await ctx.info(f'[{i+1}/{total}] {pact_id} — IVs...')
            with _capture_stdout() as buf:
                try:
                    ingest.update_ivs(cfg, pact_id=pact_id, year=year, month=month,
                                      upload_s3=upload_s3, _network_path=network_path)
                    iv_status = '✓ IVs'
                except Exception as exc:
                    iv_status = f'✗ IVs: {exc}'
            out = buf.getvalue().strip()
            if out:
                await ctx.info(out)

            results.append(f'{pact_id}: {mpp_status}, {iv_status}')
    finally:
        with _capture_stdout() as unmount_buf:
            ingest._unmount_iv_drive(network_path, verbose=True)
        if unmount_buf.getvalue().strip():
            await ctx.info(unmount_buf.getvalue().strip())

    return '\n'.join(results)


@mcp.tool()
def plot_ivs(pact_id: str, year: int, month: int, output_path: str = None, max_poa_variation_pct: float = 1.0) -> str:
    """Plot IV curves for a module/month from the monthly IV CSV in Box Sync.

    Plots all IV curves that pass an irradiance stability filter. Only curves
    where the POA irradiance changed by at most max_poa_variation_pct between
    the start and end of the sweep are included.

    Args:
        pact_id: PACT module ID, e.g. P-0138-01
        year: Four-digit year
        month: Month number (1-12)
        output_path: Where to save the PNG (default: current working directory)
        max_poa_variation_pct: Maximum POA variation in percent — pass 1.0 for 1%,
            10.0 for 10%, etc. Higher values keep more curves. Default 1.0.
    """
    with _capture_stdout() as buf:
        saved_path = ingest.plot_iv_month(
            cfg, pact_id=pact_id, year=year, month=month,
            output_path=output_path, max_poa_variation_pct=max_poa_variation_pct,
        )
    output = buf.getvalue().strip()
    return f'{output}\n{saved_path}'.strip() if output else saved_path


@mcp.tool()
def find_iv_files(pact_id: str, date: str) -> str:
    """Find IV-curve CSV files for a module/date from the SNL network drive.

    Connects to smb://snl/collaborative/pvpact/Outdoor_data/ if not already
    mounted, opens the daily zip (YYMMDD.zip), and returns the paths of all
    IV files matching YYYYMMDDHHMM_<pact_id>_IV.csv.

    Args:
        pact_id: PACT module ID, e.g. P-0138-01
        date: Date of the IV measurements (YYYY-MM-DD)
    """
    with _capture_stdout() as buf:
        paths = ingest.find_iv_files(cfg, pact_id=pact_id, date_str=date)
    output = buf.getvalue().strip()
    if paths:
        files_list = '\n'.join(paths)
        return f'{output}\n\n{files_list}' if output else files_list
    return output or f'No IV files found for {pact_id} on {date}.'


# ---- S3 tools --------------------------------------------------------------

@mcp.tool()
def s3_list(prefix: str = '') -> str:
    """List objects in the PACT S3 bucket, optionally filtered by prefix.

    Args:
        prefix: Key prefix to filter by, e.g. 'P-0138-XX/Outdoor_SNL/data/'
    """
    with _capture_stdout() as buf:
        results = ingest.s3_list(cfg, prefix=prefix)
    lines = [f'{key}  ({size:,} bytes,  {modified:%Y-%m-%d %H:%M})'
             for key, size, modified in results]
    header = f'{len(results)} object(s) in s3://{cfg["s3_bucket"]}/{prefix or ""}\n'
    return header + '\n'.join(lines) if lines else f'No objects found with prefix "{prefix}".'


@mcp.tool()
def s3_upload(local_path: str, s3_key: str) -> str:
    """Upload a local file to the PACT S3 bucket.

    Args:
        local_path: Absolute path to the local file to upload
        s3_key: Destination key in S3, e.g. 'P-0138-XX/Outdoor_SNL/data/point-data/file.csv'
    """
    with _capture_stdout() as buf:
        ingest.s3_upload(cfg, local_path=local_path, s3_key=s3_key)
    return buf.getvalue().strip()


@mcp.tool()
def s3_download(s3_key: str, local_path: str) -> str:
    """Download a file from the PACT S3 bucket to a local path.

    Args:
        s3_key: S3 object key to download
        local_path: Local destination path (parent directories created if needed)
    """
    with _capture_stdout() as buf:
        ingest.s3_download(cfg, s3_key=s3_key, local_path=local_path)
    return buf.getvalue().strip()


@mcp.tool()
def s3_delete(prefix: str, pattern: str = '*') -> str:
    """Delete objects from the PACT S3 bucket matching a prefix and wildcard pattern.

    Args:
        prefix: Key prefix identifying the directory, e.g. 'P-0138-XX/Outdoor_SNL/data/'
        pattern: Wildcard pattern matched against the full key, e.g. '*.csv' or '*2025-01*'
                 (default '*' = all objects under prefix)
    """
    with _capture_stdout() as buf:
        deleted = ingest.s3_delete(cfg, prefix=prefix, pattern=pattern)
    output = buf.getvalue().strip()
    summary = f'Deleted {len(deleted)} object(s).'
    return f'{output}\n{summary}'.strip() if output else summary


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mcp.run(transport="streamable-http")
