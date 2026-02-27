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
    site: str = "SNL",
    notes: str = "",
) -> str:
    """Add a new module to the setup CSV and metadata.

    Args:
        pact_id: PACT module ID, e.g. P-0150-01
        psel_id: PSEL (database) integer ID
        area: Module area in m²
        module_type: Module type string, e.g. MHP or OPV
        start_date: Outdoor deployment start date (YYYY-MM-DD)
        site: Site key from pact_config.json (default SNL)
        notes: Optional notes for the setup CSV
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
def delete_module(pact_id: str, purge: bool = False) -> str:
    """Permanently delete a module from the setup CSV and module-metadata.json.

    Use this only for modules added by mistake or with incorrect information.

    Args:
        pact_id: PACT module ID to delete, e.g. P-0150-01
        purge: If True and no other modules remain in the batch, also delete
               the entire batch directory tree from Box Sync (including all
               data files). Default False.
    """
    with _capture_stdout() as buf:
        registry.delete_module(cfg, pact_id=pact_id, purge=purge)
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
def update_module(pact_id: str, year: int, month: int, upload_s3: bool = True) -> str:
    """Fetch one month of DB data for a module, write point-data CSV and
    regenerate the bar chart. Optionally uploads both files to S3.

    Args:
        pact_id: PACT module ID, e.g. P-0042-04
        year: Four-digit year
        month: Month number (1-12)
        upload_s3: Whether to upload files to S3 (default True)
    """
    with _capture_stdout() as buf:
        ingest.update_module_month(
            cfg, pact_id=pact_id, year=year, month=month, upload_s3=upload_s3
        )
    return buf.getvalue()


@mcp.tool()
async def update_batch(ctx: Context, batch: str, year: int, month: int, upload_s3: bool = True) -> str:
    """Fetch one month of DB data for all active modules in a batch.

    Args:
        batch: Batch prefix, e.g. P-0042
        year: Four-digit year
        month: Month number (1-12)
        upload_s3: Whether to upload files to S3 (default True)
    """
    batch_prefix = batch[:6]
    modules_df = registry.read_modules(cfg)
    active = modules_df[
        (modules_df['Active'] == 'Y') &
        (modules_df['PACT_id'].str.startswith(batch_prefix))
    ]

    if active.empty:
        return f'No active modules found for batch {batch_prefix}.'

    total = len(active)
    results = []
    await ctx.info(f'Updating {total} module(s) in {batch_prefix} for {year}-{month:02d}')

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
async def update_all(ctx: Context, year: int, month: int, upload_s3: bool = True) -> str:
    """Fetch one month of DB data for all active modules.

    Args:
        year: Four-digit year
        month: Month number (1-12)
        upload_s3: Whether to upload files to S3 (default True)
    """
    modules_df = registry.read_modules(cfg)
    active = modules_df[modules_df['Active'] == 'Y']

    if active.empty:
        return 'No active modules found.'

    total = len(active)
    results = []
    await ctx.info(f'Updating {total} active module(s) for {year}-{month:02d}')

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


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mcp.run(transport="streamable-http")
