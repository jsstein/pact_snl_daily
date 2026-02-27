"""Command-line interface for PACT module administration."""

import argparse
import sys

from . import config, ingest, registry


def _load_config_or_exit():
    try:
        return config.load_config()
    except FileNotFoundError:
        print(
            'Error: pact_config.json not found.\n'
            'Copy pact_config.example.json to pact_config.json and fill in your paths.'
        )
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        prog='python -m pact_admin',
        description='PACT module administration CLI',
    )
    sub = parser.add_subparsers(dest='command', required=True)

    # ---- add-module --------------------------------------------------------
    p = sub.add_parser('add-module', help='Add a new module to the setup CSV and metadata')
    p.add_argument('--pact-id', required=True, metavar='P-XXXX-XX',
                   help='PACT module ID (e.g. P-0150-01)')
    p.add_argument('--psel-id', required=True, type=int, metavar='NNNN',
                   help='PSEL (database) ID')
    p.add_argument('--area', required=True, type=float, metavar='M2',
                   help='Module area in m²')
    p.add_argument('--type', required=True, dest='module_type', metavar='TYPE',
                   help='Module type string (e.g. MHP, OPV)')
    p.add_argument('--start-date', required=True, metavar='YYYY-MM-DD',
                   help='Outdoor deployment start date')
    p.add_argument('--site', default='SNL', metavar='SITE_KEY',
                   help='Site key from pact_config.json (default: SNL)')
    p.add_argument('--notes', default='', metavar='TEXT',
                   help='Optional notes for the setup CSV')

    # ---- retire-module -----------------------------------------------------
    p = sub.add_parser('retire-module', help='Mark a module as inactive (Active=N)')
    p.add_argument('--pact-id', required=True, metavar='P-XXXX-XX')
    p.add_argument('--end-date', required=True, metavar='YYYY-MM-DD',
                   help='Date the module was removed from outdoor deployment')

    # ---- add-censor --------------------------------------------------------
    p = sub.add_parser('add-censor',
                       help='Add a censoring condition to module-metadata.json')
    p.add_argument('--pact-id', required=True, metavar='P-XXXX-XX or site',
                   help='Module ID, or "site" to apply to all modules active during the period')
    p.add_argument('--start', required=True, metavar='YYYY-MM-DD')
    p.add_argument('--end', required=True, metavar='YYYY-MM-DD')
    p.add_argument('--comment', default='', metavar='TEXT')

    # ---- sync-metadata -----------------------------------------------------
    sub.add_parser('sync-metadata',
                   help='Regenerate all module-metadata.json files from the setup CSVs (idempotent)')

    # ---- add-snow-day ------------------------------------------------------
    p = sub.add_parser('add-snow-day',
                       help='Add a snow day to all existing site-metadata.json files')
    p.add_argument('--date', required=True, metavar='YYYY-MM-DD')

    # ---- add-indoor --------------------------------------------------------
    p = sub.add_parser('add-indoor',
                       help='Record a period when a module was indoors')
    p.add_argument('--pact-id', required=True, metavar='P-XXXX-XX')
    p.add_argument('--start', required=True, metavar='YYYY-MM-DD')
    p.add_argument('--end', required=True, metavar='YYYY-MM-DD')
    p.add_argument('--comment', default='', metavar='TEXT')

    # ---- update-module -----------------------------------------------------
    p = sub.add_parser(
        'update-module',
        help='Fetch one month of DB data for a module, write point-data CSV '
             'and regenerate the bar chart',
    )
    p.add_argument('--pact-id', required=True, metavar='P-XXXX-XX',
                   help='PACT module ID (e.g. P-0042-04)')
    p.add_argument('--year', required=True, type=int, metavar='YYYY',
                   help='Four-digit year')
    p.add_argument('--month', required=True, type=int, metavar='M',
                   help='Month number (1–12)')
    p.add_argument('--no-s3', action='store_true', dest='no_s3',
                   help='Skip uploading files to S3 (useful off-network)')

    # ---- efficiency-plot ---------------------------------------------------
    p = sub.add_parser(
        'efficiency-plot',
        help='Plot daily efficiency vs. date for all (or filtered) modules',
    )
    p.add_argument('--output', default='efficiency_plot.png', metavar='PATH',
                   help='Output PNG file path (default: efficiency_plot.png)')
    p.add_argument('--active-only', action='store_true', dest='active_only',
                   help='Only include modules listed as Active=Y in setup CSV')
    p.add_argument('--batch', default=None, metavar='P-XXXX',
                   help='Only include modules from this batch prefix')

    # ---- update-batch ------------------------------------------------------
    p = sub.add_parser(
        'update-batch',
        help='Fetch one month of DB data for all active modules in a batch',
    )
    p.add_argument('--batch', required=True, metavar='P-XXXX',
                   help='Batch prefix, e.g. P-0042 (or P-0042-XX)')
    p.add_argument('--year', required=True, type=int, metavar='YYYY',
                   help='Four-digit year')
    p.add_argument('--month', required=True, type=int, metavar='M',
                   help='Month number (1–12)')
    p.add_argument('--no-s3', action='store_true', dest='no_s3',
                   help='Skip uploading files to S3')

    # ---- update-all --------------------------------------------------------
    p = sub.add_parser(
        'update-all',
        help='Fetch one month of DB data for all active modules',
    )
    p.add_argument('--year', required=True, type=int, metavar='YYYY',
                   help='Four-digit year')
    p.add_argument('--month', required=True, type=int, metavar='M',
                   help='Month number (1–12)')
    p.add_argument('--no-s3', action='store_true', dest='no_s3',
                   help='Skip uploading files to S3')

    # ---- list-modules ------------------------------------------------------
    p = sub.add_parser('list-modules', help='List modules from the setup CSV')
    p.add_argument('--all', action='store_true', dest='show_all',
                   help='Include inactive (Active=N) modules')

    args = parser.parse_args()
    cfg = _load_config_or_exit()

    if args.command == 'add-module':
        registry.add_module(
            cfg,
            pact_id=args.pact_id,
            psel_id=args.psel_id,
            area=args.area,
            module_type=args.module_type,
            start_date=args.start_date,
            site=args.site,
            notes=args.notes,
        )

    elif args.command == 'retire-module':
        registry.retire_module(cfg, pact_id=args.pact_id, end_date=args.end_date)

    elif args.command == 'add-censor':
        registry.add_censor(
            cfg,
            pact_id=args.pact_id,
            start=args.start,
            end=args.end,
            comment=args.comment,
        )

    elif args.command == 'sync-metadata':
        registry.sync_metadata(cfg)

    elif args.command == 'add-snow-day':
        registry.add_snow_day(cfg, date=args.date)

    elif args.command == 'add-indoor':
        registry.add_indoor(
            cfg,
            pact_id=args.pact_id,
            start=args.start,
            end=args.end,
            comment=args.comment,
        )

    elif args.command == 'update-module':
        ingest.update_module_month(
            cfg,
            pact_id=args.pact_id,
            year=args.year,
            month=args.month,
            upload_s3=not args.no_s3,
        )

    elif args.command == 'efficiency-plot':
        ingest.plot_all_efficiency(
            cfg,
            output_path=args.output,
            active_only=args.active_only,
            batch=args.batch,
        )

    elif args.command == 'update-batch':
        ingest.update_batch_month(
            cfg,
            batch=args.batch,
            year=args.year,
            month=args.month,
            upload_s3=not args.no_s3,
        )

    elif args.command == 'update-all':
        ingest.update_all_month(
            cfg,
            year=args.year,
            month=args.month,
            upload_s3=not args.no_s3,
        )

    elif args.command == 'list-modules':
        df = registry.list_modules(cfg, active_only=not args.show_all)
        print(df.to_string(index=False))


if __name__ == '__main__':
    main()
