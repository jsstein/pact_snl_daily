"""Plot daily efficiency from a CSV file produced by the efficiency-plot command.

Usage
-----
    # Interactive plotly HTML (opens in browser):
    python3.10 plot_efficiency_csv.py efficiency_plot.csv

    # Save to a specific HTML file instead of opening in browser:
    python3.10 plot_efficiency_csv.py efficiency_plot.csv --output my_plot.html

    # Truncate each module at its T80 date (reads sidecar *_t80.json):
    python3.10 plot_efficiency_csv.py efficiency_plot.csv --pre-t80

    # Static PNG (matplotlib):
    python3.10 plot_efficiency_csv.py efficiency_plot.csv --png
"""

import argparse
import json
import sys
from pathlib import Path

import pandas as pd


def plot_plotly(df, output=None):
    try:
        import plotly.graph_objects as go
    except ImportError:
        print('plotly is not installed. Run: pip install plotly')
        sys.exit(1)

    fig = go.Figure()
    for module in df.columns:
        series = df[module].dropna()
        if series.empty:
            continue
        fig.add_trace(go.Scatter(
            x=series.index,
            y=series.values,
            mode='lines',
            name=module,
            line=dict(width=1, color='black'),
            hovertemplate=(
                '%{x|%Y-%m-%d}<br>'
                'Efficiency: %{y:.2f}%<br>'
                'Module: ' + module + '<extra></extra>'
            ),
        ))

    fig.update_layout(
        title='PACT Daily Module Efficiency',
        xaxis_title='Date',
        yaxis_title='Daily Efficiency (%)',
        showlegend=False,
        hovermode='closest',
    )

    if output:
        fig.write_html(output)
        print(f'Saved: {output}')
    else:
        fig.show()


def plot_matplotlib(df, output):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(14, 6))
    for module in df.columns:
        series = df[module].dropna()
        if series.empty:
            continue
        ax.plot(series.index, series.values, linewidth=0.8, color='black')

    ax.set_xlabel('Date')
    ax.set_ylabel('Daily Efficiency (%)')
    ax.set_title('PACT Daily Module Efficiency')
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(output, bbox_inches='tight', dpi=150)
    plt.close(fig)
    print(f'Saved: {output}')


def main():
    parser = argparse.ArgumentParser(description='Plot efficiency CSV from efficiency-plot command')
    parser.add_argument('csv', metavar='CSV', help='Path to the efficiency CSV file')
    parser.add_argument('--output', default=None, metavar='PATH',
                        help='Output file path (default: open in browser for plotly, '
                             'or <csv_name>.png for --png)')
    parser.add_argument('--png', action='store_true',
                        help='Generate a static PNG using matplotlib instead of interactive plotly')
    parser.add_argument('--pre-t80', action='store_true', dest='pre_t80',
                        help='Truncate each module at its T80 date using the '
                             'sidecar *_t80.json file saved alongside the CSV')
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f'Error: file not found: {csv_path}')
        sys.exit(1)

    df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
    print(f'Loaded {len(df.columns)} modules, {len(df)} dates from {csv_path}')

    if args.pre_t80:
        t80_json_path = csv_path.with_name(csv_path.stem + '_t80.json')
        if not t80_json_path.exists():
            print(f'Warning: --pre-t80 requested but sidecar not found: {t80_json_path}')
            print('Run efficiency-plot without --pre-t80 first to generate the sidecar.')
        else:
            with open(t80_json_path) as f:
                t80_dates = json.load(f)
            for module in df.columns:
                t80_str = t80_dates.get(module)
                if t80_str is not None:
                    df.loc[pd.Timestamp(t80_str):, module] = float('nan')
            print(f'Applied T80 cutoffs from {t80_json_path}')

    if args.png:
        output = args.output or csv_path.with_suffix('.png')
        plot_matplotlib(df, output)
    else:
        plot_plotly(df, output=args.output)


if __name__ == '__main__':
    main()
