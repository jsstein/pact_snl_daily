# pact_snl_daily CLI Examples

## Module registry

### Add a new module
```
python -m pact_admin add-module \
  --pact-id P-0150-01 \
  --psel-id 1234 \
  --area 1.234 \
  --type MHP \
  --start-date 2026-02-15 \
  --site SNL \
  --notes "optional text"
```

### Retire / decommission a module
```
python -m pact_admin retire-module --pact-id P-0150-01 --end-date 2026-12-01
```

### Delete a module (entered by mistake or with wrong information)
Removes the module from the setup CSV and metadata. Data files in Box Sync are NOT deleted.
```
python -m pact_admin delete-module --pact-id P-0150-01
```

### List active modules
```
python -m pact_admin list-modules
```

### List all modules (including retired)
```
python -m pact_admin list-modules --all
```

### Censor a specific module
```
python -m pact_admin add-censor \
  --pact-id P-0150-01 \
  --start 2026-03-01 \
  --end 2026-03-05 \
  --comment "shaded by maintenance equipment"
```

### Add a site-wide censor event
```
python -m pact_admin add-censor \
  --pact-id site \
  --start 2026-03-01 \
  --end 2026-03-05 \
  --comment "snowstorm"
```

### Sync metadata
Run after editing the setup CSV (e.g. to remove a censor condition):
```
python -m pact_admin sync-metadata
```

---

## Data ingestion

### Update point-data and bar chart for one module/month (with S3 upload)
```
python -m pact_admin update-module \
  --pact-id P-0042-04 \
  --year 2026 \
  --month 2
```

### Update one module/month (skip S3 upload, e.g. off-network)
```
python -m pact_admin update-module \
  --pact-id P-0042-04 \
  --year 2026 \
  --month 2 \
  --no-s3
```

### Update all active modules in a batch for one month
```
python -m pact_admin update-batch \
  --batch P-0042 \
  --year 2026 \
  --month 2
```

### Update all active modules in a batch (skip S3 upload)
```
python -m pact_admin update-batch \
  --batch P-0042 \
  --year 2026 \
  --month 2 \
  --no-s3
```

### Update all active modules for one month
```
python -m pact_admin update-all \
  --year 2026 \
  --month 2
```

### Update all active modules (skip S3 upload)
```
python -m pact_admin update-all \
  --year 2026 \
  --month 2 \
  --no-s3
```

---

## Efficiency plots

All `efficiency-plot` runs also save a companion CSV (`<output>.csv`) with
daily efficiency values (one column per module) and a sidecar
`<output>_t80.json` with each module's T80 date.

### Static PNG — all modules
```
python -m pact_admin efficiency-plot --output efficiency_plot.png
```

### Static PNG — active modules only
```
python -m pact_admin efficiency-plot \
  --output efficiency_plot.png \
  --active-only
```

### Static PNG — single batch
```
python -m pact_admin efficiency-plot \
  --output efficiency_plot.png \
  --batch P-0042
```

### Static PNG — data truncated at each module's T80 date
```
python -m pact_admin efficiency-plot \
  --pre-t80 \
  --output efficiency_pre_t80.png
```

### Interactive HTML (hover to see PACT-ID)
```
python -m pact_admin efficiency-plot --plotly
```

### Interactive HTML — active modules only, custom output path
```
python -m pact_admin efficiency-plot \
  --plotly \
  --active-only \
  --output my_plot.html
```

### Interactive HTML — data truncated at T80
```
python -m pact_admin efficiency-plot --plotly --pre-t80
```

---

## Re-plotting from CSV (standalone script)

`plot_efficiency_csv.py` reads a CSV produced by `efficiency-plot` and
re-generates the plot without re-running the full ingestion pipeline.
Useful for quick testing or adjusting plot options.

### Interactive HTML (opens in browser)
```
python3.10 plot_efficiency_csv.py efficiency_plot.csv
```

### Interactive HTML — save to file instead of opening in browser
```
python3.10 plot_efficiency_csv.py efficiency_plot.csv --output my_plot.html
```

### Interactive HTML — data truncated at T80 (reads sidecar `*_t80.json`)
```
python3.10 plot_efficiency_csv.py efficiency_plot.csv --pre-t80
```

### Static PNG
```
python3.10 plot_efficiency_csv.py efficiency_plot.csv --png
```

---

## Module summary table

Prints a table with `pact_id`, `start_date`, `end_date`, `days_to_t80`,
and `max_efficiency_pct` for each module.

### All modules (active and retired)
```
python -m pact_admin module-summary
```

### Active modules only, saved to CSV
```
python -m pact_admin module-summary --active-only --output summary.csv
```
