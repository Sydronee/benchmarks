# UHC and Cigna Benchmark Runbook

This runbook ingests UHC and Cigna rates into one database, builds the
benchmark rollups, and serves the comparison dashboard.

## Project layout and prerequisites

- `TiC/` contains the ingestion and benchmark-building scripts.
- `TiC/transparency.duckdb` contains the raw rates.
- `Benchmarks/enrichment.duckdb` contains NPPES, ZIP/county, and code reference data.
- `Benchmarks/` serves the dashboard and stores the enrichment database.

Run TiC commands from `TiC/` and dashboard commands from `Benchmarks/`.
DuckDB allows one writer at a time; stop the dashboard before ingestion or
benchmark rebuilds. The CMS Cigna index JSON and NPPES monthly CSV must be
obtained separately; this project does not include download commands for them.

## Install dependencies

From `TiC/`:

```bash
python -m pip install -r requirements.txt
```

From `Benchmarks/`:

```bash
npm ci
```

## Ingest UHC

`fetch_uhc_index.py` writes CSV, while `runner.py` requires a JSON manifest.
Convert the fetched URLs to the runner format before ingesting. Start with a
small limit to test; use `--limit 0` to crawl every top-level index.

From `TiC/`:

```bash
python fetch_uhc_index.py --limit 50 --out uhc_rate_files.csv
python -c "import csv, json; from urllib.parse import urlparse; rows = csv.DictReader(open('uhc_rate_files.csv', encoding='utf-8', newline='')); json.dump({'blobs': [{'name': urlparse(row['rate_file_url']).path.rsplit('/', 1)[-1], 'downloadUrl': row['rate_file_url']} for row in rows]}, open('uhc_manifest.json', 'w', encoding='utf-8'), indent=2)"
python runner.py --manifest uhc_manifest.json --db transparency.duckdb --progress uhc_processed_count.txt --schema schema.sql --run-log uhc_runs.jsonl
```

For the full UHC manifest, rerun the first command with `--limit 0`, then
repeat the conversion and ingestion commands. Use a separate progress file
for each manifest/database ingestion run.

## Ingest Cigna

Download the current Cigna CMS index JSON separately, then provide its path as
the first argument to `fetch_cigna_index.py`. The commands below assume the
index is saved as `cigna_index.json`.

From `TiC/`:

```bash
python fetch_cigna_index.py cigna_index.json --out cigna_in_network_rates.json
python check_cigna_file_sizes.py --manifest cigna_in_network_rates.json --out cigna_file_sizes.json
python create_size_sorted_manifest.py --sizes cigna_file_sizes.json --out cigna_in_network_rates_by_size.json
python runner.py --manifest cigna_in_network_rates_by_size.json --db transparency.duckdb --progress cigna_sorted_into_transparency_processed_count.txt --schema schema.sql --run-log cigna_sorted_runs.jsonl
```

The ingestion command is resumable. For a bounded batch, add `-m 25` to it.
Progress files are specific to their manifest and database; do not reuse a
counter from another ingestion.

## Refresh geographic enrichment

After ingesting new providers, refresh NPPES using the current CMS monthly CSV,
then reload the ZIP/county crosswalk. Missing geographic enrichment does not
remove payer-level rates, but prevents county/provider geography from being
assigned.

From `TiC/`:

```bash
python load_nppes.py --nppes /path/to/npidata_pfile.csv --transparency-db transparency.duckdb --enrichment-db ../Benchmarks/enrichment.duckdb
python load_zip_county.py --enrichment-db ../Benchmarks/enrichment.duckdb
```

## Build benchmarks

Run after ingestion or enrichment changes. A full build materializes the
benchmark table, creates six indexes, and computes six aggregate tables.
`--drop-first` rebuilds the materialized benchmark table.

From `TiC/`:

```bash
python build_benchmarks.py --transparency-db transparency.duckdb --enrichment-db ../Benchmarks/enrichment.duckdb --drop-first
```

For large builds, put DuckDB spill files on a volume with sufficient free
space. The lower-memory path builds the base table first, then its statistics:

```bash
mkdir -p /path/to/duckdb-temp
python build_benchmarks.py --transparency-db transparency.duckdb --enrichment-db ../Benchmarks/enrichment.duckdb --drop-first --skip-indexes --skip-stats --threads 4 --temp-directory /path/to/duckdb-temp
python build_benchmarks.py --transparency-db transparency.duckdb --enrichment-db ../Benchmarks/enrichment.duckdb --stats-only --threads 4 --temp-directory /path/to/duckdb-temp
```

The stats-only command requires the benchmark table from the preceding build.
The low-memory path skips secondary indexes. A manual `CHECKPOINT` is not
needed for normal recovery; DuckDB recovers its write-ahead log when reopening
the database.

The aggregate tables are `benchmarks_code_stats`, `benchmarks_geo_stats`,
`benchmarks_geo_payer_stats`, `benchmarks_payer_stats`,
`benchmarks_payer_provider_stats`, and `benchmarks_provider_stats`.

## Validate the data

Run from `TiC/`:

```bash
python -c "import duckdb; c = duckdb.connect('transparency.duckdb', read_only=True); print(c.execute(\"SELECT reporting_entity_name, COUNT(*) FROM payers WHERE lower(reporting_entity_name) LIKE '%cigna%' OR lower(reporting_entity_name) LIKE '%united%' GROUP BY 1 ORDER BY 2 DESC\").fetchall()); c.close()"
python -c "import duckdb; c = duckdb.connect('transparency.duckdb', read_only=True); print(c.execute('SELECT payer_name, county_name, n_rates, n_providers, rate_min, rate_p25, rate_median, rate_p75, rate_max FROM benchmarks_geo_payer_stats WHERE billing_code = ? AND billing_code_type = ? AND county_fips = ? ORDER BY rate_median DESC', ['59025', 'CPT', '48201']).fetchall()); c.close()"
```

The second query checks the Harris County, Texas comparison for CPT `59025`.

## Run the dashboard

From `Benchmarks/`:

```bash
node server_benchmarks.js
```

Open `http://localhost:5544`. The server defaults to
`../TiC/transparency.duckdb`; set `DUCKDB_PATH` to use another database:

```bash
DUCKDB_PATH=/path/to/transparency.duckdb node server_benchmarks.js
```

The dashboard displays code statistics, state and county rollups,
payer-level rates, payer-by-county comparisons, and provider-level details.

## Operational notes

- Ingest both payers into `transparency.duckdb` for a combined comparison.
- Do not commit the DuckDB files, progress counters, run logs, or manifests
  containing signed download URLs.
- Rebuild the benchmark tables after every ingestion or enrichment change;
  the dashboard reads materialized rollups rather than raw rates.
- If Cigna appears in `benchmarks_payer_stats` but not county results, refresh
  NPPES and the ZIP/county enrichment, then rebuild the benchmarks.
