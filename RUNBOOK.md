# UHC and Cigna Benchmark Runbook

This runbook builds one comparison database containing UHC and Cigna rates,
then serves the county and payer comparison dashboard.

## Repository Layout

- `TiC/` downloads and ingests transparency files.
- `TiC/transparency.duckdb` is the shared raw-rate database.
- `TiC/enrichment.duckdb` contains NPPES, ZIP/county, and code reference data.
- `Benchmarks/` builds benchmark rollups and serves the dashboard.

DuckDB allows one writer at a time. Stop the dashboard before ingesting or
rebuilding the database.

## Install Dependencies

From `TiC/`:

```bash
python -m pip install -r requirements.txt
```

From `Benchmarks/`:

```bash
npm install
```

## Ingest UHC

Create or refresh the UHC manifest, then ingest it into the shared database:

```bash
cd /home/syd/Documents/GitHub/TiC

python fetch_uhc_index.py --limit 50 --out in_network_rates_filtered.json

python runner.py \
  --manifest in_network_rates_filtered.json \
  --db transparency.duckdb \
  --progress uhc_into_transparency_processed_count.txt \
  --schema schema.sql \
  --run-log uhc_into_transparency_runs.jsonl
```

Use the complete UHC manifest instead of `--limit 50` when appropriate.

## Ingest Cigna

Create a Cigna manifest from the CMS index if needed:

```bash
cd /home/syd/Documents/GitHub/TiC

python fetch_cigna_index.py \
  2026-09-01_cigna-health-life-insurance-company_index.json \
  --out cigna_in_network_rates.json

python check_cigna_file_sizes.py
python create_size_sorted_manifest.py
```

Ingest the size-sorted manifest into the same `transparency.duckdb` used by
UHC. Use a new progress file for this shared-database run:

```bash
python runner.py \
  --manifest cigna_in_network_rates_by_size.json \
  --db transparency.duckdb \
  --progress cigna_sorted_into_transparency_processed_count.txt \
  --schema schema.sql \
  --run-log cigna_sorted_into_transparency_runs.jsonl
```

The command is resumable. Re-running it continues from the progress counter.
For a bounded batch, use `-m 25` with a space before the line-continuation
backslash:

```bash
python runner.py -m 25 \
  --manifest cigna_in_network_rates_by_size.json \
  --db transparency.duckdb \
  --progress cigna_sorted_into_transparency_processed_count.txt \
  --schema schema.sql \
  --run-log cigna_sorted_into_transparency_runs.jsonl
```

Do not use `-m 25\` because the shell can pass `25n` as the argument.

## Refresh Geographic Enrichment

County results require provider NPIs to be present in NPPES and ZIP codes to
be present in the ZIP/county crosswalk. After new Cigna or UHC providers are
ingested, refresh NPPES using the current CMS monthly CSV:

```bash
cd /home/syd/Documents/GitHub/TiC

python load_nppes.py \
  --nppes /path/to/npidata_pfile_*.csv \
  --transparency-db transparency.duckdb \
  --enrichment-db enrichment.duckdb

python load_zip_county.py \
  --enrichment-db enrichment.duckdb
```

If a newly ingested provider is absent from NPPES, its payer-level rates still
appear in the dashboard, but county/provider geography cannot be assigned.

## Build Benchmarks

Run this after ingestion or enrichment changes:

```bash
cd /home/syd/Documents/GitHub/TiC

python build_benchmarks.py --transparency-db transparency.duckdb --enrichment-db enrichment.duckdb --drop-first
```

The build materializes a second copy of the rates, creates six indexes, and
computes several percentile-heavy aggregate tables. Its working footprint can
therefore be several times larger than the source database, especially when
DuckDB is recovering an interrupted transaction. Put temporary files on a
volume with enough free space and, for a lower-memory initial build, use:

```bash
mkdir -p /path/to/duckdb-temp

python build_benchmarks.py --transparency-db cigna.duckdb --enrichment-db enrichment.duckdb --drop-first --skip-indexes --skip-stats --threads 4 --temp-directory /path/to/duckdb-temp
```

Build the statistics separately:

```bash
python build_benchmarks.py --transparency-db cigna.duckdb --enrichment-db enrichment.duckdb --stats-only --threads 4 --temp-directory /path/to/duckdb-temp
```

After an interrupted run, first ensure no process is still using the database,
then consolidate recovery data before rebuilding:

```bash
python -c "import duckdb; con = duckdb.connect('cigna.duckdb'); con.execute('CHECKPOINT'); con.close()"
```

Use `--drop-first` when rebuilding. Without it, `CREATE TABLE IF NOT EXISTS
benchmarks` can leave an old benchmark table untouched.

This creates the base `benchmarks` table and the comparison rollups,
including:

- `benchmarks_geo_stats`
- `benchmarks_geo_payer_stats`
- `benchmarks_payer_stats`
- `benchmarks_payer_provider_stats`
- `benchmarks_provider_stats`

## Validate the Data

Check that both payers are loaded:

```bash
cd /home/syd/Documents/GitHub/TiC

python -c "import duckdb; c=duckdb.connect('transparency.duckdb', read_only=True); print(c.execute(\"SELECT reporting_entity_name, COUNT(*) FROM payers WHERE lower(reporting_entity_name) LIKE '%cigna%' OR lower(reporting_entity_name) LIKE '%united%' GROUP BY 1 ORDER BY 2 DESC\").fetchall()); c.close()"
```

Check a known shared county example:

```bash
duckdb -readonly transparency.duckdb <<'SQL'
SELECT payer_name, county_name, n_rates, n_providers,
       rate_min, rate_p25, rate_median, rate_p75, rate_max
FROM benchmarks_geo_payer_stats
WHERE billing_code = '59025'
  AND billing_code_type = 'CPT'
  AND county_fips = '48201'
ORDER BY rate_median DESC;
SQL
```

This currently gives the Harris County, Texas comparison for CPT `59025`.

## Run the Dashboard

From `Benchmarks/`:

```bash
cd /home/syd/Documents/GitHub/Benchmarks
node server_benchmarks.js
```

Open `http://localhost:5544`.

The server defaults to `../TiC/transparency.duckdb`. To use another database:

```bash
DUCKDB_PATH=/path/to/transparency.duckdb node server_benchmarks.js
```

Search for a CPT and type. The dashboard shows:

- overall code statistics
- state and county rollups
- all payer-level rates, including providers without geographic enrichment
- payer-by-county comparisons where geography is available
- provider-level drill-down for a selected payer and county

## Operational Notes

- Do not ingest Cigna into a separate database if the goal is one combined
  comparison. Write both UHC and Cigna into `transparency.duckdb`.
- Do not commit `transparency.duckdb`, progress counters, run logs, or signed
  manifests containing temporary download URLs.
- Rebuild benchmarks after any ingestion or enrichment change; the dashboard
  reads materialized benchmark tables rather than raw rates.
- If the dashboard shows UHC but not Cigna, first check `benchmarks_payer_stats`
  and rebuild with `--drop-first`. If Cigna appears there but not by county,
  refresh NPPES and the ZIP/county enrichment.