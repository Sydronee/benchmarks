#!/usr/bin/env python3
"""Fills the numbers for the 'Data Inputs' sheet from your DuckDB files (read-only).

Run from the TiC folder:
    pip install duckdb
    python collect_metrics.py --transparency transparency.duckdb --enrichment enrichment.duckdb

Outputs:
    data_inputs_paste.txt : one value per line, aligned to Data Inputs rows 5..49.
                            Select cell C5 on the 'Data Inputs' sheet and paste (Ctrl+V).
    metrics_results.csv   : key,value,error  (for checking / screenshots)
Rows that need manual entry (db_gb, validation counts) are left blank; the two pre-filled
counters keep their snapshot value. A query that fails (e.g. table not built yet) leaves the cell blank.
"""
import argparse, csv, os
import duckdb

SPEC = [(5, None, None, None, None), (6, 'uhc_files', '', None, 724), (7, 'cigna_files', '', None, 25), (8, 'src_files_db', 'T', 'SELECT COUNT(DISTINCT source_file) FROM negotiated_rates', None), (9, 'rows_total', 'T', 'SELECT COUNT(*) FROM negotiated_rates', None), (10, 'rows_uhc', 'T', "SELECT COUNT(*) FROM negotiated_rates r JOIN payers p USING(payer_id) WHERE lower(p.reporting_entity_name) LIKE '%united%'", None), (11, 'rows_cigna', 'T', "SELECT COUNT(*) FROM negotiated_rates r JOIN payers p USING(payer_id) WHERE lower(p.reporting_entity_name) LIKE '%cigna%'", None), (12, 'payer_rows', 'T', 'SELECT COUNT(*) FROM payers', None), (13, 'payer_entities', 'T', 'SELECT COUNT(DISTINCT reporting_entity_name) FROM payers', None), (14, 'plans', 'T', 'SELECT COUNT(DISTINCT plan_id) FROM payers', None), (15, 'billing_codes', 'T', 'SELECT COUNT(*) FROM billing_codes', None), (16, 'distinct_codes', 'T', "SELECT COUNT(DISTINCT billing_code||'|'||billing_code_type) FROM billing_codes", None), (17, 'code_types', 'T', 'SELECT COUNT(DISTINCT billing_code_type) FROM billing_codes', None), (18, 'provider_rows', 'T', 'SELECT COUNT(*) FROM providers', None), (19, 'npi_distinct', 'T', 'SELECT COUNT(DISTINCT npi) FROM providers', None), (20, 'tin_distinct', 'T', 'SELECT COUNT(DISTINCT tin_value) FROM providers', None), (21, 'db_gb', 'F', None, None), (22, None, None, None, None), (23, 'nppes_rows', 'E', 'SELECT COUNT(*) FROM nppes', None), (24, 'zip_rows', 'E', 'SELECT COUNT(*) FROM zip_county', None), (25, 'code_desc_rows', 'E', 'SELECT COUNT(*) FROM code_descriptions', None), (26, 'taxonomy_rows', 'E', 'SELECT COUNT(*) FROM taxonomy_descriptions', None), (27, None, None, None, None), (28, 'bm_rows', 'T', 'SELECT COUNT(*) FROM benchmarks', None), (29, 'bm_npi_pct', 'T', 'SELECT COUNT(npi)*1.0/COUNT(*) FROM benchmarks', None), (30, 'bm_state_pct', 'T', 'SELECT COUNT(provider_state)*1.0/COUNT(*) FROM benchmarks', None), (31, 'bm_county_pct', 'T', 'SELECT COUNT(county_fips)*1.0/COUNT(*) FROM benchmarks', None), (32, 'bm_canon_pct', 'T', 'SELECT SUM(has_canonical_description::INT)*1.0/COUNT(*) FROM benchmarks', None), (33, 'bm_states', 'T', 'SELECT COUNT(DISTINCT provider_state) FROM benchmarks', None), (34, 'bm_counties', 'T', 'SELECT COUNT(DISTINCT county_fips) FROM benchmarks', None), (35, 'bm_codes', 'T', "SELECT COUNT(DISTINCT billing_code||'|'||billing_code_type) FROM benchmarks", None), (36, 'bm_providers', 'T', 'SELECT COUNT(DISTINCT npi) FROM benchmarks', None), (37, 'bm_multi_payer', 'T', 'SELECT COUNT(*) FROM (SELECT billing_code,billing_code_type,county_fips FROM benchmarks_geo_payer_stats WHERE county_fips IS NOT NULL GROUP BY 1,2,3 HAVING COUNT(DISTINCT payer_name)>=2)', None), (38, 'bm_shared_codes', 'T', 'SELECT COUNT(*) FROM (SELECT billing_code,billing_code_type FROM benchmarks_payer_stats GROUP BY 1,2 HAVING COUNT(DISTINCT payer_name)>=2)', None), (39, 't_code_stats', 'T', 'SELECT COUNT(*) FROM benchmarks_code_stats', None), (40, 't_geo_stats', 'T', 'SELECT COUNT(*) FROM benchmarks_geo_stats', None), (41, 't_geo_payer', 'T', 'SELECT COUNT(*) FROM benchmarks_geo_payer_stats', None), (42, 't_payer', 'T', 'SELECT COUNT(*) FROM benchmarks_payer_stats', None), (43, 't_payer_prov', 'T', 'SELECT COUNT(*) FROM benchmarks_payer_provider_stats', None), (44, 't_provider', 'T', 'SELECT COUNT(*) FROM benchmarks_provider_stats', None), (45, None, None, None, None), (46, 'null_rate_cnt', 'T', 'SELECT COUNT(*) FROM negotiated_rates WHERE negotiated_rate IS NULL OR negotiated_rate<=0', None), (47, 'npi_invalid', 'T', 'SELECT COUNT(DISTINCT npi) FROM providers WHERE npi IS NOT NULL AND (npi<1000000000 OR npi>=3000000000)', None), (48, 'val_errors', 'F', None, None), (49, 'val_warns', 'F', None, None)]  # (sheet_row, key, db, sql, default)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--transparency", default="transparency.duckdb")
    ap.add_argument("--enrichment", default="enrichment.duckdb")
    a = ap.parse_args()
    paths = {"T": a.transparency, "E": a.enrichment}
    conns = {k: duckdb.connect(p, read_only=True) for k, p in paths.items() if os.path.exists(p)}
    # blank-lined value per sheet row
    out, results = [], []
    for row, key, db, sql, default in SPEC:
        val, err = ("" if default is None else default), ""
        if key and sql and db in conns:
            try:
                val = conns[db].execute(sql).fetchone()[0]
                if val is None: val = ""
            except Exception as e:
                val, err = "", str(e).splitlines()[0]
        elif key and sql:
            err = "database file not found"
        if key: results.append((key, val, err))
        out.append("" if val is None else str(val))
    with open("data_inputs_paste.txt", "w") as f: f.write("\n".join(out) + "\n")
    with open("metrics_results.csv", "w", newline="") as f:
        w = csv.writer(f); w.writerow(["key", "value", "error"]); w.writerows(results)
    for k, v, e in results: print(f"{k:18} {v!s:>20}  {e}")
    print("\nWrote data_inputs_paste.txt (paste into Data Inputs!C5) and metrics_results.csv")

if __name__ == "__main__":
    main()
