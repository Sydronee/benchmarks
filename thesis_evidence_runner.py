#!/usr/bin/env python3

"""
Table 4.8 measurement collector for the TiC multi-payer project.

Measures, READ-ONLY where possible:

1. Cigna manifest inventory
2. UHC manifest inventory
3. Processed-file counters
4. Cigna/UHC/combined negotiated-rate row counts
5. transparency.duckdb size
6. enrichment.duckdb size
7. Benchmark table/statistical-table row counts
8. Enrichment table row counts
9. Run-log event counts and observed timestamp spans
10. Optional API response-time measurements

It does NOT:
- modify transparency.duckdb
- modify enrichment.duckdb
- download rate files
- run ingestion
- rebuild benchmarks
- run NPPES
- run enrichment loaders
- delete project files

Run from the TiC repository root:

    python measure_table_4_8.py

Optional API measurement:

    python measure_table_4_8.py --api-url http://127.0.0.1:5544

Optional custom endpoint:

    python measure_table_4_8.py ^
        --api-url http://127.0.0.1:5544 ^
        --api-path "/api/benchmark/summary?code=99213&type=CPT" ^
        --api-runs 20
"""

from __future__ import annotations

import argparse
import csv
import json
import platform
import re
import socket
import statistics
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Optional


# ============================================================
# GENERAL HELPERS
# ============================================================

def human_bytes(value: Optional[int]) -> str:
    if value is None:
        return "N/A"

    value = int(value)

    units = [
        ("B", 1),
        ("KB", 1024),
        ("MB", 1024 ** 2),
        ("GB", 1024 ** 3),
        ("TB", 1024 ** 4),
    ]

    for unit, divisor in reversed(units):
        if value >= divisor:
            return f"{value / divisor:.3f} {unit}"

    return f"{value} B"


def duration_text(seconds: Optional[float]) -> str:
    if seconds is None:
        return "N/A"

    seconds = float(seconds)

    if seconds < 60:
        return f"{seconds:.3f} s"

    if seconds < 3600:
        return f"{seconds / 60:.3f} min ({seconds:.1f} s)"

    return f"{seconds / 3600:.3f} h ({seconds:.1f} s)"


def safe_json(path: Path) -> Optional[Any]:
    try:
        return json.loads(
            path.read_text(
                encoding="utf-8"
            )
        )
    except Exception:
        return None


def resolve_repo() -> Path:
    current = Path.cwd().resolve()

    candidates = [
        current,
        current / "TiC",
        current.parent,
    ]

    for p in candidates:
        if (
            (p / "schema.sql").exists()
            and (p / "runner.py").exists()
        ):
            return p

    raise RuntimeError(
        "TiC repository not found.\n"
        "Run this script from the TiC repository root."
    )


# ============================================================
# MANIFEST DETECTION
# ============================================================

def extract_manifest_entries(obj: Any) -> list[dict[str, Any]]:
    """
    Supports the repository's documented manifest forms:
        {"blobs": [...]}
        {"files": [...]}
    and a top-level list.
    """

    if isinstance(obj, dict):

        for key in ("blobs", "files"):

            value = obj.get(key)

            if isinstance(value, list):
                return [
                    x for x in value
                    if isinstance(x, dict)
                ]

    if isinstance(obj, list):

        return [
            x for x in obj
            if isinstance(x, dict)
        ]

    return []


def entry_size(entry: dict[str, Any]) -> Optional[int]:

    for key in (
        "size_bytes",
        "size",
        "expected_size",
        "expected_size_bytes",
    ):

        value = entry.get(key)

        if value is None:
            continue

        try:
            return int(value)
        except Exception:
            pass

    return None


def looks_like_rate_manifest(
    entries: list[dict[str, Any]]
) -> bool:

    if not entries:
        return False

    sample = entries[:10]

    score = 0

    for entry in sample:

        keys = {
            str(k).lower()
            for k in entry.keys()
        }

        if keys.intersection(
            {
                "downloadurl",
                "download_url",
                "url",
                "address",
                "bloburl",
                "path",
            }
        ):
            score += 1

        if keys.intersection(
            {
                "name",
                "filename",
                "file_name",
            }
        ):
            score += 1

    return score >= 2


def classify_manifest(path: Path) -> str:

    name = path.name.lower()

    if "cigna" in name:
        return "Cigna"

    if "uhc" in name or "united" in name:
        return "UHC"

    return "Unknown"


def find_manifests(repo: Path) -> list[dict[str, Any]]:

    found = []

    # Explicitly preferred files first.
    preferred = [
        "cigna_in_network_rates_by_size.json",
        "cigna_in_network_rates.json",
        "cigna_file_sizes.json",
        "uhc_blobs_raw.json",
        "uhc_blobs.json",
        "uhc_manifest.json",
        "uhc_manifests.json",
        "manifest.json",
    ]

    seen = set()

    for filename in preferred:

        path = repo / filename

        if not path.exists():
            continue

        obj = safe_json(path)
        entries = extract_manifest_entries(obj)

        if looks_like_rate_manifest(entries):

            key = str(
                path.resolve()
            ).lower()

            if key not in seen:

                seen.add(key)

                found.append(
                    {
                        "path": path,
                        "payer": classify_manifest(path),
                        "entries": entries,
                    }
                )

    # Then inspect root-level JSON files.
    for path in sorted(
        repo.glob("*.json")
    ):

        key = str(
            path.resolve()
        ).lower()

        if key in seen:
            continue

        obj = safe_json(path)

        if obj is None:
            continue

        entries = extract_manifest_entries(obj)

        if not looks_like_rate_manifest(entries):
            continue

        payer = classify_manifest(path)

        # Avoid treating arbitrary JSON files as UHC/Cigna
        # manifests unless the filename gives us a payer clue.
        if payer == "Unknown":
            continue

        seen.add(key)

        found.append(
            {
                "path": path,
                "payer": payer,
                "entries": entries,
            }
        )

    return found


def manifest_summary(
    manifest: dict[str, Any]
) -> dict[str, Any]:

    entries = manifest["entries"]

    sizes = [
        entry_size(e)
        for e in entries
    ]

    sizes = [
        x for x in sizes
        if x is not None
    ]

    return {
        "path": str(
            manifest["path"].name
        ),
        "payer": manifest["payer"],
        "file_count": len(entries),
        "sized_file_count": len(sizes),
        "compressed_bytes": sum(sizes),
        "smallest_bytes": (
            min(sizes)
            if sizes else None
        ),
        "largest_bytes": (
            max(sizes)
            if sizes else None
        ),
    }


# ============================================================
# PROGRESS COUNTERS
# ============================================================

def find_progress_files(
    repo: Path
) -> list[Path]:

    names = [
        "processed_count.txt",
        "cigna_sorted_processed_count.txt",
        "cigna_sorted_into_transparency_processed_count.txt",
        "cigna_processed_count.txt",
        "uhc_processed_count.txt",
        "uhc_sorted_processed_count.txt",
        "uhc_sorted_into_transparency_processed_count.txt",
        "uhc_progress.txt",
        "cigna_progress.txt",
    ]

    found = []

    for name in names:

        path = repo / name

        if path.exists():
            found.append(path)

    return found


def read_progress(
    path: Path
) -> Optional[int]:

    try:
        value = int(
            path.read_text(
                encoding="utf-8"
            ).strip()
        )

        return value

    except Exception:
        return None


def progress_summary(
    repo: Path
) -> list[dict[str, Any]]:

    output = []

    for path in find_progress_files(repo):

        value = read_progress(path)

        name = path.name.lower()

        payer = "Unknown"

        if "cigna" in name:
            payer = "Cigna"

        elif "uhc" in name or "united" in name:
            payer = "UHC"

        elif name == "processed_count.txt":
            payer = "Shared / inspect"

        output.append(
            {
                "file": path.name,
                "payer": payer,
                "processed": value,
            }
        )

    return output


# ============================================================
# RUN LOGS
# ============================================================

TIMESTAMP_KEYS = (
    "timestamp",
    "timestamp_utc",
    "time",
    "created_at",
    "completed_at",
)


def parse_timestamp(
    event: dict[str, Any]
) -> Optional[float]:

    value = None

    for key in TIMESTAMP_KEYS:

        if key in event:

            value = event[key]

            break

    if not value:
        return None

    text = str(value)

    # Normalize ISO timestamp to UTC-compatible form.
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:

        from datetime import datetime

        dt = datetime.fromisoformat(
            text
        )

        return dt.timestamp()

    except Exception:
        return None


def payer_from_log(path: Path) -> str:

    name = path.name.lower()

    if "cigna" in name:
        return "Cigna"

    if "uhc" in name or "united" in name:
        return "UHC"

    return "Unknown"


def find_run_logs(
    repo: Path
) -> list[Path]:

    paths = []

    for path in repo.glob("*.jsonl"):

        name = path.name.lower()

        if (
            "run" in name
            or "progress" in name
            or "ingest" in name
        ):

            paths.append(path)

    return sorted(
        paths
    )


def analyze_run_log(
    path: Path
) -> dict[str, Any]:

    events = []

    try:

        lines = path.read_text(
            encoding="utf-8",
            errors="replace"
        ).splitlines()

    except Exception as exc:

        return {
            "path": path.name,
            "payer": payer_from_log(path),
            "error": (
                f"{type(exc).__name__}: {exc}"
            ),
        }

    for line in lines:

        if not line.strip():
            continue

        try:

            obj = json.loads(line)

            if isinstance(obj, dict):
                events.append(obj)

        except Exception:
            continue

    complete = [
        e for e in events
        if str(
            e.get("status", "")
        ).lower() == "complete"
    ]

    failed = [
        e for e in events
        if str(
            e.get("status", "")
        ).lower() == "failed"
    ]

    timestamp_values = [
        parse_timestamp(e)
        for e in complete
    ]

    timestamp_values = [
        x for x in timestamp_values
        if x is not None
    ]

    span = None

    if len(timestamp_values) >= 2:

        span = (
            max(timestamp_values)
            - min(timestamp_values)
        )

    return {
        "path": path.name,
        "payer": payer_from_log(path),
        "events": len(events),
        "complete_events": len(complete),
        "failed_events": len(failed),
        "observed_complete_event_span_seconds": span,
    }


# ============================================================
# DUCKDB
# ============================================================

def open_duckdb(
    path: Path
):

    import duckdb

    return duckdb.connect(
        str(path),
        read_only=True
    )


def locate_database(
    repo: Path,
    names: list[str]
) -> Optional[Path]:

    for name in names:

        path = repo / name

        if path.exists():
            return path

    return None


def get_table_names(
    con
) -> set[str]:

    return {
        row[0]
        for row in con.execute(
            "SHOW TABLES"
        ).fetchall()
    }


def count_table(
    con,
    table: str
) -> Optional[int]:

    try:

        return int(
            con.execute(
                f"SELECT COUNT(*) FROM \"{table}\""
            ).fetchone()[0]
        )

    except Exception:
        return None


def database_summary(
    db_path: Path
) -> dict[str, Any]:

    output = {
        "path": str(db_path),
        "bytes": db_path.stat().st_size,
        "human_size": human_bytes(
            db_path.stat().st_size
        ),
    }

    try:

        con = open_duckdb(
            db_path
        )

    except Exception as exc:

        output["open_error"] = (
            f"{type(exc).__name__}: {exc}"
        )

        return output

    try:

        tables = get_table_names(
            con
        )

        output["tables"] = sorted(
            tables
        )

        target_tables = [
            "payers",
            "billing_codes",
            "negotiated_rates",
            "providers",
            "benchmarks",
            "benchmarks_code_stats",
            "benchmarks_geo_stats",
            "benchmarks_geo_payer_stats",
            "benchmarks_payer_stats",
            "benchmarks_payer_provider_stats",
            "benchmarks_provider_stats",
        ]

        for table in target_tables:

            if table in tables:

                output[
                    f"{table}_rows"
                ] = count_table(
                    con,
                    table
                )

        # ----------------------------------------------------
        # Payer-level rate counts
        # ----------------------------------------------------

        if (
            "payers" in tables
            and "negotiated_rates" in tables
        ):

            rows = con.execute(
                """
                SELECT
                    p.payer_id,
                    p.reporting_entity_name,
                    COUNT(nr.*) AS rate_rows
                FROM payers p
                LEFT JOIN negotiated_rates nr
                    ON nr.payer_id = p.payer_id
                GROUP BY
                    p.payer_id,
                    p.reporting_entity_name
                ORDER BY
                    rate_rows DESC
                """
            ).fetchall()

            payer_rows = []

            for row in rows:

                payer_rows.append(
                    {
                        "payer_id": row[0],
                        "reporting_entity_name": row[1],
                        "rate_rows": int(row[2]),
                    }
                )

            output[
                "payer_rate_breakdown"
            ] = payer_rows

            cigna_rate_rows = 0
            uhc_rate_rows = 0

            for row in payer_rows:

                label = str(
                    row[
                        "reporting_entity_name"
                    ] or ""
                ).lower()

                if (
                    "cigna" in label
                    or "cigna health" in label
                ):

                    cigna_rate_rows += (
                        row["rate_rows"]
                    )

                if (
                    "united" in label
                    or "uhc" in label
                ):

                    uhc_rate_rows += (
                        row["rate_rows"]
                    )

            output[
                "cigna_rate_rows_detected"
            ] = cigna_rate_rows

            output[
                "uhc_rate_rows_detected"
            ] = uhc_rate_rows

            output[
                "combined_detected_rate_rows"
            ] = (
                cigna_rate_rows
                + uhc_rate_rows
            )

    except Exception as exc:

        output["query_error"] = (
            f"{type(exc).__name__}: {exc}"
        )

    finally:

        try:
            con.close()
        except Exception:
            pass

    return output


# ============================================================
# ENRICHMENT DATABASE
# ============================================================

def enrichment_summary(
    db_path: Path
) -> dict[str, Any]:

    output = {
        "path": str(db_path),
        "bytes": db_path.stat().st_size,
        "human_size": human_bytes(
            db_path.stat().st_size
        ),
    }

    try:

        con = open_duckdb(
            db_path
        )

    except Exception as exc:

        output["open_error"] = (
            f"{type(exc).__name__}: {exc}"
        )

        return output

    try:

        tables = get_table_names(
            con
        )

        output["tables"] = sorted(
            tables
        )

        counts = {}

        for table in tables:

            value = count_table(
                con,
                table
            )

            if value is not None:
                counts[table] = value

        output["table_rows"] = counts

    except Exception as exc:

        output["query_error"] = (
            f"{type(exc).__name__}: {exc}"
        )

    finally:

        try:
            con.close()
        except Exception:
            pass

    return output


# ============================================================
# OPTIONAL API MEASUREMENT
# ============================================================

def measure_api(
    base_url: str,
    path: str,
    runs: int
) -> dict[str, Any]:

    url = (
        base_url.rstrip("/")
        + "/"
        + path.lstrip("/")
    )

    values = []
    statuses = []
    errors = []

    for _ in range(
        max(1, runs)
    ):

        started = time.perf_counter()

        try:

            request = urllib.request.Request(
                url,
                headers={
                    "User-Agent":
                        "TiC-Thesis-Measurement/1.0"
                }
            )

            with urllib.request.urlopen(
                request,
                timeout=30
            ) as response:

                response.read()

                status = response.status

            elapsed_ms = (
                time.perf_counter()
                - started
            ) * 1000

            values.append(
                elapsed_ms
            )

            statuses.append(
                status
            )

        except Exception as exc:

            elapsed_ms = (
                time.perf_counter()
                - started
            ) * 1000

            errors.append(
                {
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "elapsed_ms": elapsed_ms,
                }
            )

    result = {
        "url": url,
        "requested_runs": runs,
        "successful_runs": len(values),
        "statuses": statuses,
        "errors": errors,
    }

    if values:

        result["median_ms"] = (
            statistics.median(values)
        )

        result["mean_ms"] = (
            statistics.mean(values)
        )

        result["min_ms"] = min(values)
        result["max_ms"] = max(values)

    return result


# ============================================================
# THESIS TABLE 4.8 MAPPING
# ============================================================

def build_table_4_8(
    manifests,
    progress,
    transparency,
    enrichment,
    api_result
):

    cigna_manifest = next(
        (
            x for x in manifests
            if x["payer"] == "Cigna"
        ),
        None
    )

    uhc_manifest = next(
        (
            x for x in manifests
            if x["payer"] == "UHC"
        ),
        None
    )

    cigna_inventory = (
        manifest_summary(
            cigna_manifest
        )
        if cigna_manifest
        else None
    )

    uhc_inventory = (
        manifest_summary(
            uhc_manifest
        )
        if uhc_manifest
        else None
    )

    cigna_processed = None
    uhc_processed = None

    for row in progress:

        if row["payer"] == "Cigna":

            cigna_processed = row[
                "processed"
            ]

        elif row["payer"] == "UHC":

            uhc_processed = row[
                "processed"
            ]

    combined_processed = None

    if (
        cigna_processed is not None
        and uhc_processed is not None
    ):

        combined_processed = (
            cigna_processed
            + uhc_processed
        )

    elif (
        cigna_processed is not None
    ):

        combined_processed = (
            cigna_processed
        )

    elif (
        uhc_processed is not None
    ):

        combined_processed = (
            uhc_processed
        )

    total_inventory_bytes = 0

    if cigna_inventory:

        total_inventory_bytes += (
            cigna_inventory[
                "compressed_bytes"
            ]
        )

    if uhc_inventory:

        total_inventory_bytes += (
            uhc_inventory[
                "compressed_bytes"
            ]
        )

    metrics = {
        "Input data size (compressed)": (
            human_bytes(
                total_inventory_bytes
            )
            if total_inventory_bytes
            else "NOT AVAILABLE"
        ),

        "Number of source files processed": (
            str(combined_processed)
            if combined_processed is not None
            else "NOT AVAILABLE"
        ),

        "Records processed (rate rows stored)": (
            str(
                transparency.get(
                    "combined_detected_rate_rows"
                )
            )
            if transparency.get(
                "combined_detected_rate_rows"
            ) is not None
            else "NOT AVAILABLE"
        ),

        "Ingestion duration": (
            "NOT MEASURED — "
            "run-log event span is not guaranteed "
            "to equal total ingestion time"
        ),

        "Peak memory during ingestion": (
            "NOT MEASURED"
        ),

        "Transparency database size": (
            transparency.get(
                "human_size",
                "NOT AVAILABLE"
            )
        ),

        "NPPES processing duration": (
            "NOT MEASURED"
        ),

        "Enrichment duration": (
            "NOT MEASURED"
        ),

        "Benchmark build duration": (
            "NOT MEASURED"
        ),

        "Benchmark database size": (
            transparency.get(
                "human_size",
                "NOT AVAILABLE"
            )
        ),

        "API response time": (
            (
                f"{api_result['median_ms']:.2f} ms median"
            )
            if api_result
            and "median_ms" in api_result
            else "NOT MEASURED"
        ),

        "Dashboard load time": (
            "MANUAL MEASUREMENT REQUIRED"
        ),
    }

    return metrics


# ============================================================
# OUTPUT
# ============================================================

def write_json(
    path: Path,
    data: Any
):

    path.write_text(
        json.dumps(
            data,
            indent=2,
            default=str
        ),
        encoding="utf-8"
    )


def write_csv(
    path: Path,
    table: dict[str, str]
):

    with path.open(
        "w",
        newline="",
        encoding="utf-8"
    ) as f:

        writer = csv.writer(f)

        writer.writerow(
            [
                "Metric",
                "Value"
            ]
        )

        for key, value in table.items():

            writer.writerow(
                [
                    key,
                    value
                ]
            )


def print_section(title: str):

    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


# ============================================================
# MAIN
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--repo",
        default=".",
        help="TiC repository root"
    )

    parser.add_argument(
        "--out",
        default="table_4_8_measurements",
        help="Output directory"
    )

    parser.add_argument(
        "--api-url",
        default=None,
        help=(
            "Optional API base URL, "
            "e.g. http://127.0.0.1:5544"
        )
    )

    parser.add_argument(
        "--api-path",
        default=(
            "/api/benchmark/"
            "summary?code=99213&type=CPT"
        ),
        help="API path for latency measurement"
    )

    parser.add_argument(
        "--api-runs",
        type=int,
        default=20,
        help="Number of API measurements"
    )

    args = parser.parse_args()

    repo = (
        Path(args.repo)
        .resolve()
    )

    if not (
        (repo / "schema.sql").exists()
        and (repo / "runner.py").exists()
    ):

        print(
            "ERROR: This does not appear to be the TiC repository root."
        )

        return 2

    output = (
        repo / args.out
    )

    output.mkdir(
        parents=True,
        exist_ok=True
    )

    print()
    print("=" * 78)
    print("TiC TABLE 4.8 MEASUREMENT COLLECTOR")
    print("=" * 78)
    print()
    print(
        f"Repository: {repo}"
    )
    print(
        f"Computer: {socket.gethostname()}"
    )
    print(
        f"Python: {platform.python_version()}"
    )
    print(
        f"OS: {platform.platform()}"
    )
    print()
    print(
        "MODE: READ-ONLY"
    )

    # --------------------------------------------------------
    # MANIFESTS
    # --------------------------------------------------------

    print_section(
        "1. SOURCE MANIFESTS"
    )

    manifests = find_manifests(
        repo
    )

    manifest_results = []

    if not manifests:

        print(
            "No runner-compatible UHC/Cigna manifest was found."
        )

    else:

        for manifest in manifests:

            summary = manifest_summary(
                manifest
            )

            manifest_results.append(
                summary
            )

            print()
            print(
                f"Payer: {summary['payer']}"
            )

            print(
                f"Manifest: {summary['path']}"
            )

            print(
                f"Files: {summary['file_count']}"
            )

            print(
                "Files with size metadata: "
                f"{summary['sized_file_count']}"
            )

            print(
                "Compressed size: "
                f"{summary['compressed_bytes']} bytes "
                f"({human_bytes(summary['compressed_bytes'])})"
            )

            if summary[
                "smallest_bytes"
            ] is not None:

                print(
                    "Smallest: "
                    f"{summary['smallest_bytes']} bytes"
                )

                print(
                    "Largest: "
                    f"{summary['largest_bytes']} bytes "
                    f"({human_bytes(summary['largest_bytes'])})"
                )

    # --------------------------------------------------------
    # PROGRESS
    # --------------------------------------------------------

    print_section(
        "2. PROCESSED FILE COUNTERS"
    )

    progress = progress_summary(
        repo
    )

    if not progress:

        print(
            "No known progress counter files found."
        )

    else:

        for row in progress:

            print(
                f"{row['file']:<55} "
                f"{str(row['processed']):>8} "
                f"({row['payer']})"
            )

    # --------------------------------------------------------
    # TRANSPARENCY DATABASE
    # --------------------------------------------------------

    print_section(
        "3. TRANSPARENCY DATABASE"
    )

    transparency_path = locate_database(
        repo,
        [
            "transparency.duckdb",
            "transparency.duckdb.tmp",
        ]
    )

    transparency = {}

    if transparency_path is None:

        print(
            "transparency.duckdb not found."
        )

    else:

        transparency = database_summary(
            transparency_path
        )

        print(
            f"Path: {transparency_path}"
        )

        print(
            "File size: "
            f"{human_bytes(transparency['bytes'])}"
        )

        if "open_error" in transparency:

            print(
                "READ ERROR:"
            )

            print(
                transparency["open_error"]
            )

        else:

            print()

            for table in (
                "payers",
                "billing_codes",
                "negotiated_rates",
                "providers",
                "benchmarks",
                "benchmarks_code_stats",
                "benchmarks_geo_stats",
                "benchmarks_geo_payer_stats",
                "benchmarks_payer_stats",
                "benchmarks_payer_provider_stats",
                "benchmarks_provider_stats",
            ):

                key = (
                    f"{table}_rows"
                )

                if key in transparency:

                    print(
                        f"{table:<38} "
                        f"{transparency[key]:>15,} rows"
                    )

            print()

            print(
                "Detected Cigna rate rows: "
                f"{transparency.get('cigna_rate_rows_detected', 0):,}"
            )

            print(
                "Detected UHC rate rows: "
                f"{transparency.get('uhc_rate_rows_detected', 0):,}"
            )

            print(
                "Detected combined rate rows: "
                f"{transparency.get('combined_detected_rate_rows', 0):,}"
            )

            if transparency.get(
                "payer_rate_breakdown"
            ):

                print()

                print(
                    "Payer breakdown:"
                )

                for row in transparency[
                    "payer_rate_breakdown"
                ]:

                    print(
                        f"  "
                        f"{str(row['reporting_entity_name'])[:55]:<55} "
                        f"{row['rate_rows']:>15,}"
                    )

    # --------------------------------------------------------
    # ENRICHMENT DATABASE
    # --------------------------------------------------------

    print_section(
        "4. ENRICHMENT DATABASE"
    )

    enrichment_path = locate_database(
        repo,
        [
            "enrichment.duckdb"
        ]
    )

    enrichment = {}

    if enrichment_path is None:

        print(
            "enrichment.duckdb not found."
        )

    else:

        enrichment = enrichment_summary(
            enrichment_path
        )

        print(
            f"Path: {enrichment_path}"
        )

        print(
            "File size: "
            f"{human_bytes(enrichment['bytes'])}"
        )

        if "open_error" in enrichment:

            print(
                "READ ERROR:"
            )

            print(
                enrichment["open_error"]
            )

        else:

            for table, rows in sorted(
                enrichment.get(
                    "table_rows",
                    {}
                ).items()
            ):

                print(
                    f"{table:<38} "
                    f"{rows:>15,} rows"
                )

    # --------------------------------------------------------
    # RUN LOGS
    # --------------------------------------------------------

    print_section(
        "5. RUN LOGS"
    )

    run_logs = find_run_logs(
        repo
    )

    log_results = []

    if not run_logs:

        print(
            "No JSON-lines run logs detected."
        )

    else:

        for path in run_logs:

            result = analyze_run_log(
                path
            )

            log_results.append(
                result
            )

            print()

            print(
                f"Log: {result['path']}"
            )

            print(
                f"Payer: {result['payer']}"
            )

            if "error" in result:

                print(
                    f"Error: {result['error']}"
                )

                continue

            print(
                f"Events: {result['events']}"
            )

            print(
                f"Complete: {result['complete_events']}"
            )

            print(
                f"Failed: {result['failed_events']}"
            )

            span = result.get(
                "observed_complete_event_span_seconds"
            )

            if span is not None:

                print(
                    "Observed complete-event span: "
                    f"{duration_text(span)}"
                )

    # --------------------------------------------------------
    # OPTIONAL API
    # --------------------------------------------------------

    api_result = None

    if args.api_url:

        print_section(
            "6. API RESPONSE-TIME MEASUREMENT"
        )

        print(
            f"URL: "
            f"{args.api_url.rstrip('/')}"
            f"/{args.api_path.lstrip('/')}"
        )

        print(
            f"Runs: {args.api_runs}"
        )

        api_result = measure_api(
            args.api_url,
            args.api_path,
            args.api_runs
        )

        print(
            "Successful requests: "
            f"{api_result['successful_runs']}"
        )

        if "median_ms" in api_result:

            print(
                f"Median: "
                f"{api_result['median_ms']:.2f} ms"
            )

            print(
                f"Mean: "
                f"{api_result['mean_ms']:.2f} ms"
            )

            print(
                f"Minimum: "
                f"{api_result['min_ms']:.2f} ms"
            )

            print(
                f"Maximum: "
                f"{api_result['max_ms']:.2f} ms"
            )

        if api_result["errors"]:

            print()

            print(
                "Errors:"
            )

            for error in api_result["errors"]:

                print(
                    f"  "
                    f"{error['type']}: "
                    f"{error['message']}"
                )

    else:

        print_section(
            "6. API RESPONSE-TIME MEASUREMENT"
        )

        print(
            "Skipped. Start the benchmark server and rerun "
            "with --api-url to measure this automatically."
        )

    # --------------------------------------------------------
    # TABLE 4.8
    # --------------------------------------------------------

    print_section(
        "7. TABLE 4.8 MAPPING"
    )

    table_4_8 = build_table_4_8(
        manifests,
        progress,
        transparency,
        enrichment,
        api_result
    )

    for metric, value in table_4_8.items():

        print(
            f"{metric:<42} {value}"
        )

    # --------------------------------------------------------
    # SAVE RESULTS
    # --------------------------------------------------------

    print_section(
        "8. SAVING EVIDENCE"
    )

    complete_report = {
        "generated_local_time":
            time.strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        "repository":
            str(repo),
        "environment": {
            "python":
                platform.python_version(),
            "platform":
                platform.platform(),
            "machine":
                platform.machine(),
            "processor":
                platform.processor(),
        },
        "manifests":
            manifest_results,
        "progress":
            progress,
        "transparency_db":
            transparency,
        "enrichment_db":
            enrichment,
        "run_logs":
            log_results,
        "api":
            api_result,
        "table_4_8":
            table_4_8,
    }

    json_path = (
        output /
        "table_4_8_measurements.json"
    )

    csv_path = (
        output /
        "table_4_8_measurements.csv"
    )

    md_path = (
        output /
        "table_4_8_measurements.md"
    )

    write_json(
        json_path,
        complete_report
    )

    write_csv(
        csv_path,
        table_4_8
    )

    md_lines = [
        "# Table 4.8 Measurement Evidence",
        "",
        f"Repository: `{repo}`",
        "",
        "## Table 4.8 values",
        "",
        "| Metric | Value |",
        "|---|---|",
    ]

    for metric, value in table_4_8.items():

        md_lines.append(
            f"| {metric} | {value} |"
        )

    md_lines.extend(
        [
            "",
            "## Caution",
            "",
            "Run-log timestamp spans are reported as observed event spans "
            "and are not automatically treated as complete ingestion duration.",
            "No production-scale performance value is inferred from source-code "
            "constants.",
        ]
    )

    md_path.write_text(
        "\n".join(
            md_lines
        ) + "\n",
        encoding="utf-8"
    )

    print(
        json_path
    )

    print(
        csv_path
    )

    print(
        md_path
    )

    print()
    print(
        "Done. No ingestion, benchmark rebuild, or database modification "
        "was performed."
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(
        main()
    )
