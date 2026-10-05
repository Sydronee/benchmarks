#!/usr/bin/env python3

"""
TiC - Table 4.8 Performance Measurement Script

Measures the remaining Table 4.8 values without directly modifying
the transparency database.

IMPORTANT:
    - Do NOT run this while another process is performing a benchmark build.
    - API measurements only send GET requests.
    - Database size is measured from the filesystem.
    - Dashboard load time must be measured manually in the browser.

Usage examples:

    python table_4_8_measure.py

    python table_4_8_measure.py ^
        --nppes-cmd "python your_nppes_script.py" ^
        --enrichment-cmd "python your_enrichment_script.py" ^
        --benchmark-cmd "python build_benchmarks.py"

PowerShell:

    python .\table_4_8_measure.py `
        --nppes-cmd "python your_nppes_script.py" `
        --enrichment-cmd "python your_enrichment_script.py" `
        --benchmark-cmd "python build_benchmarks.py"

API:

    python table_4_8_measure.py --api-url http://localhost:3001
"""

import argparse
import os
import subprocess
import sys
import time
import statistics
from pathlib import Path
from datetime import datetime


# ============================================================
# CONFIGURATION
# ============================================================

DEFAULT_API_URL = "http://localhost:3001"

DEFAULT_DB_NAMES = [
    "transparency.duckdb",
    "benchmark.duckdb",
    "benchmarks.duckdb",
]

# Common API endpoints from the TiC benchmark server.
API_ENDPOINTS = [
    "/api/health",
    "/api/code-types",
    "/api/benchmark/summary?code=99213&type=CPT",
]


# ============================================================
# HELPERS
# ============================================================

def format_bytes(value):
    if value is None:
        return "N/A"

    value = float(value)

    units = ["B", "KB", "MB", "GB", "TB"]

    for unit in units:
        if value < 1024:
            return f"{value:.2f} {unit}"
        value /= 1024

    return f"{value:.2f} PB"


def format_seconds(seconds):
    if seconds is None:
        return "NOT MEASURED"

    if seconds < 1:
        return f"{seconds * 1000:.2f} ms"

    if seconds < 60:
        return f"{seconds:.2f} s"

    minutes = seconds / 60

    if minutes < 60:
        return f"{minutes:.2f} min"

    hours = minutes / 60

    return f"{hours:.2f} h"


def find_repo_root():
    current = Path.cwd().resolve()

    candidates = [
        current,
        current.parent,
        current.parent.parent,
    ]

    for path in candidates:
        if (path / "transparency.duckdb").exists():
            return path

    return current


def find_database(repo):
    """
    Find the transparency/benchmark DuckDB database.

    Prefers transparency.duckdb because that is the database used
    by the TiC benchmark server.
    """

    preferred = repo / "transparency.duckdb"

    if preferred.exists():
        return preferred

    for name in DEFAULT_DB_NAMES:
        candidate = repo / name

        if candidate.exists():
            return candidate

    return None


def database_size(repo):
    db = find_database(repo)

    if db is None:
        return None, None

    try:
        size = db.stat().st_size
        return db, size
    except OSError:
        return db, None


# ============================================================
# COMMAND EXECUTION
# ============================================================

def run_timed_command(command, label, cwd):
    """
    Execute a user-supplied command and measure wall-clock duration.

    This is intentionally explicit rather than guessing which script
    performs NPPES/enrichment/benchmark processing.
    """

    print()
    print("=" * 72)
    print(label)
    print("=" * 72)

    print("Command:")
    print(command)

    print()
    print("Starting...")

    start = time.perf_counter()

    try:
        completed = subprocess.run(
            command,
            cwd=str(cwd),
            shell=True,
            text=True,
        )

        elapsed = time.perf_counter() - start

    except KeyboardInterrupt:
        elapsed = time.perf_counter() - start

        print()
        print("Interrupted by user.")

        return {
            "status": "INTERRUPTED",
            "seconds": elapsed,
            "returncode": None,
        }

    except Exception as exc:
        elapsed = time.perf_counter() - start

        print()
        print("ERROR:", repr(exc))

        return {
            "status": "ERROR",
            "seconds": elapsed,
            "returncode": None,
        }

    print()
    print("Return code:", completed.returncode)
    print("Duration:", format_seconds(elapsed))

    if completed.returncode == 0:
        status = "PASS"
    else:
        status = "FAILED"

    return {
        "status": status,
        "seconds": elapsed,
        "returncode": completed.returncode,
    }


# ============================================================
# API MEASUREMENT
# ============================================================

def measure_api(api_url, repetitions):
    """
    Measure HTTP GET response time.

    Uses urllib from the Python standard library so that no
    third-party package is required.
    """

    import urllib.request
    import urllib.error

    print()
    print("=" * 72)
    print("API RESPONSE TIME")
    print("=" * 72)

    api_url = api_url.rstrip("/")

    print("API server:", api_url)
    print("Requests per endpoint:", repetitions)

    results = []

    for endpoint in API_ENDPOINTS:

        url = api_url + endpoint

        print()
        print("Endpoint:", endpoint)

        endpoint_times = []

        for i in range(repetitions):

            start = time.perf_counter()

            try:
                request = urllib.request.Request(
                    url,
                    method="GET",
                    headers={
                        "User-Agent": "TiC-Table-4.8-Measurement"
                    },
                )

                with urllib.request.urlopen(
                    request,
                    timeout=30,
                ) as response:

                    response.read()

                    status_code = response.status

                elapsed = time.perf_counter() - start

                endpoint_times.append(elapsed)

                print(
                    f"  {i + 1:02d}: "
                    f"HTTP {status_code} - "
                    f"{elapsed * 1000:.2f} ms"
                )

            except urllib.error.HTTPError as exc:

                elapsed = time.perf_counter() - start

                print(
                    f"  {i + 1:02d}: "
                    f"HTTP {exc.code} - "
                    f"{elapsed * 1000:.2f} ms"
                )

            except Exception as exc:

                elapsed = time.perf_counter() - start

                print(
                    f"  {i + 1:02d}: "
                    f"ERROR - "
                    f"{elapsed * 1000:.2f} ms"
                )

        if endpoint_times:

            median = statistics.median(endpoint_times)
            mean = statistics.mean(endpoint_times)
            minimum = min(endpoint_times)
            maximum = max(endpoint_times)

            print()
            print("  Median :", f"{median * 1000:.2f} ms")
            print("  Mean   :", f"{mean * 1000:.2f} ms")
            print("  Min    :", f"{minimum * 1000:.2f} ms")
            print("  Max    :", f"{maximum * 1000:.2f} ms")

            results.extend(endpoint_times)

    if not results:
        print()
        print("No successful API measurements were obtained.")

        return None

    overall_median = statistics.median(results)

    print()
    print("Overall median API response time:")
    print(f"{overall_median * 1000:.2f} ms")

    return overall_median


# ============================================================
# DATABASE SIZE
# ============================================================

def measure_database(repo):
    print()
    print("=" * 72)
    print("DATABASE SIZE")
    print("=" * 72)

    db, size = database_size(repo)

    if db is None:

        print("No DuckDB database found.")

        return None

    print("Database:")
    print(db)

    print()
    print("Size:")
    print(format_bytes(size))

    print()
    print("Bytes:")
    print(size)

    return size


# ============================================================
# SCRIPT DISCOVERY
# ============================================================

def show_possible_scripts(repo):

    print()
    print("=" * 72)
    print("POSSIBLE TiC PROCESSING SCRIPTS")
    print("=" * 72)

    patterns = [
        "*nppes*.py",
        "*enrich*.py",
        "*benchmark*.py",
        "*build*.py",
    ]

    found = set()

    for pattern in patterns:

        for path in repo.glob(pattern):

            if path.is_file():
                found.add(path)

    if not found:

        print("No obvious processing scripts found.")

        return

    for path in sorted(found):

        print(" ", path.name)


# ============================================================
# FINAL REPORT
# ============================================================

def print_report(
    repo,
    nppes_result,
    enrichment_result,
    benchmark_result,
    db_size,
    api_median,
):

    print()
    print()
    print("#" * 72)
    print("TABLE 4.8 MEASUREMENT RESULTS")
    print("#" * 72)

    print()
    print("Repository:")
    print(repo)

    print()
    print("Measurement timestamp:")
    print(datetime.now().astimezone().isoformat())

    print()
    print("-" * 72)

    print("NPPES processing duration:")
    print(
        format_seconds(
            nppes_result["seconds"]
            if nppes_result
            else None
        )
    )

    print()

    print("Enrichment duration:")
    print(
        format_seconds(
            enrichment_result["seconds"]
            if enrichment_result
            else None
        )
    )

    print()

    print("Benchmark build duration:")
    print(
        format_seconds(
            benchmark_result["seconds"]
            if benchmark_result
            else None
        )
    )

    print()

    print("Benchmark database size:")
    print(format_bytes(db_size))

    print()

    print("API response time (overall median):")

    if api_median is None:
        print("NOT MEASURED")
    else:
        print(f"{api_median * 1000:.2f} ms")

    print()

    print("Dashboard load time:")
    print("MANUAL MEASUREMENT REQUIRED")

    print()
    print("-" * 72)

    print()
    print("COPY THESE VALUES INTO TABLE 4.8:")
    print()

    print(
        "NPPES processing duration =",
        format_seconds(
            nppes_result["seconds"]
            if nppes_result
            else None
        ),
    )

    print(
        "Enrichment duration =",
        format_seconds(
            enrichment_result["seconds"]
            if enrichment_result
            else None
        ),
    )

    print(
        "Benchmark build duration =",
        format_seconds(
            benchmark_result["seconds"]
            if benchmark_result
            else None
        ),
    )

    print(
        "Benchmark database size =",
        format_bytes(db_size),
    )

    if api_median is not None:
        print(
            "API response time =",
            f"{api_median * 1000:.2f} ms",
        )
    else:
        print("API response time = NOT MEASURED")

    print("Dashboard load time = MANUAL")


# ============================================================
# MAIN
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description="Measure remaining TiC thesis Table 4.8 metrics."
    )

    parser.add_argument(
        "--repo",
        default=None,
        help="TiC repository directory. Defaults to current directory.",
    )

    parser.add_argument(
        "--nppes-cmd",
        default=None,
        help="Command that performs the NPPES processing.",
    )

    parser.add_argument(
        "--enrichment-cmd",
        default=None,
        help="Command that performs enrichment.",
    )

    parser.add_argument(
        "--benchmark-cmd",
        default=None,
        help="Command that performs benchmark construction.",
    )

    parser.add_argument(
        "--api-url",
        default=DEFAULT_API_URL,
        help="Benchmark API base URL.",
    )

    parser.add_argument(
        "--api-repetitions",
        type=int,
        default=5,
        help="Number of API requests per endpoint.",
    )

    parser.add_argument(
        "--no-api",
        action="store_true",
        help="Skip API measurement.",
    )

    args = parser.parse_args()

    if args.repo:

        repo = Path(args.repo).resolve()

    else:

        repo = find_repo_root()

    print()
    print("=" * 72)
    print("TiC - TABLE 4.8 PERFORMANCE MEASUREMENT")
    print("=" * 72)

    print()
    print("Repository:")
    print(repo)

    print()
    print(
        "This script does NOT automatically guess processing commands."
    )

    print(
        "Only commands explicitly supplied by you will be executed."
    )

    # --------------------------------------------------------
    # Show possible scripts
    # --------------------------------------------------------

    show_possible_scripts(repo)

    # --------------------------------------------------------
    # NPPES
    # --------------------------------------------------------

    nppes_result = None

    if args.nppes_cmd:

        nppes_result = run_timed_command(
            args.nppes_cmd,
            "NPPES PROCESSING",
            repo,
        )

    else:

        print()
        print("NPPES processing:")
        print("NOT MEASURED")
        print(
            "No --nppes-cmd supplied."
        )

    # --------------------------------------------------------
    # Enrichment
    # --------------------------------------------------------

    enrichment_result = None

    if args.enrichment_cmd:

        enrichment_result = run_timed_command(
            args.enrichment_cmd,
            "ENRICHMENT",
            repo,
        )

    else:

        print()
        print("Enrichment:")
        print("NOT MEASURED")
        print(
            "No --enrichment-cmd supplied."
        )

    # --------------------------------------------------------
    # Benchmark build
    # --------------------------------------------------------

    benchmark_result = None

    if args.benchmark_cmd:

        print()
        print(
            "WARNING:"
        )
        print(
            "The benchmark command will actually execute the "
            "repository benchmark builder."
        )
        print(
            "Make sure the database is not locked by another "
            "benchmark process."
        )

        answer = input(
            "\nRun benchmark command? [y/N]: "
        ).strip().lower()

        if answer == "y":

            benchmark_result = run_timed_command(
                args.benchmark_cmd,
                "BENCHMARK BUILD",
                repo,
            )

        else:

            print("Benchmark build skipped.")

    else:

        print()
        print("Benchmark build:")
        print("NOT MEASURED")
        print(
            "No --benchmark-cmd supplied."
        )

    # --------------------------------------------------------
    # Database size
    # --------------------------------------------------------

    db_size = measure_database(repo)

    # --------------------------------------------------------
    # API
    # --------------------------------------------------------

    api_median = None

    if not args.no_api:

        api_median = measure_api(
            args.api_url,
            args.api_repetitions,
        )

    else:

        print()
        print("API measurement skipped.")

    # --------------------------------------------------------
    # Final report
    # --------------------------------------------------------

    print_report(
        repo,
        nppes_result,
        enrichment_result,
        benchmark_result,
        db_size,
        api_median,
    )


if __name__ == "__main__":
    main()
