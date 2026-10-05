#!/usr/bin/env python3
"""
Targeted rerun of the remaining local TiC/Cigna thesis cases.

Runs ONLY local tests:
  TC-19, TC-20, TC-21, TC-23, TC-27, TC-29, TC-30, TC-31, TC-32, TC-33.

It does NOT start or call the benchmark API.
It does NOT modify transparency.duckdb or enrichment.duckdb.
It uses isolated temporary fixtures and the repository's own modules.

Place this beside thesis_evidence_runner_cigna_v1_2.py in the TiC repo root,
then run:

    python thesis_targeted_cigna_rerun.py
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


@dataclass
class Result:
    case: str
    status: str
    actual: str
    note: str = ""


def load_previous_runner(repo: Path):
    """Import helper functions/constants from the repository-specific v1.2 runner."""
    path = repo / "thesis_evidence_runner_cigna_v1_2.py"

    if not path.exists():
        raise FileNotFoundError(
            "Missing thesis_evidence_runner_cigna_v1_2.py. "
            "Put this script in the same TiC folder as v1.2."
        )

    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "tic_prev_runner",
        path
    )

    if spec is None or spec.loader is None:
        raise RuntimeError(
            "Could not load thesis_evidence_runner_cigna_v1_2.py"
        )

    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    return mod


class LocalServer:
    """
    Robust local HTTP fixture server.

    The important difference from the previous fixture server is that every
    response explicitly closes the connection. This avoids the local HTTP
    fixture becoming the cause of runner-resume failures.
    """

    def __init__(
        self,
        files: dict[str, bytes],
        missing: set[str] | None = None
    ):
        self.files = files
        self.missing = missing or set()

        parent = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):  # noqa: N802
                name = self.path.split("?", 1)[0].lstrip("/")

                if name in parent.missing or name not in parent.files:
                    body = b"not found"

                    self.send_response(404)
                    self.send_header(
                        "Content-Length",
                        str(len(body))
                    )
                    self.send_header(
                        "Connection",
                        "close"
                    )
                    self.end_headers()

                    self.wfile.write(body)
                    self.close_connection = True
                    return

                body = parent.files[name]

                self.send_response(200)
                self.send_header(
                    "Content-Type",
                    "application/octet-stream"
                )
                self.send_header(
                    "Content-Length",
                    str(len(body))
                )
                self.send_header(
                    "Connection",
                    "close"
                )
                self.end_headers()

                self.wfile.write(body)
                self.close_connection = True

            def log_message(self, *_args):
                return

        self.server = ThreadingHTTPServer(
            ("127.0.0.1", 0),
            Handler
        )

        self.thread = threading.Thread(
            target=self.server.serve_forever,
            daemon=True
        )

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_port}"

    def start(self):
        self.thread.start()
        return self

    def stop(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


def write_json(path: Path, obj: Any):
    path.write_text(
        json.dumps(obj, indent=2),
        encoding="utf-8"
    )


def run_cmd(
    cmd: list[str],
    cwd: Path,
    timeout: int = 300
) -> subprocess.CompletedProcess[str]:

    return subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=timeout
    )


def read_progress(path: Path) -> int:
    try:
        return int(
            path.read_text(
                encoding="utf-8"
            ).strip() or "0"
        )
    except Exception:
        return 0


def read_events(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    out = []

    for line in path.read_text(
        encoding="utf-8",
        errors="replace"
    ).splitlines():

        if not line.strip():
            continue

        try:
            out.append(json.loads(line))
        except Exception:
            pass

    return out


def runner_integration(
    repo: Path,
    root: Path,
    mod
) -> list[Result]:

    results: list[Result] = []

    raw = [
        json.dumps(
            mod.synthetic_mrf("RUN-T01"),
            separators=(",", ":")
        ).encode(),

        json.dumps(
            mod.synthetic_mrf("RUN-T02"),
            separators=(",", ":")
        ).encode(),

        json.dumps(
            mod.synthetic_mrf("RUN-T03"),
            separators=(",", ":")
        ).encode(),
    ]

    files = {
        "file1.json": raw[0],
        "file2.json": raw[1],
        "file3.json": raw[2],
    }

    server = LocalServer(
        files,
        missing={"missing.bin"}
    ).start()

    try:

        manifest = root / "runner_manifest.json"
        progress = root / "runner_progress.txt"
        log = root / "runner_runs.jsonl"
        db = root / "runner.duckdb"
        downloads = root / "downloads"

        write_json(
            manifest,
            {
                "blobs": [
                    {
                        "name": name,
                        "downloadUrl": f"{server.url}/{name}",
                        "size_bytes": len(files[name])
                    }
                    for name in (
                        "file1.json",
                        "file2.json",
                        "file3.json"
                    )
                ]
            }
        )

        def run_runner(
            max_files: int | None = None
        ):
            cmd = [
                sys.executable,
                str(repo / "runner.py"),

                "--manifest",
                str(manifest),

                "--db",
                str(db),

                "--progress",
                str(progress),

                "--schema",
                str(repo / "schema.sql"),

                "--download-dir",
                str(downloads),

                "--run-log",
                str(log)
            ]

            if max_files is not None:
                cmd += [
                    "--max-files",
                    str(max_files)
                ]

            return run_cmd(cmd, repo)

        # -------------------------------------------------------------
        # TC-19
        #
        # Run two files first, then resume from progress=2.
        # -------------------------------------------------------------

        if progress.exists():
            progress.unlink()

        if log.exists():
            log.unlink()

        if db.exists():
            db.unlink()

        if downloads.exists():
            shutil.rmtree(downloads)

        first = run_runner(2)

        second = run_runner()

        events = read_events(log)

        complete = [
            e
            for e in events
            if e.get("status") == "complete"
        ]

        left = (
            [
                p.name
                for p in downloads.glob("*")
            ]
            if downloads.exists()
            else []
        )

        ok19 = (
            first.returncode == 0
            and second.returncode == 0
            and read_progress(progress) == 3
            and len(complete) == 3
            and not left
        )

        details19 = (
            f"first_rc={first.returncode}; "
            f"second_rc={second.returncode}; "
            f"progress={read_progress(progress)}; "
            f"complete_events={len(complete)}; "
            f"downloads_left={left}"
        )

        results.append(
            Result(
                "TC-19",
                "PASS" if ok19 else "FAIL",
                details19,
                "Resume check uses a connection-closing local HTTP fixture."
            )
        )

        # -------------------------------------------------------------
        # TC-20
        #
        # No work remains after all files have completed.
        # -------------------------------------------------------------

        third = run_runner()

        ok20 = (
            third.returncode == 0
            and
            "All files have already been processed!"
            in third.stdout
        )

        details20 = (
            f"returncode={third.returncode}; "
            f"message_found="
            f"{'All files have already been processed!' in third.stdout}"
        )

        results.append(
            Result(
                "TC-20",
                "PASS" if ok20 else "FAIL",
                details20
            )
        )

        # -------------------------------------------------------------
        # TC-21
        #
        # Missing download must be logged as failed and progress must
        # remain at zero.
        # -------------------------------------------------------------

        fm = root / "failure_manifest.json"
        fp = root / "failure_progress.txt"
        fl = root / "failure_runs.jsonl"
        fd = root / "failure.duckdb"
        fdir = root / "failure_downloads"

        write_json(
            fm,
            {
                "blobs": [
                    {
                        "name": "missing.bin",
                        "downloadUrl":
                            f"{server.url}/missing.bin",
                        "size_bytes": 50
                    },
                    {
                        "name": "file1.json",
                        "downloadUrl":
                            f"{server.url}/file1.json",
                        "size_bytes":
                            len(files["file1.json"])
                    },
                ]
            }
        )

        proc = run_cmd(
            [
                sys.executable,
                str(repo / "runner.py"),

                "--manifest",
                str(fm),

                "--db",
                str(fd),

                "--progress",
                str(fp),

                "--schema",
                str(repo / "schema.sql"),

                "--download-dir",
                str(fdir),

                "--run-log",
                str(fl)
            ],
            repo,
            timeout=180
        )

        fevents = read_events(fl)

        failed = [
            e
            for e in fevents
            if e.get("status") == "failed"
        ]

        ok21 = (
            proc.returncode == 0
            and read_progress(fp) == 0
            and len(failed) >= 1
        )

        details21 = (
            f"returncode={proc.returncode}; "
            f"progress={read_progress(fp)}; "
            f"failed_events={len(failed)}"
        )

        results.append(
            Result(
                "TC-21",
                "PASS" if ok21 else "FAIL",
                details21,
                "Runner should handle the download failure internally "
                "and stop without advancing progress."
            )
        )

    finally:
        server.stop()

    return results


def make_validation_baseline(
    db: Path,
    repo: Path,
    mod
):
    """
    Create a deliberately clean validation fixture.

    The fact table explicitly inserts all 13 schema columns, including
    ingested_at, so the synthetic fixture cannot fail because of a
    column/value-count mismatch.
    """

    import duckdb

    mod.new_transparency_db(
        db,
        repo
    )

    con = duckdb.connect(str(db))

    try:

        payer = con.execute(
            """
            INSERT INTO payers (
                reporting_entity_name,
                reporting_entity_type,
                plan_name,
                plan_id,
                plan_id_type,
                plan_market_type,
                last_updated_on,
                version,
                source_file
            )
            VALUES (?,?,?,?,?,?,?,?,?)
            RETURNING payer_id
            """,
            (
                "Synthetic Cigna Test Payer",
                "health insurance issuer",
                "Plan",
                "P-VALID",
                "HIOS",
                "group",
                "2026-09-01",
                "1",
                "valid.json",
            )
        ).fetchone()[0]

        code = con.execute(
            """
            INSERT INTO billing_codes (
                billing_code,
                billing_code_type,
                billing_code_type_version,
                description,
                name,
                negotiation_arrangement
            )
            VALUES (?,?,?,?,?,?)
            RETURNING code_id
            """,
            (
                "99213",
                "CPT",
                "2026",
                "Office visit",
                "Office Visit",
                "ffs",
            )
        ).fetchone()[0]

        npi = mod.NPI_ORG

        con.execute(
            """
            INSERT INTO providers (
                provider_reference_id,
                npi,
                tin_type,
                tin_value,
                facility_name,
                network_name,
                group_key
            )
            VALUES (?,?,?,?,?,?,?)
            """,
            (
                101,
                npi,
                "ein",
                "12-3456789",
                "Valid Facility",
                ["Synthetic Network"],
                "ref:101",
            )
        )

        # Explicitly all 13 negotiated_rates columns.
        con.execute(
            """
            INSERT INTO negotiated_rates (
                payer_id,
                code_id,
                negotiation_arrangement,
                billing_class,
                setting,
                negotiated_type,
                negotiated_rate,
                service_code,
                billing_code_modifier,
                expiration_date,
                provider_reference_ids,
                source_file,
                ingested_at
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?, ?, CURRENT_TIMESTAMP)
            """,
            (
                payer,
                code,
                "ffs",
                "institutional",
                "outpatient",
                "negotiated",
                125.0,
                ["11"],
                [],
                "2026-12-31",
                [101],
                "valid.json",
            )
        )

        con.execute("CHECKPOINT")

    finally:
        con.close()


def validation_case(
    repo: Path,
    db: Path,
    outdir: Path,
    fast: bool = False
):

    txt = outdir / (
        db.stem +
        ("_fast.txt" if fast else ".txt")
    )

    js = outdir / (
        db.stem +
        ("_fast.json" if fast else ".json")
    )

    cmd = [
        sys.executable,
        str(repo / "validate_data.py"),

        "--db",
        str(db),

        "--txt",
        str(txt),

        "--json",
        str(js),

        "--fail-on",
        "error"
    ]

    if fast:
        cmd.append("--fast")

    p = run_cmd(
        cmd,
        repo,
        timeout=180
    )

    data = (
        json.loads(
            js.read_text(
                encoding="utf-8"
            )
        )
        if js.exists()
        else []
    )

    return p, data


def check_by_name(
    data: list[dict[str, Any]],
    name: str
):

    return next(
        (
            x for x in data
            if x.get("name") == name
        ),
        None
    )


def benchmark_case(
    repo: Path,
    root: Path,
    mod,
    approx: bool
) -> tuple[Path, str]:

    """
    Create a benchmark fixture with all 13 negotiated_rates columns
    explicitly supplied.
    """

    import duckdb
    import build_benchmarks

    suffix = (
        "approx"
        if approx
        else "exact"
    )

    db = root / (
        f"benchmark_{suffix}.duckdb"
    )

    enr = root / (
        f"enrichment_{suffix}.duckdb"
    )

    # -------------------------------------------------------------
    # Core DB
    # -------------------------------------------------------------

    mod.new_transparency_db(
        db,
        repo
    )

    # -------------------------------------------------------------
    # Enrichment DB
    # -------------------------------------------------------------

    e = duckdb.connect(
        str(enr)
    )

    try:

        e.execute(
            """
            CREATE TABLE nppes (
                npi BIGINT PRIMARY KEY,
                entity_type VARCHAR,
                provider_name VARCHAR,
                first_name VARCHAR,
                last_name VARCHAR,
                org_name VARCHAR,
                city VARCHAR,
                state VARCHAR,
                zip5 VARCHAR,
                taxonomy_code VARCHAR,
                taxonomy_is_primary VARCHAR
            )
            """
        )

        e.execute(
            """
            CREATE TABLE zip_county (
                zip5 VARCHAR,
                fips VARCHAR,
                county_name VARCHAR,
                state_abbr VARCHAR,
                tot_ratio DOUBLE
            )
            """
        )

        e.execute(
            """
            CREATE TABLE code_descriptions (
                billing_code VARCHAR NOT NULL,
                billing_code_type VARCHAR NOT NULL,
                description VARCHAR NOT NULL,
                PRIMARY KEY (
                    billing_code,
                    billing_code_type
                )
            )
            """
        )

        e.executemany(
            """
            INSERT INTO nppes
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            [
                (
                    mod.NPI_ORG,
                    "2",
                    "Org Provider",
                    None,
                    None,
                    "Org Provider",
                    "Houston",
                    "TX",
                    "77001",
                    "207Q00000X",
                    "Y",
                ),
                (
                    mod.NPI_INDIV,
                    "1",
                    "Individual Provider",
                    "Ada",
                    "Example",
                    None,
                    "Houston",
                    "TX",
                    "77001",
                    "207R00000X",
                    "Y",
                ),
                (
                    mod.NPI_ORG_2,
                    "2",
                    "Second Provider",
                    None,
                    None,
                    "Second Provider",
                    "Houston",
                    "TX",
                    "77001",
                    "207Q00000X",
                    "Y",
                ),
            ]
        )

        e.executemany(
            """
            INSERT INTO zip_county
            VALUES (?,?,?,?,?)
            """,
            [
                (
                    "77001",
                    "48201",
                    "Harris County",
                    "TX",
                    0.7,
                ),
                (
                    "77001",
                    "48157",
                    "Fort Bend County",
                    "TX",
                    0.3,
                ),
            ]
        )

        e.execute(
            """
            INSERT INTO code_descriptions
            VALUES (?,?,?)
            """,
            (
                "99213",
                "CPT",
                "Office o/p visit est",
            )
        )

        e.execute("CHECKPOINT")

    finally:
        e.close()

    # -------------------------------------------------------------
    # Populate transparency DB
    # -------------------------------------------------------------

    c = duckdb.connect(
        str(db)
    )

    try:

        p1 = c.execute(
            """
            INSERT INTO payers (
                reporting_entity_name,
                reporting_entity_type,
                plan_name,
                plan_id,
                plan_id_type,
                plan_market_type,
                last_updated_on,
                version,
                source_file
            )
            VALUES (?,?,?,?,?,?,?,?,?)
            RETURNING payer_id
            """,
            (
                "Payer One",
                "health insurance issuer",
                "Plan A",
                "P1",
                "HIOS",
                "group",
                "2026-09-01",
                "1",
                "p1.json",
            )
        ).fetchone()[0]

        p2 = c.execute(
            """
            INSERT INTO payers (
                reporting_entity_name,
                reporting_entity_type,
                plan_name,
                plan_id,
                plan_id_type,
                plan_market_type,
                last_updated_on,
                version,
                source_file
            )
            VALUES (?,?,?,?,?,?,?,?,?)
            RETURNING payer_id
            """,
            (
                "Payer Two",
                "health insurance issuer",
                "Plan B",
                "P2",
                "HIOS",
                "group",
                "2026-09-01",
                "1",
                "p2.json",
            )
        ).fetchone()[0]

        code1 = c.execute(
            """
            INSERT INTO billing_codes (
                billing_code,
                billing_code_type,
                billing_code_type_version,
                description,
                name,
                negotiation_arrangement
            )
            VALUES (?,?,?,?,?,?)
            RETURNING code_id
            """,
            (
                "99213",
                "CPT",
                "2026",
                "MRF office visit",
                "Office Visit",
                "ffs",
            )
        ).fetchone()[0]

        code2 = c.execute(
            """
            INSERT INTO billing_codes (
                billing_code,
                billing_code_type,
                billing_code_type_version,
                description,
                name,
                negotiation_arrangement
            )
            VALUES (?,?,?,?,?,?)
            RETURNING code_id
            """,
            (
                "470",
                "MS-DRG",
                "2026",
                "MRF DRG",
                "DRG 470",
                "ffs",
            )
        ).fetchone()[0]

        c.executemany(
            """
            INSERT INTO providers (
                provider_reference_id,
                npi,
                tin_type,
                tin_value,
                facility_name,
                network_name,
                group_key
            )
            VALUES (?,?,?,?,?,?,?)
            """,
            [
                (
                    101,
                    mod.NPI_INDIV,
                    "ein",
                    "12-3456789",
                    "Individual Facility",
                    ["Network"],
                    "ref:101",
                ),
                (
                    101,
                    mod.NPI_ORG,
                    "ein",
                    "12-3456789",
                    "Organization Facility",
                    ["Network"],
                    "ref:101",
                ),
                (
                    102,
                    mod.NPI_ORG_2,
                    "ein",
                    "98-7654321",
                    "Second Facility",
                    ["Network 2"],
                    "ref:102",
                ),
            ]
        )

        rows = [
            (
                p1,
                code1,
                "ffs",
                "institutional",
                "outpatient",
                "negotiated",
                100.0,
                ["11"],
                [],
                "2026-12-31",
                [101],
                "p1.json",
            ),
            (
                p1,
                code1,
                "ffs",
                "institutional",
                "outpatient",
                "negotiated",
                150.0,
                ["11"],
                [],
                "2026-12-31",
                [101],
                "p1.json",
            ),
            (
                p2,
                code1,
                "ffs",
                "institutional",
                "outpatient",
                "negotiated",
                110.0,
                ["11"],
                [],
                "2026-12-31",
                [101],
                "p2.json",
            ),
            (
                p2,
                code1,
                "ffs",
                "institutional",
                "outpatient",
                "negotiated",
                130.0,
                ["11"],
                [],
                "2026-12-31",
                [102],
                "p2.json",
            ),
            (
                p1,
                code2,
                "ffs",
                "institutional",
                "inpatient",
                "negotiated",
                0.0,
                [],
                [],
                "2026-12-31",
                [101],
                "p1.json",
            ),
            (
                p1,
                code2,
                "ffs",
                "institutional",
                "inpatient",
                "negotiated",
                20000000.0,
                [],
                [],
                "2026-12-31",
                [102],
                "p1.json",
            ),
            (
                p2,
                code2,
                "ffs",
                "institutional",
                "inpatient",
                "percentage",
                5000.0,
                [],
                [],
                "2026-12-31",
                [101],
                "p2.json",
            ),
        ]

        c.executemany(
            """
            INSERT INTO negotiated_rates (
                payer_id,
                code_id,
                negotiation_arrangement,
                billing_class,
                setting,
                negotiated_type,
                negotiated_rate,
                service_code,
                billing_code_modifier,
                expiration_date,
                provider_reference_ids,
                source_file,
                ingested_at
            )
            VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,
                CURRENT_TIMESTAMP
            )
            """,
            rows
        )

        c.execute("CHECKPOINT")

    finally:
        c.close()

    # -------------------------------------------------------------
    # Actual repository benchmark builder
    # -------------------------------------------------------------

    started = time.perf_counter()

    build_benchmarks.build(
        str(db),
        str(enr),
        True,
        False,
        False,
        False,
        2,
        "512MB",
        str(root / "duckdb_tmp"),
        approx
    )

    elapsed_ms = (
        time.perf_counter() - started
    ) * 1000

    return db, f"build_elapsed_ms={elapsed_ms:.1f}"


def main() -> int:

    repo = Path.cwd().resolve()

    if (
        not (repo / "runner.py").exists()
        or
        not (repo / "schema.sql").exists()
    ):
        print(
            "ERROR: run this from the TiC repository root."
        )
        return 2

    mod = load_previous_runner(repo)

    root = Path(
        tempfile.mkdtemp(
            prefix="tic_targeted_",
            dir=str(repo)
        )
    )

    outdir = (
        repo /
        "thesis_targeted_evidence"
    )

    outdir.mkdir(
        parents=True,
        exist_ok=True
    )

    results: list[Result] = []

    try:

        print(
            "TiC / Cigna targeted thesis rerun (no API)"
        )

        print(
            f"Repository: {repo}"
        )

        print(
            f"Temporary test directory: {root}"
        )

        print()

        # =============================================================
        # TC-19, TC-20, TC-21
        # =============================================================

        print(
            "[1/4] Runner cases TC-19 to TC-21 ..."
        )

        results.extend(
            runner_integration(
                repo,
                root,
                mod
            )
        )

        # =============================================================
        # TC-23
        # =============================================================

        print(
            "[2/4] Benchmark cases TC-23 and TC-27 ..."
        )

        try:

            db, timing = benchmark_case(
                repo,
                root,
                mod,
                approx=False
            )

            import duckdb

            con = duckdb.connect(
                str(db),
                read_only=True
            )

            try:

                tables = {
                    r[0]
                    for r in con.execute(
                        "SHOW TABLES"
                    ).fetchall()
                }

            finally:
                con.close()

            expected_stats = {
                "benchmarks_code_stats",
                "benchmarks_geo_stats",
                "benchmarks_geo_payer_stats",
                "benchmarks_payer_stats",
                "benchmarks_payer_provider_stats",
                "benchmarks_provider_stats",
            }

            actual_stats = {
                t
                for t in tables
                if (
                    t.startswith("benchmarks_")
                    and
                    t.endswith("stats")
                )
            }

            ok23 = (
                "benchmarks" in tables
                and
                actual_stats == expected_stats
            )

            results.append(
                Result(
                    "TC-23",
                    "PASS" if ok23 else "FAIL",
                    (
                        f"benchmark_present="
                        f"{'benchmarks' in tables}; "
                        f"stat_tables="
                        f"{sorted(actual_stats)}; "
                        f"{timing}"
                    )
                )
            )

        except Exception as e:

            results.append(
                Result(
                    "TC-23",
                    "FAIL",
                    f"{type(e).__name__}: {e}"
                )
            )

        # =============================================================
        # TC-27
        # =============================================================

        try:

            db, timing = benchmark_case(
                repo,
                root,
                mod,
                approx=True
            )

            import duckdb

            con = duckdb.connect(
                str(db),
                read_only=True
            )

            try:

                n = int(
                    con.execute(
                        "SELECT COUNT(*) FROM benchmarks"
                    ).fetchone()[0]
                )

                tables = {
                    r[0]
                    for r in con.execute(
                        "SHOW TABLES"
                    ).fetchall()
                }

            finally:
                con.close()

            expected_stats = {
                "benchmarks_code_stats",
                "benchmarks_geo_stats",
                "benchmarks_geo_payer_stats",
                "benchmarks_payer_stats",
                "benchmarks_payer_provider_stats",
                "benchmarks_provider_stats",
            }

            actual_stats = {
                t
                for t in tables
                if (
                    t.startswith("benchmarks_")
                    and
                    t.endswith("stats")
                )
            }

            ok27 = (
                n == 4
                and
                actual_stats == expected_stats
            )

            results.append(
                Result(
                    "TC-27",
                    "PASS" if ok27 else "FAIL",
                    (
                        f"benchmark_rows={n}; "
                        f"stat_tables={sorted(actual_stats)}; "
                        f"{timing}"
                    )
                )
            )

        except Exception as e:

            results.append(
                Result(
                    "TC-27",
                    "FAIL",
                    f"{type(e).__name__}: {e}"
                )
            )

        # =============================================================
        # TC-29 through TC-33
        # =============================================================

        print(
            "[3/4] Validation cases TC-29 to TC-33 ..."
        )

        valid_db = (
            root /
            "validation_valid.duckdb"
        )

        reports = (
            root /
            "validation_reports"
        )

        reports.mkdir(
            exist_ok=True
        )

        make_validation_baseline(
            valid_db,
            repo,
            mod
        )

        # -------------------------------------------------------------
        # TC-29
        # -------------------------------------------------------------

        try:

            p, data = validation_case(
                repo,
                valid_db,
                reports,
                fast=False
            )

            errors = [
                x
                for x in data
                if (
                    x.get("level") == "error"
                    and
                    not x.get("passed")
                )
            ]

            ok = (
                p.returncode == 0
                and
                not errors
            )

            results.append(
                Result(
                    "TC-29",
                    "PASS" if ok else "FAIL",
                    (
                        f"returncode={p.returncode}; "
                        f"error_failures={len(errors)}"
                    ),
                    "A clean baseline should produce no error-level failures."
                )
            )

        except Exception as e:

            results.append(
                Result(
                    "TC-29",
                    "FAIL",
                    f"{type(e).__name__}: {e}"
                )
            )

        # -------------------------------------------------------------
        # TC-30
        # -------------------------------------------------------------

        try:

            p, data = validation_case(
                repo,
                valid_db,
                reports,
                fast=True
            )

            skipped = [
                x
                for x in data
                if str(
                    x.get("note", "")
                ).startswith(
                    "Skipped (--fast)"
                )
            ]

            ok = (
                p.returncode == 0
                and
                len(skipped) >= 1
            )

            results.append(
                Result(
                    "TC-30",
                    "PASS" if ok else "FAIL",
                    (
                        f"returncode={p.returncode}; "
                        f"fast_skips={len(skipped)}"
                    )
                )
            )

        except Exception as e:

            results.append(
                Result(
                    "TC-30",
                    "FAIL",
                    f"{type(e).__name__}: {e}"
                )
            )

        # -------------------------------------------------------------
        # TC-31
        # -------------------------------------------------------------

        try:

            import duckdb

            db = (
                root /
                "validation_orphan_payer.duckdb"
            )

            shutil.copy2(
                valid_db,
                db
            )

            con = duckdb.connect(
                str(db)
            )

            try:

                con.execute(
                    """
                    INSERT INTO negotiated_rates (
                        payer_id,
                        code_id,
                        negotiation_arrangement,
                        billing_class,
                        setting,
                        negotiated_type,
                        negotiated_rate,
                        service_code,
                        billing_code_modifier,
                        expiration_date,
                        provider_reference_ids,
                        source_file,
                        ingested_at
                    )
                    VALUES (
                        ?,?,?,?,?,?,?,?,?,?,?,?,
                        CURRENT_TIMESTAMP
                    )
                    """,
                    (
                        999999,
                        1,
                        "ffs",
                        "institutional",
                        "outpatient",
                        "negotiated",
                        125.0,
                        ["11"],
                        [],
                        "2026-12-31",
                        [101],
                        "orphan.json",
                    )
                )

                con.execute("CHECKPOINT")

            finally:
                con.close()

            p, data = validation_case(
                repo,
                db,
                reports
            )

            chk = (
                check_by_name(
                    data,
                    "orphan_payer_id"
                )
                or
                {}
            )

            ok = (
                p.returncode == 1
                and
                chk.get("passed") is False
                and
                int(
                    chk.get(
                        "n_bad",
                        0
                    )
                ) >= 1
            )

            results.append(
                Result(
                    "TC-31",
                    "PASS" if ok else "FAIL",
                    (
                        f"returncode={p.returncode}; "
                        f"orphan_payer={chk}"
                    )
                )
            )

        except Exception as e:

            results.append(
                Result(
                    "TC-31",
                    "FAIL",
                    f"{type(e).__name__}: {e}"
                )
            )

        # -------------------------------------------------------------
        # TC-32
        # -------------------------------------------------------------

        try:

            import duckdb

            db = (
                root /
                "validation_bad_npi.duckdb"
            )

            shutil.copy2(
                valid_db,
                db
            )

            con = duckdb.connect(
                str(db)
            )

            try:

                con.execute(
                    """
                    INSERT INTO providers (
                        provider_reference_id,
                        npi,
                        tin_type,
                        tin_value,
                        facility_name,
                        network_name,
                        group_key
                    )
                    VALUES (?,?,?,?,?,?,?)
                    """,
                    (
                        999,
                        mod.NPI_BAD,
                        "ein",
                        "12-3456789",
                        "Bad NPI",
                        ["N"],
                        "ref:999",
                    )
                )

                con.execute("CHECKPOINT")

            finally:
                con.close()

            p, data = validation_case(
                repo,
                db,
                reports
            )

            chk = (
                check_by_name(
                    data,
                    "npi_invalid"
                )
                or
                {}
            )

            ok = (
                int(
                    chk.get(
                        "n_bad",
                        0
                    )
                ) >= 1
            )

            results.append(
                Result(
                    "TC-32",
                    "PASS" if ok else "FAIL",
                    (
                        f"returncode={p.returncode}; "
                        f"npi_invalid={chk}"
                    )
                )
            )

        except Exception as e:

            results.append(
                Result(
                    "TC-32",
                    "FAIL",
                    f"{type(e).__name__}: {e}"
                )
            )

        # -------------------------------------------------------------
        # TC-33
        # -------------------------------------------------------------

        try:

            import duckdb

            db = (
                root /
                "validation_domain.duckdb"
            )

            shutil.copy2(
                valid_db,
                db
            )

            con = duckdb.connect(
                str(db)
            )

            try:

                # Two invalid zero/negative negotiated rates.
                for rate in (
                    0.0,
                    -1.0
                ):

                    con.execute(
                        """
                        INSERT INTO negotiated_rates (
                            payer_id,
                            code_id,
                            negotiation_arrangement,
                            billing_class,
                            setting,
                            negotiated_type,
                            negotiated_rate,
                            service_code,
                            billing_code_modifier,
                            expiration_date,
                            provider_reference_ids,
                            source_file,
                            ingested_at
                        )
                        VALUES (
                            ?,?,?,?,?,?,?,?,?,?,?,?,
                            CURRENT_TIMESTAMP
                        )
                        """,
                        (
                            1,
                            1,
                            "ffs",
                            "institutional",
                            "outpatient",
                            "negotiated",
                            rate,
                            [],
                            [],
                            "2026-12-31",
                            [101],
                            "domain.json",
                        )
                    )

                # Two high-rate outliers.
                for rate in (
                    300000.0,
                    500000.0
                ):

                    con.execute(
                        """
                        INSERT INTO negotiated_rates (
                            payer_id,
                            code_id,
                            negotiation_arrangement,
                            billing_class,
                            setting,
                            negotiated_type,
                            negotiated_rate,
                            service_code,
                            billing_code_modifier,
                            expiration_date,
                            provider_reference_ids,
                            source_file,
                            ingested_at
                        )
                        VALUES (
                            ?,?,?,?,?,?,?,?,?,?,?,?,
                            CURRENT_TIMESTAMP
                        )
                        """,
                        (
                            1,
                            1,
                            "ffs",
                            "institutional",
                            "outpatient",
                            "negotiated",
                            rate,
                            [],
                            [],
                            "2026-12-31",
                            [101],
                            "domain.json",
                        )
                    )

                # Invalid NPI values.
                for idx, npi in enumerate(
                    [
                        mod.NPI_BAD,
                        mod.NPI_BAD + 2,
                        3000000000
                    ],
                    start=1
                ):

                    con.execute(
                        """
                        INSERT INTO providers (
                            provider_reference_id,
                            npi,
                            tin_type,
                            tin_value,
                            facility_name,
                            network_name,
                            group_key
                        )
                        VALUES (?,?,?,?,?,?,?)
                        """,
                        (
                            1000 + idx,
                            npi,
                            "ein",
                            "12-3456789",
                            f"Bad {idx}",
                            ["N"],
                            f"ref:{1000 + idx}",
                        )
                    )

                con.execute("CHECKPOINT")

            finally:
                con.close()

            p, data = validation_case(
                repo,
                db,
                reports
            )

            z = (
                check_by_name(
                    data,
                    "dollar_rate_not_positive"
                )
                or
                {}
            )

            o = (
                check_by_name(
                    data,
                    "dollar_rate_outlier"
                )
                or
                {}
            )

            n = (
                check_by_name(
                    data,
                    "npi_invalid"
                )
                or
                {}
            )

            ok = (
                p.returncode == 1
                and
                int(
                    z.get(
                        "n_bad",
                        0
                    )
                ) >= 2
                and
                int(
                    o.get(
                        "n_bad",
                        0
                    )
                ) >= 2
                and
                int(
                    n.get(
                        "n_bad",
                        0
                    )
                ) >= 1
            )

            results.append(
                Result(
                    "TC-33",
                    "PASS" if ok else "FAIL",
                    (
                        f"returncode={p.returncode}; "
                        f"zero_rate={z}; "
                        f"outlier={o}; "
                        f"npi={n}"
                    )
                )
            )

        except Exception as e:

            results.append(
                Result(
                    "TC-33",
                    "FAIL",
                    f"{type(e).__name__}: {e}"
                )
            )

        # =============================================================
        # Report
        # =============================================================

        print(
            "[4/4] Writing report ..."
        )

        summary = {
            k: sum(
                r.status == k
                for r in results
            )
            for k in (
                "PASS",
                "FAIL"
            )
        }

        payload = {
            "generated_utc":
                time.strftime(
                    "%Y-%m-%dT%H:%M:%SZ",
                    time.gmtime()
                ),

            "api_used": False,

            "results":
                [
                    asdict(r)
                    for r in results
                ],

            "summary":
                summary
        }

        (
            outdir /
            "targeted_results.json"
        ).write_text(
            json.dumps(
                payload,
                indent=2,
                default=str
            ),
            encoding="utf-8"
        )

        lines = [
            "# TiC / Cigna Targeted Thesis Rerun",
            "",
            (
                f"PASS: **{summary['PASS']}** | "
                f"FAIL: **{summary['FAIL']}** | "
                f"TOTAL: **{len(results)}**"
            ),
            "",
            "| Case | Status | Actual result | Note |",
            "|---|---|---|---|"
        ]

        for r in results:

            actual = (
                r.actual
                .replace("|", "\\|")
                .replace("\n", " ")
            )

            note = (
                r.note
                .replace("|", "\\|")
                .replace("\n", " ")
            )

            lines.append(
                f"| {r.case} | **{r.status}** | "
                f"{actual} | {note} |"
            )

        (
            outdir /
            "targeted_results.md"
        ).write_text(
            "\n".join(lines) + "\n",
            encoding="utf-8"
        )

        print()
        print(
            f"PASS={summary['PASS']} | "
            f"FAIL={summary['FAIL']} | "
            f"TOTAL={len(results)}"
        )

        for r in results:
            print(
                f"{r.case}: {r.status} :: "
                f"{r.actual[:220]}"
            )

        print()

        print(
            f"Report: "
            f"{outdir / 'targeted_results.md'}"
        )

        print(
            "No benchmark API was contacted by this script."
        )

        return (
            0
            if summary["FAIL"] == 0
            else 1
        )

    except Exception as e:

        print(
            f"BLOCKED: {type(e).__name__}: {e}"
        )

        return 2

    finally:
        shutil.rmtree(
            root,
            ignore_errors=True
        )


if __name__ == "__main__":
    raise SystemExit(main())
