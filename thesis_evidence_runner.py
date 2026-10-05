#!/usr/bin/env python3

"""
TiC / Cigna standalone thesis evidence rerunner

Runs only the previously failed local test cases:
    TC-10
    TC-19
    TC-20
    TC-21
    TC-23
    TC-27
    TC-29
    TC-30
    TC-31
    TC-32
    TC-33

No API server is required.

Run from the TiC repository root:

    python .\thesis_evidence_runner_cigna_standalone.py
"""

from __future__ import annotations

import json
import gzip
import hashlib
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import urllib.parse
import zipfile
from dataclasses import dataclass, asdict
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


# ============================================================
# TEST RESULT
# ============================================================

@dataclass
class TestResult:
    case: str
    area: str
    status: str
    actual: str
    elapsed_ms: float
    notes: str = ""


# ============================================================
# NPI HELPERS
# ============================================================

def valid_npi(prefix9: str) -> int:
    s = str(prefix9)

    if len(s) != 9 or not s.isdigit():
        raise ValueError("prefix9 must contain exactly 9 digits")

    payload = "80840" + s

    total = 0

    for i, ch in enumerate(reversed(payload)):
        d = int(ch)

        if i % 2 == 0:
            d *= 2

            if d > 9:
                d -= 9

        total += d

    check_digit = (10 - (total % 10)) % 10

    return int(s + str(check_digit))


NPI_ORG = valid_npi("222222222")
NPI_INDIV = valid_npi("123456789")
NPI_ORG_2 = valid_npi("211111111")

NPI_BAD = NPI_ORG + 1


# ============================================================
# REPOSITORY DISCOVERY
# ============================================================

def find_repo() -> Path:
    here = Path.cwd().resolve()

    candidates = [
        here,
        here / "TiC",
        here / "tic-cigna",
        here.parent,
    ]

    for candidate in candidates:
        if (
            (candidate / "schema.sql").exists()
            and (candidate / "stream_parser.py").exists()
        ):
            return candidate.resolve()

    raise FileNotFoundError(
        "Could not find the TiC repository.\n"
        "Run this script from the TiC repository folder."
    )


# ============================================================
# SYNTHETIC MRF
# ============================================================

def synthetic_mrf(plan_id="SYNTH-01"):
    embedded_provider = {
        "npi": [valid_npi("199999999")],
        "tin": {
            "type": "ein",
            "value": "12-3456789"
        },
        "business_name": "Embedded Facility"
    }

    return {
        "reporting_entity_name": "Synthetic Cigna Test Payer",
        "reporting_entity_type": "health insurance issuer",
        "plan_name": "Synthetic Institutional Plan",
        "plan_id": plan_id,
        "plan_id_type": "HIOS",
        "plan_market_type": "group",
        "last_updated_on": "2026-09-01",
        "version": "1",

        "provider_references": [
            {
                "provider_group_id": 101,
                "network_name": ["Synthetic Network"],
                "provider_groups": [
                    {
                        "npi": [
                            NPI_ORG,
                            NPI_INDIV
                        ],
                        "tin": {
                            "type": "ein",
                            "value": "12-3456789"
                        },
                        "business_name": "Organization Facility"
                    }
                ]
            },
            {
                "provider_group_id": 102,
                "network_name": ["Synthetic Network 2"],
                "provider_groups": [
                    {
                        "npi": [NPI_ORG_2],
                        "tin": {
                            "type": "ein",
                            "value": "98-7654321"
                        },
                        "business_name": "Second Facility"
                    }
                ]
            }
        ],

        "in_network": [
            {
                "negotiation_arrangement": "ffs",
                "billing_code": "99213",
                "billing_code_type": "CPT",
                "billing_code_type_version": "2026",
                "description": "MRF Office Visit",
                "name": "Office Visit",

                "negotiated_rates": [
                    {
                        "provider_references": [101],

                        "negotiated_prices": [
                            {
                                "negotiated_type": "negotiated",
                                "negotiated_rate": 100.0,
                                "billing_class": "institutional",
                                "setting": "outpatient",
                                "service_code": ["11"],
                                "billing_code_modifier": [],
                                "expiration_date": "2026-12-31"
                            },
                            {
                                "negotiated_type": "negotiated",
                                "negotiated_rate": 999.0,
                                "billing_class": "professional",
                                "setting": "outpatient",
                                "service_code": ["11"],
                                "billing_code_modifier": [],
                                "expiration_date": "2026-12-31"
                            }
                        ]
                    },

                    {
                        "provider_groups": [embedded_provider],

                        "negotiated_prices": [
                            {
                                "negotiated_type": "negotiated",
                                "negotiated_rate": 125.0,
                                "billing_class": "institutional",
                                "setting": "outpatient",
                                "service_code": ["11"],
                                "billing_code_modifier": [],
                                "expiration_date": "2026-12-31"
                            }
                        ]
                    }
                ]
            },

            {
                "negotiation_arrangement": "ffs",
                "billing_code": "470",
                "billing_code_type": "MS-DRG",
                "billing_code_type_version": "2026",
                "description": "MRF DRG",
                "name": "DRG 470",

                "negotiated_rates": [
                    {
                        "provider_references": [101],

                        "negotiated_prices": [
                            {
                                "negotiated_type": "negotiated",
                                "negotiated_rate": 500.0,
                                "billing_class": "institutional",
                                "setting": "inpatient",
                                "service_code": [],
                                "billing_code_modifier": [],
                                "expiration_date": "2026-12-31"
                            }
                        ]
                    },

                    {
                        "provider_references": [102],

                        "negotiated_prices": [
                            {
                                "negotiated_type": "negotiated",
                                "negotiated_rate": 600.0,
                                "billing_class": "institutional",
                                "setting": "inpatient",
                                "service_code": [],
                                "billing_code_modifier": [],
                                "expiration_date": "2026-12-31"
                            }
                        ]
                    },

                    {
                        "provider_references": [101],

                        "negotiated_prices": [
                            {
                                "negotiated_type": "negotiated",
                                "negotiated_rate": 700.0,
                                "billing_class": "institutional",
                                "setting": "inpatient",
                                "service_code": [],
                                "billing_code_modifier": [],
                                "expiration_date": "2026-12-31"
                            }
                        ]
                    }
                ]
            }
        ]
    }


# ============================================================
# LOCAL HTTP SERVER
# ============================================================

class LocalFileServer:

    def __init__(self, files, missing=None):

        self.files = files
        self.missing = missing or set()

        parent = self

        class Handler(BaseHTTPRequestHandler):

            protocol_version = "HTTP/1.1"

            def do_GET(self):

                name = urllib.parse.urlparse(
                    self.path
                ).path.lstrip("/")

                if (
                    name in parent.missing
                    or name not in parent.files
                ):

                    body = b"not found"

                    self.send_response(404)

                    self.send_header(
                        "Content-Length",
                        str(len(body))
                    )

                    self.end_headers()

                    self.wfile.write(body)

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

                self.end_headers()

                self.wfile.write(body)

            def log_message(self, *_args):
                pass

        self.server = ThreadingHTTPServer(
            ("127.0.0.1", 0),
            Handler
        )

        self.thread = threading.Thread(
            target=self.server.serve_forever,
            daemon=True
        )

    @property
    def base_url(self):
        return (
            f"http://127.0.0.1:"
            f"{self.server.server_port}"
        )

    def start(self):
        self.thread.start()
        return self

    def stop(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


# ============================================================
# BASIC HELPERS
# ============================================================

def expect(condition, actual):
    return (
        "PASS" if condition else "FAIL",
        actual
    )


def run_test(results, case, area, function):

    started = time.perf_counter()

    try:

        status, actual = function()

        results.append(
            TestResult(
                case=case,
                area=area,
                status=status,
                actual=str(actual),
                elapsed_ms=(
                    time.perf_counter()
                    - started
                ) * 1000,
            )
        )

    except Exception as exc:

        results.append(
            TestResult(
                case=case,
                area=area,
                status="FAIL",
                actual=(
                    f"{type(exc).__name__}: "
                    f"{exc}"
                ),
                elapsed_ms=(
                    time.perf_counter()
                    - started
                ) * 1000,
                notes=traceback.format_exc(
                    limit=5
                ).replace("\n", " ")
            )
        )


def new_db(path, repo):

    import duckdb

    con = duckdb.connect(str(path))

    try:
        con.execute(
            (repo / "schema.sql").read_text(
                encoding="utf-8"
            )
        )
    finally:
        con.close()


def process_fixture(
    db,
    fixture,
    repo,
    label="fixture.json"
):

    import stream_parser

    new_db(db, repo)

    con = __import__("duckdb").connect(
        str(db)
    )

    try:
        stream_parser.process_file(
            con,
            str(fixture),
            label
        )
    finally:
        con.close()


def counts(db):

    import duckdb

    con = duckdb.connect(
        str(db),
        read_only=True
    )

    try:

        result = {}

        for table in (
            "payers",
            "billing_codes",
            "negotiated_rates",
            "providers"
        ):

            result[table] = int(
                con.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0]
            )

        return result

    finally:
        con.close()


# ============================================================
# FIXTURES
# ============================================================

def create_fixtures(root):

    raw = json.dumps(
        synthetic_mrf(),
        separators=(",", ":")
    ).encode()

    plain = root / "fixture.json"
    plain.write_bytes(raw)

    second = root / "fixture-second.json"

    second.write_text(
        json.dumps(
            synthetic_mrf("SYNTH-02")
        ),
        encoding="utf-8"
    )

    return plain, second


# ============================================================
# TC-10
# ============================================================

def tc10(root, repo, plain, second):

    import duckdb
    import stream_parser

    db = root / "tc10.duckdb"

    new_db(db, repo)

    con = duckdb.connect(str(db))

    try:

        stream_parser.process_file(
            con,
            str(plain),
            "fixture.json"
        )

        stream_parser.process_file(
            con,
            str(second),
            "fixture-second.json"
        )

    finally:
        con.close()

    con = duckdb.connect(
        str(db),
        read_only=True
    )

    try:

        synthetic_ids = int(
            con.execute(
                """
                SELECT COUNT(DISTINCT provider_reference_id)
                FROM providers
                WHERE provider_reference_id < 0
                """
            ).fetchone()[0]
        )

        payers = int(
            con.execute(
                "SELECT COUNT(*) FROM payers"
            ).fetchone()[0]
        )

    finally:
        con.close()

    return expect(
        synthetic_ids == 1 and payers == 2,
        (
            f"distinct_negative_ids="
            f"{synthetic_ids}; "
            f"payer_rows={payers}"
        )
    )


# ============================================================
# ACQUISITION TESTS
# ============================================================

def acquisition_tests(
    results,
    root,
    repo
):

    import ingest_utils

    files = {
        "file1.bin": b"alpha" * 100,
        "file2.bin": b"beta" * 100,
        "file3.bin": b"gamma" * 100,
        "file1.json": json.dumps(
            synthetic_mrf("RUN-01"),
            separators=(",", ":")
        ).encode(),
        "file2.json": json.dumps(
            synthetic_mrf("RUN-02"),
            separators=(",", ":")
        ).encode(),
        "file3.json": json.dumps(
            synthetic_mrf("RUN-03"),
            separators=(",", ":")
        ).encode(),
    }

    server = LocalFileServer(
        files,
        missing={"missing.bin"}
    ).start()

    try:

        manifest = root / "runner_manifest.json"
        progress = root / "runner_progress.txt"
        run_log = root / "runner_runs.jsonl"
        db = root / "runner.duckdb"
        download_dir = root / "downloads"

        manifest.write_text(
            json.dumps(
                {
                    "blobs": [
                        {
                            "name": "file1.json",
                            "downloadUrl":
                                f"{server.base_url}/file1.json",
                            "size_bytes":
                                len(files["file1.json"])
                        },
                        {
                            "name": "file2.json",
                            "downloadUrl":
                                f"{server.base_url}/file2.json",
                            "size_bytes":
                                len(files["file2.json"])
                        },
                        {
                            "name": "file3.json",
                            "downloadUrl":
                                f"{server.base_url}/file3.json",
                            "size_bytes":
                                len(files["file3.json"])
                        }
                    ]
                }
            ),
            encoding="utf-8"
        )

        def run_runner(max_files=None):

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
                str(download_dir),

                "--run-log",
                str(run_log)
            ]

            if max_files is not None:

                cmd.extend([
                    "--max-files",
                    str(max_files)
                ])

            return subprocess.run(
                cmd,
                cwd=str(repo),
                capture_output=True,
                text=True,
                timeout=300
            )

        # ----------------------------------------------------
        # TC-19
        # ----------------------------------------------------

        def tc19():

            progress.write_text(
                "2",
                encoding="utf-8"
            )

            proc = run_runner()

            value = int(
                progress.read_text(
                    encoding="utf-8"
                ).strip()
            )

            complete_events = 0

            if run_log.exists():

                for line in run_log.read_text(
                    encoding="utf-8"
                ).splitlines():

                    if (
                        line.strip()
                        and json.loads(line).get(
                            "status"
                        ) == "complete"
                    ):

                        complete_events += 1

            remaining = []

            if download_dir.exists():

                remaining = [
                    p.name
                    for p in download_dir.glob("*")
                ]

            return expect(
                proc.returncode == 0
                and value == 3
                and complete_events >= 1
                and not remaining,

                (
                    f"returncode={proc.returncode}; "
                    f"progress={value}; "
                    f"complete_events={complete_events}; "
                    f"downloads_left={remaining}"
                )
            )

        run_test(
            results,
            "TC-19",
            "Acquisition / Runner",
            tc19
        )

        # ----------------------------------------------------
        # TC-20
        # ----------------------------------------------------

        def tc20():

            proc = run_runner()

            matched = (
                "All files have already been processed!"
                in proc.stdout
            )

            return expect(
                proc.returncode == 0
                and matched,

                (
                    f"returncode={proc.returncode}; "
                    f"matched_message={matched}"
                )
            )

        run_test(
            results,
            "TC-20",
            "Acquisition / Runner",
            tc20
        )

        # ----------------------------------------------------
        # TC-21
        # ----------------------------------------------------

        def tc21():

            fail_manifest = (
                root / "failure_manifest.json"
            )

            fail_progress = (
                root / "failure_progress.txt"
            )

            fail_log = (
                root / "failure_runs.jsonl"
            )

            fail_db = (
                root / "failure.duckdb"
            )

            fail_downloads = (
                root / "failure_downloads"
            )

            fail_manifest.write_text(
                json.dumps(
                    {
                        "blobs": [
                            {
                                "name": "missing.bin",
                                "downloadUrl":
                                    f"{server.base_url}/missing.bin",
                                "size_bytes": 50
                            },
                            {
                                "name": "file1.bin",
                                "downloadUrl":
                                    f"{server.base_url}/file1.bin",
                                "size_bytes":
                                    len(files["file1.bin"])
                            }
                        ]
                    }
                ),
                encoding="utf-8"
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(repo / "runner.py"),

                    "--manifest",
                    str(fail_manifest),

                    "--db",
                    str(fail_db),

                    "--progress",
                    str(fail_progress),

                    "--schema",
                    str(repo / "schema.sql"),

                    "--download-dir",
                    str(fail_downloads),

                    "--run-log",
                    str(fail_log)
                ],
                cwd=str(repo),
                capture_output=True,
                text=True,
                timeout=300
            )

            progress_value = 0

            if fail_progress.exists():

                progress_value = int(
                    fail_progress.read_text(
                        encoding="utf-8"
                    ).strip()
                )

            events = []

            if fail_log.exists():

                events = [
                    json.loads(line)
                    for line in fail_log.read_text(
                        encoding="utf-8"
                    ).splitlines()
                    if line.strip()
                ]

            failed = any(
                e.get("status") == "failed"
                for e in events
            )

            return expect(
                proc.returncode == 0
                and progress_value == 0
                and failed,

                (
                    f"returncode={proc.returncode}; "
                    f"progress={progress_value}; "
                    f"failed_event={failed}"
                )
            )

        run_test(
            results,
            "TC-21",
            "Acquisition / Runner",
            tc21
        )

    finally:

        server.stop()


# ============================================================
# BENCHMARK FIXTURE
# ============================================================

def create_benchmark_fixture(
    root,
    repo
):

    import duckdb

    db = root / "benchmark.duckdb"
    enrichment = root / "enrichment.duckdb"

    new_db(db, repo)

    e = duckdb.connect(
        str(enrichment)
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
            INSERT INTO nppes VALUES
            (?,?,?,?,?,?,?,?,?,?,?)
            """,
            [
                (
                    NPI_ORG,
                    "2",
                    "Org Provider",
                    None,
                    None,
                    "Org Provider",
                    "Houston",
                    "TX",
                    "77001",
                    "207Q00000X",
                    "Y"
                ),
                (
                    NPI_INDIV,
                    "1",
                    "Individual Provider",
                    "Ada",
                    "Example",
                    None,
                    "Houston",
                    "TX",
                    "77001",
                    "207R00000X",
                    "Y"
                ),
                (
                    NPI_ORG_2,
                    "2",
                    "Second Provider",
                    None,
                    None,
                    "Second Provider",
                    "Houston",
                    "TX",
                    "77001",
                    "207Q00000X",
                    "Y"
                )
            ]
        )

        e.executemany(
            """
            INSERT INTO zip_county VALUES
            (?,?,?,?,?)
            """,
            [
                (
                    "77001",
                    "48201",
                    "Harris County",
                    "TX",
                    0.7
                ),
                (
                    "77001",
                    "48157",
                    "Fort Bend County",
                    "TX",
                    0.3
                )
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
                "Office o/p visit est"
            )
        )

        e.execute("CHECKPOINT")

    finally:
        e.close()

    c = duckdb.connect(str(db))

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
                "p1.json"
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
                "p2.json"
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
                "ffs"
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
                "ffs"
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
                    NPI_INDIV,
                    "ein",
                    "12-3456789",
                    "Individual Facility",
                    ["Network"],
                    "ref:101"
                ),
                (
                    101,
                    NPI_ORG,
                    "ein",
                    "12-3456789",
                    "Organization Facility",
                    ["Network"],
                    "ref:101"
                ),
                (
                    102,
                    NPI_ORG_2,
                    "ein",
                    "98-7654321",
                    "Second Facility",
                    ["Network 2"],
                    "ref:102"
                )
            ]
        )

        rates = [
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
                "p1.json"
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
                "p1.json"
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
                "p2.json"
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
                "p2.json"
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
                "p1.json"
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
                "p1.json"
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
                "p2.json"
            )
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
                source_file
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            rates
        )

        c.execute("CHECKPOINT")

    finally:
        c.close()

    return db, enrichment


# ============================================================
# TC-23 / TC-27
# ============================================================

def benchmark_tests(
    results,
    root,
    repo
):

    import duckdb
    import build_benchmarks

    # --------------------------------------------------------
    # TC-23
    # --------------------------------------------------------

    def tc23():

        db, enrichment = create_benchmark_fixture(
            root,
            repo
        )

        build_benchmarks.build(
            str(db),
            str(enrichment),
            True,
            False,
            False,
            False,
            2,
            "512MB",
            str(root / "duckdb_tmp"),
            False
        )

        con = duckdb.connect(
            str(db),
            read_only=True
        )

        try:

            tables = {
                row[0]
                for row in con.execute(
                    "SHOW TABLES"
                ).fetchall()
            }

        finally:
            con.close()

        expected = {
            "benchmarks_code_stats",
            "benchmarks_geo_stats",
            "benchmarks_geo_payer_stats",
            "benchmarks_payer_stats",
            "benchmarks_payer_provider_stats",
            "benchmarks_provider_stats"
        }

        actual_stats = {
            t for t in tables
            if t.startswith("benchmarks_")
            and t.endswith("stats")
        }

        return expect(
            "benchmarks" in tables
            and actual_stats == expected,

            (
                f"benchmark_tables="
                f"{sorted(tables)}; "
                f"benchmark_stat_tables="
                f"{sorted(actual_stats)}"
            )
        )

    run_test(
        results,
        "TC-23",
        "Benchmark",
        tc23
    )

    # --------------------------------------------------------
    # TC-27
    # --------------------------------------------------------

    def tc27():

        db, enrichment = create_benchmark_fixture(
            root,
            repo
        )

        build_benchmarks.build(
            str(db),
            str(enrichment),
            True,
            False,
            False,
            False,
            2,
            "512MB",
            str(root / "duckdb_tmp_approx"),
            True
        )

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
                row[0]
                for row in con.execute(
                    "SHOW TABLES"
                ).fetchall()
            }

        finally:
            con.close()

        stats = [
            t for t in tables
            if t.startswith("benchmarks_")
            and t.endswith("stats")
        ]

        return expect(
            n == 4 and len(stats) == 6,

            (
                f"benchmark_rows={n}; "
                f"stats_tables={sorted(stats)}"
            )
        )

    run_test(
        results,
        "TC-27",
        "Benchmark",
        tc27
    )


# ============================================================
# VALIDATION HELPERS
# ============================================================

def validation_run(
    repo,
    db,
    out_dir,
    fast=False
):

    txt = out_dir / (
        db.stem
        + ("_fast.txt" if fast else ".txt")
    )

    js = out_dir / (
        db.stem
        + ("_fast.json" if fast else ".json")
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

    proc = subprocess.run(
        cmd,
        cwd=str(repo),
        capture_output=True,
        text=True,
        timeout=300
    )

    data = []

    if js.exists():

        data = json.loads(
            js.read_text(
                encoding="utf-8"
            )
        )

    return proc, data


def find_check(data, name):

    for row in data:

        if row.get("name") == name:
            return row

    return {}


def copy_db(src, dst):
    shutil.copy2(src, dst)


# ============================================================
# VALIDATION TESTS
# ============================================================

def validation_tests(
    results,
    root,
    repo,
    plain
):

    import duckdb

    valid_db = (
        root / "validation_valid.duckdb"
    )

    process_fixture(
        valid_db,
        plain,
        repo
    )

    reports = (
        root / "validation_reports"
    )

    reports.mkdir(
        parents=True,
        exist_ok=True
    )

    # --------------------------------------------------------
    # TC-29
    # --------------------------------------------------------

    def tc29():

        proc, data = validation_run(
            repo,
            valid_db,
            reports,
            fast=False
        )

        errors = [
            r for r in data
            if (
                r.get("level") == "error"
                and not r.get("passed")
            )
        ]

        checks = [
            r for r in data
            if r.get("level") != "info"
        ]

        return expect(
            proc.returncode == 0
            and not errors,

            (
                f"returncode={proc.returncode}; "
                f"error_failures={len(errors)}; "
                f"checks={len(checks)}"
            )
        )

    run_test(
        results,
        "TC-29",
        "Validation",
        tc29
    )

    # --------------------------------------------------------
    # TC-30
    # --------------------------------------------------------

    def tc30():

        proc, data = validation_run(
            repo,
            valid_db,
            reports,
            fast=True
        )

        skipped = [
            r for r in data
            if str(
                r.get("note", "")
            ).startswith(
                "Skipped (--fast)"
            )
        ]

        return expect(
            proc.returncode == 0
            and len(skipped) >= 1,

            (
                f"returncode={proc.returncode}; "
                f"fast_skips={len(skipped)}"
            )
        )

    run_test(
        results,
        "TC-30",
        "Validation",
        tc30
    )

    # --------------------------------------------------------
    # TC-31
    # --------------------------------------------------------

    def tc31():

        db = (
            root /
            "validation_orphan_payer.duckdb"
        )

        copy_db(
            valid_db,
            db
        )

        con = duckdb.connect(
            str(db)
        )

        try:

            row = con.execute(
                """
                SELECT
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
                    source_file
                FROM negotiated_rates
                LIMIT 1
                """
            ).fetchone()

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
                    source_file
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    999999,
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    row[9],
                    row[10],
                    row[11]
                )
            )

            con.execute(
                "CHECKPOINT"
            )

        finally:
            con.close()

        proc, data = validation_run(
            repo,
            db,
            reports
        )

        check = find_check(
            data,
            "orphan_payer_id"
        )

        return expect(
            proc.returncode == 1
            and not check.get(
                "passed",
                True
            )
            and check.get(
                "n_bad",
                0
            ) >= 1,

            (
                f"returncode={proc.returncode}; "
                f"orphan_payer={check}"
            )
        )

    run_test(
        results,
        "TC-31",
        "Validation",
        tc31
    )

    # --------------------------------------------------------
    # TC-32
    # --------------------------------------------------------

    def tc32():

        db = (
            root /
            "validation_bad_npi.duckdb"
        )

        copy_db(
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
                    NPI_BAD,
                    "ein",
                    "12-3456789",
                    "Bad NPI Facility",
                    ["N"],
                    "ref:999"
                )
            )

            con.execute(
                "CHECKPOINT"
            )

        finally:
            con.close()

        proc, data = validation_run(
            repo,
            db,
            reports
        )

        check = find_check(
            data,
            "npi_invalid"
        )

        return expect(
            check.get("n_bad", 0) >= 1,

            (
                f"returncode={proc.returncode}; "
                f"npi_invalid={check}"
            )
        )

    run_test(
        results,
        "TC-32",
        "Validation",
        tc32
    )

    # --------------------------------------------------------
    # TC-33
    # --------------------------------------------------------

    def tc33():

        db = (
            root /
            "validation_domain.duckdb"
        )

        copy_db(
            valid_db,
            db
        )

        con = duckdb.connect(
            str(db)
        )

        try:

            row = con.execute(
                """
                SELECT
                    payer_id,
                    code_id,
                    negotiation_arrangement,
                    billing_class,
                    setting,
                    expiration_date,
                    provider_reference_ids,
                    source_file
                FROM negotiated_rates
                LIMIT 1
                """
            ).fetchone()

            # Two zero/negative rates.
            for rate in (0.0, -1.0):

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
                        source_file
                    )
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        row[0],
                        row[1],
                        row[2],
                        row[3],
                        row[4],
                        "negotiated",
                        rate,
                        [],
                        [],
                        row[5],
                        row[6],
                        row[7]
                    )
                )

            # Two outliers.
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
                        source_file
                    )
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        row[0],
                        row[1],
                        row[2],
                        row[3],
                        row[4],
                        "negotiated",
                        rate,
                        [],
                        [],
                        row[5],
                        row[6],
                        row[7]
                    )
                )

            # Invalid NPIs.
            bad_npis = [
                NPI_BAD,
                NPI_BAD + 2,
                3000000000
            ]

            for idx, npi in enumerate(
                bad_npis,
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
                        f"ref:{1000 + idx}"
                    )
                )

            con.execute(
                "CHECKPOINT"
            )

        finally:
            con.close()

        proc, data = validation_run(
            repo,
            db,
            reports
        )

        zero_rate = find_check(
            data,
            "dollar_rate_not_positive"
        )

        outlier = find_check(
            data,
            "dollar_rate_outlier"
        )

        invalid_npi = find_check(
            data,
            "npi_invalid"
        )

        ok = (
            proc.returncode == 1
            and zero_rate.get(
                "n_bad",
                0
            ) >= 2
            and outlier.get(
                "n_bad",
                0
            ) >= 2
            and invalid_npi.get(
                "n_bad",
                0
            ) >= 1
        )

        return expect(
            ok,

            (
                f"returncode={proc.returncode}; "
                f"zero_rate={zero_rate}; "
                f"outlier={outlier}; "
                f"npi={invalid_npi}"
            )
        )

    run_test(
        results,
        "TC-33",
        "Validation",
        tc33
    )


# ============================================================
# MAIN
# ============================================================

def main():

    repo = find_repo()

    print("=" * 78)
    print("TiC / Cigna Standalone Thesis Evidence Rerunner")
    print("=" * 78)
    print()
    print("Repository:")
    print(repo)
    print()
    print("API server is NOT required.")
    print()

    output = (
        repo /
        "thesis_test_evidence_cigna_rerun"
    )

    if output.exists():
        shutil.rmtree(output)

    output.mkdir(
        parents=True,
        exist_ok=True
    )

    root = Path(
        tempfile.mkdtemp(
            prefix="tic_failed_tests_",
            dir=str(output)
        )
    )

    results = []

    try:

        plain, second = create_fixtures(
            root
        )

        # ----------------------------------------------------
        # TC-10
        # ----------------------------------------------------

        run_test(
            results,
            "TC-10",
            "Ingestion",
            lambda: tc10(
                root,
                repo,
                plain,
                second
            )
        )

        # ----------------------------------------------------
        # TC-19 / TC-20 / TC-21
        # ----------------------------------------------------

        acquisition_tests(
            results,
            root,
            repo
        )

        # ----------------------------------------------------
        # TC-23 / TC-27
        # ----------------------------------------------------

        benchmark_tests(
            results,
            root,
            repo
        )

        # ----------------------------------------------------
        # TC-29 through TC-33
        # ----------------------------------------------------

        validation_tests(
            results,
            root,
            repo,
            plain
        )

        # ----------------------------------------------------
        # REPORT
        # ----------------------------------------------------

        report = {
            "repository": str(repo),
            "generated": time.strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "tests": [
                asdict(r)
                for r in results
            ],
            "summary": {
                "PASS": sum(
                    r.status == "PASS"
                    for r in results
                ),
                "FAIL": sum(
                    r.status == "FAIL"
                    for r in results
                ),
                "TOTAL": len(results)
            }
        }

        (output / "rerun_results.json").write_text(
            json.dumps(
                report,
                indent=2,
                default=str
            ),
            encoding="utf-8"
        )

        lines = []

        lines.append(
            "# Cigna Failed-Test Rerun"
        )

        lines.append("")
        lines.append(
            f"Repository: `{repo}`"
        )

        lines.append("")
        lines.append(
            "| Case | Area | Status | Actual | Time (ms) |"
        )

        lines.append(
            "|---|---|---|---|---:|"
        )

        for r in results:

            actual = (
                r.actual
                .replace("|", "\\|")
                .replace("\n", " ")
            )

            lines.append(
                f"| {r.case} | "
                f"{r.area} | "
                f"**{r.status}** | "
                f"{actual} | "
                f"{r.elapsed_ms:.2f} |"
            )

        lines.append("")
        lines.append(
            "## Summary"
        )

        lines.append("")

        lines.append(
            f"PASS: **{report['summary']['PASS']}**  "
            f"FAIL: **{report['summary']['FAIL']}**  "
            f"TOTAL: **{report['summary']['TOTAL']}**"
        )

        lines.append("")
        lines.append(
            "These results were generated by executing the "
            "repository implementation. No PASS status is "
            "manufactured for a failed assertion."
        )

        (output / "rerun_results.md").write_text(
            "\n".join(lines),
            encoding="utf-8"
        )

        # ----------------------------------------------------
        # TERMINAL OUTPUT
        # ----------------------------------------------------

        print()
        print("=" * 78)
        print("RESULTS")
        print("=" * 78)

        for r in results:

            print(
                f"{r.case:<6} "
                f"{r.status:<6} "
                f"{r.actual[:220]}"
            )

        print()
        print("=" * 78)

        print(
            f"PASS  = {report['summary']['PASS']}"
        )

        print(
            f"FAIL  = {report['summary']['FAIL']}"
        )

        print(
            f"TOTAL = {report['summary']['TOTAL']}"
        )

        print("=" * 78)

        print()
        print(
            f"Evidence written to:"
        )

        print(output)

        print()
        print(
            "Files:"
        )

        print(
            output /
            "rerun_results.md"
        )

        print(
            output /
            "rerun_results.json"
        )

        print()

        return (
            0
            if report["summary"]["FAIL"] == 0
            else 1
        )

    except Exception as exc:

        print()
        print("=" * 78)
        print("RUNNER ERROR")
        print("=" * 78)
        print(
            f"{type(exc).__name__}: {exc}"
        )
        print()
        traceback.print_exc()
        print()
        print(
            f"Output directory: {output}"
        )

        return 2

    finally:

        shutil.rmtree(
            root,
            ignore_errors=True
        )


if __name__ == "__main__":
    raise SystemExit(main())
