#!/usr/bin/env python3
"""
TiC / Cigna thesis evidence runner
===================================

Repository-specific functional and evidence runner for the Cigna TiC pipeline.

Design goals:
  * exercises the actual repository modules, not a reimplementation;
  * uses isolated synthetic fixtures for deterministic functional tests;
  * does not modify the repository's production database or Cigna source files;
  * measures safe, read-only production snapshots when existing artifacts are present;
  * optionally tests the external benchmark API if it is already running;
  * records dashboard cases as manual/unsupported where a browser/API repository
    is not present in this Cigna repository.

Run from the Cigna repository root (or place this script beside the repository files):
    python thesis_evidence_runner_cigna.py

Optional API testing:
    python thesis_evidence_runner_cigna.py --api-url http://127.0.0.1:5544

Optional explicit paths:
    python thesis_evidence_runner_cigna.py --repo . --out thesis_test_evidence

The script requires the same Python dependencies as the repository:
    duckdb, ijson, requests
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import platform
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import zipfile
import gzip
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Optional

SCRIPT_VERSION = "1.2"


@dataclass
class TestResult:
    case: str
    area: str
    description: str
    expected: str
    status: str  # PASS | FAIL | MANUAL | NOT RUN | BLOCKED
    actual: str
    elapsed_ms: float
    notes: str = ""


class LocalFileServer:
    """Tiny deterministic HTTP server used only for download/runner fixtures."""

    def __init__(self, files: dict[str, bytes], missing: set[str] | None = None):
        self.files = files
        self.missing = missing or set()
        parent = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):  # noqa: N802
                name = urllib.parse.urlparse(self.path).path.lstrip("/")
                if name in parent.missing or name not in parent.files:
                    body = b"not found"
                    self.send_response(404)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                body = parent.files[name]
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args):
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_port}"

    def start(self):
        self.thread.start()
        return self

    def stop(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def valid_npi(prefix9: str) -> int:
    """Generate a CMS-check-digit-valid synthetic NPI."""
    s = str(prefix9)
    if len(s) != 9 or not s.isdigit():
        raise ValueError("prefix9 must be exactly 9 digits")
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
NPI_EMBEDDED = valid_npi("199999999")
NPI_ORG_2 = valid_npi("211111111")
NPI_BAD = NPI_ORG + 1  # deliberately invalid check digit


def synthetic_mrf(plan_id: str = "SYNTH-01", source_variant: str = "one") -> dict[str, Any]:
    """Small MRF-shaped fixture with 5 institutional and 1 professional row."""
    embedded = {
        "npi": [NPI_EMBEDDED],
        "tin": {"type": "ein", "value": "12-3456789"},
        "business_name": "Embedded Facility",
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
                        "npi": [NPI_ORG, NPI_INDIV],
                        "tin": {"type": "ein", "value": "12-3456789"},
                        "business_name": "Organization Facility",
                    }
                ],
            },
            {
                "provider_group_id": 102,
                "network_name": ["Synthetic Network 2"],
                "provider_groups": [
                    {
                        "npi": [NPI_ORG_2],
                        "tin": {"type": "ein", "value": "98-7654321"},
                        "business_name": "Second Facility",
                    }
                ],
            },
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
                                "expiration_date": "2026-12-31",
                            },
                            {
                                "negotiated_type": "negotiated",
                                "negotiated_rate": 999.0,
                                "billing_class": "professional",
                                "setting": "outpatient",
                                "service_code": ["11"],
                                "billing_code_modifier": [],
                                "expiration_date": "2026-12-31",
                            },
                        ],
                    },
                    {
                        "provider_groups": [embedded],
                        "negotiated_prices": [
                            {
                                "negotiated_type": "negotiated",
                                "negotiated_rate": 125.0,
                                "billing_class": "institutional",
                                "setting": "outpatient",
                                "service_code": ["11"],
                                "billing_code_modifier": [],
                                "expiration_date": "2026-12-31",
                            }
                        ],
                    },
                ],
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
                                "expiration_date": "2026-12-31",
                            }
                        ],
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
                                "expiration_date": "2026-12-31",
                            }
                        ],
                    },
                    {
                        "provider_references": [101],
                        "negotiated_prices": [
                            {
                                "negotiated_type": "fee schedule",
                                "negotiated_rate": 650.0,
                                "billing_class": "institutional",
                                "setting": "inpatient",
                                "service_code": [],
                                "billing_code_modifier": [],
                                "expiration_date": "2026-12-31",
                            }
                        ],
                    },
                ],
            },
        ],
    }



def write_json(path: Path, obj: Any):
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def make_fixture_files(root: Path) -> dict[str, Path]:
    obj = synthetic_mrf()
    plain = root / "fixture.json"
    gz = root / "fixture.json.gz"
    z = root / "fixture.zip"
    zgz = root / "fixture-gzip-member.zip"
    bad = root / "fixture-truncated.json"
    no_member = root / "not-in-network.zip"

    raw = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    plain.write_bytes(raw)
    with gzip.open(gz, "wb") as f:
        f.write(raw)
    with zipfile.ZipFile(z, "w", compression=zipfile.ZIP_DEFLATED) as arc:
        arc.writestr("sample/in-network-rates.json", raw)
    gz_member = gzip.compress(raw)
    with zipfile.ZipFile(zgz, "w", compression=zipfile.ZIP_DEFLATED) as arc:
        arc.writestr("sample/in-network-rates.json.gz", gz_member)
    bad.write_bytes(raw[:-40])
    with zipfile.ZipFile(no_member, "w", compression=zipfile.ZIP_DEFLATED) as arc:
        arc.writestr("sample/other.json", b"{}")

    second = root / "fixture-second.json"
    write_json(second, synthetic_mrf(plan_id="SYNTH-02", source_variant="two"))
    return {k: v for k, v in {
        "plain": plain, "gz": gz, "zip": z, "zip_gz": zgz,
        "bad": bad, "no_member": no_member, "second": second
    }.items()}


def new_transparency_db(path: Path, repo: Path):
    import duckdb  # repository dependency
    con = duckdb.connect(str(path))
    try:
        con.execute((repo / "schema.sql").read_text(encoding="utf-8"))
    finally:
        con.close()


def fresh_parsed_db(path: Path, fixture: Path, label: str, repo: Path):
    import duckdb
    import stream_parser
    new_transparency_db(path, repo)
    con = duckdb.connect(str(path))
    try:
        stream_parser.process_file(con, str(fixture), label)
    finally:
        con.close()


def get_counts(db: Path) -> dict[str, int]:
    import duckdb
    con = duckdb.connect(str(db), read_only=True)
    try:
        out = {}
        for table in ("payers", "billing_codes", "negotiated_rates", "providers"):
            out[table] = int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        return out
    finally:
        con.close()


def file_size(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0


def safe_text(v: Any) -> str:
    try:
        return json.dumps(v, default=str, sort_keys=True)
    except Exception:
        return str(v)


def discover_repo(candidate: Path) -> Path:
    candidate = candidate.resolve()
    if (candidate / "stream_parser.py").exists() and (candidate / "schema.sql").exists():
        return candidate
    # Allow script to sit beside the repo root.
    for p in [candidate, candidate / "tic-cigna", candidate.parent]:
        if (p / "stream_parser.py").exists() and (p / "schema.sql").exists():
            return p.resolve()
    raise FileNotFoundError(
        f"Could not find the TiC Cigna repository (stream_parser.py + schema.sql) near {candidate}"
    )


def import_repo(repo: Path):
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))
    import stream_parser  # noqa: F401
    import ingest_utils  # noqa: F401
    return stream_parser, ingest_utils


def call_case(results: list[TestResult], case: str, area: str, desc: str,
              expected: str, fn: Callable[[], tuple[str, str]], notes: str = ""):
    t0 = time.perf_counter()
    try:
        status, actual = fn()
    except Exception as e:
        status = "FAIL"
        actual = f"{type(e).__name__}: {e}"
        notes = (notes + " | " if notes else "") + traceback.format_exc(limit=3).replace("\n", " ")
    results.append(TestResult(case, area, desc, expected, status, actual,
                              (time.perf_counter() - t0) * 1000.0, notes))


def expect(condition: bool, actual: str) -> tuple[str, str]:
    return ("PASS" if condition else "FAIL", actual)


def test_ingestion(results, root: Path, fixtures: dict[str, Path], repo: Path):
    import duckdb
    import stream_parser

    # TC-01
    def tc01():
        db = root / "tc01.duckdb"
        fresh_parsed_db(db, fixtures["plain"], "fixture.json", repo)
        c = get_counts(db)
        return expect(c == {"payers": 1, "billing_codes": 2, "negotiated_rates": 5, "providers": 4}, safe_text(c))
    call_case(results, "TC-01", "Ingestion", "Ingest plain JSON fixture",
              "payers=1, billing_codes=2, negotiated_rates=5, providers=4", tc01)

    # TC-02
    def tc02():
        db = root / "tc02.duckdb"
        fresh_parsed_db(db, fixtures["gz"], "fixture.json.gz", repo)
        c = get_counts(db)
        return expect(c == {"payers": 1, "billing_codes": 2, "negotiated_rates": 5, "providers": 4}, safe_text(c))
    call_case(results, "TC-02", "Ingestion", "Ingest GZIP-compressed JSON fixture",
              "same normalized counts as TC-01", tc02)

    # TC-03
    def tc03():
        db = root / "tc03.duckdb"
        fresh_parsed_db(db, fixtures["zip"], "fixture.zip", repo)
        c = get_counts(db)
        return expect(c == {"payers": 1, "billing_codes": 2, "negotiated_rates": 5, "providers": 4}, safe_text(c))
    call_case(results, "TC-03", "Ingestion", "Ingest ZIP archive with JSON member fixture",
              "same normalized counts as TC-01", tc03)

    # TC-04
    def tc04():
        db = root / "tc04.duckdb"
        fresh_parsed_db(db, fixtures["zip_gz"], "fixture-gzip-member.zip", repo)
        c = get_counts(db)
        return expect(c == {"payers": 1, "billing_codes": 2, "negotiated_rates": 5, "providers": 4}, safe_text(c))
    call_case(results, "TC-04", "Ingestion", "Ingest ZIP archive with GZIP member",
              "same normalized counts as TC-01", tc04)

    # TC-05
    def tc05():
        db = root / "tc05.duckdb"
        fresh_parsed_db(db, fixtures["plain"], "fixture.json", repo)
        con = duckdb.connect(str(db), read_only=True)
        try:
            bad = int(con.execute("SELECT COUNT(*) FROM negotiated_rates WHERE billing_class <> 'institutional' OR billing_class IS NULL").fetchone()[0])
        finally:
            con.close()
        return expect(bad == 0, f"non_institutional_rows={bad}")
    call_case(results, "TC-05", "Ingestion", "Professional price entries are excluded",
              "0 non-institutional rows in negotiated_rates", tc05)

    # TC-06
    def tc06():
        db1 = root / "tc06_default.duckdb"
        fresh_parsed_db(db1, fixtures["plain"], "fixture.json", repo)
        default_counts = get_counts(db1)
        db2 = root / "tc06_batch2.duckdb"
        new_transparency_db(db2, repo)
        con = duckdb.connect(str(db2))
        old = stream_parser.BATCH_SIZE
        stream_parser.BATCH_SIZE = 2
        try:
            stream_parser.process_file(con, str(fixtures["plain"]), "fixture.json")
        finally:
            stream_parser.BATCH_SIZE = old
            con.close()
        batch_counts = get_counts(db2)
        return expect(default_counts == batch_counts and batch_counts["negotiated_rates"] == 5,
                      f"default={default_counts}; batch_size=2={batch_counts}")
    call_case(results, "TC-06", "Ingestion", "Result independent of batch size",
              "same normalized counts with BATCH_SIZE forced to 2", tc06)

    # TC-07
    def tc07():
        payload = json.loads(fixtures["plain"].read_text(encoding="utf-8"))
        payload["in_network"] = [payload["in_network"][0]] * 1
        payload["in_network"][0]["description"] = "X" * 100
        # Put a large trailing blob after the array-start point.
        raw = (json.dumps({
            **{k: v for k, v in payload.items() if k != "in_network"},
            "in_network": payload["in_network"],
            "trailing": "Z" * 3_500_000,
        }, separators=(",", ":"))).encode("utf-8")

        class CountingBytes(io.BytesIO):
            def __init__(self, b: bytes):
                super().__init__(b)
                self.total_read = 0
            def read(self, *args, **kwargs):
                out = super().read(*args, **kwargs)
                self.total_read += len(out)
                return out

        s = CountingBytes(raw)
        header = stream_parser.extract_header(s)
        frac = s.total_read / max(len(raw), 1)
        ok = bool(header.get("reporting_entity_name")) and frac < 0.25
        return expect(ok, f"header_fields={len(header)}; bytes_read={s.total_read}; total={len(raw)}; fraction={frac:.3f}")
    call_case(results, "TC-07", "Ingestion", "Header extraction stops at in_network start",
              "header extracted after reading only a small fraction of the stream", tc07)

    # TC-08
    def tc08():
        db = root / "tc08.duckdb"
        fresh_parsed_db(db, fixtures["plain"], "fixture.json", repo)
        con = duckdb.connect(str(db), read_only=True)
        try:
            rows = con.execute("SELECT COUNT(*), COUNT(DISTINCT npi) FROM providers WHERE provider_reference_id=101").fetchone()
        finally:
            con.close()
        return expect(rows == (2, 2), f"ref101_rows={rows[0]}; distinct_npis={rows[1]}")
    call_case(results, "TC-08", "Ingestion", "Multiple NPIs preserved under one provider reference",
              "provider_reference_id=101 has 2 provider rows with 2 distinct NPIs", tc08)

    # TC-09
    def tc09():
        db = root / "tc09.duckdb"
        fresh_parsed_db(db, fixtures["plain"], "fixture.json", repo)
        con = duckdb.connect(str(db), read_only=True)
        try:
            provider_ids = [r[0] for r in con.execute("SELECT DISTINCT provider_reference_id FROM providers WHERE provider_reference_id < 0").fetchall()]
            rate_neg = int(con.execute("SELECT COUNT(*) FROM negotiated_rates r WHERE EXISTS (SELECT 1 FROM unnest(r.provider_reference_ids) x(id) WHERE id < 0)").fetchone()[0])
        finally:
            con.close()
        return expect(len(provider_ids) == 1 and rate_neg == 1,
                      f"negative_provider_ids={provider_ids}; rate_rows_with_negative_ref={rate_neg}")
    call_case(results, "TC-09", "Ingestion", "Embedded provider group gets synthetic negative identifier",
              "one negative provider reference appears in providers and a rate row", tc09)

    # TC-10
    def tc10():
        db = root / "tc10.duckdb"
        new_transparency_db(db, repo)
        con = duckdb.connect(str(db))
        try:
            stream_parser.process_file(con, str(fixtures["plain"]), "fixture.json")
            stream_parser.process_file(con, str(fixtures["second"]), "fixture-second.json")
        finally:
            con.close()
        con = duckdb.connect(str(db), read_only=True)
        try:
            syn = int(con.execute("SELECT COUNT(DISTINCT provider_reference_id) FROM providers WHERE provider_reference_id < 0").fetchone()[0])
            payers = int(con.execute("SELECT COUNT(*) FROM payers").fetchone()[0])
        finally:
            con.close()
        return expect(syn == 1 and payers == 2, f"distinct_negative_ids={syn}; payer_rows={payers}")
    call_case(results, "TC-10", "Ingestion", "Repeated embedded provider group reuses synthetic identifier",
              "1 synthetic identifier across 2 files; 2 payer rows", tc10)

    # TC-11
    def tc11():
        db = root / "tc11.duckdb"
        new_transparency_db(db, repo)
        con = duckdb.connect(str(db))
        try:
            try:
                stream_parser.process_file(con, str(fixtures["bad"]), "fixture-truncated.json")
            except Exception as e:
                err = f"{type(e).__name__}: {e}"
            else:
                err = "NO EXCEPTION"
        finally:
            con.close()
        counts = get_counts(db)
        ok = ("IncompleteJSONError" in err or "IncompleteJSON" in err) and all(v == 0 for v in counts.values())
        return expect(ok, f"exception={err}; counts_after_failure={counts}")
    call_case(results, "TC-11", "Ingestion", "Malformed JSON rolls back the file transaction",
              "exception raised and all normalized tables remain empty", tc11)

    # TC-12
    def tc12():
        db = root / "tc12.duckdb"
        new_transparency_db(db, repo)
        con = duckdb.connect(str(db))
        try:
            try:
                stream_parser.process_file(con, str(fixtures["no_member"]), "not-in-network.zip")
            except Exception as e:
                err = f"{type(e).__name__}: {e}"
            else:
                err = "NO EXCEPTION"
        finally:
            con.close()
        return expect(err.startswith("ValueError:") and "No in-network-rates JSON member" in err, err)
    call_case(results, "TC-12", "Ingestion", "ZIP without in-network-rates member is rejected",
              "ValueError naming the missing member", tc12)


def test_acquisition(results, root: Path, fixtures: dict[str, Path], repo: Path):
    import ingest_utils

    runner_bytes = [
        json.dumps(synthetic_mrf("RUN-01"), separators=(",", ":")).encode("utf-8"),
        json.dumps(synthetic_mrf("RUN-02"), separators=(",", ":")).encode("utf-8"),
        json.dumps(synthetic_mrf("RUN-03"), separators=(",", ":")).encode("utf-8"),
    ]
    files = {
        "file1.bin": b"alpha" * 100, "file2.bin": b"beta" * 100, "file3.bin": b"gamma" * 100,
        "wrong.bin": b"wrong-content",
        "file1.json": runner_bytes[0], "file2.json": runner_bytes[1], "file3.json": runner_bytes[2],
    }
    server = LocalFileServer(files, missing={"missing.bin"}).start()
    try:
        # TC-13
        def tc13():
            target = root / "dl13" / "valid.bin"
            meta = ingest_utils.download_file(f"{server.base_url}/file1.bin", str(target), expected_size=len(files["file1.bin"]))
            ok = target.exists() and target.stat().st_size == len(files["file1.bin"]) and len(meta.get("sha256", "")) == 64 and not Path(str(target) + ".tmp").exists()
            return expect(ok, safe_text({"size": target.stat().st_size if target.exists() else 0, "sha256_len": len(meta.get("sha256", "")), "cached": meta.get("cached")}))
        call_case(results, "TC-13", "Acquisition / Runner", "Streamed download validates size and records SHA-256",
                  "target exists, expected size matches, SHA-256 is 64 hex chars, no .tmp", tc13)

        # TC-14
        def tc14():
            target = root / "dl14" / "cached.bin"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(files["file1.bin"])
            meta = ingest_utils.download_file(f"{server.base_url}/file1.bin", str(target), expected_size=len(files["file1.bin"]))
            return expect(meta.get("cached") is True, safe_text(meta))
        call_case(results, "TC-14", "Acquisition / Runner", "Matching existing file is detected as cached",
                  "cached=True without a new download", tc14)

        # TC-15
        def tc15():
            target = root / "dl15" / "mismatch.bin"
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                ingest_utils.download_file(f"{server.base_url}/file1.bin", str(target), expected_size=99999, retries=1)
            except Exception as e:
                err = f"{type(e).__name__}: {e}"
            else:
                err = "NO EXCEPTION"
            ok = "RuntimeError" in err and not target.exists() and not Path(str(target) + ".tmp").exists()
            return expect(ok, f"exception={err}; target_exists={target.exists()}; tmp_exists={Path(str(target)+'.tmp').exists()}")
        call_case(results, "TC-15", "Acquisition / Runner", "Expected-size mismatch is rejected after bounded retries",
                  "RuntimeError; target and temporary file absent", tc15)

        # TC-16
        def tc16():
            target = root / "dl16" / "missing.bin"
            try:
                ingest_utils.download_file(f"{server.base_url}/missing.bin", str(target), retries=1)
            except Exception as e:
                err = f"{type(e).__name__}: {e}"
            else:
                err = "NO EXCEPTION"
            return expect("RuntimeError" in err, err)
        call_case(results, "TC-16", "Acquisition / Runner", "Unavailable remote file fails clearly",
                  "RuntimeError after retries", tc16)

        # TC-17
        def tc17():
            log = root / "tc17.jsonl"
            ingest_utils.append_run_log(str(log), status="complete", index=7, filename="x.bin", url="http://example/x")
            lines = log.read_text(encoding="utf-8").splitlines()
            obj = json.loads(lines[0])
            keys = set(obj)
            ok = len(lines) == 1 and {"timestamp", "status", "index", "filename"}.issubset(keys) and obj["status"] == "complete"
            return expect(ok, safe_text(obj))
        call_case(results, "TC-17", "Acquisition / Runner", "Run log event is one JSON line with UTC timestamp",
                  "JSON object with timestamp, status, index and filename", tc17)

        # Runner integration cases use the actual runner.py subprocess.
        manifest = root / "runner_manifest.json"
        progress = root / "runner_progress.txt"
        run_log = root / "runner_runs.jsonl"
        db = root / "runner.duckdb"
        manifest_obj = {"blobs": [
            {"name": "file1.json", "downloadUrl": f"{server.base_url}/file1.json", "size_bytes": len(files["file1.json"])},
            {"name": "file2.json", "downloadUrl": f"{server.base_url}/file2.json", "size_bytes": len(files["file2.json"])},
            {"name": "file3.json", "downloadUrl": f"{server.base_url}/file3.json", "size_bytes": len(files["file3.json"])},
        ]}
        write_json(manifest, manifest_obj)

        def run_runner(max_files: Optional[int] = None):
            cmd = [sys.executable, str(repo / "runner.py"),
                   "--manifest", str(manifest), "--db", str(db),
                   "--progress", str(progress), "--schema", str(repo / "schema.sql"),
                   "--download-dir", str(root / "downloads"), "--run-log", str(run_log)]
            if max_files is not None:
                cmd += ["--max-files", str(max_files)]
            return subprocess.run(cmd, cwd=str(repo), capture_output=True, text=True, timeout=300)

        # TC-18
        def tc18():
            for p in [progress, run_log, db]:
                if p.exists():
                    p.unlink()
            d = root / "downloads"
            if d.exists():
                shutil.rmtree(d)
            proc = run_runner(2)
            value = int(progress.read_text(encoding="utf-8").strip()) if progress.exists() else -1
            complete_events = 0
            if run_log.exists():
                for line in run_log.read_text(encoding="utf-8").splitlines():
                    if line.strip() and json.loads(line).get("status") == "complete":
                        complete_events += 1
            ok = proc.returncode == 0 and value == 2 and complete_events == 2
            return expect(ok, f"returncode={proc.returncode}; progress={value}; complete_events={complete_events}")
        call_case(results, "TC-18", "Acquisition / Runner", "Runner processes bounded number of files and records progress",
                  "progress=2 after --max-files 2", tc18)

        # TC-19
        def tc19():
            proc = run_runner()
            value = int(progress.read_text(encoding="utf-8").strip()) if progress.exists() else -1
            complete_events = 0
            if run_log.exists():
                for line in run_log.read_text(encoding="utf-8").splitlines():
                    if line.strip() and json.loads(line).get("status") == "complete":
                        complete_events += 1
            left = [p.name for p in (root / "downloads").glob("*")] if (root / "downloads").exists() else []
            ok = proc.returncode == 0 and value == 3 and complete_events == 3 and not left
            return expect(ok, f"returncode={proc.returncode}; progress={value}; complete_events={complete_events}; downloads_left={left}")
        call_case(results, "TC-19", "Acquisition / Runner", "Runner resumes from the progress counter",
                  "remaining file processed, progress=3, three complete events, downloads cleaned", tc19)

        # TC-20
        def tc20():
            proc = run_runner()
            ok = proc.returncode == 0 and "All files have already been processed!" in proc.stdout
            return expect(ok, f"returncode={proc.returncode}; matched_message={'All files have already been processed!' in proc.stdout}")
        call_case(results, "TC-20", "Acquisition / Runner", "Runner does nothing after completion",
                  "completion message and no further processing", tc20)

        # TC-21
        def tc21():
            fail_manifest = root / "failure_manifest.json"
            fail_progress = root / "failure_progress.txt"
            fail_log = root / "failure_runs.jsonl"
            fail_db = root / "failure.duckdb"
            write_json(fail_manifest, {"blobs": [
                {"name": "missing.bin", "downloadUrl": f"{server.base_url}/missing.bin", "size_bytes": 50},
                {"name": "file1.bin", "downloadUrl": f"{server.base_url}/file1.bin", "size_bytes": len(files["file1.bin"])},
            ]})
            proc = subprocess.run([
                sys.executable, str(repo / "runner.py"), "--manifest", str(fail_manifest),
                "--db", str(fail_db), "--progress", str(fail_progress),
                "--schema", str(repo / "schema.sql"), "--download-dir", str(root / "failure_downloads"),
                "--run-log", str(fail_log)
            ], cwd=str(repo), capture_output=True, text=True, timeout=300)
            progress_value = int(fail_progress.read_text(encoding="utf-8").strip()) if fail_progress.exists() else 0
            failed = []
            if fail_log.exists():
                failed = [json.loads(x) for x in fail_log.read_text(encoding="utf-8").splitlines() if x.strip()]
            ok = proc.returncode == 0 and progress_value == 0 and any(e.get("status") == "failed" for e in failed)
            return expect(ok, f"returncode={proc.returncode}; progress={progress_value}; events={safe_text(failed)}")
        call_case(results, "TC-21", "Acquisition / Runner", "Download failure stops the runner without advancing progress",
                  "failed log event and progress remains at 0", tc21)
    finally:
        server.stop()


def make_benchmark_fixture(db: Path, enrichment: Path, repo: Path):
    import duckdb
    new_transparency_db(db, repo)
    # Build enrichment DB with the shape actually consumed by build_benchmarks.py
    e = duckdb.connect(str(enrichment))
    try:
        e.execute("DROP TABLE IF EXISTS nppes")
        e.execute("DROP TABLE IF EXISTS zip_county")
        e.execute("DROP TABLE IF EXISTS code_descriptions")
        e.execute("""
            CREATE TABLE nppes (
                npi BIGINT PRIMARY KEY, entity_type VARCHAR, provider_name VARCHAR,
                first_name VARCHAR, last_name VARCHAR, org_name VARCHAR,
                city VARCHAR, state VARCHAR, zip5 VARCHAR, taxonomy_code VARCHAR,
                taxonomy_is_primary VARCHAR
            )
        """)
        e.execute("""
            CREATE TABLE zip_county (
                zip5 VARCHAR, fips VARCHAR, county_name VARCHAR, state_abbr VARCHAR, tot_ratio DOUBLE
            )
        """)
        # Note: build_benchmarks.py expects code_descriptions.description; the loader
        # in this repository creates that exact column in its actual runtime schema.
        e.execute("""
            CREATE TABLE code_descriptions (
                billing_code VARCHAR NOT NULL, billing_code_type VARCHAR NOT NULL,
                description VARCHAR NOT NULL, PRIMARY KEY (billing_code, billing_code_type)
            )
        """)
        e.executemany("INSERT INTO nppes VALUES (?,?,?,?,?,?,?,?,?,?,?)", [
            (NPI_ORG, "2", "Org Provider", None, None, "Org Provider", "Houston", "TX", "77001", "207Q00000X", "Y"),
            (NPI_INDIV, "1", "Individual Provider", "Ada", "Example", None, "Houston", "TX", "77001", "207R00000X", "Y"),
            (NPI_ORG_2, "2", "Second Provider", None, None, "Second Provider", "Houston", "TX", "77001", "207Q00000X", "Y"),
        ])
        e.executemany("INSERT INTO zip_county VALUES (?,?,?,?,?)", [
            ("77001", "48201", "Harris County", "TX", 0.7),
            ("77001", "48157", "Fort Bend County", "TX", 0.3),
        ])
        e.execute("INSERT INTO code_descriptions VALUES (?,?,?)", ("99213", "CPT", "Office o/p visit est"))
        e.execute("CHECKPOINT")
    finally:
        e.close()

    c = duckdb.connect(str(db))
    try:
        p1 = c.execute("""
            INSERT INTO payers (reporting_entity_name, reporting_entity_type, plan_name, plan_id,
                                plan_id_type, plan_market_type, last_updated_on, version, source_file)
            VALUES (?,?,?,?,?,?,?,?,?) RETURNING payer_id
        """, ("Payer One", "health insurance issuer", "Plan A", "P1", "HIOS", "group", "2026-09-01", "1", "p1.json")).fetchone()[0]
        p2 = c.execute("""
            INSERT INTO payers (reporting_entity_name, reporting_entity_type, plan_name, plan_id,
                                plan_id_type, plan_market_type, last_updated_on, version, source_file)
            VALUES (?,?,?,?,?,?,?,?,?) RETURNING payer_id
        """, ("Payer Two", "health insurance issuer", "Plan B", "P2", "HIOS", "group", "2026-09-01", "1", "p2.json")).fetchone()[0]
        code1 = c.execute("""
            INSERT INTO billing_codes (billing_code, billing_code_type, billing_code_type_version,
                                       description, name, negotiation_arrangement)
            VALUES (?,?,?,?,?,?) RETURNING code_id
        """, ("99213", "CPT", "2026", "MRF office visit", "Office Visit", "ffs")).fetchone()[0]
        code2 = c.execute("""
            INSERT INTO billing_codes (billing_code, billing_code_type, billing_code_type_version,
                                       description, name, negotiation_arrangement)
            VALUES (?,?,?,?,?,?) RETURNING code_id
        """, ("470", "MS-DRG", "2026", "MRF DRG", "DRG 470", "ffs")).fetchone()[0]
        c.executemany("""
            INSERT INTO providers (provider_reference_id, npi, tin_type, tin_value, facility_name, network_name, group_key)
            VALUES (?,?,?,?,?,?,?)
        """, [
            (101, NPI_INDIV, "ein", "12-3456789", "Individual Facility", ["Network"], "ref:101"),
            (101, NPI_ORG, "ein", "12-3456789", "Organization Facility", ["Network"], "ref:101"),
            (102, NPI_ORG_2, "ein", "98-7654321", "Second Facility", ["Network 2"], "ref:102"),
        ])
        rates = [
            (p1, code1, "ffs", "institutional", "outpatient", "negotiated", 100.0, ["11"], [], "2026-12-31", [101], "p1.json"),
            (p1, code1, "ffs", "institutional", "outpatient", "negotiated", 150.0, ["11"], [], "2026-12-31", [101], "p1.json"),
            (p2, code1, "ffs", "institutional", "outpatient", "negotiated", 110.0, ["11"], [], "2026-12-31", [101], "p2.json"),
            (p2, code1, "ffs", "institutional", "outpatient", "negotiated", 130.0, ["11"], [], "2026-12-31", [102], "p2.json"),
            (p1, code2, "ffs", "institutional", "inpatient", "negotiated", 0.0, [], [], "2026-12-31", [101], "p1.json"),
            (p1, code2, "ffs", "institutional", "inpatient", "negotiated", 20000000.0, [], [], "2026-12-31", [102], "p1.json"),
            (p2, code2, "ffs", "institutional", "inpatient", "percentage", 5000.0, [], [], "2026-12-31", [101], "p2.json"),
        ]
        c.executemany("""
            INSERT INTO negotiated_rates (
                payer_id, code_id, negotiation_arrangement, billing_class, setting,
                negotiated_type, negotiated_rate, service_code, billing_code_modifier,
                expiration_date, provider_reference_ids, source_file
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, rates)
        c.execute("CHECKPOINT")
    finally:
        c.close()


def run_benchmark_case(root: Path, repo: Path, approx: bool = False, skip_indexes=False, skip_stats=False) -> Path:
    import build_benchmarks
    db = root / ("benchmark_approx.duckdb" if approx else "benchmark_exact.duckdb")
    enrichment = root / ("enrichment_approx.duckdb" if approx else "enrichment_exact.duckdb")
    make_benchmark_fixture(db, enrichment, repo)
    build_benchmarks.build(str(db), str(enrichment), True, False, skip_indexes, skip_stats,
                           2, "512MB", str(root / "duckdb_tmp"), approx)
    return db


def show_tables(db: Path) -> list[str]:
    import duckdb
    con = duckdb.connect(str(db), read_only=True)
    try:
        return sorted(r[0] for r in con.execute("SHOW TABLES").fetchall())
    finally:
        con.close()


def test_benchmarks(results, root: Path, repo: Path):
    import duckdb

    def tc22():
        db = run_benchmark_case(root, repo)
        con = duckdb.connect(str(db), read_only=True)
        try:
            n = int(con.execute("SELECT COUNT(*) FROM benchmarks").fetchone()[0])
        finally:
            con.close()
        return expect(n == 4, f"benchmarks_rows={n}")
    call_case(results, "TC-22", "Benchmark", "Benchmark build applies rate-quality filter",
              "4 qualifying rows remain", tc22)

    def tc23():
        db = run_benchmark_case(root, repo, approx=False, skip_indexes=False, skip_stats=False)
        tables = show_tables(db)
        stats = {t for t in tables if t.startswith("benchmarks_") and t.endswith("stats")}
        expected = {"benchmarks_code_stats", "benchmarks_geo_stats", "benchmarks_geo_payer_stats",
                    "benchmarks_payer_stats", "benchmarks_payer_provider_stats", "benchmarks_provider_stats"}
        return expect("benchmarks" in tables and stats == expected,
                      f"benchmark_tables={tables}; benchmark_stat_tables={sorted(stats)}")
    call_case(results, "TC-23", "Benchmark", "Base benchmark and six statistical tables are created",
              "benchmarks + six benchmarks_*_stats tables", tc23)

    def tc24():
        con = duckdb.connect(str(root / "benchmark_exact.duckdb"), read_only=True)
        try:
            row = con.execute("SELECT county_fips, county_name FROM benchmarks WHERE billing_code='99213' LIMIT 1").fetchone()
        finally:
            con.close()
        return expect(row == ("48201", "Harris County"), f"row={row}")
    call_case(results, "TC-24", "Benchmark", "Dominant ZIP county is selected",
              "ZIP 77001 resolves to Harris County (ratio 0.7)", tc24)

    def tc25():
        con = duckdb.connect(str(root / "benchmark_exact.duckdb"), read_only=True)
        try:
            row = con.execute("SELECT npi, npi_count FROM benchmarks WHERE billing_code='99213' ORDER BY negotiated_rate LIMIT 1").fetchone()
        finally:
            con.close()
        return expect(row == (NPI_ORG, 2), f"selected_npi={row[0] if row else None}; npi_count={row[1] if row else None}")
    call_case(results, "TC-25", "Benchmark", "Provider bridge prefers organization NPI",
              f"selected NPI={NPI_ORG} with npi_count=2", tc25)

    def tc26():
        con = duckdb.connect(str(root / "benchmark_exact.duckdb"), read_only=True)
        try:
            row = con.execute("SELECT code_description, has_canonical_description FROM benchmarks WHERE billing_code='99213' LIMIT 1").fetchone()
        finally:
            con.close()
        return expect(row == ("Office o/p visit est", True), f"description={row[0] if row else None}; canonical={row[1] if row else None}")
    call_case(results, "TC-26", "Benchmark", "Canonical code description overrides MRF free text",
              "canonical description present and marked true", tc26)

    def tc27():
        db = run_benchmark_case(root, repo, approx=True)
        con = duckdb.connect(str(db), read_only=True)
        try:
            stats = [t for t in show_tables(db) if t.startswith("benchmarks_") and t.endswith("stats")]
            n = int(con.execute("SELECT COUNT(*) FROM benchmarks").fetchone()[0])
        finally:
            con.close()
        return expect(n == 4 and len(stats) == 6, f"benchmark_rows={n}; stats_tables={stats}")
    call_case(results, "TC-27", "Benchmark", "Approximate-percentile option builds statistics",
              "benchmark build completes and all six statistical tables exist", tc27)

    def tc28():
        db = root / "benchmark_skip.duckdb"
        enrichment = root / "enrichment_skip.duckdb"
        make_benchmark_fixture(db, enrichment, repo)
        import build_benchmarks
        build_benchmarks.build(str(db), str(enrichment), True, False, True, True, 1, "512MB", str(root / "duckdb_tmp"), False)
        tables = show_tables(db)
        extra = [t for t in tables if t.startswith("benchmarks_") and t.endswith("stats")]
        return expect("benchmarks" in tables and not extra,
                      f"tables={tables}; benchmark_stat_tables={extra}")
    call_case(results, "TC-28", "Benchmark", "Skip-indexes + skip-stats produces only base benchmark table",
              "no benchmark stat tables; base benchmarks table remains", tc28)


def copy_db(src: Path, dst: Path):
    shutil.copy2(src, dst)


def validation_json(db: Path, out_dir: Path, fast: bool = False, fail_on: str = "error") -> tuple[subprocess.CompletedProcess[str], list[dict[str, Any]]]:
    txt = out_dir / (db.stem + ("_fast.txt" if fast else ".txt"))
    js = out_dir / (db.stem + ("_fast.json" if fast else ".json"))
    cmd = [sys.executable, str(db.parent / "__dummy__")]  # replaced by caller
    raise RuntimeError("internal helper should be invoked through validation_run")


def validation_run(repo: Path, db: Path, out_dir: Path, fast: bool = False, fail_on: str = "error"):
    txt = out_dir / (db.stem + ("_fast.txt" if fast else ".txt"))
    js = out_dir / (db.stem + ("_fast.json" if fast else ".json"))
    cmd = [sys.executable, str(repo / "validate_data.py"), "--db", str(db),
           "--txt", str(txt), "--json", str(js), "--fail-on", fail_on]
    if fast:
        cmd.append("--fast")
    proc = subprocess.run(cmd, cwd=str(repo), capture_output=True, text=True, timeout=300)
    data = json.loads(js.read_text(encoding="utf-8")) if js.exists() else []
    return proc, data


def find_check(data: list[dict[str, Any]], name: str) -> Optional[dict[str, Any]]:
    for row in data:
        if row.get("name") == name:
            return row
    return None


def test_validation(results, root: Path, repo: Path):
    # Build a valid baseline by parsing the clean fixture first.
    valid_db = root / "validation_valid.duckdb"
    fresh_parsed_db(valid_db, root / "fixtures" / "fixture.json", "fixture.json", repo)
    valid_dir = root / "validation_reports"
    valid_dir.mkdir(parents=True, exist_ok=True)

    def tc29():
        proc, data = validation_run(repo, valid_db, valid_dir, fast=False, fail_on="error")
        errors = [r for r in data if r.get("level") == "error" and not r.get("passed")]
        checks = [r for r in data if r.get("level") != "info"]
        return expect(proc.returncode == 0 and not errors,
                      f"returncode={proc.returncode}; error_failures={len(errors)}; checks={len(checks)}")
    call_case(results, "TC-29", "Validation", "Validator on a consistent synthetic database",
              "exit code 0 and no error-level failures", tc29)

    def tc30():
        proc, data = validation_run(repo, valid_db, valid_dir, fast=True, fail_on="error")
        skipped = [r for r in data if r.get("note", "").startswith("Skipped (--fast)")]
        return expect(proc.returncode == 0 and len(skipped) >= 1, f"returncode={proc.returncode}; fast_skips={len(skipped)}")
    call_case(results, "TC-30", "Validation", "Fast validator mode completes",
              "exit code 0 and full-table explode checks are skipped", tc30)

    def tc31():
        db = root / "validation_orphan_payer.duckdb"
        copy_db(valid_db, db)
        import duckdb
        c = duckdb.connect(str(db))
        try:
            c.execute("""
                INSERT INTO negotiated_rates (
                    payer_id, code_id, negotiation_arrangement, billing_class, setting,
                    negotiated_type, negotiated_rate, service_code, billing_code_modifier,
                    expiration_date, provider_reference_ids, source_file
                )
                SELECT 999999, code_id, negotiation_arrangement, billing_class, setting,
                       negotiated_type, negotiated_rate, service_code, billing_code_modifier,
                       expiration_date, provider_reference_ids, source_file
                FROM negotiated_rates LIMIT 1
            """)
            c.execute("CHECKPOINT")
        finally:
            c.close()
        proc, data = validation_run(repo, db, valid_dir, fail_on="error")
        chk = find_check(data, "orphan_payer_id") or {}
        return expect(proc.returncode == 1 and not chk.get("passed", True) and chk.get("n_bad", 0) >= 1,
                      f"returncode={proc.returncode}; orphan_payer={safe_text(chk)}")
    call_case(results, "TC-31", "Validation", "Orphan payer_id is detected",
              "orphan_payer_id fails and exit code is 1", tc31)

    def tc32():
        db = root / "validation_bad_npi.duckdb"
        copy_db(valid_db, db)
        import duckdb
        c = duckdb.connect(str(db))
        try:
            c.execute("INSERT INTO providers (provider_reference_id, npi, tin_type, tin_value, facility_name, network_name, group_key) VALUES (?,?,?,?,?,?,?)",
                      (999, NPI_BAD, "ein", "12-3456789", "Bad NPI Facility", ["N"], "ref:999"))
            c.execute("CHECKPOINT")
        finally:
            c.close()
        proc, data = validation_run(repo, db, valid_dir, fail_on="error")
        chk = find_check(data, "npi_invalid") or {}
        return expect(chk.get("n_bad", 0) >= 1 and proc.returncode == 0,
                      f"returncode={proc.returncode}; npi_invalid={safe_text(chk)}")
    call_case(results, "TC-32", "Validation", "Invalid NPI check digit is flagged",
              "npi_invalid reports at least one bad NPI", tc32)

    def tc33():
        db = root / "validation_domain.duckdb"
        copy_db(valid_db, db)
        import duckdb
        c = duckdb.connect(str(db))
        try:
            # Two zero dollar rates -> error; two outliers -> warnings; three invalid NPIs -> warning.
            rows = c.execute("SELECT payer_id, code_id, negotiation_arrangement, billing_class, setting, expiration_date, provider_reference_ids, source_file FROM negotiated_rates LIMIT 1").fetchone()
            for rate in (0.0, -1.0):
                c.execute("""
                    INSERT INTO negotiated_rates (
                        payer_id, code_id, negotiation_arrangement, billing_class, setting,
                        negotiated_type, negotiated_rate, service_code, billing_code_modifier,
                        expiration_date, provider_reference_ids, source_file
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                          (rows[0], rows[1], rows[2], rows[3], rows[4], "negotiated", rate, [], [], rows[5], rows[6], rows[7]))
            for rate in (300000.0, 500000.0):
                c.execute("""
                    INSERT INTO negotiated_rates (
                        payer_id, code_id, negotiation_arrangement, billing_class, setting,
                        negotiated_type, negotiated_rate, service_code, billing_code_modifier,
                        expiration_date, provider_reference_ids, source_file
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                          (rows[0], rows[1], rows[2], rows[3], rows[4], "negotiated", rate, [], [], rows[5], rows[6], rows[7]))
            for idx, npi in enumerate([NPI_BAD, NPI_BAD + 2, 3000000000], start=1):
                c.execute("INSERT INTO providers (provider_reference_id, npi, tin_type, tin_value, facility_name, network_name, group_key) VALUES (?,?,?,?,?,?,?)",
                          (1000 + idx, npi, "ein", "12-3456789", f"Bad {idx}", ["N"], f"ref:{1000+idx}"))
            c.execute("CHECKPOINT")
        finally:
            c.close()
        proc, data = validation_run(repo, db, valid_dir, fail_on="error")
        z = find_check(data, "dollar_rate_not_positive") or {}
        o = find_check(data, "dollar_rate_outlier") or {}
        n = find_check(data, "npi_invalid") or {}
        ok = proc.returncode == 1 and z.get("n_bad", 0) >= 2 and o.get("n_bad", 0) >= 2 and n.get("n_bad", 0) >= 1
        return expect(ok, f"returncode={proc.returncode}; zero_rate={safe_text(z)}; outlier={safe_text(o)}; npi={safe_text(n)}")
    call_case(results, "TC-33", "Validation", "Validator detects rate-domain and NPI-format violations",
              "zero-rate error; outlier warning; invalid-NPI warning; exit code 1", tc33)


def http_json(url: str, timeout: float = 10.0) -> tuple[int, Any, float, str]:
    req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "TiC-Thesis-Evidence/1.0"})
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            elapsed = (time.perf_counter() - t0) * 1000
            txt = body.decode("utf-8", errors="replace")
            try:
                obj = json.loads(txt)
            except Exception:
                obj = txt
            return r.status, obj, elapsed, txt
    except urllib.error.HTTPError as e:
        elapsed = (time.perf_counter() - t0) * 1000
        body = e.read().decode("utf-8", errors="replace")
        try:
            obj = json.loads(body)
        except Exception:
            obj = body
        return e.code, obj, elapsed, body


def api_url(base: str, path: str, params: dict[str, Any] | None = None) -> str:
    u = base.rstrip("/") + path
    if params:
        u += "?" + urllib.parse.urlencode(params)
    return u


def extract_list(obj: Any) -> list:
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        for k in ("data", "rows", "results", "items", "payers", "states", "counties", "providers"):
            if isinstance(obj.get(k), list):
                return obj[k]
    return []


def test_api(results, api_base: Optional[str], api_code: str, api_type: str, api_state: str, api_payer: str, api_county: str):
    if not api_base:
        for case, desc in [
            ("TC-34", "GET /api/health"), ("TC-35", "GET /api/code-types"),
            ("TC-36", "GET /api/benchmark/summary"), ("TC-37", "GET states/counties"),
            ("TC-38", "GET payers/payer-counties"), ("TC-39", "GET providers"),
            ("TC-40", "Missing required parameter"), ("TC-41", "Quote/SQL input rejection"),
            ("TC-42", "Unknown billing code"), ("TC-43", "Providers without state"),
        ]:
            results.append(TestResult(case, "API", desc,
                                       "HTTP/API behavior as documented in the thesis",
                                       "NOT RUN", "API server not supplied; use --api-url when the benchmark API is already running.", 0.0))
        return

    tests = [
        ("TC-34", "GET /api/health", "/api/health", {}, lambda code, obj: code == 200 and isinstance(obj, dict)),
        ("TC-35", "GET /api/code-types", "/api/code-types", {}, lambda code, obj: code == 200 and len(extract_list(obj)) >= 1),
        ("TC-36", "GET /api/benchmark/summary", "/api/benchmark/summary", {"code": api_code, "type": api_type}, lambda code, obj: code == 200),
        ("TC-37", "GET /api/benchmark/states", "/api/benchmark/states", {"code": api_code, "type": api_type}, lambda code, obj: code == 200),
        ("TC-38", "GET /api/benchmark/payers", "/api/benchmark/payers", {"code": api_code, "type": api_type, "state": api_state}, lambda code, obj: code == 200),
        ("TC-39", "GET /api/benchmark/providers", "/api/benchmark/providers", {"code": api_code, "type": api_type, "payer": api_payer, "state": api_state, "county": api_county}, lambda code, obj: code == 200),
        ("TC-40", "Missing type", "/api/benchmark/summary", {"code": api_code}, lambda code, obj: code == 400),
        ("TC-41", "Quote/SQL fragment input", "/api/benchmark/summary", {"code": "99213' OR 1=1 --", "type": api_type}, lambda code, obj: code == 400),
        ("TC-42", "Unknown billing code", "/api/benchmark/summary", {"code": "00000", "type": "CPT"}, lambda code, obj: code == 200),
        ("TC-43", "Providers without state", "/api/benchmark/providers", {"code": api_code, "type": api_type, "payer": api_payer}, lambda code, obj: code == 400),
    ]
    for case, desc, path, params, pred in tests:
        t0 = time.perf_counter()
        try:
            code, obj, ms, raw = http_json(api_url(api_base, path, params))
            ok = bool(pred(code, obj))
            actual = f"HTTP {code}; response={safe_text(obj)[:800]}"
            status = "PASS" if ok else "FAIL"
            results.append(TestResult(case, "API", desc, "behavior from thesis specification", status, actual, ms))
        except Exception as e:
            results.append(TestResult(case, "API", desc, "behavior from thesis specification", "FAIL", f"{type(e).__name__}: {e}", (time.perf_counter()-t0)*1000))


def dashboard_cases(results, repo: Path):
    # The supplied Cigna repository contains analytics.html, while the thesis
    # API/dashboard cases refer to benchmark_dashboard.html in the separate
    # benchmark/API repository. We therefore never claim those browser cases pass here.
    expected_api_dashboard = repo / "benchmark_dashboard.html"
    analytics = repo / "analytics.html"
    static_notes = []
    if analytics.exists():
        html = analytics.read_text(encoding="utf-8", errors="replace")
        static_notes.append(f"analytics.html present ({len(html):,} chars)")
        static_notes.append("DOM boot hook present" if "DOMContentLoaded" in html else "DOM boot hook missing")
        static_notes.append("Chart.js reference present" if "chart.js" in html.lower() else "Chart.js reference not found")
    if expected_api_dashboard.exists():
        static_notes.append("benchmark_dashboard.html present")

    for case, desc, expected in [
        ("TC-44", "Page load populates billing-code-type selector", "Observed browser behavior required"),
        ("TC-45", "Empty code shows required-code message", "Observed browser behavior required"),
        ("TC-46", "Valid code renders KPI/payer/state content", "Observed browser behavior required"),
        ("TC-47", "Unknown code shows no-rates message", "Observed browser behavior required"),
        ("TC-48", "First state auto-selected", "Observed browser behavior required"),
        ("TC-49", "Selecting a state reloads county/payer-county and hides providers", "Observed browser behavior required"),
        ("TC-50", "Selecting payer-county reveals provider section", "Observed browser behavior required"),
        ("TC-51", "API server stopped produces error message", "Observed browser behavior required"),
    ]:
        results.append(TestResult(case, "Dashboard", desc, expected, "MANUAL",
                                   "Browser observation is required; this Cigna repo does not contain the thesis's benchmark_dashboard.html/API runtime. " + " | ".join(static_notes), 0.0,
                                   "Use the generated dashboard_manual_checklist.md to record the observation."))


def read_manifest(path: Path) -> tuple[int, int]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        blobs = obj.get("blobs") or obj.get("files") or []
        sizes = [int(x.get("size_bytes")) for x in blobs if isinstance(x, dict) and x.get("size_bytes") is not None]
        return len(blobs), sum(sizes)
    except Exception:
        return 0, 0


def find_file(repo: Path, names: list[str]) -> Optional[Path]:
    for name in names:
        p = repo / name
        if p.exists():
            return p
    return None


def production_snapshot(repo: Path, out: Path) -> dict[str, Any]:
    import duckdb

    manifest = find_file(repo, ["cigna_in_network_rates_by_size.json", "cigna_in_network_rates.json", "cigna_file_sizes.json"])
    db = find_file(repo, ["transparency.duckdb", "cigna.duckdb"])
    enrichment = find_file(repo, ["enrichment.duckdb"])
    progress = find_file(repo, ["cigna_sorted_into_transparency_processed_count.txt", "cigna_sorted_processed_count.txt", "processed_count.txt"])
    run_log = find_file(repo, ["cigna_sorted_into_transparency_runs.jsonl", "cigna_sorted_runs.jsonl", "ingestion_runs.jsonl"])

    snap: dict[str, Any] = {"timestamp_utc": now_utc(), "repository": str(repo)}
    if manifest:
        nfiles, total_bytes = read_manifest(manifest)
        snap["manifest"] = {"path": str(manifest.name), "source_files": nfiles, "compressed_bytes_listed": total_bytes}
    else:
        snap["manifest"] = None

    if progress:
        try:
            snap["processed_count"] = int(progress.read_text(encoding="utf-8").strip() or "0")
        except Exception:
            snap["processed_count"] = None
    else:
        snap["processed_count"] = None

    snap["transparency_db"] = None
    if db and db.exists():
        rec: dict[str, Any] = {"path": str(db), "bytes": db.stat().st_size}
        try:
            con = duckdb.connect(str(db), read_only=True)
            try:
                tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
                rec["tables"] = sorted(tables)
                for t in ["payers", "billing_codes", "negotiated_rates", "providers", "benchmarks"]:
                    if t in tables:
                        rec[t + "_rows"] = int(con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0])
            finally:
                con.close()
            snap["transparency_db"] = rec
        except Exception as e:
            rec["read_error"] = f"{type(e).__name__}: {e}"
            snap["transparency_db"] = rec

    snap["enrichment_db"] = None
    if enrichment and enrichment.exists():
        rec = {"path": str(enrichment), "bytes": enrichment.stat().st_size}
        try:
            con = duckdb.connect(str(enrichment), read_only=True)
            try:
                tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
                rec["tables"] = sorted(tables)
                for t in ["nppes", "zip_county", "code_descriptions"]:
                    if t in tables:
                        rec[t + "_rows"] = int(con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0])
            finally:
                con.close()
            snap["enrichment_db"] = rec
        except Exception as e:
            rec["read_error"] = f"{type(e).__name__}: {e}"
            snap["enrichment_db"] = rec

    if run_log and run_log.exists():
        events = []
        for line in run_log.read_text(encoding="utf-8", errors="replace").splitlines():
            if not line.strip():
                continue
            try:
                o = json.loads(line)
                events.append(o)
            except Exception:
                continue
        completes = [e for e in events if e.get("status") == "complete" and e.get("timestamp")]
        failed = [e for e in events if e.get("status") == "failed"]
        times = []
        for e in completes:
            try:
                times.append(datetime.fromisoformat(e["timestamp"]))
            except Exception:
                pass
        rec = {"path": str(run_log), "events": len(events), "complete_events": len(completes), "failed_events": len(failed)}
        if times:
            rec["first_complete_utc"] = min(times).isoformat()
            rec["last_complete_utc"] = max(times).isoformat()
            rec["observed_complete_event_span_seconds"] = (max(times) - min(times)).total_seconds()
        snap["run_log"] = rec
    else:
        snap["run_log"] = None

    return snap


def write_dashboard_checklist(out: Path):
    text = f"""# Dashboard manual evidence checklist\n\nGenerated: {now_utc()}\n\nUse the thesis's benchmark API/dashboard environment. Do not mark a case PASS until you have observed it in the browser.\n\n| Case | Action | Actual observation | Status |\n|---|---|---|---|\n| TC-44 | Open dashboard and inspect billing-code-type selector |  |  |\n| TC-45 | Leave billing code empty and trigger Compare |  |  |\n| TC-46 | Enter a valid code/type and trigger Compare |  |  |\n| TC-47 | Enter unknown code 00000 / CPT and trigger Compare |  |  |\n| TC-48 | After a valid comparison, inspect first state selection |  |  |\n| TC-49 | Click a different state row |  |  |\n| TC-50 | Click a payer-county row |  |  |\n| TC-51 | Stop the API server, then perform a search |  |  |\n\nEvidence rule: paste the observed message/visible result in the thesis, rather than assuming the expected string occurred.\n"""
    (out / "dashboard_manual_checklist.md").write_text(text, encoding="utf-8")


def write_reports(results: list[TestResult], out: Path, snapshot: dict[str, Any], repo: Path, metadata: dict[str, Any]):
    out.mkdir(parents=True, exist_ok=True)
    data = {
        "runner_version": SCRIPT_VERSION,
        "generated_utc": now_utc(),
        "repository": str(repo),
        "environment": metadata,
        "results": [asdict(r) for r in results],
        "production_snapshot": snapshot,
        "summary": {
            "PASS": sum(r.status == "PASS" for r in results),
            "FAIL": sum(r.status == "FAIL" for r in results),
            "MANUAL": sum(r.status == "MANUAL" for r in results),
            "NOT RUN": sum(r.status == "NOT RUN" for r in results),
            "BLOCKED": sum(r.status == "BLOCKED" for r in results),
            "TOTAL": len(results),
        },
    }
    (out / "thesis_test_results.json").write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")

    with (out / "thesis_test_results.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(asdict(results[0]).keys()) if results else list(TestResult.__annotations__.keys()))
        w.writeheader()
        for r in results:
            w.writerow(asdict(r))

    md = []
    md.append("# TiC / Cigna Thesis Test Evidence")
    md.append("")
    md.append(f"Generated: `{data['generated_utc']}`")
    md.append(f"Repository: `{repo}`")
    md.append(f"Runner version: `{SCRIPT_VERSION}`")
    md.append("")
    md.append("## Summary")
    md.append("")
    s = data["summary"]
    md.append(f"PASS: **{s['PASS']}** | FAIL: **{s['FAIL']}** | MANUAL: **{s['MANUAL']}** | NOT RUN: **{s['NOT RUN']}** | BLOCKED: **{s['BLOCKED']}** | TOTAL: **{s['TOTAL']}**")
    md.append("")
    md.append("## Test results")
    md.append("")
    md.append("| Case | Area | Status | Actual result | Notes |")
    md.append("|---|---|---|---|---|")
    for r in results:
        actual = r.actual.replace("|", "\\|").replace("\n", " ")
        notes = r.notes.replace("|", "\\|").replace("\n", " ")
        md.append(f"| {r.case} | {r.area} | **{r.status}** | {actual} | {notes} |")
    md.append("")
    md.append("## Production snapshot")
    md.append("")
    md.append("The following is read-only evidence observed on the machine running this script. It is not a substitute for an end-to-end ingestion-duration measurement if no timing record exists.")
    md.append("")
    md.append("```json")
    md.append(json.dumps(snapshot, indent=2, default=str))
    md.append("```")
    md.append("")
    md.append("## Thesis Table 4.8 mapping")
    md.append("")
    manifest = snapshot.get("manifest") or {}
    db = snapshot.get("transparency_db") or {}
    enrichment = snapshot.get("enrichment_db") or {}
    run = snapshot.get("run_log") or {}
    md.append("| Metric | Evidence produced by runner |")
    md.append("|---|---|")
    md.append(f"| Input data size (compressed) | {manifest.get('compressed_bytes_listed', 'NOT AVAILABLE')} bytes from Cigna manifest |")
    md.append(f"| Number of source files processed | {snapshot.get('processed_count', 'NOT AVAILABLE')} processed-count snapshot; manifest contains {manifest.get('source_files', 'NOT AVAILABLE')} files |")
    md.append(f"| Records processed (rate rows stored) | {db.get('negotiated_rates_rows', 'NOT AVAILABLE')} rows in transparency DB |")
    md.append("| Ingestion duration | NOT inferred from source-code constants; see run-log observed event span below |")
    md.append("| Peak memory during ingestion | NOT measured unless a dedicated production ingestion measurement is run |")
    md.append(f"| Transparency database size | {db.get('bytes', 'NOT AVAILABLE')} bytes |")
    md.append("| NPPES processing duration | NOT recorded by the repository's NPPES loader; do not invent |")
    md.append("| Enrichment duration | NOT recorded by the repository loaders; do not invent |")
    md.append("| Benchmark build duration | NOT inferred from source-code constants; run benchmark builder under measurement if needed |")
    md.append(f"| Benchmark database size | {db.get('bytes', 'NOT AVAILABLE')} bytes (benchmarks reside in transparency.duckdb in this repo) |")
    md.append("| API response time | Populated only when `--api-url` is supplied and reachable |")
    md.append("| Dashboard load time | Manual browser measurement required |")
    if run.get("observed_complete_event_span_seconds") is not None:
        md.append("")
        md.append(f"Observed complete-event timestamp span in run log: **{run['observed_complete_event_span_seconds']:.1f} s** (this is not automatically labeled as full ingestion duration).")
    md.append("")
    md.append("## Interpretation rules")
    md.append("")
    md.append("PASS means the repository behavior was actually demonstrated by this run. FAIL means the assertion did not match the current repository behavior. MANUAL means browser observation is intentionally required. NOT RUN means the required external service was not supplied. No PASS is manufactured for an unexecuted case.")
    (out / "thesis_test_results.md").write_text("\n".join(md), encoding="utf-8")
    write_dashboard_checklist(out)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repo", default=".", help="Cigna TiC repository directory (default: current directory)")
    ap.add_argument("--out", default="thesis_test_evidence_cigna", help="Evidence output directory")
    ap.add_argument("--api-url", default=None, help="Optional benchmark API base URL, e.g. http://127.0.0.1:5544")
    ap.add_argument("--api-code", default="99213")
    ap.add_argument("--api-type", default="CPT")
    ap.add_argument("--api-state", default="TX")
    ap.add_argument("--api-payer", default="")
    ap.add_argument("--api-county", default="48201")
    args = ap.parse_args()

    repo = discover_repo(Path(args.repo))
    out = (repo / args.out).resolve() if not Path(args.out).is_absolute() else Path(args.out).resolve()
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)

    metadata = {
        "python": platform.python_version(),
        "os": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "hostname": socket.gethostname(),
    }
    try:
        metadata["node"] = subprocess.run(["node", "--version"], capture_output=True, text=True, timeout=5).stdout.strip()
    except Exception:
        metadata["node"] = "not available"

    results: list[TestResult] = []
    root = Path(tempfile.mkdtemp(prefix="tic_thesis_", dir=str(out)))
    fixtures_dir = root / "fixtures"
    fixtures_dir.mkdir(parents=True, exist_ok=True)
    fixtures = make_fixture_files(fixtures_dir)

    try:
        stream_parser, ingest_utils = import_repo(repo)
        try:
            import duckdb
            metadata["duckdb"] = duckdb.__version__
        except Exception as e:
            metadata["duckdb"] = f"import error: {e}"
            raise
        try:
            import ijson
            metadata["ijson"] = getattr(ijson, "__version__", "unknown")
        except Exception as e:
            metadata["ijson"] = f"import error: {e}"
            raise
        try:
            import requests
            metadata["requests"] = getattr(requests, "__version__", "unknown")
        except Exception as e:
            metadata["requests"] = f"import error: {e}"
            raise

        print("TiC / Cigna thesis evidence runner")
        print(f"Repository : {repo}")
        print(f"Output     : {out}")
        print(f"Python     : {metadata['python']}")
        print(f"DuckDB     : {metadata['duckdb']}")
        print()

        print("[1/5] Ingestion tests...")
        test_ingestion(results, root, fixtures, repo)
        print("[2/5] Acquisition/runner tests...")
        test_acquisition(results, root, fixtures, repo)
        print("[3/5] Benchmark tests...")
        test_benchmarks(results, root, repo)
        print("[4/5] Validation tests...")
        test_validation(results, root, repo)
        print("[5/5] API/dashboard cases...")
        test_api(results, args.api_url, args.api_code, args.api_type, args.api_state, args.api_payer, args.api_county)
        dashboard_cases(results, repo)

        snapshot = production_snapshot(repo, out)
        write_reports(results, out, snapshot, repo, metadata)

        summary = {
            "PASS": sum(r.status == "PASS" for r in results),
            "FAIL": sum(r.status == "FAIL" for r in results),
            "MANUAL": sum(r.status == "MANUAL" for r in results),
            "NOT RUN": sum(r.status == "NOT RUN" for r in results),
            "TOTAL": len(results),
        }
        print()
        print("=" * 78)
        print("RESULT SUMMARY")
        print("=" * 78)
        print(" | ".join(f"{k}={v}" for k, v in summary.items()))
        print()
        for r in results:
            print(f"{r.case:<6} {r.status:<8} {r.actual[:180]}")
        print()
        print(f"Evidence written to: {out}")
        print(f"  - {out / 'thesis_test_results.md'}")
        print(f"  - {out / 'thesis_test_results.json'}")
        print(f"  - {out / 'thesis_test_results.csv'}")
        print(f"  - {out / 'dashboard_manual_checklist.md'}")
        print()
        print("NOTE: Existing production artifacts were only read. Functional tests use isolated temporary fixtures.")
        print("NOTE: Do not copy production Cigna data outside the work machine. Bring back only the text/CSV/JSON report if policy permits.")
        return 0 if summary["FAIL"] == 0 else 1

    except Exception as e:
        # Always leave an evidence artifact explaining the block.
        results.append(TestResult("RUNNER", "Runner", "Initialize and execute repository-specific suite",
                                  "script completes", "BLOCKED", f"{type(e).__name__}: {e}", 0.0,
                                  traceback.format_exc(limit=8)))
        snapshot = production_snapshot(repo, out)
        write_reports(results, out, snapshot, repo, metadata)
        print(f"\nBLOCKED: {type(e).__name__}: {e}")
        print(f"See: {out / 'thesis_test_results.md'}")
        return 2
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
