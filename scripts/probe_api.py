#!/usr/bin/env python3
"""
Probe the assessment API and print a report.

Standard library only - no pip install required.

Usage:
    export ASSESSMENT_API_BASE_URL="https://<project>.supabase.co/rest/v1"
    export ASSESSMENT_API_KEY="<key>"
    export ASSESSMENT_AUTH_TOKEN="$ASSESSMENT_API_KEY"
    python3 probe_api.py

Or place the three values in a .env file beside this script and just run it.

The script only reads. It never writes to the API and never prints your
credentials.
"""

import json
import os
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

TIMEOUT = 30


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

def load_dotenv() -> None:
    """Load KEY=VALUE lines from a .env file beside this script, if present."""
    path = Path(__file__).with_name(".env")
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def get_config() -> tuple[str, str, str]:
    load_dotenv()
    base = os.environ.get("ASSESSMENT_API_BASE_URL", "").rstrip("/")
    key = os.environ.get("ASSESSMENT_API_KEY", "")
    token = os.environ.get("ASSESSMENT_AUTH_TOKEN", "") or key

    missing = [
        name
        for name, val in (
            ("ASSESSMENT_API_BASE_URL", base),
            ("ASSESSMENT_API_KEY", key),
        )
        if not val
    ]
    if missing:
        sys.exit(
            "Missing environment variables: "
            + ", ".join(missing)
            + "\nSet them, or create a .env file beside this script."
        )
    return base, key, token


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------

def request(base, key, token, path, params=None, prefer=None):
    """GET a path. Returns (status, headers dict, parsed body or raw text)."""
    url = f"{base}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params, safe=".*")

    req = urllib.request.Request(url, method="GET")
    req.add_header("apikey", key)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/json")
    if prefer:
        req.add_header("Prefer", prefer)

    ctx = ssl.create_default_context()
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT, context=ctx) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            headers = {k.lower(): v for k, v in resp.headers.items()}
            try:
                return resp.status, headers, json.loads(raw)
            except json.JSONDecodeError:
                return resp.status, headers, raw
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        headers = {k.lower(): v for k, v in e.headers.items()} if e.headers else {}
        return e.code, headers, raw
    except Exception as e:  # noqa: BLE001 - report anything that goes wrong
        return None, {}, f"{type(e).__name__}: {e}"


def rule(title):
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


# --------------------------------------------------------------------------
# Probes
# --------------------------------------------------------------------------

def main() -> None:
    base, key, token = get_config()

    host = urllib.parse.urlparse(base).netloc
    print(f"Base URL host : {host}")
    print(f"API key       : {'set (' + str(len(key)) + ' chars)' if key else 'MISSING'}")
    print(f"Auth token    : {'same as key' if token == key else 'set, differs from key'}")

    # 1 -------------------------------------------------------------------
    rule("1. CONNECTIVITY AND AUTH")
    status, headers, body = request(base, key, token, "/transactions", {"limit": 1})
    print(f"HTTP status: {status}")
    if status != 200:
        print("Response body:")
        print(body)
        print("\nStopping - fix auth or URL before continuing.")
        return
    print("Auth OK.")

    # 2 -------------------------------------------------------------------
    rule("2. COLUMNS RETURNED BY THE API  (critical for watermark design)")
    if isinstance(body, list) and body:
        cols = list(body[0].keys())
        print(f"column count: {len(cols)}")
        for c in cols:
            print(f"  - {c}")

        documented = {
            "transaction_id", "account_id", "transaction_date", "amount",
            "currency", "transaction_type", "merchant_name",
            "merchant_category", "status", "country_code",
        }
        extra = [c for c in cols if c not in documented]
        print(f"\nUndocumented extra columns: {extra if extra else 'none'}")

        ingestion_like = [
            c for c in cols
            if any(t in c.lower() for t in
                   ("created", "inserted", "updated", "loaded", "ingest", "modified"))
        ]
        if ingestion_like:
            print(f"*** INGESTION-TIME COLUMN FOUND: {ingestion_like}")
            print("*** If present, this is the correct watermark column.")
        else:
            print("No ingestion-time column - watermark must use transaction_date")
            print("plus a lookback window.")

        print("\nSample record:")
        print(json.dumps(body[0], indent=2)[:900])
    else:
        print("Unexpected body shape:")
        print(str(body)[:500])

    # 3 -------------------------------------------------------------------
    rule("3. EXACT ROW COUNT")
    status, headers, body = request(
        base, key, token, "/transactions", {"limit": 1}, prefer="count=exact"
    )
    cr = headers.get("content-range")
    print(f"Content-Range header: {cr}")
    if cr and "/" in cr:
        total = cr.split("/")[-1]
        print(f"*** TOTAL RECORDS AT SOURCE: {total}")
        print("    (backup CSV contains 352)")
    else:
        print("No Content-Range returned - count must be derived by paging.")

    # 4 -------------------------------------------------------------------
    rule("4. DEFAULT PAGE SIZE AND SERVER-SIDE CAP")
    status, _, body = request(base, key, token, "/transactions")
    n_default = len(body) if isinstance(body, list) else "n/a"
    print(f"no limit param        -> {n_default} records")

    for requested in (100, 1000, 2000):
        status, _, body = request(base, key, token, "/transactions", {"limit": requested})
        got = len(body) if isinstance(body, list) else "n/a"
        note = ""
        if isinstance(got, int) and got < requested:
            note = "  <-- capped or dataset exhausted"
        print(f"limit={requested:<5}            -> {got} records{note}")

    # 5 -------------------------------------------------------------------
    rule("5. PAGINATION BEHAVIOUR (limit + offset + order)")
    p = {"limit": 5, "offset": 0, "order": "transaction_date.asc"}
    _, _, page1 = request(base, key, token, "/transactions", p)
    p = {"limit": 5, "offset": 5, "order": "transaction_date.asc"}
    _, _, page2 = request(base, key, token, "/transactions", p)

    if isinstance(page1, list) and isinstance(page2, list):
        ids1 = [r.get("transaction_id") for r in page1]
        ids2 = [r.get("transaction_id") for r in page2]
        print(f"page 1 (offset 0): {ids1}")
        print(f"page 2 (offset 5): {ids2}")
        overlap = set(ids1) & set(ids2)
        print(f"overlap between pages: {overlap if overlap else 'none - pagination is stable'}")

    # 6 -------------------------------------------------------------------
    rule("6. OFFSET BEYOND END  (loop termination condition)")
    _, _, body = request(base, key, token, "/transactions", {"limit": 5, "offset": 99999})
    if isinstance(body, list):
        print(f"offset=99999 -> {len(body)} records (empty list expected)")
    else:
        print(f"offset=99999 -> non-list response: {str(body)[:300]}")

    # 7 -------------------------------------------------------------------
    rule("7. DATE FILTER  (the watermark mechanism)")
    for flt in (
        "gte.2024-01-01T00:00:00Z",
        "gte.2024-03-01T00:00:00Z",
        "gte.2024-12-31T00:00:00Z",
    ):
        status, headers, body = request(
            base, key, token, "/transactions",
            {"transaction_date": flt, "limit": 1},
            prefer="count=exact",
        )
        cr = headers.get("content-range", "?")
        total = cr.split("/")[-1] if "/" in cr else "?"
        print(f"transaction_date={flt:32} -> {total} records (status {status})")

    # 8 -------------------------------------------------------------------
    rule("8. ACCOUNT FILTER")
    status, headers, _ = request(
        base, key, token, "/transactions",
        {"account_id": "eq.ACC-1001", "limit": 1},
        prefer="count=exact",
    )
    cr = headers.get("content-range", "?")
    print(f"account_id=eq.ACC-1001 -> {cr.split('/')[-1] if '/' in cr else '?'} records")

    # 9 -------------------------------------------------------------------
    rule("9. MAX AND MIN transaction_date AT SOURCE")
    _, _, body = request(
        base, key, token, "/transactions",
        {"order": "transaction_date.desc", "limit": 3},
    )
    if isinstance(body, list):
        print("latest 3 by transaction_date:")
        for r in body:
            print(f"  {r.get('transaction_id')}  {r.get('transaction_date')}  "
                  f"status={r.get('status')}")
        print("\n  NOTE: if the latest dates are the invalid records (April or")
        print("  November 2024), that confirms the watermark trap - the watermark")
        print("  must be computed from validated records only.")

    _, _, body = request(
        base, key, token, "/transactions",
        {"order": "transaction_date.asc", "limit": 3},
    )
    if isinstance(body, list):
        print("\nearliest 3 by transaction_date:")
        for r in body:
            print(f"  {r.get('transaction_id')}  {r.get('transaction_date')}")

    # 10 ------------------------------------------------------------------
    rule("10. ERROR BEHAVIOUR  (what the retry logic must handle)")
    status, _, body = request(base, key, token, "/transactions", {"limit": "abc"})
    print(f"limit=abc          -> HTTP {status}: {str(body)[:200]}")

    status, _, body = request(base, key, token, "/no_such_table", {"limit": 1})
    print(f"unknown table      -> HTTP {status}: {str(body)[:200]}")

    req_bad = urllib.request.Request(f"{base}/transactions?limit=1")
    try:
        with urllib.request.urlopen(req_bad, timeout=TIMEOUT) as r:
            print(f"no auth headers    -> HTTP {r.status}")
    except urllib.error.HTTPError as e:
        print(f"no auth headers    -> HTTP {e.code} (expected 401)")
    except Exception as e:  # noqa: BLE001
        print(f"no auth headers    -> {type(e).__name__}")

    rule("PROBE COMPLETE")
    print("Paste this entire output back into the chat.")
    print("No credentials were printed.")


if __name__ == "__main__":
    main()
