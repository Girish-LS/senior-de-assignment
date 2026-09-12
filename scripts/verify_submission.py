#!/usr/bin/env python3
"""Verify the submission against the assessment requirements.

    python scripts/verify_submission.py

Checks structure, README coverage, credential safety, test suite, and the
internal consistency of the committed outputs. Exits non-zero if anything
fails, so it can also run in CI.

This is the submission checklist expressed as code. Written because a
checklist that is read is a checklist that gets skipped under time pressure,
and because the most expensive failure here - a committed credential - is
silent until someone else finds it.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

GREEN, RED, YELLOW, DIM, RESET = (
    "\033[32m", "\033[31m", "\033[33m", "\033[2m", "\033[0m"
)
if sys.platform == "win32":
    # Enable ANSI on Windows terminals that need it; degrade quietly if not.
    try:
        import ctypes
        ctypes.windll.kernel32.SetConsoleMode(
            ctypes.windll.kernel32.GetStdHandle(-11), 7
        )
    except Exception:  # noqa: BLE001
        GREEN = RED = YELLOW = DIM = RESET = ""

results: list[tuple[str, bool, str]] = []


def check(name: str, passed: bool, detail: str = "") -> bool:
    """Record and print one check. `detail` explains a failure and is shown
    only when the check fails, so a passing run stays readable."""
    results.append((name, passed, detail))
    mark = f"{GREEN}PASS{RESET}" if passed else f"{RED}FAIL{RESET}"
    line = f"  [{mark}] {name}"
    if detail and not passed:
        line += f"\n         {DIM}{detail}{RESET}"
    print(line)
    return passed


def section(title: str) -> None:
    print(f"\n{title}")
    print("-" * 72)


# ---------------------------------------------------------------------------
# 1. Repository structure, per assessment section 6
# ---------------------------------------------------------------------------

section("1. Repository structure (assessment section 6)")

REQUIRED = [
    "README.md",
    "ingestion/ingest_transactions.py",
    "ingestion/incremental_ingest.py",
    "dbt_project/dbt_project.yml",
    "dbt_project/models/marts/daily_account_summary.sql",
    "dbt_project/models/marts/schema.yml",
    "sql/daily_account_summary.sql",
    "tests",
    "outputs/quarantine_sample.csv",
    "outputs/daily_account_summary_sample.csv",
    "outputs/watermark_run1.json",
    "outputs/watermark_run2.json",
    "docs/product_platform_note.md",
]

missing = [p for p in REQUIRED if not (ROOT / p).exists()]
check(
    f"all {len(REQUIRED)} required paths present",
    not missing,
    "missing: " + ", ".join(missing) if missing else "",
)

# ---------------------------------------------------------------------------
# 2. README checklist, per assessment section 7
# ---------------------------------------------------------------------------

section("2. README checklist (assessment section 7)")

readme = (ROOT / "README.md").read_text(encoding="utf-8")
headings = re.findall(r"^##\s+(.+)$", readme, re.M)
heading_blob = " | ".join(headings).lower()

CHECKLIST = {
    "setup and run instructions": ["quick start", "setup", "install"],
    "technology choice and rationale": ["technology", "rationale", "choices"],
    "validation and duplicate handling": ["validation", "duplicate"],
    "incremental / watermark / late-arriving": ["incremental", "watermark", "late"],
    "testing approach": ["testing", "tests"],
    "known limitations": ["limitation"],
    "AI tool usage disclosure": ["ai tool", "ai usage", "ai-assisted"],
}

for item, keywords in CHECKLIST.items():
    found = next((k for k in keywords if k in heading_blob), None)
    check(item, found is not None, "" if found else f"no heading matching {keywords}")

# ---------------------------------------------------------------------------
# 3. Credential safety - the check that matters most
# ---------------------------------------------------------------------------

section("3. Credential safety (assessment sections 2 and 9)")

gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8") if (
    ROOT / ".gitignore"
).exists() else ""

for pattern in [".env", "assignment.env", ".venv", "warehouse", "state"]:
    check(
        f".gitignore excludes {pattern}",
        pattern in gitignore,
        "" if pattern in gitignore else f"add '{pattern}' to .gitignore",
    )

# Scan every file git would track for anything resembling a credential.
SECRET_PATTERNS = [
    (r"sb_publishable_[A-Za-z0-9_\-]{10,}", "Supabase publishable key"),
    (r"sb_secret_[A-Za-z0-9_\-]{10,}", "Supabase secret key"),
    (r"eyJ[A-Za-z0-9_\-]{20,}\.[A-Za-z0-9_\-]{20,}", "JWT"),
    (r"ghp_[A-Za-z0-9]{20,}", "GitHub token"),
    (r"Authorization:\s*Bearer\s+[A-Za-z0-9_\-]{20,}", "hardcoded bearer token"),
]

IGNORE_DIRS = {".git", ".venv", "venv", "__pycache__", "warehouse", "state",
               ".pytest_cache", "node_modules", "target"}
IGNORE_FILES = {".env", "assignment.env", ".env.local", "assignment.env.txt"}

leaks: list[str] = []
scanned = 0
for path in ROOT.rglob("*"):
    if not path.is_file():
        continue
    if any(part in IGNORE_DIRS for part in path.parts):
        continue
    if path.name in IGNORE_FILES:
        continue
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:  # noqa: BLE001
        continue
    scanned += 1
    for pattern, label in SECRET_PATTERNS:
        if re.search(pattern, text):
            leaks.append(f"{path.relative_to(ROOT)} ({label})")

check(
    f"no credentials in {scanned} scanned files",
    not leaks,
    "FOUND: " + "; ".join(leaks) if leaks else "",
)

# If a repository exists, ask git directly rather than inferring.
if (ROOT / ".git").exists():
    try:
        tracked = subprocess.run(
            ["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, timeout=30
        ).stdout.split()
        bad = [f for f in tracked
               if Path(f).name in IGNORE_FILES or f.startswith((".venv/", "warehouse/", "state/"))]
        check(
            f"git tracks no credential or state files ({len(tracked)} tracked)",
            not bad,
            "TRACKED: " + ", ".join(bad) if bad else "",
        )
    except Exception as exc:  # noqa: BLE001
        check("git tracked-file check", False, f"could not run git: {exc}")
else:
    print(f"  {DIM}[skip] git not initialised yet{RESET}")

# ---------------------------------------------------------------------------
# 4. Test suite
# ---------------------------------------------------------------------------

section("4. Test suite")

proc = subprocess.run(
    [sys.executable, "-m", "unittest", "discover", "-s", "tests"],
    cwd=ROOT, capture_output=True, text=True, timeout=300,
)
tail = (proc.stderr or proc.stdout).strip().splitlines()
summary = next((ln for ln in reversed(tail) if ln.startswith("Ran ")), "no summary")
check(f"tests pass ({summary})", proc.returncode == 0,
      "" if proc.returncode == 0 else "\n".join(tail[-12:]))

# ---------------------------------------------------------------------------
# 5. Output consistency
# ---------------------------------------------------------------------------

section("5. Committed output consistency")

try:
    w1 = json.loads((ROOT / "outputs/watermark_run1.json").read_text(encoding="utf-8"))
    w2 = json.loads((ROOT / "outputs/watermark_run2.json").read_text(encoding="utf-8"))
    r1, r2 = w1["run"], w2["run"]

    check("run 1 is a genuine first run",
          r1["watermark_before"] is None,
          f"watermark_before was {r1['watermark_before']!r}, expected null")

    check("run 1 ingested the full dataset",
          r1["records_fetched"] == 352,
          f"fetched {r1['records_fetched']}, expected 352")

    check("run 1 found exactly 3 invalid records",
          r1["records_quarantined"] == 3,
          f"quarantined {r1['records_quarantined']}")

    check("run 1 found exactly 5 duplicates",
          r1["records_duplicate"] == 5,
          f"duplicates {r1['records_duplicate']}")

    # The watermark trap: two invalid records are dated April and November
    # 2024, later than any valid record. A watermark derived from raw rather
    # than validated data would jump there and every later run would return
    # nothing, silently and with a successful exit code.
    wm = r1["watermark_after"] or ""
    check("watermark derived from VALID records only",
          wm.startswith("2024-03-"),
          f"watermark is {wm!r}; a value in 2024-04 or 2024-11 means the "
          f"invalid records leaked into the watermark")

    check("run 2 applied a lookback window",
          r2["effective_filter_from"] is not None
          and r2["effective_filter_from"] < r2["watermark_before"],
          f"filter_from={r2['effective_filter_from']!r} "
          f"watermark_before={r2['watermark_before']!r}")

    check("run 2 did not advance the watermark (no new data)",
          r2["watermark_before"] == r2["watermark_after"],
          f"{r2['watermark_before']!r} -> {r2['watermark_after']!r}")

    check("run 2 inserted no duplicate rows",
          r2["records_duplicate"] == 0,
          f"duplicates {r2['records_duplicate']}")

    # Which source produced these outputs. The fixture is a different dataset
    # from the live API, so committed outputs should come from the API.
    source_note = "live API" if wm == "2024-03-30T21:01:36Z" else (
        "CSV fixture" if wm == "2024-03-30T22:35:29Z" else "unknown")
    print(f"  {DIM}[info] outputs generated from: {source_note} "
          f"(watermark {wm}){RESET}")
    if source_note == "CSV fixture":
        print(f"  {YELLOW}       consider regenerating from the API so a reviewer "
              f"reproduces these figures{RESET}")

except Exception as exc:  # noqa: BLE001
    check("watermark outputs readable", False, str(exc))

# Quarantine: distinct records, not one row per run.
try:
    import csv
    q = list(csv.DictReader(
        (ROOT / "outputs/quarantine_sample.csv").open(newline="", encoding="utf-8")))
    ids = [r["transaction_id"] for r in q]
    check(f"quarantine sample holds distinct records ({len(ids)} rows)",
          len(ids) == len(set(ids)),
          "duplicate transaction_ids present" if len(ids) != len(set(ids)) else "")
    check("every quarantined record states a reason",
          all(r.get("error_reason", "").strip() for r in q))
except Exception as exc:  # noqa: BLE001
    check("quarantine sample readable", False, str(exc))

# ---------------------------------------------------------------------------

section("Summary")

failed = [n for n, ok, _ in results if not ok]
passed = len(results) - len(failed)
print(f"  {passed} passed, {len(failed)} failed, {len(results)} checks total")

if failed:
    print(f"\n{RED}  Not ready to submit. Failing checks:{RESET}")
    for n in failed:
        print(f"    - {n}")
    sys.exit(1)

print(f"\n{GREEN}  All checks passed.{RESET}")
sys.exit(0)
