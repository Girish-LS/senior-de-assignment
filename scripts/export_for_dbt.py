"""Export the SQLite warehouse tables to CSV so dbt can read them.

Why this exists
---------------
The ingestion pipeline writes SQLite, because sqlite3 ships with Python and
duckdb does not, and the pipeline is deliberately dependency-free.

dbt-duckdb cannot open a SQLite file directly without DuckDB's sqlite_scanner
extension, and extension downloads are blocked on corporate networks - the
proxy returns 403 for extensions.duckdb.org. Relying on it would mean the dbt
path works on an open network and fails silently everywhere else.

Exporting to CSV avoids the problem entirely. DuckDB reads CSV natively with
no extension, so the dbt project runs anywhere. The dbt sources point at these
files through `external_location` in models/staging/schema.yml.

The cost is an extra materialisation step and CSV's lack of typing. Both are
acceptable at this volume, and both are stated in the README rather than left
for a reviewer to discover. At production scale this would be Parquet, which
preserves types and predicate-pushdown; CSV is chosen here only because it is
inspectable by hand, which matters more for an assessment than speed does.

Usage:
    python scripts/export_for_dbt.py
"""

from __future__ import annotations

import csv
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ingestion.config import ConfigError, load_settings  # noqa: E402

logger = logging.getLogger("export_for_dbt")

# Tables dbt treats as sources. The pipeline owns them; dbt only reads them.
SOURCE_TABLES = ("bronze_transactions", "quarantine_transactions")


def export_table(conn: sqlite3.Connection, table: str, destination: Path) -> int:
    cursor = conn.execute(f"SELECT * FROM {table}")  # noqa: S608 - fixed names
    columns = [d[0] for d in cursor.description]

    destination.parent.mkdir(parents=True, exist_ok=True)
    # newline="" is required on Windows or csv writes blank lines between rows.
    with destination.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        rows = 0
        for row in cursor:
            writer.writerow(row)
            rows += 1
    return rows


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)sZ %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    try:
        # Reads the warehouse only; no API credentials needed.
        settings = load_settings(require_api=False)
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2

    warehouse = settings.warehouse_path
    if not warehouse.exists():
        print(
            f"warehouse not found at {warehouse}\n"
            "Run the ingestion pipeline first:\n"
            "  python -m ingestion.ingest_transactions",
            file=sys.stderr,
        )
        return 1

    export_dir = warehouse.parent / "export"

    conn = sqlite3.connect(warehouse)
    try:
        for table in SOURCE_TABLES:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            if not exists:
                print(f"table {table} is absent; run the pipeline first",
                      file=sys.stderr)
                return 1

            path = export_dir / f"{table}.csv"
            rows = export_table(conn, table, path)
            logger.info("%-28s -> %-42s %6d rows", table, path, rows)
    finally:
        conn.close()

    print(f"\nExported to {export_dir}")
    print("dbt sources read these files. Next:")
    print("  cd dbt_project && dbt build --target duckdb")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
