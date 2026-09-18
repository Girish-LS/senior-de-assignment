"""Ingest from the REST API directly into Databricks Unity Catalog.

    python scripts/ingest_to_databricks.py            # incremental
    python scripts/ingest_to_databricks.py --mode full

Why this exists
---------------
The SQLite path proves the pipeline runs with no dependencies. This path
proves it runs on the target platform, end to end, with no manual step
between the API and the warehouse.

It reuses the pipeline's own components rather than reimplementing them -
`TransactionsApiClient`, `validate_record`, `natural_key_hash`,
`flag_duplicates`, `apply_lookback` and `max_valid_transaction_date` are
imported, not copied. Only the storage backend differs. That matters: two
copies of validation logic would drift, and the whole argument for a single
silver layer is that a rule should be stated once.

What differs from the SQLite backend
------------------------------------
1.  Delta tables in Unity Catalog rather than a local file.
2.  `MERGE INTO` rather than `INSERT ... ON CONFLICT`. Spark SQL has no
    upsert clause; MERGE is the equivalent and is what Delta is built for.
3.  The watermark is read from and written to Databricks, so the incremental
    behaviour belongs to this warehouse rather than being carried over from
    the local run.

Tables are created in the `bronze` schema, which the dbt source declares
explicitly. Bronze, silver and gold are the layer names the layers actually
have - see dbt_project/macros/get_custom_schema.sql for why that needed an
override.

Credentials come from the environment, never from a file:
    DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN
    DATABRICKS_CATALOG  (optional, default "workspace")
    DATABRICKS_BRONZE_SCHEMA (optional, default "bronze")
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ingestion.api_client import TransactionsApiClient  # noqa: E402
from ingestion.config import ConfigError, load_settings  # noqa: E402
from ingestion.dedupe import flag_duplicates, natural_key_hash  # noqa: E402
from ingestion.models import Transaction, validate_record  # noqa: E402
from ingestion.pipeline import (  # noqa: E402
    SOURCE_SYSTEM,
    apply_lookback,
    max_valid_transaction_date,
)

logger = logging.getLogger("ingest_to_databricks")

# Insert in batches. Large enough that 349 rows is a handful of statements,
# small enough that a single statement stays well inside any length limit.
INSERT_BATCH = 100


# --------------------------------------------------------------------------
# SQL literal rendering
#
# Values are rendered inline rather than bound as parameters. The reason is
# not convenience: MERGE INTO with a multi-row VALUES source and bound
# parameters behaves inconsistently across connector versions, and a silent
# type coercion in a money column is exactly the class of bug this pipeline
# exists to avoid. Inline literals with explicit quote escaping are verbose
# and predictable.
#
# The input is the pipeline's own validated output, not user input, so this is
# not an injection surface - but merchant names legitimately contain
# apostrophes ("Macy's"), so escaping is a correctness requirement.
# --------------------------------------------------------------------------


def sql_str(value: Any) -> str:
    """Render a value as a SQL string literal, or NULL."""
    if value is None:
        return "NULL"
    if isinstance(value, Decimal):
        # Bronze stores amount as text to avoid any float round-trip.
        return "'" + format(value.normalize(), "f") + "'"
    return "'" + str(value).replace("'", "''") + "'"


def sql_int(value: Any) -> str:
    """Render a value as a SQL integer literal, or NULL."""
    if value is None:
        return "NULL"
    return str(int(value))


# --------------------------------------------------------------------------
# Schema
#
# Every transaction field is STRING, matching the SQLite backend. Bronze is a
# faithful record of what the API sent; interpreting types is staging's job,
# done once and explicitly. Storing amount as STRING also keeps it out of
# floating point before anyone has decided on a precision.
# --------------------------------------------------------------------------

BRONZE_COLUMNS = [
    "transaction_id", "source_id", "account_id", "transaction_date", "amount",
    "currency", "transaction_type", "merchant_name", "merchant_category",
    "status", "country_code", "natural_key_hash", "is_duplicate",
    "duplicate_of", "ingestion_timestamp", "ingestion_run_id", "source_system",
]

BRONZE_DDL = """
CREATE TABLE IF NOT EXISTS {fq} (
    transaction_id      STRING  NOT NULL,
    source_id           INT,
    account_id          STRING  NOT NULL,
    transaction_date    STRING  NOT NULL,
    amount              STRING  NOT NULL,
    currency            STRING  NOT NULL,
    transaction_type    STRING  NOT NULL,
    merchant_name       STRING  NOT NULL,
    merchant_category   STRING  NOT NULL,
    status              STRING  NOT NULL,
    country_code        STRING  NOT NULL,
    natural_key_hash    STRING  NOT NULL,
    is_duplicate        INT     NOT NULL,
    duplicate_of        STRING,
    ingestion_timestamp STRING  NOT NULL,
    ingestion_run_id    STRING  NOT NULL,
    source_system       STRING  NOT NULL
) USING DELTA
COMMENT 'Bronze: every validated record as the API sent it, plus ingestion metadata. Duplicates present and flagged.'
"""

QUARANTINE_COLUMNS = [
    "quarantine_id", "transaction_id", "source_id", "raw_record",
    "error_reason", "error_count", "failed_fields", "ingestion_timestamp",
    "ingestion_run_id", "source_system",
]

QUARANTINE_DDL = """
CREATE TABLE IF NOT EXISTS {fq} (
    quarantine_id       STRING  NOT NULL,
    transaction_id      STRING,
    source_id           INT,
    raw_record          STRING  NOT NULL,
    error_reason        STRING  NOT NULL,
    error_count         INT     NOT NULL,
    failed_fields       STRING  NOT NULL,
    ingestion_timestamp STRING  NOT NULL,
    ingestion_run_id    STRING  NOT NULL,
    source_system       STRING  NOT NULL
) USING DELTA
COMMENT 'Quarantine: records that failed validation, with every reason. Append-only across runs.'
"""

WATERMARK_DDL = """
CREATE TABLE IF NOT EXISTS {fq} (
    source_system   STRING  NOT NULL,
    watermark_value STRING,
    run_id          STRING  NOT NULL,
    updated_at      STRING  NOT NULL
) USING DELTA
COMMENT 'High-water mark: max transaction_date among VALIDATED records. Advanced only on a fully successful run.'
"""

METRICS_DDL = """
CREATE TABLE IF NOT EXISTS {fq} (
    run_id                STRING  NOT NULL,
    run_mode              STRING  NOT NULL,
    run_status            STRING  NOT NULL,
    source_system         STRING  NOT NULL,
    watermark_before      STRING,
    watermark_after       STRING,
    effective_filter_from STRING,
    records_fetched       INT     NOT NULL,
    records_valid         INT     NOT NULL,
    records_quarantined   INT     NOT NULL,
    records_duplicate     INT     NOT NULL,
    http_requests         INT     NOT NULL,
    http_retries          INT     NOT NULL,
    duration_seconds      DOUBLE  NOT NULL,
    started_at            STRING  NOT NULL,
    completed_at          STRING  NOT NULL
) USING DELTA
COMMENT 'One row per ingestion run. Makes quarantine rate and watermark movement queryable as history, not just visible in logs.'
"""


# --------------------------------------------------------------------------
# Connection
# --------------------------------------------------------------------------


def databricks_config() -> dict[str, str]:
    """Read Databricks settings from the environment, failing fast."""
    cfg = {
        "host": os.environ.get("DATABRICKS_HOST", "").strip(),
        "http_path": os.environ.get("DATABRICKS_HTTP_PATH", "").strip(),
        "token": os.environ.get("DATABRICKS_TOKEN", "").strip(),
        "catalog": os.environ.get("DATABRICKS_CATALOG", "workspace").strip(),
        "schema": os.environ.get("DATABRICKS_BRONZE_SCHEMA", "bronze").strip(),
    }
    missing = [k for k in ("host", "http_path", "token") if not cfg[k]]
    if missing:
        raise ConfigError(
            "Missing Databricks settings: "
            + ", ".join("DATABRICKS_" + m.upper() for m in missing)
            + "\nSet them in this shell, for example:\n"
            '  $env:DATABRICKS_HOST = "dbc-xxxx.cloud.databricks.com"\n'
            '  $env:DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/<id>"\n'
            '  $env:DATABRICKS_TOKEN = "dapi..."'
        )
    # A host pasted with the scheme is a common mistake and the connector
    # rejects it unhelpfully.
    cfg["host"] = cfg["host"].removeprefix("https://").removeprefix("http://").rstrip("/")
    return cfg


def connect(cfg: dict[str, str]):
    try:
        from databricks import sql as dbsql
    except ImportError as exc:  # pragma: no cover
        raise ConfigError(
            "databricks-sql-connector is not installed.\n"
            "  pip install dbt-databricks   (installs the connector too)"
        ) from exc

    return dbsql.connect(
        server_hostname=cfg["host"],
        http_path=cfg["http_path"],
        access_token=cfg["token"],
    )


def execute(cursor, statement: str) -> None:
    cursor.execute(statement)


def query_one(cursor, statement: str):
    cursor.execute(statement)
    return cursor.fetchone()


# --------------------------------------------------------------------------
# Load
# --------------------------------------------------------------------------


def create_objects(cursor, catalog: str, schema: str) -> dict[str, str]:
    """Create the schema and the four Delta tables. Idempotent."""
    execute(cursor, f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

    fq = {
        "bronze": f"`{catalog}`.`{schema}`.`bronze_transactions`",
        "quarantine": f"`{catalog}`.`{schema}`.`quarantine_transactions`",
        "watermark": f"`{catalog}`.`{schema}`.`pipeline_watermark`",
        "metrics": f"`{catalog}`.`{schema}`.`ingestion_run_metrics`",
    }
    execute(cursor, BRONZE_DDL.format(fq=fq["bronze"]))
    execute(cursor, QUARANTINE_DDL.format(fq=fq["quarantine"]))
    execute(cursor, WATERMARK_DDL.format(fq=fq["watermark"]))
    execute(cursor, METRICS_DDL.format(fq=fq["metrics"]))
    return fq


def read_watermark(cursor, fq_watermark: str) -> str | None:
    row = query_one(
        cursor,
        f"SELECT watermark_value FROM {fq_watermark} "
        f"WHERE source_system = {sql_str(SOURCE_SYSTEM)} LIMIT 1",
    )
    return row[0] if row and row[0] else None


def insert_batches(cursor, fq_table: str, columns: list[str], rows: list[list[str]]) -> None:
    """Insert pre-rendered literal rows in batches."""
    if not rows:
        return
    col_list = ", ".join(f"`{c}`" for c in columns)
    for start in range(0, len(rows), INSERT_BATCH):
        chunk = rows[start : start + INSERT_BATCH]
        values = ",\n  ".join("(" + ", ".join(r) + ")" for r in chunk)
        execute(cursor, f"INSERT INTO {fq_table} ({col_list}) VALUES\n  {values}")


def upsert_bronze(cursor, fq_bronze: str, rows: list[list[str]]) -> None:
    """Upsert via a staging table and MERGE INTO.

    Spark SQL has no `INSERT ... ON CONFLICT`. MERGE is the Delta equivalent,
    and it is what makes a rerun safe: the watermark filter uses `gte`, so the
    boundary record is deliberately re-fetched on every incremental run and a
    blind INSERT would duplicate it each time.
    """
    if not rows:
        return
    staging = fq_bronze.rsplit(".", 1)[0] + ".`_bronze_staging`"

    execute(cursor, f"DROP TABLE IF EXISTS {staging}")
    # An empty table with an identical schema. CTAS with a false predicate is
    # cheaper than DEEP CLONE (which would copy every row only for it to be
    # deleted) and needs no clone privilege.
    execute(
        cursor,
        f"CREATE TABLE {staging} USING DELTA AS "
        f"SELECT * FROM {fq_bronze} WHERE 1 = 0",
    )
    insert_batches(cursor, staging, BRONZE_COLUMNS, rows)

    # UPDATE SET * / INSERT * require identical schemas, which the clone
    # guarantees.
    execute(
        cursor,
        f"""
        MERGE INTO {fq_bronze} AS target
        USING {staging} AS source
           ON target.transaction_id = source.transaction_id
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """,
    )
    execute(cursor, f"DROP TABLE IF EXISTS {staging}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Ingest from the API straight into Databricks Unity Catalog."
    )
    parser.add_argument(
        "--mode",
        choices=("full", "incremental"),
        default="incremental",
        help="full ignores the watermark; incremental applies it with the "
        "configured lookback. Default: incremental (which behaves as full "
        "when no watermark exists yet).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)sZ %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    # The Databricks connector logs every HTTP round trip at INFO, which
    # buries the pipeline's own output. Raise its threshold so the run summary
    # stays readable; warnings and errors still surface.
    for noisy in ("databricks", "databricks.sql", "thrift", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    try:
        settings = load_settings(require_api=True)
        dbx = databricks_config()
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2

    run_id = "run-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
    started = datetime.now(timezone.utc)
    started_iso = started.strftime("%Y-%m-%dT%H:%M:%SZ")
    t0 = time.monotonic()

    logger.info("run %s targeting %s.%s", run_id, dbx["catalog"], dbx["schema"])

    conn = connect(dbx)
    try:
        cursor = conn.cursor()
        fq = create_objects(cursor, dbx["catalog"], dbx["schema"])

        # ---- decide the filter -------------------------------------------
        watermark_before = read_watermark(cursor, fq["watermark"])
        if args.mode == "full" or not watermark_before:
            since = None
            effective_from = None
            logger.info("mode=full: reading all records, no date filter")
        else:
            effective_from = apply_lookback(watermark_before, settings.lookback_hours)
            # Pass the bare timestamp: iter_transactions applies the `gte.`
            # prefix itself. Prefixing here too produced `gte.gte.<ts>`, which
            # the API matched against nothing and returned zero rows - a run
            # that reported success while silently fetching nothing, which is
            # precisely the failure shape this pipeline exists to prevent.
            since = effective_from
            logger.info(
                "mode=incremental: watermark=%s lookback=%dh filter=gte.%s",
                watermark_before, settings.lookback_hours, effective_from,
            )

        # ---- fetch and validate, reusing the pipeline's own components ----
        valid: list[Transaction] = []
        invalid: list[tuple[dict[str, Any], list[str]]] = []
        fetched = 0

        # Constructed exactly as pipeline.py does, so retry, timeout and page
        # size behaviour is identical on both paths rather than diverging.
        client = TransactionsApiClient(
            base_url=settings.api_base_url,
            api_key=settings.api_key,
            auth_token=settings.auth_token,
            page_size=settings.page_size,
            max_retries=settings.max_retries,
            backoff_base_seconds=settings.backoff_base_seconds,
            timeout_seconds=settings.request_timeout_seconds,
        )
        for raw in client.iter_transactions(since=since):
            fetched += 1
            txn, reasons = validate_record(raw)
            if txn is None:
                invalid.append((raw, reasons))
            else:
                valid.append(txn)
        http_requests = client.stats.requests_made
        http_retries = client.stats.retries

        logger.info("fetched %d records", fetched)

        hashes, duplicate_of, duplicate_count = flag_duplicates(valid)
        ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        bronze_rows = [
            [
                sql_str(t.transaction_id),
                sql_int(None),
                sql_str(t.account_id),
                sql_str(t.transaction_date),
                sql_str(t.amount),
                sql_str(t.currency),
                sql_str(t.transaction_type),
                sql_str(t.merchant_name),
                sql_str(t.merchant_category),
                sql_str(t.status),
                sql_str(t.country_code),
                sql_str(hashes[t.transaction_id]),
                sql_int(1 if duplicate_of.get(t.transaction_id) else 0),
                sql_str(duplicate_of.get(t.transaction_id)),
                sql_str(ingested_at),
                sql_str(run_id),
                sql_str(SOURCE_SYSTEM),
            ]
            for t in valid
        ]

        quarantine_rows = [
            [
                sql_str(f"{run_id}-{i:05d}"),
                sql_str(raw.get("transaction_id")),
                sql_int(None),
                sql_str(json.dumps(raw, sort_keys=True, default=str)),
                sql_str("; ".join(reasons)),
                sql_int(len(reasons)),
                sql_str(",".join(sorted({r.split(":")[0] for r in reasons}))),
                sql_str(ingested_at),
                sql_str(run_id),
                sql_str(SOURCE_SYSTEM),
            ]
            for i, (raw, reasons) in enumerate(invalid)
        ]

        # ---- write -------------------------------------------------------
        # Quarantine is idempotent within a run: this run's own rows are
        # cleared first, so re-running a failed run does not double-count.
        # It stays append-only ACROSS runs, because "was bad yesterday, still
        # bad today" is an audit fact worth keeping.
        execute(
            cursor,
            f"DELETE FROM {fq['quarantine']} WHERE ingestion_run_id = {sql_str(run_id)}",
        )
        insert_batches(cursor, fq["quarantine"], QUARANTINE_COLUMNS, quarantine_rows)
        upsert_bronze(cursor, fq["bronze"], bronze_rows)

        # ---- advance the watermark, from VALID records only ---------------
        #
        # This is the decision that matters. Two invalid records in this
        # dataset are dated April and November 2024, later than every valid
        # record. A watermark taken over raw records lands on the November
        # date, and every subsequent run then asks the API for records newer
        # than that and receives nothing - silently, with a successful exit
        # code, forever.
        #
        # It is advanced only now, after every record has been written. Moving
        # it earlier means a mid-run failure skips unprocessed records
        # permanently.
        if args.mode == "incremental" and effective_from and fetched == 0:
            # The lookback window always contains at least the record that set
            # the watermark, so an empty result means the filter is wrong
            # rather than that there is no new data. Failing loudly here is the
            # difference between a caught bug and silent data loss.
            raise RuntimeError(
                f"incremental run fetched 0 records with filter gte.{effective_from}, "
                f"but the window should contain the watermark record "
                f"({watermark_before}). The filter is malformed - refusing to "
                f"report success."
            )

        new_watermark = max_valid_transaction_date(valid)
        watermark_after = watermark_before
        if new_watermark and (not watermark_before or new_watermark > watermark_before):
            watermark_after = new_watermark

        execute(
            cursor,
            f"DELETE FROM {fq['watermark']} WHERE source_system = {sql_str(SOURCE_SYSTEM)}",
        )
        execute(
            cursor,
            f"INSERT INTO {fq['watermark']} "
            f"(source_system, watermark_value, run_id, updated_at) VALUES "
            f"({sql_str(SOURCE_SYSTEM)}, {sql_str(watermark_after)}, "
            f"{sql_str(run_id)}, {sql_str(ingested_at)})",
        )

        duration = round(time.monotonic() - t0, 3)
        completed_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        execute(
            cursor,
            f"INSERT INTO {fq['metrics']} VALUES ("
            f"{sql_str(run_id)}, {sql_str(args.mode)}, {sql_str('success')}, "
            f"{sql_str(SOURCE_SYSTEM)}, {sql_str(watermark_before)}, "
            f"{sql_str(watermark_after)}, {sql_str(effective_from)}, "
            f"{sql_int(fetched)}, {sql_int(len(valid))}, "
            f"{sql_int(len(invalid))}, {sql_int(duplicate_count)}, "
            f"{sql_int(http_requests)}, {sql_int(http_retries)}, "
            f"{duration}, {sql_str(started_iso)}, {sql_str(completed_iso)})",
        )

        summary = {
            "run_id": run_id,
            "target": f"{dbx['catalog']}.{dbx['schema']}",
            "mode": args.mode,
            "watermark_before": watermark_before,
            "effective_filter_from": effective_from,
            "watermark_after": watermark_after,
            "duration_seconds": duration,
            "http_requests": http_requests,
            "http_retries": http_retries,
            "fetched": fetched,
            "valid": len(valid),
            "quarantined": len(invalid),
            "duplicate": duplicate_count,
        }
        logger.info("run %s succeeded: %s", run_id, json.dumps(summary))
        print(json.dumps(summary, indent=2))

        bronze_total = query_one(cursor, f"SELECT COUNT(*) FROM {fq['bronze']}")[0]
        print(f"\nbronze_transactions now holds {bronze_total} rows")
        print("Next:  cd dbt_project && dbt build --target databricks")
        return 0

    except Exception as exc:  # noqa: BLE001
        # The watermark is not advanced on failure, so a failed run is safe to
        # simply re-run.
        logger.error("run %s failed: %s", run_id, exc)
        print(f"\nFAILED: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
