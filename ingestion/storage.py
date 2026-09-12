"""Storage layer: bronze, quarantine, watermark and run metrics.

SQLite rather than DuckDB
-------------------------
DuckDB was the intended engine and is the better analytical choice. It cannot
be installed in the target environment, which blocks the public package index.
sqlite3 ships with Python, so the project runs with no install step at all.

At 352 rows the difference is immaterial. The SQL used here is deliberately
portable - no engine-specific functions - so moving to DuckDB, Postgres or
Databricks is a connection change rather than a rewrite. The one place this
costs something is the summary's `top_category`, where a window function
would be cleaner; SQLite has supported window functions since 3.25 (2018) so
it is in fact available, and the model uses it.

Layering
--------
bronze      every record the API returned, unmodified, plus ingestion
            metadata. Append-and-upsert only, never edited by transformation.
quarantine  records that failed validation, with every reason and the
            ingestion timestamp. Sits alongside bronze, not inside silver.
watermark   run history, not a single mutable value, so the incremental
            state is auditable.
run_metrics per-run counters, persisted so that quarantine rate and duplicate
            rate can be trended rather than only logged.

Idempotency
-----------
Bronze loads are upserts keyed on transaction_id, not blind inserts. This is
what makes a re-run safe and is forced by the watermark design: using `gte`
deliberately re-reads the boundary, so an insert would duplicate those rows
on every run.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from collections.abc import Iterable, Sequence
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
-- ---------------------------------------------------------------------
-- BRONZE: faithful record of what the source returned.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze_transactions (
    transaction_id      TEXT PRIMARY KEY,
    source_id           INTEGER,
    account_id          TEXT NOT NULL,
    transaction_date    TEXT NOT NULL,
    amount              TEXT NOT NULL,   -- Decimal as text; never float
    currency            TEXT NOT NULL,
    transaction_type    TEXT NOT NULL,
    merchant_name       TEXT NOT NULL,
    merchant_category   TEXT NOT NULL,
    status              TEXT NOT NULL,
    country_code        TEXT NOT NULL,
    natural_key_hash    TEXT NOT NULL,
    is_duplicate        INTEGER NOT NULL DEFAULT 0,
    duplicate_of        TEXT,
    ingestion_timestamp TEXT NOT NULL,
    ingestion_run_id    TEXT NOT NULL,
    source_system       TEXT NOT NULL DEFAULT 'transactions_api'
);

CREATE INDEX IF NOT EXISTS idx_bronze_date
    ON bronze_transactions (transaction_date);
CREATE INDEX IF NOT EXISTS idx_bronze_natural_key
    ON bronze_transactions (natural_key_hash);
CREATE INDEX IF NOT EXISTS idx_bronze_account_date
    ON bronze_transactions (account_id, transaction_date);

-- ---------------------------------------------------------------------
-- QUARANTINE: records that failed validation, with every reason.
-- Keyed on (transaction_id, run) so a re-run does not lose the history of
-- a record that was bad yesterday and is still bad today.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS quarantine_transactions (
    quarantine_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id      TEXT,
    source_id           INTEGER,
    raw_record          TEXT NOT NULL,   -- verbatim JSON as received
    error_reason        TEXT NOT NULL,   -- all violations, semicolon separated
    error_count         INTEGER NOT NULL,
    failed_fields       TEXT NOT NULL,   -- comma separated field names
    ingestion_timestamp TEXT NOT NULL,
    ingestion_run_id    TEXT NOT NULL,
    source_system       TEXT NOT NULL DEFAULT 'transactions_api'
);

CREATE INDEX IF NOT EXISTS idx_quarantine_run
    ON quarantine_transactions (ingestion_run_id);

CREATE INDEX IF NOT EXISTS idx_quarantine_txn
    ON quarantine_transactions (transaction_id);

-- One row per defective record rather than one per (record, run).
--
-- The underlying table is append-only on purpose: knowing that a record was
-- bad yesterday and is still bad today is an audit fact worth keeping. But
-- it means a record the source never fixes is re-quarantined on every run,
-- so the raw table is the wrong thing to hand a human.
--
-- Two records in this dataset make that concrete. TXN-0345 is dated November
-- 2024 and TXN-0346 April 2024, both later than any valid record, so both sit
-- permanently inside the incremental window and return on every run forever.
--
-- occurrence_count turns that from noise into signal: a count of 1 is a
-- transient defect, a climbing count is a defect the source has not fixed and
-- is worth raising upstream.
CREATE VIEW IF NOT EXISTS quarantine_current AS
SELECT
    transaction_id,
    COUNT(*)                    AS occurrence_count,
    MIN(ingestion_timestamp)    AS first_seen_at,
    MAX(ingestion_timestamp)    AS last_seen_at,
    COUNT(DISTINCT ingestion_run_id) AS runs_seen_in,
    -- Latest reported state, by the most recent ingestion of this record.
    (SELECT q2.error_count   FROM quarantine_transactions q2
      WHERE q2.transaction_id = q1.transaction_id
      ORDER BY q2.ingestion_timestamp DESC, q2.quarantine_id DESC
      LIMIT 1)                  AS error_count,
    (SELECT q2.failed_fields FROM quarantine_transactions q2
      WHERE q2.transaction_id = q1.transaction_id
      ORDER BY q2.ingestion_timestamp DESC, q2.quarantine_id DESC
      LIMIT 1)                  AS failed_fields,
    (SELECT q2.error_reason  FROM quarantine_transactions q2
      WHERE q2.transaction_id = q1.transaction_id
      ORDER BY q2.ingestion_timestamp DESC, q2.quarantine_id DESC
      LIMIT 1)                  AS error_reason,
    (SELECT q2.raw_record    FROM quarantine_transactions q2
      WHERE q2.transaction_id = q1.transaction_id
      ORDER BY q2.ingestion_timestamp DESC, q2.quarantine_id DESC
      LIMIT 1)                  AS raw_record
FROM quarantine_transactions q1
GROUP BY transaction_id;

-- ---------------------------------------------------------------------
-- WATERMARK: run history. A single mutable value would give no audit trail
-- and no way to answer "when did this last advance, and by how much".
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_watermark (
    watermark_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    source_system           TEXT NOT NULL,
    run_id                  TEXT NOT NULL,
    run_started_at          TEXT NOT NULL,
    run_completed_at        TEXT,
    run_status              TEXT NOT NULL,   -- running | success | failed
    watermark_before        TEXT,
    watermark_after         TEXT,
    effective_filter_from   TEXT,            -- watermark minus lookback
    records_fetched         INTEGER NOT NULL DEFAULT 0,
    records_valid           INTEGER NOT NULL DEFAULT 0,
    records_quarantined     INTEGER NOT NULL DEFAULT 0,
    records_duplicate       INTEGER NOT NULL DEFAULT 0,
    error_message           TEXT
);

CREATE INDEX IF NOT EXISTS idx_watermark_source
    ON pipeline_watermark (source_system, run_started_at);

-- ---------------------------------------------------------------------
-- RUN METRICS: counters persisted as data so quality can be trended.
-- A quarantine rate is only meaningful against its own history.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ingestion_run_metrics (
    run_id              TEXT PRIMARY KEY,
    source_system       TEXT NOT NULL,
    run_started_at      TEXT NOT NULL,
    run_completed_at    TEXT,
    run_status          TEXT NOT NULL,
    run_mode            TEXT NOT NULL,   -- full | incremental
    records_fetched     INTEGER NOT NULL DEFAULT 0,
    records_valid       INTEGER NOT NULL DEFAULT 0,
    records_quarantined INTEGER NOT NULL DEFAULT 0,
    records_duplicate   INTEGER NOT NULL DEFAULT 0,
    quarantine_rate     REAL,
    duplicate_rate      REAL,
    http_requests       INTEGER NOT NULL DEFAULT 0,
    http_retries        INTEGER NOT NULL DEFAULT 0,
    duration_seconds    REAL,
    error_message       TEXT
);
"""


def utc_now_iso() -> str:
    """Current UTC time in the same format the contract uses."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def connect(db_path: Path) -> sqlite3.Connection:
    """Open the warehouse, creating the schema if absent."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    # Enforce declared foreign keys and get better concurrency behaviour.
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    return conn


# --------------------------------------------------------------------------
# Bronze
# --------------------------------------------------------------------------


def upsert_bronze(
    conn: sqlite3.Connection, rows: Sequence[dict[str, Any]]
) -> int:
    """Upsert validated records into bronze.

    ON CONFLICT rather than INSERT so that re-running a load - which the
    watermark's gte overlap guarantees will happen - does not duplicate rows.
    The conflict target is transaction_id: the application-level identifier,
    which is distinct from business-level identity handled by deduplication.
    """
    if not rows:
        return 0
    sql = """
        INSERT INTO bronze_transactions (
            transaction_id, source_id, account_id, transaction_date, amount,
            currency, transaction_type, merchant_name, merchant_category,
            status, country_code, natural_key_hash, is_duplicate,
            duplicate_of, ingestion_timestamp, ingestion_run_id, source_system
        ) VALUES (
            :transaction_id, :source_id, :account_id, :transaction_date,
            :amount, :currency, :transaction_type, :merchant_name,
            :merchant_category, :status, :country_code, :natural_key_hash,
            :is_duplicate, :duplicate_of, :ingestion_timestamp,
            :ingestion_run_id, :source_system
        )
        ON CONFLICT(transaction_id) DO UPDATE SET
            source_id           = excluded.source_id,
            account_id          = excluded.account_id,
            transaction_date    = excluded.transaction_date,
            amount              = excluded.amount,
            currency            = excluded.currency,
            transaction_type    = excluded.transaction_type,
            merchant_name       = excluded.merchant_name,
            merchant_category   = excluded.merchant_category,
            status              = excluded.status,
            country_code        = excluded.country_code,
            natural_key_hash    = excluded.natural_key_hash,
            is_duplicate        = excluded.is_duplicate,
            duplicate_of        = excluded.duplicate_of,
            ingestion_timestamp = excluded.ingestion_timestamp,
            ingestion_run_id    = excluded.ingestion_run_id
    """
    with conn:  # transactional: a mid-load failure leaves no partial batch
        conn.executemany(sql, rows)
    return len(rows)


def insert_quarantine(
    conn: sqlite3.Connection, rows: Sequence[dict[str, Any]]
) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO quarantine_transactions (
            transaction_id, source_id, raw_record, error_reason, error_count,
            failed_fields, ingestion_timestamp, ingestion_run_id, source_system
        ) VALUES (
            :transaction_id, :source_id, :raw_record, :error_reason,
            :error_count, :failed_fields, :ingestion_timestamp,
            :ingestion_run_id, :source_system
        )
    """
    with conn:
        conn.executemany(sql, rows)
    return len(rows)


def clear_quarantine_for_run(conn: sqlite3.Connection, run_id: str) -> None:
    """Remove a run's quarantine rows before rewriting them.

    Quarantine is append-only across runs but idempotent within a run, so a
    retried run replaces its own rows rather than accumulating copies.
    """
    with conn:
        conn.execute(
            "DELETE FROM quarantine_transactions WHERE ingestion_run_id = ?",
            (run_id,),
        )


# --------------------------------------------------------------------------
# Watermark
# --------------------------------------------------------------------------


def get_watermark(conn: sqlite3.Connection, source_system: str) -> str | None:
    """Highest watermark from a run that actually succeeded.

    Only successful runs count. Advancing on a failed or partial run would
    move the filter past records that were never persisted, and those records
    would then never be fetched again - silent, permanent data loss.
    """
    row = conn.execute(
        """
        SELECT watermark_after
        FROM pipeline_watermark
        WHERE source_system = ?
          AND run_status = 'success'
          AND watermark_after IS NOT NULL
        ORDER BY watermark_after DESC
        LIMIT 1
        """,
        (source_system,),
    ).fetchone()
    return row["watermark_after"] if row else None


def start_run(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    source_system: str,
    watermark_before: str | None,
    effective_filter_from: str | None,
    run_mode: str,
) -> None:
    started = utc_now_iso()
    with conn:
        conn.execute(
            """
            INSERT INTO pipeline_watermark (
                source_system, run_id, run_started_at, run_status,
                watermark_before, effective_filter_from
            ) VALUES (?, ?, ?, 'running', ?, ?)
            """,
            (source_system, run_id, started, watermark_before,
             effective_filter_from),
        )
        conn.execute(
            """
            INSERT INTO ingestion_run_metrics (
                run_id, source_system, run_started_at, run_status, run_mode
            ) VALUES (?, ?, ?, 'running', ?)
            """,
            (run_id, source_system, started, run_mode),
        )


def complete_run(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    status: str,
    watermark_after: str | None,
    counts: dict[str, int],
    http_requests: int,
    http_retries: int,
    duration_seconds: float,
    error_message: str | None = None,
) -> None:
    """Finalise the run and, only on success, advance the watermark."""
    completed = utc_now_iso()
    fetched = counts.get("fetched", 0)
    quarantine_rate = (
        counts.get("quarantined", 0) / fetched if fetched else 0.0
    )
    duplicate_rate = counts.get("duplicate", 0) / fetched if fetched else 0.0

    with conn:
        conn.execute(
            """
            UPDATE pipeline_watermark
               SET run_completed_at    = ?,
                   run_status          = ?,
                   watermark_after     = ?,
                   records_fetched     = ?,
                   records_valid       = ?,
                   records_quarantined = ?,
                   records_duplicate   = ?,
                   error_message       = ?
             WHERE run_id = ?
            """,
            (
                completed, status, watermark_after, fetched,
                counts.get("valid", 0), counts.get("quarantined", 0),
                counts.get("duplicate", 0), error_message, run_id,
            ),
        )
        conn.execute(
            """
            UPDATE ingestion_run_metrics
               SET run_completed_at    = ?,
                   run_status          = ?,
                   records_fetched     = ?,
                   records_valid       = ?,
                   records_quarantined = ?,
                   records_duplicate   = ?,
                   quarantine_rate     = ?,
                   duplicate_rate      = ?,
                   http_requests       = ?,
                   http_retries        = ?,
                   duration_seconds    = ?,
                   error_message       = ?
             WHERE run_id = ?
            """,
            (
                completed, status, fetched, counts.get("valid", 0),
                counts.get("quarantined", 0), counts.get("duplicate", 0),
                quarantine_rate, duplicate_rate, http_requests, http_retries,
                duration_seconds, error_message, run_id,
            ),
        )


def watermark_history(
    conn: sqlite3.Connection, source_system: str
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT run_id, run_started_at, run_completed_at, run_status,
               watermark_before, watermark_after, effective_filter_from,
               records_fetched, records_valid, records_quarantined,
               records_duplicate
        FROM pipeline_watermark
        WHERE source_system = ?
        ORDER BY watermark_id
        """,
        (source_system,),
    ).fetchall()
    return [dict(r) for r in rows]


# --------------------------------------------------------------------------
# Export helpers
# --------------------------------------------------------------------------


def decimal_to_text(value: Decimal) -> str:
    """Store money as text to avoid any float round-trip.

    SQLite has no decimal type. Storing the canonical string preserves the
    exact value; arithmetic casts to REAL only inside the summary query,
    where the magnitudes involved are far below the precision limit.
    """
    return format(value, "f")


def rows_to_dicts(rows: Iterable[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(r) for r in rows]


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
