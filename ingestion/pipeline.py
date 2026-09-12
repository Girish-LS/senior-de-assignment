"""Ingestion pipeline: fetch, validate, quarantine, deduplicate, persist.

Covers Task 1 (raw ingestion with data quality handling) and Task 3
(incremental ingestion with watermark logic), because they are the same
pipeline with a different filter. Splitting them into two scripts would
duplicate the validation and persistence path and invite the two copies to
diverge.

The watermark trap
------------------
`transaction_date` is a *business* timestamp: when the transaction happened,
not when the source learned about it. Two consequences, both live in this
dataset:

1.  The watermark must be computed from *validated* records only. Two
    quarantined records carry dates of 2024-04-15 and 2024-11-31, while every
    valid record falls in January to March. A watermark taken over raw
    records would jump to November, and every subsequent run would filter
    `gte 2024-11-31` and return nothing. Forever. The pipeline would report
    success, the dashboard would stay green, and the data would silently stop
    arriving. This is the single worst failure mode available here, and it is
    a one-line mistake.

2.  A record dated Tuesday but written to the source on Thursday is invisible
    to a filter on business time once the watermark has passed Wednesday. The
    lookback window bounds that exposure by re-reading a trailing period on
    every run. The re-read is free because bronze loads are upserts.

Probing the API confirmed there is no created_at or inserted_at column, so
business time is the only filter available and the lookback is mandatory
rather than belt-and-braces. The undocumented `id` sequence would be a better
cursor, and that is noted in the design note as the change to make first.
"""

from __future__ import annotations

import csv
import json
import logging
import time
import uuid
from collections.abc import Iterable, Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ingestion import storage
from ingestion.api_client import ApiError, TransactionsApiClient
from ingestion.config import Settings
from ingestion.dedupe import flag_duplicates
from ingestion.models import SCHEMA_FIELDS, Transaction, validate_record

logger = logging.getLogger(__name__)

SOURCE_SYSTEM = "transactions_api"
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class IngestionResult(dict):
    """Run summary. A plain dict so it serialises without ceremony."""


# --------------------------------------------------------------------------
# Record sources
# --------------------------------------------------------------------------


def records_from_api(
    client: TransactionsApiClient, since: str | None
) -> Iterator[dict[str, Any]]:
    yield from client.iter_transactions(since=since)


def records_from_csv(
    path: Path, since: str | None
) -> Iterator[dict[str, Any]]:
    """Read the offline fixture.

    Provided as a fallback so the pipeline is demonstrable without network
    access. The fixture was verified to hold the same 352 records as the live
    API, so results are identical.

    The date filter is applied as a string comparison, which mirrors what the
    API does: `transaction_date` is almost certainly a text column at source
    (Postgres would have rejected '2024-11-31' outright and normalised
    '2024-04-15 09:30:00'), so `gte` there is also lexicographic. ISO-8601
    sorts chronologically when the format is uniform, which is what makes
    this work.
    """
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if since and row.get("transaction_date", "") < since:
                continue
            yield row


# --------------------------------------------------------------------------
# Watermark arithmetic
# --------------------------------------------------------------------------


def apply_lookback(watermark: str, lookback_hours: int) -> str:
    """Shift the watermark backwards to form the effective filter.

    Returns the watermark unchanged if it cannot be parsed, which is the safe
    direction: a filter that is too broad costs a little redundant work, a
    filter that is too narrow loses data.
    """
    if lookback_hours <= 0:
        return watermark
    try:
        ts = datetime.strptime(watermark, TIMESTAMP_FORMAT).replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        logger.warning("watermark %r is unparseable; using it as-is", watermark)
        return watermark
    return (ts - timedelta(hours=lookback_hours)).strftime(TIMESTAMP_FORMAT)


def max_valid_transaction_date(transactions: Iterable[Transaction]) -> str | None:
    """Highest transaction_date across *validated* records only.

    The restriction to valid records is the whole point. See module docstring.
    """
    dates = [t.transaction_date for t in transactions]
    return max(dates) if dates else None


# --------------------------------------------------------------------------
# The pipeline
# --------------------------------------------------------------------------


def run_ingestion(
    settings: Settings,
    *,
    mode: str = "full",
    source: str = "api",
    csv_path: Path | None = None,
    run_id: str | None = None,
) -> IngestionResult:
    """Execute one ingestion run.

    mode   'full' ignores any stored watermark and reads everything.
           'incremental' filters from (watermark - lookback).
    source 'api' reads the live endpoint; 'csv' reads the offline fixture.
    """
    run_id = run_id or f"run-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    started = time.monotonic()

    conn = storage.connect(settings.warehouse_path)

    # ---- decide the filter -------------------------------------------
    watermark_before = storage.get_watermark(conn, SOURCE_SYSTEM)

    if mode == "full":
        # First run, or a deliberate full reload. The initial watermark is
        # explicit configuration rather than an implicit null, so first-run
        # behaviour is a decision rather than an emergent property.
        effective_filter: str | None = None
        logger.info("mode=full: reading all records, no date filter")
    elif watermark_before is None:
        effective_filter = None
        logger.info(
            "mode=incremental but no prior successful run: "
            "behaving as a first run and reading all records"
        )
    else:
        effective_filter = apply_lookback(watermark_before, settings.lookback_hours)
        logger.info(
            "mode=incremental: watermark=%s lookback=%dh filter=gte.%s",
            watermark_before, settings.lookback_hours, effective_filter,
        )

    storage.start_run(
        conn,
        run_id=run_id,
        source_system=SOURCE_SYSTEM,
        watermark_before=watermark_before,
        effective_filter_from=effective_filter,
        run_mode=mode,
    )

    client: TransactionsApiClient | None = None
    counts = {"fetched": 0, "valid": 0, "quarantined": 0, "duplicate": 0}
    error_message: str | None = None

    try:
        # ---- fetch ----------------------------------------------------
        if source == "csv":
            if csv_path is None:
                raise ValueError("source='csv' requires csv_path")
            raw_records = list(records_from_csv(csv_path, effective_filter))
        else:
            client = TransactionsApiClient(
                base_url=settings.api_base_url,
                api_key=settings.api_key,
                auth_token=settings.auth_token,
                page_size=settings.page_size,
                max_retries=settings.max_retries,
                backoff_base_seconds=settings.backoff_base_seconds,
                timeout_seconds=settings.request_timeout_seconds,
            )
            raw_records = list(records_from_api(client, effective_filter))

        counts["fetched"] = len(raw_records)
        logger.info("fetched %d records", counts["fetched"])

        # ---- validate -------------------------------------------------
        ingested_at = storage.utc_now_iso()
        valid: list[Transaction] = []
        source_ids: dict[str, Any] = {}
        quarantine_rows: list[dict[str, Any]] = []

        for raw in raw_records:
            txn, reasons = validate_record(raw)
            if txn is None:
                failed_fields = sorted({r.split(":", 1)[0] for r in reasons})
                quarantine_rows.append(
                    {
                        "transaction_id": raw.get("transaction_id"),
                        "source_id": raw.get("id"),
                        "raw_record": json.dumps(raw, default=str, sort_keys=True),
                        "error_reason": "; ".join(reasons),
                        "error_count": len(reasons),
                        "failed_fields": ",".join(failed_fields),
                        "ingestion_timestamp": ingested_at,
                        "ingestion_run_id": run_id,
                        "source_system": SOURCE_SYSTEM,
                    }
                )
                continue
            valid.append(txn)
            if "id" in raw:
                source_ids[txn.transaction_id] = raw["id"]

        counts["valid"] = len(valid)
        counts["quarantined"] = len(quarantine_rows)

        # ---- deduplicate (flag, do not drop) --------------------------
        hashes, duplicate_of, redundant = flag_duplicates(valid)
        counts["duplicate"] = redundant

        # ---- persist --------------------------------------------------
        bronze_rows = [
            {
                "transaction_id": t.transaction_id,
                "source_id": source_ids.get(t.transaction_id),
                "account_id": t.account_id,
                "transaction_date": t.transaction_date,
                "amount": storage.decimal_to_text(t.amount),
                "currency": t.currency,
                "transaction_type": t.transaction_type,
                "merchant_name": t.merchant_name,
                "merchant_category": t.merchant_category,
                "status": t.status,
                "country_code": t.country_code,
                "natural_key_hash": hashes[t.transaction_id],
                "is_duplicate": 1 if duplicate_of.get(t.transaction_id) else 0,
                "duplicate_of": duplicate_of.get(t.transaction_id),
                "ingestion_timestamp": ingested_at,
                "ingestion_run_id": run_id,
                "source_system": SOURCE_SYSTEM,
            }
            for t in valid
        ]

        storage.upsert_bronze(conn, bronze_rows)
        storage.clear_quarantine_for_run(conn, run_id)
        storage.insert_quarantine(conn, quarantine_rows)

        # ---- advance the watermark ------------------------------------
        # Computed over VALID records only. See module docstring.
        batch_max = max_valid_transaction_date(valid)
        if batch_max is None:
            # No new data. This is a normal outcome, not a failure: the
            # watermark stays where it was and the run exits successfully.
            watermark_after = watermark_before
            logger.info("no valid records in this batch; watermark unchanged")
        elif watermark_before is None:
            watermark_after = batch_max
        else:
            # Never move backwards. The lookback re-reads older records, and
            # a naive max over the batch could otherwise regress the mark.
            watermark_after = max(batch_max, watermark_before)

        duration = time.monotonic() - started
        storage.complete_run(
            conn,
            run_id=run_id,
            status="success",
            watermark_after=watermark_after,
            counts=counts,
            http_requests=client.stats.requests_made if client else 0,
            http_retries=client.stats.retries if client else 0,
            duration_seconds=duration,
        )

        result = IngestionResult(
            run_id=run_id,
            status="success",
            mode=mode,
            source=source,
            watermark_before=watermark_before,
            effective_filter_from=effective_filter,
            watermark_after=watermark_after,
            duration_seconds=round(duration, 3),
            http_requests=client.stats.requests_made if client else 0,
            http_retries=client.stats.retries if client else 0,
            **counts,
        )
        logger.info("run %s succeeded: %s", run_id, json.dumps(result, default=str))
        return result

    except (ApiError, Exception) as exc:  # noqa: BLE001 - recorded then re-raised
        error_message = f"{type(exc).__name__}: {exc}"
        duration = time.monotonic() - started
        # Watermark is NOT advanced on failure. A failed run that moved the
        # mark would skip past records it never persisted.
        storage.complete_run(
            conn,
            run_id=run_id,
            status="failed",
            watermark_after=None,
            counts=counts,
            http_requests=client.stats.requests_made if client else 0,
            http_retries=client.stats.retries if client else 0,
            duration_seconds=duration,
            error_message=error_message,
        )
        logger.error("run %s failed: %s", run_id, error_message)
        raise

    finally:
        conn.close()
