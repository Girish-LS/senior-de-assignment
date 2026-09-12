"""Export sample outputs for review.

    python -m ingestion.export_outputs

Writes the artefacts the assessment asks for into outputs/, so a reviewer can
understand the results without running anything:

    quarantine_sample.csv           captured defects with every reason
    daily_account_summary_sample.csv  the mart
    bronze_sample.csv               raw layer including duplicate flags
    duplicate_groups.csv            every detected duplicate pair
    watermark_run1.json             watermark state after the first run
    watermark_run2.json             watermark state after the second run
    run_metrics.csv                 per-run counters
    data_quality_report.txt         assertion results
"""

from __future__ import annotations

import csv
import logging
import sqlite3
from pathlib import Path

from ingestion import storage
from ingestion.config import ConfigError, load_settings
from ingestion.pipeline import SOURCE_SYSTEM
from ingestion.transform import run_assertions

logger = logging.getLogger(__name__)


def export_query(conn: sqlite3.Connection, sql: str, path: Path) -> int:
    rows = conn.execute(sql).fetchall()
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return 0
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))
    return len(rows)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        # Reads the warehouse only; no API credentials needed.
        settings = load_settings(require_api=False)
    except ConfigError as exc:
        logger.error("configuration error: %s", exc)
        return 2

    out = settings.outputs_dir
    conn = storage.connect(settings.warehouse_path)

    try:
        # One row per defective record. The underlying table is append-only
        # across runs, so records the source never fixes would otherwise
        # appear once per run and the sample would misrepresent the defect
        # count. occurrence_count preserves the recurrence signal.
        n = export_query(
            conn,
            """
            SELECT transaction_id, error_count, failed_fields, error_reason,
                   occurrence_count, runs_seen_in, first_seen_at, last_seen_at,
                   raw_record
            FROM quarantine_current
            ORDER BY transaction_id
            """,
            out / "quarantine_sample.csv",
        )
        logger.info("quarantine_sample.csv          %d distinct records", n)

        # Full audit history, every occurrence in every run.
        n = export_query(
            conn,
            """
            SELECT transaction_id, error_count, failed_fields, error_reason,
                   ingestion_timestamp, ingestion_run_id, raw_record
            FROM quarantine_transactions
            ORDER BY transaction_id, ingestion_run_id
            """,
            out / "quarantine_history.csv",
        )
        logger.info("quarantine_history.csv         %d rows (all occurrences)", n)

        n = export_query(
            conn,
            """
            SELECT account_id, transaction_date, total_debit_amount,
                   total_credit_amount, net_amount, transaction_count,
                   distinct_merchants, top_category, currencies, updated_at
            FROM daily_account_summary
            ORDER BY account_id, transaction_date
            """,
            out / "daily_account_summary_sample.csv",
        )
        logger.info("daily_account_summary_sample.csv %d rows", n)

        n = export_query(
            conn,
            """
            SELECT transaction_id, source_id, account_id, transaction_date,
                   amount, currency, transaction_type, merchant_name,
                   merchant_category, status, country_code, natural_key_hash,
                   is_duplicate, duplicate_of, ingestion_timestamp,
                   ingestion_run_id
            FROM bronze_transactions
            ORDER BY transaction_id
            LIMIT 50
            """,
            out / "bronze_sample.csv",
        )
        logger.info("bronze_sample.csv              %d rows (first 50)", n)

        n = export_query(
            conn,
            """
            SELECT b.natural_key_hash,
                   b.transaction_id   AS duplicate_transaction_id,
                   b.duplicate_of     AS surviving_transaction_id,
                   b.account_id, b.transaction_date, b.amount, b.currency,
                   b.transaction_type, b.merchant_name, b.merchant_category,
                   b.status, b.country_code
            FROM bronze_transactions b
            WHERE b.is_duplicate = 1
            ORDER BY b.duplicate_of
            """,
            out / "duplicate_groups.csv",
        )
        logger.info("duplicate_groups.csv           %d rows", n)

        n = export_query(
            conn,
            """
            SELECT run_id, run_mode, run_status, run_started_at,
                   run_completed_at, records_fetched, records_valid,
                   records_quarantined, records_duplicate, quarantine_rate,
                   duplicate_rate, http_requests, http_retries,
                   duration_seconds
            FROM ingestion_run_metrics
            ORDER BY run_started_at
            """,
            out / "run_metrics.csv",
        )
        logger.info("run_metrics.csv                %d rows", n)

        # Watermark snapshots, one per run in history.
        history = storage.watermark_history(conn, SOURCE_SYSTEM)
        for index, entry in enumerate(history, start=1):
            storage.write_json(
                out / f"watermark_run{index}.json",
                {
                    "source_system": SOURCE_SYSTEM,
                    "snapshot_after_run": index,
                    "lookback_hours": settings.lookback_hours,
                    "run": entry,
                    "full_history": history[:index],
                },
            )
            logger.info("watermark_run%d.json            written", index)

        # Data quality report.
        results = run_assertions(conn)
        report = ["Data quality assertions", "=" * 60]
        report += [str(r) for r in results]
        failed = [r for r in results if not r.passed]
        report += [
            "=" * 60,
            f"{len(results) - len(failed)} passed, {len(failed)} failed",
        ]
        (out / "data_quality_report.txt").write_text(
            "\n".join(report) + "\n", encoding="utf-8"
        )
        logger.info("data_quality_report.txt        %d assertions", len(results))

        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
