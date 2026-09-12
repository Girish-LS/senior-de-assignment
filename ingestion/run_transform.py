"""CLI entry point for the transformation layer (Task 2).

    python -m ingestion.run_transform
    python -m ingestion.run_transform --since 2024-03-01T00:00:00Z
    python -m ingestion.run_transform --check-idempotency

Builds daily_account_summary from bronze and runs the data quality
assertions. Exits non-zero if any assertion fails, so the transformation is a
gate rather than a report nobody reads.
"""

from __future__ import annotations

import argparse
import logging
import sys

from ingestion import storage
from ingestion.config import ConfigError, load_settings
from ingestion.ingest_transactions import configure_logging
from ingestion.transform import EPOCH, build_summary, run_assertions


def check_idempotency(conn) -> bool:
    """Rebuild twice and compare, ignoring updated_at.

    An incremental model whose second run differs from its first is a silent
    correctness bug. This is the cheapest way to catch it, and the only one
    that actually proves the claim rather than asserting it.
    """
    build_summary(conn, since=EPOCH, run_timestamp="2000-01-01T00:00:00Z")
    first = conn.execute(
        """
        SELECT account_id, transaction_date, total_debit_amount,
               total_credit_amount, net_amount, transaction_count,
               distinct_merchants, top_category, currencies
        FROM daily_account_summary
        ORDER BY account_id, transaction_date
        """
    ).fetchall()

    build_summary(conn, since=EPOCH, run_timestamp="2001-01-01T00:00:00Z")
    second = conn.execute(
        """
        SELECT account_id, transaction_date, total_debit_amount,
               total_credit_amount, net_amount, transaction_count,
               distinct_merchants, top_category, currencies
        FROM daily_account_summary
        ORDER BY account_id, transaction_date
        """
    ).fetchall()

    identical = [tuple(r) for r in first] == [tuple(r) for r in second]
    print(
        f"idempotency: {len(first)} rows first pass, {len(second)} second pass, "
        f"{'IDENTICAL' if identical else 'DIFFERENT'}"
    )
    return identical


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build daily_account_summary and assert its quality."
    )
    parser.add_argument(
        "--since",
        default=EPOCH,
        help="rebuild only days at or after this timestamp (default: full rebuild)",
    )
    parser.add_argument(
        "--check-idempotency",
        action="store_true",
        help="build twice and verify the output is unchanged",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    configure_logging(args.verbose)
    log = logging.getLogger("transform")

    try:
        # Reads the warehouse only; no API credentials needed.
        settings = load_settings(require_api=False)
    except ConfigError as exc:
        log.error("configuration error: %s", exc)
        return 2

    conn = storage.connect(settings.warehouse_path)
    try:
        if args.check_idempotency:
            ok = check_idempotency(conn)
            if not ok:
                return 1
        else:
            rows = build_summary(conn, since=args.since)
            print(f"daily_account_summary: {rows} rows")

        print()
        print("Data quality assertions")
        print("-" * 60)
        results = run_assertions(conn)
        for result in results:
            print(result)

        failed = [r for r in results if not r.passed]
        print("-" * 60)
        print(f"{len(results) - len(failed)} passed, {len(failed)} failed")
        return 1 if failed else 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
