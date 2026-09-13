"""Tests for deduplication, watermark logic, and end-to-end idempotency.

    python -m unittest discover -s tests -v

These are the tests that protect the design decisions rather than the field
rules. The watermark tests in particular cover the failure mode that this
dataset is built to expose.
"""

from __future__ import annotations

import csv
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from ingestion import storage
from ingestion.config import Settings
from ingestion.dedupe import flag_duplicates, natural_key_hash
from ingestion.models import Transaction, validate_record
from ingestion.pipeline import (
    SOURCE_SYSTEM,
    apply_lookback,
    max_valid_transaction_date,
    run_ingestion,
)
from ingestion.transform import EPOCH, build_summary, run_assertions

FIXTURE = Path(__file__).resolve().parent.parent / "data" / "transactions.csv"


def txn(**overrides) -> Transaction:
    base = {
        "transaction_id": "TXN-0001",
        "account_id": "ACC-1001",
        "transaction_date": "2024-01-15T08:23:11Z",
        "amount": "142.50",
        "currency": "USD",
        "transaction_type": "debit",
        "merchant_name": "Amazon",
        "merchant_category": "e-commerce",
        "status": "completed",
        "country_code": "US",
    }
    base.update(overrides)
    result, reasons = validate_record(base)
    assert result is not None, reasons
    return result


class TestNaturalKeyHash(unittest.TestCase):
    def test_differs_only_by_transaction_id_produces_same_hash(self):
        """The definition of a duplicate: same transaction, different id."""
        a = txn(transaction_id="TXN-0062")
        b = txn(transaction_id="TXN-0348")
        self.assertEqual(natural_key_hash(a), natural_key_hash(b))

    def test_any_other_field_changes_the_hash(self):
        base = txn()
        for field, value in [
            ("account_id", "ACC-1002"),
            ("transaction_date", "2024-01-16T08:23:11Z"),
            ("amount", "142.51"),
            ("currency", "EUR"),
            ("transaction_type", "credit"),
            ("merchant_name", "eBay"),
            ("merchant_category", "retail"),
            ("status", "pending"),
            ("country_code", "GB"),
        ]:
            with self.subTest(field=field):
                self.assertNotEqual(
                    natural_key_hash(base),
                    natural_key_hash(txn(**{field: value})),
                )

    def test_amount_representation_is_normalised(self):
        """238.2 and 238.20 are the same amount. Without normalisation they
        would hash differently and a real duplicate would go undetected."""
        self.assertEqual(
            natural_key_hash(txn(amount="238.2")),
            natural_key_hash(txn(amount="238.20")),
        )

    def test_hash_is_stable_across_calls(self):
        self.assertEqual(natural_key_hash(txn()), natural_key_hash(txn()))


class TestFlagDuplicates(unittest.TestCase):
    def test_no_duplicates_in_distinct_records(self):
        records = [
            txn(transaction_id="TXN-0001"),
            txn(transaction_id="TXN-0002", amount="99.99"),
        ]
        _, duplicate_of, count = flag_duplicates(records)
        self.assertEqual(count, 0)
        self.assertTrue(all(v is None for v in duplicate_of.values()))

    def test_survivor_is_lowest_transaction_id(self):
        """Deterministic survivorship. An arbitrary survivor would make
        downstream aggregates non-reproducible."""
        records = [
            txn(transaction_id="TXN-0348"),
            txn(transaction_id="TXN-0062"),
        ]
        _, duplicate_of, count = flag_duplicates(records)
        self.assertEqual(count, 1)
        self.assertIsNone(duplicate_of["TXN-0062"])
        self.assertEqual(duplicate_of["TXN-0348"], "TXN-0062")

    def test_survivorship_independent_of_input_order(self):
        forward = flag_duplicates(
            [txn(transaction_id="TXN-0062"), txn(transaction_id="TXN-0348")]
        )[1]
        reverse = flag_duplicates(
            [txn(transaction_id="TXN-0348"), txn(transaction_id="TXN-0062")]
        )[1]
        self.assertEqual(forward, reverse)

    def test_group_of_three_leaves_two_redundant(self):
        records = [
            txn(transaction_id=f"TXN-000{i}") for i in (1, 2, 3)
        ]
        _, duplicate_of, count = flag_duplicates(records)
        self.assertEqual(count, 2)
        self.assertIsNone(duplicate_of["TXN-0001"])


class TestWatermarkArithmetic(unittest.TestCase):
    def test_lookback_shifts_backwards(self):
        self.assertEqual(
            apply_lookback("2024-03-30T22:35:29Z", 72),
            "2024-03-27T22:35:29Z",
        )

    def test_zero_lookback_is_identity(self):
        self.assertEqual(
            apply_lookback("2024-03-30T22:35:29Z", 0),
            "2024-03-30T22:35:29Z",
        )

    def test_unparseable_watermark_is_returned_unchanged(self):
        """Safe direction: too broad a filter costs redundant work, too
        narrow a filter loses data."""
        self.assertEqual(apply_lookback("not-a-date", 72), "not-a-date")

    def test_lookback_crosses_month_boundary(self):
        self.assertEqual(
            apply_lookback("2024-03-01T00:00:00Z", 48),
            "2024-02-28T00:00:00Z",
        )

    def test_max_valid_date_ignores_invalid_records(self):
        """THE critical test.

        Two quarantined records in the dataset carry dates of 2024-04-15 and
        2024-11-31, while every valid record falls in January to March. A
        watermark computed over raw records would jump to November, and every
        later run would filter gte that date and return nothing - forever,
        silently, with the pipeline reporting success.

        max_valid_transaction_date only ever sees validated records, which is
        what makes this safe.
        """
        valid = [
            txn(transaction_id="TXN-0001", transaction_date="2024-01-15T08:23:11Z"),
            txn(transaction_id="TXN-0002", transaction_date="2024-03-30T22:35:29Z"),
        ]
        self.assertEqual(
            max_valid_transaction_date(valid), "2024-03-30T22:35:29Z"
        )

    def test_max_valid_date_of_empty_batch_is_none(self):
        """No new data is a normal outcome, not a failure."""
        self.assertIsNone(max_valid_transaction_date([]))


class PipelineTestCase(unittest.TestCase):
    """Base class giving each test an isolated warehouse."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        root = Path(self._tmp.name)
        self.settings = Settings(
            api_base_url="https://example.invalid/rest/v1",
            api_key="test-key",
            auth_token="test-key",
            lookback_hours=72,
        )
        # Redirect warehouse and outputs into the temp directory.
        object.__setattr__(self.settings, "_root", root)
        self.warehouse = root / "warehouse" / "transactions.sqlite"
        type(self.settings).warehouse_path = property(
            lambda s, p=self.warehouse: p
        )
        type(self.settings).outputs_dir = property(lambda s, r=root: r / "outputs")
        type(self.settings).state_dir = property(lambda s, r=root: r / "state")

    def tearDown(self):
        self._tmp.cleanup()


class TestEndToEnd(PipelineTestCase):
    @unittest.skipUnless(FIXTURE.exists(), "fixture not present")
    def test_full_run_counts_match_known_dataset(self):
        result = run_ingestion(
            self.settings, mode="full", source="csv", csv_path=FIXTURE
        )
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["fetched"], 352)
        self.assertEqual(result["valid"], 349)
        self.assertEqual(result["quarantined"], 3)
        self.assertEqual(result["duplicate"], 5)

    @unittest.skipUnless(FIXTURE.exists(), "fixture not present")
    def test_watermark_avoids_the_invalid_date_trap(self):
        """The watermark must land on the highest VALID date, not on the
        November date carried by a quarantined record."""
        result = run_ingestion(
            self.settings, mode="full", source="csv", csv_path=FIXTURE
        )
        self.assertEqual(result["watermark_after"], "2024-03-30T22:35:29Z")
        self.assertNotIn("2024-11", result["watermark_after"])
        self.assertNotIn("2024-04", result["watermark_after"])

    @unittest.skipUnless(FIXTURE.exists(), "fixture not present")
    def test_second_run_inserts_no_duplicate_rows(self):
        """Idempotency: re-running must not change the row count."""
        run_ingestion(self.settings, mode="full", source="csv", csv_path=FIXTURE)
        conn = storage.connect(self.settings.warehouse_path)
        before = conn.execute(
            "SELECT COUNT(*) FROM bronze_transactions"
        ).fetchone()[0]
        conn.close()

        run_ingestion(
            self.settings, mode="incremental", source="csv", csv_path=FIXTURE
        )
        conn = storage.connect(self.settings.warehouse_path)
        after = conn.execute(
            "SELECT COUNT(*) FROM bronze_transactions"
        ).fetchone()[0]
        conn.close()

        self.assertEqual(before, after)
        self.assertEqual(after, 349)

    @unittest.skipUnless(FIXTURE.exists(), "fixture not present")
    def test_incremental_with_no_prior_run_behaves_as_first_run(self):
        result = run_ingestion(
            self.settings, mode="incremental", source="csv", csv_path=FIXTURE
        )
        self.assertEqual(result["status"], "success")
        self.assertIsNone(result["watermark_before"])
        self.assertEqual(result["fetched"], 352)

    @unittest.skipUnless(FIXTURE.exists(), "fixture not present")
    def test_watermark_never_moves_backwards(self):
        first = run_ingestion(
            self.settings, mode="full", source="csv", csv_path=FIXTURE
        )
        second = run_ingestion(
            self.settings, mode="incremental", source="csv", csv_path=FIXTURE
        )
        self.assertGreaterEqual(
            second["watermark_after"], first["watermark_after"]
        )

    def test_no_new_data_leaves_watermark_unchanged(self):
        """A run that finds nothing is a success, not a failure."""
        with tempfile.NamedTemporaryFile(
            "w", suffix=".csv", delete=False, newline=""
        ) as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "transaction_id", "account_id", "transaction_date",
                    "amount", "currency", "transaction_type", "merchant_name",
                    "merchant_category", "status", "country_code",
                ],
            )
            writer.writeheader()
            empty = Path(handle.name)

        result = run_ingestion(
            self.settings, mode="full", source="csv", csv_path=empty
        )
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["fetched"], 0)
        self.assertIsNone(result["watermark_after"])
        empty.unlink()

    @unittest.skipUnless(FIXTURE.exists(), "fixture not present")
    def test_quarantine_captures_every_reason(self):
        run_ingestion(self.settings, mode="full", source="csv", csv_path=FIXTURE)
        conn = storage.connect(self.settings.warehouse_path)
        rows = conn.execute(
            """
            SELECT transaction_id, error_count, error_reason, failed_fields
            FROM quarantine_transactions
            ORDER BY transaction_id
            """
        ).fetchall()
        conn.close()

        self.assertEqual(len(rows), 3)
        by_id = {r["transaction_id"]: r for r in rows}

        # TXN-0345: impossible date, bad category, unassigned country code.
        self.assertEqual(by_id["TXN-0345"]["error_count"], 3)
        self.assertIn("country_code", by_id["TXN-0345"]["failed_fields"])

        # TXN-0346: malformed date, negative amount, blank merchant, bad status.
        self.assertEqual(by_id["TXN-0346"]["error_count"], 4)
        self.assertIn("merchant_name", by_id["TXN-0346"]["failed_fields"])

        # TXN-0347: zero amount, bad type, bad category, unassigned country.
        self.assertEqual(by_id["TXN-0347"]["error_count"], 4)
        self.assertIn("amount", by_id["TXN-0347"]["failed_fields"])


class TestSummary(PipelineTestCase):
    @unittest.skipUnless(FIXTURE.exists(), "fixture not present")
    def setUp(self):
        super().setUp()
        run_ingestion(self.settings, mode="full", source="csv", csv_path=FIXTURE)
        self.conn = storage.connect(self.settings.warehouse_path)

    def tearDown(self):
        self.conn.close()
        super().tearDown()

    def test_summary_builds_and_passes_all_assertions(self):
        build_summary(self.conn, since=EPOCH)
        results = run_assertions(self.conn)
        failures = [r for r in results if not r.passed]
        self.assertEqual(failures, [], f"failing assertions: {failures}")

    def test_summary_is_idempotent(self):
        """Rebuilding over unchanged input must produce identical output."""
        columns = """
            account_id, transaction_date, total_debit_amount,
            total_credit_amount, net_amount, transaction_count,
            distinct_merchants, top_category, currencies
        """
        build_summary(self.conn, since=EPOCH, run_timestamp="2000-01-01T00:00:00Z")
        first = [
            tuple(r)
            for r in self.conn.execute(
                f"SELECT {columns} FROM daily_account_summary "
                "ORDER BY account_id, transaction_date"
            )
        ]
        build_summary(self.conn, since=EPOCH, run_timestamp="2001-01-01T00:00:00Z")
        second = [
            tuple(r)
            for r in self.conn.execute(
                f"SELECT {columns} FROM daily_account_summary "
                "ORDER BY account_id, transaction_date"
            )
        ]
        self.assertEqual(first, second)

    def test_summary_excludes_non_completed_statuses(self):
        build_summary(self.conn, since=EPOCH)
        expected = self.conn.execute(
            "SELECT COUNT(*) FROM bronze_transactions "
            "WHERE status = 'completed' AND is_duplicate = 0"
        ).fetchone()[0]
        actual = self.conn.execute(
            "SELECT SUM(transaction_count) FROM daily_account_summary"
        ).fetchone()[0]
        self.assertEqual(actual, expected)

    def test_summary_excludes_duplicates(self):
        """Four of the five duplicate pairs are completed. Counting them
        would inflate the totals - the defect surfaces as wrong money."""
        build_summary(self.conn, since=EPOCH)
        counted = self.conn.execute(
            "SELECT SUM(transaction_count) FROM daily_account_summary"
        ).fetchone()[0]
        all_completed = self.conn.execute(
            "SELECT COUNT(*) FROM bronze_transactions WHERE status = 'completed'"
        ).fetchone()[0]
        self.assertLess(counted, all_completed)

    def test_top_category_is_null_only_for_credit_only_days(self):
        build_summary(self.conn, since=EPOCH)
        mismatched = self.conn.execute(
            """
            SELECT COUNT(*) FROM daily_account_summary
            WHERE (top_category IS NULL AND total_debit_amount > 0)
               OR (top_category IS NOT NULL AND total_debit_amount = 0)
            """
        ).fetchone()[0]
        self.assertEqual(mismatched, 0)

    def test_currencies_are_sorted(self):
        build_summary(self.conn, since=EPOCH)
        for row in self.conn.execute(
            "SELECT currencies FROM daily_account_summary"
        ):
            values = row["currencies"].split(",")
            self.assertEqual(values, sorted(values))


class QuarantineCurrentViewTests(unittest.TestCase):
    """A record the source never fixes is re-quarantined on every run.

    The underlying table is append-only by design, because "was bad
    yesterday, still bad today" is an audit fact worth keeping. The view
    collapses that to one row per record so the defect count is not inflated,
    while occurrence_count preserves the recurrence signal.
    """

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.conn = storage.connect(Path(self.tmp.name) / "w.duckdb")

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def _quarantine(self, run_id: str, txn_id: str, reason: str, seen_at: str):
        storage.insert_quarantine(
            self.conn,
            [
                {
                    "transaction_id": txn_id,
                    "source_id": None,
                    "raw_record": "{}",
                    "error_reason": reason,
                    "error_count": 1,
                    "failed_fields": "amount",
                    "ingestion_timestamp": seen_at,
                    "ingestion_run_id": run_id,
                    "source_system": SOURCE_SYSTEM,
                }
            ],
        )

    def test_repeated_record_collapses_to_one_row(self):
        self._quarantine("run-1", "TXN-0345", "first", "2026-01-01T00:00:00Z")
        self._quarantine("run-2", "TXN-0345", "second", "2026-01-02T00:00:00Z")
        self._quarantine("run-2", "TXN-0347", "only once", "2026-01-02T00:00:00Z")

        rows = {
            r["transaction_id"]: r
            for r in self.conn.execute("SELECT * FROM quarantine_current")
        }
        self.assertEqual(len(rows), 2, "one row per distinct record")
        self.assertEqual(rows["TXN-0345"]["occurrence_count"], 2)
        self.assertEqual(rows["TXN-0345"]["runs_seen_in"], 2)
        self.assertEqual(rows["TXN-0347"]["occurrence_count"], 1)

    def test_view_reports_the_most_recent_error_state(self):
        self._quarantine("run-1", "TXN-0345", "first", "2026-01-01T00:00:00Z")
        self._quarantine("run-2", "TXN-0345", "second", "2026-01-02T00:00:00Z")
        row = self.conn.execute(
            "SELECT * FROM quarantine_current WHERE transaction_id = 'TXN-0345'"
        ).fetchone()
        self.assertEqual(row["error_reason"], "second")
        self.assertEqual(row["first_seen_at"], "2026-01-01T00:00:00Z")
        self.assertEqual(row["last_seen_at"], "2026-01-02T00:00:00Z")

    def test_history_table_retains_every_occurrence(self):
        """The audit trail must not be lost to the convenience view."""
        self._quarantine("run-1", "TXN-0345", "first", "2026-01-01T00:00:00Z")
        self._quarantine("run-2", "TXN-0345", "second", "2026-01-02T00:00:00Z")
        n = self.conn.execute(
            "SELECT COUNT(*) AS n FROM quarantine_transactions"
        ).fetchone()["n"]
        self.assertEqual(n, 2)


class OfflineModeConfigTests(unittest.TestCase):
    """A reviewer without API credentials must still be able to run the
    pipeline end to end against the bundled fixture."""

    def test_csv_mode_does_not_require_api_credentials(self):
        import os
        from ingestion.config import load_settings

        preserved = {
            k: os.environ.pop(k, None)
            for k in (
                "ASSESSMENT_API_BASE_URL",
                "ASSESSMENT_API_KEY",
                "ASSESSMENT_AUTH_TOKEN",
            )
        }
        try:
            with tempfile.TemporaryDirectory() as empty:
                settings = load_settings(
                    search_root=Path(empty), require_api=False
                )
            self.assertEqual(settings.api_base_url, "")
            self.assertEqual(settings.page_size, 100)
        finally:
            for k, v in preserved.items():
                if v is not None:
                    os.environ[k] = v

    def test_api_mode_still_fails_fast_without_credentials(self):
        import os
        from ingestion.config import ConfigError, load_settings

        preserved = {
            k: os.environ.pop(k, None)
            for k in ("ASSESSMENT_API_BASE_URL", "ASSESSMENT_API_KEY")
        }
        try:
            with tempfile.TemporaryDirectory() as empty:
                with self.assertRaises(ConfigError):
                    load_settings(search_root=Path(empty), require_api=True)
        finally:
            for k, v in preserved.items():
                if v is not None:
                    os.environ[k] = v


if __name__ == "__main__":
    unittest.main()
