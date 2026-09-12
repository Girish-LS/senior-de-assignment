"""Unit tests for schema validation.

Written with unittest so they run on a bare Python install:

    python -m unittest discover -s tests -v

pytest will also collect them unchanged if it is available.

Tests target the *rules*, not the implementation. Each planted defect in the
dataset has a named test, so a regression points at a specific rule rather
than at "validation broke".
"""

from __future__ import annotations

import unittest
from decimal import Decimal

from ingestion.models import (
    CURRENCIES,
    MERCHANT_CATEGORIES,
    NATURAL_KEY_FIELDS,
    SCHEMA_FIELDS,
    STATUSES,
    TRANSACTION_TYPES,
    Transaction,
    validate_record,
)


def make_record(**overrides):
    """A valid record, with optional overrides.

    Values are strings throughout, matching how both the live API and the CSV
    fixture present them.
    """
    record = {
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
    record.update(overrides)
    return record


class TestHappyPath(unittest.TestCase):
    def test_valid_record_passes(self):
        txn, reasons = validate_record(make_record())
        self.assertEqual(reasons, [])
        self.assertIsInstance(txn, Transaction)
        self.assertEqual(txn.transaction_id, "TXN-0001")

    def test_amount_arrives_as_string_and_becomes_decimal(self):
        """The live API serialises numerics as JSON strings. Verified by
        probing the endpoint before writing the validator."""
        txn, reasons = validate_record(make_record(amount="1455.8"))
        self.assertEqual(reasons, [])
        self.assertEqual(txn.amount, Decimal("1455.8"))
        self.assertIsInstance(txn.amount, Decimal)

    def test_amount_accepts_native_number(self):
        txn, reasons = validate_record(make_record(amount=142.5))
        self.assertEqual(reasons, [])
        self.assertEqual(txn.amount, Decimal("142.5"))

    def test_every_accepted_enum_value_is_valid(self):
        for field, values in (
            ("currency", CURRENCIES),
            ("transaction_type", TRANSACTION_TYPES),
            ("merchant_category", MERCHANT_CATEGORIES),
            ("status", STATUSES),
        ):
            for value in values:
                with self.subTest(field=field, value=value):
                    _, reasons = validate_record(make_record(**{field: value}))
                    self.assertEqual(reasons, [])

    def test_transaction_datetime_is_utc(self):
        txn, _ = validate_record(make_record())
        dt = txn.transaction_datetime
        self.assertEqual((dt.year, dt.month, dt.day), (2024, 1, 15))
        self.assertEqual(dt.utcoffset().total_seconds(), 0)

    def test_transaction_day_is_the_calendar_date(self):
        txn, _ = validate_record(make_record())
        self.assertEqual(txn.transaction_day, "2024-01-15")

    def test_extra_undocumented_fields_are_ignored(self):
        """The API returns a Supabase-internal `id` outside the contract. It
        must not fail validation; it is captured as metadata instead."""
        _, reasons = validate_record(make_record(id=446))
        self.assertEqual(reasons, [])

    def test_transaction_is_immutable(self):
        """Bronze records are never edited after validation."""
        txn, _ = validate_record(make_record())
        with self.assertRaises(Exception):
            txn.amount = Decimal("999")


class TestAmount(unittest.TestCase):
    def test_must_be_strictly_positive(self):
        """Specification says > 0, not >= 0. Zero must fail.
        TXN-0347 carries 0.0 and TXN-0346 carries -127.5."""
        for bad in ["0", "0.0", "0.00", "-127.5", "-0.01"]:
            with self.subTest(amount=bad):
                txn, reasons = validate_record(make_record(amount=bad))
                self.assertIsNone(txn)
                self.assertTrue(
                    any("strictly greater than zero" in r for r in reasons)
                )

    def test_rejects_more_than_two_decimal_places(self):
        txn, reasons = validate_record(make_record(amount="10.123"))
        self.assertIsNone(txn)
        self.assertTrue(any("2 decimal places" in r for r in reasons))

    def test_accepts_fewer_than_two_decimal_places(self):
        for good in ["100", "238.2", "1455.8"]:
            with self.subTest(amount=good):
                _, reasons = validate_record(make_record(amount=good))
                self.assertEqual(reasons, [])

    def test_rejects_non_numeric(self):
        for bad in ["abc", "", "   ", "1,234.00", None, [], {}]:
            with self.subTest(amount=bad):
                txn, _ = validate_record(make_record(amount=bad))
                self.assertIsNone(txn)

    def test_rejects_non_finite(self):
        for bad in ["NaN", "Infinity", "-Infinity"]:
            with self.subTest(amount=bad):
                txn, _ = validate_record(make_record(amount=bad))
                self.assertIsNone(txn)

    def test_rejects_boolean(self):
        """bool subclasses int; an amount of True is nonsense."""
        txn, _ = validate_record(make_record(amount=True))
        self.assertIsNone(txn)


class TestTransactionDate(unittest.TestCase):
    def test_rejects_space_separator_and_missing_z(self):
        """TXN-0346: '2024-04-15 09:30:00'."""
        txn, reasons = validate_record(
            make_record(transaction_date="2024-04-15 09:30:00")
        )
        self.assertIsNone(txn)
        self.assertTrue(any("ISO 8601" in r for r in reasons))

    def test_rejects_impossible_calendar_day(self):
        """TXN-0345: '2024-11-31T14:22:00Z' matches the date pattern, but
        November has 30 days. This is why a regex alone is insufficient."""
        txn, reasons = validate_record(
            make_record(transaction_date="2024-11-31T14:22:00Z")
        )
        self.assertIsNone(txn)
        self.assertTrue(any("not a real calendar date" in r for r in reasons))

    def test_rejects_malformed_and_impossible_dates(self):
        for bad in [
            "2024-02-30T00:00:00Z",     # February never has 30 days
            "2023-02-29T00:00:00Z",     # 2023 is not a leap year
            "2024-04-31T00:00:00Z",     # April has 30 days
            "2024-13-01T00:00:00Z",     # month 13
            "2024-00-01T00:00:00Z",     # month 0
            "2024-01-15T25:00:00Z",     # hour 25
            "2024-01-15T08:60:00Z",     # minute 60
            "2024-01-15T08:23:11+00:00",  # offset rather than Z
            "2024-01-15T08:23:11",      # no timezone designator
            "2024-01-15",               # date only
            "15/01/2024",               # wrong format
            "",
        ]:
            with self.subTest(date=bad):
                txn, _ = validate_record(make_record(transaction_date=bad))
                self.assertIsNone(txn)

    def test_accepts_leap_day(self):
        """2024 is a leap year, so 29 February is valid."""
        _, reasons = validate_record(
            make_record(transaction_date="2024-02-29T12:00:00Z")
        )
        self.assertEqual(reasons, [])


class TestCountryCode(unittest.TestCase):
    def test_rejects_unassigned_lookalikes(self):
        """All match ^[A-Z]{2}$ but are not assigned codes. 'UK' appears in
        TXN-0345 and 'EN' in TXN-0347."""
        for bad in ["UK", "EN", "XX", "ZZ", "EU"]:
            with self.subTest(code=bad):
                txn, reasons = validate_record(make_record(country_code=bad))
                self.assertIsNone(txn)
                self.assertTrue(
                    any("not an officially assigned" in r for r in reasons)
                )

    def test_accepts_every_code_present_in_the_dataset(self):
        for good in ["US", "GB", "DE", "NL", "FR", "ES", "AU", "JP"]:
            with self.subTest(code=good):
                _, reasons = validate_record(make_record(country_code=good))
                self.assertEqual(reasons, [])

    def test_rejects_malformed(self):
        for bad in ["us", "Us", "USA", "U", "", "U1", "  ", "12"]:
            with self.subTest(code=bad):
                txn, _ = validate_record(make_record(country_code=bad))
                self.assertIsNone(txn)


class TestMerchantName(unittest.TestCase):
    def test_rejects_whitespace_only(self):
        """A string of spaces passes a length check and must still fail.
        TXN-0346 carries three spaces."""
        for bad in ["", " ", "   ", "\t", "\n", "  \t "]:
            with self.subTest(name=repr(bad)):
                txn, reasons = validate_record(make_record(merchant_name=bad))
                self.assertIsNone(txn)
                self.assertTrue(
                    any(r.startswith("merchant_name:") for r in reasons)
                )

    def test_allows_punctuation(self):
        """Real merchants in the dataset include B&Q and Macy's."""
        for name in ["B&Q", "Macy's", "Whole Foods", "ACH Transfer", "H&M"]:
            with self.subTest(name=name):
                _, reasons = validate_record(make_record(merchant_name=name))
                self.assertEqual(reasons, [])


class TestCaseSensitivity(unittest.TestCase):
    def test_enums_are_case_sensitive(self):
        for field, bad in [
            ("currency", "usd"),
            ("currency", "Usd"),
            ("transaction_type", "Credit"),    # TXN-0347
            ("transaction_type", "DEBIT"),
            ("status", "Completed"),           # TXN-0346
            ("status", "COMPLETED"),
            ("merchant_category", "Groceries"),
            ("merchant_category", "E-Commerce"),
        ]:
            with self.subTest(field=field, value=bad):
                txn, reasons = validate_record(make_record(**{field: bad}))
                self.assertIsNone(txn)
                self.assertTrue(any(r.startswith(f"{field}:") for r in reasons))

    def test_enums_reject_values_outside_the_set(self):
        for field, bad in [
            ("merchant_category", "fast_food"),  # TXN-0345
            ("merchant_category", "Finance"),    # TXN-0347
            ("currency", "INR"),
            ("currency", "BTC"),
            ("status", "cancelled"),
            ("transaction_type", "refund"),
        ]:
            with self.subTest(field=field, value=bad):
                txn, _ = validate_record(make_record(**{field: bad}))
                self.assertIsNone(txn)


class TestIdentifierFormats(unittest.TestCase):
    def test_transaction_id_format(self):
        for bad in ["TXN0001", "txn-0001", "TX-0001", "0001", "", "TXN-"]:
            with self.subTest(value=bad):
                txn, _ = validate_record(make_record(transaction_id=bad))
                self.assertIsNone(txn)

    def test_account_id_format(self):
        for bad in ["ACC1001", "acc-1001", "ACC-101", "ACC-10011", "ACC-ABCD"]:
            with self.subTest(value=bad):
                txn, _ = validate_record(make_record(account_id=bad))
                self.assertIsNone(txn)


class TestErrorReporting(unittest.TestCase):
    def test_all_violations_reported_not_just_the_first(self):
        """The planted defects carry three and four violations each. A
        validator that short-circuits would hide most of them and make the
        quarantine output useless for remediation."""
        txn, reasons = validate_record(
            make_record(
                amount="-5",
                country_code="UK",
                merchant_name="   ",
                status="Completed",
            )
        )
        self.assertIsNone(txn)
        self.assertEqual(len(reasons), 4)
        self.assertEqual(
            {r.split(":")[0] for r in reasons},
            {"amount", "country_code", "merchant_name", "status"},
        )

    def test_reasons_ordered_by_schema_field_order(self):
        """Stable ordering keeps quarantine output diffable across runs."""
        _, reasons = validate_record(
            make_record(country_code="UK", amount="-5", transaction_date="nope")
        )
        self.assertEqual(
            [r.split(":")[0] for r in reasons],
            ["transaction_date", "amount", "country_code"],
        )

    def test_missing_required_field_is_reported(self):
        for field in SCHEMA_FIELDS:
            with self.subTest(field=field):
                record = make_record()
                del record[field]
                txn, reasons = validate_record(record)
                self.assertIsNone(txn)
                self.assertTrue(any(r.startswith(f"{field}:") for r in reasons))

    def test_completely_empty_record_reports_every_field(self):
        txn, reasons = validate_record({})
        self.assertIsNone(txn)
        self.assertEqual(len(reasons), len(SCHEMA_FIELDS))


class TestSchemaConstants(unittest.TestCase):
    def test_natural_key_excludes_transaction_id(self):
        """Duplicates are defined as matching on every field except the id."""
        self.assertNotIn("transaction_id", NATURAL_KEY_FIELDS)
        self.assertEqual(len(NATURAL_KEY_FIELDS), len(SCHEMA_FIELDS) - 1)

    def test_schema_has_ten_fields(self):
        self.assertEqual(len(SCHEMA_FIELDS), 10)


if __name__ == "__main__":
    unittest.main()
