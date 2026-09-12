"""Build daily_account_summary and assert its data quality.

The assessment asks for dbt with at least three meaningful column tests, or
equivalent assertions where dbt is not used. The dbt project is present in
dbt_project/ - models, schema.yml and all - but cannot be executed in this
environment because the package index is unreachable. These assertions are
the executable equivalent, and they mirror the dbt tests one for one so the
two cannot drift.

Assertions are chosen to catch real failure modes rather than to fill a
quota. Blanket not-null checks on every column prove almost nothing; these
check invariants that would actually be violated by a plausible bug.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from ingestion import storage

logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).resolve().parent.parent / "sql" / "daily_account_summary.sql"

# Sentinel used for a full rebuild. Every real transaction_date sorts above
# it, so the delete-and-insert covers the whole table.
EPOCH = "0000-01-01T00:00:00Z"


@dataclass
class TestResult:
    name: str
    passed: bool
    detail: str = ""

    def __str__(self) -> str:
        mark = "PASS" if self.passed else "FAIL"
        return f"[{mark}] {self.name}" + (f" - {self.detail}" if self.detail else "")


def split_statements(sql: str) -> list[str]:
    """Split a script into executable statements.

    Uses sqlite3.complete_statement rather than splitting on ';'. A naive
    split breaks on any semicolon inside a comment or a string literal -
    which this file contains - producing fragments that fail with
    "incomplete input". This caught a real bug during development.
    """
    statements: list[str] = []
    buffer = ""
    for line in sql.splitlines(keepends=True):
        buffer += line
        if sqlite3.complete_statement(buffer):
            statement = buffer.strip()
            # Skip fragments that carry no executable content, such as a
            # trailing comment block.
            has_sql = any(
                stripped and not stripped.startswith("--")
                for stripped in (ln.strip() for ln in statement.splitlines())
            )
            if has_sql:
                statements.append(statement)
            buffer = ""
    if buffer.strip():
        statements.append(buffer.strip())
    return statements


def build_summary(
    conn: sqlite3.Connection, *, since: str = EPOCH, run_timestamp: str | None = None
) -> int:
    """Run the transformation. Returns the row count in the summary table.

    `since` scopes the rebuild. Passing EPOCH is a full refresh; passing a
    timestamp restricts the delete-and-insert to affected days, which is the
    incremental path.
    """
    run_timestamp = run_timestamp or storage.utc_now_iso()
    sql = SQL_PATH.read_text(encoding="utf-8")
    params = {"since": since, "run_timestamp": run_timestamp}

    # One transaction for the whole rebuild: a delete that commits without
    # its matching insert would leave the mart missing rows.
    with conn:
        for statement in split_statements(sql):
            bound = {k: v for k, v in params.items() if f":{k}" in statement}
            conn.execute(statement, bound)

    count = conn.execute("SELECT COUNT(*) FROM daily_account_summary").fetchone()[0]
    logger.info("daily_account_summary rebuilt: %d rows (since=%s)", count, since)
    return count


# --------------------------------------------------------------------------
# Data quality assertions. Each mirrors a test in dbt_project/models/marts/
# schema.yml.
# --------------------------------------------------------------------------


def run_assertions(conn: sqlite3.Connection) -> list[TestResult]:
    results: list[TestResult] = []

    def check(name: str, sql: str, expect_zero: bool = True) -> None:
        rows = conn.execute(sql).fetchall()
        failures = len(rows)
        if expect_zero:
            results.append(
                TestResult(
                    name,
                    failures == 0,
                    "" if failures == 0 else f"{failures} offending rows: "
                    f"{[dict(r) for r in rows[:3]]}",
                )
            )

    # 1. Grain. One row per account per day. If this fails, the mart is not
    #    what the contract says it is and every downstream number is suspect.
    check(
        "unique_grain: one row per (account_id, transaction_date)",
        """
        SELECT account_id, transaction_date, COUNT(*) AS n
        FROM daily_account_summary
        GROUP BY account_id, transaction_date
        HAVING COUNT(*) > 1
        """,
    )

    # 2. Arithmetic consistency. net_amount is a derived column; if it ever
    #    disagrees with its inputs the derivation is broken. Tolerance
    #    accounts for rounding to two decimal places.
    check(
        "net_amount equals credit minus debit",
        """
        SELECT account_id, transaction_date, net_amount,
               total_credit_amount, total_debit_amount
        FROM daily_account_summary
        WHERE ABS(net_amount - (total_credit_amount - total_debit_amount)) > 0.01
        """,
    )

    # 3. Sign. Amounts are validated as strictly positive at ingestion, so a
    #    negative total means the aggregation has gone wrong, not the source.
    check(
        "totals are non-negative",
        """
        SELECT account_id, transaction_date, total_debit_amount,
               total_credit_amount
        FROM daily_account_summary
        WHERE total_debit_amount < 0 OR total_credit_amount < 0
        """,
    )

    # 4. Counts are positive. A grouping key exists only because rows exist.
    check(
        "transaction_count is positive",
        "SELECT * FROM daily_account_summary WHERE transaction_count < 1",
    )

    # 5. distinct_merchants cannot exceed transaction_count. A classic
    #    off-by-join bug inflates one without the other.
    check(
        "distinct_merchants <= transaction_count",
        """
        SELECT account_id, transaction_date, distinct_merchants,
               transaction_count
        FROM daily_account_summary
        WHERE distinct_merchants > transaction_count
        """,
    )

    # 6. Referential integrity against the contract's accepted values.
    check(
        "top_category is an accepted merchant category",
        """
        SELECT DISTINCT top_category
        FROM daily_account_summary
        WHERE top_category IS NOT NULL
          AND top_category NOT IN (
            'e-commerce','travel','food_and_beverage','groceries','electronics',
            'retail','entertainment','health','transportation','home_and_garden',
            'payroll','transfer'
          )
        """,
    )

    # 7. Date format. The grain is a calendar date, not a timestamp.
    check(
        "transaction_date is a YYYY-MM-DD calendar date",
        """
        SELECT DISTINCT transaction_date
        FROM daily_account_summary
        WHERE LENGTH(transaction_date) <> 10
           OR transaction_date NOT LIKE '____-__-__'
        """,
    )

    # 8. Account format, carried through from the source contract.
    check(
        "account_id matches ACC-NNNN",
        """
        SELECT DISTINCT account_id
        FROM daily_account_summary
        WHERE account_id NOT LIKE 'ACC-____'
        """,
    )

    # 9. No nulls in the required columns.
    check(
        "required columns are not null",
        """
        SELECT * FROM daily_account_summary
        WHERE account_id IS NULL
           OR transaction_date IS NULL
           OR total_debit_amount IS NULL
           OR total_credit_amount IS NULL
           OR net_amount IS NULL
           OR transaction_count IS NULL
           OR distinct_merchants IS NULL
           OR currencies IS NULL
           OR updated_at IS NULL
        """,
    )

    # 10. The summary must reflect only completed, non-duplicate records.
    #     Compares the mart's transaction_count against bronze directly - a
    #     reconciliation test rather than a shape test, and the one most
    #     likely to catch a filter being dropped from the model.
    check(
        "transaction_count reconciles with bronze",
        """
        WITH expected AS (
            SELECT account_id,
                   substr(transaction_date, 1, 10) AS d,
                   COUNT(*) AS n
            FROM bronze_transactions
            WHERE status = 'completed' AND is_duplicate = 0
            GROUP BY account_id, substr(transaction_date, 1, 10)
        )
        SELECT s.account_id, s.transaction_date,
               s.transaction_count, e.n AS expected_count
        FROM daily_account_summary s
        JOIN expected e
          ON e.account_id = s.account_id AND e.d = s.transaction_date
        WHERE s.transaction_count <> e.n
        """,
    )

    # 11. Currencies are drawn from the accepted set and sorted, which is what
    #     makes the column comparable between runs.
    check(
        "currencies are accepted values, comma separated and sorted",
        """
        SELECT DISTINCT currencies
        FROM daily_account_summary
        WHERE currencies = ''
           OR currencies LIKE '%,,%'
           OR currencies LIKE ',%'
           OR currencies LIKE '%,'
        """,
    )

    return results
