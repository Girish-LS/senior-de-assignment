-- ===========================================================================
-- daily_account_summary
--
-- One row per (account_id, transaction_date), covering completed transactions
-- only. Quarantined records are structurally excluded: they never reach
-- bronze, so no filter is required and none can be forgotten.
--
-- IDEMPOTENCY
-- -----------
-- Implemented as delete-then-insert on whole grouping keys, not as an update
-- or an append.
--
-- The distinction matters. An aggregate must be recomputed from all of its
-- constituent rows: if a late transaction arrives for a past date, that
-- date's row has to be rebuilt entirely, not adjusted. Appending to an
-- aggregate produces a number that is silently wrong, and merging matched
-- rows invites exactly that mistake. Deleting the grouping key and
-- recomputing expresses the correct intent.
--
-- Re-running over unchanged input therefore produces byte-identical output,
-- apart from updated_at which is by definition the time of computation.
--
-- DETERMINISM
-- -----------
-- top_category breaks ties by category name ascending. Without an explicit
-- tie-break, two runs over identical input could return different values and
-- the idempotency claim would be false in a way that is easy to miss.
--
-- KNOWN LIMITATION: MULTI-CURRENCY SUMMATION
-- ------------------------------------------
-- total_debit_amount and total_credit_amount sum `amount` across up to seven
-- currencies. The result adds USD to JPY and is therefore arithmetically
-- valid and financially meaningless.
--
-- This is implemented as specified rather than silently corrected: fixing it
-- needs an FX rate source and an as-of convention, which is a product
-- decision rather than an implementation detail. The `currencies` column
-- exposes the problem to consumers so it cannot be mistaken for a
-- single-currency figure, and it is the first assumption raised in the
-- design note. The production options are a per-currency grain, or native
-- amounts alongside a reporting-currency column computed from a dated FX
-- table.
--
-- DEFINITION OF SPEND
-- -------------------
-- top_category ranks by debit total only. Credits are inflows - payroll and
-- transfers in - and including them would let a salary payment outrank every
-- actual purchase. Stated here and in the contract so the definition is not
-- rediscovered differently by the next person.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS daily_account_summary (
    account_id          TEXT    NOT NULL,
    transaction_date    TEXT    NOT NULL,   -- UTC calendar date, YYYY-MM-DD
    total_debit_amount  REAL    NOT NULL,
    total_credit_amount REAL    NOT NULL,
    net_amount          REAL    NOT NULL,
    transaction_count   INTEGER NOT NULL,
    distinct_merchants  INTEGER NOT NULL,
    top_category        TEXT,
    currencies          TEXT    NOT NULL,
    updated_at          TEXT    NOT NULL,
    PRIMARY KEY (account_id, transaction_date)
);

-- ---------------------------------------------------------------------------
-- Step 1: remove the grouping keys about to be recomputed.
--
-- Scoped to the keys present in the source selection rather than truncating
-- the table, so an incremental run touches only affected days. With no
-- incremental filter applied this is equivalent to a full rebuild.
-- ---------------------------------------------------------------------------
DELETE FROM daily_account_summary
WHERE (account_id, transaction_date) IN (
    SELECT account_id, substr(transaction_date, 1, 10)
    FROM bronze_transactions
    WHERE status = 'completed'
      AND is_duplicate = 0
      AND transaction_date >= :since
);

-- ---------------------------------------------------------------------------
-- Step 2: recompute and insert.
-- ---------------------------------------------------------------------------
INSERT INTO daily_account_summary (
    account_id,
    transaction_date,
    total_debit_amount,
    total_credit_amount,
    net_amount,
    transaction_count,
    distinct_merchants,
    top_category,
    currencies,
    updated_at
)
WITH completed AS (
    SELECT
        account_id,
        substr(transaction_date, 1, 10) AS transaction_day,
        CAST(amount AS REAL)            AS amount,
        currency,
        transaction_type,
        merchant_name,
        merchant_category
    FROM bronze_transactions
    WHERE status = 'completed'
      -- Deduplication: keep the surviving record of each natural-key group.
      -- Duplicates are flagged in bronze rather than dropped at ingestion,
      -- so the decision stays reversible and the duplicate rate stays
      -- observable. See ingestion/dedupe.py.
      AND is_duplicate = 0
      AND transaction_date >= :since
),

aggregated AS (
    SELECT
        account_id,
        transaction_day,
        SUM(CASE WHEN transaction_type = 'debit'  THEN amount ELSE 0 END)
            AS total_debit_amount,
        SUM(CASE WHEN transaction_type = 'credit' THEN amount ELSE 0 END)
            AS total_credit_amount,
        COUNT(*)                        AS transaction_count,
        COUNT(DISTINCT merchant_name)   AS distinct_merchants
    FROM completed
    GROUP BY account_id, transaction_day
),

-- Rank categories by debit spend within each account-day. The ORDER BY
-- carries an explicit tie-break on category name so the result is stable
-- across runs.
category_spend AS (
    SELECT
        account_id,
        transaction_day,
        merchant_category,
        SUM(CASE WHEN transaction_type = 'debit' THEN amount ELSE 0 END)
            AS category_debit,
        ROW_NUMBER() OVER (
            PARTITION BY account_id, transaction_day
            ORDER BY
                SUM(CASE WHEN transaction_type = 'debit' THEN amount ELSE 0 END) DESC,
                merchant_category ASC
        ) AS rank_in_day
    FROM completed
    GROUP BY account_id, transaction_day, merchant_category
),

top_category AS (
    SELECT account_id, transaction_day, merchant_category
    FROM category_spend
    WHERE rank_in_day = 1
      -- A day with credits only has no spend, so "the category with the
      -- highest total spend" is undefined. Returning the alphabetically
      -- first category of a zero-spend day would be arbitrary and
      -- misleading - a payroll credit would appear as the day's top
      -- spending category. NULL is the honest answer, and the contract
      -- documents that this column is nullable for credit-only days.
      AND category_debit > 0
)

SELECT
    a.account_id,
    a.transaction_day,
    ROUND(a.total_debit_amount, 2)                              AS total_debit_amount,
    ROUND(a.total_credit_amount, 2)                             AS total_credit_amount,
    ROUND(a.total_credit_amount - a.total_debit_amount, 2)      AS net_amount,
    a.transaction_count,
    a.distinct_merchants,
    t.merchant_category                                         AS top_category,
    -- Distinct currencies, sorted, as a comma-separated string. Sorting makes
    -- the value comparable between runs; an unordered concatenation would
    -- differ run to run and break the idempotency test.
    (
        SELECT GROUP_CONCAT(cur, ',')
        FROM (
            SELECT DISTINCT c.currency AS cur
            FROM completed c
            WHERE c.account_id = a.account_id
              AND c.transaction_day = a.transaction_day
            ORDER BY c.currency
        )
    )                                                           AS currencies,
    :run_timestamp                                              AS updated_at
FROM aggregated a
LEFT JOIN top_category t
       ON t.account_id = a.account_id
      AND t.transaction_day = a.transaction_day;
