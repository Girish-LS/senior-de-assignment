{{
    config(
        materialized='incremental',
        unique_key=['account_id', 'transaction_date'],
        incremental_strategy='delete+insert',
        on_schema_change='fail'
    )
}}

-- ===========================================================================
-- daily_account_summary
--
-- One row per account per UTC calendar day, completed transactions only.
--
-- WHY delete+insert AND NOT merge
-- --------------------------------
-- An aggregate must be recomputed from all of its constituent rows. `merge`
-- updates matched rows, which invites the mistake of adjusting an aggregate
-- rather than rebuilding it: if a late transaction arrives for a past date,
-- the old row's contribution has to be discarded entirely, not amended.
-- Deleting the whole grouping key and recomputing expresses that correctly.
--
-- `append` would be simply wrong here: it produces multiple rows per
-- grouping key and breaks the stated grain.
--
-- WHY incremental AND NOT a full table
-- -------------------------------------
-- A full rebuild is trivially idempotent and is the safer choice at this
-- volume - and `dbt run --full-refresh` remains supported and is exercised
-- in CI precisely because it is the recovery path. Incremental is used
-- because compute cost should scale with new data rather than with total
-- history, and cost ownership is a first-class engineering concern rather
-- than an afterthought.
--
-- The incremental predicate filters on transaction_date with a lookback,
-- matching the ingestion lookback, so a late-arriving record that lands in
-- bronze also triggers recomputation of its business date.
-- ===========================================================================

with completed as (

    select
        account_id,
        transaction_day,
        amount,
        currency,
        transaction_type,
        merchant_name,
        merchant_category
    from {{ ref('stg_transactions') }}
    where status = 'completed'

    {% if is_incremental() %}
      -- Recompute only days that could have changed. Without the lookback a
      -- transaction dated last Tuesday but loaded today would never trigger
      -- a rebuild of last Tuesday, and the mart would stay quietly wrong.
      and transaction_day >= (
          select coalesce(max(transaction_date), '1900-01-01')
                 - interval '{{ var("summary_lookback_days") }} days'
          from {{ this }}
      )
    {% endif %}

),

aggregated as (

    select
        account_id,
        transaction_day,
        sum(case when transaction_type = 'debit'  then amount else 0 end)
            as total_debit_amount,
        sum(case when transaction_type = 'credit' then amount else 0 end)
            as total_credit_amount,
        count(*)                      as transaction_count,
        count(distinct merchant_name) as distinct_merchants,
        {{ dbt.listagg(
              measure='distinct currency',
              delimiter_text="','",
              order_by_clause='order by currency'
        ) }} as currencies
    from completed
    group by account_id, transaction_day

),

-- Rank categories by debit spend. The explicit tie-break on category name is
-- what makes this deterministic: without it, two runs over identical input
-- could return different values and the idempotency claim would be false.
category_spend as (

    select
        account_id,
        transaction_day,
        merchant_category,
        sum(case when transaction_type = 'debit' then amount else 0 end)
            as category_debit,
        row_number() over (
            partition by account_id, transaction_day
            order by
                sum(case when transaction_type = 'debit' then amount else 0 end) desc,
                merchant_category asc
        ) as rank_in_day
    from completed
    group by account_id, transaction_day, merchant_category

),

top_category as (

    select account_id, transaction_day, merchant_category
    from category_spend
    where rank_in_day = 1
      -- Undefined for a day with credits only: there is no spend to rank.
      -- NULL rather than an arbitrary category.
      and category_debit > 0

)

select
    a.account_id,
    a.transaction_day                                        as transaction_date,

    -- KNOWN LIMITATION: these sum across up to seven currencies, so the
    -- figure adds USD to JPY and is not meaningful as money. Implemented as
    -- specified; the `currencies` column exposes the problem to consumers.
    -- See docs/product_platform_note.md for the production options.
    cast(round(a.total_debit_amount, 2) as decimal(18,2))     as total_debit_amount,
    cast(round(a.total_credit_amount, 2) as decimal(18,2))    as total_credit_amount,
    cast(round(a.total_credit_amount - a.total_debit_amount, 2)
         as decimal(18,2))                                    as net_amount,

    a.transaction_count,
    a.distinct_merchants,
    t.merchant_category                                       as top_category,
    a.currencies,
    cast({{ dbt.current_timestamp() }} as timestamp)                             as updated_at

from aggregated a
left join top_category t
       on t.account_id     = a.account_id
      and t.transaction_day = a.transaction_day

