{{
    config(
        materialized='view'
    )
}}

-- ===========================================================================
-- stg_transactions
--
-- The silver layer: validated, deduplicated, typed. This is the single place
-- where "what counts as a usable transaction" is answered, so that a second
-- mart cannot answer it differently. That divergence - the same metric
-- meaning two things in two dashboards - is the failure this layer exists to
-- prevent.
--
-- Bronze is not filtered for validity here because invalid records never
-- reach bronze: they are routed to quarantine at ingestion. What this model
-- does resolve is duplication.
-- ===========================================================================

with bronze as (

    select * from {{ source('raw', 'bronze_transactions') }}

),

deduplicated as (

    select
        transaction_id,
        source_id,
        account_id,
        transaction_date,

        -- Cast at the silver boundary, not in the mart. Bronze stores the
        -- amount as text to avoid any float round-trip through storage; this
        -- is the one place it becomes a number.
        cast(amount as {{ dbt.type_numeric() }}) as amount,

        currency,
        transaction_type,
        merchant_name,
        merchant_category,
        status,
        country_code,
        natural_key_hash,

        -- UTC calendar date, which is the grain of the daily mart.
        cast(substr(transaction_date, 1, 10) as date) as transaction_day,

        ingestion_timestamp,
        ingestion_run_id,
        source_system

    from bronze

    -- Duplicates are flagged at ingestion rather than dropped, so bronze
    -- stays a faithful record of what the source sent and the duplicate rate
    -- remains observable. The correctness decision is applied here, where it
    -- is reversible by rebuilding this view.
    where is_duplicate = 0

)

select * from deduplicated
