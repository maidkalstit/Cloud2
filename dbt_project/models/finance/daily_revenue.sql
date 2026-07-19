{{
    config(
        materialized='incremental',
        unique_key='revenue_date',
        incremental_strategy='merge'
    )
}}

with raw_transactions as (
    select * from {{ source('silver', 'silver_transactions') }}
    
    {% if is_incremental() %}
    -- Why: 'date_sub' filters and processes only the last 3 days of data during incremental runs.
    -- This handles late-arriving streaming records safely without triggering a full table scan,
    -- keeping our compute cost at absolute zero on our 4GB RAM VM.
    where created_date >= date_sub(current_date(), 3)
    {% endif %}
),

daily_aggregation as (
    select
        created_date as revenue_date,
        count(distinct order_id) as total_orders,
        
        -- Rule 1: Strict precision preservation. Summing decimal fields must result 
        -- in a DECIMAL(18,2) representation to prevent accounting discrepancies.
        cast(sum(payment_value) as decimal(18,2)) as gross_revenue,
        cast(avg(payment_value) as decimal(18,2)) as average_order_value
    from raw_transactions
    group by created_date
)

select * from daily_aggregation