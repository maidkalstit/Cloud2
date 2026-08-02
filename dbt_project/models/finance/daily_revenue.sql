{{
    config(
        materialized='incremental',
        unique_key='revenue_date',
        incremental_strategy='merge'
    )
}}

with raw_transactions as (
    select * from {{ source('silver', 'silver_transactions') }}
    where created_date is not null 
      and trim(created_date) != ''
      and order_id is not null
      and trim(order_id) != ''
    
    {% if is_incremental() %}
      and created_date >= (select coalesce(max(revenue_date), '1970-01-01') from {{ this }} where revenue_date is not null and trim(revenue_date) != '')
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
    where created_date is not null and trim(created_date) != ''
    group by created_date
)

select * from daily_aggregation
where revenue_date is not null and trim(revenue_date) != ''