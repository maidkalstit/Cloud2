{{
    config(
        materialized='table'
    )
}}

with raw_transactions as (
    -- Reads directly from our dynamic source contract
    select * from {{ source('silver', 'silver_transactions') }}
),

customer_metrics as (
    select
        customer_id,
        min(created_date) as first_purchase_date,
        max(created_date) as last_purchase_date,
        count(order_id) as total_orders,
        
        -- Rule 1: Rigid monetary precision enforcement using explicit decimal casting
        cast(sum(payment_value) as decimal(18,2)) as total_spent,
        
        -- Spark SQL Specific Optimization: Aggregates distinct historical states into a single string
        -- Why: Using collect_set avoids string duplicates across rows, providing a clean 
        -- historical comma-separated trace of statuses for marketing list segmentation.
        concat_ws(', ', collect_set(order_status)) as order_statuses
    from raw_transactions
    group by customer_id
)

select * from customer_metrics