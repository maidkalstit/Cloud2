{{
    config(
        materialized='table'
    )
}}

with raw_transactions as (
    -- Reads directly from our dynamic source contract
    select 
        *,
        -- Business Mapping: Translate technical remediation flags into Business-friendly labels
        case 
            when order_status = 'UNKNOWN' and _is_remediated = true 
                then 'Thiếu thông tin trạng thái'
            else order_status 
        end as business_order_status
    from {{ source('silver', 'silver_transactions') }}
    where customer_id is not null
      and order_id is not null
      and created_date is not null
),

customer_metrics as (
    select
        customer_id,
        min(created_date) as first_purchase_date,
        max(created_date) as last_purchase_date,
        count(order_id) as total_orders,
        
        -- Rule 1: Rigid monetary precision enforcement using native decimal aggregation
        sum(parsed_payment_value) as total_spent,
        
        -- Spark SQL Specific Optimization: Aggregates distinct historical states into a single string
        -- We use the business-friendly mapped status for BI consumption
        concat_ws(', ', collect_set(business_order_status)) as order_statuses
    from raw_transactions
    group by customer_id
)

select * from customer_metrics