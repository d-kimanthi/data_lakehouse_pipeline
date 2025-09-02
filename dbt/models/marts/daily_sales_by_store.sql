{{
    config(
        materialized='table'
    )
}}

with source_orders as (
    select * from {{ source('retail', 'orders') }}
),

final as (
    select
        store_id,
        store_state,
        store_city,
        date_trunc('day', event_time) as date,
        category as product_category,
        count(distinct order_id) as total_orders,
        sum(quantity) as total_items_sold,
        sum(quantity * unit_price) as total_revenue,
        avg(unit_price) as avg_unit_price,
        count(distinct customer_id) as unique_customers,
        sum(case when payment_method = 'credit_card' then 1 else 0 end) as credit_card_transactions,
        sum(case when payment_method = 'debit_card' then 1 else 0 end) as debit_card_transactions,
        sum(case when payment_method = 'cash' then 1 else 0 end) as cash_transactions,
        sum(case when payment_method = 'mobile_payment' then 1 else 0 end) as mobile_payment_transactions
    from source_orders
    group by 1, 2, 3, 4, 5
)

select * from final
