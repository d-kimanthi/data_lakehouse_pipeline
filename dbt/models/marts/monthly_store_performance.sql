{{
    config(
        materialized='table'
    )
}}

with fact_sales as (
    select * from {{ ref('fact_sales') }}
),

dim_stores as (
    select * from {{ ref('dim_stores') }}
),

dim_products as (
    select * from {{ ref('dim_products') }}
),

store_metrics as (
    select
        s.store_id,
        s.store_state,
        s.store_city,
        date_trunc('month', f.event_time) as month,
        p.product_category,
        count(distinct f.order_id) as total_orders,
        count(distinct customer_key) as unique_customers,
        sum(f.quantity) as total_items_sold,
        sum(f.amount) as total_revenue,
        sum(f.amount) / count(distinct f.order_id) as avg_order_value,
        sum(case when f.payment_method = 'credit_card' then f.amount else 0 end) as credit_card_revenue,
        sum(case when f.payment_method = 'mobile_payment' then f.amount else 0 end) as mobile_payment_revenue
    from fact_sales f
    join dim_stores s on f.store_key = s.store_key
    join dim_products p on f.product_key = p.product_key
    group by 1, 2, 3, 4, 5
)

select * from store_metrics
