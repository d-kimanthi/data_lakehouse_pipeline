{{
    config(
        materialized='table'
    )
}}

with fact_sales as (
    select * from {{ ref('fact_sales') }}
),

dim_customers as (
    select * from {{ ref('dim_customers') }}
),

dim_products as (
    select * from {{ ref('dim_products') }}
),

customer_metrics as (
    select
        c.customer_id,
        c.customer_segment,
        p.product_category,
        count(distinct f.order_id) as total_orders,
        sum(f.quantity) as total_items_purchased,
        sum(f.amount) as total_spent,
        avg(f.amount) as avg_order_value,
        array_agg(distinct f.payment_method) as payment_methods_used,
        min(f.event_time) as first_purchase_date,
        max(f.event_time) as last_purchase_date,
        datediff('day', min(f.event_time), max(f.event_time)) as customer_lifetime_days
    from fact_sales f
    join dim_customers c on f.customer_key = c.customer_key
    join dim_products p on f.product_key = p.product_key
    group by 1, 2, 3
),

customer_segments as (
    select
        customer_id,
        customer_segment,
        product_category,
        total_orders,
        total_spent,
        avg_order_value,
        customer_lifetime_days,
        case
            when total_spent > 1000 and total_orders > 10 then 'VIP'
            when total_spent > 500 then 'High Value'
            when total_orders > 5 then 'Frequent'
            else 'Standard'
        end as value_segment
    from customer_metrics
)

select * from customer_segments
