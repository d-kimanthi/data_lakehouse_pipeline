{{
    config(
        materialized='table'
    )
}}

with orders as (
    select * from {{ source('retail', 'orders') }}
),

dim_stores as (
    select * from {{ ref('dim_stores') }}
),

dim_products as (
    select * from {{ ref('dim_products') }}
),

dim_customers as (
    select * from {{ ref('dim_customers') }}
),

final as (
    select
        orders.order_id,
        dim_customers.customer_key,
        dim_stores.store_key,
        dim_products.product_key,
        orders.quantity,
        orders.unit_price,
        orders.amount,
        orders.currency,
        orders.payment_method,
        orders.event_time,
        orders.processing_time
    from orders
    left join dim_stores 
        on orders.store_id = dim_stores.store_id
    left join dim_products 
        on orders.product_id = dim_products.product_id
    left join dim_customers 
        on orders.customer_id = dim_customers.customer_id
)

select * from final
