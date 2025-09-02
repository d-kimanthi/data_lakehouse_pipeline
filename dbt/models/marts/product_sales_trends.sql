{{
    config(
        materialized='table'
    )
}}

with fact_sales as (
    select * from {{ ref('fact_sales') }}
),

dim_products as (
    select * from {{ ref('dim_products') }}
),

product_metrics as (
    select
        p.product_id,
        p.product_name,
        p.product_category,
        date_trunc('day', f.event_time) as sale_date,
        sum(f.quantity) as daily_units_sold,
        sum(f.amount) as daily_revenue,
        count(distinct f.order_id) as number_of_orders,
        count(distinct f.customer_key) as unique_customers
    from fact_sales f
    join dim_products p on f.product_key = p.product_key
    group by 1, 2, 3, 4
),

product_trends as (
    select
        *,
        avg(daily_units_sold) over (
            partition by product_id
            order by sale_date
            rows between 6 preceding and current row
        ) as moving_avg_units_7day,
        avg(daily_revenue) over (
            partition by product_id
            order by sale_date
            rows between 6 preceding and current row
        ) as moving_avg_revenue_7day,
        daily_units_sold - lag(daily_units_sold, 1) over (
            partition by product_id
            order by sale_date
        ) as units_sold_day_over_day_change
    from product_metrics
)

select * from product_trends
