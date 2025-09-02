{{
    config(
        materialized='view'
    )
}}

with source as (
    select distinct
        customer_id,
        store_id,
        payment_method
    from {{ source('retail', 'orders') }}
),

customer_stats as (
    select
        customer_id,
        count(distinct store_id) as visited_stores,
        array_agg(distinct payment_method) as payment_methods_used
    from source
    group by 1
),

final as (
    select
        customer_id,
        visited_stores,
        payment_methods_used,
        case 
            when visited_stores = 1 then 'Single Store'
            when visited_stores <= 3 then 'Few Stores'
            else 'Multi Store'
        end as customer_segment
    from customer_stats
)

select * from final
