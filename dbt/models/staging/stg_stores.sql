{{
    config(
        materialized='view'
    )
}}

with source as (
    select distinct
        customer_id,
        store_id,
        store_state,
        store_city
    from {{ source('retail', 'orders') }}
),

final as (
    select
        store_id,
        store_state,
        store_city,
        count(distinct customer_id) as total_customers,
        row_number() over (partition by store_state order by store_city) as store_city_rank
    from source
    group by 1, 2, 3
)

select * from final
